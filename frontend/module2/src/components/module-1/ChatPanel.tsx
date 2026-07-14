import React, { useState, useRef, useEffect, useCallback } from "react";
import { X, Send, Loader2, MessageSquare, Trash2, Sparkles } from "lucide-react";
import { motion, AnimatePresence } from "motion/react";
import { softSpring } from "../common/ui";

interface ChatMessage {
  role: "user" | "assistant" | "system_note";
  content: string;
}

interface SelectedItem {
  type: string;
  id: string;
  label: string;
}

interface ChatPanelProps {
  sessionId: string;
  apiKey: string;
  stage: number;
  selectedItem: SelectedItem | null;
  isOpen: boolean;
  onClose: () => void;
  onClearSelection: () => void;
}

const STAGE_PROMPTS: Record<number, string[]> = {
  1: ["What file types are supported?", "How big can my upload be?", "What happens after upload?"],
  2: ["Summarize the extracted tables", "Any tables I should remove?", "What do the columns look like?"],
  3: ["How should I clean this data?", "Which columns can I drop?", "Explain deduplication"],
  4: ["Why were these groups chosen?", "Should I merge any groups?", "What are unassigned tables?"],
  5: ["How should I clean this data?", "Which columns can I drop?", "Check for duplicates"],
  6: ["Which table should be the base?", "Explain common columns", "What are good join keys?"],
  7: ["Summarize the merge results", "Why are some fill rates low?", "How do I improve quality?"],
  8: ["Which views are ready?", "How do I unlock more views?", "Explain partial views"],
};

function formatAssistantText(text: string) {
  const parts = text.split(/(`[^`]+`|\*\*[^*]+\*\*)/g);
  return parts.map((part, i) => {
    if (part.startsWith("`") && part.endsWith("`")) {
      return <code key={i} className="px-1 py-0.5 bg-neutral-100 dark:bg-neutral-800 text-red-700 dark:text-red-400 rounded text-[11px] font-mono">{part.slice(1, -1)}</code>;
    }
    if (part.startsWith("**") && part.endsWith("**")) {
      return <strong key={i} className="font-bold">{part.slice(2, -2)}</strong>;
    }
    return <span key={i}>{part}</span>;
  });
}

export default function ChatPanel({ sessionId, apiKey, stage, selectedItem, isOpen, onClose, onClearSelection }: ChatPanelProps) {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [isStreaming, setIsStreaming] = useState(false);
  const [streamedText, setStreamedText] = useState("");
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const abortRef = useRef<AbortController | null>(null);
  const prevStageRef = useRef(stage);
  const prevSelectedRef = useRef<string | null>(null);

  useEffect(() => {
    return () => { abortRef.current?.abort(); };
  }, []);

  const scrollToBottom = useCallback(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, []);

  useEffect(() => { scrollToBottom(); }, [messages, streamedText, scrollToBottom]);

  useEffect(() => {
    if (prevStageRef.current !== stage) {
      setMessages([]);
      setStreamedText("");
      prevStageRef.current = stage;
      prevSelectedRef.current = null;
    }
  }, [stage]);

  useEffect(() => {
    const newKey = selectedItem ? `${selectedItem.type}:${selectedItem.id}` : null;
    if (newKey && newKey !== prevSelectedRef.current) {
      prevSelectedRef.current = newKey;
      setMessages(prev => [...prev, { role: "system_note", content: `Now focused on: ${selectedItem!.label}` }]);
    } else if (!newKey && prevSelectedRef.current) {
      prevSelectedRef.current = null;
    }
  }, [selectedItem]);

  useEffect(() => {
    if (isOpen) inputRef.current?.focus();
  }, [isOpen]);

  const sendMessage = useCallback(async (text?: string) => {
    const trimmed = (text || input).trim();
    if (!trimmed || isStreaming) return;

    const userMsg: ChatMessage = { role: "user", content: trimmed };
    const newMessages = [...messages, userMsg];
    setMessages(newMessages);
    setInput("");
    setIsStreaming(true);
    setStreamedText("");

    const controller = new AbortController();
    abortRef.current = controller;

    try {
      const apiMessages = newMessages
        .filter(m => m.role !== "system_note")
        .map(m => ({ role: m.role, content: m.content }));

      const res = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sessionId,
          messages: apiMessages,
          stage,
          selectedItem,
          apiKey,
        }),
        signal: controller.signal,
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: "Chat request failed." }));
        setMessages(prev => [...prev, { role: "assistant", content: `Error: ${err.error}` }]);
        setIsStreaming(false);
        return;
      }

      if (!res.body) throw new Error("No response body");
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let accumulated = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        const text = decoder.decode(value, { stream: true });
        const lines = text.split("\n");

        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          const payload = line.slice(6).trim();
          if (payload === "[DONE]") continue;
          try {
            const parsed = JSON.parse(payload);
            if (parsed.content) {
              accumulated += parsed.content;
              setStreamedText(accumulated);
            }
            if (parsed.error) {
              accumulated += `\n\nError: ${parsed.error}`;
              setStreamedText(accumulated);
            }
          } catch { /* skip malformed chunks */ }
        }
      }

      if (accumulated) {
        setMessages(prev => [...prev, { role: "assistant", content: accumulated }]);
      }
    } catch (err: any) {
      if (err.name !== "AbortError") {
        setMessages(prev => [...prev, { role: "assistant", content: `Error: ${err.message}` }]);
      }
    } finally {
      setIsStreaming(false);
      setStreamedText("");
      abortRef.current = null;
    }
  }, [input, isStreaming, messages, sessionId, stage, selectedItem, apiKey]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  };

  const clearChat = () => {
    if (abortRef.current) abortRef.current.abort();
    setMessages([]);
    setStreamedText("");
    setIsStreaming(false);
  };

  if (!isOpen) return null;

  const starterPrompts = STAGE_PROMPTS[stage] || STAGE_PROMPTS[1];

  return (
    <motion.div
      initial={{ x: 384, opacity: 0 }}
      animate={{ x: 0, opacity: 1 }}
      exit={{ x: 384, opacity: 0 }}
      transition={softSpring}
      className="w-96 border-l border-neutral-200/80 dark:border-neutral-700/80 bg-white/95 dark:bg-neutral-900/95 backdrop-blur-sm flex flex-col h-full shrink-0"
    >
      {/* Header */}
      <div className="px-5 py-3.5 border-b border-neutral-200/80 dark:border-neutral-700/80 bg-gradient-to-r from-neutral-50 to-white dark:from-neutral-800 dark:to-neutral-900 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-2.5">
          <div className="w-7 h-7 rounded-lg bg-gradient-to-br from-red-600 to-rose-600 flex items-center justify-center">
            <Sparkles className="w-3.5 h-3.5 text-white" />
          </div>
          <div>
            <h3 className="text-sm font-semibold tracking-tight text-neutral-900 dark:text-white">Data Assistant</h3>
            <p className="text-[10px] text-neutral-400 dark:text-neutral-500">AI-powered insights</p>
          </div>
        </div>
        <div className="flex items-center gap-1">
          {messages.length > 0 && (
            <button onClick={clearChat} aria-label="Clear chat" className="p-1.5 rounded-lg hover:bg-neutral-100 dark:hover:bg-neutral-800 text-neutral-400 hover:text-neutral-700 dark:hover:text-neutral-200 transition-colors" title="Clear chat">
              <Trash2 className="w-3.5 h-3.5" />
            </button>
          )}
          <button onClick={onClose} aria-label="Close chat" className="p-1.5 rounded-lg hover:bg-neutral-100 dark:hover:bg-neutral-800 text-neutral-400 hover:text-neutral-700 dark:hover:text-neutral-200 transition-colors">
            <X className="w-4 h-4" />
          </button>
        </div>
      </div>

      {/* Selected item chip */}
      {selectedItem && (
        <div className="px-5 py-2.5 border-b border-neutral-100 dark:border-neutral-800 bg-red-50/40 dark:bg-red-950/20 flex items-center gap-2">
          <span className="text-[10px] font-semibold text-red-600 dark:text-red-400 uppercase tracking-wider">Focused:</span>
          <span className="text-xs font-medium text-neutral-700 dark:text-neutral-200 truncate flex-1 bg-white/60 dark:bg-neutral-800/60 px-2 py-0.5 rounded-lg border border-red-100 dark:border-red-900/40 shadow-sm">{selectedItem.label}</span>
          <button onClick={onClearSelection} className="p-0.5 rounded-md hover:bg-red-100 dark:hover:bg-red-950/30 text-neutral-400 hover:text-red-600 dark:hover:text-red-400 transition-colors shrink-0">
            <X className="w-3 h-3" />
          </button>
        </div>
      )}

      {/* Messages */}
      <div className="flex-1 overflow-y-auto p-4 space-y-3">
        {messages.length === 0 && !isStreaming && (
          <div className="py-8 text-center">
            <div className="mx-auto mb-4 flex h-14 w-14 items-center justify-center rounded-2xl bg-red-50 dark:bg-red-950/30">
              <MessageSquare className="w-7 h-7 text-red-300 dark:text-red-500" />
            </div>
            <p className="text-sm font-semibold tracking-tight text-neutral-500 dark:text-neutral-400">Ask me anything about your data</p>
            <p className="text-xs text-neutral-400 dark:text-neutral-500 mt-1">I can see the context of your current step</p>
            <div className="mt-5 space-y-2">
              {starterPrompts.map((prompt) => (
                <button
                  key={prompt}
                  onClick={() => sendMessage(prompt)}
                  className="w-full text-left px-3.5 py-2.5 rounded-xl border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 text-xs text-neutral-600 dark:text-neutral-300 hover:border-red-200 dark:hover:border-red-800 hover:bg-red-50/30 dark:hover:bg-red-950/20 hover:text-red-700 dark:hover:text-red-400 transition-all"
                >
                  {prompt}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg, i) => {
          if (msg.role === "system_note") {
            return (
              <div key={i} className="text-center">
                <span className="text-[10px] font-medium text-neutral-400 dark:text-neutral-500 bg-neutral-100 dark:bg-neutral-800 px-2.5 py-1 rounded-full">{msg.content}</span>
              </div>
            );
          }
          if (msg.role === "user") {
            return (
              <div key={i} className="flex justify-end">
                <div className="max-w-[85%] px-3.5 py-2.5 rounded-2xl rounded-br-md bg-red-600 text-white text-xs leading-relaxed shadow-sm">
                  {msg.content}
                </div>
              </div>
            );
          }
          return (
            <div key={i} className="flex justify-start">
              <div className="max-w-[85%] px-3.5 py-2.5 rounded-2xl rounded-bl-md bg-neutral-100 dark:bg-neutral-800 text-neutral-800 dark:text-neutral-200 text-xs leading-relaxed whitespace-pre-wrap shadow-sm">
                {formatAssistantText(msg.content)}
              </div>
            </div>
          );
        })}

        {isStreaming && streamedText && (
          <div className="flex justify-start">
            <div className="max-w-[85%] px-3.5 py-2.5 rounded-2xl rounded-bl-md bg-neutral-100 dark:bg-neutral-800 text-neutral-800 dark:text-neutral-200 text-xs leading-relaxed whitespace-pre-wrap shadow-sm">
              {formatAssistantText(streamedText)}
              <span className="inline-block w-1.5 h-3.5 bg-neutral-400 dark:bg-neutral-500 rounded-sm ml-0.5 animate-pulse" />
            </div>
          </div>
        )}

        {isStreaming && !streamedText && (
          <div className="flex justify-start">
            <div className="px-3.5 py-2.5 rounded-2xl rounded-bl-md bg-neutral-100 dark:bg-neutral-800 shadow-sm">
              <Loader2 className="w-4 h-4 animate-spin text-neutral-400 dark:text-neutral-500" />
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div className="px-4 py-3 border-t border-neutral-200/80 dark:border-neutral-700/80 bg-white dark:bg-neutral-900 shrink-0">
        <div className="flex items-end gap-2">
          <textarea
            ref={inputRef}
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about your data..."
            aria-label="Chat message input"
            rows={1}
            disabled={isStreaming}
            className="flex-1 resize-none text-xs border border-neutral-200 dark:border-neutral-700 rounded-xl px-3.5 py-2.5 bg-white dark:bg-neutral-800 text-neutral-900 dark:text-neutral-100 focus:outline-none focus:ring-2 focus:ring-red-500 focus:border-transparent disabled:opacity-50 max-h-24 placeholder:text-neutral-400 dark:placeholder:text-neutral-500 transition-shadow"
            style={{ minHeight: "38px" }}
          />
          <motion.button
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
            onClick={() => sendMessage()}
            disabled={isStreaming || !input.trim()}
            className="p-2.5 rounded-xl bg-gradient-to-br from-red-600 to-rose-600 text-white disabled:opacity-30 disabled:cursor-not-allowed transition-all shrink-0 shadow-md shadow-red-200/30 dark:shadow-red-900/30"
          >
            {isStreaming ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
          </motion.button>
        </div>
      </div>
    </motion.div>
  );
}
