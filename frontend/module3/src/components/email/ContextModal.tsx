import { useState } from "react";
import { X, Plus, Trash2 } from "lucide-react";
import type { EmailContext, NextStep } from "../../types";

interface Props {
  onSubmit: (context: EmailContext) => void;
  onCancel: () => void;
}

const EMPTY_STEP: NextStep = { action: "", owner: "", timeline: "" };

export default function ContextModal({ onSubmit, onCancel }: Props) {
  const [recipientName, setRecipientName] = useState("");
  const [clientName, setClientName] = useState("");
  const [senderName, setSenderName] = useState("");
  const [senderRole, setSenderRole] = useState("");
  const [scopeNote, setScopeNote] = useState("");
  const [nextSteps, setNextSteps] = useState<NextStep[]>([{ ...EMPTY_STEP }]);

  const handleAddStep = () => setNextSteps([...nextSteps, { ...EMPTY_STEP }]);

  const handleRemoveStep = (idx: number) =>
    setNextSteps(nextSteps.filter((_, i) => i !== idx));

  const handleStepChange = (idx: number, field: keyof NextStep, value: string) =>
    setNextSteps(nextSteps.map((s, i) => (i === idx ? { ...s, [field]: value } : s)));

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSubmit({
      recipient_name: recipientName.trim(),
      client_name: clientName.trim(),
      sender_name: senderName.trim(),
      sender_role: senderRole.trim(),
      scope_note: scopeNote.trim(),
      next_steps: nextSteps.filter((s) => s.action.trim()),
    });
  };

  const inputCls =
    "w-full px-3 py-2 text-sm rounded-lg border border-neutral-200 dark:border-neutral-700 " +
    "bg-white dark:bg-neutral-800 text-neutral-900 dark:text-neutral-100 " +
    "focus:outline-none focus:ring-2 focus:ring-primary/40 placeholder:text-neutral-400";

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm">
      <div className="relative w-full max-w-lg max-h-[90vh] overflow-y-auto rounded-2xl bg-white dark:bg-neutral-900 border border-neutral-200 dark:border-neutral-800 shadow-2xl">
        <div className="sticky top-0 z-10 flex items-center justify-between px-6 py-4 border-b border-neutral-100 dark:border-neutral-800 bg-white dark:bg-neutral-900 rounded-t-2xl">
          <h2 className="text-base font-semibold text-neutral-900 dark:text-neutral-100">
            Email Context
          </h2>
          <button
            onClick={onCancel}
            className="p-1.5 rounded-lg text-neutral-400 hover:text-neutral-600 dark:hover:text-neutral-300 hover:bg-neutral-100 dark:hover:bg-neutral-800 transition-colors"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="p-6 space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-xs font-medium text-neutral-500 dark:text-neutral-400 mb-1">
                Recipient Name
              </label>
              <input
                className={inputCls}
                placeholder="e.g. John"
                value={recipientName}
                onChange={(e) => setRecipientName(e.target.value)}
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-neutral-500 dark:text-neutral-400 mb-1">
                Client Name
              </label>
              <input
                className={inputCls}
                placeholder="e.g. Acme Corp"
                value={clientName}
                onChange={(e) => setClientName(e.target.value)}
              />
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-xs font-medium text-neutral-500 dark:text-neutral-400 mb-1">
                Sender Name
              </label>
              <input
                className={inputCls}
                placeholder="e.g. Jane Smith"
                value={senderName}
                onChange={(e) => setSenderName(e.target.value)}
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-neutral-500 dark:text-neutral-400 mb-1">
                Sender Role
              </label>
              <input
                className={inputCls}
                placeholder="e.g. Senior Consultant, London"
                value={senderRole}
                onChange={(e) => setSenderRole(e.target.value)}
              />
            </div>
          </div>

          <div>
            <label className="block text-xs font-medium text-neutral-500 dark:text-neutral-400 mb-1">
              Scope Note (optional)
            </label>
            <input
              className={inputCls}
              placeholder='e.g. "We will proceed with the indirect spend dataset for this exercise."'
              value={scopeNote}
              onChange={(e) => setScopeNote(e.target.value)}
            />
          </div>

          <div>
            <div className="flex items-center justify-between mb-2">
              <label className="text-xs font-medium text-neutral-500 dark:text-neutral-400">
                Next Steps
              </label>
              <button
                type="button"
                onClick={handleAddStep}
                className="flex items-center gap-1 text-xs text-primary hover:text-primary-hover transition-colors"
              >
                <Plus className="w-3 h-3" /> Add Row
              </button>
            </div>

            <div className="space-y-2">
              {nextSteps.map((step, idx) => (
                <div key={idx} className="flex items-start gap-2">
                  <input
                    className={inputCls + " flex-[3]"}
                    placeholder="Action"
                    value={step.action}
                    onChange={(e) => handleStepChange(idx, "action", e.target.value)}
                  />
                  <input
                    className={inputCls + " flex-[2]"}
                    placeholder="Owner"
                    value={step.owner}
                    onChange={(e) => handleStepChange(idx, "owner", e.target.value)}
                  />
                  <input
                    className={inputCls + " flex-[2]"}
                    placeholder="Timeline"
                    value={step.timeline}
                    onChange={(e) => handleStepChange(idx, "timeline", e.target.value)}
                  />
                  {nextSteps.length > 1 && (
                    <button
                      type="button"
                      onClick={() => handleRemoveStep(idx)}
                      className="mt-2 p-1 text-neutral-400 hover:text-red-500 transition-colors"
                    >
                      <Trash2 className="w-3.5 h-3.5" />
                    </button>
                  )}
                </div>
              ))}
            </div>
          </div>

          <div className="flex justify-end gap-3 pt-2">
            <button
              type="button"
              onClick={onCancel}
              className="px-4 py-2 text-sm font-medium text-neutral-600 dark:text-neutral-400 hover:bg-neutral-100 dark:hover:bg-neutral-800 rounded-lg transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-5 py-2 text-sm font-semibold text-white bg-primary hover:bg-primary-hover rounded-lg transition-colors"
            >
              Generate Email
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
