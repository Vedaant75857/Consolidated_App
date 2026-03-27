"""Accumulates token usage and cost across multiple API calls."""

from __future__ import annotations

import threading
import time

from .client import get_model

MODEL_RATES: dict[str, tuple[float, float]] = {
    "gpt-4o": (2.50, 10.00),
    "gpt-4o-mini": (0.15, 0.60),
    "@personal-openai/gpt-4o": (2.50, 10.00),
    "@personal-openai/gpt-5.2": (2.50, 10.00),
    "@personal-openai/gpt-5.4": (2.50, 10.00),
}


class CostTracker:
    def __init__(self, model: str | None = None):
        self.model = model or get_model()
        self.prompt_tokens = 0
        self.completion_tokens = 0
        self.calls = 0
        self.errors: list[str] = []
        self.start_time = time.time()
        self._lock = threading.Lock()

    def record(self, response) -> None:
        with self._lock:
            usage = getattr(response, "usage", None)
            if usage:
                self.prompt_tokens += getattr(usage, "prompt_tokens", 0) or 0
                self.completion_tokens += getattr(usage, "completion_tokens", 0) or 0
            self.calls += 1

    def record_error(self, msg: str) -> None:
        with self._lock:
            self.errors.append(str(msg))

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens

    @property
    def cost_usd(self) -> float:
        inp_rate, out_rate = MODEL_RATES.get(self.model, (2.50, 10.00))
        return (self.prompt_tokens * inp_rate + self.completion_tokens * out_rate) / 1_000_000

    @property
    def elapsed_s(self) -> float:
        return time.time() - self.start_time

    def summary(self) -> dict:
        return {
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "api_calls": self.calls,
            "cost_usd": round(self.cost_usd, 6),
            "elapsed_seconds": round(self.elapsed_s, 1),
            "errors": self.errors[:20],
        }
