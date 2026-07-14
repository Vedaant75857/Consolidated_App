"""Concurrent batch AI mapping for normalisation agents."""

from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable

from .client import get_client, get_model
from .cost_tracker import CostTracker


def batch_ai_mapping(
    unique_vals: list[str],
    system_prompt: str,
    user_prompt_template: str,
    api_key: str | None = None,
    batch_size: int = 80,
    max_workers: int = 4,
    progress_cb: Callable[[int, int, float], None] | None = None,
    cost_tracker: CostTracker | None = None,
) -> tuple[dict[str, Any], CostTracker]:
    """Process unique values through AI in concurrent batches.

    user_prompt_template should contain ``{batch}`` which will be replaced
    with the JSON-serialised batch list.
    """
    if cost_tracker is None:
        cost_tracker = CostTracker()
    if not unique_vals:
        return {}, cost_tracker

    client = get_client(api_key)
    model = get_model()
    mapping: dict[str, Any] = {}
    lock = threading.Lock()
    completed = [0]

    batches = [unique_vals[i : i + batch_size] for i in range(0, len(unique_vals), batch_size)]
    total_batches = len(batches)

    retry_attempts = max(1, int(os.getenv("AI_BATCH_RETRY_ATTEMPTS", "2")))
    retry_backoff = max(0.0, float(os.getenv("AI_BATCH_RETRY_BACKOFF_SEC", "0.4")))

    def _process(batch_idx: int, batch: list[str]) -> None:
        prompt = user_prompt_template.replace("{batch}", json.dumps(batch))
        err: Exception | None = None
        for attempt in range(retry_attempts):
            try:
                resp = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": prompt},
                    ],
                    response_format={"type": "json_object"},
                )
                cost_tracker.record(resp)
                parsed = json.loads(resp.choices[0].message.content)
                with lock:
                    mapping.update(parsed)
                err = None
                break
            except Exception as exc:
                err = exc
                if attempt < retry_attempts - 1:
                    time.sleep(retry_backoff * (2**attempt))
        if err is not None:
            cost_tracker.record_error(f"Batch {batch_idx + 1}: {err}")

        with lock:
            completed[0] += 1
            if progress_cb and completed[0] > 0:
                elapsed = cost_tracker.elapsed_s
                eta = (elapsed / completed[0]) * (total_batches - completed[0])
                progress_cb(completed[0], total_batches, round(eta, 1))

    if total_batches == 1:
        _process(0, batches[0])
    else:
        workers = min(max_workers, total_batches)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_process, i, b): i for i, b in enumerate(batches)}
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as exc:
                    cost_tracker.record_error(str(exc))

    return mapping, cost_tracker
