"""
Helper Functions Module
Utility functions for data cleaning agents
"""

import os
import re
import json
import time
import threading
import pandas as pd
import openai
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from portkey_ai import Portkey as _Portkey
    _HAS_PORTKEY = True
except ImportError:
    _HAS_PORTKEY = False


# ─── AI Client ────────────────────────────────────────────────────────────────

def get_client(api_key=None):
    """
    Get an AI client (Portkey or OpenAI) based on the current app state.

    Resolution order:
      1. If api_key is explicitly provided, detect provider from key prefix.
      2. Otherwise read from app_state (set via .env or user input).
      3. Fall back to environment variables.

    Returns a client with the standard chat.completions.create() interface.
    """
    provider = os.getenv('AI_PROVIDER', 'portkey').lower()

    try:
        from flask_app.state import app_state
        provider = app_state.get('ai_provider', provider)
        if not api_key:
            api_key = app_state.get('openai_api_key')
    except Exception:
        pass

    if not api_key:
        if provider == 'portkey':
            api_key = os.getenv('PORTKEY_API_KEY', '')
        else:
            api_key = os.getenv('OPENAI_API_KEY', '')

    if not api_key:
        raise ValueError("Missing API Key. Set it in .env or enter it in the app.")

    if provider == 'portkey' and _HAS_PORTKEY:
        base_url = os.getenv('PORTKEY_BASE_URL', 'https://portkey.bain.dev/v1')
        try:
            from flask_app.config import Config
            base_url = Config.PORTKEY_BASE_URL
        except Exception:
            pass
        return _Portkey(api_key=api_key, base_url=base_url)

    return openai.OpenAI(api_key=api_key)


def get_model():
    """Get the configured model name."""
    try:
        from flask_app.state import app_state
        return app_state.get('ai_model', '@personal-openai/gpt-5.2')
    except Exception:
        pass
    provider = os.getenv('AI_PROVIDER', 'portkey').lower()
    if provider == 'portkey':
        return os.getenv('PORTKEY_MODEL', '@personal-openai/gpt-5.2')
    return 'gpt-4o'


# ─── Cost Tracker ─────────────────────────────────────────────────────────────

MODEL_RATES = {
    'gpt-4o':                       (2.50, 10.00),
    'gpt-4o-mini':                  (0.15, 0.60),
    '@personal-openai/gpt-4o':      (2.50, 10.00),
    '@personal-openai/gpt-5.2':     (2.50, 10.00),
}


class CostTracker:
    """Accumulates token usage and cost across multiple API calls."""

    def __init__(self, model=None):
        self.model = model or get_model()
        self.prompt_tokens = 0
        self.completion_tokens = 0
        self.calls = 0
        self.errors = []
        self.start_time = time.time()
        self._lock = threading.Lock()

    def record(self, response):
        with self._lock:
            if hasattr(response, 'usage') and response.usage:
                self.prompt_tokens += getattr(response.usage, 'prompt_tokens', 0) or 0
                self.completion_tokens += getattr(response.usage, 'completion_tokens', 0) or 0
            self.calls += 1

    def record_error(self, msg):
        with self._lock:
            self.errors.append(str(msg))

    @property
    def total_tokens(self):
        return self.prompt_tokens + self.completion_tokens

    @property
    def cost_usd(self):
        inp_rate, out_rate = MODEL_RATES.get(self.model, (2.50, 10.00))
        return (self.prompt_tokens * inp_rate + self.completion_tokens * out_rate) / 1_000_000

    @property
    def elapsed_s(self):
        return time.time() - self.start_time

    def summary(self):
        return {
            'prompt_tokens': self.prompt_tokens,
            'completion_tokens': self.completion_tokens,
            'total_tokens': self.total_tokens,
            'api_calls': self.calls,
            'cost_usd': round(self.cost_usd, 6),
            'elapsed_seconds': round(self.elapsed_s, 1),
            'errors': self.errors[:20],
        }


# ─── Concurrent Batch AI Mapping ──────────────────────────────────────────────

def _batch_ai_mapping(unique_vals, system_prompt, user_prompt_template,
                      api_key=None, batch_size=80, max_workers=4,
                      progress_cb=None, cost_tracker=None):
    """
    Process unique values through AI in concurrent batches.

    Args:
        unique_vals: list of string values to map
        system_prompt: system message for AI
        user_prompt_template: prompt with {batch} placeholder
        api_key: optional API key
        batch_size: values per batch
        max_workers: concurrent threads
        progress_cb: callback(completed_batches, total_batches, eta_seconds)
        cost_tracker: CostTracker instance (created if None)

    Returns:
        (mapping_dict, cost_tracker)
    """
    if cost_tracker is None:
        cost_tracker = CostTracker()

    if not unique_vals:
        return {}, cost_tracker

    client = get_client(api_key)
    model = get_model()
    mapping = {}
    _lock = threading.Lock()
    completed = [0]

    batches = [unique_vals[i:i + batch_size] for i in range(0, len(unique_vals), batch_size)]
    total_batches = len(batches)

    def _process_batch(batch_idx, batch):
        prompt = user_prompt_template.replace('{batch}', json.dumps(batch))
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
            with _lock:
                mapping.update(parsed)
        except Exception as e:
            cost_tracker.record_error(f"Batch {batch_idx + 1}: {e}")
            print(f"  [WARN] Batch {batch_idx + 1}/{total_batches} failed: {e}")
        finally:
            with _lock:
                completed[0] += 1
                if progress_cb and completed[0] > 0:
                    elapsed = cost_tracker.elapsed_s
                    eta = (elapsed / completed[0]) * (total_batches - completed[0])
                    progress_cb(completed[0], total_batches, round(eta, 1))

    if total_batches == 1:
        _process_batch(0, batches[0])
    else:
        workers = min(max_workers, total_batches)
        print(f"  Processing {total_batches} batches ({workers} workers, {batch_size}/batch)...")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_process_batch, i, b): i
                for i, b in enumerate(batches)
            }
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    cost_tracker.record_error(str(e))

    return mapping, cost_tracker


# ─── Column Finder ────────────────────────────────────────────────────────────

def _find_column(df, keywords, ai_client=None, model=None, ai_description=None):
    """
    Find a column by keyword matching first, AI fallback second.

    Args:
        df: DataFrame
        keywords: list of keyword strings to match (case-insensitive substring)
        ai_client: optional AI client for fallback
        model: optional model name
        ai_description: what to ask AI to find (e.g. "Supplier Name")

    Returns:
        column name or None
    """
    cols = df.columns.tolist()

    for kw in keywords:
        kw_low = kw.lower()
        for c in cols:
            if kw_low in str(c).lower():
                return c

    if not ai_client or not ai_description:
        return None

    prompt = (
        f"Analyze these column headers: {json.dumps(cols)}\n"
        f"Identify the single column that best represents **{ai_description}**.\n"
        f'Return JSON ONLY: {{ "target_column": "Exact Column Name" }} or {{ "target_column": null }}'
    )
    try:
        resp = ai_client.chat.completions.create(
            model=model or get_model(),
            messages=[
                {"role": "system", "content": "Output JSON only."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        target = json.loads(resp.choices[0].message.content).get("target_column")
        if target and target in df.columns:
            return target
    except Exception as e:
        print(f"  [WARN] AI column detection failed: {e}")

    return None


# ─── DataFrame Utilities ──────────────────────────────────────────────────────

def identify_header_row(df):
    """Identify which row contains the headers."""
    if len(df) == 0:
        return 0
    for i in range(min(10, len(df))):
        try:
            row_values = df.iloc[i].astype(str).tolist()
            non_empty = [
                x.strip() for x in row_values
                if isinstance(x, str) and x.strip() and x.strip().lower() not in ('nan', 'none', '')
            ]
            if len(non_empty) > len(df.columns) * 0.5:
                return i
        except Exception:
            continue
    return 0


def make_unique(columns):
    """Make column names unique."""
    seen = set()
    new_cols = []
    for idx, col in enumerate(columns):
        if col is None or (isinstance(col, float) and pd.isna(col)):
            c = f"Unnamed_{idx}"
        else:
            c = str(col).strip()
            if not c or c.lower() in ['nan', 'none', '']:
                c = f"Unnamed_{idx}"

        original_c = c
        i = 1
        while c in seen:
            c = f"{original_c}_{i}"
            i += 1

        seen.add(c)
        new_cols.append(c)
    return new_cols
