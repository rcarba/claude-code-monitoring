"""Anthropic API pricing per token (USD).

Loads up-to-date prices from LiteLLM's published JSON on first use, with a
hardcoded fallback so things still work offline.
"""

from __future__ import annotations

import json
import urllib.request

LITELLM_URL = (
    "https://raw.githubusercontent.com/BerriAI/litellm/main/"
    "model_prices_and_context_window.json"
)

# Fallback (USD per token) if remote fetch fails. Verified against ccusage
# / LiteLLM April 2026 prices for Opus 4.6/4.7 (cheaper than legacy Opus).
FALLBACK = {
    "claude-opus-4-7":   {"input": 5e-6,  "output": 25e-6, "cache_5m": 6.25e-6, "cache_1h": 10e-6,  "cache_read": 0.5e-6},
    "claude-opus-4-6":   {"input": 5e-6,  "output": 25e-6, "cache_5m": 6.25e-6, "cache_1h": 10e-6,  "cache_read": 0.5e-6},
    "claude-opus-4-1":   {"input": 15e-6, "output": 75e-6, "cache_5m": 18.75e-6,"cache_1h": 30e-6,  "cache_read": 1.5e-6},
    "claude-opus-4":     {"input": 15e-6, "output": 75e-6, "cache_5m": 18.75e-6,"cache_1h": 30e-6,  "cache_read": 1.5e-6},
    "claude-sonnet-4-6": {"input": 3e-6,  "output": 15e-6, "cache_5m": 3.75e-6, "cache_1h": 6e-6,   "cache_read": 0.3e-6},
    "claude-sonnet-4-5": {"input": 3e-6,  "output": 15e-6, "cache_5m": 3.75e-6, "cache_1h": 6e-6,   "cache_read": 0.3e-6},
    "claude-sonnet-4":   {"input": 3e-6,  "output": 15e-6, "cache_5m": 3.75e-6, "cache_1h": 6e-6,   "cache_read": 0.3e-6},
    "claude-haiku-4-5":  {"input": 1e-6,  "output": 5e-6,  "cache_5m": 1.25e-6, "cache_1h": 2e-6,   "cache_read": 0.1e-6},
    "claude-3-5-sonnet": {"input": 3e-6,  "output": 15e-6, "cache_5m": 3.75e-6, "cache_1h": 6e-6,   "cache_read": 0.3e-6},
    "claude-3-5-haiku":  {"input": 0.8e-6,"output": 4e-6,  "cache_5m": 1e-6,    "cache_1h": 1.6e-6, "cache_read": 0.08e-6},
    "claude-3-haiku":    {"input": 0.25e-6,"output": 1.25e-6,"cache_5m":0.3e-6, "cache_1h": 0.5e-6, "cache_read": 0.03e-6},
}

_PRICES: dict[str, dict[str, float]] = {}


def _load_litellm() -> dict[str, dict[str, float]]:
    try:
        with urllib.request.urlopen(LITELLM_URL, timeout=10) as resp:
            data = json.load(resp)
    except Exception as e:
        print(f"[pricing] LiteLLM fetch failed ({e}); using fallback")
        return dict(FALLBACK)
    out: dict[str, dict[str, float]] = {}
    for name, entry in data.items():
        if not isinstance(entry, dict):
            continue
        if entry.get("litellm_provider") != "anthropic" and "claude" not in name.lower():
            continue
        inp = entry.get("input_cost_per_token")
        out_t = entry.get("output_cost_per_token")
        if inp is None or out_t is None:
            continue
        cw = entry.get("cache_creation_input_token_cost", inp * 1.25)
        cr = entry.get("cache_read_input_token_cost", inp * 0.1)
        out[name] = {
            "input":      float(inp),
            "output":     float(out_t),
            "cache_5m":   float(cw),
            "cache_1h":   float(cw) * 1.6,
            "cache_read": float(cr),
        }
    for k, v in FALLBACK.items():
        out.setdefault(k, v)
    print(f"[pricing] loaded {len(out)} models from LiteLLM")
    return out


def _prices() -> dict[str, dict[str, float]]:
    global _PRICES
    if not _PRICES:
        _PRICES = _load_litellm()
    return _PRICES


def _match(model: str) -> dict[str, float] | None:
    if not model:
        return None
    table = _prices()
    if model in table:
        return table[model]
    candidates = [k for k in table if k in model or model in k]
    if candidates:
        return table[max(candidates, key=len)]
    if "opus" in model:
        return table.get("claude-opus-4-7") or FALLBACK["claude-opus-4-7"]
    if "sonnet" in model:
        return table.get("claude-sonnet-4-6") or FALLBACK["claude-sonnet-4-6"]
    if "haiku" in model:
        return table.get("claude-haiku-4-5") or FALLBACK["claude-haiku-4-5"]
    return None


def cost_usd(model: str, input_tokens: int, output_tokens: int,
             cache_5m: int, cache_1h: int, cache_read: int) -> float:
    p = _match(model)
    if not p:
        return 0.0
    return (
        input_tokens  * p["input"]
        + output_tokens * p["output"]
        + cache_5m      * p["cache_5m"]
        + cache_1h      * p["cache_1h"]
        + cache_read    * p["cache_read"]
    )
