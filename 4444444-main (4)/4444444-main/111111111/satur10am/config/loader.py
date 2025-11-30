"""Configuration loader with sane defaults.

The refactor standardises configuration loading across the bot.  YAML is
preferred but JSON is accepted.  Missing keys are filled from sensible
defaults so the bot can boot with minimal setup in dry-run mode.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, Any

try:
    import yaml
except Exception:  # pragma: no cover - fallback for minimal envs
    yaml = None

log = logging.getLogger(__name__)

DEFAULTS: Dict[str, Any] = {
    "chain": "bsc",
    "execution": {"enabled": False, "gas_limit": 250000},
    "strategy": {"min_profit_bps": 5, "data_filter": {"mode": "event"}},
    "event_filter": {
        "enabled": True,
        "mode": "event",
        "use_quicknode": False,
        "quicknode_ws_url": None,
        "min_liq_usd_for_subscription": 100_000,
        "min_price_change_pct": 0.05,
        "max_subscriptions": 500,
        "max_candidates_per_tick": 100,
        "candidate_dedup_window_ms": 2000,
        "candidate_queue_limit": 1000,
        "poll_interval_ms": 1000,
        "core_pairs": [],
    },
    "tokens": {},
    "pairs": [],
}


def _load_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        log.warning("[config] config file not found: %s; using defaults", path)
        return {}
    if path.suffix.lower() in {".yaml", ".yml"}:
        if yaml:
            return yaml.safe_load(path.read_text()) or {}
        log.warning("[config] PyYAML not installed; skipping YAML parse for %s", path)
        return {}
    return json.loads(path.read_text())


def _merge_dict(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge_dict(out[k], v)
        else:
            out[k] = v
    return out


def load_bot_config(path: str | Path) -> Dict[str, Any]:
    """Load configuration file and merge with defaults."""

    raw = _load_file(Path(path)) if path else {}
    return _merge_dict(DEFAULTS, raw)


__all__ = ["load_bot_config", "DEFAULTS"]
