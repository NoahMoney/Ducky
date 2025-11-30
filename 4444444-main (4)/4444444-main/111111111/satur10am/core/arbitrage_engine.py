"""Arbitrage engine orchestrator.

This module wraps the legacy ``arb.multi_hop_engine`` entrypoint so that
it can be invoked via a clean class-based API. All execution remains
behind the ``TradeExecutor`` dry-run flag and must be explicitly toggled
by configuration.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from arb.multi_hop_engine import run_strategy
from scanner.event_filter import EventDrivenFilter

log = logging.getLogger(__name__)


class ArbitrageEngine:
    """Thin orchestrator around the strategy tick."""

    def __init__(self, *, chain: str, config: Dict[str, Any], event_filter: Optional[EventDrivenFilter] = None):
        self.chain = chain
        self.config = config
        self.event_filter = event_filter

    def tick(self, **kwargs):
        """Run a single strategy tick using the legacy implementation."""

        kwargs.setdefault("event_filter", self.event_filter)
        log.debug("[engine] tick start chain=%s", self.chain)
        return run_strategy(self.chain, **kwargs)


__all__ = ["ArbitrageEngine"]
