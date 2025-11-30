"""Dry-run friendly executor placeholder.

Execution remains disabled by default.  This module wraps trade
submission logic behind a clear class interface and provides verbose
logging to highlight when a trade would have been attempted.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict

log = logging.getLogger(__name__)


@dataclass
class ExecutionResult:
    route: list[str]
    success: bool
    status: str
    details: Dict[str, Any]


class TradeExecutor:
    """Execution wrapper that respects the global dry-run toggle."""

    def __init__(self, *, dry_run_default: bool = True):
        self.dry_run_default = dry_run_default

    def execute(self, route: list[str], expected_profit_bps: float) -> ExecutionResult:
        status = "dry_run" if self.dry_run_default else "submitted"
        log.info(
            "[executor] %s route=%s expected_profit_bps=%.2f",
            "Simulating" if self.dry_run_default else "Submitting",
            "->".join(route),
            expected_profit_bps,
        )
        return ExecutionResult(route=route, success=True, status=status, details={"expected_profit_bps": expected_profit_bps})


__all__ = ["TradeExecutor", "ExecutionResult"]
