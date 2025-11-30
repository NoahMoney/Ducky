"""Legacy-compatible multi-hop strategy wrapper.

The original project provided a complex multi-hop engine wired to many
external services.  For the refactored, self-contained bot we expose a
compact ``run_strategy`` function that reuses
:class:`core.arbitrage_engine.ArbitrageEngine` so existing callers remain
compatible while benefiting from the cleaner architecture.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from core.arbitrage_engine import ArbitrageEngine
from execution.trade_executor import TradeExecutor
from scanner.event_filter import EventDrivenFilter
from scanner.registry import Registry

log = logging.getLogger(__name__)


def run_strategy(
    chain: str,
    registry: Registry,
    tokens_cfg: Dict[str, Any] | None = None,
    dexes_cfg: Dict[str, Any] | None = None,
    strategy_cfg: Dict[str, Any] | None = None,
    risk_cfg: Dict[str, Any] | None = None,
    risk_limits_obj: Any | None = None,
    monitoring_cfg: Dict[str, Any] | None = None,
    executor: TradeExecutor | None = None,
    risk_manager: Any | None = None,
    rpc: Any | None = None,
    multi_hop_cfg: Dict[str, Any] | None = None,
    universe_tokens: Any | None = None,
    tokens_policy_cfg: Dict[str, Any] | None = None,
    execution_cfg: Dict[str, Any] | None = None,
    block_num: int | None = None,
    *_args,
    event_filter: EventDrivenFilter | None = None,
    **_kwargs,
):
    """Entry point retained for compatibility with the old orchestrator."""

    engine = ArbitrageEngine(
        chain=chain,
        registry=registry,
        executor=executor or TradeExecutor(dry_run_default=True),
        config={"strategy": strategy_cfg or {}, "execution": execution_cfg or {}},
        event_filter=event_filter,
    )
    return engine.tick()


__all__ = ["run_strategy"]
