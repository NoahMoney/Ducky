"""Arbitrage engine orchestrator.

The engine consumes candidates from the :class:`scanner.event_filter.EventDrivenFilter`
when available and falls back to brute-force evaluation of all pools in
the registry.  It runs purely in dry-run mode unless the executor is
explicitly configured otherwise.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional

from core.simulation import SwapQuote, best_cross_dex_cycle
from execution.trade_executor import TradeExecutor
from scanner.event_filter import CandidatePool, EventDrivenFilter
from scanner.registry import Pair, Registry

log = logging.getLogger(__name__)


class ArbitrageEngine:
    """Evaluate cross-DEX spreads and (optionally) dispatch execution."""

    def __init__(
        self,
        *,
        chain: str,
        registry: Registry,
        executor: TradeExecutor,
        config: Dict[str, Any],
        event_filter: Optional[EventDrivenFilter] = None,
    ) -> None:
        self.chain = chain
        self.registry = registry
        self.executor = executor
        self.config = config
        self.event_filter = event_filter

        strategy_cfg = config.get("strategy", {}) if isinstance(config, dict) else {}
        self.min_profit_bps = float(strategy_cfg.get("min_profit_bps", 0))
        self.data_mode = str(strategy_cfg.get("data_filter", {}).get("mode", "event")).lower()

    # ------------------------------------------------------------------
    def _iter_candidates(self) -> Iterable[Pair]:
        if self.data_mode == "event" and self.event_filter:
            for candidate in self.event_filter.get_candidates():
                pair = self.registry.get_pair(candidate.pair_address)
                if pair:
                    yield pair
        else:
            for pair in self.registry.pairs:
                yield pair

    # ------------------------------------------------------------------
    def tick(self) -> Dict[str, Any]:
        """Run one evaluation tick and return structured results."""

        opportunities: List[Dict[str, Any]] = []
        for pair in self._iter_candidates():
            pools = self.registry.get_pairs_for_tokens(pair.token0, pair.token1)
            quote = best_cross_dex_cycle(pools, amount_in=1.0)
            if quote is None:
                continue
            if quote.profit_bps < self.min_profit_bps:
                log.debug(
                    "[engine] Reject pair=%s/%s best_profit=%.4f bps",
                    pair.token0,
                    pair.token1,
                    quote.profit_bps,
                )
                continue
            log.info(
                "[BLOCK] %s/%s | Input: %.4f | Output: %.4f | Net: %.4f bps",
                pair.token0,
                pair.token1,
                quote.amount_in,
                quote.amount_out,
                quote.profit_bps,
            )
            result = self.executor.execute(quote.path, quote.profit_bps)
            opportunities.append(
                {
                    "pair": f"{pair.token0}/{pair.token1}",
                    "quote": quote,
                    "execution": result,
                }
            )
        return {"opportunities": opportunities}


__all__ = ["ArbitrageEngine"]
