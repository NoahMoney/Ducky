"""Profit simulation utilities.

The functions in this module are intentionally deterministic so they can
be unit-tested and composed by the arbitrage engine.  They implement the
constant-product swap equation with configurable fee handling and simple
cycle profit estimation.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional

from scanner.registry import Pair


@dataclass
class SwapQuote:
    amount_in: float
    amount_out: float
    path: List[str]
    profit_bps: float


FEE_DENOMINATOR = 10_000


def _amount_out(amount_in: float, reserve_in: float, reserve_out: float, fee_bps: int) -> float:
    if amount_in <= 0 or reserve_in <= 0 or reserve_out <= 0:
        return 0.0
    fee_factor = (FEE_DENOMINATOR - fee_bps) / FEE_DENOMINATOR
    amount_in_with_fee = amount_in * fee_factor
    numerator = amount_in_with_fee * reserve_out
    denominator = reserve_in + amount_in_with_fee
    return numerator / denominator if denominator > 0 else 0.0


def simulate_two_pool_cycle(pool_buy: Pair, pool_sell: Pair, amount_in: float) -> SwapQuote:
    """Simulate buying in ``pool_buy`` then selling in ``pool_sell``.

    The function assumes both pools share the same token pair but possibly
    inverted directions.  Profit is computed in basis points relative to
    the starting amount.
    """

    buy_out = _amount_out(amount_in, pool_buy.reserve0, pool_buy.reserve1, pool_buy.fee_bps)
    sell_out = _amount_out(buy_out, pool_sell.reserve1, pool_sell.reserve0, pool_sell.fee_bps)
    profit = sell_out - amount_in
    profit_bps = (profit / amount_in * 10_000) if amount_in > 0 else 0.0
    return SwapQuote(
        amount_in=amount_in,
        amount_out=sell_out,
        path=[pool_buy.dex, pool_sell.dex],
        profit_bps=profit_bps,
    )


def best_cross_dex_cycle(pools: Iterable[Pair], amount_in: float = 1.0) -> Optional[SwapQuote]:
    """Return the best cross-DEX cycle among provided pools."""

    pool_list = list(pools)
    if len(pool_list) < 2:
        return None

    best: Optional[SwapQuote] = None
    for i, buy_pool in enumerate(pool_list):
        for sell_pool in pool_list[i + 1 :]:
            quote_forward = simulate_two_pool_cycle(buy_pool, sell_pool, amount_in)
            quote_reverse = simulate_two_pool_cycle(sell_pool, buy_pool, amount_in)
            for candidate in (quote_forward, quote_reverse):
                if best is None or candidate.profit_bps > best.profit_bps:
                    best = candidate
    return best


__all__ = ["SwapQuote", "best_cross_dex_cycle", "simulate_two_pool_cycle"]
