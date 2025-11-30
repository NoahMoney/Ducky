"""Simple in-memory registry for tokens and liquidity pools.

The registry abstracts storage of tokens/pairs discovered by scanners or
loaded from configuration.  It exposes lookups used by both the
``EventDrivenFilter`` and the arbitrage engine without requiring a
backing database.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence


@dataclass
class Token:
    address: str
    symbol: str
    decimals: int


@dataclass
class Pair:
    pair: str
    dex: str
    token0: str
    token1: str
    reserve0: int
    reserve1: int
    fee_bps: int = 25  # UniswapV2 default 0.25%


@dataclass
class Registry:
    """Minimal registry for pairs and tokens.

    The original project relied on a larger registry with persistence
    and on-chain refresh hooks.  This simplified version keeps the same
    interface surface that the refactored engine expects: iterable
    ``pairs`` and ``get_token`` helpers.
    """

    tokens: Dict[str, Token] = field(default_factory=dict)
    pairs: List[Pair] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Token operations
    # ------------------------------------------------------------------
    def add_token(self, token: Token) -> None:
        self.tokens[token.address.lower()] = token

    def get_token(self, address: str) -> Optional[Token]:
        return self.tokens.get(address.lower())

    # ------------------------------------------------------------------
    # Pair operations
    # ------------------------------------------------------------------
    def add_pair(self, pair: Pair) -> None:
        self.pairs.append(pair)

    def get_pairs_for_tokens(self, token_a: str, token_b: str) -> List[Pair]:
        a, b = token_a.lower(), token_b.lower()
        out: List[Pair] = []
        for pair in self.pairs:
            t0, t1 = pair.token0.lower(), pair.token1.lower()
            if {t0, t1} == {a, b}:
                out.append(pair)
        return out

    def get_pair(self, pair_address: str) -> Optional[Pair]:
        target = pair_address.lower()
        for pair in self.pairs:
            if pair.pair.lower() == target:
                return pair
        return None

    def update_reserves(self, pair_address: str, reserve0: int, reserve1: int) -> None:
        for pair in self.pairs:
            if pair.pair.lower() == pair_address.lower():
                pair.reserve0 = reserve0
                pair.reserve1 = reserve1
                break

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------
    @classmethod
    def from_config(cls, config: Dict[str, object]) -> "Registry":
        registry = cls()
        tokens_cfg = config.get("tokens", {}) if isinstance(config, dict) else {}
        for addr, meta in tokens_cfg.items():
            if not isinstance(meta, dict):
                continue
            symbol = meta.get("symbol") or addr[:6]
            decimals = int(meta.get("decimals", 18))
            registry.add_token(Token(address=addr, symbol=symbol, decimals=decimals))

        pairs_cfg: Sequence[dict] = config.get("pairs", []) if isinstance(config, dict) else []
        for entry in pairs_cfg:
            if not isinstance(entry, dict):
                continue
            try:
                pair = Pair(
                    pair=entry["address"].lower(),
                    dex=entry.get("dex", "unknown"),
                    token0=entry["token0"].lower(),
                    token1=entry["token1"].lower(),
                    reserve0=int(entry.get("reserve0", 0)),
                    reserve1=int(entry.get("reserve1", 0)),
                    fee_bps=int(entry.get("fee_bps", 25)),
                )
            except KeyError:
                continue
            registry.add_pair(pair)
        return registry


__all__ = ["Registry", "Token", "Pair"]
