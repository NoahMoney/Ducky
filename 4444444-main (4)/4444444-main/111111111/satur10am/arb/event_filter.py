"""Event-driven Swap filter for Uniswap V2-style pools on BSC.

This component listens for Swap events on a curated set of pools and
surfaces only those pools whose price has moved meaningfully.  It is
intended to replace the brute-force pair-iteration loop with an
on-demand queue of candidate pools that experienced a notable price
change.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Set

from web3 import Web3

SWAP_TOPIC = Web3.keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex()

log = logging.getLogger(__name__)


@dataclass
class CandidatePool:
    """A pool whose recent Swap suggests a potential arbitrage."""

    pair_address: str
    dex: str
    token0: str
    token1: str
    price_change_pct: float
    timestamp: float


@dataclass
class _PairMeta:
    pair_address: str
    dex: str
    token0: str
    token1: str
    decimals0: int
    decimals1: int
    reserve0: int
    reserve1: int
    last_mid: float


class EventDrivenFilter:
    """Listen for Swap events and surface candidate pools."""

    def __init__(
        self,
        *,
        rpc,
        registry,
        dex_configs: Dict[str, Dict[str, object]],
        strategy_cfg: Dict[str, object],
        config: Optional[Dict[str, object]] = None,
    ) -> None:
        self.rpc = rpc
        self.registry = registry
        self.dex_configs = dex_configs
        self.strategy_cfg = strategy_cfg or {}
        self.config = config or {}
        self.enabled = bool(self.config.get("enabled", True))
        self.mode = str(self.config.get("mode", "event")).lower()
        self.min_liq_threshold = float(self.config.get("min_liq_usd_for_subscription", 0))
        self.max_subscriptions = int(self.config.get("max_subscriptions", 500))
        self.min_price_change_pct = float(self.config.get("min_price_change_pct", 0.05))
        self.max_candidates_per_tick = int(self.config.get("max_candidates_per_tick", 100))
        self.dedup_window = float(self.config.get("candidate_dedup_window_ms", 2000)) / 1000.0
        self.queue_limit = int(self.config.get("candidate_queue_limit", 1000))
        self.poll_interval = float(self.config.get("poll_interval_ms", 1000)) / 1000.0

        self.quicknode_ws_url = self.config.get("quicknode_ws_url")
        self.use_quicknode = bool(self.config.get("use_quicknode", True))

        self._queue: Deque[CandidatePool] = deque()
        self._last_enqueued: Dict[str, float] = {}
        self._pair_meta: Dict[str, _PairMeta] = {}
        self._subscriptions: Set[str] = set()
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start background listener if enabled."""

        if not self.enabled:
            log.info("[event-filter] Disabled via config; staying in brute-force mode")
            return

        self._subscriptions = self._select_subscription_pairs()
        if not self._subscriptions:
            log.warning("[event-filter] No pools eligible for subscription; filter inactive")
            return

        log.info(
            "[event-filter] Subscribed to %d pools (min_liq_usd=%.0f, max_subscriptions=%d)",
            len(self._subscriptions),
            self.min_liq_threshold,
            self.max_subscriptions,
        )
        self._stop_event.clear()
        self._task = asyncio.create_task(self._poll_swaps())

    async def stop(self) -> None:
        """Stop background tasks."""

        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    def get_candidates(self, max_items: int | None = None) -> List[CandidatePool]:
        """Return and clear recent candidates."""

        items: List[CandidatePool] = []
        limit = max_items or self.max_candidates_per_tick
        while self._queue and len(items) < limit:
            items.append(self._queue.popleft())
        if items:
            log.info("[event-filter] Tick candidates pulled=%d", len(items))
        return items

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _select_subscription_pairs(self) -> Set[str]:
        pairs: List[_PairMeta] = []
        core_pairs: List[tuple[str, str]] = []
        core_cfg = self.config.get("core_pairs") or []
        for entry in core_cfg:
            if isinstance(entry, (list, tuple)) and len(entry) == 2:
                a, b = entry
                if isinstance(a, str) and isinstance(b, str):
                    core_pairs.append((a.lower(), b.lower()))

        for pair in getattr(self.registry, "pairs", []):
            liq_score = max(int(getattr(pair, "reserve0", 0)), int(getattr(pair, "reserve1", 0)))
            t0 = getattr(pair, "token0", "").lower()
            t1 = getattr(pair, "token1", "").lower()
            if not t0 or not t1:
                continue
            dec0 = self._token_decimals(t0)
            dec1 = self._token_decimals(t1)
            if dec0 is None or dec1 is None:
                continue
            address = getattr(pair, "pair", None) or getattr(pair, "address", None)
            if not isinstance(address, str):
                continue
            mid = self._mid_price(
                getattr(pair, "reserve0", 0),
                getattr(pair, "reserve1", 0),
                dec0,
                dec1,
            )
            meta = _PairMeta(
                pair_address=address.lower(),
                dex=getattr(pair, "dex", ""),
                token0=t0,
                token1=t1,
                decimals0=dec0,
                decimals1=dec1,
                reserve0=int(getattr(pair, "reserve0", 0)),
                reserve1=int(getattr(pair, "reserve1", 0)),
                last_mid=mid,
            )
            pairs.append(meta)
            self._pair_meta[meta.pair_address] = meta

        def _is_core(meta: _PairMeta) -> bool:
            return (meta.token0, meta.token1) in core_pairs or (meta.token1, meta.token0) in core_pairs

        pairs.sort(key=lambda p: max(p.reserve0, p.reserve1), reverse=True)
        selected: List[_PairMeta] = []
        for meta in pairs:
            if len(selected) >= self.max_subscriptions:
                break
            if _is_core(meta):
                selected.append(meta)
                continue
            if self.min_liq_threshold <= 0:
                selected.append(meta)
                continue
            # We do not have USD pricing here; use reserve magnitude as a proxy
            if max(meta.reserve0, meta.reserve1) >= self.min_liq_threshold:
                selected.append(meta)
        return {p.pair_address for p in selected}

    async def _poll_swaps(self) -> None:
        last_block: Optional[int] = None
        while not self._stop_event.is_set():
            try:
                block_hex = await self.rpc.call("eth_blockNumber", [])
                block_num = int(block_hex, 16) if isinstance(block_hex, str) else int(block_hex)
            except Exception as exc:  # pragma: no cover - best-effort safety
                log.debug("[event-filter] Failed to fetch latest block: %s", exc)
                await asyncio.sleep(self.poll_interval)
                continue

            from_block = block_num if last_block is None else last_block + 1
            last_block = block_num
            if not self._subscriptions:
                await asyncio.sleep(self.poll_interval)
                continue

            params = {
                "fromBlock": hex(from_block),
                "toBlock": hex(block_num),
                "topics": [SWAP_TOPIC],
                "address": list(self._subscriptions),
            }
            try:
                logs = await self.rpc.call("eth_getLogs", [params])
            except Exception as exc:  # pragma: no cover - best-effort safety
                log.debug("[event-filter] eth_getLogs failed: %s", exc)
                await asyncio.sleep(self.poll_interval)
                continue

            for log_entry in logs or []:
                try:
                    self._handle_log(log_entry)
                except Exception as exc:  # pragma: no cover - defensive
                    log.debug("[event-filter] Failed to process log: %s", exc)
            await asyncio.sleep(self.poll_interval)

    def _handle_log(self, log_entry: dict) -> None:
        pair_addr = (log_entry.get("address") or "").lower()
        meta = self._pair_meta.get(pair_addr)
        if meta is None:
            return
        amounts = self._decode_swap_amounts(log_entry.get("data", ""))
        if amounts is None:
            return
        amount0_in, amount1_in, amount0_out, amount1_out = amounts
        new_res0 = max(meta.reserve0 + amount0_in - amount0_out, 0)
        new_res1 = max(meta.reserve1 + amount1_in - amount1_out, 0)
        if new_res0 == 0 or new_res1 == 0:
            return
        new_mid = self._mid_price(new_res0, new_res1, meta.decimals0, meta.decimals1)
        if new_mid <= 0 or meta.last_mid <= 0:
            meta.reserve0, meta.reserve1, meta.last_mid = new_res0, new_res1, new_mid
            return
        price_change_pct = abs(new_mid - meta.last_mid) / meta.last_mid * 100
        meta.reserve0, meta.reserve1, meta.last_mid = new_res0, new_res1, new_mid

        if price_change_pct < self.min_price_change_pct:
            log.debug(
                "[event-filter] Swap detected pair=%s dex=%s price_change=%.4f%% -> IGNORED",
                pair_addr,
                meta.dex,
                price_change_pct,
            )
            return
        self._enqueue_candidate(meta, price_change_pct)

    def _enqueue_candidate(self, meta: _PairMeta, price_change_pct: float) -> None:
        now = time.time()
        last = self._last_enqueued.get(meta.pair_address, 0)
        if now - last < self.dedup_window:
            return
        self._last_enqueued[meta.pair_address] = now
        cand = CandidatePool(
            pair_address=meta.pair_address,
            dex=meta.dex,
            token0=meta.token0,
            token1=meta.token1,
            price_change_pct=price_change_pct,
            timestamp=now,
        )
        if len(self._queue) >= self.queue_limit:
            self._queue.popleft()
        self._queue.append(cand)
        log.info(
            "[event-filter] Swap detected pair=%s dex=%s price_change=%.4f%% -> ENQUEUED",
            meta.pair_address,
            meta.dex,
            price_change_pct,
        )

    def _token_decimals(self, addr: str) -> Optional[int]:
        tok = self.registry.get_token(addr) if hasattr(self.registry, "get_token") else None
        if tok is None:
            return None
        dec = getattr(tok, "decimals", None)
        return dec if isinstance(dec, int) else None

    @staticmethod
    def _mid_price(reserve0: int, reserve1: int, dec0: int, dec1: int) -> float:
        if reserve0 <= 0 or reserve1 <= 0:
            return 0.0
        num = reserve1 / float(10 ** dec1)
        den = reserve0 / float(10 ** dec0)
        if den <= 0:
            return 0.0
        return num / den

    @staticmethod
    def _decode_swap_amounts(data: str) -> Optional[tuple[int, int, int, int]]:
        if not isinstance(data, str):
            return None
        payload = data[2:] if data.startswith("0x") else data
        if len(payload) < 64 * 4:
            return None
        try:
            fields = [int(payload[i : i + 64], 16) for i in range(0, 64 * 4, 64)]
            return fields[0], fields[1], fields[2], fields[3]
        except Exception:  # pragma: no cover - defensive parsing
            return None


__all__ = ["CandidatePool", "EventDrivenFilter", "SWAP_TOPIC"]
