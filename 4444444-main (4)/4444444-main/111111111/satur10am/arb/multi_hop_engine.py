"""Multi-hop arbitrage engine for BSC.

This module supports two complementary route discovery mechanisms:

1) Path-based enumeration ("paths" mode) – this is the original
   behaviour which enumerates all token cycles up to a given length
   and all combinations of enabled DEXes.  Each candidate route is
   priced using the constant product formula and filtered on
   liquidity, slippage and risk policies.

2) Bellman–Ford negative cycle detection on a log-price graph
   ("bellman" mode) – this mode constructs a directed graph from the
   same set of pools and tokens as the path enumerator.  Each edge
   weight is the negative logarithm of the price after fees so that
   profitable arbitrage opportunities correspond to negative cycles.
   The classic Bellman–Ford algorithm is used to detect such cycles.

The active behaviour is controlled by ``config.multi_hop.mode``:

    - ``"paths"``   – only use explicit path enumeration (legacy)
    - ``"bellman"`` – only use Bellman–Ford cycles as candidates
    - ``"hybrid"``  – run both mechanisms and deduplicate before ranking

Bellman–Ford uses a graph built from the same filtered pool list and
token universe that the scanner and path-based engine already use.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN, getcontext
from typing import Dict, Iterable, List, Set, Optional, Tuple

from arb.arb_quote_service import get_path_quote
from arb.bellman import build_log_price_graph, find_negative_cycles
from core.models import Pair
from core.rpc_manager import RPCManager
from discovery.registry import Registry
from execution.trade_executor import TradeExecutor, RiskManager
from utils.metrics import auto_metrics
from utils.route_history import RouteHistory
from utils.route_scoring import get_route_score as history_route_score
from utils.route_scoring import suggest_route_notional_usd
from utils.pnl_store import PnLStore
from utils.route_performance import RoutePerformance
from utils.risk_limits import RiskLimits
from utils.risk_metrics import compute_pnl_summary
from utils.telegram_notifier import TelegramNotifier

log = logging.getLogger(__name__)

getcontext().prec = 28


def _to_decimal_safe(value, default: Decimal = Decimal(0), context: str = "") -> Decimal:
    """Safely convert *value* to :class:`Decimal`.

    Avoids ``InvalidOperation`` from malformed inputs (``None``, ``""``,
    non-numeric strings, etc.) by returning *default* instead.  A minimal
    debug log is emitted so problematic inputs can be traced without
    disrupting the tick loop.
    """

    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:  # pragma: no cover - defensive
        log.debug(
            "[decimal-debug] Failed Decimal conversion (%s): value=%r default=%s exc=%s",
            context,
            value,
            default,
            exc,
        )
        return default


def _normalize_gas_price_wei(raw: Decimal) -> Decimal:
    """Best-effort conversion of a gas price into wei."""

    if raw <= 0:
        return Decimal(0)
    # Heuristic: if the value is tiny, assume gwei and upscale
    if raw < Decimal(1_000_000):
        return raw * Decimal(1_000_000_000)
    return raw


def compute_route_profit(
    *,
    amount_in: Decimal,
    amount_out: Decimal,
    base_price_usd: Decimal,
    gas_price: Decimal,
    gas_limit: int,
) -> Dict[str, Decimal]:
    """Return profit metrics for a route using consistent math."""

    amount_in = _to_decimal_safe(amount_in, Decimal(0), "compute_route_profit.amount_in")
    amount_out = _to_decimal_safe(amount_out, Decimal(0), "compute_route_profit.amount_out")
    gas_limit = int(gas_limit)

    base_price_usd = _to_decimal_safe(base_price_usd, Decimal(0), "compute_route_profit.base_price")

    profit_base = amount_out - amount_in
    profit_bps = Decimal(0)
    if amount_in > 0:
        profit_bps = (profit_base / amount_in) * Decimal(10_000)

    gross_usd = profit_base * base_price_usd
    gas_price = _to_decimal_safe(gas_price, Decimal(0), "compute_route_profit.gas_price")
    gas_wei = _normalize_gas_price_wei(gas_price)
    gas_cost_base = gas_wei * Decimal(gas_limit) / (Decimal(10) ** 18)
    gas_usd = gas_cost_base * base_price_usd
    net_usd = gross_usd - gas_usd

    return {
        "profit_bps": profit_bps,
        "gross_usd": gross_usd,
        "gas_usd": gas_usd,
        "net_usd": net_usd,
    }


def _estimate_token_price_usd(
    token_addr: str,
    *,
    base_addr: str,
    base_price_usd: Decimal,
    stable_set: Set[str],
    decimals_by_addr: Dict[str, int],
    reserve0: int,
    reserve1: int,
) -> Optional[Decimal]:
    """Estimate USD price of a token using its pool against a known asset."""

    lo_addr = token_addr.lower()
    if lo_addr in stable_set:
        return Decimal(1)
    if lo_addr == base_addr.lower():
        return base_price_usd

    dec0 = decimals_by_addr.get(lo_addr)
    dec1 = decimals_by_addr.get(base_addr)
    if dec0 is None or dec1 is None or reserve0 <= 0 or reserve1 <= 0:
        return None

    base_amount = Decimal(reserve1) / (Decimal(10) ** dec1)
    token_amount = Decimal(reserve0) / (Decimal(10) ** dec0)
    if token_amount <= 0 or base_amount <= 0:
        return None
    # Price implied by pool ratio against the base asset
    return (base_amount / token_amount) * base_price_usd


def estimate_pool_liquidity_usd(
    pair,
    *,
    base_addr: str,
    base_price_usd: Decimal,
    decimals_by_addr: Dict[str, int],
    stable_addrs: Set[str],
) -> Optional[Decimal]:
    """Approximate pool liquidity in USD using base/stable anchors."""

    t0 = pair.token0.lower()
    t1 = pair.token1.lower()
    dec0 = decimals_by_addr.get(t0)
    dec1 = decimals_by_addr.get(t1)
    if dec0 is None or dec1 is None:
        return None

    price0 = None
    price1 = None
    if t0 in stable_addrs:
        price0 = Decimal(1)
    elif t0 == base_addr.lower():
        price0 = base_price_usd

    if t1 in stable_addrs:
        price1 = Decimal(1)
    elif t1 == base_addr.lower():
        price1 = base_price_usd

    # If one side has a known anchor price, infer the other via the ratio
    if price0 is None and price1 is not None:
        price0 = _estimate_token_price_usd(
            t0,
            base_addr=t1,
            base_price_usd=price1,
            stable_set=stable_addrs,
            decimals_by_addr=decimals_by_addr,
            reserve0=pair.reserve0,
            reserve1=pair.reserve1,
        )
    elif price1 is None and price0 is not None:
        price1 = _estimate_token_price_usd(
            t1,
            base_addr=t0,
            base_price_usd=price0,
            stable_set=stable_addrs,
            decimals_by_addr=decimals_by_addr,
            reserve0=pair.reserve1,
            reserve1=pair.reserve0,
        )

    if price0 is None or price1 is None:
        return None

    amt0 = Decimal(pair.reserve0) / (Decimal(10) ** dec0)
    amt1 = Decimal(pair.reserve1) / (Decimal(10) ** dec1)
    return amt0 * price0 + amt1 * price1


def compute_leg_slippage(leg) -> Decimal:
    """Return slippage fraction for a leg compared to mid-price after fees."""

    if leg.reserve_in <= 0 or leg.reserve_out <= 0 or leg.amount_in <= 0:
        return Decimal(1)
    fee_fraction = Decimal(1) - (Decimal(leg.fee_bps) / Decimal(10_000))
    mid = leg.amount_in * (leg.reserve_out / leg.reserve_in) * fee_fraction
    if mid <= 0:
        return Decimal(1)
    impact = Decimal(1) - (leg.amount_out / mid)
    return max(impact, Decimal(0))


def compute_route_key(cycle_tokens: List[str], dex_path: List[str]) -> str:
    """Return a deterministic string key for a route."""

    token_part = "->".join(cycle_tokens)
    dex_part = ",".join(dex_path)
    return f"{token_part}|{dex_part}"


def get_route_score(
    *,
    chain: str,
    route_key: str,
    history: Optional[RouteHistory],
    max_age_sec: float,
    tau: float,
) -> float:
    if history is None:
        return 0.0
    try:
        return history_route_score(
            chain=chain,
            route_key=route_key,
            history=history,
            max_age_sec=max_age_sec,
            tau=tau,
        )
    except Exception:
        return 0.0


@dataclass
class Edge:
    dex: str
    token_in: str
    token_out: str
    fee_bps: int
    pair_address: str
    reserve_in: int
    reserve_out: int


@dataclass
class ArbGraph:
    nodes: Set[str]
    edges: List[Edge]


def enumerate_cycles(base_token: str, tokens: Iterable[str], max_len: int) -> List[List[str]]:
    """Generate cycles starting and ending with ``base_token``.

    For example, with ``max_len=3`` and tokens = [WBNB, USDT, BUSD] this
    yields:

    * [WBNB, USDT, WBNB]
    * [WBNB, BUSD, WBNB]
    * [WBNB, USDT, BUSD, WBNB]
    * [WBNB, BUSD, USDT, WBNB]

    The base token itself is not included in the intermediate tokens.
    """
    toks = [t for t in tokens if t != base_token]
    cycles: List[List[str]] = []
    # 2-leg cycles
    for t1 in toks:
        cycles.append([base_token, t1, base_token])

    if max_len >= 3:
        seen_tri: Set[frozenset[str]] = set()
        for t1, t2 in itertools.permutations(toks, 2):
            key = frozenset([t1, t2])
            if key in seen_tri:
                continue
            seen_tri.add(key)
            cycles.append([base_token, t1, t2, base_token])

    if max_len >= 4:
        seen_quad: Set[frozenset[str]] = set()
        for t1, t2, t3 in itertools.permutations(toks, 3):
            key = frozenset([t1, t2, t3])
            if key in seen_quad:
                continue
            seen_quad.add(key)
            cycles.append([base_token, t1, t2, t3, base_token])
    return cycles


def enumerate_dex_paths(dexes: List[str], length: int) -> List[List[str]]:
    """Generate all sequences of DEXes for a given path length."""
    return list(itertools.product(dexes, repeat=length))


def build_graph(
    chain: str,
    registry: Registry,
    dex_configs: Dict[str, Dict[str, object]],
    active_dexes: List[str],
    allowed_tokens: Optional[Set[str]] = None,
) -> ArbGraph:
    dex_fees = {dex: int(cfg.get("fee_bps", 0)) for dex, cfg in dex_configs.items()}
    edges: List[Edge] = []
    nodes: Set[str] = set()

    allowed = {t.lower() for t in allowed_tokens} if allowed_tokens else None

    for pair in registry.pairs:
        if pair.chain != chain or pair.dex not in active_dexes:
            continue
        t0 = pair.token0.lower()
        t1 = pair.token1.lower()
        if allowed is not None and (t0 not in allowed or t1 not in allowed):
            continue
        tok0 = registry.get_token(t0)
        tok1 = registry.get_token(t1)
        if tok0 is None or tok1 is None:
            log.debug("[multi_hop:%s] skipping pair missing token meta %s/%s", chain, t0, t1)
            continue
        if tok0.decimals == 0 or tok1.decimals == 0:
            log.debug("[multi_hop:%s] skipping zero-decimal token pair %s/%s", chain, t0, t1)
            continue
        fee_bps = dex_fees.get(pair.dex, 0)
        edges.append(
            Edge(
                dex=pair.dex,
                token_in=t0,
                token_out=t1,
                fee_bps=fee_bps,
                pair_address=pair.pair,
                reserve_in=pair.reserve0,
                reserve_out=pair.reserve1,
            )
        )
        edges.append(
            Edge(
                dex=pair.dex,
                token_in=t1,
                token_out=t0,
                fee_bps=fee_bps,
                pair_address=pair.pair,
                reserve_in=pair.reserve1,
                reserve_out=pair.reserve0,
            )
        )
        nodes.add(t0)
        nodes.add(t1)

    log.debug(
        "[multi_hop:%s] Graph built: %d tokens, %d edges across %d DEXes",
        chain,
        len(nodes),
        len(edges),
        len(set(e.dex for e in edges)),
    )
    return ArbGraph(nodes=nodes, edges=edges)


def _edge_mid_rate(edge: Edge, decimals: Dict[str, int]) -> Optional[Decimal]:
    dec_in = decimals.get(edge.token_in)
    dec_out = decimals.get(edge.token_out)
    if dec_in is None or dec_out is None or dec_in <= 0 or dec_out <= 0:
        return None
    base_in = Decimal(edge.reserve_in) / (Decimal(10) ** dec_in)
    base_out = Decimal(edge.reserve_out) / (Decimal(10) ** dec_out)
    if base_in <= 0 or base_out <= 0:
        return None
    fee_adj = Decimal(1) - (Decimal(edge.fee_bps) / Decimal(10_000))
    return (base_out / base_in) * fee_adj


def log_simple_loops(
    chain: str,
    graph: ArbGraph,
    decimals: Dict[str, int],
    symbol_by_addr: Dict[str, str],
    top_n: int = 3,
) -> None:
    pair_map: Dict[tuple[str, str], List[Edge]] = {}
    for edge in graph.edges:
        key = tuple(sorted((edge.token_in, edge.token_out)))
        pair_map.setdefault(key, []).append(edge)

    loop_edges: List[tuple[Decimal, Edge, Edge]] = []
    for edges in pair_map.values():
        if len(edges) < 2:
            continue
        for a, b in itertools.permutations(edges, 2):
            if a.dex == b.dex:
                continue
            rate_fwd = _edge_mid_rate(a, decimals)
            rate_back = _edge_mid_rate(b, decimals)
            if rate_fwd is None or rate_back is None:
                continue
            loop = rate_fwd * rate_back
            loop_edges.append((loop, a, b))

    for loop, edge_a, edge_b in sorted(loop_edges, key=lambda x: x[0], reverse=True)[:top_n]:
        sym_a = symbol_by_addr.get(edge_a.token_in, edge_a.token_in)
        sym_b = symbol_by_addr.get(edge_a.token_out, edge_a.token_out)
        log.info(
            "[multi_hop:%s] LOOP_CHECK %s->%s %s/%s edge=%.4f",
            chain,
            sym_a,
            sym_b,
            edge_a.dex,
            edge_b.dex,
            float(loop),
        )


def run_strategy(
    chain: str,
    registry: Registry,
    static_tokens_cfg: Dict[str, Dict[str, object]],
    dex_configs: Dict[str, Dict[str, object]],
    strategy_cfg: Dict[str, object],
    risk_cfg: Dict[str, object],
    risk_limits: Optional[RiskLimits],
    monitoring_cfg: Optional[Dict[str, object]],
    executor: TradeExecutor,
    risk_manager: RiskManager,
    rpc: RPCManager,
    multi_hop_cfg: Optional[Dict[str, object]] = None,
    universe_tokens: Optional[List[str]] = None,
    token_policy_cfg: Optional[Dict[str, object]] = None,
    execution_cfg: Optional[Dict[str, object]] = None,
    block_number: Optional[int] = None,
    mempool_watcher=None,
    pnl_store: Optional[PnLStore] = None,
    telegram_notifier: Optional[TelegramNotifier] = None,
    route_performance: Optional[RoutePerformance] = None,
    performance_cfg: Optional[Dict[str, object]] = None,
) -> None:
    """Run a single tick of the multi-hop arbitrage strategy.

    This function builds candidate cycles and DEX combinations, quotes
    each candidate and executes (or dry-runs) trades that meet the
    minimum profit threshold and risk requirements.
    """
    auto_metrics.tick()
    tick_counter = getattr(run_strategy, "_tick_counter", 0) + 1
    run_strategy._tick_counter = tick_counter
    monitoring_cfg = monitoring_cfg or {}
    pnl_summary_interval = int(monitoring_cfg.get("pnl_summary_interval_ticks", 0) or 0)
    candidates_considered = 0
    simulated_count = 0
    mempool_rejected = 0
    perf_pruned = 0
    executed_count = 0
    execution_results = []
    profit_usd_total = Decimal(0)

    base_token = strategy_cfg.get("base_token")
    if not base_token:
        print("[multi_hop] No base_token configured; aborting")
        return

    mh_cfg = multi_hop_cfg or {}
    perf_cfg = performance_cfg or {}
    perf_engine = (
        route_performance if route_performance and perf_cfg.get("enabled", True) else None
    )
    perf_score_min = float(perf_cfg.get("min_route_score_to_simulate", 0.0))

    # ------------------------------------------------------------------
    # Multi-hop mode and Bellman-Ford configuration
    #
    # The ``multi_hop`` section of the config may specify a ``mode``
    # parameter controlling which route discovery mechanism to run.  The
    # supported values are:
    #   - "paths"   : only run the legacy path enumerator
    #   - "bellman" : only run the Bellman–Ford negative cycle detector
    #   - "hybrid"  : run both and merge the results
    # Defaults to "paths" for backwards compatibility.
    mode = str(mh_cfg.get("mode", "paths")).lower()
    run_paths = mode in ("paths", "hybrid", None)
    run_bellman = mode in ("bellman", "hybrid")
    event_listener_enabled = bool(mh_cfg.get("event_listener_enabled", False))

    mempool_cfg = mh_cfg.get("mempool_integration", {}) or {}
    mempool_enabled = bool(mempool_cfg.get("enabled", False))
    mempool_mode = "off"
    if mempool_enabled:
        mempool_mode = str(mempool_cfg.get("mode", "conservative")).lower()
    max_allowed_impact_bps = float(mempool_cfg.get("max_allowed_impact_bps", 100))
    mempool_min_notional_usd = float(mempool_cfg.get("min_notional_usd", 1000))
    mempool_log_risky = bool(mempool_cfg.get("log_risky_pools", True))
    pending_impact_threshold = float(mh_cfg.get("mempool", {}).get("impact_threshold_pct", 0))

    def mempool_should_prune(pair, cycle_tokens: List[str]) -> bool:
        if mempool_watcher is None or mempool_mode == "off":
            return False
        try:
            risk = mempool_watcher.get_pool_risk(chain, pair.dex, pair.pair)
        except Exception as exc:  # pragma: no cover - defensive
            log.debug("[mempool] risk fetch failed for %s/%s: %s", pair.dex, pair.pair, exc)
            return False
        impact_bps = float(risk.get("expected_price_impact_bps", 0) or 0)
        pending_usd = float(risk.get("pending_notional_usd", 0) or 0)
        if mempool_mode == "info":
            if mempool_log_risky and risk.get("has_pending") and impact_bps > 0:
                log.info(
                    "[risk:mempool] route=%s hop=%s/%s impact_bps=%.1f notional=%.2f",
                    "->".join(cycle_tokens),
                    pair.dex,
                    pair.pair,
                    impact_bps,
                    pending_usd,
                )
            return False
        if mempool_mode == "conservative":
            if impact_bps > max_allowed_impact_bps and pending_usd > mempool_min_notional_usd:
                log.info(
                    "[risk] Reject route=%s reason=\"mempool_impact > %.1f bps on %s/%s (impact=%.1f bps, notional=%.2f USD)\"",
                    "->".join(cycle_tokens),
                    max_allowed_impact_bps,
                    pair.dex,
                    pair.pair,
                    impact_bps,
                    pending_usd,
                )
                return True
        return False

    # Graph sizing for Bellman–Ford.  ``graph_max_tokens`` places a
    # hard cap on the number of tokens included in the log-price graph
    # (after anchors).  ``max_cycle_length`` limits the number of hops
    # in a returned cycle, and ``max_bellman_cycles_per_tick`` caps
    # the total number of cycles returned per tick.  ``min_profit_bps``
    # sets a minimum profit threshold for Bellman-derived routes in
    # basis points.
    graph_max_tokens = int(mh_cfg.get("graph_max_tokens", 40))
    max_cycle_length_cfg = int(mh_cfg.get("max_cycle_length", 4))
    max_bellman_cycles_per_tick = int(mh_cfg.get("max_bellman_cycles_per_tick", 5))
    min_profit_bps_bellman = _to_decimal_safe(
        mh_cfg.get("min_profit_bps", 0), Decimal(0), "cfg.min_profit_bps_bellman"
    )
    max_len = int(strategy_cfg.get("max_path_length", 3))
    mh_max_len = int(mh_cfg.get("max_hops", max_len))
    max_len = max(2, min(max_len, mh_max_len, 4))

    exec_cfg = execution_cfg or {}
    sizing_cfg = strategy_cfg.get("sizing", {}) or {}
    token_policy_cfg = token_policy_cfg or {}

    min_profit = _to_decimal_safe(strategy_cfg.get("min_profit_fraction", 0), Decimal(0), "cfg.min_profit")
    min_profit_bps = _to_decimal_safe(strategy_cfg.get("min_profit_bps", 0), Decimal(0), "cfg.min_profit_bps")
    min_profit_usd = _to_decimal_safe(strategy_cfg.get("min_profit_usd", 0), Decimal(0), "cfg.min_profit_usd")
    slippage_per_leg = _to_decimal_safe(
        strategy_cfg.get("per_leg_slippage_fraction", 0), Decimal(0), "cfg.slippage_per_leg"
    )

    target_slippage_bps = _to_decimal_safe(
        sizing_cfg.get("target_slippage_bps", 50), Decimal(50), "cfg.target_slippage_bps"
    )
    min_notional_usd = _to_decimal_safe(
        sizing_cfg.get("min_notional_usd", 50), Decimal(50), "cfg.min_notional_usd"
    )
    max_notional_usd = _to_decimal_safe(
        sizing_cfg.get("max_notional_usd", 500), Decimal(500), "cfg.max_notional_usd"
    )
    debug_standard_amount = bool(sizing_cfg.get("debug_log_standard_amount", True))

    exec_min_profit_bps = _to_decimal_safe(
        exec_cfg.get("min_profit_bps", min_profit_bps), min_profit_bps, "cfg.exec_min_profit_bps"
    )
    exec_min_net_usd = _to_decimal_safe(exec_cfg.get("min_net_usd", 0), Decimal(0), "cfg.exec_min_net_usd")
    max_trades_per_tick = int(exec_cfg.get("max_trades_per_tick", 1))
    max_price_age_sec = float(exec_cfg.get("max_price_age_sec", 5))

    gas_limit = int(exec_cfg.get("gas_limit", strategy_cfg.get("gas_limit", 600_000)))
    gas_price_cfg = exec_cfg.get("gas_price_gwei", exec_cfg.get("gas_price_wei", 0))
    gas_price = _to_decimal_safe(gas_price_cfg, Decimal(0), "cfg.gas_price")
    amount_in = _to_decimal_safe(strategy_cfg.get("amount_in_base_token", 1), Decimal(1), "cfg.amount_in")

    route_history_enabled = bool(mh_cfg.get("route_history_enabled", True))
    route_history_dir = str(mh_cfg.get("route_history_dir", "data"))
    route_history_file = str(mh_cfg.get("route_history_file", "route_history.jsonl"))
    route_history = RouteHistory(route_history_dir, route_history_file) if route_history_enabled else None
    route_score_min = float(mh_cfg.get("route_score_min", -1000.0))
    route_score_soft_min = float(mh_cfg.get("route_score_soft_min", -1000.0))
    route_score_boost = _to_decimal_safe(mh_cfg.get("route_score_boost", 0), Decimal(0), "cfg.route_score_boost")
    route_score_max_age = float(mh_cfg.get("route_score_max_age_sec", 86_400))
    route_score_tau = float(mh_cfg.get("route_score_tau_sec", 14_400))

    adaptive_sizing_enabled = bool(mh_cfg.get("adaptive_sizing_enabled", False))
    adaptive_sizing_lookback_sec = float(mh_cfg.get("adaptive_sizing_lookback_sec", 14_400))
    adaptive_sizing_max_multiplier = float(mh_cfg.get("adaptive_sizing_max_multiplier", 3.0))
    risk_limits_obj = risk_limits if isinstance(risk_limits, RiskLimits) else None

    def _perf_score(route_key: str) -> float:
        if perf_engine is None:
            return 1.0
        try:
            return float(perf_engine.get_score(route_key))
        except Exception:
            return 1.0

    def _perf_banned(route_key: str) -> tuple[bool, Optional[str]]:
        if perf_engine is None:
            return False, None
        try:
            return perf_engine.is_banned(route_key, time.time())
        except Exception:
            return False, None

    def _perf_penalise(route_key: str, reason: str) -> None:
        if perf_engine is None:
            return
        try:
            perf_engine.record_trade(
                {
                    "timestamp": time.time(),
                    "route_key": route_key,
                    "net_usd": -0.01,
                    "status": "sim_failed",
                    "sim_reason": reason,
                }
            )
        except Exception:
            log.debug("[perf] Failed to penalise route %s", route_key)

    # Build token metadata from registry (single source of truth)
    registry_tokens = list(registry.tokens.values())
    base_addr = None
    for t in registry_tokens:
        if t.symbol == base_token or t.address.lower() == base_token.lower():
            base_addr = t.address.lower()
            break
    if base_addr is None:
        print(f"[multi_hop:{chain}] Base token {base_token} missing from registry metadata")
        return

    # Multi-hop graph configuration
    use_universe_tokens = bool(mh_cfg.get("use_universe_tokens", True))
    # Limit the dynamic token universe used for both path enumeration and
    # Bellman–Ford graph construction.  ``graph_max_tokens`` overrides
    # any legacy ``max_tokens`` setting in the config.
    max_graph_tokens = max(1, graph_max_tokens)
    max_paths_per_tick = max(1, int(mh_cfg.get("max_paths_per_tick", 5000)))
    min_universe_tokens = int(mh_cfg.get("min_required_tokens", 0))
    anchor_syms_cfg = mh_cfg.get("forced_anchors", []) or []

    # Resolve anchor symbols to addresses present in the registry
    anchor_syms = [str(sym).upper() for sym in anchor_syms_cfg if isinstance(sym, str)]
    if base_token.upper() not in anchor_syms:
        anchor_syms.append(base_token.upper())

    def _resolve_symbol(sym: str) -> Optional[str]:
        for tok in registry_tokens:
            if tok.symbol.upper() == sym:
                return tok.address.lower()
        return None

    anchor_addrs: List[str] = []
    for sym in anchor_syms:
        addr = _resolve_symbol(sym)
        if addr:
            anchor_addrs.append(addr)

    # Build graph token list from universe (if enabled) plus anchors
    universe_list = universe_tokens or []
    dynamic_candidates: List[str] = []
    seen_dyn = set()
    for addr in universe_list:
        if not isinstance(addr, str):
            continue
        lo = addr.lower()
        if lo.startswith("0x") and len(lo) == 42 and lo not in seen_dyn:
            seen_dyn.add(lo)
            dynamic_candidates.append(lo)

    using_universe = use_universe_tokens and bool(dynamic_candidates)
    if using_universe and min_universe_tokens and len(dynamic_candidates) < min_universe_tokens:
        log.info(
            "[multi_hop:%s] Universe too small for multi-hop graph (size=%d < %d); using core token set only",
            chain,
            len(dynamic_candidates),
            min_universe_tokens,
        )
        using_universe = False

    if using_universe:
        dynamic_candidates = dynamic_candidates[:max_graph_tokens]

    graph_token_addrs: List[str] = []
    seen_graph: Set[str] = set()

    def _add_graph_addr(addr: Optional[str]) -> None:
        if not addr:
            return
        lo = addr.lower()
        if lo in seen_graph:
            return
        if registry.get_token(lo) is None:
            return
        seen_graph.add(lo)
        graph_token_addrs.append(lo)

    for addr in anchor_addrs:
        _add_graph_addr(addr)

    if using_universe:
        for addr in dynamic_candidates:
            _add_graph_addr(addr)

    _add_graph_addr(base_addr)

    anchor_set = set(anchor_addrs)
    anchors_in_graph = len([addr for addr in graph_token_addrs if addr in anchor_set])
    dynamic_in_graph = len([addr for addr in graph_token_addrs if addr not in anchor_set])

    if not graph_token_addrs:
        print(f"[multi_hop:{chain}] No graph tokens available; aborting tick")
        return

    if using_universe:
        log.info(
            "[multi_hop:%s] Building token graph from universe: tokens=%d (anchors=%d, dynamic=%d)",
            chain,
            len(graph_token_addrs),
            anchors_in_graph,
            dynamic_in_graph,
        )
    else:
        log.info(
            "[multi_hop:%s] Building token graph from anchors only: tokens=%d (anchors=%d)",
            chain,
            len(graph_token_addrs),
            anchors_in_graph,
        )

    graph_token_addr_set = set(graph_token_addrs)
    tokens_config = {}
    for tok in registry_tokens:
        lo = tok.address.lower()
        if lo in graph_token_addr_set:
            tokens_config[tok.symbol] = {"address": tok.address, "decimals": tok.decimals}

    token_syms = list(tokens_config.keys())
    if not token_syms:
        print(f"[multi_hop:%chain] No enabled tokens in registry; aborting tick")
        return
    if base_token not in token_syms:
        print(f"[multi_hop] base_token {base_token} not present in registry tokens")
        return

    # DEXes to consider
    dexes = list(dex_configs.keys())
    if not dexes:
        print(f"[multi_hop:{chain}] No enabled DEXes configured; aborting tick")
        return
    active_dexes = [
        dex_name
        for dex_name in dexes
        if registry.dex_pool_count(dex_name, chain) > 0
    ]
    log.info("[multi_hop:%s] Active DEXes with pools: %s", chain, active_dexes)
    if not active_dexes:
        print(f"[multi_hop:{chain}] No DEX pools available in registry; aborting tick")
        return
    log.info(
        "[multi_hop:%s] Strategy universe: %d tokens, %d pools across %d DEXes",
        chain,
        registry.token_count(),
        registry.pair_count(),
        len(active_dexes),
    )
    # Map dex to fee_bps
    dex_fees = {
        dex: int(cfg.get("fee_bps", 0))
        for dex, cfg in dex_configs.items()
        if dex in active_dexes
    }

    # Token lookup helpers
    addr_by_symbol: Dict[str, str] = {}
    decimals_by_symbol: Dict[str, int] = {}
    symbol_by_addr: Dict[str, str] = {}
    decimals_by_addr: Dict[str, int] = {}
    for sym, meta in tokens_config.items():
        addr = meta.get("address")
        dec = meta.get("decimals")
        if isinstance(addr, str) and isinstance(dec, int):
            lower = addr.lower()
            addr_by_symbol[sym] = lower
            decimals_by_symbol[sym] = dec
            symbol_by_addr[lower] = sym
            decimals_by_addr[lower] = dec

    # Build multi-DEX pair map for cross-DEX routing
    pair_dex_map: Dict[tuple[str, str], Set[str]] = {}
    for pair in registry.pairs:
        if pair.chain != chain or pair.dex not in active_dexes:
            continue
        sym0 = symbol_by_addr.get(pair.token0.lower())
        sym1 = symbol_by_addr.get(pair.token1.lower())
        if not sym0 or not sym1:
            continue
        lo, hi = (sym0, sym1) if sym0 <= sym1 else (sym1, sym0)
        key = (lo, hi)
        pair_dex_map.setdefault(key, set()).add(pair.dex)

    multi_dex_pairs = {k: v for k, v in pair_dex_map.items() if len(v) >= 2}
    log.info("[multi_hop:%s] Multi-DEX pairs available: %d", chain, len(multi_dex_pairs))

    graph = build_graph(
        chain, registry, dex_configs, active_dexes, allowed_tokens=graph_token_addr_set
    )
    log.info(
        "[multi_hop:%s] Graph built: %d tokens, %d edges across %d DEXes",
        chain,
        len(graph.nodes),
        len(graph.edges),
        len(set(e.dex for e in graph.edges)),
    )
    log_simple_loops(chain, graph, decimals_by_addr, symbol_by_addr)

    def estimate_base_price_usd() -> Optional[Decimal]:
        base_addr_local = addr_by_symbol.get(base_token)
        base_dec = decimals_by_symbol.get(base_token)
        if not base_addr_local or base_dec is None:
            return None
        best_price: Optional[Decimal] = None
        best_liquidity = 0
        stable_syms_local = ("USDT", "BUSD", "USDC")
        for stable in stable_syms_local:
            stable_addr = addr_by_symbol.get(stable)
            stable_dec = decimals_by_symbol.get(stable)
            if not stable_addr or stable_dec is None:
                continue
            best_pair = None
            best_pair_liq = 0
            best_res0 = None
            best_res1 = None
            for dex_name in active_dexes:
                eff = registry.get_effective_reserves(base_addr_local, stable_addr, dex_name)
                if eff is None:
                    continue
                pair, res0, res1, _ = eff
                if res0 <= 0 or res1 <= 0:
                    continue
                liq = max(res0, res1)
                if liq > best_pair_liq:
                    best_pair = pair
                    best_pair_liq = liq
                    best_res0 = res0
                    best_res1 = res1
            if best_pair is None:
                continue
            if best_pair.token0.lower() == base_addr_local:
                base_reserve = best_res0 or 0
                stable_reserve = best_res1 or 0
                stable_decimals = stable_dec
            else:
                base_reserve = best_res1 or 0
                stable_reserve = best_res0 or 0
                stable_decimals = stable_dec
            base_amt = Decimal(base_reserve) / (Decimal(10) ** base_dec)
            stable_amt = Decimal(stable_reserve) / (Decimal(10) ** stable_decimals)
            if base_amt <= 0:
                continue
            price = stable_amt / base_amt
            if price > 0 and best_pair_liq > best_liquidity:
                best_price = price
                best_liquidity = best_pair_liq
        return best_price

    base_token_price_usd = estimate_base_price_usd() or Decimal(0)

    # ------------------------------------------------------------------
    # Price debug logging
    # ------------------------------------------------------------------
    # Log a tiny set of mid-prices each tick so we can validate that reserves
    # map to sensible prices without spamming the logs.
    interesting_pairs = [("WBNB", "USDT"), ("WBNB", "BUSD"), ("USDT", "BUSD")]
    for sym_a, sym_b in interesting_pairs:
        addr_a = tokens_config.get(sym_a, {}).get("address")
        addr_b = tokens_config.get(sym_b, {}).get("address")
        dec_a = tokens_config.get(sym_a, {}).get("decimals")
        dec_b = tokens_config.get(sym_b, {}).get("decimals")
        if not all(isinstance(v, (str, int)) for v in (addr_a, addr_b, dec_a, dec_b)):
            continue
        for dex_name in active_dexes:
            eff = registry.get_effective_reserves(addr_a, addr_b, dex_name)
            if eff is None:
                continue
            pair, res_a, res_b, _ = eff
            if res_a <= 0 or res_b <= 0:
                continue
            dec_in, dec_out = dec_a, dec_b
            if pair.token0.lower() != addr_a.lower():
                res_a, res_b = res_b, res_a
                dec_in, dec_out = dec_b, dec_a
            num = Decimal(res_b) / (Decimal(10) ** dec_out)
            den = Decimal(res_a) / (Decimal(10) ** dec_in)
            if den <= 0:
                continue
            mid = num / den
            log.info(
                "[multi_hop:%s] PRICE %s %s/%s mid=%.6f",
                chain,
                dex_name,
                sym_a,
                sym_b,
                float(mid),
            )

    # Enumerate cycles and dex paths
    cycles = enumerate_cycles(base_token, token_syms, max_len)
    paths_pruned = 0
    raw_paths = 0
    processed_paths = 0
    cycles_no_loops: List[List[str]] = []
    for cycle_tokens in cycles:
        intermediates = cycle_tokens[1:-1]
        path_length = len(cycle_tokens) - 1
        path_count = len(active_dexes) ** path_length
        if len(set(intermediates)) != len(intermediates):
            paths_pruned += path_count
            continue
        cycles_no_loops.append(cycle_tokens)
        raw_paths += path_count
    cycles = cycles_no_loops
    # If path enumeration is disabled, skip all path evaluation by clearing
    # the list of cycles.  This prevents the heavy nested loops below from
    # running when ``multi_hop.mode`` is set to ``bellman``.
    if not run_paths:
        cycles = []
        raw_paths = 0
        processed_paths = 0

    # Logging
    print(
        f"[multi_hop:{chain}] Tick start — tokens={token_syms}, "
        f"dexes={active_dexes}, max_path={max_len}, candidates={len(cycles)}"
    )

    profitable_routes = 0
    paths_considered = 0
    candidates: List[dict] = []
    best_candidate: Optional[dict] = None
    route_scores_seen: List[float] = []

    min_pool_liquidity_usd = _to_decimal_safe(
        risk_cfg.get("min_pool_liquidity_usd", 0), Decimal(0), "cfg.min_pool_liquidity_usd"
    )
    risk_overrides = risk_cfg.get(chain, {}) if isinstance(risk_cfg.get(chain, {}), dict) else {}
    route_notional_cap_usd = _to_decimal_safe(
        risk_overrides.get("max_notional_per_trade_usd", risk_cfg.get("max_notional_per_trade_usd", max_notional_usd)),
        max_notional_usd,
        "cfg.route_notional_cap_usd",
    )

    stable_syms = {"USDT", "BUSD", "USDC"}
    stable_addrs = {addr_by_symbol[s] for s in stable_syms if addr_by_symbol.get(s)}

    sample_amount_in = _to_decimal_safe(
        strategy_cfg.get("sample_amount_in_base_token", 1), Decimal(1), "cfg.sample_amount_in"
    )
    sample_amount_in = max(sample_amount_in, Decimal("0.00000001"))
    sample_limit = int(strategy_cfg.get("sample_paths", 10))
    sample_limit = max(1, min(sample_limit, 25))
    sample_logged = 0

    blacklist_addrs = {
        tok.lower() for tok in token_policy_cfg.get("blacklist", []) if isinstance(tok, str)
    }
    whitelist_addrs = {
        tok.lower() for tok in token_policy_cfg.get("whitelist", []) if isinstance(tok, str)
    }

    def log_prune(path_tokens: List[str], dex_path: List[str], reason: str) -> None:
        log.debug(
            "[multi_hop:%s] PRUNE path=%s dexes=%s reason=%s",
            chain,
            "->".join(path_tokens),
            ",".join(dex_path),
            reason,
        )

    target_slippage_fraction = target_slippage_bps / Decimal(10_000)

    def within_policies(route_addrs: Set[str]) -> Tuple[bool, str]:
        if blacklist_addrs and route_addrs & blacklist_addrs:
            return False, "token blacklisted"
        if whitelist_addrs:
            allowed = route_addrs.issubset(whitelist_addrs | set(anchor_addrs))
            if not allowed:
                return False, "token not whitelisted"
        return True, "ok"

    # Evaluate each cycle
    stop_processing = False
    for cycle_tokens in cycles:
        if stop_processing:
            break
        path_length = len(cycle_tokens) - 1
        dex_paths = enumerate_dex_paths(active_dexes, path_length)

        for dex_path in dex_paths:
            if processed_paths >= max_paths_per_tick:
                stop_processing = True
                break
            processed_paths += 1
            if len(set(dex_path)) == 1 and len(active_dexes) > 1:
                paths_pruned += 1
                log_prune(cycle_tokens, list(dex_path), "single_dex_path")
                continue

            route_addr_set: Set[str] = set()
            for sym in cycle_tokens:
                addr = addr_by_symbol.get(sym)
                if addr:
                    route_addr_set.add(addr)
            allowed, reason = within_policies(route_addr_set)
            if not allowed:
                paths_pruned += 1
                log.info(
                    "[risk] Reject route=%s reason=\"%s\"",
                    "->".join(cycle_tokens),
                    reason,
                )
                continue

            prune_path = False
            for i in range(path_length):
                sym_in = cycle_tokens[i]
                sym_out = cycle_tokens[i + 1]
                dex = dex_path[i]

                addr_in = addr_by_symbol.get(sym_in)
                addr_out = addr_by_symbol.get(sym_out)
                if addr_in is None or addr_out is None:
                    prune_path = True
                    log_prune(cycle_tokens, list(dex_path), "missing_token_metadata")
                    break

                eff = registry.get_effective_reserves(addr_in, addr_out, dex)
                if eff is None:
                    prune_path = True
                    log_prune(cycle_tokens, list(dex_path), "missing_pool")
                    break
                pair, res0, res1, impact_pct = eff

                if res0 <= 0 or res1 <= 0:
                    prune_path = True
                    log_prune(cycle_tokens, list(dex_path), "zero_reserve")
                    break

                if pending_impact_threshold > 0 and impact_pct >= pending_impact_threshold:
                    log.info(
                        "[mempool] reject hop=%s->%s dex=%s impact=%.2f%% threshold=%.2f%%",
                        sym_in,
                        sym_out,
                        dex,
                        impact_pct,
                        pending_impact_threshold,
                    )
                    prune_path = True
                    break

                if mempool_should_prune(pair, cycle_tokens):
                    prune_path = True
                    break

                if min_pool_liquidity_usd > 0:
                    liq_pair = Pair(
                        chain=pair.chain,
                        dex=pair.dex,
                        pair=pair.pair,
                        token0=pair.token0,
                        token1=pair.token1,
                        reserve0=res0,
                        reserve1=res1,
                        liquidity=max(res0, res1),
                        last_updated=pair.last_updated,
                        last_block=pair.last_block,
                    )
                    liq = estimate_pool_liquidity_usd(
                        liq_pair,
                        base_addr=base_addr,
                        base_price_usd=base_token_price_usd,
                        decimals_by_addr=decimals_by_addr,
                        stable_addrs=stable_addrs,
                    )

                    # NEW: detailed debug log so we can see which hop is failing
                    log.info(
                        "[risk-debug] hop=%s->%s dex=%s pair=%s pool_liq_usd=%s threshold=%.0f base_price_usd=%.4f",
                        sym_in,
                        sym_out,
                        dex,
                        pair.pair,
                        "n/a" if liq is None else f"{float(liq):.2f}",
                        float(min_pool_liquidity_usd),
                        float(base_token_price_usd),
                    )

                    if liq is None or liq < min_pool_liquidity_usd:
                        prune_path = True
                        log.info(
                            "[risk] Reject route=%s reason=\"illiquid hop (<%.0f USD)\"",
                            "->".join(cycle_tokens),
                            float(min_pool_liquidity_usd),
                        )
                        break

            if prune_path:
                paths_pruned += 1
                continue

            if registry.has_pending_swap_on_route(
                [addr_by_symbol.get(sym) for sym in cycle_tokens], list(dex_path)
            ):
                log.info("[mempool] reject route: pending swap impact")
                paths_pruned += 1
                continue

            has_cross_dex_leg = False
            for i in range(path_length):
                key = tuple(sorted((cycle_tokens[i], cycle_tokens[i + 1])))
                if key in multi_dex_pairs:
                    has_cross_dex_leg = True
                    break

            if not has_cross_dex_leg:
                paths_pruned += 1
                log_prune(cycle_tokens, list(dex_path), "no_cross_dex_leg")
                continue

            route_key = compute_route_key(cycle_tokens, list(dex_path))
            route_score = get_route_score(
                chain=chain,
                route_key=route_key,
                history=route_history,
                max_age_sec=route_score_max_age,
                tau=route_score_tau,
            )
            route_scores_seen.append(route_score)
            banned, ban_reason = _perf_banned(route_key)
            if banned:
                log.info(
                    "[perf] Skipping banned route=%s (%s)",
                    route_key,
                    ban_reason or "banned",
                )
                auto_metrics.inc("routes_banned")
                continue
            perf_score = _perf_score(route_key)
            if perf_score < perf_score_min:
                log.info(
                    "[multi_hop:%s] [perf-score] Skip route=%s score=%.2f (min=%.2f)",
                    chain,
                    "->".join(cycle_tokens),
                    perf_score,
                    perf_score_min,
                )
                continue
            if route_score < route_score_min:
                log.info(
                    "[multi_hop:%s] [route-score] Skip route=%s score=%.2f (min=%.2f)",
                    chain,
                    "->".join(cycle_tokens),
                    route_score,
                    route_score_min,
                )
                continue

            profit_fraction_threshold = max(min_profit, min_profit_bps / Decimal(10_000))

            # Determine dynamic amount respecting slippage and notional caps
            route_min_usd = min_notional_usd
            route_max_usd = max_notional_usd
            if adaptive_sizing_enabled and route_history is not None:
                adj_min, adj_max = suggest_route_notional_usd(
                    chain,
                    route_key,
                    route_history,
                    min_notional_usd,
                    max_notional_usd,
                    adaptive_sizing_lookback_sec,
                    adaptive_sizing_max_multiplier,
                )
                if (adj_min != route_min_usd) or (adj_max != route_max_usd):
                    log.info(
                        "[multi_hop:%s] [sizing] Route=%s adjusted notional bounds from %.0f–%.0f to %.0f–%.0f USD",
                        chain,
                        "->".join(cycle_tokens),
                        float(route_min_usd),
                        float(route_max_usd),
                        float(adj_min),
                        float(adj_max),
                    )
                route_min_usd, route_max_usd = adj_min, adj_max

            if base_token_price_usd > 0:
                upper_amount = route_max_usd / base_token_price_usd
                lower_amount = route_min_usd / base_token_price_usd
                cap_amount = route_notional_cap_usd / base_token_price_usd
                upper_amount = min(upper_amount, cap_amount)
            else:
                upper_amount = amount_in
                lower_amount = amount_in
            min_amount = max(lower_amount, Decimal("0.00000001"))
            upper_amount = max(min_amount, upper_amount)
            if perf_engine is not None:
                adjusted_amt = _to_decimal_safe(
                    perf_engine.adjust_size(float(upper_amount), route_key),
                    upper_amount,
                    "perf.adjust_size_path",
                )
                upper_amount = max(min_amount, adjusted_amt)

            def quote_for_amount(amount: Decimal) -> Optional[object]:
                q = get_path_quote(
                    chain=chain,
                    registry=registry,
                    tokens_config=tokens_config,
                    dex_fees=dex_fees,
                    path_tokens=cycle_tokens,
                    dex_path=list(dex_path),
                    amount_in=amount,
                    slippage_per_leg=slippage_per_leg,
                )
                if q is None:
                    return None
                for leg in q.legs:
                    if compute_leg_slippage(leg) > target_slippage_fraction:
                        return None
                return q

            best_quote = None
            amt_try = upper_amount
            while amt_try >= min_amount:
                q = quote_for_amount(amt_try)
                if q:
                    best_quote = q
                    break
                amt_try = amt_try / Decimal(2)

            if best_quote is None:
                log_prune(cycle_tokens, list(dex_path), "slippage_or_quote_fail")
                _perf_penalise(route_key, "slippage")
                continue

            paths_considered += 1

            if debug_standard_amount and sample_logged < sample_limit:
                sample_quote = quote_for_amount(sample_amount_in)
                if sample_quote:
                    pd = compute_route_profit(
                        amount_in=sample_quote.amount_in,
                        amount_out=sample_quote.amount_out,
                        base_price_usd=base_token_price_usd,
                        gas_price=gas_price,
                        gas_limit=gas_limit,
                    )
                    log.info(
                        "[multi_hop:%s] PATH route=%s dexes=%s amount_in=%.6f %s amount_out=%.6f %s profit_bps=%.3f gross_usd=%.3f gas_usd=%.3f net_usd=%.3f",
                        chain,
                        "->".join(cycle_tokens),
                        ",".join(dex_path),
                        float(sample_quote.amount_in),
                        base_token,
                        float(sample_quote.amount_out),
                        base_token,
                        float(pd["profit_bps"]),
                        float(pd["gross_usd"]),
                        float(pd["gas_usd"]),
                        float(pd["net_usd"]),
                    )
                    sample_logged += 1

            pd_best = compute_route_profit(
                amount_in=best_quote.amount_in,
                amount_out=best_quote.amount_out,
                base_price_usd=base_token_price_usd,
                gas_price=gas_price,
                gas_limit=gas_limit,
            )

            if best_candidate is None or pd_best["profit_bps"] > best_candidate["profit_bps"]:
                best_candidate = {
                    "quote": best_quote,
                    "cycle_tokens": cycle_tokens,
                    "dex_path": list(dex_path),
                    "profit_bps": pd_best["profit_bps"],
                    "gross_usd": pd_best["gross_usd"],
                    "gas_usd": pd_best["gas_usd"],
                    "net_usd": pd_best["net_usd"],
                }

            log.info(
                "[multi_hop:%s] PATH route=%s dexes=%s amount_in=%.6f amount_out=%.6f profit_bps=%.3f gross_usd=%.3f gas_usd=%.3f net_usd=%.3f",
                chain,
                "->".join(cycle_tokens),
                ",".join(dex_path),
                float(best_quote.amount_in),
                float(best_quote.amount_out),
                float(pd_best["profit_bps"]),
                float(pd_best["gross_usd"]),
                float(pd_best["gas_usd"]),
                float(pd_best["net_usd"]),
            )

            if best_quote.spread_fraction < profit_fraction_threshold:
                continue
            if pd_best["profit_bps"] < exec_min_profit_bps:
                continue
            if pd_best["net_usd"] < exec_min_net_usd or pd_best["net_usd"] < min_profit_usd:
                continue
            if pd_best["net_usd"] <= 0:
                continue

            allowed_rm, reason_rm = risk_manager.evaluate(
                amount_in_base=best_quote.amount_in,
                base_price_usd=base_token_price_usd,
                expected_profit_usd=pd_best["net_usd"],
                block_number=block_number,
            )
            log.info("%s route=%s dexes=%s", reason_rm, "->".join(cycle_tokens), ",".join(dex_path))
            if not allowed_rm:
                continue

            candidates.append(
                {
                    "quote": best_quote,
                    "cycle_tokens": cycle_tokens,
                    "dex_path": list(dex_path),
                    "profit": pd_best,
                    "route_key": route_key,
                    "perf_score": perf_score,
                }
            )

    if stop_processing:
        log.info(
            "[multi_hop:%s] Max paths per tick reached (%d); remaining paths skipped",
            chain,
            max_paths_per_tick,
        )

    # ------------------------------------------------------------------
    # Bellman-Ford negative cycle detection on the log-price graph
    #
    # When the multi-hop mode includes "bellman" we construct a
    # weighted graph from the same universe of tokens and liquidity
    # pools used by the path enumerator.  Each edge weight is the
    # negative logarithm of the price after fee (out_per_1_in).  We
    # then run Bellman–Ford from each configured base token to detect
    # cycles with negative total weight.  Each such cycle corresponds
    # to a potential arbitrage opportunity.
    #
    # Detected cycles are converted into route objects using the
    # existing quoting and risk evaluation code.  Candidates are
    # deduplicated against any previously collected path candidates.
    if run_bellman:
        # Initialise cycle evaluation counter
        cycles_eval = 0
        # Limit the token list to the configured graph_max_tokens
        bellman_tokens = graph_token_addrs[:graph_max_tokens]
        try:
            bellman_graph = build_log_price_graph(
                registry=registry,
                chain=chain,
                tokens=bellman_tokens,
                active_dexes=active_dexes,
                dex_fees=dex_fees,
                decimals_by_addr=decimals_by_addr,
                base_addr=base_addr,
                base_price_usd=base_token_price_usd,
                stable_addrs=stable_addrs,
                min_pool_liquidity_usd=min_pool_liquidity_usd,
            )
            log.info(
                "[multi_hop:%s] Bellman-Ford graph stats: nodes=%d, edges=%d, graph_tokens=%d",
                chain,
                len(bellman_graph.nodes),
                len(bellman_graph.edges),
                len(bellman_tokens),
            )
            raw_cycles = find_negative_cycles(
                graph=bellman_graph,
                base_tokens=anchor_addrs,
                max_cycle_length=max_cycle_length_cfg,
                max_cycles=max_bellman_cycles_per_tick,
            )
            log.info(
                "[multi_hop:%s] Bellman-Ford found %d negative cycles (raw) for bases=%s",
                chain,
                len(raw_cycles),
                ",".join([symbol_by_addr.get(a, a) for a in anchor_addrs]),
            )
        except Exception as exc:
            log.warning("[multi_hop:%s] Bellman-Ford detection failed: %s", chain, exc)
            raw_cycles = []
        # Build a set of existing candidate keys to avoid duplicates
        seen_route_keys = set(
            compute_route_key(c.get("cycle_tokens", []), c.get("dex_path", [])) for c in candidates
        )
        # Process each negative cycle
        for tokens_closed, edges_seq in raw_cycles:
            # Convert token addresses to symbols; skip if any token is unknown
            cycle_syms: List[str] = []
            missing = False
            for addr in tokens_closed:
                sym = symbol_by_addr.get(addr)
                if not sym:
                    missing = True
                    break
                cycle_syms.append(sym)
            if missing:
                continue
            # path_tokens should include the start token again at the end (consistent with path enumerator)
            path_tokens = cycle_syms
            # Minimum cycle length of 2 (i.e. at least one hop) implies at least 3 symbols including closure
            if len(path_tokens) < 3:
                continue
            # Build dex path from edges
            dex_path = [edge.dex for edge in edges_seq]
            # Sanity check: path length matches dex path
            if len(dex_path) != len(path_tokens) - 1:
                continue
            # Filter out cycles with repeated intermediate tokens (loops) similar to path enumerator
            intermediates = path_tokens[1:-1]
            if len(set(intermediates)) != len(intermediates):
                continue
            # Token policies: black/whitelist
            route_addr_set = {addr_by_symbol.get(sym) for sym in set(path_tokens)}
            allowed, reason = within_policies(route_addr_set)
            if not allowed:
                log.info(
                    "[risk] Reject cycle=%s reason=\"%s\"",
                    "->".join(path_tokens),
                    reason,
                )
                continue
            # Cross-DEX requirement: at least one pair exists on multiple DEXes
            has_cross_dex_leg = False
            for i in range(len(path_tokens) - 1):
                key = tuple(sorted((path_tokens[i], path_tokens[i + 1])))
                if key in multi_dex_pairs:
                    has_cross_dex_leg = True
                    break
            if not has_cross_dex_leg:
                continue
            route_key_str = compute_route_key(path_tokens, dex_path)
            route_score = get_route_score(
                chain=chain,
                route_key=route_key_str,
                history=route_history,
                max_age_sec=route_score_max_age,
                tau=route_score_tau,
            )
            route_scores_seen.append(route_score)
            banned, ban_reason = _perf_banned(route_key_str)
            if banned:
                log.info(
                    "[perf] Skipping banned route=%s (%s)",
                    route_key_str,
                    ban_reason or "banned",
                )
                auto_metrics.inc("routes_banned")
                continue
            perf_score = _perf_score(route_key_str)
            if perf_score < perf_score_min:
                log.info(
                    "[multi_hop:%s] [perf-score] Skip route=%s score=%.2f (min=%.2f)",
                    chain,
                    "->".join(path_tokens),
                    perf_score,
                    perf_score_min,
                )
                continue
            if route_score < route_score_min:
                log.info(
                    "[multi_hop:%s] [route-score] Skip route=%s score=%.2f (min=%.2f)",
                    chain,
                    "->".join(path_tokens),
                    route_score,
                    route_score_min,
                )
                continue
            # Determine dynamic amount based on notional limits and slippage
            route_min_usd = min_notional_usd
            route_max_usd = max_notional_usd
            if adaptive_sizing_enabled and route_history is not None:
                adj_min, adj_max = suggest_route_notional_usd(
                    chain,
                    route_key_str,
                    route_history,
                    min_notional_usd,
                    max_notional_usd,
                    adaptive_sizing_lookback_sec,
                    adaptive_sizing_max_multiplier,
                )
                if (adj_min != route_min_usd) or (adj_max != route_max_usd):
                    log.info(
                        "[multi_hop:%s] [sizing] Route=%s adjusted notional bounds from %.0f–%.0f to %.0f–%.0f USD",
                        chain,
                        "->".join(path_tokens),
                        float(route_min_usd),
                        float(route_max_usd),
                        float(adj_min),
                        float(adj_max),
                    )
                route_min_usd, route_max_usd = adj_min, adj_max
            if base_token_price_usd > 0:
                upper_amount = route_max_usd / base_token_price_usd
                lower_amount = route_min_usd / base_token_price_usd
                cap_amount = route_notional_cap_usd / base_token_price_usd
                upper_amount = min(upper_amount, cap_amount)
            else:
                upper_amount = amount_in
                lower_amount = amount_in
            min_amount = max(lower_amount, Decimal("0.00000001"))
            upper_amount = max(min_amount, upper_amount)
            if perf_engine is not None:
                adjusted_amt = _to_decimal_safe(
                    perf_engine.adjust_size(float(upper_amount), route_key_str),
                    upper_amount,
                    "perf.adjust_size_cycle",
                )
                upper_amount = max(min_amount, adjusted_amt)
            # Quote helper for the cycle
            def quote_for_amount_cycle(amount: Decimal) -> Optional[object]:
                q = get_path_quote(
                    chain=chain,
                    registry=registry,
                    tokens_config=tokens_config,
                    dex_fees=dex_fees,
                    path_tokens=path_tokens,
                    dex_path=list(dex_path),
                    amount_in=amount,
                    slippage_per_leg=slippage_per_leg,
                )
                if q is None:
                    return None
                for leg in q.legs:
                    if compute_leg_slippage(leg) > target_slippage_fraction:
                        return None
                return q
            best_quote = None
            amt_try = upper_amount
            while amt_try >= min_amount:
                q = quote_for_amount_cycle(amt_try)
                if q:
                    best_quote = q
                    break
                amt_try = amt_try / Decimal(2)
            if best_quote is None:
                _perf_penalise(route_key_str, "slippage")
                continue
            pd_best = compute_route_profit(
                amount_in=best_quote.amount_in,
                amount_out=best_quote.amount_out,
                base_price_usd=base_token_price_usd,
                gas_price=gas_price,
                gas_limit=gas_limit,
            )
            # Optionally log sample quote using the debug standard amount
            if debug_standard_amount and sample_logged < sample_limit:
                sample_quote = quote_for_amount_cycle(sample_amount_in)
                if sample_quote:
                    pd = compute_route_profit(
                        amount_in=sample_quote.amount_in,
                        amount_out=sample_quote.amount_out,
                        base_price_usd=base_token_price_usd,
                        gas_price=gas_price,
                        gas_limit=gas_limit,
                    )
                    log.info(
                        "[multi_hop:%s] CYCLE route=%s dexes=%s amount_in=%.6f %s amount_out=%.6f %s profit_bps=%.3f gross_usd=%.3f gas_usd=%.3f net_usd=%.3f",
                        chain,
                        "->".join(path_tokens),
                        ",".join(dex_path),
                        float(sample_quote.amount_in),
                        base_token,
                        float(sample_quote.amount_out),
                        base_token,
                        float(pd["profit_bps"]),
                        float(pd["gross_usd"]),
                        float(pd["gas_usd"]),
                        float(pd["net_usd"]),
                    )
                    sample_logged += 1
            # Skip cycles below the Bellman-specific profit threshold
            if pd_best["profit_bps"] < min_profit_bps_bellman:
                continue
            # Update overall best candidate based on profit
            if best_candidate is None or pd_best["profit_bps"] > best_candidate.get("profit_bps", Decimal(0)):
                best_candidate = {
                    "quote": best_quote,
                    "cycle_tokens": path_tokens,
                    "dex_path": list(dex_path),
                    "profit_bps": pd_best["profit_bps"],
                    "gross_usd": pd_best["gross_usd"],
                    "gas_usd": pd_best["gas_usd"],
                    "net_usd": pd_best["net_usd"],
                }
            # Apply global execution thresholds
            if pd_best["profit_bps"] < exec_min_profit_bps:
                continue
            if pd_best["net_usd"] < exec_min_net_usd or pd_best["net_usd"] < min_profit_usd:
                continue
            if pd_best["net_usd"] <= 0:
                continue
            # Risk manager evaluation
            allowed_rm, reason_rm = risk_manager.evaluate(
                amount_in_base=best_quote.amount_in,
                base_price_usd=base_token_price_usd,
                expected_profit_usd=pd_best["net_usd"],
                block_number=block_number,
            )
            log.info("%s cycle=%s dexes=%s", reason_rm, "->".join(path_tokens), ",".join(dex_path))
            if not allowed_rm:
                continue
            route_key = route_key_str
            if route_key in seen_route_keys:
                continue
            seen_route_keys.add(route_key)
            candidates.append(
                {
                    "quote": best_quote,
                    "cycle_tokens": path_tokens,
                    "dex_path": list(dex_path),
                    "profit": pd_best,
                    "route_key": route_key,
                    "perf_score": perf_score,
                }
            )
            cycles_eval += 1

    # ------------------------------------------------------------------
    # End of Bellman-Ford integration
    # ------------------------------------------------------------------

    # Sort all collected candidates (paths and cycles) by descending net USD
    candidates = sorted(
        candidates,
        key=lambda c: (c["profit"]["net_usd"], c.get("perf_score", 1.0)),
        reverse=True,
    )
    candidates_considered = len(candidates)

    # Prefilter candidates using mempool / performance layers
    filtered_candidates = []
    mempool_rejected = 0
    perf_pruned = 0
    for cand in candidates:
        route_tokens = cand.get("cycle_tokens") or []
        route_dexes = cand.get("dex_path") or []
        route_key = cand.get("route_key") or compute_route_key(route_tokens, route_dexes)
        if perf_engine is not None:
            try:
                if perf_engine.is_banned(route_key):
                    auto_metrics.inc("routes_banned")
                    perf_pruned += 1
                    continue
            except Exception:
                pass
        if registry is not None:
            try:
                if registry.has_pending_swap_on_route(route_tokens, route_dexes):
                    mempool_rejected += 1
                    continue
                if hasattr(registry, "is_route_threatened_by_mempool") and registry.is_route_threatened_by_mempool((route_tokens, route_dexes)):
                    mempool_rejected += 1
                    continue
            except Exception:
                pass
        size_mult = 1.0
        if perf_engine is not None:
            try:
                size_mult = float(perf_engine.get_size_multiplier(route_key))
            except Exception:
                size_mult = 1.0
        if size_mult <= 0:
            perf_pruned += 1
            continue
        scaled_profit = {k: v * Decimal(size_mult) for k, v in cand.get("profit", {}).items()}
        scaled_quote = cand.get("quote")
        if scaled_quote is not None and hasattr(scaled_quote, "amount_in"):
            try:
                scaled_quote.amount_in *= Decimal(size_mult)
                scaled_quote.amount_out *= Decimal(size_mult)
            except Exception:
                pass
        cand["profit"] = scaled_profit
        cand["size_multiplier"] = size_mult
        filtered_candidates.append(cand)

    candidates = filtered_candidates
    profitable_routes = len(candidates)

    if candidates:
        to_execute = candidates[:max_trades_per_tick]
    else:
        to_execute = []

    now_ts = time.time()
    log.info(
        "[multi_hop:%s] Tick summary: mode=%s, paths_eval=%d, cycles_eval=%d, "
        "profitable_routes=%d, executed=%d (top net_usd=%s)",
        chain,
        mode,
        paths_considered,
        cycles_eval if 'cycles_eval' in locals() else 0,
        profitable_routes,
        len(to_execute),
        f"{float(candidates[0]['profit']['net_usd']):+.2f}" if candidates else "n/a",
    )

    now_ts = time.time()
    for cand in to_execute:
        quote = cand["quote"]
        profit = cand["profit"]
        cycle_tokens = cand["cycle_tokens"]
        dex_path = cand["dex_path"]
        route_key = cand.get("route_key") or compute_route_key(cycle_tokens, dex_path)

        banned, ban_reason = _perf_banned(route_key)
        if banned:
            log.info("[perf] Skip banned execution route=%s (%s)", route_key, ban_reason)
            auto_metrics.inc("routes_banned")
            continue

        refreshed_profit = profit
        refreshed_quote = quote
        if event_listener_enabled:
            stale = any(leg.pair_last_updated <= 0 for leg in quote.legs)
        else:
            stale = any(now_ts - leg.pair_last_updated > max_price_age_sec for leg in quote.legs)
        if stale:
            dex_filter = {leg.dex for leg in quote.legs}
            try:
                asyncio.run(
                    registry.refresh_reserves(
                        rpc,
                        dex_filter=set(dex_filter),
                        batch_size=10,
                        sleep=0.0,
                    )
                )
            except Exception as exc:
                log.warning("[risk] Reserve refresh failed before execution: %s", exc)

            refreshed_quote = get_path_quote(
                chain=chain,
                registry=registry,
                tokens_config=tokens_config,
                dex_fees=dex_fees,
                path_tokens=cycle_tokens,
                dex_path=list(dex_path),
                amount_in=quote.amount_in,
                slippage_per_leg=slippage_per_leg,
            )
            if refreshed_quote:
                refreshed_profit = compute_route_profit(
                    amount_in=refreshed_quote.amount_in,
                    amount_out=refreshed_quote.amount_out,
                    base_price_usd=base_token_price_usd,
                    gas_price=gas_price,
                    gas_limit=gas_limit,
                )
            if (
                refreshed_quote is None
                or refreshed_profit["profit_bps"] < exec_min_profit_bps
                or refreshed_profit["net_usd"] < exec_min_net_usd
            ):
                log.info(
                    "[risk] Reject before send: profit vanished after re-quote (old=%.3f, new=%.3f)",
                    float(profit["net_usd"]),
                    float(refreshed_profit["net_usd"] if refreshed_quote else Decimal(0)),
                )
                _perf_penalise(route_key, "requote_fail")
                auto_metrics.inc("simulations_failed")
                continue

        slip_fraction = executor.slippage_bps / Decimal(10_000)
        min_out = refreshed_quote.amount_out * (Decimal(1) - slip_fraction)

        log.info(
            "[multi_hop:%s] EXEC_CANDIDATE route=%s dexes=%s amount_in=%.6f amount_out_est=%.6f profit_bps=%.3f gross_usd=%.3f gas_usd=%.3f net_usd=%.3f",
            chain,
            "->".join(cycle_tokens),
            ",".join(dex_path),
            float(refreshed_quote.amount_in),
            float(refreshed_quote.amount_out),
            float(refreshed_profit["profit_bps"]),
            float(refreshed_profit["gross_usd"]),
            float(refreshed_profit["gas_usd"]),
            float(refreshed_profit["net_usd"]),
        )

        detail_parts = [f"{refreshed_quote.amount_in} {cycle_tokens[0]}"]
        amt = refreshed_quote.amount_in
        for leg in refreshed_quote.legs:
            amt = leg.amount_out
            detail_parts.append(f"{amt:.6g} {leg.token_out}")
        print(f"[DETAILS] input={refreshed_quote.amount_in} {cycle_tokens[0]} → " + " → ".join(detail_parts[1:]))

        simulated_count += 1
        auto_metrics.inc("simulations_run")
        profit_usd_total += refreshed_profit.get("net_usd", Decimal(0))
        try:
            res = executor.execute_trade(
                path_tokens=cycle_tokens,
                dex_path=list(dex_path),
                amount_in=refreshed_quote.amount_in,
                min_amount_out=min_out,
                amount_out_est=refreshed_quote.amount_out,
                dry_run=strategy_cfg.get("dry_run", True),
            )
            execution_results.append(res)
        except Exception:
            auto_metrics.inc("simulations_failed")
            log.exception("[multi_hop:%s] execution call failed", chain)
        executed_count += 1

        if pnl_store is not None:
            try:
                amount_in_usd = (
                    float(refreshed_quote.amount_in * base_token_price_usd)
                    if base_token_price_usd
                    else 0.0
                )
                amount_out_usd = (
                    float(refreshed_quote.amount_out * base_token_price_usd)
                    if base_token_price_usd
                    else 0.0
                )
                trade_record = {
                    "timestamp": time.time(),
                    "chain": chain,
                    "tx_hash": executor.last_tx_hash,
                    "block_number": block_number,
                    "route_key": route_key,
                    "tokens": list(cycle_tokens),
                    "dex_path": list(dex_path),
                    "amount_in_base": float(refreshed_quote.amount_in),
                    "amount_out_base": float(refreshed_quote.amount_out),
                    "amount_in_usd": amount_in_usd,
                    "amount_out_usd": amount_out_usd,
                    "gross_usd": float(refreshed_profit["gross_usd"]),
                    "gas_usd": float(refreshed_profit["gas_usd"]),
                    "net_usd": float(refreshed_profit["net_usd"]),
                    "status": "submitted" if executor.last_tx_hash else "simulated",
                    "reason": "normal",
                    "sim_reason": "ok",
                }
                pnl_store.record_trade(trade_record)
                executor.notify_route_performance(trade_record)
                if telegram_notifier is not None:
                    try:
                        telegram_notifier.send_trade_pnl(trade_record)
                    except Exception:
                        log.exception("Unexpected error while sending Telegram PnL notification")
            except Exception:
                log.exception("Failed to record trade PnL")

        if route_history is not None:
            try:
                record_data = dict(cand.get("history_record") or {})
                record_data.update(
                    {
                        "timestamp": time.time(),
                        "chain": chain,
                        "mode": cand.get("source", "path"),
                        "route_key": cand.get("route_key", compute_route_key(cycle_tokens, dex_path)),
                        "cycle_tokens": cycle_tokens,
                        "dex_path": dex_path,
                        "amount_in_base": quote.amount_in,
                        "amount_out_base": quote.amount_out,
                        "profit_bps": profit["profit_bps"],
                        "gross_usd": profit["gross_usd"],
                        "gas_usd": profit["gas_usd"],
                        "net_usd": profit["net_usd"],
                        "live_verified": True,
                        "executed": True,
                        "execution_result": "success",
                        "base_price_usd": float(base_token_price_usd) if base_token_price_usd is not None else record_data.get("base_price_usd"),
                        "notional_usd": record_data.get("notional_usd"),
                    }
                )
                if record_data.get("notional_usd") is None and base_token_price_usd:
                    record_data["notional_usd"] = float(quote.amount_in * base_token_price_usd)
                route_history.record(record_data)
            except Exception:
                pass

    if profitable_routes == 0:
        print(f"[multi_hop:{chain}] No profitable cycles found this tick")
        if best_candidate:
            bc = best_candidate
            log.info(
                "[multi_hop:%s] EXEC_CANDIDATE(best_effort) route=%s dexes=%s amount_in=%.6f amount_out_est=%.6f profit_bps=%.3f gross_usd=%.3f gas_usd=%.3f net_usd=%.3f",
                chain,
                "->".join(bc["cycle_tokens"]),
                ",".join(bc["dex_path"]),
                float(bc["quote"].amount_in),
                float(bc["quote"].amount_out),
                float(bc["profit_bps"]),
                float(bc["gross_usd"]),
                float(bc["gas_usd"]),
                float(bc["net_usd"]),
            )

    # Final tick completion print including path and cycle statistics
    if run_paths:
        path_eval_msg = f"evaluated {paths_considered} paths, pruned {paths_pruned}, processed {processed_paths}/{raw_paths} (limit {max_paths_per_tick})"
    else:
        path_eval_msg = "path enumeration disabled"
    if run_bellman:
        cycle_eval_msg = f", cycles evaluated {cycles_eval if 'cycles_eval' in locals() else 0}"
    else:
        cycle_eval_msg = ""
    print(
        f"[multi_hop:{chain}] Tick complete — {path_eval_msg}{cycle_eval_msg}, profitable {profitable_routes}"
    )

    log.info(
        "[multi-hop] tick summary: candidates=%d simulated=%d mempool_rejected=%d performance_pruned=%d executed=%d profit_usd=%0.2f",
        candidates_considered,
        simulated_count,
        mempool_rejected,
        perf_pruned,
        executed_count,
        float(profit_usd_total),
    )

    if pnl_store is not None and pnl_summary_interval > 0 and tick_counter % pnl_summary_interval == 0:
        try:
            recent = pnl_store.load_recent(chain=chain, max_age_sec=86_400)
            summary = compute_pnl_summary(recent)
            log.info(
                "[pnl:%s] 24h net=%.2f gross=%.2f gas=%.2f trades=%d wins=%d losses=%d max_dd=%.2f",
                chain,
                summary.get("net_usd", 0.0),
                summary.get("gross_usd", 0.0),
                summary.get("gas_usd", 0.0),
                summary.get("total_trades", 0),
                summary.get("wins", 0),
                summary.get("losses", 0),
                summary.get("max_drawdown_usd", 0.0),
            )
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("[pnl:%s] Failed to compute PnL summary: %s", chain, exc)
    # Metrics: only count path stats when enumeration is enabled
    if run_paths:
        auto_metrics.inc("paths_evaluated", paths_considered)
        auto_metrics.inc("paths_pruned", paths_pruned)
    auto_metrics.inc("profitable_routes", profitable_routes)
    return {"execution_results": execution_results}
