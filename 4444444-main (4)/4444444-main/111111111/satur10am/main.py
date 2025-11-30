"""
Master orchestration for the arbitrage bot.

Fully patched for the new DexTools-only universe system:
 - No UniverseConfig
 - No DailyUniverseSelector
 - UniverseManager is the only source of dynamic tokens
 - Universe tokens filter scanner + graph + strategy (Option A)
"""

from __future__ import annotations
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
import logging
import time
import traceback
from pathlib import Path
from typing import Dict, List, Optional

from arb.multi_hop_engine import run_strategy
from config.loader import load_bot_config, load_tokens_config
from universe import UniverseManager
from core.mempool_listener import MempoolListener, get_last_mempool_event_ts
from core.pair_event_listener import PairEventListener, get_last_pair_event_ts
from core.rpc_manager import RPCManager
from discovery.factory_scanner import FactoryScanner
from discovery.registry import Registry
from execution.trade_executor import RiskManager, TradeExecutor
from utils.logging import setup_logging
from utils.metrics import auto_metrics
from utils.pnl_store import PnLStore
from utils import risk_limits
from utils.risk_limits import RiskLimits
from utils.route_performance import RoutePerformance
from utils.telegram_notifier import TelegramNotifier



# ======================================================================
# ENABLED DEX FILTER
# ======================================================================
def _filter_enabled_dexes(dexes_cfg: Dict[str, Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    filtered: Dict[str, Dict[str, object]] = {}
    for dex_name, meta in (dexes_cfg or {}).items():
        enabled = meta.get("enabled") if isinstance(meta, dict) else False
        if enabled:
            filtered[dex_name] = meta
    return filtered



# ======================================================================
# SCANNER WRAPPER
# ======================================================================
async def run_scanners(scanners: List[FactoryScanner], registry: Registry) -> None:
    if not scanners:
        return
    tasks = [scanner.scan(registry) for scanner in scanners]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            print(f"[scanner] error: {r}")



# ======================================================================
# BLOCK-ALIGNED LOOP (UNCHANGED)
# ======================================================================
async def block_aligned_loop(
    rpc: RPCManager,
    registry: Registry,
    executor: TradeExecutor,
    risk_manager: RiskManager,
    risk_limits_obj: RiskLimits,
    pnl_store: PnLStore,
    telegram_notifier: TelegramNotifier,
    route_performance: RoutePerformance,
    strategy_cfg: Dict[str, object],
    loop_cfg: Dict[str, object],
    multi_hop_cfg: Dict[str, object],
    tokens_cfg: Dict[str, Dict[str, object]],
    dexes_cfg: Dict[str, Dict[str, object]],
    risk_cfg: Dict[str, object],
    monitoring_cfg: Dict[str, object],
    universe_tokens: List[str],
    tokens_policy_cfg: Dict[str, object],
    execution_cfg: Dict[str, object],
    pair_listener: Optional[PairEventListener],
    mempool_listener: Optional[MempoolListener],
) -> None:

    logger = logging.getLogger(__name__)
    last_block = None
    tick_counter = 0
    health_interval = int(loop_cfg.get("health_tick_interval", 10))
    poll_interval = float(loop_cfg.get("poll_interval_ms", 500)) / 1000
    consecutive_failures = 0
    prev_in_cooldown = False

    kill_cfg = risk_limits_obj.config if hasattr(risk_limits_obj, "config") else {}
    kill_cfg = risk_limits._load_kill_cfg(kill_cfg) if isinstance(kill_cfg, dict) else {}

    max_daily_loss_usd = float(kill_cfg.get("max_daily_loss_usd", 0))
    max_consecutive_failures = int(kill_cfg.get("max_consecutive_failures", 0))
    ws_offline_threshold_sec = float(kill_cfg.get("ws_offline_threshold_sec", 180))
    cooldown_minutes = int(kill_cfg.get("cooldown_minutes", 60))

    while True:
        tick_counter += 1
        auto_metrics.inc("ticks")
        now_ts = time.time()
        in_cooldown = risk_limits.is_in_cooldown(now_ts)

        # ------------------------------------------
        # Fetch block
        # ------------------------------------------
        block_hex = await rpc.call("eth_blockNumber", [])
        try:
            block_num = int(block_hex, 16) if isinstance(block_hex, str) else (
                block_hex if isinstance(block_hex, int) else None
            )
        except:
            block_num = None

        if block_num is None or (last_block is not None and block_num <= last_block):
            await asyncio.sleep(poll_interval)
            continue

        last_block = block_num

        # ------------------------------------------
        # Run strategy tick
        # ------------------------------------------
        try:
            loop = asyncio.get_running_loop()
            original_dry_run = executor.dry_run_default

            if in_cooldown:
                executor.dry_run_default = True

            strategy_result = await loop.run_in_executor(
                None,
                run_strategy,
                "bsc",
                registry,
                tokens_cfg,
                dexes_cfg,
                strategy_cfg,
                risk_cfg,
                risk_limits_obj,
                monitoring_cfg,
                executor,
                risk_manager,
                rpc,
                multi_hop_cfg,
                universe_tokens,
                tokens_policy_cfg,
                execution_cfg,
                block_num,
                None,
                pnl_store,
                telegram_notifier,
                route_performance,
                {},
            )

            executor.dry_run_default = original_dry_run

        except Exception as exc:
            print(f"[main] strategy tick error: {exc}")
            traceback.print_exc()
            strategy_result = None

        # ------------------------------------------
        # Parse execution results
        # ------------------------------------------
        execution_results = []
        if isinstance(strategy_result, dict):
            execution_results = strategy_result.get("execution_results", []) or []

        real_results = [
            r for r in execution_results
            if getattr(r, "status", r.get("status", "")) != "dry_run"
        ]

        had_failure = any(getattr(r, "success", r.get("success", False)) is False for r in real_results)
        had_success = any(getattr(r, "success", r.get("success", False)) is True for r in real_results)

        if real_results:
            if had_failure:
                consecutive_failures += 1
            if had_success:
                consecutive_failures = 0

        # ------------------------------------------
        # Kill switch logic
        # ------------------------------------------
        pnl_24h = pnl_store.get_24h_pnl()
        pnl_value = float(pnl_24h.get("net_usd", pnl_24h.get("pnl", 0))) if isinstance(pnl_24h, dict) else float(pnl_24h)

        if max_daily_loss_usd > 0 and pnl_value < -max_daily_loss_usd:
            risk_limits.trigger_kill_switch("max_daily_loss", now_ts)
            in_cooldown = True

        if max_consecutive_failures > 0 and consecutive_failures > max_consecutive_failures:
            risk_limits.trigger_kill_switch("consecutive_failures", now_ts)
            in_cooldown = True

        # ------------------------------------------
        # Health logging
        # ------------------------------------------
        if tick_counter % max(1, health_interval) == 0:
            print(f"[health] ticks={tick_counter} pnl24h={pnl_value:.2f} fails={consecutive_failures}")



# ======================================================================
# ASYNC MAIN — FULLY PATCHED
# ======================================================================
async def async_main() -> None:
    setup_logging()
    repo_root = Path(__file__).resolve().parent

    # ----------------------------
    # Load configs
    # ----------------------------
    cfg = load_bot_config(repo_root / "config" / "config.yaml")
    tokens_cfg = load_tokens_config(repo_root / "config" / "tokens.yaml")

    chain_cfg = cfg.get("chain", {})
    dexes_cfg = cfg.get("dexes", {})
    strategy_cfg = cfg.get("strategy", {})
    execution_cfg = cfg.get("execution", {})
    risk_cfg = cfg.get("risk", {})
    risk_limits_cfg = cfg.get("risk_limits", {})
    multi_hop_cfg = cfg.get("multi_hop", {})
    telemetry_cfg = cfg.get("telemetry", {})

    # ----------------------------
    # RPC manager
    # ----------------------------
    rpc_urls = chain_cfg.get("rpc_urls", [])
    if not rpc_urls:
        raise SystemExit("No RPC URLs configured")
    rpc = RPCManager(rpc_urls)

    # ==================================================================
    # NEW: DEXTOOLS-ONLY UNIVERSE LOADING
    # ==================================================================
    print("[UNIVERSE] Using DexTools-only UniverseManager (Option A)")
    universe_manager = UniverseManager(cfg)
    universe_tokens = universe_manager.load_or_build_universe()

    # ==================================================================
    # REGISTRY INITIALIZATION
    # ==================================================================
    registry = Registry()
    for sym, meta in (tokens_cfg or {}).items():
        if meta.get("enabled", True):
            addr = meta.get("address")
            dec = meta.get("decimals")
            if isinstance(addr, str) and isinstance(dec, int):
                registry.add_token(sym, addr, dec)

    # ==================================================================
    # SCANNER INITIALIZATION — FILTERED BY UNIVERSE
    # ==================================================================
    filtered_dexes_cfg = _filter_enabled_dexes(dexes_cfg)
    tracked_set = set(universe_tokens)

    factory_scanners = []
    for dex_name, dex_meta in filtered_dexes_cfg.items():
        factory_addr = dex_meta.get("factory")
        if isinstance(factory_addr, str):
            factory_scanners.append(
                FactoryScanner(
                    rpc=rpc,
                    chain=chain_cfg.get("name", "bsc"),
                    dex_name=dex_name,
                    factory_address=factory_addr,
                    tracked_tokens=tracked_set,     # <<< FILTER HERE
                )
            )

    await run_scanners(factory_scanners, registry)

    # ==================================================================
    # LISTENERS, EXECUTOR, RISK
    # ==================================================================
    pair_listener = None
    mempool_listener = None

    pnl_store = PnLStore(cfg.get("pnl_store_path"))
    risk_limits_obj = RiskLimits(risk_limits_cfg, pnl_store)
    route_performance = RoutePerformance(data_dir="data")
    telegram_notifier = TelegramNotifier(cfg, logging.getLogger(__name__))

    executor = TradeExecutor(
        chain_cfg=chain_cfg,
        dex_configs=filtered_dexes_cfg,
        tokens_config=tokens_cfg,
        execution_cfg=execution_cfg,
        registry=registry,
        route_performance=route_performance,
    )
    risk_manager = RiskManager(risk_cfg)

    # ==================================================================
    # LOOP CONFIG
    # ==================================================================
    loop_cfg = {
        "health_tick_interval": telemetry_cfg.get("health_tick_interval", 10),
        "poll_interval_ms": cfg.get("arb_loop", {}).get("poll_interval_ms", 500),
    }

    # ==================================================================
    # ENTER BLOCK-ALIGNED LOOP
    # ==================================================================
    await block_aligned_loop(
        rpc,
        registry,
        executor,
        risk_manager,
        risk_limits_obj,
        pnl_store,
        telegram_notifier,
        route_performance,
        strategy_cfg,
        loop_cfg,
        multi_hop_cfg,
        tokens_cfg,
        filtered_dexes_cfg,
        risk_cfg,
        cfg.get("monitoring", {}),
        universe_tokens,
        cfg.get("tokens", {}),
        execution_cfg,
        pair_listener,
        mempool_listener,
    )



# ======================================================================
# ENTRY POINT
# ======================================================================
if __name__ == "__main__":
    asyncio.run(async_main())
