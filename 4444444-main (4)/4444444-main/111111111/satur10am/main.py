"""Entrypoint for the refactored arbitrage bot.

This script wires together configuration loading, registry bootstrap,
optional event-driven filtering, and the arbitrage engine.  Execution
remains disabled by default; trades are only simulated unless explicitly
enabled in configuration.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from config.loader import load_bot_config
from core.arbitrage_engine import ArbitrageEngine
from execution.trade_executor import TradeExecutor
from app_logging import setup_logging
from scanner.event_filter import EventDrivenFilter
from scanner.registry import Registry
from utils.rpc import JsonRpcClient, NullRPC


async def _maybe_start_filter(event_filter: EventDrivenFilter | None) -> None:
    if event_filter:
        await event_filter.start()


async def run() -> None:
    parser = argparse.ArgumentParser(description="Event-driven BSC arbitrage bot (refactored)")
    parser.add_argument("--config", type=Path, default=Path("config/bot.yaml"), help="Path to config file")
    parser.add_argument("--rpc", type=str, default=None, help="JSON-RPC endpoint (optional)")
    args = parser.parse_args()

    setup_logging()
    config = load_bot_config(args.config)

    registry = Registry.from_config(config)
    rpc = JsonRpcClient(args.rpc) if args.rpc else NullRPC()
    event_filter = EventDrivenFilter(rpc=rpc, registry=registry, dex_configs=config.get("dexes", {}), strategy_cfg=config.get("strategy", {}), config=config.get("event_filter", {}))
    executor = TradeExecutor(dry_run_default=not bool(config.get("execution", {}).get("enabled", False)))
    engine = ArbitrageEngine(chain=config.get("chain", "bsc"), registry=registry, executor=executor, config=config, event_filter=event_filter)

    await _maybe_start_filter(event_filter if config.get("event_filter", {}).get("enabled", True) else None)

    log = logging.getLogger("main")
    log.info("[bootstrap] registry_pairs=%d tokens=%d", len(registry.pairs), len(registry.tokens))
    try:
        while True:
            engine.tick()
            await asyncio.sleep(1.0)
    except KeyboardInterrupt:
        log.info("[shutdown] stopping event filter")
        await event_filter.stop()


if __name__ == "__main__":
    asyncio.run(run())
