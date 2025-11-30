# BSC Event-Driven Arbitrage Bot (Refactored)

This repository contains a modularised, dry-run-first arbitrage bot for
Uniswap V2-style DEXes on BSC. The refactor keeps execution behind
explicit configuration toggles while introducing a clean separation of
concerns and event-driven data handling.

## Architecture Overview

```
core/            # Strategy orchestration, simulations, math helpers
scanner/         # Event-driven swap listener and registry
execution/       # Dry-run friendly executor + gas estimation hooks
app_logging/     # Structured, coloured logging helpers
utils/           # Reusable safety/retry + RPC helpers
config/          # Configuration loader + defaults
arb/             # Compatibility shims for legacy imports
main.py          # Entrypoint wired to the refactored modules
```

### Event-Driven Filtering
`scanner/event_filter.py` subscribes to Swap events for high-liquidity
pools and enqueues candidate pools only when price movements exceed a
configurable threshold. The arbitrage engine consumes these candidates
instead of brute-forcing the full pair list every tick.

### Logging
`app_logging/structured.py` installs a coloured stream handler with a
compact, readable format. Import `setup_logging()` at startup to enable
structured logs without shadowing the standard `logging` module.

### Safety & RPC Helpers
`utils/safety.py` provides async retry helpers and numeric clamping to
wrap RPC calls, and `utils/rpc.py` ships a minimal JSON-RPC client plus a
null RPC stub for offline testing.

### Simulation
`core/simulation.py` houses deterministic constant-product math and
cross-DEX cycle evaluation so profit estimates are consistent and
unit-testable.

## Running

The entrypoint remains `main.py`. It expects a JSON-RPC endpoint and
registry populated with pairs/tokens. Execution remains **disabled by
default** and should only be toggled deliberately in configuration.

Example skeleton:

```bash
python main.py --config config/bot.yaml --rpc https://bsc-dataseed.binance.org/
```

## Notes

* Wallet/private-key handling is untouched; execution remains guarded by
  a config toggle.
* Compatibility shims under `arb/` keep legacy imports intact while the
  new module layout is adopted.
* This codebase is intentionally lightweight for clarity; extend the
  `execution/`, `scanner/`, and `config/` packages to integrate with your
  production components or plug in v3 data sources.
