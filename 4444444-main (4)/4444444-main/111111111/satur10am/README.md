# BSC Event-Driven Arbitrage Bot (Refactored)

This repository contains a slimmed-down, modularised arbitrage bot
prototype for Uniswap V2-style DEXes on BSC. The refactor focuses on
clarity, safety and extensibility while keeping execution behind explicit
configuration toggles.

## Architecture Overview

```
core/        # Strategy orchestration, simulations, math helpers
scanner/     # Event-driven swap listener
execution/   # (placeholder) trade execution + gas estimation hooks
logging/     # Structured, coloured logging helpers
utils/       # Reusable safety/retry helpers
config/      # Configuration loaders/placeholders
arb/         # Compatibility shims for legacy imports
main.py      # Entrypoint wired to the refactored modules
```

### Event-Driven Filtering
`scanner/event_filter.py` subscribes to Swap events for high-liquidity
pools and enqueues candidate pools only when price movements exceed a
configurable threshold. The arbitrage engine can read filtered
candidates instead of brute-forcing the full pair list each tick.

### Logging
`logging/structured.py` installs a coloured stream handler with a
compact, readable format. Import `setup_logging()` at startup to enable
structured logs.

### Safety Helpers
`utils/safety.py` provides async retry helpers and numeric clamping to
wrap RPC calls or other transient operations without littering the code
base with boilerplate.

## Running

The entrypoint remains `main.py`. It expects a JSON-RPC endpoint and
registry populated with pairs/tokens. Execution remains **disabled by
default** and should only be toggled deliberately in configuration.

Example skeleton:

```bash
python main.py --config config/bot.yaml
```

## Notes

* Wallet/private-key handling is untouched; execution remains guarded.
* The refactor keeps compatibility shims under `arb/` so previous import
  paths remain valid while the new module layout is adopted.
* This codebase is intentionally lightweight for clarity; extend the
  `execution/` and `config/` packages to integrate with your production
  components.
