"""Structured, colored logging utilities for the arbitrage bot."""
from __future__ import annotations

import logging
import sys
from typing import Optional

# Simple ANSI color map
LEVEL_COLORS = {
    "DEBUG": "\033[36m",
    "INFO": "\033[32m",
    "WARNING": "\033[33m",
    "ERROR": "\033[31m",
    "CRITICAL": "\033[35m",
}
RESET = "\033[0m"


class ColorFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - presentation
        base = super().format(record)
        color = LEVEL_COLORS.get(record.levelname, "")
        return f"{color}{base}{RESET}" if color else base


def setup_logging(level: int = logging.INFO, terse: bool = False) -> None:
    """Install a colored stream handler if none exists."""

    root = logging.getLogger()
    if root.handlers:
        return

    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s" if not terse else "[%(levelname)s] %(message)s"
    handler.setFormatter(ColorFormatter(fmt))
    root.setLevel(level)
    root.addHandler(handler)


__all__ = ["setup_logging", "ColorFormatter"]
