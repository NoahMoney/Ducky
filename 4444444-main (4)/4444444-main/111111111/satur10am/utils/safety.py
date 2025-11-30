"""Safety and retry helpers used across the bot."""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Iterable, Optional

log = logging.getLogger(__name__)


async def async_retry(fn: Callable[[], Any], retries: int = 3, delay: float = 0.5, backoff: float = 2.0) -> Any:
    """Retry an awaitable callable with exponential backoff."""
    attempt = 0
    exc: Optional[Exception] = None
    while attempt < retries:
        try:
            return await fn()
        except Exception as err:  # pragma: no cover - defensive
            exc = err
            attempt += 1
            sleep_for = delay * (backoff ** (attempt - 1))
            log.debug("[retry] attempt=%d/%d sleep=%.2fs err=%s", attempt, retries, sleep_for, err)
            await asyncio.sleep(sleep_for)
    if exc:
        raise exc
    raise RuntimeError("async_retry exhausted without exception detail")


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


__all__ = ["async_retry", "clamp"]
