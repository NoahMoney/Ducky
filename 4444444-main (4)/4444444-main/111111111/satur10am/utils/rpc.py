"""Lightweight async JSON-RPC client with retry hooks."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Iterable, Optional

import aiohttp

log = logging.getLogger(__name__)


class RPCError(RuntimeError):
    pass


class JsonRpcClient:
    def __init__(self, endpoint: str, *, timeout: float = 10.0):
        self.endpoint = endpoint
        self.timeout = timeout
        self._id = 0

    async def call(self, method: str, params: Iterable[Any]) -> Any:
        self._id += 1
        payload = {"jsonrpc": "2.0", "method": method, "params": list(params), "id": self._id}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
            async with session.post(self.endpoint, json=payload) as resp:
                if resp.status != 200:
                    raise RPCError(f"RPC HTTP {resp.status}")
                data = await resp.json(loads=json.loads)
        if "error" in data:
            raise RPCError(str(data["error"]))
        return data.get("result")


class NullRPC:
    """Offline-friendly RPC stub used for local testing."""

    def __init__(self):
        self._block = 0

    async def call(self, method: str, params: Iterable[Any]) -> Any:  # pragma: no cover - trivial
        if method == "eth_blockNumber":
            self._block += 1
            return hex(self._block)
        return []


__all__ = ["JsonRpcClient", "NullRPC", "RPCError"]
