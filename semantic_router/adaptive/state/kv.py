"""Small atomic storage boundary shared by SQLite and etcd implementations."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from semantic_router.adaptive.state.base import StateUnavailable

MAX_KEYS = 64
MAX_VALUE_BYTES = 524288


def encode(value: dict[str, Any]) -> bytes:
    result = json.dumps(value, separators=(",", ":"), allow_nan=False).encode()
    if len(result) > MAX_VALUE_BYTES:
        raise StateUnavailable("state record too large")
    return result


def check_keys(keys: Sequence[str]) -> None:
    if len(keys) > MAX_KEYS or len(set(keys)) != len(keys):
        raise StateUnavailable("invalid state transaction size")


@dataclass(frozen=True)
class Cell:
    revision: int
    value: dict[str, Any] | None


class KVStore(Protocol):
    async def initialize(self) -> None: ...

    async def read(self, keys: Sequence[str]) -> dict[str, Cell]:
        """Read all keys from one consistent, current snapshot."""
        ...

    async def commit(
        self, expected: Mapping[str, Cell], writes: Mapping[str, dict[str, Any]]
    ) -> bool:
        """Atomically compare every read revision and apply all writes, or none."""
        ...

    async def aclose(self) -> None: ...
