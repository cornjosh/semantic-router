"""Embedded local-volume SQLite. No database daemon or optional dependency."""

from __future__ import annotations

import asyncio
import json
import re
import sqlite3
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, TypeVar

from semantic_router.adaptive.state.base import StateUnavailable
from semantic_router.adaptive.state.kv import Cell, check_keys, encode

T = TypeVar("T")


class SQLiteKV:
    def __init__(self, *, path: str, timeout_ms: int = 2000):
        if not path or path == ":memory:" or path.startswith("file:"):
            raise ValueError("SQLite requires a persistent local file path")
        self.path = Path(path).absolute()
        # Older system Python builds carry the SQLite WAL-reset corruption bug.
        # Preserve zero extra dependencies using rollback journaling there.
        # Use the conservative upstream fixed release floor (backported distro
        # fixes may also be safe, but cannot be detected from version alone).
        self.journal_mode = (
            "wal" if sqlite3.sqlite_version_info >= (3, 51, 3) else "delete"
        )
        if self._filesystem_type() in {
            "nfs",
            "nfs4",
            "cifs",
            "smb3",
            "ceph",
            "glusterfs",
            "9p",
            "fuse.sshfs",
            "fuse.rclone",
            "fuse.glusterfs",
        }:
            raise ValueError(
                "SQLite requires a local filesystem; use etcd for shared state"
            )
        self.timeout_ms = timeout_ms
        self._slots = asyncio.Semaphore(8)
        self._pending: set[asyncio.Task[Any]] = set()
        self._closed = False

    def _filesystem_type(self) -> str | None:
        """Reject known network mounts on Linux, resolving symlinks first.

        Other platforms still require an operator-supplied local volume; mount
        detection is a guard against common mistakes, not a filesystem proof.
        """
        mountinfo = Path("/proc/self/mountinfo")
        if not mountinfo.exists():
            return None
        resolved = self.path.resolve()
        matches = []
        for line in mountinfo.read_text().splitlines():
            fields, filesystem = line.split(" - ", 1)
            mount = re.sub(
                r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), fields.split()[4]
            )
            if resolved.is_relative_to(mount):
                matches.append((len(mount), filesystem.split()[0]))
        return max(matches)[1] if matches else None

    async def _run(self, action: Callable[[sqlite3.Connection], T]) -> T:
        if self._closed:
            raise StateUnavailable("shared state closed")

        def execute() -> T:
            try:
                # One connection/transaction stays on one executor thread.
                conn = sqlite3.connect(str(self.path), timeout=self.timeout_ms / 1000)
                try:
                    conn.execute("PRAGMA synchronous=FULL")
                    return action(conn)
                finally:
                    conn.close()
            except (sqlite3.Error, OSError):
                raise StateUnavailable("shared state unavailable") from None

        try:
            await asyncio.wait_for(self._slots.acquire(), self.timeout_ms / 1000)
        except asyncio.TimeoutError:
            raise StateUnavailable("shared state busy") from None
        task = asyncio.create_task(asyncio.to_thread(execute))
        self._pending.add(task)

        def completed(task: asyncio.Task[Any]) -> None:
            self._pending.discard(task)
            self._slots.release()
            if not task.cancelled():
                task.exception()  # Consume errors even if caller disconnected.

        task.add_done_callback(completed)
        # Cancellation cannot abort a thread in COMMIT. Keep its slot until it
        # finishes; a retry resolves an ambiguous acknowledgement by identity.
        return await asyncio.shield(task)

    async def initialize(self) -> None:
        def initialize(conn: sqlite3.Connection) -> None:
            # Concurrent first opens can make journal_mode fail with BUSY
            # immediately despite busy_timeout. Retry only this initialization
            # race, on the worker thread and within the existing time budget.
            deadline = time.monotonic() + self.timeout_ms / 1000
            while True:
                try:
                    mode = conn.execute(
                        f"PRAGMA journal_mode={self.journal_mode}"
                    ).fetchone()[0]
                    break
                except sqlite3.OperationalError as exc:
                    if str(exc) != "database is locked" or time.monotonic() >= deadline:
                        raise
                    time.sleep(0.01)
            if mode != self.journal_mode:
                raise StateUnavailable("SQLite durable journal unavailable")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS adaptive_kv "
                "(key TEXT PRIMARY KEY, revision INTEGER NOT NULL, value BLOB NOT NULL)"
            )
            conn.commit()

        # The operator supplies the persistent directory; never silently make a
        # missing mount look healthy by creating an ephemeral replacement.
        await self._run(initialize)

    async def read(self, keys: Sequence[str]) -> dict[str, Cell]:
        check_keys(keys)

        def read(conn: sqlite3.Connection) -> dict[str, Cell]:
            conn.execute("BEGIN")
            result = {}
            for key in keys:
                row = conn.execute(
                    "SELECT revision,value FROM adaptive_kv WHERE key=?", (key,)
                ).fetchone()
                result[key] = Cell(row[0], json.loads(row[1])) if row else Cell(0, None)
            conn.commit()
            return result

        return await self._run(read)

    async def commit(
        self, expected: Mapping[str, Cell], writes: Mapping[str, dict[str, Any]]
    ) -> bool:
        check_keys(list(expected))
        if not writes.keys() <= expected.keys():
            raise ValueError("writes require compared keys")
        encoded = {key: encode(value) for key, value in writes.items()}

        def commit(conn: sqlite3.Connection) -> bool:
            conn.execute("BEGIN IMMEDIATE")
            for key, cell in expected.items():
                row = conn.execute(
                    "SELECT revision FROM adaptive_kv WHERE key=?", (key,)
                ).fetchone()
                if (row[0] if row else 0) != cell.revision:
                    conn.rollback()
                    return False
            for key, value in encoded.items():
                conn.execute(
                    "INSERT INTO adaptive_kv(key,revision,value) VALUES (?,?,?) "
                    "ON CONFLICT(key) DO UPDATE SET revision=excluded.revision, "
                    "value=excluded.value",
                    (key, expected[key].revision + 1, value),
                )
            conn.commit()
            return True

        return await self._run(commit)

    async def aclose(self) -> None:
        self._closed = True
        if self._pending:
            await asyncio.gather(*self._pending, return_exceptions=True)
