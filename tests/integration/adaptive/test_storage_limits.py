"""Local durability and deployment-boundary regression tests."""

import asyncio
import sqlite3
import time

import pytest

from semantic_router.adaptive.state.base import StateUnavailable
from semantic_router.adaptive.state.factory import open_state
from tests.integration.adaptive.test_shared_state import ARMS, context, decision


@pytest.mark.asyncio
async def test_sqlite_durable_journal_and_missing_mount_fail_closed(tmp_path):
    path = tmp_path / "router.db"
    store = open_state(path=str(path))
    try:
        await store.initialize()
        with sqlite3.connect(path) as conn:
            assert (
                conn.execute("PRAGMA journal_mode").fetchone()[0]
                == store.backend.journal_mode
            )
        assert await store.ready()
    finally:
        await store.aclose()
    missing = open_state(path=str(tmp_path / "missing-mount" / "router.db"))
    try:
        with pytest.raises(StateUnavailable):
            await missing.initialize()
        assert not (tmp_path / "missing-mount").exists()
    finally:
        await missing.aclose()


@pytest.mark.asyncio
async def test_sqlite_lock_timeout_does_not_block_async_loop(tmp_path):
    path = tmp_path / "router.db"
    store = open_state(path=str(path), timeout_ms=100)
    await store.initialize()
    lock = sqlite3.connect(path)
    lock.execute("BEGIN IMMEDIATE")
    ticks = 0

    async def ticker():
        nonlocal ticks
        for _ in range(10):
            await asyncio.sleep(0.01)
            ticks += 1

    start = time.monotonic()
    try:
        task = asyncio.create_task(ticker())
        with pytest.raises(StateUnavailable):
            await store.record_decision(proposal=decision())
        await task
        assert ticks == 10 and time.monotonic() - start < 1
    finally:
        lock.rollback()
        lock.close()
        await store.aclose()


@pytest.mark.asyncio
async def test_pending_limit_is_atomic_and_replay_still_works(tmp_path):
    store = open_state(path=str(tmp_path / "router.db"), max_pending_per_cluster=1)
    await store.initialize()
    try:
        accepted = await store.record_decision(proposal=decision())
        assert await store.record_decision(proposal=accepted) == accepted
        with pytest.raises(StateUnavailable, match="pending capacity"):
            await store.record_decision(proposal=decision())
        snap = await store.snapshot(
            context=context(), models=ARMS, cluster_ids=["east"]
        )
        assert snap.clusters["east"].inflight == 1
    finally:
        await store.aclose()


def test_sqlite_rejects_network_filesystem_before_opening(monkeypatch, tmp_path):
    from semantic_router.adaptive.state.sqlite import SQLiteKV

    monkeypatch.setattr(SQLiteKV, "_filesystem_type", lambda self: "nfs")
    with pytest.raises(ValueError, match="local filesystem"):
        open_state(path=str(tmp_path / "router.db"))


@pytest.mark.asyncio
async def test_many_workers_can_initialize_same_sqlite_database(tmp_path):
    for attempt in range(5):
        stores = [
            open_state(path=str(tmp_path / f"init-{attempt}.db")) for _ in range(12)
        ]
        try:
            await asyncio.gather(*(store.initialize() for store in stores))
            assert all(await asyncio.gather(*(store.ready() for store in stores)))
        finally:
            await asyncio.gather(*(store.aclose() for store in stores))


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [(3, 37, 2), (3, 51, 2)])
async def test_old_sqlite_uses_rollback_journal_to_avoid_wal_reset_bug(
    tmp_path, monkeypatch, version
):
    monkeypatch.setattr(sqlite3, "sqlite_version_info", version)
    store = open_state(path=str(tmp_path / "old-runtime.db"))
    try:
        await store.initialize()
        with sqlite3.connect(tmp_path / "old-runtime.db") as conn:
            assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    finally:
        await store.aclose()
