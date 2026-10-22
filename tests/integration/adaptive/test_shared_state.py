"""Real PostgreSQL, unique schema per test, including separate Python processes."""

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from semantic_router.adaptive.selection import (
    ClusterOption,
    LearningConfig,
    LoadConfig,
    choose_cluster,
    choose_model,
)
from semantic_router.adaptive.state.base import (
    Decision,
    Feedback,
    LearningContext,
    ModelArm,
    Observation,
    StateConflict,
    StateNotFound,
)
from semantic_router.adaptive.state.postgres import PostgresState

ARMS = (ModelArm(model_id="a", version="1"), ModelArm(model_id="b", version="1"))


def context(**updates):
    return LearningContext(
        tenant_id="tenant-a",
        policy_version="p1",
        feature_version="f1",
        category="code",
        difficulty="easy",
    ).model_copy(update=updates)


def decision(**updates):
    now = datetime.now(timezone.utc)
    return Decision(
        request_id=str(uuid4()),
        fingerprint="a" * 64,
        context=context(),
        model_id="a",
        model_version="1",
        cluster_id="east",
        candidates=ARMS,
        propensity=1,
        created_at=now,
        expires_at=now + timedelta(seconds=120),
    ).model_copy(update=updates)


def observation(d, **updates):
    return Observation(
        event_id=str(uuid4()),
        decision_id=d.decision_id,
        attempt_id=d.attempt_id,
        cluster_id=d.cluster_id,
        observed_at=datetime.now(timezone.utc),
        outcome="success",
        ttft_ms=100,
        latency_ms=500,
    ).model_copy(update=updates)


def feedback(d, **updates):
    return Feedback(
        event_id=str(uuid4()),
        decision_id=d.decision_id,
        model_id=d.model_id,
        model_version=d.model_version,
        policy_version=d.context.policy_version,
        success=False,
    ).model_copy(update=updates)


@pytest_asyncio.fixture
async def stores():
    dsn = os.environ.get(
        "ADAPTIVE_TEST_DSN", "postgresql://postgres:postgres@127.0.0.1:5432/postgres"
    )
    schema = "adaptive_test_" + uuid4().hex
    first = PostgresState(
        dsn=dsn, schema=schema, load=LoadConfig(failure_threshold=2, cooldown_s=1)
    )
    second = PostgresState(
        dsn=dsn, schema=schema, load=LoadConfig(failure_threshold=2, cooldown_s=1)
    )
    await asyncio.gather(first.initialize(), second.initialize())
    try:
        yield first, second, dsn, schema
    finally:
        await first.aclose()
        await second.aclose()
        async with await psycopg.AsyncConnection.connect(dsn) as conn:
            await conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )


@pytest.mark.asyncio
async def test_atomic_decision_replay_and_conflicting_request_id(stores):
    first, second, _, _ = stores
    a = decision()
    b = a.model_copy(update={"decision_id": uuid4(), "cluster_id": "west"})
    decisions = await asyncio.gather(
        *(
            store.record_decision(proposal=proposal)
            for store, proposal in [(first, a), (second, b)] * 5
        )
    )
    assert len({d.decision_id for d in decisions}) == 1
    assert len({d.cluster_id for d in decisions}) == 1
    with pytest.raises(StateConflict):
        await second.record_decision(
            proposal=a.model_copy(update={"fingerprint": "b" * 64})
        )
    found = await second.lookup(
        tenant_id="tenant-a", request_id=a.request_id, fingerprint=a.fingerprint
    )
    assert found == decisions[0]
    assert (
        await second.lookup(
            tenant_id="other", request_id=a.request_id, fingerprint=a.fingerprint
        )
        is None
    )
    snap = await second.snapshot(
        context=context(), models=ARMS, cluster_ids=("east", "west")
    )
    assert sum(s.inflight for s in snap.clusters.values()) == 1


@pytest.mark.asyncio
async def test_feedback_once_changes_other_replica_and_survives_restart(stores):
    first, second, dsn, schema = stores
    d = await first.record_decision(proposal=decision())
    event = feedback(d)
    applied = await asyncio.gather(
        *(
            store.feedback(tenant_id="tenant-a", event=event)
            for store in (first, second) * 5
        )
    )
    assert sum(applied) == 1
    await first.aclose()
    restarted = PostgresState(dsn=dsn, schema=schema)
    try:
        snap = await restarted.snapshot(
            context=context(), models=ARMS, cluster_ids=("east",)
        )
        assert snap.learning["a"].failures == 1
        assert snap.learning["a"].successes == 0
        assert (
            choose_model(
                candidates=ARMS, counts=snap.learning, config=LearningConfig()
            ).model_id
            == "b"
        )
        assert (
            await restarted.lookup(
                tenant_id="tenant-a", request_id=d.request_id, fingerprint=d.fingerprint
            )
            == d
        )
    finally:
        await restarted.aclose()


@pytest.mark.asyncio
async def test_observations_do_not_train_quality_and_terminal_events_are_idempotent(
    stores,
):
    first, second, _, _ = stores
    d = await first.record_decision(proposal=decision())
    event = observation(d, outcome="upstream_error", ttft_ms=None)
    applied = await asyncio.gather(
        *(
            store.observe(tenant_id="tenant-a", event=event)
            for store in (first, second) * 5
        )
    )
    assert sum(applied) == 1
    duplicate_callback = event.model_copy(update={"event_id": str(uuid4())})
    assert await second.observe(tenant_id="tenant-a", event=duplicate_callback) is False
    snap = await second.snapshot(context=context(), models=ARMS, cluster_ids=("east",))
    assert snap.learning["a"].successes == snap.learning["a"].failures == 0
    assert snap.clusters["east"].inflight == 0
    assert snap.clusters["east"].ttft_ms is None
    assert snap.clusters["east"].latency_ms is None
    assert snap.clusters["east"].samples == 1
    assert snap.clusters["east"].error_rate == 1
    with pytest.raises(StateConflict):
        await first.observe(
            tenant_id="tenant-a", event=event.model_copy(update={"outcome": "success"})
        )


@pytest.mark.asyncio
async def test_wrong_tenant_model_policy_and_conflicting_feedback_cannot_poison_learning(
    stores,
):
    first, _, _, _ = stores
    d = await first.record_decision(proposal=decision())
    with pytest.raises(StateNotFound):
        await first.feedback(tenant_id="other", event=feedback(d))
    for update in ({"model_id": "b"}, {"model_version": "2"}, {"policy_version": "p2"}):
        with pytest.raises(StateConflict):
            await first.feedback(tenant_id="tenant-a", event=feedback(d, **update))
    event = feedback(d)
    assert await first.feedback(tenant_id="tenant-a", event=event)
    with pytest.raises(StateConflict):
        await first.feedback(
            tenant_id="tenant-a", event=event.model_copy(update={"success": True})
        )
    with pytest.raises(StateConflict):
        await first.feedback(tenant_id="tenant-a", event=feedback(d, success=True))
    for ctx in (
        context(tenant_id="other"),
        context(policy_version="p2"),
        context(feature_version="f2"),
        context(category="other"),
        context(difficulty="hard"),
    ):
        assert (
            await first.snapshot(context=ctx, models=ARMS, cluster_ids=())
        ).learning["a"].failures == 0
    other_version = (ModelArm(model_id="a", version="2"),)
    assert (
        await first.snapshot(context=context(), models=other_version, cluster_ids=())
    ).learning["a"].failures == 0


@pytest.mark.asyncio
async def test_out_of_order_observations_do_not_rewind_ewma_and_missing_ttft_is_not_zero(
    stores,
):
    first, _, _, _ = stores
    now = datetime.now(timezone.utc)
    old = now - timedelta(seconds=60)
    a = await first.record_decision(proposal=decision(created_at=old))
    b = await first.record_decision(proposal=decision(created_at=old))
    c = await first.record_decision(proposal=decision(created_at=old))
    await first.observe(
        tenant_id="tenant-a",
        event=observation(a, observed_at=now, ttft_ms=100, latency_ms=500),
    )
    await first.observe(
        tenant_id="tenant-a",
        event=observation(
            b, observed_at=now - timedelta(seconds=10), ttft_ms=400, latency_ms=1000
        ),
    )
    await first.observe(
        tenant_id="tenant-a",
        event=observation(
            c,
            observed_at=now + timedelta(milliseconds=1),
            ttft_ms=None,
            latency_ms=1000,
        ),
    )
    snap = await first.snapshot(context=context(), models=ARMS, cluster_ids=("east",))
    state = snap.clusters["east"]
    assert state.ttft_ms == 100
    assert state.latency_ms == pytest.approx(600)
    assert state.samples == 2
    assert state.inflight == 0


@pytest.mark.asyncio
async def test_failure_cooldown_and_missing_outcomes_expire_without_quality_updates(
    stores,
):
    first, _, _, _ = stores
    now = datetime.now(timezone.utc)
    for _ in range(2):
        d = await first.record_decision(proposal=decision())
        await first.observe(
            tenant_id="tenant-a", event=observation(d, outcome="timeout", ttft_ms=None)
        )
    await first.record_decision(
        proposal=decision(
            created_at=now - timedelta(seconds=10),
            expires_at=now - timedelta(seconds=1),
        )
    )
    snap = await first.snapshot(context=context(), models=ARMS, cluster_ids=("east",))
    assert snap.clusters["east"].cooldown_until > now
    assert snap.clusters["east"].inflight == 0
    assert snap.learning["a"].failures == 0


@pytest.mark.asyncio
async def test_two_real_processes_deduplicate_feedback_in_shared_database(stores):
    first, _, dsn, schema = stores
    d = await first.record_decision(proposal=decision())
    event = feedback(d)
    program = """
import asyncio, os, sys
from semantic_router.adaptive.state.postgres import PostgresState
from semantic_router.adaptive.state.base import Feedback
async def main():
    store = PostgresState(dsn=os.environ["WORKER_DSN"], schema=os.environ["WORKER_SCHEMA"])
    try:
        applied = await store.feedback(tenant_id="tenant-a", event=Feedback.model_validate_json(os.environ["WORKER_EVENT"]))
        sys.stdout.write("applied" if applied else "duplicate")
    finally:
        await store.aclose()
asyncio.run(main())
"""
    env = {
        **os.environ,
        "WORKER_DSN": dsn,
        "WORKER_SCHEMA": schema,
        "WORKER_EVENT": event.model_dump_json(),
    }
    children = [
        await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            program,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        for _ in range(2)
    ]
    try:
        outputs = await asyncio.wait_for(
            asyncio.gather(*(child.communicate() for child in children)), timeout=30
        )
        assert all(child.returncode == 0 for child in children), outputs
        assert sorted(out.decode() for out, _ in outputs) == ["applied", "duplicate"]
        assert (
            await first.snapshot(context=context(), models=ARMS, cluster_ids=())
        ).learning["a"].failures == 1
    finally:
        for child in children:
            if child.returncode is None:
                child.kill()
                await child.wait()


@pytest.mark.asyncio
async def test_new_samples_reset_expired_ewma_instead_of_reviving_old_values(stores):
    _, _, dsn, schema = stores
    store = PostgresState(dsn=dsn, schema=schema, load=LoadConfig(stale_after_s=0.2))
    try:
        first = await store.record_decision(proposal=decision())
        await store.observe(
            tenant_id="tenant-a", event=observation(first, latency_ms=10000)
        )
        await asyncio.sleep(0.25)
        second = await store.record_decision(proposal=decision())
        await store.observe(
            tenant_id="tenant-a",
            event=observation(second, ttft_ms=None, latency_ms=500),
        )
        state = (
            await store.snapshot(context=context(), models=ARMS, cluster_ids=("east",))
        ).clusters["east"]
        assert state.latency_ms == 500
        assert state.ttft_at < state.observed_at
    finally:
        await store.aclose()


@pytest.mark.asyncio
async def test_concurrent_distinct_feedback_does_not_lose_updates(stores):
    first, second, _, _ = stores
    decisions = await asyncio.gather(
        *(first.record_decision(proposal=decision()) for _ in range(12))
    )
    await asyncio.gather(
        *(
            store.feedback(tenant_id="tenant-a", event=feedback(d, success=True))
            for store, d in zip((first, second) * 6, decisions)
        )
    )
    snap = await second.snapshot(context=context(), models=ARMS, cluster_ids=())
    assert snap.learning["a"].successes == 12


@pytest.mark.asyncio
async def test_observation_rejects_wrong_identity_and_future_clock(stores):
    first, _, _, _ = stores
    d = await first.record_decision(proposal=decision())
    with pytest.raises(StateNotFound):
        await first.observe(tenant_id="other", event=observation(d))
    for update in (
        {"cluster_id": "west"},
        {"attempt_id": "other"},
        {"observed_at": datetime.now(timezone.utc) + timedelta(minutes=1)},
    ):
        with pytest.raises(StateConflict):
            await first.observe(tenant_id="tenant-a", event=observation(d, **update))
    cancelled = observation(d, outcome="client_cancel", ttft_ms=None)
    await first.observe(tenant_id="tenant-a", event=cancelled)
    snapshot = await first.snapshot(
        context=context(), models=ARMS, cluster_ids=("east",)
    )
    assert snapshot.clusters["east"].samples == 0
    assert snapshot.clusters["east"].inflight == 0


@pytest.mark.asyncio
async def test_poll_lease_has_one_owner_and_fences_expired_writes(stores):
    first, second, _, _ = stores
    claims = await asyncio.gather(
        first.claim_poll(cluster_id="east", owner="one", lease_seconds=0.1),
        second.claim_poll(cluster_id="east", owner="two", lease_seconds=0.1),
    )
    assert sum(claims) == 1
    old_owner = "one" if claims[0] else "two"
    await asyncio.sleep(0.12)
    assert await second.claim_poll(
        cluster_id="east", owner="replacement", lease_seconds=5
    )
    assert not await first.record_active_load(
        cluster_id="east", owner=old_owner, load=0.9
    )
    assert await second.record_active_load(
        cluster_id="east", owner="replacement", load=0.2
    )
    state = (
        await first.snapshot(context=context(), models=ARMS, cluster_ids=("east",))
    ).clusters["east"]
    assert state.active_load == 0.2
    assert state.active_at is not None
    assert state.samples == 0


@pytest.mark.asyncio
async def test_active_pollers_share_lease_and_use_real_http(stores):
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    from semantic_router.adaptive.telemetry import TelemetryPoller, TelemetryTarget

    first, second, _, _ = stores
    calls = []

    async def handler(req):
        calls.append(req.method)
        return web.json_response({"gpu": {"utilization": 75}})

    app = web.Application()
    app.router.add_get("/metrics", handler)
    async with TestServer(app) as server:
        target = TelemetryTarget(
            cluster_id="east",
            enabled=True,
            endpoint=str(server.make_url("/metrics")),
            load_path="gpu.utilization",
            divisor=100,
        )
        pollers = (
            TelemetryPoller(store=first, targets=(target,)),
            TelemetryPoller(store=second, targets=(target,)),
        )
        try:
            results = await asyncio.gather(*(poller.poll_once() for poller in pollers))
            assert sorted(result["east"] for result in results) == ["leased", "updated"]
            assert calls == ["GET"]
            state = (
                await first.snapshot(
                    context=context(), models=ARMS, cluster_ids=("east",)
                )
            ).clusters["east"]
            assert state.active_load == 0.75
        finally:
            await asyncio.gather(*(poller.aclose() for poller in pollers))


@pytest.mark.asyncio
@pytest.mark.parametrize("reply", ["oversized", "invalid", "slow"])
async def test_failed_active_poll_does_not_fabricate_zero_load(stores, reply):
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    from semantic_router.adaptive.telemetry import TelemetryPoller, TelemetryTarget

    first, _, _, _ = stores

    async def handler(req):
        if reply == "slow":
            await asyncio.sleep(0.2)
        if reply == "oversized":
            return web.Response(text="x" * 2000)
        return web.json_response({"load": -1})

    app = web.Application()
    app.router.add_get("/load", handler)
    async with TestServer(app) as server:
        target = TelemetryTarget(
            cluster_id="east",
            enabled=True,
            endpoint=str(server.make_url("/load")),
            timeout_ms=50,
            max_response_bytes=64,
        )
        poller = TelemetryPoller(store=first, targets=(target,))
        try:
            assert await poller.poll_once() == {"east": "unavailable"}
            state = (
                await first.snapshot(
                    context=context(), models=ARMS, cluster_ids=("east",)
                )
            ).clusters["east"]
            assert state.active_load is None
        finally:
            await poller.aclose()


@pytest.mark.asyncio
async def test_disabled_active_polling_needs_neither_endpoint_key_nor_database():
    from semantic_router.adaptive.telemetry import TelemetryPoller, TelemetryTarget

    store = PostgresState(dsn="postgresql://invalid:secret@127.0.0.1:1/missing")
    poller = TelemetryPoller(
        store=store,
        targets=(
            TelemetryTarget(cluster_id="east", api_key_env="ABSENT_TELEMETRY_KEY"),
        ),
    )
    try:
        assert await poller.poll_once() == {"east": "disabled"}
    finally:
        await poller.aclose()
        await store.aclose()


@pytest.mark.asyncio
async def test_actual_passive_cluster_metrics_change_the_next_choice(stores):
    first, second, _, _ = stores
    options = tuple(
        ClusterOption(cluster_id=name, model_id="a") for name in ("east", "west")
    )
    snap = await first.snapshot(
        context=context(), models=ARMS, cluster_ids=("east", "west")
    )
    assert (
        choose_cluster(
            model_id="a",
            candidates=options,
            states=snap.clusters,
            config=LoadConfig(),
            now=snap.captured_at,
        ).cluster_id
        == "east"
    )
    for name, ttft, latency in (("east", 1500, 5000), ("west", 100, 500)):
        d = await first.record_decision(proposal=decision(cluster_id=name))
        await second.observe(
            tenant_id="tenant-a", event=observation(d, ttft_ms=ttft, latency_ms=latency)
        )
    snap = await first.snapshot(
        context=context(), models=ARMS, cluster_ids=("east", "west")
    )
    assert (
        choose_cluster(
            model_id="a",
            candidates=options,
            states=snap.clusters,
            config=LoadConfig(),
            now=snap.captured_at,
        ).cluster_id
        == "west"
    )


@pytest.mark.asyncio
async def test_database_failure_is_explicit_and_contains_no_dsn_secret():
    from semantic_router.adaptive.state.base import StateUnavailable

    store = PostgresState(
        dsn="postgresql://missing:private-password@127.0.0.1:1/postgres", timeout_ms=100
    )
    try:
        with pytest.raises(StateUnavailable) as exc:
            await store.snapshot(context=context(), models=ARMS, cluster_ids=("east",))
        assert str(exc.value) == "shared state unavailable"
        assert "private-password" not in str(exc.value)
    finally:
        await store.aclose()
