"""Actual consensus failover, not merely two clients of one mocked endpoint."""

import asyncio
from uuid import uuid4

import pytest

from semantic_router.adaptive.state.base import StateUnavailable
from semantic_router.adaptive.state.factory import open_state
from tests.integration.adaptive.test_shared_state import (
    ARMS,
    context,
    decision,
    feedback,
)


@pytest.mark.asyncio
async def test_leader_loss_quorum_loss_and_wal_restart(etcd_cluster):
    cluster = etcd_cluster
    store = open_state(
        backend="etcd",
        endpoints=cluster.endpoints,
        prefix="/failover/" + uuid4().hex + "/",
        timeout_ms=1500,
    )
    await store.initialize()
    d = await store.record_decision(proposal=decision())
    event = feedback(d)
    assert await store.feedback(tenant_id="tenant-a", event=event)
    leader = await asyncio.to_thread(cluster.leader)
    survivors = [i for i in range(3) if i != leader]
    try:
        await asyncio.to_thread(cluster.stop, leader)
        await asyncio.to_thread(cluster.wait, survivors)
        assert await store.ready()
        assert (
            await store.lookup(
                tenant_id="tenant-a", request_id=d.request_id, fingerprint=d.fingerprint
            )
            == d
        )
        assert not await store.feedback(tenant_id="tenant-a", event=event)
        after_failure = await store.record_decision(proposal=decision())
        assert await store.feedback(tenant_id="tenant-a", event=feedback(after_failure))

        await asyncio.to_thread(cluster.stop, survivors[0])
        with pytest.raises(StateUnavailable):
            await store.ready()
        with pytest.raises(StateUnavailable):
            await store.record_decision(proposal=decision())

        # All members restart from their individual disk WALs, not fresh stores.
        await asyncio.to_thread(cluster.stop, survivors[1])
        for i in range(3):
            cluster.start(i)
        await asyncio.to_thread(cluster.wait)
        assert await store.ready()
        snapshot = await store.snapshot(
            context=context(), models=ARMS, cluster_ids=["east"]
        )
        assert snapshot.learning["a"].failures == 2
        assert not await store.feedback(tenant_id="tenant-a", event=event)
    finally:
        for i in range(3):
            cluster.start(i)
        await asyncio.to_thread(cluster.wait)
        await store.aclose()
