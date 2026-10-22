"""Backend-independent, atomic routing state transitions.

The stores only persist decision metadata, never prompts or generated responses.
No process-local authoritative cache: every operation reads a consistent snapshot.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, TypeVar

from semantic_router.adaptive.selection import LoadConfig
from semantic_router.adaptive.state.base import (
    ClusterState,
    Decision,
    Feedback,
    LearningContext,
    ModelArm,
    Observation,
    QualityCounts,
    StateConflict,
    StateNotFound,
    StateSnapshot,
    StateUnavailable,
    fingerprint,
)
from semantic_router.adaptive.state.kv import KVStore

T = TypeVar("T")
Values = dict[str, dict[str, Any] | None]
Writes = dict[str, dict[str, Any]]


def key(kind: str, *parts: str) -> str:
    return kind + "/" + fingerprint({"parts": list(parts)})


def quality_key(context: LearningContext, model_id: str, version: str) -> str:
    return key("quality", *context.model_dump().values(), model_id, version)


def clock() -> datetime:
    return datetime.now(timezone.utc)


class DurableState:
    def __init__(
        self,
        *,
        backend: KVStore,
        load: LoadConfig | None = None,
        timeout_ms: int = 2000,
        max_pending_per_cluster: int = 4096,
    ):
        if not 1 <= timeout_ms <= 60000 or not 1 <= max_pending_per_cluster <= 4096:
            raise ValueError("invalid state limits")
        self.backend = backend
        self.load = (load or LoadConfig()).model_copy(deep=True)
        self.timeout_ms = timeout_ms
        self.max_pending_per_cluster = max_pending_per_cluster

    async def _update(
        self, keys: Sequence[str], transform: Callable[[Values], tuple[T, Writes]]
    ) -> T:
        async def attempt() -> T:
            for number in range(64):
                cells = await self.backend.read(keys)
                result, writes = transform({k: c.value for k, c in cells.items()})
                if not writes or await self.backend.commit(cells, writes):
                    return result
                # Full jitter prevents synchronized replicas spinning on one key.
                await asyncio.sleep(
                    random.uniform(0, min(0.001 * 2 ** min(number, 6), 0.05))
                )
            raise StateUnavailable("shared state contention")

        try:
            return await asyncio.wait_for(attempt(), self.timeout_ms / 1000)
        except asyncio.TimeoutError:
            raise StateUnavailable("shared state deadline exceeded") from None

    async def initialize(self) -> None:
        await self.backend.initialize()

        def initialize(values: Values) -> tuple[None, Writes]:
            value = values["schema"]
            if value is not None and value != {"version": 1}:
                raise StateUnavailable("unsupported state schema version")
            return None, {} if value else {"schema": {"version": 1}}

        await self._update(["schema"], initialize)

    async def ready(self) -> bool:
        cells = await self.backend.read(["schema"])
        return cells["schema"].value == {"version": 1}

    async def record_decision(self, *, proposal: Decision) -> Decision:
        proposal = Decision.model_validate(proposal.model_dump())
        request_key = key("request", proposal.context.tenant_id, proposal.request_id)
        decision_key = key("decision", str(proposal.decision_id))
        cluster_key = key("cluster", proposal.cluster_id)

        def record(values: Values) -> tuple[Decision, Writes]:
            prior = values[request_key]
            if prior is not None:
                existing = Decision.model_validate(prior)
                if existing.fingerprint != proposal.fingerprint:
                    raise StateConflict("request ID reused with different content")
                return existing, {}
            if values[decision_key] is not None:
                raise StateConflict("decision ID reused for another request")
            now = clock()
            cluster = values[cluster_key] or {"stats": {}, "pending": {}}
            pending = {
                d: expiry
                for d, expiry in cluster["pending"].items()
                if expiry > now.timestamp()
            }
            if proposal.expires_at > now:
                if len(pending) >= self.max_pending_per_cluster:
                    raise StateUnavailable("cluster pending capacity reached")
                pending[str(proposal.decision_id)] = proposal.expires_at.timestamp()
            stats = ClusterState.model_validate(cluster["stats"])
            latest = max(
                proposal.created_at, stats.last_dispatch_at or proposal.created_at
            )
            payload = proposal.model_dump(mode="json")
            return proposal, {
                request_key: payload,
                decision_key: {"decision": payload},
                cluster_key: {
                    "stats": stats.model_copy(
                        update={"last_dispatch_at": latest}
                    ).model_dump(mode="json"),
                    "pending": pending,
                },
            }

        return await self._update([request_key, decision_key, cluster_key], record)

    async def lookup(
        self, *, tenant_id: str, request_id: str, fingerprint: str
    ) -> Decision | None:
        name = key("request", tenant_id, request_id)
        cells = await self.backend.read([name])
        value = cells[name].value
        if value is None:
            return None
        result = Decision.model_validate(value)
        if result.fingerprint != fingerprint:
            raise StateConflict("request ID reused with different content")
        return result

    @staticmethod
    def _decision(value: dict[str, Any] | None, tenant_id: str) -> Decision:
        if value is None:
            raise StateNotFound("decision not found")
        decision = Decision.model_validate(value["decision"])
        if decision.context.tenant_id != tenant_id:
            raise StateNotFound("decision not found")
        return decision

    @staticmethod
    def _event(
        record: dict[str, Any],
        existing: dict[str, Any] | None,
        kind: str,
        event: Feedback | Observation,
    ) -> dict[str, Any] | None:
        payload = event.model_dump(mode="json")
        digest = fingerprint({"kind": kind, **payload})
        if existing is not None:
            if existing["fingerprint"] != digest:
                raise StateConflict("event ID reused with different content")
            return None
        prior = record.get(kind)
        if prior is not None:
            if kind == "feedback" and prior["payload"]["success"] != payload["success"]:
                raise StateConflict("quality feedback already recorded")
            return None
        return {"fingerprint": digest, "payload": payload}

    async def feedback(self, *, tenant_id: str, event: Feedback) -> bool:
        event = Feedback.model_validate(event.model_dump())
        decision_key = key("decision", str(event.decision_id))
        # The immutable decision supplies the versioned learning key. The update
        # below reads/compares it again, so attribution and dedup remain atomic.
        cells = await self.backend.read([decision_key])
        decision = self._decision(cells[decision_key].value, tenant_id)
        count_key = quality_key(
            decision.context, decision.model_id, decision.model_version
        )
        event_key = key("event", tenant_id, event.event_id)

        def update(values: Values) -> tuple[bool, Writes]:
            record = values[decision_key]
            current = self._decision(record, tenant_id)
            if (event.model_id, event.model_version, event.policy_version) != (
                current.model_id,
                current.model_version,
                current.context.policy_version,
            ):
                raise StateConflict("feedback attribution mismatch")
            assert record is not None
            accepted = self._event(record, values[event_key], "feedback", event)
            if accepted is None:
                return False, {}
            counts = QualityCounts.model_validate(values[count_key] or {}).model_dump()
            counts["successes" if event.success else "failures"] += 1
            return True, {
                decision_key: {**record, "feedback": accepted},
                event_key: accepted,
                count_key: counts,
            }

        return await self._update([decision_key, event_key, count_key], update)

    async def observe(self, *, tenant_id: str, event: Observation) -> bool:
        event = Observation.model_validate(event.model_dump())
        decision_key = key("decision", str(event.decision_id))
        event_key = key("event", tenant_id, event.event_id)
        cluster_key = key("cluster", event.cluster_id)

        def update(values: Values) -> tuple[bool, Writes]:
            record = values[decision_key]
            decision = self._decision(record, tenant_id)
            if (event.cluster_id, event.attempt_id) != (
                decision.cluster_id,
                decision.attempt_id,
            ):
                raise StateConflict("observation attribution mismatch")
            now = clock()
            if event.observed_at > now + timedelta(
                seconds=5
            ) or event.observed_at < decision.created_at - timedelta(seconds=5):
                raise StateConflict("observation timestamp outside decision lifetime")
            assert record is not None
            accepted = self._event(record, values[event_key], "observation", event)
            if accepted is None:
                return False, {}
            cluster = values[cluster_key] or {"stats": {}, "pending": {}}
            pending = {
                d: expiry
                for d, expiry in cluster["pending"].items()
                if d != str(decision.decision_id) and expiry > now.timestamp()
            }
            state = ClusterState.model_validate(cluster["stats"])
            state = self._observe(state, event, now)
            return True, {
                decision_key: {**record, "observation": accepted},
                event_key: accepted,
                cluster_key: {
                    "stats": state.model_dump(mode="json"),
                    "pending": pending,
                },
            }

        return await self._update([decision_key, event_key, cluster_key], update)

    def _observe(
        self, state: ClusterState, event: Observation, now: datetime
    ) -> ClusterState:
        if (
            event.outcome == "client_cancel"
            or (now - event.observed_at).total_seconds() > self.load.stale_after_s
            or (
                state.observed_at is not None and event.observed_at <= state.observed_at
            )
        ):
            return state

        def recent(timestamp: datetime | None) -> bool:
            return (
                timestamp is not None
                and (now - timestamp).total_seconds() <= self.load.stale_after_s
            )

        def ewma(old: float | None, new: float | None) -> float | None:
            if new is None:
                return old
            return (
                new
                if old is None
                else self.load.ewma_alpha * new + (1 - self.load.ewma_alpha) * old
            )

        failed = event.outcome in ("upstream_error", "timeout")
        streak = (
            (state.failure_streak if recent(state.observed_at) else 0) + 1
            if failed
            else 0
        )
        cooldown = (
            now + timedelta(seconds=self.load.cooldown_s)
            if streak >= self.load.failure_threshold
            else (state.cooldown_until if failed else None)
        )
        return state.model_copy(
            update={
                "observed_at": event.observed_at,
                "ttft_ms": ewma(
                    state.ttft_ms
                    if recent(state.ttft_at) or event.ttft_ms is None
                    else None,
                    event.ttft_ms,
                ),
                "ttft_at": event.observed_at
                if event.ttft_ms is not None
                else state.ttft_at,
                "latency_at": event.observed_at
                if event.outcome == "success"
                else state.latency_at,
                "latency_ms": ewma(
                    state.latency_ms
                    if recent(state.latency_at) or event.outcome != "success"
                    else None,
                    event.latency_ms if event.outcome == "success" else None,
                ),
                "error_rate": ewma(
                    state.error_rate if recent(state.observed_at) else None,
                    float(failed),
                ),
                "samples": state.samples + 1,
                "failure_streak": streak,
                "cooldown_until": cooldown,
            }
        )

    async def snapshot(
        self,
        *,
        context: LearningContext,
        models: Sequence[ModelArm],
        cluster_ids: Sequence[str],
    ) -> StateSnapshot:
        model_keys = {
            m.model_id: quality_key(context, m.model_id, m.version) for m in models
        }
        cluster_keys = {c: key("cluster", c) for c in cluster_ids}
        cells = await self.backend.read(
            list(model_keys.values()) + list(cluster_keys.values())
        )
        now = clock()
        learning = {
            m: QualityCounts.model_validate(cells[k].value or {})
            for m, k in model_keys.items()
        }
        clusters = {}
        for cluster_id, name in cluster_keys.items():
            value = cells[name].value or {"stats": {}, "pending": {}}
            state = ClusterState.model_validate(value["stats"])
            clusters[cluster_id] = state.model_copy(
                update={
                    "inflight": sum(
                        expiry > now.timestamp() for expiry in value["pending"].values()
                    )
                }
            )
        return StateSnapshot(learning=learning, clusters=clusters, captured_at=now)

    async def claim_poll(
        self, *, cluster_id: str, owner: str, lease_seconds: float
    ) -> bool:
        if not owner or not 0 < lease_seconds <= 3600:
            raise ValueError("invalid polling lease")
        name = key("lease", cluster_id)

        def claim(values: Values) -> tuple[bool, Writes]:
            current = values[name]
            now = clock().timestamp()
            if current and current["expires_at"] > now and current["owner"] != owner:
                return False, {}
            return True, {name: {"owner": owner, "expires_at": now + lease_seconds}}

        return await self._update([name], claim)

    async def record_active_load(
        self, *, cluster_id: str, owner: str, load: float
    ) -> bool:
        if isinstance(load, bool) or not 0 <= load <= 1:
            raise ValueError("active load must be a finite ratio")
        lease_key, cluster_key = key("lease", cluster_id), key("cluster", cluster_id)

        def update(values: Values) -> tuple[bool, Writes]:
            lease = values[lease_key]
            now = clock()
            if (
                not lease
                or lease["owner"] != owner
                or lease["expires_at"] <= now.timestamp()
            ):
                return False, {}
            cluster = values[cluster_key] or {"stats": {}, "pending": {}}
            state = ClusterState.model_validate(cluster["stats"]).model_copy(
                update={"active_load": float(load), "active_at": now}
            )
            return True, {
                cluster_key: {**cluster, "stats": state.model_dump(mode="json")}
            }

        return await self._update([lease_key, cluster_key], update)

    async def aclose(self) -> None:
        await self.backend.aclose()
