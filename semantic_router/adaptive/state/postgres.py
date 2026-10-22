"""Transactional state shared by every decision-service replica.

Each transaction owns its connection. A per-replica semaphore caps connections;
deployments can put a transaction-mode PgBouncer endpoint in front of PostgreSQL.
All leases, event attribution and updates are enforced by the database.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
import re
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

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


def _context(context: LearningContext) -> tuple[str, ...]:
    return (
        context.tenant_id,
        context.policy_version,
        context.feature_version,
        context.category,
        context.difficulty,
    )


class PostgresState:
    def __init__(
        self,
        *,
        dsn: str,
        schema: str = "adaptive",
        load: LoadConfig | None = None,
        concurrency: int = 16,
        timeout_ms: int = 2000,
    ):
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", schema):
            raise ValueError("invalid state schema")
        if not 1 <= concurrency <= 1024 or not 1 <= timeout_ms <= 60000:
            raise ValueError("invalid database limits")
        self.load = (load or LoadConfig()).model_copy(deep=True)
        self.schema = schema
        self._dsn, self._timeout_ms = dsn, timeout_ms
        self._slots = asyncio.Semaphore(concurrency)
        self._closed = False

    @asynccontextmanager
    async def _transaction(
        self, *, initialize: bool = False
    ) -> AsyncIterator[psycopg.AsyncConnection[dict[str, Any]]]:
        if self._closed:
            raise StateUnavailable("shared state closed")
        try:
            await asyncio.wait_for(
                self._slots.acquire(), timeout=self._timeout_ms / 1000
            )
        except asyncio.TimeoutError:
            raise StateUnavailable("shared state busy") from None
        try:
            async with await psycopg.AsyncConnection.connect(
                self._dsn,
                row_factory=dict_row,
                connect_timeout=max(1, math.ceil(self._timeout_ms / 1000)),
                options=f"-c statement_timeout={self._timeout_ms} -c lock_timeout={self._timeout_ms}",
            ) as conn:
                if not initialize:
                    await conn.execute(
                        sql.SQL("SET LOCAL search_path TO {}, pg_catalog").format(
                            sql.Identifier(self.schema)
                        )
                    )
                yield conn
        except psycopg.Error:
            raise StateUnavailable("shared state unavailable") from None
        finally:
            self._slots.release()

    async def initialize(self) -> None:
        async with self._transaction(initialize=True) as conn:
            lock = int.from_bytes(
                hashlib.sha256(self.schema.encode()).digest()[:8], "big", signed=True
            )
            await conn.execute("SELECT pg_advisory_xact_lock(%s)", (lock,))
            await conn.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(self.schema)
                )
            )
            await conn.execute(
                sql.SQL("SET LOCAL search_path TO {}, pg_catalog").format(
                    sql.Identifier(self.schema)
                )
            )
            await conn.execute(
                Path(__file__)
                .with_name("migrations")
                .joinpath("001_initial.sql")
                .read_text(),
                prepare=False,
            )
            row = await (
                await conn.execute("SELECT version FROM schema_version WHERE singleton")
            ).fetchone()
            if row is None or row["version"] != 1:
                raise StateUnavailable("unsupported state schema version")

    async def ready(self) -> bool:
        async with self._transaction() as conn:
            row = await (
                await conn.execute("SELECT version FROM schema_version WHERE singleton")
            ).fetchone()
            return row is not None and row["version"] == 1

    async def record_decision(self, *, proposal: Decision) -> Decision:
        proposal = Decision.model_validate(proposal.model_dump())
        async with self._transaction() as conn:
            await conn.execute(
                """INSERT INTO decisions
                (decision_id, tenant_id, request_id, fingerprint, cluster_id, created_at, expires_at, payload)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING""",
                (
                    proposal.decision_id,
                    proposal.context.tenant_id,
                    proposal.request_id,
                    proposal.fingerprint,
                    proposal.cluster_id,
                    proposal.created_at,
                    proposal.expires_at,
                    Jsonb(proposal.model_dump(mode="json")),
                ),
            )
            row = await (
                await conn.execute(
                    "SELECT fingerprint, payload FROM decisions WHERE tenant_id=%s AND request_id=%s",
                    (proposal.context.tenant_id, proposal.request_id),
                )
            ).fetchone()
            if row is None:
                raise StateConflict("decision ID reused for another request")
            if row["fingerprint"] != proposal.fingerprint:
                raise StateConflict("request ID reused with different content")
            return Decision.model_validate(row["payload"])

    async def lookup(
        self, *, tenant_id: str, request_id: str, fingerprint: str
    ) -> Decision | None:
        async with self._transaction() as conn:
            row = await (
                await conn.execute(
                    "SELECT fingerprint, payload FROM decisions WHERE tenant_id=%s AND request_id=%s",
                    (tenant_id, request_id),
                )
            ).fetchone()
            if row is None:
                return None
            if row["fingerprint"] != fingerprint:
                raise StateConflict("request ID reused with different content")
            return Decision.model_validate(row["payload"])

    async def _decision(
        self,
        conn: psycopg.AsyncConnection[dict[str, Any]],
        tenant_id: str,
        decision_id: UUID,
    ) -> Decision:
        row = await (
            await conn.execute(
                "SELECT payload FROM decisions WHERE tenant_id=%s AND decision_id=%s FOR UPDATE",
                (tenant_id, decision_id),
            )
        ).fetchone()
        if row is None:
            raise StateNotFound("decision not found")
        return Decision.model_validate(row["payload"])

    async def _event(
        self,
        conn: psycopg.AsyncConnection[dict[str, Any]],
        tenant_id: str,
        kind: str,
        event: Observation | Feedback,
    ) -> bool:
        payload = event.model_dump(mode="json")
        digest = fingerprint({"kind": kind, **payload})
        inserted = await (
            await conn.execute(
                """INSERT INTO events
            (tenant_id,event_id,kind,decision_id,fingerprint,payload) VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING RETURNING event_id""",
                (
                    tenant_id,
                    event.event_id,
                    kind,
                    event.decision_id,
                    digest,
                    Jsonb(payload),
                ),
            )
        ).fetchone()
        if inserted is not None:
            return True
        existing = await (
            await conn.execute(
                "SELECT fingerprint FROM events WHERE tenant_id=%s AND event_id=%s",
                (tenant_id, event.event_id),
            )
        ).fetchone()
        if existing is not None:
            if existing["fingerprint"] != digest:
                raise StateConflict("event ID reused with different content")
            return False
        # First terminal observation wins across end/reset callbacks. A quality
        # label can be repeated with a new ID, but cannot be silently revised.
        if kind == "feedback":
            prior = await (
                await conn.execute(
                    "SELECT payload FROM events WHERE kind=%s AND decision_id=%s",
                    (kind, event.decision_id),
                )
            ).fetchone()
            if prior is None or prior["payload"]["success"] != payload["success"]:
                raise StateConflict("quality feedback already recorded")
        return False

    async def feedback(self, *, tenant_id: str, event: Feedback) -> bool:
        event = Feedback.model_validate(event.model_dump())
        async with self._transaction() as conn:
            decision = await self._decision(conn, tenant_id, event.decision_id)
            if (event.model_id, event.model_version, event.policy_version) != (
                decision.model_id,
                decision.model_version,
                decision.context.policy_version,
            ):
                raise StateConflict("feedback attribution mismatch")
            if not await self._event(conn, tenant_id, "feedback", event):
                return False
            await conn.execute(
                """INSERT INTO quality_counts
                (tenant_id,policy_version,feature_version,category,difficulty,model_id,model_version,successes,failures)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (tenant_id,policy_version,feature_version,category,difficulty,model_id,model_version)
                DO UPDATE SET successes=quality_counts.successes+EXCLUDED.successes,
                    failures=quality_counts.failures+EXCLUDED.failures""",
                (
                    *_context(decision.context),
                    decision.model_id,
                    decision.model_version,
                    int(event.success),
                    int(not event.success),
                ),
            )
            return True

    async def _cluster(
        self, conn: psycopg.AsyncConnection[dict[str, Any]], cluster_id: str
    ) -> ClusterState:
        await conn.execute(
            "INSERT INTO cluster_stats (cluster_id,payload) VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (cluster_id, Jsonb(ClusterState().model_dump(mode="json"))),
        )
        row = await (
            await conn.execute(
                "SELECT payload FROM cluster_stats WHERE cluster_id=%s FOR UPDATE",
                (cluster_id,),
            )
        ).fetchone()
        assert row is not None
        return ClusterState.model_validate(row["payload"])

    async def _save_cluster(
        self,
        conn: psycopg.AsyncConnection[dict[str, Any]],
        cluster_id: str,
        state: ClusterState,
    ) -> None:
        await conn.execute(
            "UPDATE cluster_stats SET payload=%s WHERE cluster_id=%s",
            (Jsonb(state.model_dump(mode="json")), cluster_id),
        )

    async def observe(self, *, tenant_id: str, event: Observation) -> bool:
        event = Observation.model_validate(event.model_dump())
        async with self._transaction() as conn:
            decision = await self._decision(conn, tenant_id, event.decision_id)
            if (event.cluster_id, event.attempt_id) != (
                decision.cluster_id,
                decision.attempt_id,
            ):
                raise StateConflict("observation attribution mismatch")
            clock = await (
                await conn.execute("SELECT clock_timestamp() AS now")
            ).fetchone()
            assert clock is not None
            now: datetime = clock["now"]
            if event.observed_at > now + timedelta(
                seconds=5
            ) or event.observed_at < decision.created_at - timedelta(seconds=5):
                raise StateConflict("observation timestamp outside decision lifetime")
            if not await self._event(conn, tenant_id, "observation", event):
                return False
            await conn.execute(
                "UPDATE decisions SET terminal=true WHERE decision_id=%s",
                (decision.decision_id,),
            )
            if (
                event.outcome == "client_cancel"
                or (now - event.observed_at).total_seconds() > self.load.stale_after_s
            ):
                return True
            state = await self._cluster(conn, event.cluster_id)
            if state.observed_at is not None and event.observed_at <= state.observed_at:
                return True
            alpha = self.load.ewma_alpha

            def recent(timestamp: datetime | None) -> bool:
                return (
                    timestamp is not None
                    and (now - timestamp).total_seconds() <= self.load.stale_after_s
                )

            def ewma(old: float | None, new: float | None) -> float | None:
                if new is None:
                    return old
                return new if old is None else alpha * new + (1 - alpha) * old

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
            updated = state.model_copy(
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
            await self._save_cluster(conn, event.cluster_id, updated)
            return True

    async def snapshot(
        self,
        *,
        context: LearningContext,
        models: Sequence[ModelArm],
        cluster_ids: Sequence[str],
    ) -> StateSnapshot:
        learning = {model.model_id: QualityCounts() for model in models}
        clusters = {cluster: ClusterState() for cluster in cluster_ids}
        versions = {model.model_id: model.version for model in models}
        async with self._transaction() as conn:
            await conn.execute(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
            clock = await (
                await conn.execute("SELECT transaction_timestamp() AS now")
            ).fetchone()
            assert clock is not None
            rows = await (
                await conn.execute(
                    """SELECT model_id,model_version,successes,failures FROM quality_counts
                WHERE tenant_id=%s AND policy_version=%s AND feature_version=%s AND category=%s AND difficulty=%s
                AND model_id=ANY(%s)""",
                    (*_context(context), list(versions)),
                )
            ).fetchall()
            for row in rows:
                if versions[row["model_id"]] == row["model_version"]:
                    learning[row["model_id"]] = QualityCounts(
                        successes=row["successes"], failures=row["failures"]
                    )
            rows = await (
                await conn.execute(
                    "SELECT cluster_id,payload FROM cluster_stats WHERE cluster_id=ANY(%s)",
                    (list(cluster_ids),),
                )
            ).fetchall()
            for row in rows:
                clusters[row["cluster_id"]] = ClusterState.model_validate(
                    row["payload"]
                )
            pending = await (
                await conn.execute(
                    """SELECT cluster_id,count(*) AS inflight FROM decisions
                WHERE NOT terminal AND expires_at>transaction_timestamp() AND cluster_id=ANY(%s) GROUP BY cluster_id""",
                    (list(cluster_ids),),
                )
            ).fetchall()
            for row in pending:
                clusters[row["cluster_id"]] = clusters[row["cluster_id"]].model_copy(
                    update={"inflight": row["inflight"]}
                )
            latest = await (
                await conn.execute(
                    """SELECT DISTINCT ON (cluster_id) cluster_id,created_at FROM decisions
                WHERE cluster_id=ANY(%s) ORDER BY cluster_id,created_at DESC""",
                    (list(cluster_ids),),
                )
            ).fetchall()
            for row in latest:
                clusters[row["cluster_id"]] = clusters[row["cluster_id"]].model_copy(
                    update={"last_dispatch_at": row["created_at"]}
                )
            return StateSnapshot(
                learning=learning, clusters=clusters, captured_at=clock["now"]
            )

    async def claim_poll(
        self, *, cluster_id: str, owner: str, lease_seconds: float
    ) -> bool:
        if not owner or not 0 < lease_seconds <= 3600:
            raise ValueError("invalid polling lease")
        async with self._transaction() as conn:
            row = await (
                await conn.execute(
                    """INSERT INTO polling_leases (cluster_id,owner,expires_at)
                VALUES (%s,%s,clock_timestamp()+(%s * interval '1 second'))
                ON CONFLICT (cluster_id) DO UPDATE SET owner=EXCLUDED.owner,expires_at=EXCLUDED.expires_at
                WHERE polling_leases.expires_at<=clock_timestamp() OR polling_leases.owner=EXCLUDED.owner
                RETURNING owner""",
                    (cluster_id, owner, lease_seconds),
                )
            ).fetchone()
            return row is not None

    async def record_active_load(
        self, *, cluster_id: str, owner: str, load: float
    ) -> bool:
        if isinstance(load, bool) or not 0 <= load <= 1:
            raise ValueError("active load must be a finite ratio")
        async with self._transaction() as conn:
            lease = await (
                await conn.execute(
                    """SELECT owner FROM polling_leases
                WHERE cluster_id=%s AND owner=%s AND expires_at>clock_timestamp() FOR UPDATE""",
                    (cluster_id, owner),
                )
            ).fetchone()
            if lease is None:
                return False
            state = await self._cluster(conn, cluster_id)
            clock = await (
                await conn.execute("SELECT clock_timestamp() AS now")
            ).fetchone()
            assert clock is not None
            await self._save_cluster(
                conn,
                cluster_id,
                state.model_copy(
                    update={"active_load": float(load), "active_at": clock["now"]}
                ),
            )
            return True

    async def aclose(self) -> None:
        self._closed = True
