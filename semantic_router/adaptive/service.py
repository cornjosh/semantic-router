"""Compose constraints, signal/policy evaluation, selection and durable state."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from semantic_router.adaptive.constraints import account
from semantic_router.adaptive.models import RoutingRequest
from semantic_router.adaptive.pipeline import SignalPipeline
from semantic_router.adaptive.providers import ProviderRegistry
from semantic_router.adaptive.selection import (
    NoCandidates,
    choose_cluster,
    choose_model,
)
from semantic_router.adaptive.service_config import ServiceConfig
from semantic_router.adaptive.signals.structural import StructuralSignal
from semantic_router.adaptive.state.base import (
    Decision,
    LearningContext,
    ModelArm,
    fingerprint,
)
from semantic_router.adaptive.state.factory import open_state
from semantic_router.adaptive.telemetry import TelemetryPoller


class ServiceError(ValueError):
    def __init__(self, code: str, status: int = 422):
        super().__init__(code)
        self.code, self.status = code, status


class RoutingService:
    def __init__(self, *, config: ServiceConfig):
        self.config = config.model_copy(deep=True)
        self.state = open_state(**self.config.state.model_dump(), load=self.config.load)
        self.providers = ProviderRegistry(providers=self.config.providers)
        self.pipeline = SignalPipeline(
            config=self.config.pipeline, registry=self.providers.factories
        )
        self.models = {model.model_id: model for model in self.config.models}
        self.clusters = {
            cluster.cluster_id: cluster for cluster in self.config.clusters
        }
        self.poller = TelemetryPoller(store=self.state, targets=self.config.telemetry)
        self._stop = asyncio.Event()
        self._polling: asyncio.Task[None] | None = None

    async def start(self) -> None:
        try:
            await self.state.initialize()
            if any(target.enabled for target in self.config.telemetry):
                self._polling = asyncio.create_task(self.poller.run(stop=self._stop))
        except BaseException:
            await self.aclose()
            raise

    async def aclose(self) -> None:
        self._stop.set()
        if self._polling is not None:
            await self._polling
        await self.poller.aclose()
        await self.pipeline.aclose()
        await self.providers.aclose()
        await self.state.aclose()

    def _replay(
        self, decision: Decision, eligible: dict[str, dict[str, Any]]
    ) -> Decision:
        cluster = self.clusters.get(decision.cluster_id)
        model = self.models.get(decision.model_id)
        if (
            decision.model_id not in eligible
            or model is None
            or not cluster
            or not cluster.enabled
            or cluster.model_id != model.model_id
            or model.version != decision.model_version
            or cluster.route_id != decision.route_id
            or cluster.upstream_model != decision.upstream_model
            or decision.context.policy_version != self.config.policy_version
            or decision.context.feature_version != self.config.feature_version
        ):
            raise ServiceError("decision_config_changed", 409)
        return decision

    async def route(self, *, request: RoutingRequest) -> Decision:
        request = RoutingRequest.model_validate(request.model_dump())
        allowed = self.config.tenants.get(request.tenant_id)
        if allowed is None:
            raise ServiceError("tenant_not_allowed", 403)
        if request.model != "auto" and request.model not in allowed:
            raise ServiceError("model_not_allowed", 403)
        facts = await StructuralSignal().extract(request=request)
        eligible = {}
        for name in allowed:
            if request.model not in ("auto", name):
                continue
            accounting = account(facts=facts, model=self.models[name])
            if accounting is not None and any(
                c.enabled and c.model_id == name for c in self.config.clusters
            ):
                eligible[name] = accounting
        if not eligible:
            raise ServiceError("no_capable_model")
        digest = fingerprint(request.model_dump(mode="json"))
        existing = await self.state.lookup(
            tenant_id=request.tenant_id,
            request_id=request.request_id,
            fingerprint=digest,
        )
        if existing is not None:
            return self._replay(existing, eligible)
        result = await self.pipeline.run(request=request)
        qualified = set(result.match.models) & eligible.keys()
        if not qualified:
            raise ServiceError("policy_has_no_capable_model")
        context_values = {}
        for name, field in self.config.context_fields.items():
            signal = result.signals.get(field.signal)
            value: Any = signal.value if signal and signal.status == "ok" else None
            for segment in field.path.split("."):
                value = value.get(segment) if isinstance(value, dict) else None
            # Provider labels must satisfy the durable context contract; absent
            # or unavailable signals remain explicit unknown buckets.
            if isinstance(value, str):
                context_values[name] = value
        context = LearningContext(
            tenant_id=request.tenant_id,
            policy_version=self.config.policy_version,
            feature_version=self.config.feature_version,
            **context_values,
        )
        arms = tuple(
            ModelArm.model_validate(
                self.models[name].model_dump(include=set(ModelArm.model_fields))
            )
            for name in sorted(qualified)
        )
        options = tuple(
            c for c in self.config.clusters if c.model_id in qualified and c.enabled
        )
        snapshot = await self.state.snapshot(
            context=context, models=arms, cluster_ids=[c.cluster_id for c in options]
        )
        cluster_choices = {}
        for arm in arms:
            try:
                cluster_choices[arm.model_id] = choose_cluster(
                    model_id=arm.model_id,
                    candidates=options,
                    states=snapshot.clusters,
                    config=self.config.load,
                    now=snapshot.captured_at,
                )
            except NoCandidates:
                pass
        available = tuple(arm for arm in arms if arm.model_id in cluster_choices)
        if not available:
            raise ServiceError("no_healthy_cluster", 503)
        chosen = choose_model(
            candidates=available, counts=snapshot.learning, config=self.config.learning
        )
        selected = cluster_choices[chosen.model_id]
        cluster = self.clusters[selected.cluster_id]
        now = datetime.now(timezone.utc)
        proposal = Decision(
            request_id=request.request_id,
            fingerprint=digest,
            context=context,
            model_id=chosen.model_id,
            model_version=self.models[chosen.model_id].version,
            cluster_id=cluster.cluster_id,
            candidates=available,
            propensity=chosen.propensity,
            created_at=now,
            expires_at=now + timedelta(seconds=self.config.decision_ttl_s),
            reason=f"{result.match.rule or 'fallback'}:{selected.reason}",
            signal_status={
                **{n: s.status for n, s in result.signals.items()},
                **{n: "skipped" for n in result.skipped_signals},
            },
            route_id=cluster.route_id,
            upstream_model=cluster.upstream_model,
            accounting=eligible[chosen.model_id],
        )
        persisted = await self.state.record_decision(proposal=proposal)
        return self._replay(persisted, eligible)
