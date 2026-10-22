"""Optimize only the candidates already approved by policy and hard filters."""

import random
from datetime import datetime, timezone

from pydantic import Field

from semantic_router.adaptive.models import Contract, Identifier
from semantic_router.adaptive.state.base import ClusterState, ModelArm, QualityCounts


class NoCandidates(ValueError):
    pass


class LearningConfig(Contract):
    epsilon: float = Field(default=0, ge=0, le=1)


class LoadConfig(Contract):
    ewma_alpha: float = Field(default=0.2, gt=0, le=1)
    stale_after_s: float = Field(default=60, gt=0, le=86400)
    probe_interval_s: float = Field(default=30, gt=0, le=86400)
    failure_threshold: int = Field(default=3, gt=0, le=1000, strict=True)
    cooldown_s: float = Field(default=10, gt=0, le=3600)
    ttft_reference_ms: float = Field(default=500, gt=0)
    latency_reference_ms: float = Field(default=5000, gt=0)
    ttft_weight: float = Field(default=1, ge=0)
    latency_weight: float = Field(default=0.5, ge=0)
    error_weight: float = Field(default=5, ge=0)
    inflight_weight: float = Field(default=1, ge=0)
    active_weight: float = Field(default=1, ge=0)


class ClusterOption(Contract):
    cluster_id: Identifier
    model_id: Identifier
    enabled: bool = True
    prior_ttft_ms: float = Field(default=500, gt=0)
    prior_latency_ms: float = Field(default=5000, gt=0)
    soft_capacity: int = Field(default=8, gt=0, strict=True)


class ModelSelection(Contract):
    model_id: Identifier
    propensity: float = Field(gt=0, le=1)
    scores: dict[str, float]


class ClusterSelection(Contract):
    cluster_id: Identifier
    reason: str
    score: float


def choose_model(
    *,
    candidates: tuple[ModelArm, ...],
    counts: dict[str, QualityCounts],
    config: LearningConfig,
    draw: float | None = None,
) -> ModelSelection:
    if not candidates:
        raise NoCandidates("no qualified models")
    if len({model.model_id for model in candidates}) != len(candidates):
        raise ValueError("duplicate model candidates")
    arms = sorted(candidates, key=lambda model: model.model_id)
    scores = {}
    for model in arms:
        count = counts.get(model.model_id, QualityCounts())
        alpha = model.prior_success + count.successes
        beta = model.prior_failure + count.failures
        scores[model.model_id] = alpha / (alpha + beta)
    best = max(scores, key=lambda name: scores[name])
    draw = random.SystemRandom().random() if draw is None else draw
    if not 0 <= draw < 1:
        raise ValueError("random draw must be in [0, 1)")
    chosen = best
    if draw < config.epsilon:
        chosen = arms[
            min(int(draw / config.epsilon * len(arms)), len(arms) - 1)
        ].model_id
    propensity = config.epsilon / len(arms) + (
        1 - config.epsilon if chosen == best else 0
    )
    return ModelSelection(model_id=chosen, propensity=propensity, scores=scores)


def choose_cluster(
    *,
    model_id: str,
    candidates: tuple[ClusterOption, ...],
    states: dict[str, ClusterState],
    config: LoadConfig,
    now: datetime,
) -> ClusterSelection:
    if now.tzinfo is None:
        raise ValueError("selection clock must include timezone")
    qualified = [
        candidate
        for candidate in candidates
        if candidate.model_id == model_id and candidate.enabled
    ]
    if len({candidate.cluster_id for candidate in qualified}) != len(qualified):
        raise ValueError("duplicate cluster candidates")
    scores = {}
    probes = []

    def recent(timestamp: datetime | None) -> bool:
        return (
            timestamp is not None
            and 0 <= (now - timestamp).total_seconds() <= config.stale_after_s
        )

    for candidate in qualified:
        state = states.get(candidate.cluster_id, ClusterState())
        if state.cooldown_until is not None and state.cooldown_until > now:
            continue
        fresh = (
            state.observed_at is not None
            and 0 <= (now - state.observed_at).total_seconds() <= config.stale_after_s
        )
        ttft = (
            state.ttft_ms
            if recent(state.ttft_at) and state.ttft_ms is not None
            else candidate.prior_ttft_ms
        )
        latency = (
            state.latency_ms
            if recent(state.latency_at) and state.latency_ms is not None
            else candidate.prior_latency_ms
        )
        active = (
            state.active_load
            if state.active_load is not None
            and state.active_at is not None
            and 0 <= (now - state.active_at).total_seconds() <= config.stale_after_s
            else 0
        )
        score = (
            config.ttft_weight * ttft / config.ttft_reference_ms
            + config.latency_weight * latency / config.latency_reference_ms
            + config.error_weight * (state.error_rate if fresh else 0)
            + config.inflight_weight * state.inflight / candidate.soft_capacity
            + config.active_weight * active
        )
        scores[candidate.cluster_id] = score
        if (
            not fresh
            and state.inflight == 0
            and (
                state.last_dispatch_at is None
                or (now - state.last_dispatch_at).total_seconds()
                >= config.probe_interval_s
            )
        ):
            probes.append(
                (
                    state.last_dispatch_at or datetime.min.replace(tzinfo=timezone.utc),
                    candidate.cluster_id,
                )
            )
    if not scores:
        raise NoCandidates("no available clusters")
    if probes:
        selected = min(probes)[1]
        return ClusterSelection(
            cluster_id=selected, reason="probe", score=scores[selected]
        )
    selected = min(scores, key=lambda name: (scores[name], name))
    return ClusterSelection(cluster_id=selected, reason="load", score=scores[selected])
