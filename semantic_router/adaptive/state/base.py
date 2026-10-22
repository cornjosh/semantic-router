"""Durable event contracts. Quality labels and operational outcomes are distinct."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import AwareDatetime, Field, JsonValue, field_validator, model_validator

from semantic_router.adaptive.models import Contract, Identifier, finite_json


class StateConflict(ValueError):
    """An idempotency key or event attribution conflicts with persisted state."""


class StateNotFound(ValueError):
    """No decision exists for this authenticated tenant."""


class StateUnavailable(RuntimeError):
    """Shared state is unavailable; callers must not fall back to local state."""


def fingerprint(value: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


class LearningContext(Contract):
    tenant_id: Identifier
    policy_version: Identifier
    feature_version: Identifier
    category: Identifier = "unknown"
    difficulty: Identifier = "unknown"


class ModelArm(Contract):
    model_id: Identifier
    version: Identifier
    prior_success: float = Field(default=1, gt=0, le=1000000)
    prior_failure: float = Field(default=1, gt=0, le=1000000)


class QualityCounts(Contract):
    successes: int = Field(default=0, ge=0, strict=True)
    failures: int = Field(default=0, ge=0, strict=True)


class Decision(Contract):
    decision_id: UUID = Field(default_factory=uuid4)
    attempt_id: Identifier = "initial"
    request_id: Identifier
    fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    context: LearningContext
    model_id: Identifier
    model_version: Identifier
    cluster_id: Identifier
    candidates: tuple[ModelArm, ...] = Field(min_length=1, max_length=1024)
    propensity: float = Field(gt=0, le=1)
    created_at: AwareDatetime
    expires_at: AwareDatetime
    reason: str = Field(default="policy", max_length=256)
    signal_status: dict[str, str] = Field(default_factory=dict)
    route_id: Identifier | None = None
    upstream_model: Identifier | None = None
    accounting: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("accounting")
    @classmethod
    def finite_accounting(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        finite_json(value)
        return value

    @model_validator(mode="after")
    def consistent(self) -> Decision:
        ids = [model.model_id for model in self.candidates]
        if len(set(ids)) != len(ids) or not any(
            model.model_id == self.model_id and model.version == self.model_version
            for model in self.candidates
        ):
            raise ValueError("selected model must be one of the unique candidates")
        if not 0 < (self.expires_at - self.created_at).total_seconds() <= 86400:
            raise ValueError(
                "decision accounting lifetime must be in (0, 86400] seconds"
            )
        return self


class Observation(Contract):
    event_id: Identifier
    decision_id: UUID
    attempt_id: Identifier
    cluster_id: Identifier
    observed_at: AwareDatetime
    outcome: Literal["success", "upstream_error", "timeout", "client_cancel"]
    ttft_ms: float | None = Field(default=None, ge=0, le=86400000, strict=True)
    latency_ms: float = Field(ge=0, le=86400000, strict=True)

    @model_validator(mode="after")
    def ordered_timings(self) -> Observation:
        if self.ttft_ms is not None and self.ttft_ms > self.latency_ms:
            raise ValueError("TTFT cannot exceed total latency")
        return self


class Feedback(Contract):
    event_id: Identifier
    decision_id: UUID
    model_id: Identifier
    model_version: Identifier
    policy_version: Identifier
    success: bool = Field(strict=True)


class ClusterState(Contract):
    observed_at: AwareDatetime | None = None
    ttft_at: AwareDatetime | None = None
    latency_at: AwareDatetime | None = None
    ttft_ms: float | None = Field(default=None, ge=0)
    latency_ms: float | None = Field(default=None, ge=0)
    error_rate: float = Field(default=0, ge=0, le=1)
    samples: int = Field(default=0, ge=0)
    failure_streak: int = Field(default=0, ge=0)
    cooldown_until: AwareDatetime | None = None
    inflight: int = Field(default=0, ge=0)
    last_dispatch_at: AwareDatetime | None = None
    active_load: float | None = Field(default=None, ge=0, le=1)
    active_at: AwareDatetime | None = None

    @model_validator(mode="before")
    @classmethod
    def initial_metric_times(cls, value: Any) -> Any:
        if isinstance(value, dict):
            value = dict(value)
            for metric in ("ttft", "latency"):
                if value.get(f"{metric}_ms") is not None:
                    value.setdefault(f"{metric}_at", value.get("observed_at"))
        return value


class StateSnapshot(Contract):
    learning: dict[str, QualityCounts]
    clusters: dict[str, ClusterState]
    captured_at: datetime
