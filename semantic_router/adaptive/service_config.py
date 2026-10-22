"""Trusted service catalog/auth configuration, distinct from facts and policy."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from semantic_router.adaptive.config import PipelineConfig
from semantic_router.adaptive.models import Contract, Identifier, Name
from semantic_router.adaptive.providers import ProviderConfig, ProviderRegistry
from semantic_router.adaptive.selection import ClusterOption, LearningConfig, LoadConfig
from semantic_router.adaptive.state.base import ModelArm
from semantic_router.adaptive.telemetry import TelemetryTarget


class AccountingConfig(Contract):
    # Generic chat templates do not have an exact universal tokenizer. A deployer
    # must explicitly opt into a bound and supply media/framing allowances.
    mode: Literal["exact_required", "utf8_upper_bound"] = "exact_required"
    framing_tokens: int = Field(default=64, ge=0, le=100000, strict=True)
    tokens_per_message: int = Field(default=32, ge=0, le=10000, strict=True)
    media_tokens: dict[Literal["image", "audio", "video"], int] = Field(
        default_factory=dict
    )

    @model_validator(mode="after")
    def bounded_media(self) -> AccountingConfig:
        if any(
            type(v) is not int or not 1 <= v <= 1000000
            for v in self.media_tokens.values()
        ):
            raise ValueError("media token allowances must be positive bounded integers")
        return self


class ModelConfig(ModelArm):
    enabled: bool = True
    tools: bool = False
    modalities: tuple[Literal["text", "image", "audio", "video"], ...] = ("text",)
    response_formats: tuple[Literal["text", "json_object", "json_schema"], ...] = (
        "text",
    )
    max_context_tokens: int = Field(gt=0, le=10000000, strict=True)
    max_output_tokens: int = Field(gt=0, le=1000000, strict=True)
    accounting: AccountingConfig = Field(default_factory=AccountingConfig)


class ClusterConfig(ClusterOption):
    route_id: Name
    upstream_model: Identifier


class StateConfig(Contract):
    backend: Literal["sqlite", "etcd"] = "sqlite"
    path: str = "/data/adaptive.db"
    endpoints: tuple[str, ...] = ()
    prefix: str = "/semantic-router/"
    timeout_ms: int = Field(default=2000, gt=0, le=60000, strict=True)
    max_pending_per_cluster: int = Field(default=4096, gt=0, le=4096, strict=True)
    token_env: str | None = None
    ca_file: str | None = None
    cert_file: str | None = None
    key_file: str | None = None

    @model_validator(mode="after")
    def explicit_backend(self) -> StateConfig:
        if self.backend == "etcd" and not self.endpoints:
            raise ValueError("etcd endpoints are required")
        if self.backend == "sqlite" and (
            self.endpoints
            or self.token_env
            or self.ca_file
            or self.cert_file
            or self.key_file
        ):
            raise ValueError("etcd connection options require etcd backend")
        return self


Role = Literal["route", "observe", "feedback", "metrics"]


class Principal(Contract):
    token_env: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    tenant_id: Identifier
    roles: tuple[Role, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def independent_feedback(self) -> Principal:
        if "feedback" in self.roles and (
            "route" in self.roles or "observe" in self.roles
        ):
            raise ValueError("quality feedback requires a separate credential")
        return self


class ContextField(Contract):
    signal: Name
    path: str = Field(pattern=r"^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$")


class ServiceConfig(Contract):
    policy_version: Identifier
    feature_version: Identifier
    state: StateConfig = Field(default_factory=StateConfig)
    pipeline: PipelineConfig
    providers: dict[Name, ProviderConfig] = Field(default_factory=dict)
    models: tuple[ModelConfig, ...] = Field(min_length=1, max_length=32)
    clusters: tuple[ClusterConfig, ...] = Field(min_length=1, max_length=32)
    tenants: dict[Identifier, tuple[Identifier, ...]] = Field(min_length=1)
    principals: tuple[Principal, ...] = Field(min_length=1)
    context_fields: dict[Literal["category", "difficulty"], ContextField] = Field(
        default_factory=dict
    )
    learning: LearningConfig = Field(default_factory=LearningConfig)
    load: LoadConfig = Field(default_factory=LoadConfig)
    telemetry: tuple[TelemetryTarget, ...] = ()
    request_timeout_ms: int = Field(default=5000, gt=0, le=60000, strict=True)
    max_request_bytes: int = Field(default=262144, ge=1024, le=4194304, strict=True)
    concurrency: int = Field(default=64, gt=0, le=4096, strict=True)
    decision_ttl_s: int = Field(default=300, gt=0, le=86400, strict=True)

    @model_validator(mode="after")
    def references(self) -> ServiceConfig:
        models = {model.model_id for model in self.models}
        clusters = {cluster.cluster_id for cluster in self.clusters}
        if len(models) != len(self.models) or len(clusters) != len(self.clusters):
            raise ValueError("duplicate model/cluster ID")
        if any(c.model_id not in models for c in self.clusters):
            raise ValueError("unknown cluster model")
        if any(not ids or not set(ids) <= models for ids in self.tenants.values()):
            raise ValueError("unknown tenant model")
        if any(
            principal.tenant_id not in self.tenants for principal in self.principals
        ):
            raise ValueError("unknown principal tenant")
        if len({p.token_env for p in self.principals}) != len(self.principals):
            raise ValueError("credential environment references must be distinct")
        if any(t.cluster_id not in clusters for t in self.telemetry) or len(
            {t.cluster_id for t in self.telemetry}
        ) != len(self.telemetry):
            raise ValueError("unknown or duplicate telemetry cluster")
        names = {s.name for s in self.pipeline.signals} | {"request"}
        if any(field.signal not in names for field in self.context_fields.values()):
            raise ValueError("unknown learning context signal")
        self.pipeline.validate_models(model_ids=models)
        ProviderRegistry(providers=self.providers).validate(
            signals=self.pipeline.signals
        )
        return self
