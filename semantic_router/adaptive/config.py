"""Trusted configuration; signal definitions contain no routing destinations."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, JsonValue, model_validator

from semantic_router.adaptive.models import Contract, Name, finite_json


class SignalSpec(Contract):
    name: Name
    kind: Name
    enabled: bool = True
    required: bool = False
    stage: int = Field(default=1, ge=1, strict=True)
    timeout_ms: int = Field(default=500, gt=0, le=60000, strict=True)
    parameters: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def valid_signal(self) -> SignalSpec:
        if self.name == "request":
            raise ValueError("request is a reserved built-in signal")
        if self.required and not self.enabled:
            raise ValueError("a required signal cannot be disabled")
        finite_json(self.parameters)
        return self


class Predicate(Contract):
    signal: Name
    path: str = Field(
        min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$"
    )
    op: Literal["eq", "in", "gte", "lte", "exists"]
    value: JsonValue = None

    @model_validator(mode="after")
    def typed_operand(self) -> Predicate:
        finite_json(self.value)
        if self.op in ("gte", "lte") and type(self.value) not in (int, float):
            raise ValueError("numeric comparison requires a numeric operand")
        if self.op == "in" and not isinstance(self.value, list):
            raise ValueError("in requires an array operand")
        if self.op == "exists" and type(self.value) is not bool:
            raise ValueError("exists requires a boolean operand")
        return self


class PolicyRule(Contract):
    name: Name
    when: tuple[Predicate, ...] = ()
    models: tuple[str, ...] = Field(min_length=1)
    shortcut: bool = False

    @model_validator(mode="after")
    def unique_models(self) -> PolicyRule:
        if any(not name.strip() for name in self.models) or len(
            set(self.models)
        ) != len(self.models):
            raise ValueError("rule model IDs must be nonempty and unique")
        return self


class PolicyConfig(Contract):
    rules: tuple[PolicyRule, ...] = ()
    fallback_models: tuple[str, ...] = ()

    @model_validator(mode="after")
    def unique_rules(self) -> PolicyConfig:
        if len({rule.name for rule in self.rules}) != len(self.rules):
            raise ValueError("duplicate policy rule names")
        if any(not name.strip() for name in self.fallback_models) or len(
            set(self.fallback_models)
        ) != len(self.fallback_models):
            raise ValueError("fallback model IDs must be nonempty and unique")
        return self


class PipelineConfig(Contract):
    signals: tuple[SignalSpec, ...] = ()
    policy: PolicyConfig = Field(default_factory=PolicyConfig)
    timeout_ms: int = Field(default=1000, gt=0, le=60000, strict=True)

    @model_validator(mode="after")
    def signal_references(self) -> PipelineConfig:
        names = {signal.name for signal in self.signals}
        if len(names) != len(self.signals):
            raise ValueError("duplicate signal names")
        names.add("request")
        for rule in self.policy.rules:
            for predicate in rule.when:
                if predicate.signal not in names:
                    raise ValueError(f"unknown signal reference: {predicate.signal}")
        return self

    def validate_models(self, *, model_ids: set[str]) -> None:
        configured = set(self.policy.fallback_models)
        for rule in self.policy.rules:
            configured.update(rule.models)
        if missing := configured - model_ids:
            raise ValueError(f"unknown model IDs: {', '.join(sorted(missing))}")
