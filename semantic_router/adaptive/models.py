"""Facts exchanged between request ingestion, extraction and policy evaluation."""

from __future__ import annotations

import math
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_validator,
    model_validator,
)

Identifier = Annotated[
    str, Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:/-]*$")
]
Name = Annotated[
    str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z][A-Za-z0-9_-]*$")
]
SignalError = Literal["timeout", "provider_error", "invalid_result", "not_configured"]


def finite_json(value: JsonValue) -> JsonValue:
    """JSON numbers used for routing must be finite, including inside containers."""
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("non-finite JSON number")
    if isinstance(value, dict):
        for item in value.values():
            finite_json(item)
    elif isinstance(value, list):
        for item in value:
            finite_json(item)
    return value


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class Message(Contract):
    role: Literal["system", "developer", "user", "assistant", "tool"]
    content: str | tuple[dict[str, JsonValue], ...] | None = None
    name: str | None = None
    tool_call_id: str | None = None
    tool_calls: tuple[dict[str, JsonValue], ...] = ()

    @field_validator("tool_calls", mode="before")
    @classmethod
    def optional_tool_calls(cls, value: object) -> object:
        return () if value is None else value


class RoutingRequest(Contract):
    request_id: Identifier
    tenant_id: Identifier
    model: str = Field(default="auto", min_length=1, max_length=256)
    messages: tuple[Message, ...] = Field(min_length=1, max_length=128)
    tools: tuple[dict[str, JsonValue], ...] = Field(default=(), max_length=128)
    tool_choice: str | dict[str, JsonValue] | None = None
    response_format: dict[str, JsonValue] | None = None
    max_tokens: int | None = Field(default=None, gt=0, strict=True)
    max_completion_tokens: int | None = Field(default=None, gt=0, strict=True)
    stream: bool = False

    @field_validator("tools", mode="before")
    @classmethod
    def optional_tools(cls, value: object) -> object:
        return () if value is None else value

    @model_validator(mode="after")
    def unambiguous_limit(self) -> RoutingRequest:
        if self.max_tokens is not None and self.max_completion_tokens is not None:
            raise ValueError("set only one generation limit")
        finite_json(self.model_dump(mode="json"))
        return self


class SignalResult(Contract):
    name: Name
    status: Literal["ok", "unavailable"] = "ok"
    value: dict[str, JsonValue] = Field(default_factory=dict)
    source: str = ""
    elapsed_ms: float = Field(default=0, ge=0)
    error: SignalError | None = None

    @field_validator("value")
    @classmethod
    def finite_values(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        finite_json(value)
        return value


class PolicyMatch(Contract):
    rule: str | None = None
    models: tuple[str, ...] = ()
    shortcut: bool = False


class PipelineResult(Contract):
    signals: dict[str, SignalResult]
    match: PolicyMatch
    skipped_signals: tuple[str, ...] = ()
    elapsed_ms: float = Field(ge=0)
