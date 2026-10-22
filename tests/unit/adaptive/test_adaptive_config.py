"""Adaptive configuration checks use a distinct module name for repository mypy."""

import pytest
from pydantic import ValidationError

from semantic_router.adaptive.config import (
    PipelineConfig,
    PolicyConfig,
    PolicyRule,
    Predicate,
    SignalSpec,
)
from semantic_router.adaptive.models import RoutingRequest, SignalResult


@pytest.mark.parametrize(
    "signals",
    [
        [{"name": "request", "kind": "custom"}],
        [{"name": "s", "kind": "one"}, {"name": "s", "kind": "two"}],
        [{"name": "s", "kind": "one", "timeout_ms": -1}],
        [{"name": "s", "kind": "one", "stage": -1}],
        [{"name": "s", "kind": "one", "enabled": False, "required": True}],
    ],
)
def test_invalid_signal_config_fails_before_serving_requests(signals):
    with pytest.raises(ValidationError):
        PipelineConfig(signals=signals)


def test_unknown_signal_reference_is_a_config_error():
    with pytest.raises(ValidationError, match="missing"):
        PipelineConfig(
            policy=PolicyConfig(
                rules=[
                    PolicyRule(
                        name="r",
                        when=[Predicate(signal="missing", path="x", op="eq", value=1)],
                        models=["m"],
                    )
                ]
            )
        )


def test_model_allowlist_validation_rejects_unknown_targets():
    config = PipelineConfig(policy=PolicyConfig(fallback_models=["typo"]))
    with pytest.raises(ValueError, match="typo"):
        config.validate_models(model_ids={"real-model"})


@pytest.mark.parametrize(
    "op,value",
    [
        ("gte", "0.5"),
        ("lte", True),
        ("in", "coding"),
        ("exists", 1),
        ("gte", float("nan")),
        ("eq", {"nested": float("inf")}),
    ],
)
def test_invalid_comparisons_and_nonfinite_values_are_rejected(op, value):
    with pytest.raises(ValidationError):
        Predicate(signal="s", path="x", op=op, value=value)


def test_signal_results_reject_nested_nonfinite_provider_output():
    with pytest.raises(ValidationError):
        SignalResult(name="s", value={"scores": [float("nan")]})


@pytest.mark.parametrize(
    "changes",
    [
        {"messages": []},
        {"messages": [{"role": "invented", "content": "hello"}]},
        {"max_tokens": -1},
        {"max_tokens": 1, "max_completion_tokens": 2},
        {"request_id": ""},
        {"tenant_id": ""},
    ],
)
def test_invalid_request_cannot_reach_signal_providers(changes):
    data = {
        "request_id": "r",
        "tenant_id": "t",
        "messages": [{"role": "user", "content": "hello"}],
    }
    data.update(changes)
    with pytest.raises(ValidationError):
        RoutingRequest.model_validate(data)


def test_disabled_signal_can_still_be_referenced_by_a_reusable_policy():
    config = PipelineConfig(
        signals=[SignalSpec(name="semantic", kind="remote", enabled=False)],
        policy=PolicyConfig(
            rules=[
                PolicyRule(
                    name="r",
                    when=[
                        Predicate(
                            signal="semantic", path="label", op="eq", value="coding"
                        )
                    ],
                    models=["small"],
                )
            ]
        ),
    )
    config.validate_models(model_ids={"small"})
