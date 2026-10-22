"""Policy results never bypass tenant/capability/context constraints."""

import pytest
from pydantic import ValidationError

from semantic_router.adaptive.models import Message, RoutingRequest
from semantic_router.adaptive.service import RoutingService, ServiceError
from semantic_router.adaptive.service_config import ServiceConfig
from semantic_router.adaptive.state.base import Feedback


def config(path, **updates):
    value = {
        "policy_version": "p1",
        "feature_version": "f1",
        "state": {"backend": "sqlite", "path": str(path)},
        "pipeline": {"policy": {"fallback_models": ["a", "b"]}},
        "models": [
            {
                "model_id": name,
                "version": "v1",
                "max_context_tokens": 8192,
                "max_output_tokens": 1024,
                "tools": name == "b",
                "accounting": {
                    "mode": "utf8_upper_bound",
                    "framing_tokens": 64,
                    "tokens_per_message": 32,
                },
            }
            for name in ("a", "b")
        ],
        "clusters": [
            {
                "cluster_id": cluster,
                "model_id": model,
                "route_id": cluster,
                "upstream_model": model + "-served",
            }
            for model, cluster in [("a", "east"), ("a", "west"), ("b", "large")]
        ],
        "tenants": {"tenant-a": ["a", "b"], "tenant-b": ["b"]},
        "principals": [
            {
                "token_env": "GATEWAY_TOKEN",
                "tenant_id": "tenant-a",
                "roles": ["route", "observe"],
            },
            {
                "token_env": "FEEDBACK_TOKEN",
                "tenant_id": "tenant-a",
                "roles": ["feedback"],
            },
        ],
    }
    value.update(updates)
    return ServiceConfig.model_validate(value)


def request(**updates):
    return RoutingRequest(
        request_id="request-1",
        tenant_id="tenant-a",
        messages=(Message(role="user", content="hello"),),
    ).model_copy(update=updates)


@pytest.mark.asyncio
async def test_persisted_complete_decision_replays_and_feedback_changes_next_choice(
    tmp_path,
):
    service = RoutingService(config=config(tmp_path / "state.db"))
    await service.start()
    try:
        first = await service.route(request=request())
        assert first.model_id == "a" and first.upstream_model == "a-served"
        assert first.route_id in ("east", "west")
        assert first.accounting["method"] == "declared_utf8_upper_bound"
        assert await service.route(request=request()) == first
        await service.state.feedback(
            tenant_id="tenant-a",
            event=Feedback(
                event_id="quality-1",
                decision_id=first.decision_id,
                model_id="a",
                model_version="v1",
                policy_version="p1",
                success=False,
            ),
        )
        next_decision = await service.route(request=request(request_id="request-2"))
        assert next_decision.model_id == "b"
        assert next_decision.propensity == 1
    finally:
        await service.aclose()


@pytest.mark.asyncio
async def test_fixed_model_and_tools_cannot_be_changed_by_fallback(tmp_path):
    service = RoutingService(config=config(tmp_path / "state.db"))
    await service.start()
    try:
        result = await service.route(request=request(model="b"))
        assert result.model_id == "b"
        with pytest.raises(ServiceError, match="no_capable_model"):
            await service.route(
                request=request(
                    request_id="tools",
                    model="a",
                    tools=({"type": "function", "function": {"name": "tool"}},),
                )
            )
        with pytest.raises(ServiceError, match="model_not_allowed"):
            await service.route(
                request=request(request_id="tenant", tenant_id="tenant-b", model="a")
            )
    finally:
        await service.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"max_tokens": 2048},
        {"messages": (Message(role="user", content="a" * 9000),)},
        {
            "response_format": {
                "type": "json_schema",
                "json_schema": {"name": "x", "schema": {}},
            }
        },
        {
            "messages": (
                Message(
                    role="user",
                    content=(
                        {
                            "type": "image_url",
                            "image_url": {"url": "https://example.invalid/image"},
                        },
                    ),
                ),
            )
        },
    ],
)
async def test_hard_constraints_apply_before_expensive_signals(tmp_path, changes):
    service = RoutingService(config=config(tmp_path / "state.db"))
    await service.start()

    async def must_not_run(**kwargs):
        raise AssertionError("hard-ineligible request reached signal provider")

    service.pipeline.run = must_not_run
    try:
        with pytest.raises(ServiceError, match="no_capable_model"):
            await service.route(request=request(**changes))
    finally:
        await service.aclose()


@pytest.mark.asyncio
async def test_replay_after_permission_or_route_change_is_rejected(tmp_path):
    path = tmp_path / "state.db"
    first = RoutingService(config=config(path))
    await first.start()
    decision = await first.route(request=request())
    await first.aclose()
    cfg = config(path).model_dump()
    for cluster in cfg["clusters"]:
        if cluster["cluster_id"] == decision.cluster_id:
            cluster["upstream_model"] = "different"
    changed = RoutingService(config=ServiceConfig.model_validate(cfg))
    await changed.start()
    try:
        with pytest.raises(ServiceError, match="decision_config_changed"):
            await changed.route(request=request())
    finally:
        await changed.aclose()


def test_gateway_credentials_cannot_train_quality(tmp_path):
    with pytest.raises(ValidationError):
        config(
            tmp_path / "state.db",
            principals=[
                {
                    "token_env": "TOKEN",
                    "tenant_id": "tenant-a",
                    "roles": ["route", "feedback"],
                }
            ],
        )


def test_catalog_and_disabled_provider_validation(tmp_path):
    with pytest.raises(ValidationError):
        config(tmp_path / "state.db", tenants={"tenant-a": ["missing"]})
    cfg = config(
        tmp_path / "state.db",
        pipeline={
            "signals": [{"name": "off", "kind": "llm", "enabled": False}],
            "policy": {"fallback_models": ["a"]},
        },
    )
    assert cfg.pipeline.signals[0].enabled is False


@pytest.mark.asyncio
async def test_capacity_facts_are_extracted_independently_of_model_policy():
    from semantic_router.adaptive.signals.structural import StructuralSignal

    query = request(
        messages=(
            Message(
                role="user",
                content=(
                    {"type": "text", "text": "中文"},
                    {
                        "type": "image_url",
                        "image_url": {"url": "https://example.invalid/a"},
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": "https://example.invalid/b"},
                    },
                ),
            ),
        ),
        tools=(
            {
                "type": "function",
                "function": {"name": "lookup", "description": "large tool description"},
            },
        ),
    )
    facts = await StructuralSignal().extract(request=query)
    assert facts["media_counts"] == {"image": 2}
    assert facts["payload_bytes"] > len("中文".encode())
    assert "model_id" not in facts and "context_limit" not in facts


@pytest.mark.asyncio
async def test_missing_exact_token_counter_rejects_instead_of_estimating(tmp_path):
    cfg = config(tmp_path / "state.db").model_dump()
    for model in cfg["models"]:
        model["accounting"] = {"mode": "exact_required"}
    service = RoutingService(config=ServiceConfig.model_validate(cfg))
    await service.start()
    try:
        with pytest.raises(ServiceError, match="no_capable_model"):
            await service.route(request=request())
    finally:
        await service.aclose()


@pytest.mark.asyncio
async def test_tool_shortcut_skips_provider_but_still_filters_capability(tmp_path):
    cfg = config(tmp_path / "state.db").model_dump()
    cfg["providers"] = {
        "never": {
            "protocol": "llm",
            "endpoint": "http://127.0.0.1:1/unreachable",
            "model": "unused",
        }
    }
    cfg["pipeline"] = {
        "signals": [
            {
                "name": "semantic",
                "kind": "llm",
                "parameters": {
                    "provider": "never",
                    "questions": {
                        "category": {
                            "instructions": "classify",
                            "options": {"code": "code", "general": "general"},
                        }
                    },
                },
            }
        ],
        "policy": {
            "rules": [
                {
                    "name": "tool",
                    "when": [
                        {
                            "signal": "request",
                            "path": "is_tool_continuation",
                            "op": "eq",
                            "value": True,
                        }
                    ],
                    "models": ["a", "b"],
                    "shortcut": True,
                }
            ]
        },
    }
    service = RoutingService(config=ServiceConfig.model_validate(cfg))
    await service.start()
    try:
        decision = await service.route(
            request=request(
                messages=(
                    Message(role="tool", content="result", tool_call_id="call-1"),
                )
            )
        )
        assert decision.model_id == "b"
        assert decision.signal_status["semantic"] == "skipped"
        assert service.providers._clients == {}
    finally:
        await service.aclose()


@pytest.mark.asyncio
async def test_media_requires_explicit_budget_and_full_schema_counts(tmp_path):
    cfg = config(tmp_path / "state.db").model_dump()
    for model in cfg["models"]:
        model["modalities"] = ["text", "image"]
    image_request = request(
        messages=(
            Message(
                role="user",
                content=(
                    {
                        "type": "image_url",
                        "image_url": {"url": "https://example.invalid/image"},
                    },
                ),
            ),
        )
    )
    service = RoutingService(config=ServiceConfig.model_validate(cfg))
    await service.start()
    try:
        with pytest.raises(ServiceError, match="no_capable_model"):
            await service.route(request=image_request)
    finally:
        await service.aclose()
    cfg["models"][0]["accounting"]["media_tokens"] = {"image": 1024}
    second = RoutingService(config=ServiceConfig.model_validate(cfg))
    await second.start()
    try:
        result = await second.route(request=image_request)
        assert result.accounting["input_tokens_bound"] > 1024
        assert result.model_id == "a"
    finally:
        await second.aclose()


@pytest.mark.asyncio
async def test_response_schema_bytes_are_included_in_capacity(tmp_path):
    cfg = config(tmp_path / "state.db").model_dump()
    for model in cfg["models"]:
        model["response_formats"] = ["json_schema"]
        model["max_context_tokens"] = 2048
    service = RoutingService(config=ServiceConfig.model_validate(cfg))
    await service.start()
    try:
        small = {
            "type": "json_schema",
            "json_schema": {"name": "answer", "schema": {"type": "object"}},
        }
        assert (
            await service.route(request=request(response_format=small))
        ).model_id == "a"
        huge = {
            "type": "json_schema",
            "json_schema": {
                "name": "answer",
                "schema": {"type": "object", "description": "x" * 4096},
            },
        }
        with pytest.raises(ServiceError, match="no_capable_model"):
            await service.route(
                request=request(request_id="huge-schema", response_format=huge)
            )
    finally:
        await service.aclose()
