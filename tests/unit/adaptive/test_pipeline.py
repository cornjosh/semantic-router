"""Policy decisions must not leak into signal extraction or spend skipped work."""

import asyncio

import pytest

from semantic_router.adaptive.config import (
    PipelineConfig,
    PolicyConfig,
    PolicyRule,
    Predicate,
    SignalSpec,
)
from semantic_router.adaptive.models import RoutingRequest, SignalResult
from semantic_router.adaptive.pipeline import RequiredSignalUnavailable, SignalPipeline
from semantic_router.adaptive.policy import Policy
from semantic_router.adaptive.signals.structural import StructuralSignal


def request(**kwargs):
    return RoutingRequest(
        request_id="req-1",
        tenant_id="tenant-a",
        messages=[{"role": "user", "content": "Explain binary search"}],
        **kwargs,
    )


def category_rule(model="small", shortcut=False):
    return PolicyRule(
        name="coding",
        when=[Predicate(signal="semantic", path="category", op="eq", value="coding")],
        models=[model],
        shortcut=shortcut,
    )


class Facts:
    async def extract(self, request):
        return {"category": "coding", "difficulty": 0.3}


@pytest.mark.asyncio
async def test_tool_shortcut_never_constructs_or_calls_expensive_provider():
    def expensive(spec):
        raise AssertionError("shortcut constructed a later provider")

    config = PipelineConfig(
        signals=[SignalSpec(name="semantic", kind="expensive")],
        policy=PolicyConfig(
            rules=[
                PolicyRule(
                    name="tool-continuation",
                    when=[
                        Predicate(
                            signal="request",
                            path="is_tool_continuation",
                            op="eq",
                            value=True,
                        )
                    ],
                    models=["tool-model"],
                    shortcut=True,
                )
            ],
            fallback_models=["general"],
        ),
    )
    tool_request = request().model_copy(
        update={
            "messages": RoutingRequest.model_validate(
                {
                    **request().model_dump(),
                    "messages": [
                        {"role": "tool", "content": "42", "tool_call_id": "c1"}
                    ],
                }
            ).messages
        }
    )
    result = await SignalPipeline(config=config, registry={"expensive": expensive}).run(
        request=tool_request
    )
    assert result.match.models == ("tool-model",)
    assert result.match.rule == "tool-continuation"
    assert result.match.shortcut is True
    assert tuple(result.signals) == ("request",)
    assert result.skipped_signals == ("semantic",)


@pytest.mark.asyncio
async def test_disabled_unregistered_provider_requires_no_credentials_or_factory():
    pipeline = SignalPipeline(
        config=PipelineConfig(
            signals=[SignalSpec(name="semantic", kind="not-installed", enabled=False)],
            policy=PolicyConfig(rules=[category_rule()], fallback_models=["general"]),
        ),
        registry={},
    )
    result = await pipeline.run(request=request())
    assert result.match.models == ("general",)
    assert "semantic" not in result.signals
    assert result.skipped_signals == ("semantic",)


@pytest.mark.asyncio
async def test_same_extracted_facts_can_drive_different_policies():
    config = PipelineConfig(
        signals=[SignalSpec(name="semantic", kind="facts")],
        policy=PolicyConfig(rules=[category_rule()], fallback_models=["general"]),
    )
    result = await SignalPipeline(
        config=config, registry={"facts": lambda spec: Facts()}
    ).run(request=request())
    alternative = Policy(config=PolicyConfig(rules=[category_rule("large")]))
    assert result.signals["semantic"].value == {"category": "coding", "difficulty": 0.3}
    assert result.match.models == ("small",)
    assert alternative.evaluate(signals=result.signals, final=True).models == ("large",)


@pytest.mark.asyncio
async def test_provisional_match_waits_for_later_higher_priority_evidence():
    config = PipelineConfig(
        signals=[SignalSpec(name="semantic", kind="facts", stage=2)],
        policy=PolicyConfig(
            rules=[
                category_rule("specialist"),
                PolicyRule(name="provisional", models=["general"]),
            ]
        ),
    )
    result = await SignalPipeline(
        config=config, registry={"facts": lambda spec: Facts()}
    ).run(request=request())
    assert result.match.models == ("specialist",)
    assert "semantic" in result.signals


@pytest.mark.asyncio
async def test_shortcut_after_stage_skips_only_later_stages():
    config = PipelineConfig(
        signals=[
            SignalSpec(name="semantic", kind="facts", stage=1),
            SignalSpec(name="expensive", kind="not-installed", stage=2),
        ],
        policy=PolicyConfig(rules=[category_rule(shortcut=True)]),
    )
    result = await SignalPipeline(
        config=config, registry={"facts": lambda spec: Facts()}
    ).run(request=request())
    assert result.match.models == ("small",)
    assert result.skipped_signals == ("expensive",)


@pytest.mark.asyncio
async def test_stage_peers_execute_concurrently_before_policy_evaluation():
    barrier = asyncio.Event()

    class Waiting:
        async def extract(self, request):
            await asyncio.wait_for(barrier.wait(), timeout=0.2)
            return {"category": "coding"}

    class Release:
        async def extract(self, request):
            barrier.set()
            return {"ready": True}

    config = PipelineConfig(
        signals=[
            SignalSpec(name="semantic", kind="waiting"),
            SignalSpec(name="peer", kind="release"),
        ],
        policy=PolicyConfig(rules=[category_rule(shortcut=True)]),
    )
    result = await SignalPipeline(
        config=config,
        registry={"waiting": lambda spec: Waiting(), "release": lambda spec: Release()},
    ).run(request=request())
    assert result.match.models == ("small",)
    assert result.signals["peer"].value == {"ready": True}


@pytest.mark.asyncio
async def test_timeout_cancels_signal_and_uses_explicit_unavailable_fallback():
    cancelled = asyncio.Event()

    class Slow:
        async def extract(self, request):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

    config = PipelineConfig(
        timeout_ms=20,
        signals=[SignalSpec(name="semantic", kind="slow", timeout_ms=500)],
        policy=PolicyConfig(rules=[category_rule()], fallback_models=["general"]),
    )
    result = await asyncio.wait_for(
        SignalPipeline(config=config, registry={"slow": lambda spec: Slow()}).run(
            request=request()
        ),
        timeout=0.5,
    )
    assert cancelled.is_set()
    assert result.signals["semantic"].status == "unavailable"
    assert result.signals["semantic"].error == "timeout"
    assert result.match.models == ("general",)


@pytest.mark.asyncio
async def test_total_deadline_does_not_construct_later_provider():
    class Slow:
        async def extract(self, request):
            await asyncio.Event().wait()

    def forbidden(spec):
        raise AssertionError("provider constructed after request budget expired")

    result = await SignalPipeline(
        config=PipelineConfig(
            timeout_ms=15,
            signals=[
                SignalSpec(name="slow", kind="slow", stage=1),
                SignalSpec(name="later", kind="forbidden", stage=2),
            ],
        ),
        registry={"slow": lambda spec: Slow(), "forbidden": forbidden},
    ).run(request=request())
    assert result.signals["later"].error == "timeout"


@pytest.mark.asyncio
async def test_null_optional_tool_fields_are_equivalent_to_absent_fields():
    req = RoutingRequest(
        request_id="null-tools",
        tenant_id="tenant-a",
        messages=[{"role": "assistant", "content": "hello", "tool_calls": None}],
        tools=None,
    )
    facts = await StructuralSignal().extract(request=req)
    assert facts["requires_tools"] is False


@pytest.mark.asyncio
async def test_required_failure_is_not_silently_downgraded_and_errors_are_sanitized():
    class Broken:
        async def extract(self, request):
            raise RuntimeError("Bearer confidential-provider-key")

    config = PipelineConfig(
        signals=[SignalSpec(name="semantic", kind="broken", required=True)],
        policy=PolicyConfig(fallback_models=["general"]),
    )
    with pytest.raises(RequiredSignalUnavailable, match="semantic") as exc:
        await SignalPipeline(
            config=config, registry={"broken": lambda spec: Broken()}
        ).run(request=request())
    assert "confidential" not in str(exc.value)


@pytest.mark.asyncio
async def test_request_cancellation_reaches_running_signal():
    started, cancelled = asyncio.Event(), asyncio.Event()

    class Waiting:
        async def extract(self, request):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

    pipeline = SignalPipeline(
        config=PipelineConfig(signals=[SignalSpec(name="semantic", kind="waiting")]),
        registry={"waiting": lambda spec: Waiting()},
    )
    task = asyncio.create_task(pipeline.run(request=request()))
    await asyncio.wait_for(started.wait(), timeout=0.5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_signal_output_is_detached_from_provider_owned_data():
    data = {"nested": {"label": "original"}}

    class Shared:
        async def extract(self, request):
            return data

    result = await SignalPipeline(
        config=PipelineConfig(signals=[SignalSpec(name="facts", kind="shared")]),
        registry={"shared": lambda spec: Shared()},
    ).run(request=request())
    data["nested"]["label"] = "changed"
    assert result.signals["facts"].value == {"nested": {"label": "original"}}


@pytest.mark.asyncio
async def test_historical_tool_result_does_not_shortcut_a_new_user_task():
    req = RoutingRequest(
        request_id="new-task",
        tenant_id="tenant-a",
        messages=[
            {"role": "tool", "content": "old", "tool_call_id": "old-call"},
            {"role": "user", "content": "write a poem"},
        ],
    )
    facts = await StructuralSignal().extract(request=req)
    assert facts["has_tool_result"] is True
    assert facts["is_tool_continuation"] is False


@pytest.mark.asyncio
async def test_signal_instance_is_reused_and_closed_only_if_initialized():
    instances = []

    class Resource(Facts):
        closed = False

        async def aclose(self):
            self.closed = True

    def factory(spec):
        obj = Resource()
        instances.append(obj)
        return obj

    pipeline = SignalPipeline(
        config=PipelineConfig(signals=[SignalSpec(name="semantic", kind="resource")]),
        registry={"resource": factory},
    )
    await pipeline.run(request=request())
    await pipeline.run(request=request())
    await pipeline.aclose()
    assert len(instances) == 1
    assert instances[0].closed is True


@pytest.mark.asyncio
async def test_optional_provider_error_does_not_disclose_error_text():
    class Broken:
        async def extract(self, request):
            raise RuntimeError("Bearer confidential-provider-key")

    result = await SignalPipeline(
        config=PipelineConfig(signals=[SignalSpec(name="semantic", kind="broken")]),
        registry={"broken": lambda spec: Broken()},
    ).run(request=request())
    assert result.signals["semantic"].error == "provider_error"
    assert "confidential" not in result.model_dump_json()


@pytest.mark.asyncio
async def test_structural_facts_cover_full_history_and_preserve_requested_model():
    req = RoutingRequest(
        request_id="req-tools",
        tenant_id="tenant-a",
        model="fixed-model",
        messages=[
            {"role": "system", "content": "system"},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "hello"},
                    {
                        "type": "image_url",
                        "image_url": {"url": "https://example.test/image"},
                    },
                ],
            },
            {"role": "tool", "content": "result", "tool_call_id": "c1"},
        ],
        tools=[{"type": "function", "function": {"name": "lookup"}}],
        response_format={"type": "json_schema", "json_schema": {"name": "answer"}},
        max_completion_tokens=128,
        stream=True,
    )
    facts = await StructuralSignal().extract(request=req)
    assert facts["requested_model"] == "fixed-model"
    assert facts["has_tool_result"] is True
    assert facts["requires_tools"] is True
    assert facts["modalities"] == ["image", "text"]
    assert facts["response_format"] == "json_schema"
    assert facts["max_output_tokens"] == 128
    assert facts["text_characters"] == 17
    assert "input_tokens" not in facts  # Character counts are not exact token counts.


@pytest.mark.parametrize(
    "status,value", [("unavailable", {"difficulty": 1}), ("ok", {})]
)
def test_absent_evidence_never_matches_numeric_rule(status, value):
    policy = Policy(
        config=PolicyConfig(
            rules=[
                PolicyRule(
                    name="hard",
                    when=[
                        Predicate(signal="s", path="difficulty", op="gte", value=0.7)
                    ],
                    models=["large"],
                )
            ],
            fallback_models=["general"],
        )
    )
    match = policy.evaluate(
        signals={"s": SignalResult(name="s", status=status, value=value)}, final=True
    )
    assert match.models == ("general",)


@pytest.mark.parametrize("actual", [True, "0.9", None, [1]])
def test_numeric_predicates_do_not_coerce_untrusted_types(actual):
    policy = Policy(
        config=PolicyConfig(
            rules=[
                PolicyRule(
                    name="hard",
                    when=[Predicate(signal="s", path="x", op="gte", value=0.7)],
                    models=["large"],
                )
            ],
            fallback_models=["general"],
        )
    )
    match = policy.evaluate(
        signals={"s": SignalResult(name="s", value={"x": actual})}, final=True
    )
    assert match.models == ("general",)


@pytest.mark.parametrize(
    "op,expected,actual,chosen",
    [
        ("eq", True, 1, "general"),
        ("eq", 1, 1.0, "selected"),
        ("in", ["coding", "math"], "math", "selected"),
        ("in", [True], 1, "general"),
        ("lte", 0.5, 0.3, "selected"),
        ("exists", True, None, "selected"),
    ],
)
def test_policy_operators_match_typed_nested_facts(op, expected, actual, chosen):
    policy = Policy(
        config=PolicyConfig(
            rules=[
                PolicyRule(
                    name="r",
                    when=[
                        Predicate(signal="s", path="nested.x", op=op, value=expected)
                    ],
                    models=["selected"],
                )
            ],
            fallback_models=["general"],
        )
    )
    match = policy.evaluate(
        signals={"s": SignalResult(name="s", value={"nested": {"x": actual}})},
        final=True,
    )
    assert match.models == (chosen,)


def test_unavailable_signal_does_not_satisfy_exists_false():
    policy = Policy(
        config=PolicyConfig(
            rules=[
                PolicyRule(
                    name="r",
                    when=[Predicate(signal="s", path="x", op="exists", value=False)],
                    models=["selected"],
                )
            ],
            fallback_models=["general"],
        )
    )
    match = policy.evaluate(
        signals={"s": SignalResult(name="s", status="unavailable")}, final=True
    )
    assert match.models == ("general",)
