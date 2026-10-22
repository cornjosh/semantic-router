"""Real loopback wire tests; synthetic replies say nothing about model accuracy."""

import asyncio
import json
import time
from contextlib import asynccontextmanager

import pytest
from aiohttp import web

from semantic_router.adaptive.config import PipelineConfig, SignalSpec
from semantic_router.adaptive.models import RoutingRequest
from semantic_router.adaptive.pipeline import SignalPipeline
from semantic_router.adaptive.providers import ProviderConfig, ProviderRegistry


def request(messages=None):
    return RoutingRequest(
        request_id="wire-test",
        tenant_id="tenant-a",
        messages=messages or [{"role": "user", "content": "write code"}],
    )


@asynccontextmanager
async def server(handler):
    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", handler)
    runner = web.AppRunner(app, shutdown_timeout=0.1)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        yield f"http://127.0.0.1:{runner.addresses[0][1]}/custom/classify"
    finally:
        await runner.cleanup()


def questions():
    return {
        "category": {
            "instructions": "What kind of task?",
            "options": {"code": "Programming", "other": "Everything else"},
        },
        "difficulty": {
            "instructions": "How difficult?",
            "options": {"easy": "Routine", "hard": "Complex reasoning"},
        },
    }


def answer():
    return {
        "model": "jev-fixture-version",
        "answers": {
            "category": {
                "type": "choice",
                "choice": "code",
                "confidence": 0.7,
                "probabilities": {"code": 0.9, "other": 0.1},
            },
            "difficulty": {
                "type": "choice",
                "choice": "easy",
                "confidence": 0.2,
                "probabilities": {"easy": 0.6, "hard": 0.4},
            },
        },
    }


@asynccontextmanager
async def pipeline(endpoint, kind="systemone", parameters=None, **limits):
    registry = ProviderRegistry(
        providers={
            "test": ProviderConfig(
                protocol=kind, endpoint=endpoint, model="fixture", **limits
            )
        }
    )
    spec = SignalSpec(
        name="semantic",
        kind=kind,
        timeout_ms=1000,
        parameters={"provider": "test", **(parameters or {"questions": questions()})},
    )
    value = SignalPipeline(
        config=PipelineConfig(signals=[spec], timeout_ms=1500),
        registry=registry.factories,
    )
    try:
        yield value
    finally:
        await value.aclose()
        await registry.aclose()


@pytest.mark.asyncio
async def test_systemone_custom_endpoint_multiple_questions_and_connection_pool(
    monkeypatch,
):
    seen = []
    connections = []
    monkeypatch.setenv("WIRE_TEST_KEY", "synthetic-secret")

    async def handler(req):
        seen.append((req.path, req.headers.get("Authorization"), await req.json()))
        connections.append(req.transport)
        return web.json_response(answer())

    async with server(handler) as url, pipeline(url, api_key_env="WIRE_TEST_KEY") as p:
        first = await p.run(request=request())
        second = await p.run(request=request())
    fact = first.signals["semantic"].value
    assert fact["category"] == "code"
    assert fact["difficulty"] == "easy"
    assert fact["score_type"] == "provider_probability"
    assert fact["answers"]["category"]["probabilities"]["code"] == 0.9
    assert fact["answers"]["category"]["confidence"] == 0.7
    assert fact["provider_model"] == "jev-fixture-version"
    assert second.signals["semantic"].status == "ok"
    assert connections[0] is connections[1]
    assert seen[0][0:2] == ("/custom/classify", "Bearer synthetic-secret")
    assert seen[0][2] == {
        "model": "fixture",
        "state": "write code",
        "questions": {
            name: {
                "type": "choice",
                "instructions": q["instructions"],
                "criteria": q["options"],
            }
            for name, q in questions().items()
        },
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad",
    [
        {},
        {"code": 1.0},
        {"code": 0.4, "other": 0.3},
        {"code": 1.1, "other": -0.1},
        {"code": float("nan"), "other": 0.1},
        {"code": "0.9", "other": 0.1},
        {"code": True, "other": 0},
        {"code": 0.9, "other": 0.1, "invented": 0},
    ],
)
async def test_invalid_probabilities_are_unavailable_never_normalized(bad):
    body = answer()
    body["answers"]["category"]["probabilities"] = bad

    async def handler(req):
        return web.json_response(body)

    async with server(handler) as url, pipeline(url) as p:
        result = await p.run(request=request())
    assert result.signals["semantic"].status == "unavailable"
    assert result.signals["semantic"].error == "invalid_result"
    assert result.signals["semantic"].value == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", ["missing", "unknown", "choice", "confidence", "type"]
)
async def test_partial_or_inconsistent_answers_fail_as_a_whole(mutation):
    body = answer()
    if mutation == "missing":
        del body["answers"]["difficulty"]
    elif mutation == "unknown":
        body["answers"]["extra"] = body["answers"]["category"]
    elif mutation == "choice":
        body["answers"]["category"]["choice"] = "other"
    elif mutation == "confidence":
        body["answers"]["category"]["confidence"] = 2.0
    else:
        body["answers"]["category"]["type"] = "score"

    async def handler(req):
        return web.json_response(body)

    async with server(handler) as url, pipeline(url) as p:
        result = await p.run(request=request())
    assert result.signals["semantic"].error == "invalid_result"


@pytest.mark.asyncio
async def test_embedding_reorders_indices_and_caches_examples():
    seen = []

    async def handler(req):
        body = await req.json()
        seen.append(body)
        vectors = {
            "write code": [1.0, 0.0],
            "programming": [1.0, 0.0],
            "cook": [0.0, 1.0],
        }
        return web.json_response(
            {
                "model": "embed-version",
                "data": [
                    {"index": i, "embedding": vectors[text]}
                    for i, text in reversed(list(enumerate(body["input"])))
                ],
            }
        )

    params = {"examples": {"code": ["programming"], "food": ["cook"]}}
    async with server(handler) as url, pipeline(url, "embedding", params) as p:
        first = await p.run(request=request())
        second = await p.run(request=request())
    assert first.signals["semantic"].value["category"] == "code"
    assert first.signals["semantic"].value["scores"] == {"code": 1.0, "food": 0.0}
    assert second.signals["semantic"].value["score_type"] == "cosine_similarity"
    assert second.signals["semantic"].value["dimensions"] == 2
    assert seen[0]["input"] == ["programming", "cook", "write code"]
    assert seen[1]["input"] == ["write code"]
    assert seen[0]["encoding_format"] == "float"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "data",
    [
        [],
        [{"index": 0, "embedding": [0, 0]}],
        [{"index": 1, "embedding": [1, 0]}],
        [{"index": 0, "embedding": [True, 1]}],
        [{"index": 0, "embedding": [float("inf"), 1]}],
        [{"index": 0, "embedding": "base64"}],
    ],
)
async def test_invalid_embedding_is_unavailable(data):
    async def handler(req):
        return web.json_response({"data": data})

    async with (
        server(handler) as url,
        pipeline(url, "embedding", {"examples": {}}) as p,
    ):
        result = await p.run(request=request())
    assert result.signals["semantic"].error == "invalid_result"


def llm_answer(content=None, **choice_overrides):
    return {
        "model": "llm-version",
        "choices": [
            {
                "index": 0,
                "finish_reason": "stop",
                "message": {
                    "role": "assistant",
                    "content": content
                    or json.dumps(
                        {"category": "code", "difficulty": "easy", "confidence": 0.8}
                    ),
                },
                **choice_overrides,
            }
        ],
    }


@pytest.mark.asyncio
async def test_llm_uses_schema_and_only_returns_facts():
    seen = []

    async def handler(req):
        seen.append(await req.json())
        return web.json_response(llm_answer())

    async with server(handler) as url, pipeline(url, "llm") as p:
        result = await p.run(request=request())
    facts = result.signals["semantic"].value
    assert facts["category"] == "code"
    assert facts["difficulty"] == "easy"
    assert facts["confidence"] == 0.8
    assert facts["score_type"] == "self_reported_confidence"
    body = seen[0]
    assert body["stream"] is False
    assert body["response_format"]["type"] == "json_schema"
    assert body["response_format"]["json_schema"]["strict"] is True
    assert body["messages"][-1]["content"] == "write code"
    assert "tools" not in body


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        llm_answer("not JSON"),
        llm_answer('{"category":"unknown","difficulty":"easy","confidence":0.8}'),
        llm_answer('{"category":"code","difficulty":"easy","confidence":NaN}'),
        llm_answer(
            '{"category":"code","difficulty":"easy","confidence":0.8,"model":"big"}'
        ),
        llm_answer(
            '{"category":"code","category":"other","difficulty":"easy","confidence":0.8}'
        ),
        llm_answer(finish_reason="length"),
        llm_answer(message={"content": None, "refusal": "no"}),
    ],
)
async def test_llm_rejects_invalid_schema_truncation_and_refusal(body):
    async def handler(req):
        return web.json_response(body)

    async with server(handler) as url, pipeline(url, "llm") as p:
        result = await p.run(request=request())
    assert result.signals["semantic"].error == "invalid_result"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode,expected", [("latest_user", "new question"), ("conversation", None)]
)
async def test_semantic_text_selection_is_explicit_and_ignores_image_payloads(
    mode, expected
):
    seen = []

    async def handler(req):
        seen.append((await req.json())["state"])
        return web.json_response(answer())

    req = request(
        [
            {"role": "system", "content": "policy"},
            {"role": "user", "content": "old question"},
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {"id": "x", "function": {"name": "search", "arguments": "{}"}}
                ],
            },
            {"role": "tool", "content": "tool result", "tool_call_id": "x"},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "new question"},
                    {"type": "image_url", "image_url": {"url": "private-image"}},
                ],
            },
        ]
    )
    params = {"questions": questions(), "text": {"mode": mode}}
    async with server(handler) as url, pipeline(url, parameters=params) as p:
        result = await p.run(request=req)
    assert result.signals["semantic"].status == "ok"
    assert "private-image" not in seen[0]
    if expected:
        assert seen == [expected]
    else:
        assert all(
            text in seen[0]
            for text in ("old question", "new question", "tool result", "search")
        )
    assert result.signals["semantic"].value["text_selection"] == mode


@pytest.mark.asyncio
async def test_oversized_input_fails_without_network_or_silent_truncation():
    seen = []

    async def handler(req):
        seen.append(await req.json())
        return web.json_response(answer())

    async with (
        server(handler) as url,
        pipeline(
            url, parameters={"questions": questions(), "text": {"max_chars": 3}}
        ) as p,
    ):
        result = await p.run(request=request())
    assert result.signals["semantic"].status == "unavailable"
    assert seen == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [302, 401, 429, 500])
async def test_errors_do_not_retry_follow_redirects_or_expose_secrets(status):
    seen = []

    async def handler(req):
        seen.append(req.path)
        return web.Response(
            status=status,
            text="Authorization: secret prompt",
            headers={"Location": "/redirect"},
        )

    async with server(handler) as url, pipeline(url) as p:
        result = await p.run(request=request())
    assert result.signals["semantic"].error == "provider_error"
    assert "secret" not in result.model_dump_json()
    assert seen == ["/custom/classify"]


@pytest.mark.asyncio
@pytest.mark.parametrize("chunked", [False, True])
async def test_bounded_response_body_even_without_content_length(chunked):
    async def handler(req):
        if not chunked:
            return web.Response(text="x" * 4096)
        response = web.StreamResponse()
        await response.prepare(req)
        await response.write(b"x" * 4096)
        await response.write_eof()
        return response

    async with server(handler) as url, pipeline(url, max_response_bytes=1024) as p:
        result = await p.run(request=request())
    assert result.signals["semantic"].error == "invalid_result"


@pytest.mark.asyncio
async def test_provider_concurrency_queue_counts_towards_total_timeout():
    seen = []
    entered = asyncio.Event()

    async def handler(req):
        seen.append(time.monotonic())
        entered.set()
        await asyncio.sleep(0.15)
        return web.json_response(answer())

    async with (
        server(handler) as url,
        pipeline(url, concurrency=1, timeout_ms=230) as p,
    ):
        first = asyncio.create_task(p.run(request=request()))
        await asyncio.wait_for(entered.wait(), 1)
        start = time.monotonic()
        results = await asyncio.gather(
            first, p.run(request=request()), p.run(request=request())
        )
        elapsed = time.monotonic() - start
    assert results[0].signals["semantic"].status == "ok"
    assert all(result.signals["semantic"].error == "timeout" for result in results[1:])
    assert elapsed < 0.4
    assert len(seen) == 2
    assert seen[1] - seen[0] >= 0.14


@pytest.mark.asyncio
async def test_cancelled_request_releases_provider_slot():
    entered = asyncio.Event()
    seen = []

    async def handler(req):
        seen.append(req.path)
        if len(seen) == 1:
            entered.set()
            await asyncio.sleep(1)
        return web.json_response(answer())

    async with server(handler) as url, pipeline(url, concurrency=1) as p:
        task = asyncio.create_task(p.run(request=request()))
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        result = await asyncio.wait_for(p.run(request=request()), 0.5)
    assert result.signals["semantic"].status == "ok"


@pytest.mark.asyncio
async def test_disabled_external_provider_needs_no_credentials(monkeypatch):
    monkeypatch.delenv("ABSENT_WIRE_KEY", raising=False)
    registry = ProviderRegistry(
        providers={
            "unused": ProviderConfig(
                protocol="llm",
                endpoint="http://127.0.0.1:1/chat",
                model="fixture",
                api_key_env="ABSENT_WIRE_KEY",
            )
        }
    )
    p = SignalPipeline(
        config=PipelineConfig(
            signals=[
                SignalSpec(
                    name="semantic",
                    kind="llm",
                    enabled=False,
                    parameters={"provider": "unused"},
                )
            ]
        ),
        registry=registry.factories,
    )
    try:
        result = await p.run(request=request())
        assert result.skipped_signals == ("semantic",)
        assert result.match.models == ()
    finally:
        await p.aclose()
        await registry.aclose()


@pytest.mark.asyncio
async def test_missing_required_key_never_sends_anonymous_request(monkeypatch):
    monkeypatch.delenv("ABSENT_WIRE_KEY", raising=False)
    seen = []

    async def handler(req):
        seen.append(req.path)
        return web.json_response(answer())

    async with (
        server(handler) as url,
        pipeline(url, api_key_env="ABSENT_WIRE_KEY") as p,
    ):
        result = await p.run(request=request())
    assert result.signals["semantic"].error == "provider_error"
    assert not seen


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body,headers",
    [
        (b'{"answers":{},"answers":{}}', {}),
        (b'{"bad":1e999}', {}),
        (b"not-json", {}),
        (b"compressed", {"Content-Encoding": "gzip"}),
    ],
)
async def test_invalid_json_or_compression_is_not_accepted(body, headers):
    async def handler(req):
        return web.Response(body=body, headers=headers)

    async with server(handler) as url, pipeline(url) as p:
        result = await p.run(request=request())
    assert result.signals["semantic"].error == "invalid_result"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["dimension", "model", "duplicate_index"])
async def test_embedding_cache_cannot_mix_dimensions_models_or_duplicate_indices(
    change,
):
    calls = 0

    async def handler(req):
        nonlocal calls
        calls += 1
        body = await req.json()
        data = [{"index": i, "embedding": [1, 0]} for i in range(len(body["input"]))]
        model = "version-one"
        if change == "duplicate_index":
            data[-1]["index"] = 0
        if calls == 2:
            if change == "dimension":
                data[0]["embedding"] = [1, 0, 0]
            else:
                model = "version-two"
        return web.json_response({"model": model, "data": data})

    async with (
        server(handler) as url,
        pipeline(url, "embedding", {"examples": {"code": ["example"]}}) as p,
    ):
        first = await p.run(request=request())
        if change == "duplicate_index":
            assert first.signals["semantic"].error == "invalid_result"
        else:
            assert first.signals["semantic"].status == "ok"
            second = await p.run(request=request())
            assert second.signals["semantic"].error == "invalid_result"


@pytest.mark.asyncio
async def test_two_signals_share_one_provider_concurrency_limit():
    active = maximum = calls = 0

    async def handler(req):
        nonlocal active, maximum, calls
        active += 1
        calls += 1
        maximum = max(active, maximum)
        await asyncio.sleep(0.025)
        active -= 1
        return web.json_response(answer())

    async with server(handler) as url:
        registry = ProviderRegistry(
            providers={
                "shared": ProviderConfig(
                    protocol="systemone", model="fixture", endpoint=url, concurrency=1
                )
            }
        )
        specs = [
            SignalSpec(
                name=name,
                kind="systemone",
                parameters={"provider": "shared", "questions": questions()},
            )
            for name in ("one", "two")
        ]
        p = SignalPipeline(
            config=PipelineConfig(signals=specs), registry=registry.factories
        )
        try:
            results = await asyncio.gather(
                *(p.run(request=request()) for _ in range(3))
            )
            assert all(r.signals[s.name].status == "ok" for r in results for s in specs)
            assert calls == 6
            assert maximum == 1
        finally:
            await p.aclose()
            await registry.aclose()


@pytest.mark.asyncio
async def test_provider_input_byte_limit_is_distinct_from_character_limit():
    seen = []

    async def handler(req):
        seen.append(req.path)
        return web.json_response(answer())

    async with server(handler) as url, pipeline(url, max_request_bytes=64) as p:
        result = await p.run(request=request())
    assert result.signals["semantic"].error == "invalid_result"
    assert not seen
