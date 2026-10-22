"""Actual Wasm → two classifier processes → three-member etcd → HTTP providers."""

import asyncio
import os
import sys
from contextlib import AsyncExitStack
from uuid import uuid4

import aiohttp
import pytest
from aiohttp import web

from semantic_router.adaptive.models import RoutingRequest
from semantic_router.adaptive.state.base import LearningContext, fingerprint
from semantic_router.adaptive.state.factory import open_state
from tests.integration.adaptive.test_higress_wasm import (
    ROUTE_TOKEN,
    STREAM,
    free_port,
    gateway,
    serve,
)
from tests.unit.adaptive.test_service import config


@pytest.mark.asyncio
async def test_complete_chain_feedback_and_worker_failover(tmp_path, etcd_cluster):
    provider_calls, generations = [], []

    async def embed(request):
        value = await request.json()
        provider_calls.append(value["input"])
        return web.json_response(
            {
                "model": "fixture-embedding",
                "data": [
                    {
                        "index": i,
                        "embedding": [0.0, 1.0] if text == "code" else [1.0, 0.0],
                    }
                    for i, text in enumerate(value["input"])
                ],
            }
        )

    def backend(cluster):
        async def handler(request):
            generations.append((cluster, await request.json()))
            response = web.StreamResponse(headers={"content-type": "text/event-stream"})
            await response.prepare(request)
            await response.write(STREAM[:40])
            await asyncio.sleep(0.02)
            await response.write(STREAM[40:])
            await response.write_eof()
            return response

        app = web.Application()
        app.router.add_post("/v1/chat/completions", handler)
        return app

    provider = web.Application()
    provider.router.add_post("/embeddings", embed)
    children, logs = [], []
    state = None
    async with AsyncExitStack() as stack:
        port = await stack.enter_async_context(serve(provider))
        endpoints = {
            name: await stack.enter_async_context(serve(backend(name)))
            for name in ("east", "west", "large")
        }
        cfg = config(
            tmp_path / "unused.db",
            state={
                "backend": "etcd",
                "endpoints": etcd_cluster.endpoints,
                "prefix": "/chain/" + uuid4().hex + "/",
            },
            pipeline={
                "signals": [
                    {
                        "name": "semantic",
                        "kind": "embedding",
                        "required": True,
                        "parameters": {
                            "provider": "local",
                            "examples": {"general": ["hello"], "code": ["code"]},
                        },
                    }
                ],
                "policy": {
                    "rules": [
                        {
                            "name": "tools",
                            "when": [
                                {
                                    "signal": "request",
                                    "path": "requires_tools",
                                    "op": "eq",
                                    "value": True,
                                }
                            ],
                            "models": ["b"],
                            "shortcut": True,
                        }
                    ],
                    "fallback_models": ["a", "b"],
                },
            },
            providers={
                "local": {
                    "protocol": "embedding",
                    "endpoint": f"http://127.0.0.1:{port}/embeddings",
                    "model": "fixture-embedding",
                }
            },
            context_fields={"category": {"signal": "semantic", "path": "category"}},
        )
        config_path = tmp_path / "service.json"
        config_path.write_text(cfg.model_dump_json())
        worker_ports = [free_port(), free_port()]
        env = {
            **os.environ,
            "GATEWAY_TOKEN": ROUTE_TOKEN,
            "FEEDBACK_TOKEN": "quality-token-123456789",
        }
        client = await stack.enter_async_context(
            aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        )
        try:
            for i, worker_port in enumerate(worker_ports):
                log = (tmp_path / f"service-{i}.log").open("wb")
                logs.append(log)
                children.append(
                    await asyncio.create_subprocess_exec(
                        sys.executable,
                        "-m",
                        "semantic_router.adaptive",
                        "--config",
                        str(config_path),
                        "--port",
                        str(worker_port),
                        env=env,
                        stdout=log,
                        stderr=log,
                    )
                )
            for worker_port in worker_ports:
                for _ in range(200):
                    try:
                        async with client.get(
                            f"http://127.0.0.1:{worker_port}/readyz"
                        ) as response:
                            if response.status == 200:
                                break
                    except aiohttp.ClientError:
                        pass
                    await asyncio.sleep(0.05)
                else:
                    pytest.fail("worker not ready")
            state = open_state(**cfg.state.model_dump())
            await state.initialize()
            plugin = {
                "tenant_id": "tenant-a",
                "route_token": ROUTE_TOKEN,
                "observe_token": ROUTE_TOKEN,
                "timeout_ms": 4000,
                "targets": {
                    c.route_id: {
                        "model_id": c.model_id,
                        "cluster_id": c.cluster_id,
                        "upstream_model": c.upstream_model,
                    }
                    for c in cfg.clusters
                },
            }
            async with gateway(
                tmp_path, worker_ports, endpoints, plugin_config=plugin
            ) as (url, _):

                async def generate(request_id, tools=None):
                    payload = {
                        "model": "auto",
                        "messages": [{"role": "user", "content": "hello"}],
                        "stream": True,
                    }
                    if tools:
                        payload["tools"] = tools
                    async with client.post(
                        url + "/v1/chat/completions",
                        json=payload,
                        headers={"x-request-id": request_id},
                    ) as response:
                        assert response.status == 200, await response.text()
                        decision_id = response.headers["x-semantic-decision-id"]
                        assert await response.read() == STREAM
                    req = RoutingRequest(
                        request_id=request_id, tenant_id="tenant-a", **payload
                    )
                    decision = await state.lookup(
                        tenant_id="tenant-a",
                        request_id=request_id,
                        fingerprint=fingerprint(req.model_dump(mode="json")),
                    )
                    assert str(decision.decision_id) == decision_id
                    return decision

                first = await generate("first")
                assert first.model_id == "a" and first.signal_status["semantic"] == "ok"
                feedback = {
                    "schema_version": "1",
                    "tenant_id": "tenant-a",
                    "event": {
                        "event_id": "quality-chain",
                        "decision_id": str(first.decision_id),
                        "model_id": "a",
                        "model_version": "v1",
                        "policy_version": "p1",
                        "success": False,
                    },
                }
                for i, worker_port in enumerate(worker_ports):
                    async with client.post(
                        f"http://127.0.0.1:{worker_port}/v1/feedback",
                        json=feedback,
                        headers={"Authorization": "Bearer quality-token-123456789"},
                    ) as response:
                        assert response.status == 200
                        assert (await response.json())["applied"] is (i == 0)
                children[0].kill()
                await children[0].wait()
                await asyncio.sleep(
                    0.6
                )  # Envoy's active checks remove the exited worker.
                second = await generate("after-worker-loss")
                assert second.model_id == "b"
                count = len(provider_calls)
                shortcut = await generate(
                    "tool-shortcut",
                    [{"type": "function", "function": {"name": "weather"}}],
                )
                assert shortcut.model_id == "b"
                assert len(provider_calls) == count
                for _ in range(100):
                    snapshot = await state.snapshot(
                        context=LearningContext(
                            tenant_id="tenant-a",
                            policy_version="p1",
                            feature_version="f1",
                            category="general",
                        ),
                        models=cfg.models,
                        cluster_ids=list(endpoints),
                    )
                    if sum(c.samples for c in snapshot.clusters.values()) == 3:
                        break
                    await asyncio.sleep(0.03)
                assert snapshot.learning["a"].failures == 1
                assert (
                    snapshot.learning["b"].successes == 0
                )  # HTTP success is not quality.
                assert sum(c.samples for c in snapshot.clusters.values()) == 3
                assert all(c.inflight == 0 for c in snapshot.clusters.values())
                assert all(
                    c.ttft_ms is not None
                    for c in snapshot.clusters.values()
                    if c.samples
                )
                assert [body["model"] for _, body in generations] == [
                    "a-served",
                    "b-served",
                    "b-served",
                ]
        finally:
            if state:
                await state.aclose()
            for child in children:
                if child.returncode is None:
                    child.terminate()
            for child in children:
                try:
                    await asyncio.wait_for(child.wait(), 5)
                except asyncio.TimeoutError:
                    child.kill()
                    await child.wait()
            for log in logs:
                log.close()
