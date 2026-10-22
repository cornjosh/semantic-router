"""Real Higress binary + real compiled Wasm; HTTP fixtures are not LLM quality tests."""

import asyncio
import json
import os
import socket
import subprocess
from contextlib import asynccontextmanager
from uuid import uuid4

import aiohttp
import pytest
import yaml
from aiohttp import web

ROUTE_TOKEN = "test-route-token-12345"
OBSERVE_TOKEN = "test-observe-token-12345"
STREAM = (
    b": heartbeat\r\n\r\n"
    b'data: {"choices":[{"delta":{"role":"assistant","content":""}}]}\n\n'
    b'data: {"choices":[{"delta":{"tool_calls":[{"function":{"name":"weather"}}]}}]}\n\n'
    b"data: [DONE]\n\n"
)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@asynccontextmanager
async def serve(app):
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        yield site._server.sockets[0].getsockname()[1]
    finally:
        await runner.cleanup()


def bootstrap(wasm, gateway_port, classifier_port, upstream_ports):
    """Header routes deliberately precede a rejecting default to prove rerouting."""
    targets = {
        f"route-{key}": {
            "model_id": "basic",
            "cluster_id": key,
            "upstream_model": f"alias-{key}",
        }
        for key in upstream_ports
    }
    plugin = {
        "tenant_id": "tenant",
        "classifier_cluster": "classifier",
        "classifier_authority": "classifier",
        "route_token": ROUTE_TOKEN,
        "observe_token": OBSERVE_TOKEN,
        "timeout_ms": 500,
        "targets": targets,
    }
    routes = [
        {
            "match": {
                "prefix": "/",
                "headers": [
                    {"name": "x-semantic-route", "string_match": {"exact": name}}
                ],
            },
            "route": {"cluster": target["cluster_id"], "timeout": "3s"},
        }
        for name, target in targets.items()
    ] + [{"match": {"prefix": "/"}, "direct_response": {"status": 503}}]
    clusters = []
    for name, port in {"classifier": classifier_port, **upstream_ports}.items():
        clusters.append(
            {
                "name": name,
                "connect_timeout": "1s",
                "type": "STATIC",
                "load_assignment": {
                    "cluster_name": name,
                    "endpoints": [
                        {
                            "lb_endpoints": [
                                {
                                    "endpoint": {
                                        "address": {
                                            "socket_address": {
                                                "address": "127.0.0.1",
                                                "port_value": port,
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    ],
                },
            }
        )
    hcm = {
        "@type": "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
        "stat_prefix": "adaptive_test",
        "preserve_external_request_id": True,
        "route_config": {
            "name": "adaptive",
            "virtual_hosts": [{"name": "all", "domains": ["*"], "routes": routes}],
        },
        "http_filters": [
            {
                "name": "envoy.filters.http.wasm",
                "typed_config": {
                    "@type": "type.googleapis.com/envoy.extensions.filters.http.wasm.v3.Wasm",
                    "config": {
                        "name": "adaptive-routing",
                        "root_id": "adaptive-routing",
                        "configuration": {
                            "@type": "type.googleapis.com/google.protobuf.StringValue",
                            "value": json.dumps(plugin),
                        },
                        "vm_config": {
                            "vm_id": "adaptive-routing",
                            "runtime": "envoy.wasm.runtime.v8",
                            "code": {"local": {"filename": str(wasm)}},
                        },
                    },
                },
            },
            {
                "name": "envoy.filters.http.router",
                "typed_config": {
                    "@type": "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router"
                },
            },
        ],
    }
    return {
        "static_resources": {
            "listeners": [
                {
                    "name": "test",
                    "address": {
                        "socket_address": {
                            "address": "127.0.0.1",
                            "port_value": gateway_port,
                        }
                    },
                    "filter_chains": [
                        {
                            "filters": [
                                {
                                    "name": "envoy.filters.network.http_connection_manager",
                                    "typed_config": hcm,
                                }
                            ]
                        }
                    ],
                }
            ],
            "clusters": clusters,
        }
    }


@asynccontextmanager
async def gateway(tmp_path, classifier_port, upstream_ports):
    binary = os.environ.get("HIGRESS_ENVOY")
    wasm = os.environ.get("ADAPTIVE_WASM")
    if not binary or not wasm:
        pytest.skip("set HIGRESS_ENVOY and ADAPTIVE_WASM for actual Wasm runtime tests")
    port = free_port()
    config_path = tmp_path / "envoy.yaml"
    config_path.write_text(
        yaml.safe_dump(bootstrap(wasm, port, classifier_port, upstream_ports))
    )
    with (tmp_path / "envoy.log").open("w+") as log:
        process = subprocess.Popen(
            [
                binary,
                "-c",
                str(config_path),
                "--concurrency",
                "1",
                "--log-level",
                "warn",
                "--disable-hot-restart",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            async with aiohttp.ClientSession() as client:
                for _ in range(200):
                    if process.poll() is not None:
                        log.seek(0)
                        pytest.fail(log.read())
                    try:
                        async with client.get(f"http://127.0.0.1:{port}/health"):
                            break
                    except aiohttp.ClientError:
                        await asyncio.sleep(0.05)
                else:
                    pytest.fail("gateway did not start")
            yield f"http://127.0.0.1:{port}", log
        finally:
            process.terminate()
            await asyncio.to_thread(process.wait, 10)


@pytest.mark.asyncio
async def test_real_wasm_routes_sse_and_root_observations(tmp_path):
    received, observations = [], []

    async def classify(request):
        assert request.headers["Authorization"] == "Bearer " + ROUTE_TOKEN
        body = (await request.json())["request"]
        assert body["tenant_id"] == "tenant"
        assert "temperature" not in body
        cluster = body["messages"][0]["content"]
        await asyncio.sleep(0.15)  # Must not be counted as upstream TTFT.
        return web.json_response(
            {
                "schema_version": "1",
                "request_id": body["request_id"],
                "decision_id": str(uuid4()),
                "attempt_id": "initial",
                "model_id": "basic",
                "model_version": "v1",
                "cluster_id": cluster,
                "route_id": f"route-{cluster}",
                "upstream_model": f"alias-{cluster}",
                "propensity": 1,
                "policy_version": "v1",
                "feature_version": "v1",
            }
        )

    async def observe(request):
        assert request.headers["Authorization"] == "Bearer " + OBSERVE_TOKEN
        observations.append(await request.json())
        return web.json_response({"schema_version": "1", "applied": True})

    def upstream(cluster):
        async def handle(request):
            body = await request.json()
            received.append((cluster, body))
            response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
            await response.prepare(request)
            # Include split UTF-8/JSON/delimiters; bytes must survive exactly.
            for offset in range(0, len(STREAM), 13):
                await response.write(STREAM[offset : offset + 13])
                await asyncio.sleep(0.002)
            await response.write_eof()
            return response

        app = web.Application()
        app.router.add_post("/v1/chat/completions", handle)
        return app

    classifier = web.Application()
    classifier.router.add_post("/v1/route", classify)
    classifier.router.add_post("/v1/observations", observe)
    async with (
        serve(classifier) as cport,
        serve(upstream("a")) as a,
        serve(upstream("b")) as b,
    ):
        async with gateway(tmp_path, cport, {"a": a, "b": b}) as (url, _):
            async with aiohttp.ClientSession() as client:
                for cluster in ("a", "b"):
                    async with client.post(
                        url + "/v1/chat/completions",
                        json={
                            "model": "auto",
                            "messages": [{"role": "user", "content": cluster}],
                            "stream": True,
                            "temperature": 0.3,
                        },
                        headers={
                            "x-semantic-route": "route-b",
                            "x-semantic-decision-id": "forged",
                        },
                    ) as response:
                        assert response.status == 200, await response.text()
                        assert response.headers["x-semantic-decision-id"] != "forged"
                        assert await response.read() == STREAM
                for _ in range(100):
                    if len(observations) == 2:
                        break
                    await asyncio.sleep(0.02)
            assert [(c, body["model"]) for c, body in received] == [
                ("a", "alias-a"),
                ("b", "alias-b"),
            ]
            assert all(body["temperature"] == 0.3 for _, body in received)
            assert len(observations) == 2
            for item in observations:
                event = item["event"]
                assert event["outcome"] == "success"
                assert 10 < event["ttft_ms"] <= event["latency_ms"] < 150


@pytest.mark.asyncio
async def test_real_wasm_failures_never_reach_generation(tmp_path):
    received, classified = [], []

    async def classify(request):
        body = (await request.json())["request"]
        case = body["messages"][0]["content"]
        classified.append(case)
        if case == "timeout":
            await asyncio.sleep(0.8)
        if case == "unavailable":
            return web.json_response({}, status=503)
        if case == "malformed":
            return web.Response(text="not JSON")
        return web.json_response(
            {
                "schema_version": "1",
                "request_id": body["request_id"],
                "decision_id": str(uuid4()),
                "attempt_id": "initial",
                "model_id": "basic",
                "model_version": "v1",
                "cluster_id": "a",
                "route_id": "untrusted" if case == "unknown-target" else "route-a",
                "upstream_model": "alias-a",
                "propensity": 1,
                "policy_version": "v1",
                "feature_version": "v1",
            }
        )

    async def upstream(request):
        received.append(await request.read())
        return web.json_response({})

    classifier = web.Application()
    classifier.router.add_post("/v1/route", classify)
    backend = web.Application()
    backend.router.add_post("/v1/chat/completions", upstream)
    async with serve(classifier) as cport, serve(backend) as aport:
        async with gateway(tmp_path, cport, {"a": aport}) as (url, _):
            async with aiohttp.ClientSession() as client:
                for case in (
                    "unavailable",
                    "malformed",
                    "unknown-target",
                    "fixed-model",
                    "timeout",
                ):
                    started = asyncio.get_running_loop().time()
                    async with client.post(
                        url + "/v1/chat/completions",
                        json={
                            "model": "other" if case == "fixed-model" else "auto",
                            "messages": [{"role": "user", "content": case}],
                        },
                    ) as response:
                        assert response.status in (502, 503), await response.text()
                    assert asyncio.get_running_loop().time() - started < 0.75
                for body, status in (
                    (b'{"model":"auto","model":"other","messages":[]}', 400),
                    (b"x" * 262145, 413),
                ):
                    async with client.post(
                        url + "/v1/chat/completions",
                        data=body,
                        headers={"content-type": "application/json"},
                    ) as response:
                        assert response.status == status
            assert len(classified) == 5
            assert received == []


@pytest.mark.asyncio
async def test_real_wasm_upstream_reset_is_not_success(tmp_path):
    observations = []

    async def classify(request):
        body = (await request.json())["request"]
        return web.json_response(
            {
                "schema_version": "1",
                "request_id": body["request_id"],
                "decision_id": str(uuid4()),
                "attempt_id": "initial",
                "model_id": "basic",
                "model_version": "v1",
                "cluster_id": "a",
                "route_id": "route-a",
                "upstream_model": "alias-a",
                "propensity": 1,
                "policy_version": "v1",
                "feature_version": "v1",
            }
        )

    async def observe(request):
        observations.append((await request.json())["event"])
        return web.json_response({"schema_version": "1", "applied": True})

    async def upstream(request):
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        await response.write(b": heartbeat\n\n")
        await asyncio.sleep(0.02)
        request.transport.abort()
        return response

    classifier, backend = web.Application(), web.Application()
    classifier.router.add_post("/v1/route", classify)
    classifier.router.add_post("/v1/observations", observe)
    backend.router.add_post("/v1/chat/completions", upstream)
    async with serve(classifier) as cport, serve(backend) as aport:
        async with gateway(tmp_path, cport, {"a": aport}) as (url, _):
            async with aiohttp.ClientSession() as client:
                try:
                    async with client.post(
                        url + "/v1/chat/completions",
                        json={
                            "messages": [{"role": "user", "content": "a"}],
                            "stream": True,
                        },
                    ) as response:
                        await response.read()
                except aiohttp.ClientError:
                    pass
                for _ in range(100):
                    if observations:
                        break
                    await asyncio.sleep(0.02)
            assert len(observations) == 1
            assert observations[0]["outcome"] == "upstream_error"
            assert observations[0]["ttft_ms"] is None
