"""Real HTTP auth, deadline and wire-contract tests; no paid APIs."""

import asyncio

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from semantic_router.adaptive.app import create_app
from tests.unit.adaptive.test_service import config, request

AUTH = {"Authorization": "Bearer gateway-secret-at-least-16"}
QUALITY_AUTH = {"Authorization": "Bearer quality-secret-at-least-16"}


@pytest_asyncio.fixture
async def client(tmp_path, monkeypatch):
    monkeypatch.setenv("GATEWAY_TOKEN", "gateway-secret-at-least-16")
    monkeypatch.setenv("FEEDBACK_TOKEN", "quality-secret-at-least-16")
    async with TestClient(
        TestServer(create_app(config=config(tmp_path / "state.db")))
    ) as client:
        yield client


@pytest.mark.asyncio
async def test_http_route_contract_observation_feedback_and_health(client):
    for path in ["/healthz", "/readyz"]:
        assert (await client.get(path)).status == 200
    routed = await client.post(
        "/v1/route",
        json={"schema_version": "1", "request": request().model_dump(mode="json")},
        headers=AUTH,
    )
    assert routed.status == 200
    body = await routed.json()
    assert body["schema_version"] == "1"
    assert body["model_id"] == "a" and body["route_id"] in ("east", "west")
    assert body["upstream_model"] == "a-served"
    assert "messages" not in body
    event = {
        "event_id": "event-1",
        "decision_id": body["decision_id"],
        "model_id": "a",
        "model_version": "v1",
        "policy_version": "p1",
        "success": False,
    }
    envelope = {"schema_version": "1", "tenant_id": "tenant-a", "event": event}
    assert (
        await client.post("/v1/feedback", json=envelope, headers=AUTH)
    ).status == 403
    accepted = await client.post("/v1/feedback", json=envelope, headers=QUALITY_AUTH)
    assert accepted.status == 200 and (await accepted.json())["applied"] is True
    duplicate = await client.post("/v1/feedback", json=envelope, headers=QUALITY_AUTH)
    assert duplicate.status == 200 and (await duplicate.json())["applied"] is False
    assert (await client.post("/v1/chat/completions", json={})).status == 404


@pytest.mark.asyncio
async def test_auth_precedes_body_parse_and_tenant_cannot_be_spoofed(client):
    invalid = await client.post("/v1/route", data="{")
    assert invalid.status == 401
    mismatched = await client.post(
        "/v1/route",
        json={
            "schema_version": "1",
            "request": request(tenant_id="tenant-b").model_dump(mode="json"),
        },
        headers=AUTH,
    )
    assert mismatched.status == 403
    assert "messages" not in await mismatched.text()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        '{"schema_version":"1","schema_version":"2"}',
        '{"schema_version":"1","request":{"messages":[NaN]}}',
        '{"password":"DO-NOT-ECHO"}',
    ],
)
async def test_invalid_json_is_bounded_and_sanitized(client, payload):
    response = await client.post(
        "/v1/route", data=payload, headers={**AUTH, "Content-Type": "application/json"}
    )
    assert response.status == 422
    assert "DO-NOT-ECHO" not in await response.text()


@pytest.mark.asyncio
async def test_same_request_id_changed_content_is_http_conflict(client):
    body = {"schema_version": "1", "request": request().model_dump(mode="json")}
    assert (await client.post("/v1/route", json=body, headers=AUTH)).status == 200
    body["request"]["model"] = "b"
    assert (await client.post("/v1/route", json=body, headers=AUTH)).status == 409


@pytest.mark.asyncio
async def test_body_bound_and_state_outage(client):
    response = await client.post("/v1/route", data=b"x" * 300000, headers=AUTH)
    assert response.status == 413
    from semantic_router.adaptive.app import SERVICE

    await client.app[SERVICE].state.aclose()
    assert (await client.get("/readyz")).status == 503
    response = await client.post(
        "/v1/route",
        json={"schema_version": "1", "request": request().model_dump(mode="json")},
        headers=AUTH,
    )
    assert response.status == 503
    assert "state.db" not in await response.text()


@pytest.mark.asyncio
async def test_deadline_cancels_classification_and_admission_is_bounded(
    tmp_path, monkeypatch
):
    from semantic_router.adaptive.app import SERVICE

    monkeypatch.setenv("GATEWAY_TOKEN", "gateway-secret-at-least-16")
    monkeypatch.setenv("FEEDBACK_TOKEN", "quality-secret-at-least-16")
    cfg = config(tmp_path / "state.db", concurrency=1, request_timeout_ms=100)
    app = create_app(config=cfg)
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def stall(**kwargs):
        entered.set()
        try:
            await asyncio.sleep(10)
        finally:
            cancelled.set()

    app[SERVICE].pipeline.run = stall
    envelope = {"schema_version": "1", "request": request().model_dump(mode="json")}
    async with TestClient(TestServer(app)) as client:
        first = asyncio.create_task(
            client.post("/v1/route", json=envelope, headers=AUTH)
        )
        await asyncio.wait_for(entered.wait(), 1)
        overloaded = await client.post("/v1/route", json=envelope, headers=AUTH)
        assert overloaded.status == 503
        assert overloaded.headers["Retry-After"] == "1"
        assert (await first).status == 504
        assert cancelled.is_set()


@pytest.mark.asyncio
async def test_chunked_body_limit_and_compression_rejection(client):
    async def chunks():
        for _ in range(20):
            yield b"x" * 16384

    response = await client.post(
        "/v1/route", data=chunks(), headers={**AUTH, "Content-Type": "application/json"}
    )
    assert response.status == 413
    response = await client.post(
        "/v1/route",
        data=b"not-gzip",
        headers={
            **AUTH,
            "Content-Type": "application/json",
            "Content-Encoding": "gzip",
        },
    )
    assert response.status == 415


@pytest.mark.asyncio
async def test_observation_attribution_and_metrics_access(client):
    from datetime import datetime, timezone

    routed = await client.post(
        "/v1/route",
        json={"schema_version": "1", "request": request().model_dump(mode="json")},
        headers=AUTH,
    )
    decision = await routed.json()
    event = {
        "event_id": "observation-1",
        "decision_id": decision["decision_id"],
        "attempt_id": decision["attempt_id"],
        "cluster_id": decision["cluster_id"],
        "outcome": "success",
        "ttft_ms": None,
        "latency_ms": 500,
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }
    envelope = {"schema_version": "1", "tenant_id": "tenant-a", "event": event}
    accepted = await client.post("/v1/observations", json=envelope, headers=AUTH)
    assert accepted.status == 200 and (await accepted.json())["applied"]
    event["cluster_id"] = "unrelated"
    assert (
        await client.post("/v1/observations", json=envelope, headers=AUTH)
    ).status == 409
    assert (await client.get("/metrics")).status == 401
    assert (await client.get("/metrics", headers=AUTH)).status == 403
