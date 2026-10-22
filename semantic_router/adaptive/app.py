"""Bounded aiohttp API with tenant-bound, separate feedback credentials."""

from __future__ import annotations

import asyncio
import hmac
import os
import time
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from aiohttp import web
from pydantic import ValidationError

from semantic_router.adaptive.api import (
    FeedbackEnvelope,
    ObservationEnvelope,
    RouteEnvelope,
)
from semantic_router.adaptive.pipeline import RequiredSignalUnavailable
from semantic_router.adaptive.service import RoutingService, ServiceError
from semantic_router.adaptive.service_config import Principal, ServiceConfig
from semantic_router.adaptive.signals.base import InvalidSignalResult
from semantic_router.adaptive.signals.validation import strict_json
from semantic_router.adaptive.state.base import (
    StateConflict,
    StateNotFound,
    StateUnavailable,
)

SERVICE = web.AppKey("adaptive_service", RoutingService)
PRINCIPAL: Any = getattr(web, "RequestKey", lambda name, kind: name)(
    "adaptive_principal", Principal
)
ROLES = {
    "/v1/route": "route",
    "/v1/observations": "observe",
    "/v1/feedback": "feedback",
    "/metrics": "metrics",
}
Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


def error(code: str, status: int) -> web.Response:
    headers = {"Cache-Control": "no-store"}
    if status in (503, 504):
        headers["Retry-After"] = "1"
    if status == 401:
        headers["WWW-Authenticate"] = "Bearer"
    return web.json_response(
        {"schema_version": "1", "error": {"code": code}}, status=status, headers=headers
    )


class Runtime:
    def __init__(self, config: ServiceConfig):
        self.config = config
        self.credentials: list[tuple[bytes, Principal]] = []
        for principal in config.principals:
            value = os.environ.get(principal.token_env, "")
            if not 16 <= len(value) <= 4096 or any(c.isspace() for c in value):
                raise ValueError("service credential missing or invalid")
            token = value.encode()
            if any(hmac.compare_digest(token, other) for other, _ in self.credentials):
                raise ValueError("service credentials must be distinct")
            self.credentials.append((token, principal))
        self.active = 0
        self.draining = False
        self.requests: Counter[tuple[str, int]] = Counter()
        self.latency_seconds = 0.0

    def authenticate(self, request: web.Request, role: str) -> Principal:
        values = request.headers.getall("Authorization", [])
        if len(values) != 1 or not values[0].startswith("Bearer "):
            raise ServiceError("unauthorized", 401)
        candidate = values[0][7:].encode()
        identity = None
        for token, principal in self.credentials:
            if hmac.compare_digest(token, candidate):
                identity = principal
        if identity is None:
            raise ServiceError("unauthorized", 401)
        if role not in identity.roles:
            raise ServiceError("forbidden", 403)
        return identity

    @web.middleware
    async def middleware(
        self, request: web.Request, handler: Handler
    ) -> web.StreamResponse:
        start = time.monotonic()
        admitted = False
        path = (
            request.path
            if request.path in ROLES or request.path in ("/healthz", "/readyz")
            else "unknown"
        )
        try:
            if path in ROLES:
                request[PRINCIPAL] = self.authenticate(request, ROLES[path])
            if path in ("/v1/route", "/v1/observations", "/v1/feedback"):
                if self.draining or self.active >= self.config.concurrency:
                    raise ServiceError("overloaded", 503)
                self.active += 1
                admitted = True
            response = await asyncio.wait_for(
                handler(request), self.config.request_timeout_ms / 1000
            )
        except ServiceError as exc:
            response = error(exc.code, exc.status)
        except (ValidationError, InvalidSignalResult, UnicodeError):
            response = error("invalid_request", 422)
        except StateConflict:
            response = error("state_conflict", 409)
        except StateNotFound:
            response = error("decision_not_found", 404)
        except StateUnavailable:
            response = error("state_unavailable", 503)
        except RequiredSignalUnavailable:
            response = error("required_signal_unavailable", 503)
        except asyncio.TimeoutError:
            response = error("deadline_exceeded", 504)
        except web.HTTPException as exc:
            response = error("http_error", exc.status)
        except Exception:
            # Never echo exception strings or validation inputs containing prompts,
            # credentials, provider URLs, database paths or upstream responses.
            response = error("internal_error", 500)
        finally:
            if admitted:
                self.active -= 1
        self.requests[path, response.status] += 1
        self.latency_seconds += time.monotonic() - start
        response.headers["Cache-Control"] = "no-store"
        return response

    async def body(self, request: web.Request) -> dict[str, Any]:
        limit = self.config.max_request_bytes
        if request.content_length is not None and request.content_length > limit:
            raise ServiceError("body_too_large", 413)
        if request.headers.get("Content-Encoding", "identity") != "identity":
            raise ServiceError("content_encoding_unsupported", 415)
        if request.content_type != "application/json":
            raise ServiceError("content_type_unsupported", 415)
        data = bytearray()
        async for chunk in request.content.iter_chunked(16384):
            if len(data) + len(chunk) > limit:
                raise ServiceError("body_too_large", 413)
            data.extend(chunk)
        return strict_json(bytes(data))

    @staticmethod
    def tenant(request: web.Request, tenant_id: str) -> None:
        if request[PRINCIPAL].tenant_id != tenant_id:
            raise ServiceError("tenant_mismatch", 403)

    async def route(self, request: web.Request) -> web.Response:
        envelope = RouteEnvelope.model_validate(await self.body(request))
        self.tenant(request, envelope.request.tenant_id)
        decision = await request.app[SERVICE].route(request=envelope.request)
        # Expose only the proxy contract, never internal candidate priors or the
        # request fingerprint. All forwarding destinations are logical IDs.
        return web.json_response(
            {
                "schema_version": "1",
                **decision.model_dump(
                    mode="json",
                    include={
                        "decision_id",
                        "attempt_id",
                        "request_id",
                        "model_id",
                        "model_version",
                        "cluster_id",
                        "route_id",
                        "upstream_model",
                        "propensity",
                        "reason",
                        "signal_status",
                        "accounting",
                    },
                ),
                "policy_version": decision.context.policy_version,
                "feature_version": decision.context.feature_version,
            }
        )

    async def observe(self, request: web.Request) -> web.Response:
        envelope = ObservationEnvelope.model_validate(await self.body(request))
        self.tenant(request, envelope.tenant_id)
        applied = await request.app[SERVICE].state.observe(
            tenant_id=envelope.tenant_id, event=envelope.event
        )
        return web.json_response({"schema_version": "1", "applied": applied})

    async def feedback(self, request: web.Request) -> web.Response:
        envelope = FeedbackEnvelope.model_validate(await self.body(request))
        self.tenant(request, envelope.tenant_id)
        applied = await request.app[SERVICE].state.feedback(
            tenant_id=envelope.tenant_id, event=envelope.event
        )
        return web.json_response({"schema_version": "1", "applied": applied})

    async def health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def ready(self, request: web.Request) -> web.Response:
        if self.draining or not await request.app[SERVICE].state.ready():
            return error("not_ready", 503)
        return web.json_response({"status": "ready"})

    async def metrics(self, request: web.Request) -> web.Response:
        lines = ["# TYPE adaptive_requests_total counter"]
        for (path, status), count in sorted(self.requests.items()):
            lines.append(
                f'adaptive_requests_total{{path="{path}",status="{status}"}} {count}'
            )
        lines += [
            "# TYPE adaptive_requests_active gauge",
            f"adaptive_requests_active {self.active}",
            "# TYPE adaptive_request_duration_seconds_sum counter",
            f"adaptive_request_duration_seconds_sum {self.latency_seconds:.6f}",
        ]
        return web.Response(text="\n".join(lines) + "\n", content_type="text/plain")


def create_app(*, config: ServiceConfig) -> web.Application:
    config = config.model_copy(deep=True)
    runtime = Runtime(config)
    service = RoutingService(config=config)
    app = web.Application(
        middlewares=[runtime.middleware],
        client_max_size=config.max_request_bytes,
        handler_args={"auto_decompress": False},
    )
    app[SERVICE] = service

    async def lifespan(app: web.Application) -> AsyncIterator[None]:
        await service.start()
        try:
            yield
        finally:
            await service.aclose()

    async def drain(app: web.Application) -> None:
        runtime.draining = True

    app.cleanup_ctx.append(lifespan)
    app.on_shutdown.append(drain)
    app.add_routes(
        [
            web.get("/healthz", runtime.health),
            web.get("/readyz", runtime.ready),
            web.get("/metrics", runtime.metrics),
            web.post("/v1/route", runtime.route),
            web.post("/v1/observations", runtime.observe),
            web.post("/v1/feedback", runtime.feedback),
        ]
    )
    return app
