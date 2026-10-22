"""Optional background telemetry; never called synchronously by a route request."""

from __future__ import annotations

import asyncio
import os
from typing import Any
from uuid import uuid4

import aiohttp
from pydantic import Field, field_validator, model_validator

from semantic_router.adaptive.models import Contract, Identifier
from semantic_router.adaptive.providers import ProviderConfig, ProviderFailure
from semantic_router.adaptive.signals.base import InvalidSignalResult
from semantic_router.adaptive.signals.validation import number, strict_json
from semantic_router.adaptive.state.base import StateUnavailable
from semantic_router.adaptive.state.postgres import PostgresState


class TelemetryTarget(Contract):
    cluster_id: Identifier
    enabled: bool = False
    endpoint: str | None = None
    api_key_env: str | None = Field(default=None, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    timeout_ms: int = Field(default=750, gt=0, le=60000, strict=True)
    lease_seconds: float = Field(default=10, gt=0, le=3600)
    max_response_bytes: int = Field(default=65536, ge=64, le=1048576, strict=True)
    load_path: str = Field(
        default="load", pattern=r"^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$", max_length=256
    )
    divisor: float = Field(default=1, gt=0, le=1000000)

    @field_validator("endpoint")
    @classmethod
    def trusted_endpoint(cls, value: str | None) -> str | None:
        return ProviderConfig.trusted_endpoint(value) if value is not None else None

    @model_validator(mode="after")
    def enabled_configuration(self) -> TelemetryTarget:
        if self.enabled and (
            self.endpoint is None or self.lease_seconds <= self.timeout_ms / 1000
        ):
            raise ValueError(
                "enabled telemetry needs an endpoint and lease longer than HTTP timeout"
            )
        return self


class TelemetryPoller:
    def __init__(
        self,
        *,
        store: PostgresState,
        targets: tuple[TelemetryTarget, ...],
        interval_s: float = 5,
        concurrency: int = 4,
    ):
        if not 0 < interval_s <= 3600 or not 1 <= concurrency <= 128:
            raise ValueError("invalid telemetry loop limits")
        if len({target.cluster_id for target in targets}) != len(targets):
            raise ValueError("duplicate telemetry cluster IDs")
        self.store = store
        self.targets = tuple(target.model_copy(deep=True) for target in targets)
        self.interval_s = interval_s
        self._owner = str(uuid4())
        self._slots = asyncio.Semaphore(concurrency)
        self._tick_lock = asyncio.Lock()
        self._session: aiohttp.ClientSession | None = None
        self._closed = False

    async def _fetch(self, target: TelemetryTarget) -> float:
        async with self._slots:
            headers = {"Accept": "application/json", "Accept-Encoding": "identity"}
            if target.api_key_env:
                key = os.environ.get(target.api_key_env, "").strip()
                if not key:
                    raise ProviderFailure("telemetry_credential_unavailable")
                headers["Authorization"] = f"Bearer {key}"
            if self._session is None:
                self._session = aiohttp.ClientSession(
                    trust_env=False, auto_decompress=False
                )
            assert target.endpoint is not None
            async with self._session.get(
                target.endpoint,
                headers=headers,
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(total=target.timeout_ms / 1000),
            ) as response:
                if response.status != 200:
                    raise ProviderFailure("telemetry_http_error")
                if (
                    response.headers.get("Content-Encoding", "identity").lower()
                    != "identity"
                ):
                    raise InvalidSignalResult("telemetry_compressed_response")
                if (
                    response.content_length is not None
                    and response.content_length > target.max_response_bytes
                ):
                    raise InvalidSignalResult("telemetry_response_too_large")
                data = bytearray()
                async for chunk in response.content.iter_chunked(
                    min(target.max_response_bytes + 1, 16384)
                ):
                    if len(data) + len(chunk) > target.max_response_bytes:
                        raise InvalidSignalResult("telemetry_response_too_large")
                    data.extend(chunk)
                value: Any = strict_json(bytes(data))
                for part in target.load_path.split("."):
                    if not isinstance(value, dict) or part not in value:
                        raise InvalidSignalResult("telemetry_value_missing")
                    value = value[part]
                return number(value, maximum=target.divisor) / target.divisor

    async def _poll(self, target: TelemetryTarget) -> str:
        if not target.enabled:
            return "disabled"
        try:
            if not await self.store.claim_poll(
                cluster_id=target.cluster_id,
                owner=self._owner,
                lease_seconds=target.lease_seconds,
            ):
                return "leased"
            load = await asyncio.wait_for(
                self._fetch(target), timeout=target.timeout_ms / 1000
            )
            applied = await self.store.record_active_load(
                cluster_id=target.cluster_id, owner=self._owner, load=load
            )
            return "updated" if applied else "leased"
        except (
            StateUnavailable,
            ProviderFailure,
            InvalidSignalResult,
            aiohttp.ClientError,
            asyncio.TimeoutError,
        ):
            return "unavailable"

    async def poll_once(self) -> dict[str, str]:
        if self._closed:
            raise RuntimeError("telemetry poller closed")
        async with self._tick_lock:
            results = await asyncio.gather(
                *(self._poll(target) for target in self.targets)
            )
            return {
                target.cluster_id: result
                for target, result in zip(self.targets, results)
            }

    async def run(self, *, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.poll_once()
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.interval_s)
            except asyncio.TimeoutError:
                pass

    async def aclose(self) -> None:
        self._closed = True
        if self._session is not None:
            await self._session.close()
