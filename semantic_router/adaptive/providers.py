"""Bounded HTTP providers, separate from signal configuration and policy."""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Callable, Sequence
from typing import Any, Literal
from urllib.parse import urlsplit

import aiohttp
from pydantic import Field, field_validator

from semantic_router.adaptive.config import SignalSpec
from semantic_router.adaptive.models import Contract
from semantic_router.adaptive.signals.base import InvalidSignalResult, Signal
from semantic_router.adaptive.signals.options import EmbeddingOptions, QuestionOptions
from semantic_router.adaptive.signals.validation import strict_json


class ProviderConfig(Contract):
    protocol: Literal["systemone", "embedding", "llm"]
    endpoint: str = Field(min_length=1, max_length=2048)
    model: str = Field(min_length=1, max_length=256)
    api_key_env: str | None = Field(default=None, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    timeout_ms: int = Field(default=800, gt=0, le=60000, strict=True)
    concurrency: int = Field(default=16, gt=0, le=1024, strict=True)
    max_response_bytes: int = Field(default=1048576, ge=64, le=16777216, strict=True)
    max_request_bytes: int = Field(default=65536, ge=64, le=16777216, strict=True)

    @field_validator("endpoint")
    @classmethod
    def trusted_endpoint(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or any(char.isspace() or ord(char) < 32 for char in value)
        ):
            raise ValueError(
                "endpoint must be an HTTP(S) URL without credentials or query"
            )
        _ = parsed.port  # Validate a malformed port eagerly.
        return value


class ProviderFailure(RuntimeError):
    """Safe error code only; never include remote bodies, URLs or credentials."""


class HTTPProvider:
    def __init__(self, *, config: ProviderConfig):
        self.config = config
        self._slots = asyncio.Semaphore(config.concurrency)
        self._session: aiohttp.ClientSession | None = None
        self._closed = False

    async def post(self, *, body: dict[str, Any]) -> dict[str, Any]:
        # wait_for covers queue time, connect, headers and the complete body.
        # No retry: the signal/pipeline deadline is the authoritative budget.
        return await asyncio.wait_for(
            self._post(body), timeout=self.config.timeout_ms / 1000
        )

    async def _post(self, body: dict[str, Any]) -> dict[str, Any]:
        if self._closed:
            raise ProviderFailure("provider_closed")
        payload = json.dumps(body, ensure_ascii=False, allow_nan=False).encode("utf-8")
        if len(payload) > self.config.max_request_bytes:
            raise InvalidSignalResult("provider_input_too_large")
        async with self._slots:
            if self._closed:
                raise ProviderFailure("provider_closed")
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Accept-Encoding": "identity",
            }
            if self.config.api_key_env:
                key = os.environ.get(self.config.api_key_env, "").strip()
                if not key:
                    raise ProviderFailure("credential_unavailable")
                headers["Authorization"] = f"Bearer {key}"
            if self._session is None:
                self._session = aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(limit=self.config.concurrency),
                    timeout=aiohttp.ClientTimeout(total=self.config.timeout_ms / 1000),
                    trust_env=False,
                    auto_decompress=False,
                )
            try:
                async with self._session.post(
                    self.config.endpoint,
                    data=payload,
                    headers=headers,
                    allow_redirects=False,
                ) as response:
                    if response.status != 200:
                        raise ProviderFailure("provider_http_error")
                    if (
                        response.headers.get("Content-Encoding", "identity").lower()
                        != "identity"
                    ):
                        raise InvalidSignalResult("compressed_response_unsupported")
                    limit = self.config.max_response_bytes
                    if (
                        response.content_length is not None
                        and response.content_length > limit
                    ):
                        raise InvalidSignalResult("provider_response_too_large")
                    data = bytearray()
                    async for chunk in response.content.iter_chunked(
                        min(limit + 1, 16384)
                    ):
                        if len(data) + len(chunk) > limit:
                            raise InvalidSignalResult("provider_response_too_large")
                        data.extend(chunk)
                    return strict_json(bytes(data))
            except asyncio.TimeoutError:
                raise
            except aiohttp.ClientError:
                raise ProviderFailure("provider_network_error") from None

    async def aclose(self) -> None:
        self._closed = True
        if self._session is not None:
            await self._session.close()


class ProviderRegistry:
    """One lazy connection pool and concurrency budget per configured provider.

    Close the registry after closing the pipeline. Constructing either does not
    read credentials or initialize HTTP clients, including for disabled signals.
    """

    def __init__(self, *, providers: dict[str, ProviderConfig]):
        self.providers = {
            name: config.model_copy(deep=True) for name, config in providers.items()
        }
        self._clients: dict[str, HTTPProvider] = {}
        self._closed = False
        self.factories: dict[str, Callable[[SignalSpec], Signal]] = {
            kind: self._create for kind in ("systemone", "embedding", "llm")
        }

    def _resolve(
        self, spec: SignalSpec
    ) -> tuple[EmbeddingOptions | QuestionOptions, ProviderConfig]:
        if spec.kind not in self.factories:
            raise ValueError("unknown provider signal kind")
        options = (
            EmbeddingOptions.model_validate(spec.parameters)
            if spec.kind == "embedding"
            else QuestionOptions.model_validate(spec.parameters)
        )
        config = self.providers.get(options.provider)
        if config is None or config.protocol != spec.kind:
            raise ValueError("provider missing or protocol mismatch")
        return options, config

    def validate(self, *, signals: Sequence[SignalSpec]) -> None:
        """Validate reached configuration without reading keys or creating clients."""
        for spec in signals:
            if spec.enabled:
                self._resolve(spec)

    def _create(self, spec: SignalSpec) -> Signal:
        from semantic_router.adaptive.signals.embedding import EmbeddingSignal
        from semantic_router.adaptive.signals.llm import LLMSignal
        from semantic_router.adaptive.signals.systemone import SystemOneSignal

        if self._closed:
            raise ProviderFailure("provider_registry_closed")
        options, config = self._resolve(spec)
        if options.provider not in self._clients:
            self._clients[options.provider] = HTTPProvider(config=config)
        client = self._clients[options.provider]
        if isinstance(options, EmbeddingOptions):
            return EmbeddingSignal(provider=client, options=options)
        if spec.kind == "systemone":
            return SystemOneSignal(provider=client, options=options)
        return LLMSignal(provider=client, options=options)

    async def aclose(self) -> None:
        self._closed = True
        await asyncio.gather(*(client.aclose() for client in self._clients.values()))
