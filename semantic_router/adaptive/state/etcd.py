"""etcd v3 JSON gateway: linearizable snapshots and revision-based transactions.

No gRPC client dependency and no embedded consensus runtime. Use an application
etcd cluster, with TLS and narrowly scoped authentication in production.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import ssl
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit

import aiohttp

from semantic_router.adaptive.state.base import StateUnavailable
from semantic_router.adaptive.state.kv import Cell, check_keys, encode


def b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


class EtcdKV:
    def __init__(
        self,
        *,
        endpoints: Sequence[str],
        prefix: str = "/semantic-router/",
        timeout_ms: int = 2000,
        token_env: str | None = None,
        ca_file: str | None = None,
        cert_file: str | None = None,
        key_file: str | None = None,
    ):
        if not endpoints or len(endpoints) > 7:
            raise ValueError("configure 1 to 7 etcd endpoints")
        for endpoint in endpoints:
            url = urlsplit(endpoint)
            if (
                url.scheme not in ("http", "https")
                or not url.hostname
                or url.username
                or url.password
                or url.query
                or url.fragment
                or url.path not in ("", "/")
            ):
                raise ValueError("invalid etcd endpoint")
        if not prefix.startswith("/") or not prefix.endswith("/") or len(prefix) > 256:
            raise ValueError("etcd prefix must start and end with /")
        if bool(cert_file) != bool(key_file):
            raise ValueError("etcd TLS certificate and key must be provided together")
        self.endpoints = tuple(e.rstrip("/") for e in endpoints)
        self.prefix, self.timeout_ms, self.token_env = prefix, timeout_ms, token_env
        self._tls = ssl.create_default_context(cafile=ca_file)
        if cert_file and key_file:
            self._tls.load_cert_chain(certfile=cert_file, keyfile=key_file)
        self._session: aiohttp.ClientSession | None = None
        self._endpoint = 0
        self._closed = False

    def _key(self, key: str) -> str:
        return b64((self.prefix + key).encode())

    async def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        if self._closed:
            raise StateUnavailable("shared state closed")
        if self._session is None:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=16, ssl=self._tls),
                trust_env=False,
                auto_decompress=False,
            )
        headers = {"Content-Type": "application/json"}
        if self.token_env:
            token = os.environ.get(self.token_env)
            if not token:
                raise StateUnavailable("etcd credential unavailable")
            headers["Authorization"] = token
        data = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode()
        if len(data) > 1048576:
            raise StateUnavailable("state transaction too large")
        session = self._session

        async def request() -> dict[str, Any]:
            # Retrying a timed-out CAS is safe: if it committed, the unchanged
            # expected revisions fail, and the domain layer re-reads identity.
            for offset in range(len(self.endpoints)):
                index = (self._endpoint + offset) % len(self.endpoints)
                try:
                    async with session.post(
                        self.endpoints[index] + path,
                        data=data,
                        headers=headers,
                        allow_redirects=False,
                        timeout=aiohttp.ClientTimeout(
                            total=self.timeout_ms / 1000 / len(self.endpoints),
                            connect=min(0.3, self.timeout_ms / 1000),
                        ),
                    ) as response:
                        if response.status in (401, 403):
                            raise StateUnavailable("etcd authentication failed")
                        if response.status != 200:
                            continue
                        body = bytearray()
                        async for chunk in response.content.iter_chunked(65536):
                            body.extend(chunk)
                            if len(body) > 8388608:
                                raise StateUnavailable("state response too large")
                        result = json.loads(body)
                        if not isinstance(result, dict) or "error" in result:
                            continue
                        self._endpoint = index
                        return result
                except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
                    continue
            raise StateUnavailable("shared state unavailable")

        try:
            return await asyncio.wait_for(request(), self.timeout_ms / 1000)
        except asyncio.TimeoutError:
            raise StateUnavailable("shared state unavailable") from None

    async def initialize(self) -> None:
        # Schema creation happens through the same atomic domain contract.
        await self.read(["schema"])

    async def read(self, keys: Sequence[str]) -> dict[str, Cell]:
        check_keys(keys)
        if not keys:
            # Still establish quorum; an empty request must not report healthy
            # during an outage merely because no models were requested.
            await self._post("/v3/kv/range", {"key": self._key("schema")})
            return {}
        reply = await self._post(
            "/v3/kv/txn",
            {
                "success": [
                    {"request_range": {"key": self._key(k), "serializable": False}}
                    for k in keys
                ]
            },
        )
        try:
            responses = reply["responses"]
            if len(responses) != len(keys):
                raise ValueError("incomplete transaction")
            cells = {}
            for key, response in zip(keys, responses):
                records = response["response_range"].get("kvs", [])
                if not records:
                    cells[key] = Cell(0, None)
                else:
                    if len(records) != 1 or records[0]["key"] != self._key(key):
                        raise ValueError("unexpected state key")
                    record = records[0]
                    value = json.loads(base64.b64decode(record["value"], validate=True))
                    if not isinstance(value, dict):
                        raise ValueError("invalid state record")
                    cells[key] = Cell(int(record["mod_revision"]), value)
            return cells
        except (KeyError, ValueError, TypeError):
            raise StateUnavailable("invalid state response") from None

    async def commit(
        self, expected: Mapping[str, Cell], writes: Mapping[str, dict[str, Any]]
    ) -> bool:
        check_keys(list(expected))
        if not writes.keys() <= expected.keys():
            raise ValueError("writes require compared keys")
        reply = await self._post(
            "/v3/kv/txn",
            {
                "compare": [
                    {
                        "key": self._key(k),
                        "target": "MOD",
                        "result": "EQUAL",
                        "mod_revision": str(c.revision),
                    }
                    for k, c in expected.items()
                ],
                "success": [
                    {"request_put": {"key": self._key(k), "value": b64(encode(v))}}
                    for k, v in writes.items()
                ],
            },
        )
        # Protobuf JSON omits false. Missing responses on a failed comparison
        # are expected because the failure branch is empty.
        return reply.get("succeeded", False) is True

    async def aclose(self) -> None:
        self._closed = True
        if self._session is not None:
            await self._session.close()
