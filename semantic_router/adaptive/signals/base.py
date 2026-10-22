"""Extractor boundary. No policy, model catalog or target is passed here."""

from typing import Protocol

from pydantic import JsonValue

from semantic_router.adaptive.models import RoutingRequest


class Signal(Protocol):
    async def extract(self, request: RoutingRequest) -> dict[str, JsonValue]: ...


class ClosableSignal(Signal, Protocol):
    async def aclose(self) -> None: ...
