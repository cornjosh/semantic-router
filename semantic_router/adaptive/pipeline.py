"""Staged extraction, with policy-controlled early termination between stages."""

import asyncio
import time
from collections.abc import Callable, Mapping
from copy import deepcopy

from pydantic import ValidationError

from semantic_router.adaptive.config import PipelineConfig, SignalSpec
from semantic_router.adaptive.models import (
    PipelineResult,
    PolicyMatch,
    RoutingRequest,
    SignalError,
    SignalResult,
)
from semantic_router.adaptive.policy import Policy
from semantic_router.adaptive.signals.base import Signal
from semantic_router.adaptive.signals.structural import StructuralSignal


class RequiredSignalUnavailable(RuntimeError):
    """A reached required extractor failed; no fallback may hide that failure."""

    def __init__(self, signal: str):
        super().__init__(f"required signal unavailable: {signal}")
        self.signal = signal


class SignalPipeline:
    def __init__(
        self,
        *,
        config: PipelineConfig,
        registry: Mapping[str, Callable[[SignalSpec], Signal]],
    ):
        self.config = config.model_copy(deep=True)
        self.registry = dict(registry)
        self.policy = Policy(config=self.config.policy)
        self._extractors: dict[str, Signal] = {}
        self._closed = False

    async def _extract(
        self, spec: SignalSpec, request: RoutingRequest, deadline: float
    ) -> SignalResult:
        start = time.monotonic()
        error: SignalError | None = None
        try:
            remaining = deadline - start
            if remaining <= 0:
                raise asyncio.TimeoutError
            if spec.name not in self._extractors:
                factory = self.registry.get(spec.kind)
                if factory is None:
                    return SignalResult(
                        name=spec.name,
                        status="unavailable",
                        source=spec.kind,
                        error="not_configured",
                    )
                self._extractors[spec.name] = factory(spec)
            value = await asyncio.wait_for(
                self._extractors[spec.name].extract(request=request),
                timeout=min(spec.timeout_ms / 1000, remaining),
            )
            return SignalResult(
                name=spec.name,
                value=deepcopy(value),
                source=spec.kind,
                elapsed_ms=(time.monotonic() - start) * 1000,
            )
        except asyncio.TimeoutError:
            error = "timeout"
        except ValidationError:
            error = "invalid_result"
        except Exception:
            # Cancellation (BaseException) propagates. Never expose provider error
            # text: libraries sometimes put authorization headers in exceptions.
            error = "provider_error"
        return SignalResult(
            name=spec.name,
            status="unavailable",
            source=spec.kind,
            elapsed_ms=(time.monotonic() - start) * 1000,
            error=error,
        )

    async def run(self, *, request: RoutingRequest) -> PipelineResult:
        if self._closed:
            raise RuntimeError("signal pipeline is closed")
        start = time.monotonic()
        deadline = start + self.config.timeout_ms / 1000
        facts = await StructuralSignal().extract(request=request)
        signals = {
            "request": SignalResult(name="request", value=facts, source="structural")
        }

        def finish(match: PolicyMatch) -> PipelineResult:
            return PipelineResult(
                signals=signals,
                match=match,
                skipped_signals=tuple(
                    spec.name
                    for spec in self.config.signals
                    if spec.name not in signals
                ),
                elapsed_ms=(time.monotonic() - start) * 1000,
            )

        match = self.policy.evaluate(signals=signals)
        if match is not None and match.shortcut:
            return finish(match)
        enabled = [spec for spec in self.config.signals if spec.enabled]
        for stage in sorted({spec.stage for spec in enabled}):
            specs = [spec for spec in enabled if spec.stage == stage]
            results = await asyncio.gather(
                *(self._extract(spec, request, deadline) for spec in specs)
            )
            for spec, result in zip(specs, results):
                signals[spec.name] = result
                if spec.required and result.status != "ok":
                    raise RequiredSignalUnavailable(signal=spec.name)
            match = self.policy.evaluate(signals=signals)
            if match is not None and match.shortcut:
                return finish(match)
        final = self.policy.evaluate(signals=signals, final=True)
        assert final is not None
        return finish(final)

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        closers = [
            getattr(extractor, "aclose")
            for extractor in self._extractors.values()
            if hasattr(extractor, "aclose")
        ]
        # All initialized providers get a chance to release their pooled clients.
        results = await asyncio.gather(
            *(close() for close in closers), return_exceptions=True
        )
        self._extractors.clear()
        if any(isinstance(result, BaseException) for result in results):
            raise RuntimeError("signal provider cleanup failed")
