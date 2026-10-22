from pathlib import Path

import pytest
import yaml

from semantic_router.adaptive.config import PipelineConfig
from semantic_router.adaptive.models import RoutingRequest
from semantic_router.adaptive.pipeline import SignalPipeline
from semantic_router.adaptive.providers import ProviderConfig, ProviderRegistry


@pytest.mark.asyncio
async def test_structural_example_selects_candidates_without_external_providers():
    path = Path(__file__).parents[3] / "examples/adaptive/pipeline.yaml"
    config = PipelineConfig.model_validate(yaml.safe_load(path.read_text()))
    config.validate_models(model_ids={"general", "tool-model"})
    pipeline = SignalPipeline(config=config, registry={})
    ordinary = RoutingRequest(
        request_id="plain",
        tenant_id="demo",
        messages=[{"role": "user", "content": "你好"}],
    )
    tool_request = RoutingRequest(
        request_id="tool",
        tenant_id="demo",
        messages=[{"role": "tool", "content": "42", "tool_call_id": "c1"}],
    )
    assert (await pipeline.run(request=ordinary)).match.models == ("general",)
    result = await pipeline.run(request=tool_request)
    assert result.match.models == ("tool-model",)
    assert result.match.shortcut is True
    assert result.skipped_signals == ("semantic",)
    await pipeline.aclose()


@pytest.mark.asyncio
async def test_external_example_defaults_off_but_each_provider_config_is_valid():
    path = Path(__file__).parents[3] / "examples/adaptive/external-signals.yaml"
    data = yaml.safe_load(path.read_text())
    config = PipelineConfig.model_validate(data["pipeline"])
    registry = ProviderRegistry(
        providers={
            name: ProviderConfig.model_validate(value)
            for name, value in data["providers"].items()
        }
    )
    registry.validate(
        signals=[spec.model_copy(update={"enabled": True}) for spec in config.signals]
    )
    pipeline = SignalPipeline(config=config, registry=registry.factories)
    try:
        result = await pipeline.run(
            request=RoutingRequest(
                request_id="example",
                tenant_id="demo",
                messages=[{"role": "user", "content": "write code"}],
            )
        )
        assert tuple(result.signals) == ("request",)
        assert set(result.skipped_signals) == {"semantic", "similarity", "llm"}
    finally:
        await pipeline.aclose()
        await registry.aclose()
