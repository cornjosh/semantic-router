from pathlib import Path

import pytest
import yaml

from semantic_router.adaptive.config import PipelineConfig
from semantic_router.adaptive.models import RoutingRequest
from semantic_router.adaptive.pipeline import SignalPipeline


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
