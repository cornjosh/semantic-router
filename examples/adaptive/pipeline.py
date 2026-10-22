"""Run the structural-only example without model credentials or an HTTP server."""

import argparse
import asyncio
import sys
from pathlib import Path
from uuid import uuid4

import yaml

from semantic_router.adaptive.config import PipelineConfig
from semantic_router.adaptive.models import Message, RoutingRequest
from semantic_router.adaptive.pipeline import SignalPipeline


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("text", nargs="?", default="解释一下二分查找")
    parser.add_argument("--tool-result", action="store_true")
    args = parser.parse_args()
    config = PipelineConfig.model_validate(
        yaml.safe_load(Path(__file__).with_name("pipeline.yaml").read_text())
    )
    config.validate_models(model_ids={"general", "tool-model"})
    pipeline = SignalPipeline(config=config, registry={})
    message = (
        Message(role="tool", content=args.text, tool_call_id="example-call")
        if args.tool_result
        else Message(role="user", content=args.text)
    )
    try:
        result = await pipeline.run(
            request=RoutingRequest(
                request_id=str(uuid4()), tenant_id="demo", messages=(message,)
            )
        )
        sys.stdout.write(result.model_dump_json(indent=2) + "\n")
    finally:
        await pipeline.aclose()


if __name__ == "__main__":
    asyncio.run(main())
