"""Call explicitly enabled external classifiers; defaults make no network calls."""

import argparse
import asyncio
import sys
from pathlib import Path
from uuid import uuid4

import yaml

from semantic_router.adaptive.config import PipelineConfig
from semantic_router.adaptive.models import Message, RoutingRequest
from semantic_router.adaptive.pipeline import SignalPipeline
from semantic_router.adaptive.providers import ProviderConfig, ProviderRegistry


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("text", nargs="?", default="解释一下二分查找")
    parser.add_argument(
        "--config", type=Path, default=Path(__file__).with_name("external-signals.yaml")
    )
    parser.add_argument(
        "--enable",
        action="append",
        choices=("semantic", "similarity", "llm"),
        default=[],
    )
    args = parser.parse_args()
    data = yaml.safe_load(args.config.read_text())
    config = PipelineConfig.model_validate(data["pipeline"])
    config = config.model_copy(
        update={
            "signals": tuple(
                spec.model_copy(update={"enabled": True})
                if spec.name in args.enable
                else spec
                for spec in config.signals
            )
        }
    )
    providers = ProviderRegistry(
        providers={
            name: ProviderConfig.model_validate(value)
            for name, value in data["providers"].items()
        }
    )
    providers.validate(signals=config.signals)
    pipeline = SignalPipeline(config=config, registry=providers.factories)
    try:
        result = await pipeline.run(
            request=RoutingRequest(
                request_id=str(uuid4()),
                tenant_id="demo",
                messages=(Message(role="user", content=args.text),),
            )
        )
        sys.stdout.write(result.model_dump_json(indent=2) + "\n")
    finally:
        await pipeline.aclose()
        await providers.aclose()


if __name__ == "__main__":
    asyncio.run(main())
