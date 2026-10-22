"""Run the classifier API as one process; Higress forwards generation traffic."""

import argparse
from pathlib import Path

import yaml
from aiohttp import web

from semantic_router.adaptive.app import create_app
from semantic_router.adaptive.service_config import ServiceConfig


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("port must be in 1..65535")
    try:
        raw = args.config.read_bytes()
        if len(raw) > 4 * 1024 * 1024:
            raise ValueError("configuration too large")
        config = ServiceConfig.model_validate(yaml.safe_load(raw))
        app = create_app(config=config)
    except (OSError, ValueError, yaml.YAMLError):
        # Pydantic/YAML diagnostics may include arbitrary configuration values.
        parser.error("invalid or unavailable service configuration/credentials")
    web.run_app(
        app,
        host=args.host,
        port=args.port,
        access_log=None,
        print=None,
        shutdown_timeout=config.request_timeout_ms / 1000 + 5,
    )


if __name__ == "__main__":
    main()
