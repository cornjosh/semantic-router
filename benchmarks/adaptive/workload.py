"""Deterministic synthetic policy/load/learning workload through real HTTP+SQLite.

Upstream latencies and quality outcomes are scripted, not model measurements.
The 20ms embedding HTTP fixture isolates avoided work in shortcut comparisons.
"""

import argparse
import asyncio
import collections
import hashlib
import json
import os
import subprocess
import tempfile
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import yaml
from aiohttp import web

from benchmarks.adaptive.evaluate import percentiles
from semantic_router.adaptive.app import create_app
from semantic_router.adaptive.service_config import ServiceConfig


@asynccontextmanager
async def serve(app):
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        yield f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
    finally:
        await runner.cleanup()


async def scenario(name, root, samples, warmup):
    calls = 0

    async def embed(request):
        nonlocal calls
        body = await request.json()
        calls += 1
        await asyncio.sleep(0.02)
        return web.json_response(
            {
                "model": "synthetic",
                "data": [
                    {"index": i, "embedding": [1.0, 0.0]}
                    for i in range(len(body["input"]))
                ],
            }
        )

    provider = web.Application()
    provider.router.add_post("/embeddings", embed)
    async with serve(provider) as provider_url:
        cfg = yaml.safe_load(Path("examples/adaptive/service.yaml").read_text())
        cfg["state"]["path"] = str(root / f"{name}.db")
        cfg["context_fields"] = {}  # The same context for every synthetic sample.
        cfg["providers"] = {
            "fixture": {
                "protocol": "embedding",
                "model": "synthetic",
                "endpoint": provider_url + "/embeddings",
            }
        }
        cfg["pipeline"]["signals"] = (
            [
                {
                    "name": "semantic",
                    "kind": "embedding",
                    "required": True,
                    "parameters": {"provider": "fixture"},
                }
            ]
            if name in ("signals", "shortcut")
            else []
        )
        if name == "static":
            cfg["clusters"] = cfg["clusters"][:1]
            cfg["pipeline"]["policy"]["fallback_models"] = ["basic"]
        if name in ("load", "signals"):
            cfg["pipeline"]["policy"]["fallback_models"] = ["basic"]
        os.environ["GATEWAY_TOKEN"] = "benchmark-gateway-local-only"
        os.environ["FEEDBACK_TOKEN"] = "benchmark-feedback-local-only"
        os.environ["METRICS_TOKEN"] = "benchmark-metrics-local-only"
        config = ServiceConfig.model_validate(cfg)
        records, timings, errors, warmup_decisions = [], [], [], []
        async with (
            serve(create_app(config=config)) as url,
            aiohttp.ClientSession() as client,
        ):

            async def post(path, body, feedback=False):
                key = "FEEDBACK_TOKEN" if feedback else "GATEWAY_TOKEN"
                async with client.post(
                    url + path,
                    json=body,
                    headers={"Authorization": "Bearer " + os.environ[key]},
                ) as response:
                    payload = await response.json()
                    if response.status != 200:
                        raise RuntimeError(f"{path} {response.status} {payload}")
                    return payload

            for i in range(warmup + samples):
                request = {
                    "request_id": f"{name}-{i}",
                    "tenant_id": "demo",
                    "model": "auto",
                    "messages": [
                        {"role": "user", "content": "synthetic general question"}
                    ],
                }
                if name == "shortcut":
                    request["tools"] = [
                        {"type": "function", "function": {"name": "weather"}}
                    ]
                start = time.perf_counter()
                try:
                    decision = await post(
                        "/v1/route", {"schema_version": "1", "request": request}
                    )
                except RuntimeError as error:
                    errors.append(str(error))
                    continue
                duration = (time.perf_counter() - start) * 1000
                cluster = decision["cluster_id"]
                if i < warmup:
                    warmup_decisions.append(
                        {"model": decision["model_id"], "cluster": cluster}
                    )
                # east becomes slow; west stays fast. Costs are synthetic input,
                # never presented as observed generation latency or quality.
                ttft = 2000.0 if cluster == "basic-east" else 100.0
                latency = ttft + 1000.0
                event = {
                    "event_id": f"obs-{name}-{i}",
                    "decision_id": decision["decision_id"],
                    "attempt_id": decision["attempt_id"],
                    "cluster_id": cluster,
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                    "outcome": "success",
                    "ttft_ms": ttft,
                    "latency_ms": latency,
                }
                await post(
                    "/v1/observations",
                    {"schema_version": "1", "tenant_id": "demo", "event": event},
                )
                if name == "learning":
                    event = {
                        "event_id": f"quality-{i}",
                        "decision_id": decision["decision_id"],
                        "model_id": decision["model_id"],
                        "model_version": decision["model_version"],
                        "policy_version": decision["policy_version"],
                        "success": decision["model_id"] == "reasoning",
                    }
                    await post(
                        "/v1/feedback",
                        {"schema_version": "1", "tenant_id": "demo", "event": event},
                        True,
                    )
                if i >= warmup:
                    timings.append(duration)
                    records.append(
                        {
                            "model": decision["model_id"],
                            "cluster": cluster,
                            "synthetic_ttft_ms": ttft,
                        }
                    )
                if i == warmup - 1:
                    calls = 0
        return {
            "samples": len(records),
            "warmup": warmup,
            "concurrency": 1,
            "http_route_latency": percentiles(timings),
            "errors": errors,
            "provider_calls_measured": calls,
            "models": dict(collections.Counter(r["model"] for r in records)),
            "clusters": dict(collections.Counter(r["cluster"] for r in records)),
            "mean_synthetic_selected_ttft_ms": sum(
                r["synthetic_ttft_ms"] for r in records
            )
            / len(records),
            "first_decisions": records[:5],
            "warmup_decisions": warmup_decisions,
        }


async def main(args):
    with tempfile.TemporaryDirectory(
        prefix="adaptive-benchmark-", dir="/tmp"
    ) as directory:
        output = {
            "source_commit": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], text=True
            ).strip(),
            "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "store": "SQLite on local /tmp, default durable transactions",
            "fixture_delay_ms": 20,
            "quality": "scripted only, no real answer-quality claims",
            "scenarios": {},
        }
        for name in ["static", "load", "learning", "signals", "shortcut"]:
            output["scenarios"][name] = await scenario(
                name, Path(directory), args.samples, args.warmup
            )
            print(name, output["scenarios"][name], flush=True)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(output, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=200)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--output", type=Path, required=True)
    asyncio.run(main(parser.parse_args()))
