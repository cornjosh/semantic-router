"""Actual Higress/Wasm versus direct fixture latency, throughput and SSE bytes.

Uses the integration harness (dev dependencies), not a mock Wasm host. Requires
HIGRESS_ENVOY and ADAPTIVE_WASM. No external generation service or paid API.
"""

import argparse
import asyncio
import hashlib
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path

import aiohttp
import yaml
from aiohttp import web

from benchmarks.adaptive.evaluate import percentiles
from semantic_router.adaptive.app import create_app
from semantic_router.adaptive.service_config import ServiceConfig
from tests.integration.adaptive.test_higress_wasm import gateway, serve

CHUNK = b'data: {"choices":[{"delta":{"content":"fixture"}}]}\n\n'
STREAM = CHUNK + b"data: [DONE]\n\n"


async def measure(client, url, count, concurrency, prefix):
    slots = asyncio.Semaphore(concurrency)

    async def one(i):
        async with slots:
            start = time.perf_counter()
            first = None
            body = bytearray()
            async with client.post(
                url + "/v1/chat/completions",
                headers={"x-request-id": f"{prefix}-{i}"},
                json={
                    "model": "auto",
                    "stream": True,
                    "messages": [{"role": "user", "content": "benchmark"}],
                },
            ) as response:
                async for chunk in response.content.iter_any():
                    if chunk and first is None:
                        first = (time.perf_counter() - start) * 1000
                    body.extend(chunk)
                return {
                    "ttft_ms": first,
                    "total_ms": (time.perf_counter() - start) * 1000,
                    "ok": response.status == 200 and bytes(body) == STREAM,
                }

    start = time.perf_counter()
    rows = await asyncio.gather(*(one(i) for i in range(count)))
    elapsed = time.perf_counter() - start
    good = [row for row in rows if row["ok"]]
    if not good:
        raise RuntimeError("no successful gateway samples")
    return {
        "samples": count,
        "concurrency": concurrency,
        "errors": count - len(good),
        "elapsed_s": elapsed,
        "requests_per_second": count / elapsed,
        "client_ttft": percentiles([row["ttft_ms"] for row in good]),
        "client_total": percentiles([row["total_ms"] for row in good]),
        "byte_identical_streams": len(good),
    }


async def main(args):
    for name in ("HIGRESS_ENVOY", "ADAPTIVE_WASM"):
        if not os.environ.get(name):
            raise ValueError(f"{name} is required")

    async def upstream(request):
        await request.json()
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        await asyncio.sleep(0.02)
        await response.write(CHUNK)
        await asyncio.sleep(0.002)
        await response.write(STREAM[len(CHUNK) :])
        await response.write_eof()
        return response

    app = web.Application()
    app.router.add_post("/v1/chat/completions", upstream)
    with tempfile.TemporaryDirectory(
        prefix="adaptive-gateway-bench-", dir="/tmp"
    ) as directory:
        root = Path(directory)
        cfg = yaml.safe_load(Path("examples/adaptive/service.yaml").read_text())
        cfg["state"]["path"] = str(root / "state.db")
        cfg["pipeline"]["policy"]["fallback_models"] = ["basic"]
        os.environ["GATEWAY_TOKEN"] = "benchmark-gateway-local-only"
        os.environ["FEEDBACK_TOKEN"] = "benchmark-feedback-local-only"
        os.environ["METRICS_TOKEN"] = "benchmark-metrics-local-only"
        config = ServiceConfig.model_validate(cfg)
        plugin = {
            "tenant_id": "demo",
            "timeout_ms": 5000,
            "route_token": os.environ["GATEWAY_TOKEN"],
            "observe_token": os.environ["GATEWAY_TOKEN"],
            "targets": {
                c.route_id: {
                    "model_id": c.model_id,
                    "cluster_id": c.cluster_id,
                    "upstream_model": c.upstream_model,
                }
                for c in config.clusters
            },
        }
        async with (
            serve(app) as upstream_port,
            serve(create_app(config=config)) as classifier_port,
        ):
            async with gateway(
                root,
                classifier_port,
                {c.cluster_id: upstream_port for c in config.clusters},
                plugin,
            ) as (proxy_url, _):
                result = {
                    "source_commit": subprocess.check_output(
                        ["git", "rev-parse", "HEAD"], text=True
                    ).strip(),
                    "script_sha256": hashlib.sha256(
                        Path(__file__).read_bytes()
                    ).hexdigest(),
                    "wasm_sha256": hashlib.sha256(
                        Path(os.environ["ADAPTIVE_WASM"]).read_bytes()
                    ).hexdigest(),
                    "envoy_sha256": hashlib.sha256(
                        Path(os.environ["HIGRESS_ENVOY"]).read_bytes()
                    ).hexdigest(),
                    "fixture": "20ms to first content,2ms to DONE; no model inference",
                    "warmup_per_mode": 20,
                    "modes": {},
                }
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as client:
                    for concurrency in (1, 8):
                        for name, url in [
                            ("direct", f"http://127.0.0.1:{upstream_port}"),
                            ("higress_wasm_classifier", proxy_url),
                        ]:
                            key = f"{name}_c{concurrency}"
                            await measure(client, url, 20, concurrency, key + "warm")
                            result["modes"][key] = await measure(
                                client, url, args.samples, concurrency, key
                            )
                            print(key, result["modes"][key], flush=True)
                    # Give the root-owned observation queue time to drain before cleanup.
                    await asyncio.sleep(0.2)
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=200)
    parser.add_argument("--output", type=Path, required=True)
    asyncio.run(main(parser.parse_args()))
