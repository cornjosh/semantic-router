"""Two real HTTP processes share etcd; survive worker exit and restart."""

import asyncio
import os
import socket
import sys
import time

import aiohttp
import pytest

from tests.integration.adaptive.test_app import AUTH, QUALITY_AUTH
from tests.unit.adaptive.test_service import config, request


@pytest.mark.asyncio
async def test_http_workers_share_learning_and_replay_after_worker_death(
    tmp_path, etcd_cluster
):
    cfg = config(
        tmp_path / "unused.db",
        state={
            "backend": "etcd",
            "endpoints": etcd_cluster.endpoints,
            "prefix": "/http-test/" + tmp_path.name + "/",
        },
    )
    path = tmp_path / "service.json"
    path.write_text(cfg.model_dump_json())
    env = {
        **os.environ,
        "GATEWAY_TOKEN": "gateway-secret-at-least-16",
        "FEEDBACK_TOKEN": "quality-secret-at-least-16",
    }
    children = []
    ports = []
    logs = []
    for _ in range(2):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            ports.append(sock.getsockname()[1])

    async def start(index):
        log = (tmp_path / f"worker-{index}-{time.monotonic_ns()}.log").open("wb")
        logs.append(log)
        return await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "semantic_router.adaptive",
            "--config",
            str(path),
            "--port",
            str(ports[index]),
            env=env,
            stdout=log,
            stderr=log,
        )

    async with aiohttp.ClientSession(
        trust_env=False, timeout=aiohttp.ClientTimeout(total=5)
    ) as session:

        async def wait_ready(index):
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if children[index].returncode is not None:
                    pytest.fail(
                        "service exited: "
                        + "\n".join(open(log.name).read()[-2000:] for log in logs)
                    )
                try:
                    async with session.get(
                        f"http://127.0.0.1:{ports[index]}/readyz"
                    ) as response:
                        if response.status == 200:
                            return
                except aiohttp.ClientError:
                    pass
                await asyncio.sleep(0.05)
            pytest.fail("HTTP worker did not become ready")

        async def post(index, path, payload, auth=AUTH):
            async with session.post(
                f"http://127.0.0.1:{ports[index]}" + path, json=payload, headers=auth
            ) as response:
                body = await response.json()
                assert response.status == 200, body
                return body

        try:
            children.extend([await start(0), await start(1)])
            await asyncio.gather(wait_ready(0), wait_ready(1))
            envelope = {
                "schema_version": "1",
                "request": request().model_dump(mode="json"),
            }
            decisions = await asyncio.gather(
                *(post(i % 2, "/v1/route", envelope) for i in range(8))
            )
            assert len({d["decision_id"] for d in decisions}) == 1
            first = decisions[0]
            feedback = {
                "schema_version": "1",
                "tenant_id": "tenant-a",
                "event": {
                    "event_id": "quality-http",
                    "decision_id": first["decision_id"],
                    "model_id": first["model_id"],
                    "model_version": first["model_version"],
                    "policy_version": first["policy_version"],
                    "success": False,
                },
            }
            responses = await asyncio.gather(
                *(post(i % 2, "/v1/feedback", feedback, QUALITY_AUTH) for i in range(8))
            )
            assert sum(r["applied"] for r in responses) == 1
            children[0].kill()
            await children[0].wait()
            assert await post(1, "/v1/route", envelope) == first
            envelope["request"]["request_id"] = "after-death"
            assert (await post(1, "/v1/route", envelope))["model_id"] == "b"
            children[0] = await start(0)
            await wait_ready(0)
            envelope["request"]["request_id"] = "after-restart"
            assert (await post(0, "/v1/route", envelope))["model_id"] == "b"
        finally:
            for child in children:
                if child.returncode is None:
                    child.terminate()
            for child in children:
                try:
                    await asyncio.wait_for(child.wait(), 5)
                except asyncio.TimeoutError:
                    child.kill()
                    await child.wait()
            for log in logs:
                log.close()
