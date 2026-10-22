"""The chart's generated TLS material works with actual etcd and its JSON API."""

import asyncio
import os
import subprocess

import pytest

from scripts.adaptive.create_etcd_tls import create
from semantic_router.adaptive.state.base import StateUnavailable
from semantic_router.adaptive.state.factory import open_state
from tests.integration.adaptive.test_higress_wasm import free_port


@pytest.mark.asyncio
async def test_generated_etcd_tls_requires_client_certificate(tmp_path):
    binary = os.environ.get("ETCD_BIN")
    if not binary:
        pytest.skip("set ETCD_BIN for actual TLS endpoint verification")
    tls = tmp_path / "tls"
    manifest = create(destination=tls, namespace="adaptive", release="adaptive", days=2)
    assert manifest.stat().st_mode & 0o777 == 0o600
    assert tls.stat().st_mode & 0o777 == 0o700
    port, peer = free_port(), free_port()
    url = f"https://127.0.0.1:{port}"
    peer_url = f"https://127.0.0.1:{peer}"
    command = [
        binary,
        "--name=test",
        "--data-dir=" + str(tmp_path / "data"),
        "--listen-client-urls=" + url,
        "--advertise-client-urls=" + url,
        "--listen-peer-urls=" + peer_url,
        "--initial-advertise-peer-urls=" + peer_url,
        "--initial-cluster=test=" + peer_url,
        "--client-cert-auth=true",
        "--peer-client-cert-auth=true",
    ]
    for prefix in ("", "peer-"):
        for option, filename in (
            ("cert-file", "server.crt"),
            ("key-file", "server.key"),
            ("trusted-ca-file", "ca.crt"),
        ):
            command.append(f"--{prefix}{option}={tls / filename}")
    state = open_state(
        backend="etcd",
        endpoints=[url],
        prefix="/tls-test/",
        ca_file=str(tls / "ca.crt"),
        cert_file=str(tls / "client.crt"),
        key_file=str(tls / "client.key"),
    )
    anonymous = open_state(
        backend="etcd", endpoints=[url], ca_file=str(tls / "ca.crt"), timeout_ms=300
    )
    with (tmp_path / "etcd.log").open("wb") as log:
        process = subprocess.Popen(command, stdout=log, stderr=log)
        try:
            for _ in range(100):
                assert process.poll() is None
                try:
                    await state.initialize()
                    break
                except StateUnavailable:
                    pass
                await asyncio.sleep(0.03)
            assert await state.ready()
            await state.initialize()
            assert await state.claim_poll(
                cluster_id="test", owner="owner", lease_seconds=30
            )
            assert not await state.claim_poll(
                cluster_id="test", owner="other", lease_seconds=30
            )
            with pytest.raises(StateUnavailable):
                await anonymous.ready()
        finally:
            await state.aclose()
            await anonymous.aclose()
            process.terminate()
            await asyncio.to_thread(process.wait, 5)
