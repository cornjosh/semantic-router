"""Disposable real three-member etcd; no daemon, Docker, or paid service needed."""

import contextlib
import json
import os
import socket
import subprocess
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import ProxyHandler, Request, build_opener
from uuid import uuid4

import pytest


class EtcdCluster:
    def __init__(self, binary, root):
        self.root = root
        # Reserve all ports together to avoid duplicate ephemeral assignments.
        with contextlib.ExitStack() as stack:
            sockets = [stack.enter_context(socket.socket()) for _ in range(6)]
            for sock in sockets:
                sock.bind(("127.0.0.1", 0))
            ports = [sock.getsockname()[1] for sock in sockets]
        self.endpoints = [f"http://127.0.0.1:{port}" for port in ports[:3]]
        peers = [f"http://127.0.0.1:{port}" for port in ports[3:]]
        initial = ",".join(f"node{i}={peer}" for i, peer in enumerate(peers))
        self.commands = [
            [
                binary,
                "--name",
                f"node{i}",
                "--data-dir",
                str(root / f"node{i}"),
                "--listen-client-urls",
                endpoint,
                "--advertise-client-urls",
                endpoint,
                "--listen-peer-urls",
                peers[i],
                "--initial-advertise-peer-urls",
                peers[i],
                "--initial-cluster",
                initial,
                "--initial-cluster-token",
                "adaptive-test-" + root.name,
                "--log-level",
                "error",
                "--snapshot-count",
                "100",
            ]
            for i, endpoint in enumerate(self.endpoints)
        ]
        self.processes = [None] * 3
        self.logs = [open(root / f"node{i}.log", "ab") for i in range(3)]
        self.http = build_opener(ProxyHandler({}))

    def start(self, index):
        proc = self.processes[index]
        if proc is None or proc.poll() is not None:
            self.processes[index] = subprocess.Popen(
                self.commands[index], stdout=self.logs[index], stderr=self.logs[index]
            )

    def stop(self, index):
        proc = self.processes[index]
        if proc is not None and proc.poll() is None:
            proc.kill()  # Exercise abrupt member loss, including WAL recovery.
            proc.wait(timeout=5)

    def post(self, index, path, payload):
        request = Request(
            self.endpoints[index] + path,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        with self.http.open(request, timeout=0.5) as response:
            return json.load(response)

    def wait(self, indices=(0, 1, 2)):
        deadline = time.monotonic() + 15
        pending = set(indices)
        while pending and time.monotonic() < deadline:
            for index in tuple(pending):
                try:
                    self.post(index, "/v3/kv/range", {"key": "aGVhbHRo"})
                    pending.remove(index)
                except (URLError, OSError, TimeoutError):
                    pass
            if pending:
                time.sleep(0.05)
        if pending:
            logs = [Path(log.name).read_text()[-1500:] for log in self.logs]
            raise RuntimeError(f"etcd members not ready: {pending}, {logs}")

    def leader(self):
        for index in range(3):
            status = self.post(index, "/v3/maintenance/status", {})
            if status["header"]["member_id"] == status["leader"]:
                return index
        raise AssertionError("no etcd leader")

    def close(self):
        for index in range(3):
            self.stop(index)
            self.logs[index].close()


@pytest.fixture(scope="session")
def etcd_cluster(tmp_path_factory):
    binary = os.environ.get("ETCD_BIN")
    if not binary:
        pytest.skip("set ETCD_BIN to run a real three-member etcd cluster")
    root = tmp_path_factory.mktemp("adaptive-etcd-" + uuid4().hex[:8])
    cluster = EtcdCluster(binary, root)
    try:
        for index in range(3):
            cluster.start(index)
        cluster.wait()
        yield cluster
    finally:
        cluster.close()
