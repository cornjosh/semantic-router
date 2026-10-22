"""Validate real resources with an isolated Kubernetes API, without worker nodes."""

import json
import os
import ssl
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener

import pytest
import yaml

from tests.integration.adaptive.test_deployment import CHART, ROOT, render
from tests.integration.adaptive.test_higress_wasm import free_port


@pytest.fixture(scope="module")
def kubernetes_api(tmp_path_factory):
    binary, etcd = os.environ.get("KUBE_APISERVER"), os.environ.get("ETCD_BIN")
    if not binary or not etcd:
        pytest.skip("set KUBE_APISERVER and ETCD_BIN for actual Kubernetes validation")
    root = tmp_path_factory.mktemp("adaptive-kubernetes")
    processes, logs = [], []

    def openssl(*args):
        subprocess.run(
            ["openssl", *args],
            cwd=root,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

    openssl(
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        "ca.key",
        "-out",
        "ca.crt",
        "-subj",
        "/CN=adaptive-test-ca",
        "-days",
        "2",
    )
    for name, subject, extensions in (
        (
            "server",
            "/CN=localhost",
            "subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth\n",
        ),
        (
            "client",
            "/CN=adaptive-test/O=system:masters",
            "extendedKeyUsage=clientAuth\n",
        ),
    ):
        (root / f"{name}.ext").write_text(extensions)
        openssl(
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            f"{name}.key",
            "-out",
            f"{name}.csr",
            "-subj",
            subject,
        )
        openssl(
            "x509",
            "-req",
            "-in",
            f"{name}.csr",
            "-CA",
            "ca.crt",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            f"{name}.crt",
            "-days",
            "2",
            "-extfile",
            f"{name}.ext",
        )
    port, etcd_port, peer_port = free_port(), free_port(), free_port()

    def start(name, command):
        log = (root / f"{name}.log").open("wb")
        logs.append(log)
        processes.append(subprocess.Popen(command, stdout=log, stderr=log))

    context = ssl.create_default_context(cafile=str(root / "ca.crt"))
    context.load_cert_chain(str(root / "client.crt"), str(root / "client.key"))
    client = build_opener(ProxyHandler({}), HTTPSHandler(context=context))

    def call(path, body=None):
        request = Request(
            f"https://127.0.0.1:{port}" + path,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Content-Type": "application/json"},
        )
        try:
            with client.open(request, timeout=5) as response:
                data = response.read()
                return json.loads(data) if data.startswith(b"{") else data
        except HTTPError as error:
            raise AssertionError(
                f"Kubernetes {error.code}: {error.read().decode()}"
            ) from error

    try:
        start(
            "etcd",
            [
                etcd,
                "--name=test",
                "--data-dir=" + str(root / "etcd"),
                f"--listen-client-urls=http://127.0.0.1:{etcd_port}",
                f"--advertise-client-urls=http://127.0.0.1:{etcd_port}",
                f"--listen-peer-urls=http://127.0.0.1:{peer_port}",
                f"--initial-advertise-peer-urls=http://127.0.0.1:{peer_port}",
                f"--initial-cluster=test=http://127.0.0.1:{peer_port}",
            ],
        )
        start(
            "apiserver",
            [
                binary,
                "--bind-address=127.0.0.1",
                "--advertise-address=127.0.0.1",
                f"--secure-port={port}",
                f"--etcd-servers=http://127.0.0.1:{etcd_port}",
                "--service-cluster-ip-range=10.250.0.0/24",
                "--authorization-mode=RBAC",
                "--anonymous-auth=false",
                "--client-ca-file=" + str(root / "ca.crt"),
                "--tls-cert-file=" + str(root / "server.crt"),
                "--tls-private-key-file=" + str(root / "server.key"),
                "--service-account-issuer=https://adaptive.test",
                "--service-account-key-file=" + str(root / "server.key"),
                "--service-account-signing-key-file=" + str(root / "server.key"),
                "--allow-privileged=false",
            ],
        )
        for _ in range(300):
            if any(p.poll() is not None for p in processes):
                pytest.fail(
                    "control plane exited: "
                    + "\n".join(open(log.name).read()[-3000:] for log in logs)
                )
            try:
                if call("/readyz") == b"ok":
                    break
            except (URLError, OSError, AssertionError):
                pass
            time.sleep(0.1)
        else:
            pytest.fail("Kubernetes API did not become ready")
        for namespace in ("adaptive", "higress-system"):
            call(
                "/api/v1/namespaces",
                {
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": namespace},
                },
            )
        crd = yaml.safe_load(
            (ROOT / "deploy/adaptive/schemas/wasmplugin-v2.2.5.yaml").read_text()
        )
        call("/apis/apiextensions.k8s.io/v1/customresourcedefinitions", crd)
        for _ in range(100):
            status = call(
                "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/wasmplugins.extensions.higress.io"
            )
            if any(
                c["type"] == "Established" and c["status"] == "True"
                for c in status.get("status", {}).get("conditions", [])
            ):
                break
            time.sleep(0.05)
        yield call
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        for log in logs:
            log.close()


@pytest.mark.parametrize("ha", [False, True])
def test_profiles_accepted_by_real_kubernetes_api(kubernetes_api, ha):
    rendered = render(*(["-f", str(CHART / "values-ha.yaml")] if ha else []))
    assert rendered.returncode == 0, rendered.stderr
    plural = {
        "ConfigMap": "configmaps",
        "Service": "services",
        "Deployment": "deployments",
        "PersistentVolumeClaim": "persistentvolumeclaims",
        "WasmPlugin": "wasmplugins",
        "Ingress": "ingresses",
        "StatefulSet": "statefulsets",
        "PodDisruptionBudget": "poddisruptionbudgets",
        "NetworkPolicy": "networkpolicies",
    }
    for resource in yaml.safe_load_all(rendered.stdout):
        if not resource:
            continue
        version = resource["apiVersion"]
        prefix = "/api/v1" if version == "v1" else "/apis/" + version
        path = (
            prefix
            + "/namespaces/"
            + resource["metadata"]["namespace"]
            + "/"
            + plural[resource["kind"]]
        )
        result = kubernetes_api(path + "?dryRun=All&fieldValidation=Strict", resource)
        assert result["kind"] == resource["kind"]
