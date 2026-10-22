"""Render deployment profiles with Helm, then validate their application contracts."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from semantic_router.adaptive.service_config import ServiceConfig

ROOT = Path(__file__).resolve().parents[3]
CHART = ROOT / "deploy/adaptive/chart"


def render(*extra):
    helm = os.environ.get("HELM_BIN") or shutil.which("helm")
    if not helm:
        pytest.skip("set HELM_BIN to validate Kubernetes profiles")
    return subprocess.run(
        [
            helm,
            "template",
            "adaptive",
            str(CHART),
            "--namespace",
            "adaptive",
            "--set",
            "image=example.invalid/classifier:test",
            "--set",
            "gateway.wasmUrl=oci://example.invalid/plugin:test",
            *extra,
        ],
        check=False,
        text=True,
        capture_output=True,
    )


@pytest.mark.parametrize("ha", [False, True])
def test_deployment_profiles_preserve_state_and_credential_boundaries(ha):
    result = render(*(["-f", str(CHART / "values-ha.yaml")] if ha else []))
    assert result.returncode == 0, result.stderr
    docs = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    classifier = next(d for d in docs if d["kind"] == "Deployment")
    assert classifier["spec"]["replicas"] == (2 if ha else 1)
    assert classifier["spec"]["strategy"]["type"] == (
        "RollingUpdate" if ha else "Recreate"
    )
    pod = classifier["spec"]["template"]["spec"]
    assert pod["automountServiceAccountToken"] is False
    assert pod["containers"][0]["securityContext"]["readOnlyRootFilesystem"]
    cfg = next(d for d in docs if d["kind"] == "ConfigMap")
    service = ServiceConfig.model_validate(yaml.safe_load(cfg["data"]["service.yaml"]))
    assert service.state.backend == ("etcd" if ha else "sqlite")
    plugin = next(d for d in docs if d["kind"] == "WasmPlugin")["spec"]
    assert plugin["failStrategy"] == "FAIL_CLOSE"
    assert plugin["defaultConfig"]["hosts"] == ["llm.example.com"]
    assert "route_token" not in plugin["defaultConfig"]
    assert all(e["valueFrom"] == "HOST" for e in plugin["vmConfig"]["env"])
    expected = {
        c.route_id: {
            "model_id": c.model_id,
            "cluster_id": c.cluster_id,
            "upstream_model": c.upstream_model,
        }
        for c in service.clusters
    }
    assert plugin["defaultConfig"]["targets"] == expected
    routes = [d for d in docs if d["kind"] == "Ingress"]
    assert len(routes) == len(expected) + 1
    assert {
        d["metadata"]["annotations"].get("higress.io/canary-by-header-value")
        for d in routes
    } == {*expected, None}
    if ha:
        etcd = next(d for d in docs if d["kind"] == "StatefulSet")
        assert etcd["spec"]["replicas"] == 3
        assert len(service.state.endpoints) == 3
        assert all(url.startswith("https://") for url in service.state.endpoints)
        assert (
            service.state.ca_file and service.state.cert_file and service.state.key_file
        )
        assert len(etcd["spec"]["volumeClaimTemplates"]) == 1
        assert pod["affinity"]["podAntiAffinity"][
            "requiredDuringSchedulingIgnoredDuringExecution"
        ]
    else:
        assert not any(d["kind"] == "StatefulSet" for d in docs)
        assert sum(d["kind"] == "PersistentVolumeClaim" for d in docs) == 1


def test_sqlite_multiple_replicas_are_rejected():
    result = render("--set", "replicas=2")
    assert result.returncode != 0
    assert "SQLite requires replicas=1" in result.stderr
