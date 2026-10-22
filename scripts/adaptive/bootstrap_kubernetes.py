"""Download checksum-pinned Helm and Kubernetes API binaries for local validation."""

import argparse
import hashlib
import platform
import shutil
import subprocess
import tarfile
from pathlib import Path
from urllib.request import urlopen

HELM_VERSION = "3.19.0"
HELM_SHA256 = "a7f81ce08007091b86d8bd696eb4d86b8d0f2e1b9f6c714be62f82f96a594496"
KUBERNETES_VERSION = "1.34.1"
API_SHA256 = "a170577ea20f8c37a522114a259c36ec4ed397482d8f282c265248dedf2216c3"


def download(url, path, expected):
    if not path.exists() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        with urlopen(url, timeout=60) as source, path.open("wb") as output:
            shutil.copyfileobj(source, output)
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise SystemExit("tool checksum mismatch")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    if platform.system() != "Linux" or platform.machine() not in ("x86_64", "amd64"):
        parser.error("these pinned binaries target Linux amd64")
    args.destination.mkdir(parents=True, exist_ok=True)
    archive = args.destination / "helm.tar.gz"
    download(
        f"https://get.helm.sh/helm-v{HELM_VERSION}-linux-amd64.tar.gz",
        archive,
        HELM_SHA256,
    )
    helm = args.destination / "helm"
    with tarfile.open(archive) as source:
        with source.extractfile("linux-amd64/helm") as entry, helm.open("wb") as output:
            shutil.copyfileobj(entry, output)
    helm.chmod(0o755)
    server = args.destination / "kube-apiserver"
    download(
        f"https://dl.k8s.io/release/v{KUBERNETES_VERSION}/bin/linux/amd64/kube-apiserver",
        server,
        API_SHA256,
    )
    server.chmod(0o755)
    subprocess.run([str(helm), "version", "--short"], check=True)
    subprocess.run([str(server), "--version"], check=True)


if __name__ == "__main__":
    main()
