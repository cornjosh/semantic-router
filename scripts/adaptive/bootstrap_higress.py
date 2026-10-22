"""Acquire checksum-pinned Go and/or a real Higress gateway for Linux amd64 tests.

Uses skopeo for OCI downloads; no Docker daemon, mount or privileged container.
Example: uv run python scripts/adaptive/bootstrap_higress.py --destination /tmp/adaptive-tools --go --gateway
"""

import argparse
import hashlib
import json
import platform
import shutil
import subprocess
import tarfile
from pathlib import Path
from urllib.request import urlopen

GO_VERSION = "1.27.3"
GO_SHA256 = "65efbc80fdc1fa2cd2631cad5b481777a68e8725f34e89f50fc6ac714e379b40"
GATEWAY_IMAGE = "higress-registry.cn-hangzhou.cr.aliyuncs.com/higress/gateway"
# Immutable linux/amd64 manifest from the 2.2.5 multi-platform image.
GATEWAY_MANIFEST = "94ffe8340a36f24bef438cd696f623f831ad810c1b862f5215af71db1c82e161"
ENVOY_SHA256 = "b3a229ad3b785c5ebc6a52e49fd942fc5cb5da2eff5d37f6c949afd2939a7ecd"


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def install_go(destination, mirror):
    archive_path = destination / f"go{GO_VERSION}.linux-amd64.tar.gz"
    if not archive_path.exists() or digest(archive_path) != GO_SHA256:
        url = f"{mirror.rstrip('/')}/go{GO_VERSION}.linux-amd64.tar.gz"
        with urlopen(url, timeout=60) as source, archive_path.open("wb") as output:
            shutil.copyfileobj(source, output)
    if digest(archive_path) != GO_SHA256:
        raise SystemExit("Go checksum mismatch")
    with tarfile.open(archive_path) as archive:
        for member in archive:
            target = destination / member.name
            if not target.resolve().is_relative_to(destination.resolve()):
                raise SystemExit("invalid Go archive path")
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif member.isfile():
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(member) as source, target.open("wb") as output:
                    shutil.copyfileobj(source, output)
                target.chmod(member.mode & 0o777)
            else:
                raise SystemExit("unexpected Go archive link")
    subprocess.run([str(destination / "go/bin/go"), "version"], check=True)


def install_gateway(destination, image_cache):
    binary = destination / "envoy"
    if binary.exists() and digest(binary) == ENVOY_SHA256:
        subprocess.run([str(binary), "--version"], check=True)
        return
    manifest_path = image_cache / "manifest.json"
    if not manifest_path.exists() or digest(manifest_path) != GATEWAY_MANIFEST:
        subprocess.run(
            [
                "skopeo",
                "copy",
                "--preserve-digests",
                "--override-os",
                "linux",
                "--override-arch",
                "amd64",
                f"docker://{GATEWAY_IMAGE}@sha256:{GATEWAY_MANIFEST}",
                f"dir:{image_cache}",
            ],
            check=True,
        )
    if digest(manifest_path) != GATEWAY_MANIFEST:
        raise SystemExit("gateway manifest checksum mismatch")
    manifest = json.loads(manifest_path.read_text())
    for layer in manifest["layers"]:
        algorithm, checksum = layer["digest"].split(":")
        if algorithm != "sha256":
            raise SystemExit("unsupported OCI checksum")
        path = image_cache / checksum
        if digest(path) != checksum:
            raise SystemExit("gateway layer checksum mismatch")
        with tarfile.open(path) as archive:
            for member in archive:
                if (
                    member.name.lstrip("./") == "usr/local/bin/envoy"
                    and member.isfile()
                ):
                    with (
                        archive.extractfile(member) as source,
                        binary.open("wb") as output,
                    ):
                        shutil.copyfileobj(source, output)
    if not binary.exists() or digest(binary) != ENVOY_SHA256:
        raise SystemExit("gateway binary checksum mismatch")
    binary.chmod(0o755)
    subprocess.run([str(binary), "--version"], check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", required=True, type=Path)
    parser.add_argument("--go", action="store_true")
    parser.add_argument("--gateway", action="store_true")
    parser.add_argument("--go-mirror", default="https://mirrors.aliyun.com/golang")
    parser.add_argument("--image-cache", type=Path)
    args = parser.parse_args()
    if platform.system() != "Linux" or platform.machine() not in ("amd64", "x86_64"):
        parser.error("pinned test binaries support Linux amd64")
    if not args.go and not args.gateway:
        parser.error("select --go and/or --gateway")
    args.destination.mkdir(parents=True, exist_ok=True)
    if args.go:
        install_go(args.destination, args.go_mirror)
    if args.gateway:
        install_gateway(
            args.destination, args.image_cache or args.destination / "gateway-image"
        )


if __name__ == "__main__":
    main()
