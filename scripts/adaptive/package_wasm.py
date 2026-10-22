"""Package a built plugin as a reproducible OCI image layout, without a daemon.

Equivalent payload to FROM scratch + COPY plugin.wasm /plugin.wasm. Nothing is pushed.
"""

import argparse
import gzip
import hashlib
import io
import json
import re
import tarfile
from pathlib import Path


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def package(*, wasm: Path, output: Path, tag: str = "v0.1.0"):
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag):
        raise ValueError("invalid image tag")
    if not 8 <= wasm.stat().st_size <= 64 * 1024 * 1024:
        raise ValueError("invalid plugin size")
    data = wasm.read_bytes()
    if data[:8] != b"\x00asm\x01\x00\x00\x00":
        raise ValueError("not a Wasm 1 module")
    output.mkdir(parents=True, exist_ok=False)
    blobs = output / "blobs/sha256"
    blobs.mkdir(parents=True)

    def blob(content, media_type):
        digest = hashlib.sha256(content).hexdigest()
        (blobs / digest).write_bytes(content)
        return {
            "mediaType": media_type,
            "digest": "sha256:" + digest,
            "size": len(content),
        }

    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        member = tarfile.TarInfo("plugin.wasm")
        member.mode, member.size = 0o644, len(data)
        archive.addfile(member, io.BytesIO(data))
    layer = buffer.getvalue()
    descriptor = blob(
        gzip.compress(layer, mtime=0), "application/vnd.oci.image.layer.v1.tar+gzip"
    )
    config = {
        "architecture": "amd64",
        "os": "linux",
        "config": {},
        "rootfs": {
            "type": "layers",
            "diff_ids": ["sha256:" + hashlib.sha256(layer).hexdigest()],
        },
    }
    config_descriptor = blob(encode(config), "application/vnd.oci.image.config.v1+json")
    manifest = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": config_descriptor,
        "layers": [descriptor],
    }
    manifest_descriptor = blob(encode(manifest), manifest["mediaType"])
    manifest_descriptor["annotations"] = {"org.opencontainers.image.ref.name": tag}
    (output / "index.json").write_bytes(
        encode({"schemaVersion": 2, "manifests": [manifest_descriptor]})
    )
    (output / "oci-layout").write_bytes(encode({"imageLayoutVersion": "1.0.0"}))
    return manifest_descriptor["digest"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wasm", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--tag", default="v0.1.0")
    args = parser.parse_args()
    digest = package(wasm=args.wasm, output=args.output, tag=args.tag)
    print(f"Local OCI image {args.output}:{args.tag} {digest}")  # noqa: T201


if __name__ == "__main__":
    main()
