"""Download verified etcd tools for optional HA tests (Linux amd64 only).

Usage: uv run python scripts/adaptive/bootstrap_etcd.py --destination /tmp/adaptive-tools
"""

import argparse
import hashlib
import io
import platform
import tarfile
from pathlib import Path
from urllib.request import urlopen

VERSION = "v3.7.2"
SHA256 = "3a3679bc51a4ee9d30bccea1da7cd4fe62c6fc1d2ca1255068d2c53bf3026135"
URL = f"https://github.com/etcd-io/etcd/releases/download/{VERSION}/etcd-{VERSION}-linux-amd64.tar.gz"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    if platform.system() != "Linux" or platform.machine() not in ("x86_64", "amd64"):
        parser.error("this pinned artifact is for Linux amd64")
    with urlopen(URL, timeout=60) as response:
        data = response.read(64 * 1024 * 1024 + 1)
    if hashlib.sha256(data).hexdigest() != SHA256:
        raise SystemExit("etcd archive checksum mismatch")
    args.destination.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(data)) as archive:
        for name in ("etcd", "etcdctl", "etcdutl"):
            source = archive.extractfile(f"etcd-{VERSION}-linux-amd64/{name}")
            if source is None:
                raise SystemExit("etcd archive missing binary")
            target = args.destination / name
            # Copy only named regular file contents, never archive paths/links.
            target.write_bytes(source.read())
            target.chmod(0o755)
    print(f"Verified etcd {VERSION}: {args.destination}")  # noqa: T201


if __name__ == "__main__":
    main()
