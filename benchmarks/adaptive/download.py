"""Fetch pinned public data/models and verify every consumed file's SHA256.

Datasets need only the standard library. --models additionally needs modelscope
in the selected Python environment. Never download pickle weight files.
"""

import argparse
import hashlib
import json
import shutil
import tarfile
import tempfile
import urllib.request
from pathlib import Path

PROVENANCE = Path(__file__).parent / "provenance"


def verify(path, digest):
    with path.open("rb") as stream:
        actual = hashlib.file_digest(stream, "sha256").hexdigest()
    if actual != digest:
        raise ValueError(f"SHA256 mismatch: {path}")


def download(url, path, digest):
    if path.exists():
        verify(path, digest)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with (
        urllib.request.urlopen(url, timeout=60) as response,
        tempfile.TemporaryFile() as staged,
    ):
        shutil.copyfileobj(response, staged)
        staged.seek(0)
        if hashlib.file_digest(staged, "sha256").hexdigest() != digest:
            raise ValueError(f"download hash mismatch: {url}")
        staged.seek(0)
        with path.open("xb") as dest:
            shutil.copyfileobj(staged, dest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--model-cache", type=Path)
    parser.add_argument("--models", action="store_true")
    args = parser.parse_args()
    for row in json.loads((PROVENANCE / "datasets.json").read_text()):
        path = (
            args.data_root
            / row["repository"].replace("/", "--")
            / row["revision"]
            / row["file"]
        )
        download(row["url"], path, row["sha256"])
    massive = json.loads((PROVENANCE / "massive.json").read_text())
    archive = args.data_root / "amazon-massive-dataset-1.0.tar.gz"
    download(massive["source"], archive, massive["archive_sha256"])
    with tarfile.open(archive) as package:
        for row in massive["files"]:
            path = args.data_root / "massive" / row["file"]
            if not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                member = package.extractfile(row["file"])
                if member is None:
                    raise ValueError("missing dataset archive member")
                with path.open("xb") as dest:
                    shutil.copyfileobj(member, dest)
            verify(path, row["sha256"])
    if args.models:
        from modelscope import snapshot_download

        for row in json.loads((PROVENANCE / "models.json").read_text()):
            path = Path(
                snapshot_download(
                    model_id=row["model"],
                    revision=row["revision"],
                    cache_dir=str(args.model_cache) if args.model_cache else None,
                    ignore_patterns=["*.bin", ".git*"],
                    max_workers=4,
                )
            )
            for file in row["files"]:
                verify(path / file["path"], file["sha256"])
            print(row["model"], path)
    print("all requested files verified")


if __name__ == "__main__":
    main()
