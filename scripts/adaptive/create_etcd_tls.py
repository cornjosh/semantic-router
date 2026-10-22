"""Create private CA/server/client material and local Secret YAML for optional etcd.

Creates files only. Does not contact Kubernetes or publish private keys.
"""

import argparse
import base64
import re
import subprocess
from pathlib import Path

import yaml


def create(*, destination: Path, namespace: str, release: str, days: int = 365):
    label = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")
    if (
        not label.fullmatch(namespace)
        or len(namespace) > 63
        or not label.fullmatch(release)
        or len(release) > 53
        or not 1 <= days <= 3650
    ):
        raise ValueError("invalid namespace, release or certificate lifetime")
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)

    def run(*args):
        subprocess.run(
            ["openssl", *args],
            cwd=destination,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    run(
        "req",
        "-x509",
        "-newkey",
        "rsa:3072",
        "-nodes",
        "-keyout",
        "ca.key",
        "-out",
        "ca.crt",
        "-subj",
        f"/CN={release}-etcd-ca",
        "-days",
        str(days),
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
    )
    domain = f"{release}-etcd.{namespace}.svc.cluster.local"
    for name, usage, extra in (
        (
            "server",
            "serverAuth,clientAuth",
            f"subjectAltName=DNS:*.{domain},DNS:{domain},DNS:localhost,IP:127.0.0.1\n",
        ),
        ("client", "clientAuth", ""),
    ):
        (destination / f"{name}.ext").write_text(
            "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage="
            + usage
            + "\n"
            + extra
        )
        run(
            "req",
            "-newkey",
            "rsa:3072",
            "-nodes",
            "-keyout",
            f"{name}.key",
            "-out",
            f"{name}.csr",
            "-subj",
            f"/CN={release}-etcd-{name}",
        )
        run(
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
            str(days),
            "-extfile",
            f"{name}.ext",
        )
    secrets = []
    for name in ("server", "client"):
        secrets.append(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": f"adaptive-etcd-{name}", "namespace": namespace},
                "type": "Opaque",
                "data": {
                    key: base64.b64encode((destination / file).read_bytes()).decode()
                    for key, file in {
                        "ca.crt": "ca.crt",
                        "tls.crt": f"{name}.crt",
                        "tls.key": f"{name}.key",
                    }.items()
                },
            }
        )
    manifest = destination / "secrets.yaml"
    manifest.write_text(yaml.safe_dump_all(secrets))
    for path in destination.iterdir():
        path.chmod(0o600)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--namespace", default="adaptive")
    parser.add_argument("--release", default="adaptive")
    parser.add_argument("--days", type=int, default=365)
    args = parser.parse_args()
    path = create(
        destination=args.destination,
        namespace=args.namespace,
        release=args.release,
        days=args.days,
    )
    print(f"Created local TLS Secrets: {path}")  # noqa: T201


if __name__ == "__main__":
    main()
