"""Select storage explicitly: SQLite is standalone; etcd supplies cross-node HA."""

from __future__ import annotations

from collections.abc import Sequence

from semantic_router.adaptive.selection import LoadConfig
from semantic_router.adaptive.state.kv import KVStore
from semantic_router.adaptive.state.store import DurableState


def open_state(
    *,
    backend: str = "sqlite",
    path: str = "/data/adaptive.db",
    endpoints: Sequence[str] = (),
    prefix: str = "/semantic-router/",
    timeout_ms: int = 2000,
    load: LoadConfig | None = None,
    max_pending_per_cluster: int = 4096,
    token_env: str | None = None,
    ca_file: str | None = None,
    cert_file: str | None = None,
    key_file: str | None = None,
) -> DurableState:
    storage: KVStore
    if backend == "sqlite":
        from semantic_router.adaptive.state.sqlite import SQLiteKV

        storage = SQLiteKV(path=path, timeout_ms=timeout_ms)
    elif backend == "etcd":
        from semantic_router.adaptive.state.etcd import EtcdKV

        storage = EtcdKV(
            endpoints=endpoints,
            prefix=prefix,
            timeout_ms=timeout_ms,
            token_env=token_env,
            ca_file=ca_file,
            cert_file=cert_file,
            key_file=key_file,
        )
    else:
        raise ValueError("state backend must be sqlite or etcd")
    return DurableState(
        backend=storage,
        load=load,
        timeout_ms=timeout_ms,
        max_pending_per_cluster=max_pending_per_cluster,
    )
