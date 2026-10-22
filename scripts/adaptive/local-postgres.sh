#!/usr/bin/env bash
# Disposable loopback PostgreSQL for adaptive-state tests, no container daemon.
set -euo pipefail

action=${1:-start}
pg_root=${ADAPTIVE_PG_ROOT:-/tmp/semantic-router-adaptive-pg}
pg_port=${ADAPTIVE_PG_PORT:-15432}
pg_bin=${PG_BIN:-$(pg_config --bindir)}

case "$action" in start|stop|status) ;; *) echo 'Usage: local-postgres.sh [start|stop|status]' >&2; exit 2 ;; esac
[[ "$pg_root" =~ ^/([A-Za-z0-9_.-]+/)*semantic-router-[A-Za-z0-9_.-]+$ ]] || { echo 'Use an absolute test directory named semantic-router-*.' >&2; exit 2; }
[[ "$pg_port" =~ ^[0-9]+$ ]] && ((pg_port > 1024 && pg_port < 65536)) || { echo 'Invalid test port' >&2; exit 2; }
[[ -x "$pg_bin/initdb" && -x "$pg_bin/pg_ctl" ]] || { echo 'Install PostgreSQL server tools or set PG_BIN.' >&2; exit 2; }

as_pg() {
    if [[ $(id -u) == 0 ]]; then
        runuser -u postgres -- "$@"
    else
        "$@"
    fi
}

if [[ "$action" == start ]]; then
    if [[ $(id -u) == 0 ]]; then
        install -d -m 700 -o postgres -g postgres "$pg_root"
    else
        mkdir -p "$pg_root"
        chmod 700 "$pg_root"
    fi
    if [[ ! -f "$pg_root/data/PG_VERSION" ]]; then
        as_pg "$pg_bin/initdb" -D "$pg_root/data" -U postgres --auth-local=trust --auth-host=trust --encoding=UTF8 --no-locale > "$pg_root/init.log"
    fi
    if ! as_pg "$pg_bin/pg_ctl" -D "$pg_root/data" status > /dev/null 2>&1; then
        as_pg "$pg_bin/pg_ctl" -D "$pg_root/data" -l "$pg_root/server.log" -o "-h 127.0.0.1 -p $pg_port -k $pg_root" -w start
    fi
    "$pg_bin/psql" "postgresql://postgres@127.0.0.1:$pg_port/postgres" -X -v ON_ERROR_STOP=1 -c 'SELECT version();'
    echo "export ADAPTIVE_TEST_DSN=postgresql://postgres@127.0.0.1:$pg_port/postgres"
elif [[ "$action" == stop ]]; then
    as_pg "$pg_bin/pg_ctl" -D "$pg_root/data" -m fast -w stop
else
    as_pg "$pg_bin/pg_ctl" -D "$pg_root/data" status
fi
