"""Wait until PostgreSQL accepts connections.

Exits non-zero after `--timeout` seconds. Uses psycopg because it is already a
backend dependency (no extra installation required).
"""
from __future__ import annotations

import argparse
import sys
import time

try:
    import psycopg  # type: ignore
except Exception:  # pragma: no cover - exercised in environments w/o psycopg
    psycopg = None  # type: ignore


def main() -> int:
    parser = argparse.ArgumentParser(description="Wait for PostgreSQL")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", default=None, type=int)
    parser.add_argument("--db", default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--timeout", default=60, type=int, help="seconds")
    parser.add_argument("--interval", default=2, type=int, help="seconds between retries")
    args = parser.parse_args()

    import os

    host = args.host or os.getenv("POSTGRES_HOST", "localhost")
    port = args.port or int(os.getenv("POSTGRES_PORT", "5432"))
    db = args.db or os.getenv("POSTGRES_DB", "wikipulse")
    user = args.user or os.getenv("POSTGRES_USER", "wikipulse")
    password = args.password or os.getenv("POSTGRES_PASSWORD", "wikipulse")

    if psycopg is None:
        print("[wait_for_postgres] psycopg not installed", file=sys.stderr)
        return 2

    deadline = time.time() + args.timeout
    dsn = f"host={host} port={port} dbname={db} user={user} password={password}"
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            with psycopg.connect(dsn, connect_timeout=5) as conn:  # type: ignore
                conn.execute("SELECT 1")
            print(f"[wait_for_postgres] postgres ready (after {attempt} attempt(s))")
            return 0
        except Exception as exc:  # noqa: BLE001
            print(
                f"[wait_for_postgres] attempt {attempt}: {exc}",
                file=sys.stderr,
            )
            time.sleep(args.interval)
    print("[wait_for_postgres] timed out waiting for postgres", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
