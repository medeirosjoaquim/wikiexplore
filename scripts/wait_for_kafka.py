"""Wait until Kafka brokers are reachable and report metadata.

Uses `confluent-kafka` (maintained). Falls back to a plain TCP socket probe
when the library is unavailable so the script can run from a bare venv
during early setup.
"""
from __future__ import annotations

import argparse
import os
import socket
import sys
import time


def _probe_tcp(bootstrap: str, timeout: float = 5.0) -> bool:
    for broker in bootstrap.split(","):
        broker = broker.strip()
        if not broker:
            continue
        host, _, port = broker.partition(":")
        try:
            with socket.create_connection((host, int(port) or 9092), timeout=timeout):
                return True
        except OSError:
            continue
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Wait for Kafka")
    parser.add_argument("--bootstrap", default=None)
    parser.add_argument("--timeout", default=120, type=int)
    parser.add_argument("--interval", default=3, type=int)
    args = parser.parse_args()

    bootstrap = args.bootstrap or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    try:
        from confluent_kafka.admin import AdminClient  # type: ignore
    except Exception:  # noqa: BLE001
        AdminClient = None  # type: ignore

    deadline = time.time() + args.timeout
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        if AdminClient is not None:
            try:
                admin = AdminClient(
                    {
                        "bootstrap.servers": bootstrap,
                        "socket.timeout.ms": 10000,
                        "request.timeout.ms": 10000,
                    }
                )
                admin.list_topics(timeout=10)  # metadata round-trip
                print(f"[wait_for_kafka] kafka ready (after {attempt} attempt(s))")
                return 0
            except Exception as exc:  # noqa: BLE001
                print(f"[wait_for_kafka] attempt {attempt}: {exc}", file=sys.stderr)
        else:
            if _probe_tcp(bootstrap):
                print(
                    f"[wait_for_kafka] kafka reachable via TCP "
                    f"(after {attempt} attempt(s))"
                )
                return 0
            print(f"[wait_for_kafka] attempt {attempt}: tcp probe failed", file=sys.stderr)
        time.sleep(args.interval)
    print("[wait_for_kafka] timed out", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
