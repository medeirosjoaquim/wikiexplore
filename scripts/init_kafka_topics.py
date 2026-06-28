"""Idempotent Kafka topic creation.

Topics are created (or verified) on every run. Existing topics are left alone
(increasing partitions of a live topic is not done automatically — do that
deliberately).

Uses `confluent-kafka` (maintained) rather than the unmaintained
`kafka-python`, which breaks on Python 3.12 due to its vendored `six`.
"""
from __future__ import annotations

import argparse
import sys
import time

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent))

from _bootstrap import add_backend_to_path  # noqa: E402

add_backend_to_path()

from app.core.config import settings  # noqa: E402

DEFAULT_TOPIC_CONFIG = {
    "retention.ms": 21600000,  # 6h
    "cleanup.policy": "delete",
    "compression.type": "producer",
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Create Kafka topics")
    parser.add_argument("--bootstrap", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--timeout", default=120, type=int)
    args = parser.parse_args()

    from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore
    from confluent_kafka.error import KafkaException  # type: ignore

    conf = {
        "bootstrap.servers": args.bootstrap,
        "socket.timeout.ms": 10000,
        "request.timeout.ms": 10000,
        "metadata.request.timeout.ms": 10000,
    }

    deadline = time.time() + args.timeout
    admin: AdminClient | None = None
    last_exc: Exception | None = None
    while time.time() < deadline:
        try:
            admin = AdminClient(conf)
            # Force a metadata round-trip to confirm reachability.
            admin.list_topics(timeout=10)
            break
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            admin = None
            time.sleep(3)
    if admin is None:
        print(f"[init_kafka] could not connect: {last_exc}", file=sys.stderr)
        return 1

    # Discover existing topics.
    try:
        existing = set(admin.list_topics(timeout=10).topics.keys())
    except Exception as exc:  # noqa: BLE001
        print(f"[init_kafka] failed to list topics: {exc}", file=sys.stderr)
        return 1

    desired = settings.kafka_topics
    to_create: list[NewTopic] = []
    for topic, spec in desired.items():
        partitions = int(spec.get("partitions", 1))
        if topic in existing:
            print(f"[init_kafka] topic exists: {topic} ({partitions} partitions)")
            continue
        to_create.append(
            NewTopic(
                topic=topic,
                num_partitions=partitions,
                replication_factor=1,
                config=dict(DEFAULT_TOPIC_CONFIG),
            )
        )

    if to_create:
        # create_topics returns a dict {name: Future}
        futures = admin.create_topics(to_create)
        for topic, future in futures.items():
            try:
                future.result()  # block until done
                print(f"[init_kafka] created topic: {topic}")
            except KafkaException as exc:
                # TOPIC_ALREADY_EXISTS is safe — another process raced us.
                err = exc.args[0] if exc.args else exc
                code = getattr(err, "code", None)
                name = getattr(code, "name", str(code)) if code is not None else str(err)
                if name == "TOPIC_ALREADY_EXISTS":
                    print(f"[init_kafka] topic already exists: {topic}")
                else:
                    print(f"[init_kafka] FAILED to create {topic}: {err}", file=sys.stderr)
                    return 1
            except Exception as exc:  # noqa: BLE001
                print(f"[init_kafka] FAILED to create {topic}: {exc}", file=sys.stderr)
                return 1
    else:
        print("[init_kafka] all topics already present")

    print("[init_kafka] kafka topics ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
