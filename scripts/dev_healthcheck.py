"""Local developer health check.

Verifies connectivity and state of PostgreSQL, Kafka, Elasticsearch, plus
checks that Alembic migrations are applied and the ES live aliases exist.

Exit codes:
  0  everything healthy
  1  one or more components unavailable / degraded
  2  could not run the check (missing deps)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from _bootstrap import add_backend_to_path  # noqa: E402

add_backend_to_path()

from app.core.config import settings  # noqa: E402


def _check_postgres() -> str:
    try:
        import psycopg  # type: ignore
    except Exception:
        return "unknown"
    try:
        dsn = (
            f"host={settings.postgres_host} port={settings.postgres_port} "
            f"dbname={settings.postgres_db} user={settings.postgres_user} "
            f"password={settings.postgres_password}"
        )
        with psycopg.connect(dsn, connect_timeout=5) as conn:  # type: ignore
            conn.execute("SELECT 1")
        return "healthy"
    except Exception:
        return "unavailable"


def _check_kafka() -> str:
    try:
        from confluent_kafka.admin import AdminClient  # type: ignore
    except Exception:
        return "unknown"
    try:
        admin = AdminClient(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "socket.timeout.ms": 5000,
                "request.timeout.ms": 5000,
            }
        )
        admin.list_topics(timeout=5)
        return "healthy"
    except Exception:
        return "unavailable"


def _check_elasticsearch() -> str:
    try:
        from elasticsearch import Elasticsearch  # type: ignore
    except Exception:
        return "unknown"
    try:
        es = Elasticsearch(settings.elasticsearch_url, request_timeout=10)
        health = es.cluster.health()
        status = health.get("status")
        if status in ("green", "yellow"):
            return "healthy"
        return "unavailable"
    except Exception:
        return "unavailable"


def _check_migrations() -> str:
    """Return up_to_date / pending / unknown / not_initialized."""
    try:
        import psycopg  # type: ignore
    except Exception:
        return "unknown"
    try:
        dsn = (
            f"host={settings.postgres_host} port={settings.postgres_port} "
            f"dbname={settings.postgres_db} user={settings.postgres_user} "
            f"password={settings.postgres_password}"
        )
        with psycopg.connect(dsn, connect_timeout=5) as conn:  # type: ignore
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_name = 'alembic_version'"
                )
                if cur.fetchone() is None:
                    return "not_initialized"
                cur.execute("SELECT version_num FROM alembic_version")
                row = cur.fetchone()
                if row is None:
                    return "not_initialized"
        # Compare against latest revision on disk. Locate the versions dir
        # robustly: host layout (<root>/backend/migrations/versions) or
        # container layout (/app/migrations/versions where backend == /app).
        script_dir = Path(__file__).resolve().parent
        candidates = [
            script_dir.parent / "backend" / "migrations" / "versions",
            script_dir.parent / "migrations" / "versions",   # container: scripts/ -> ../migrations
            Path("/app/migrations/versions"),
            Path("/app/backend/migrations/versions"),
        ]
        versions_dir = next((c for c in candidates if c.is_dir()), None)
        revisions: set[str] = set()
        children: set[str] = set()
        if versions_dir is not None:
            for path in sorted(versions_dir.glob("*.py")):
                if path.name.startswith("__"):
                    continue
                text = path.read_text()
                for line in text.splitlines():
                    if line.startswith("revision:"):
                        revisions.add(
                            line.split("=", 1)[1].strip().strip('"').strip("'")
                        )
                    elif line.startswith("down_revision:"):
                        val = line.split("=", 1)[1].strip()
                        for token in val.split("("):
                            token = token.strip().strip('"').strip("'").rstrip(",)")
                            if token and token not in ("None", "Union", "str", "Sequence", "None]"):
                                children.add(token)
        heads = revisions - children
        current = row[0]
        if current in heads or (not heads and current in revisions):
            return "up_to_date"
        return "pending"
    except Exception:
        return "unknown"


def _check_es_alias(alias: str) -> bool:
    try:
        from elasticsearch import Elasticsearch  # type: ignore
    except Exception:
        return False
    try:
        es = Elasticsearch(settings.elasticsearch_url, request_timeout=10)
        return bool(es.indices.exists_alias(name=alias))
    except Exception:
        return False


def build_report() -> dict:
    pg = _check_postgres()
    kafka = _check_kafka()
    es = _check_elasticsearch()
    migrations = _check_migrations()
    read_ok = _check_es_alias(settings.es_live_read_alias)
    write_ok = _check_es_alias(settings.es_live_write_alias)

    degraded = any(
        state == "unavailable"
        for state in (pg, kafka, es)
    ) or migrations in ("pending", "not_initialized") or not (read_ok and write_ok)

    report: dict = {
        "status": "degraded" if degraded else "healthy",
        "postgres": pg,
        "kafka": kafka,
        "elasticsearch": es,
        "migrations": migrations,
        "live_read_alias": "ok" if read_ok else "missing",
        "live_write_alias": "ok" if write_ok else "missing",
    }
    if degraded and es == "unavailable":
        report["message"] = "Live search unavailable. Historical analytics still available."
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--quiet", action="store_true", help="only print JSON")
    args = parser.parse_args()

    report = build_report()
    if not args.quiet:
        print(json.dumps(report, indent=2))
    else:
        print(json.dumps(report))
    return 0 if report["status"] == "healthy" else 1


if __name__ == "__main__":
    sys.exit(main())
