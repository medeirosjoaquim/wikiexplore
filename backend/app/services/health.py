"""Health report builder used by the `/health` endpoint.

The app NEVER mutates schema here. If the alembic_version table is missing,
migrations are reported as `not_initialized` so the operator sees a clear
error in `/health` rather than silent `CREATE TABLE` behaviour.
"""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings


def _check_postgres(db: Session) -> str:
    try:
        db.execute(text("SELECT 1"))
        return "healthy"
    except Exception:
        return "unavailable"


def _check_kafka() -> str:
    try:
        from confluent_kafka.admin import AdminClient  # type: ignore

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


def _check_elasticsearch() -> tuple[str, bool, bool]:
    """Return (status, read_alias_ok, write_alias_ok)."""
    try:
        from elasticsearch import Elasticsearch  # type: ignore

        es = Elasticsearch(settings.elasticsearch_url, request_timeout=10)
        health = es.cluster.health()
        status = health.get("status")
        if status not in ("green", "yellow"):
            return "unavailable", False, False
        read_ok = bool(es.indices.exists_alias(name=settings.es_live_read_alias))
        write_ok = bool(es.indices.exists_alias(name=settings.es_live_write_alias))
        return "healthy", read_ok, write_ok
    except Exception:
        return "unavailable", False, False


def _check_migrations(db: Session) -> str:
    try:
        row = db.execute(
            text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_name = 'alembic_version'"
            )
        ).first()
        if row is None:
            return "not_initialized"
        version = db.execute(text("SELECT version_num FROM alembic_version")).first()
        if version is None:
            return "not_initialized"
        return "up_to_date"
    except Exception:
        return "unknown"


def build_health_report(db: Session) -> dict:
    pg = _check_postgres(db)
    kafka = _check_kafka()
    es, read_ok, write_ok = _check_elasticsearch()
    migrations = _check_migrations(db)

    degraded = (
        pg != "healthy"
        or kafka != "healthy"
        or es != "healthy"
        or migrations != "up_to_date"
        or not (read_ok and write_ok)
    )

    report: dict = {
        "status": "degraded" if degraded else "healthy",
        "postgres": pg,
        "kafka": kafka,
        "elasticsearch": es,
        "migrations": migrations,
        "live_read_alias": "ok" if read_ok else "missing",
        "live_write_alias": "ok" if write_ok else "missing",
    }
    if es != "healthy":
        report["message"] = "Live search unavailable. Historical analytics still available."
    elif migrations != "up_to_date":
        report["message"] = (
            "Database migrations are not applied. Run `make migrate` then restart."
        )
    elif not (read_ok and write_ok):
        report["message"] = "Elasticsearch live aliases missing. Run `make es-init`."
    return report
