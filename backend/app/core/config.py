"""Centralized configuration loaded from environment / .env."""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

# Walk up from this file to find the project root (where .env lives).
_BASE_DIR = Path(__file__).resolve().parents[3]
for _candidate in (_BASE_DIR / ".env", _BASE_DIR / ".env.local"):
    if _candidate.exists():
        load_dotenv(_candidate, override=False)


def _env(key: str, default: str) -> str:
    return os.getenv(key, default)


class Settings:
    """Application settings.

    Kept as a plain class (not pydantic BaseSettings) so it has no hard
    dependency on a specific settings source and can be imported by scripts
    that run outside the FastAPI process.
    """

    app_env: str = _env("APP_ENV", "development")
    log_level: str = _env("LOG_LEVEL", "INFO")

    # Postgres
    database_url: str = _env(
        "DATABASE_URL",
        "postgresql+psycopg://wikipulse:wikipulse@postgres:5432/wikipulse",
    )
    postgres_host: str = _env("POSTGRES_HOST", "postgres")
    postgres_port: str = _env("POSTGRES_PORT", "5432")
    postgres_db: str = _env("POSTGRES_DB", "wikipulse")
    postgres_user: str = _env("POSTGRES_USER", "wikipulse")
    postgres_password: str = _env("POSTGRES_PASSWORD", "wikipulse")

    # Kafka
    kafka_bootstrap_servers: str = _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic_raw: str = _env("KAFKA_TOPIC_RAW", "wiki.raw")
    kafka_topic_index: str = _env("KAFKA_TOPIC_INDEX", "wiki.index")
    kafka_topic_analytics: str = _env("KAFKA_TOPIC_ANALYTICS", "wiki.analytics")
    kafka_topic_vandalism: str = _env("KAFKA_TOPIC_VANDALISM", "wiki.vandalism")
    kafka_topic_deadletter: str = _env("KAFKA_TOPIC_DEADLETTER", "wiki.deadletter")
    kafka_consumer_group: str = _env("KAFKA_CONSUMER_GROUP", "wikipulse")

    # Elasticsearch
    elasticsearch_url: str = _env("ELASTICSEARCH_URL", "http://elasticsearch:9200")
    es_live_index_prefix: str = _env("ES_LIVE_INDEX_PREFIX", "wiki-live-events")
    es_live_read_alias: str = _env("ES_LIVE_READ_ALIAS", "wiki-live-events-read")
    es_live_write_alias: str = _env("ES_LIVE_WRITE_ALIAS", "wiki-live-events-write")
    es_template_name: str = "wiki-live-events-template"
    es_index_pattern: str = "wiki-live-events-*"
    live_retention_hours: int = int(_env("LIVE_RETENTION_HOURS", "24"))

    # ── Wikimedia EventStreams source ─────────────────────────
    wikimedia_stream_url: str = _env(
        "WIKIMEDIA_STREAM_URL",
        "https://stream.wikimedia.org/v2/stream/recentchange",
    )
    wikimedia_connect_timeout: float = float(_env("WIKIMEDIA_CONNECT_TIMEOUT", "15"))
    wikimedia_read_timeout: float = float(_env("WIKIMEDIA_READ_TIMEOUT", "0"))
    # Drop events whose type is outside this allow-list (None = keep all).
    wikimedia_event_types: str = _env("WIKIMEDIA_EVENT_TYPES", "edit,new,log")

    # ── Streaming services ────────────────────────────────────
    # Producer
    producer_idempotent: bool = _env("PRODUCER_IDEMPOTENT", "true").lower() == "true"
    producer_commit_interval_s: float = float(_env("PRODUCER_POLL_INTERVAL_S", "0.5"))

    # Indexer (ES) batching
    indexer_batch_size: int = int(_env("INDEXER_BATCH_SIZE", "100"))
    indexer_flush_interval_s: float = float(_env("INDEXER_FLUSH_INTERVAL_S", "2.0"))

    # Analytics batching (PG upserts)
    analytics_batch_size: int = int(_env("ANALYTICS_BATCH_SIZE", "200"))
    analytics_flush_interval_s: float = float(_env("ANALYTICS_FLUSH_INTERVAL_S", "2.0"))

    # Vandalism detection threshold (0..1); events at/above are stored.
    vandalism_threshold: float = float(_env("VANDALISM_THRESHOLD", "0.5"))
    vandalism_batch_size: int = int(_env("VANDALISM_BATCH_SIZE", "200"))

    # Consumer poll / session
    consumer_poll_timeout: float = float(_env("CONSUMER_POLL_TIMEOUT", "1.0"))
    consumer_auto_offset_reset: str = _env("CONSUMER_AUTO_OFFSET_RESET", "latest")
    consumer_enable_auto_commit: str = _env("CONSUMER_ENABLE_AUTO_COMMIT", "false")

    # ── Scheduler cadences (seconds) ──────────────────────────
    scheduler_hourly_interval_s: int = int(_env("SCHEDULER_HOURLY_INTERVAL_S", "60"))
    scheduler_reconcile_interval_s: int = int(_env("SCHEDULER_RECONCILE_INTERVAL_S", "120"))
    scheduler_consolidate_interval_s: int = int(_env("SCHEDULER_CONSOLIDATE_INTERVAL_S", "10800"))
    scheduler_cleanup_live_interval_s: int = int(_env("SCHEDULER_CLEANUP_LIVE_INTERVAL_S", "3600"))
    scheduler_cleanup_agg_interval_s: int = int(_env("SCHEDULER_CLEANUP_AGG_INTERVAL_S", "86400"))
    reconcile_hours: int = int(_env("RECONCILE_HOURS", "3"))

    # Live broadcast (in-API consumer -> WebSocket hub)
    live_broadcast_enabled: bool = _env("LIVE_BROADCAST_ENABLED", "true").lower() == "true"
    live_broadcast_group: str = _env("LIVE_BROADCAST_GROUP", "wikipulse-live")
    live_broadcast_max_rate_per_s: int = int(_env("LIVE_BROADCAST_MAX_RATE_PER_S", "50"))

    # Retention
    hourly_retention_days: int = int(_env("HOURLY_RETENTION_DAYS", "90"))
    consolidated_retention_days: int = int(_env("CONSOLIDATED_RETENTION_DAYS", "365"))

    # Consolidation
    consolidation_window_hours: int = int(_env("CONSOLIDATION_WINDOW_HOURS", "3"))

    @property
    def kafka_topics(self) -> dict[str, dict[str, object]]:
        """Topic name -> {partitions, replication_factor, config}."""
        return {
            self.kafka_topic_raw: {"partitions": 3},
            self.kafka_topic_index: {"partitions": 3},
            self.kafka_topic_analytics: {"partitions": 3},
            self.kafka_topic_vandalism: {"partitions": 2},
            self.kafka_topic_deadletter: {"partitions": 1},
        }


settings = Settings()


@lru_cache
def get_settings() -> Settings:
    return settings
