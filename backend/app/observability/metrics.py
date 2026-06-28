"""Prometheus metrics for the WikiPulse pipeline.

Each process (api + one per streaming service) imports this module, which
registers a shared set of counters/histograms/gauges on the default registry
and exposes ``/metrics`` on its own port. Labels (service, stage, topic, ...)
let Grafana slice throughput and latency per concern.
"""
from __future__ import annotations

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    Info,
    generate_latest,
    start_http_server,
)

from app.core.config import settings

REGISTRY: CollectorRegistry = CollectorRegistry()
Info("wikipulse", "WikiPulse observability", registry=REGISTRY).info(
    {"app": "wikipulse", "env": settings.otel_resource_environment}
)

# ── throughput ──────────────────────────────────────────────────────────────
PRODUCER_EVENTS = Counter(
    "wikipulse_producer_events_total",
    "Wikimedia events seen by the producer",
    ["outcome"],  # published | deadlettered | error
    registry=REGISTRY,
)
DEMUX_FORWARDED = Counter(
    "wikipulse_demux_forwarded_total",
    "Events republished by the demux",
    ["topic"],
    registry=REGISTRY,
)
INDEXER_EVENTS = Counter(
    "wikipulse_indexer_events_total",
    "Events bulk-indexed into Elasticsearch",
    ["outcome"],  # indexed | error
    registry=REGISTRY,
)
ANALYTICS_APPLIED = Counter(
    "wikipulse_analytics_events_total",
    "Events applied to PostgreSQL aggregates",
    ["outcome"],  # applied | error
    registry=REGISTRY,
)
VANDALISM_EVENTS = Counter(
    "wikipulse_vandalism_events_total",
    "Events scored by the vandalism consumer",
    ["outcome"],  # flagged | skipped
    registry=REGISTRY,
)
SCHEDULER_RUNS = Counter(
    "wikipulse_scheduler_runs_total",
    "Scheduler job executions",
    ["job", "outcome"],  # job in {hourly,reconcile,consolidate,cleanup_live,cleanup_agg}
    registry=REGISTRY,
)

# ── latency / size distributions ────────────────────────────────────────────
STAGE_SECONDS = Histogram(
    "wikipulse_event_processing_seconds",
    "Wall-clock time spent in a pipeline stage",
    ["stage"],  # sse_parse | kafka_produce | demux | es_bulk | pg_upsert | vandalism_score | reconcile | consolidate
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
    registry=REGISTRY,
)
KAFKA_POLL_BATCH = Histogram(
    "wikipulse_kafka_poll_batch_size",
    "Messages drained per consumer poll",
    ["consumer"],
    buckets=(1, 5, 10, 25, 50, 100, 200, 500, 1000),
    registry=REGISTRY,
)
BULK_INDEX_BATCH = Histogram(
    "wikipulse_bulk_index_batch_size",
    "Documents per Elasticsearch bulk request",
    buckets=(1, 10, 25, 50, 100, 200, 500, 1000),
    registry=REGISTRY,
)
UPSERT_BATCH = Histogram(
    "wikipulse_upsert_batch_size",
    "Rows per PostgreSQL upsert statement",
    ["table"],
    buckets=(1, 5, 10, 25, 50, 100, 200, 500, 1000),
    registry=REGISTRY,
)
HTTP_SECONDS = Histogram(
    "wikipulse_http_request_duration_seconds",
    "HTTP request latency (FastAPI)",
    ["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
    registry=REGISTRY,
)

# ── gauges ──────────────────────────────────────────────────────────────────
WS_CLIENTS = Gauge(
    "wikipulse_ws_connected_clients",
    "Connected dashboard WebSocket clients",
    registry=REGISTRY,
)
CONSUMER_LAG = Gauge(
    "wikipulse_consumer_lag",
    "Approximate Kafka consumer lag (log-end - committed offset)",
    ["group", "topic", "partition"],
    registry=REGISTRY,
)


def start_metrics_server(port: int | None = None) -> None:
    """Expose ``/metrics`` on ``port`` (a separate HTTP listener per process)."""
    if not settings.metrics_enabled:
        return
    start_http_server(port or settings.metrics_port, registry=REGISTRY)


def metrics_response() -> tuple[bytes, dict[str, str]]:
    """Expose the registry through the FastAPI app (in-process ``/metrics``)."""
    return generate_latest(REGISTRY), {"Content-Type": "text/plain; version=0.0.4; charset=utf-8"}
