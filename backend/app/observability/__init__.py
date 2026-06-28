"""Observability: Prometheus metrics + OpenTelemetry tracing.

Public surface used across the app:
- ``setup_tracing`` / ``get_tracer`` / ``span`` — tracing
- ``inject_context`` / ``extract_context`` / ``headers_with_context`` — Kafka propagation
- ``start_metrics_server`` / ``metrics_response`` + the metric objects
"""
from __future__ import annotations

from app.observability import metrics, tracing
from app.observability.metrics import (
    ANALYTICS_APPLIED,
    BULK_INDEX_BATCH,
    CONSUMER_LAG,
    DEMUX_FORWARDED,
    HTTP_SECONDS,
    INDEXER_EVENTS,
    KAFKA_POLL_BATCH,
    PRODUCER_EVENTS,
    SCHEDULER_RUNS,
    STAGE_SECONDS,
    UPSERT_BATCH,
    VANDALISM_EVENTS,
    WS_CLIENTS,
    metrics_response,
    start_metrics_server,
)
from app.observability.tracing import (
    extract_context,
    get_tracer,
    headers_with_context,
    inject_context,
    setup_tracing,
    span,
)

__all__ = [
    "ANALYTICS_APPLIED",
    "BULK_INDEX_BATCH",
    "CONSUMER_LAG",
    "DEMUX_FORWARDED",
    "HTTP_SECONDS",
    "INDEXER_EVENTS",
    "KAFKA_POLL_BATCH",
    "PRODUCER_EVENTS",
    "SCHEDULER_RUNS",
    "STAGE_SECONDS",
    "UPSERT_BATCH",
    "VANDALISM_EVENTS",
    "WS_CLIENTS",
    "extract_context",
    "get_tracer",
    "headers_with_context",
    "inject_context",
    "metrics",
    "metrics_response",
    "setup_tracing",
    "span",
    "start_metrics_server",
    "tracing",
]
