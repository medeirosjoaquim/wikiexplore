"""OpenTelemetry tracing setup + Kafka W3C trace-context propagation.

Traces are exported over OTLP/HTTP to Tempo, with Kafka message headers
carrying the W3C ``traceparent`` so a single trace spans the whole pipeline:

    producer (httpx SSE) -> wiki.raw -> demux -> {indexer, analytics, vandalism}

Each consumer links its per-message span to the producer span via the extracted
context, giving end-to-end visibility for performance debugging.
"""
from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterable
from typing import Any

from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from app.core.config import settings

log = logging.getLogger("wikipulse.tracing")

_tracer = trace.get_tracer("wikipulse")
_setup_done = False


def setup_tracing(service_name: str | None = None, instance: str | None = None) -> trace.Tracer:
    """Configure the global TracerProvider once. Idempotent and failure-tolerant.

    If tracing is disabled (or the exporter is unreachable at startup) we still
    return a tracer — spans simply go nowhere, so the app never breaks because
    of observability.
    """
    global _tracer, _setup_done
    if _setup_done or not settings.otel_enabled:
        return _tracer

    resource = Resource.create(
        {
            "service.name": service_name or settings.otel_service_name,
            "service.instance": instance or settings.otel_service_instance,
            "service.namespace": "wikipulse",
            "deployment.environment": settings.otel_resource_environment,
        }
    )
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_traces_endpoint)))
    trace.set_tracer_provider(provider)
    _tracer = trace.get_tracer("wikipulse")
    _setup_done = True
    log.info(
        "tracing enabled: service=%s instance=%s -> %s",
        service_name or settings.otel_service_name,
        instance or settings.otel_service_instance,
        settings.otel_exporter_otlp_traces_endpoint,
    )
    return _tracer


def get_tracer() -> trace.Tracer:
    return _tracer


@contextlib.contextmanager
def span(name: str, *, context=None, **attributes):
    """Convenience context manager that records a traced span with attributes.

    Pass ``context=`` to continue a remote trace (e.g. extracted from Kafka
    message headers) instead of the currently active one.
    """
    with _tracer.start_as_current_span(name, context=context) as s:
        for key, value in attributes.items():
            with contextlib.suppress(Exception):
                s.set_attribute(key, value)
        yield s


# ── Kafka W3C context propagation ───────────────────────────────────────────
# confluent-kafka message headers are list[(str, bytes)]; OTel propagators work
# on a text carrier, so we bridge str<->bytes here.


class _DictSetter:
    def set(self, carrier: dict, key: str, value: str) -> None:
        carrier[key] = value


class _DictGetter:
    def get(self, carrier: dict, key: str) -> list[str] | None:
        if carrier is None:
            return None
        val = carrier.get(key)
        return [val] if val is not None else None

    def keys(self, carrier: dict) -> Iterable[str]:
        return list(carrier.keys()) if carrier else []


def inject_context(carrier: dict | None = None) -> dict:
    """Inject the active span context into ``carrier`` (and return it).

    Returns a ``dict[str, str]`` ready to be encoded as Kafka message headers.
    """
    carrier = {} if carrier is None else carrier
    propagate.inject(carrier, setter=_DictSetter())  # type: ignore[arg-type]
    return carrier

def extract_context(headers) -> Any:
    """Extract a context from Kafka message headers (``list[(str, bytes)]``)."""
    if not headers:
        return None
    carrier = {
        k: (v.decode("utf-8") if isinstance(v, bytes | bytearray) else v)
        for k, v in headers
    }
    return propagate.extract(carrier, getter=_DictGetter())  # type: ignore[arg-type]


def headers_with_context(extra: dict | None = None) -> list[tuple[str, bytes]]:
    """Build confluent-kafka headers carrying trace context (+ any extras)."""
    out = {k: v for k, v in inject_context().items()}
    if extra:
        out.update({k: str(v) for k, v in extra.items()})
    return [(k, v.encode("utf-8")) for k, v in out.items()]
