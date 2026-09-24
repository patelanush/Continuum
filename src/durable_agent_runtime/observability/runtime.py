"""Bounded asynchronous OTLP export and safe span helpers."""

import logging
import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Span, SpanKind
from opentelemetry.trace.status import Status, StatusCode

from durable_agent_runtime.core.config import Settings
from durable_agent_runtime.observability.privacy import RedactingSpanExporter, safe_trace_attributes

logger = logging.getLogger(__name__)
_configured = False


def enabled() -> bool:
    return os.getenv("OTEL_ENABLED", "false").lower() in {"true", "1", "yes"}


def configure(service_name: str, settings: Settings) -> None:
    global _configured
    if _configured or not settings.otel_enabled:
        return
    resource = Resource(
        attributes={
            "service.name": service_name,
            "service.version": "0.1.0",
            "deployment.environment": settings.app_env,
        }
    )
    endpoint = settings.otel_exporter_otlp_endpoint
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(
        BatchSpanProcessor(
            RedactingSpanExporter(OTLPSpanExporter(endpoint=endpoint, insecure=True, timeout=3)),
            max_queue_size=2048,
            max_export_batch_size=256,
            schedule_delay_millis=1000,
            export_timeout_millis=3000,
        )
    )
    trace.set_tracer_provider(provider)
    if settings.otel_sqlalchemy_instrumentation and service_name != "continuum-mock-payments":
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

        from durable_agent_runtime.db.session import engine

        SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine, tracer_provider=provider)
    seconds = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600]
    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=endpoint, insecure=True, timeout=3),
        export_interval_millis=5000,
        export_timeout_millis=3000,
    )
    from opentelemetry import metrics

    metrics.set_meter_provider(
        MeterProvider(
            resource=resource,
            metric_readers=[reader],
            views=[
                View(
                    instrument_name="continuum_*_duration_seconds",
                    aggregation=ExplicitBucketHistogramAggregation(seconds),
                ),
                View(
                    instrument_name="continuum_*_latency_seconds",
                    aggregation=ExplicitBucketHistogramAggregation(seconds),
                ),
                View(
                    instrument_name="continuum_kafka_publish_delay_seconds",
                    aggregation=ExplicitBucketHistogramAggregation(seconds),
                ),
            ],
        )
    )
    _configured = True
    logger.info("telemetry configured service=%s", service_name)


@contextmanager
def span(
    name: str,
    attributes: Mapping[str, Any] | None = None,
    *,
    kind: SpanKind = SpanKind.INTERNAL,
    context: Any = None,
    links: Any = None,
    start_time: int | None = None,
) -> Iterator[Span]:
    tracer = trace.get_tracer("durable_agent_runtime")
    with tracer.start_as_current_span(
        name,
        context=context,
        kind=kind,
        links=links,
        start_time=start_time,
        attributes=safe_trace_attributes(attributes),
        record_exception=False,
        set_status_on_exception=False,
    ) as active:
        yield active


def error(active: Span, category: str) -> None:
    active.set_attribute("continuum.error.class", category[:100])
    active.set_status(Status(StatusCode.ERROR))
