"""Validated W3C trace context for durable outbox and Kafka propagation."""

import re
from collections.abc import Sequence

from opentelemetry import context, trace
from opentelemetry.context import Context
from opentelemetry.trace import Link
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

_TRACEPARENT = re.compile(r"^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$")
_PROPAGATOR = TraceContextTextMapPropagator()


def valid_traceparent(value: str | None) -> bool:
    if value is None:
        return False
    match = _TRACEPARENT.fullmatch(value)
    return bool(match and int(match[1], 16) and int(match[2], 16))


def current_traceparent() -> str | None:
    carrier: dict[str, str] = {}
    _PROPAGATOR.inject(carrier)
    value = carrier.get("traceparent")
    return value if valid_traceparent(value) else None


def extract_traceparent(value: str | None) -> Context:
    if not valid_traceparent(value):
        return context.Context()
    return _PROPAGATOR.extract({"traceparent": value})


def link_from_traceparent(value: str | None) -> list[Link]:
    span_context = trace.get_current_span(extract_traceparent(value)).get_span_context()
    return [Link(span_context)] if span_context.is_valid else []


def kafka_headers(traceparent: str | None) -> list[tuple[str, bytes]]:
    # No arbitrary baggage or user-supplied tracestate is persisted.
    if traceparent is None or not valid_traceparent(traceparent):
        return []
    return [("traceparent", traceparent.encode("ascii"))]


def traceparent_from_headers(headers: Sequence[tuple[str, bytes]] | None) -> str | None:
    if not headers:
        return None
    for name, raw in headers:
        if name.lower() == "traceparent":
            try:
                value = raw.decode("ascii")
            except (UnicodeError, AttributeError):
                return None
            return value if valid_traceparent(value) else None
    return None
