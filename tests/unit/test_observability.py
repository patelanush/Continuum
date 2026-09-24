"""Privacy, propagation, label bounds and fail-open local behavior."""

import os

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace.status import Status, StatusCode

from durable_agent_runtime.observability.context import (
    current_traceparent,
    extract_traceparent,
    kafka_headers,
    traceparent_from_headers,
    valid_traceparent,
)
from durable_agent_runtime.observability.metrics import (
    COUNTER_LABELS,
    HISTOGRAM_LABELS,
    count,
    validate_metric_labels,
)
from durable_agent_runtime.observability.operations import error_class
from durable_agent_runtime.observability.privacy import RedactingSpanExporter


def test_w3c_context_survives_outbox_and_kafka_boundary() -> None:
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer(__name__)
    with tracer.start_as_current_span("api.workflow.start") as api:
        persisted = current_traceparent()
        assert persisted is not None
    headers = kafka_headers(persisted)
    assert headers == [("traceparent", persisted.encode())]
    recovered = traceparent_from_headers(headers)
    with tracer.start_as_current_span(
        "kafka.consume", context=extract_traceparent(recovered)
    ) as child:
        assert child.get_span_context().trace_id == api.get_span_context().trace_id
    spans = exporter.get_finished_spans()
    assert spans[-1].parent is not None
    assert spans[-1].parent.span_id == api.get_span_context().span_id


@pytest.mark.parametrize(
    "invalid",
    [
        None,
        "",
        "garbage",
        "00-" + "0" * 32 + "-" + "1" * 16 + "-01",
        "00-" + "1" * 32 + "-" + "0" * 16 + "-01",
    ],
)
def test_malformed_trace_context_is_ignored(invalid: str | None) -> None:
    assert not valid_traceparent(invalid)
    assert kafka_headers(invalid) == []
    assert not trace.get_current_span(extract_traceparent(invalid)).get_span_context().is_valid
    if invalid is not None:
        assert traceparent_from_headers([("traceparent", invalid.encode())]) is None
    assert traceparent_from_headers([("traceparent", b"\xff")]) is None


def test_exporter_removes_sentinels_before_delegate() -> None:
    delegate = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(RedactingSpanExporter(delegate)))
    tracer = provider.get_tracer(__name__)
    with tracer.start_as_current_span("model.call") as active:
        active.set_attribute("continuum.workflow.id", "cafe7a0b-4c94-49e9-97b9-44eb59c54a7c")
        active.set_attribute("gen_ai.request.model", "scripted-fake-v1")
        active.set_attribute("continuum.workflow.type", "SUPER_SECRET_PROMPT_VALUE")
        active.set_attribute("gen_ai.provider.name", "FAKE_API_KEY_123")
        active.set_attribute("prompt", "SUPER_SECRET_PROMPT_VALUE")
        active.set_attribute("source", "PRIVATE_SOURCE_CONTENT")
        active.set_attribute("api_key", "FAKE_API_KEY_123")
        active.add_event("SUPER_SECRET_PROMPT_VALUE", {"output": "PRIVATE_SOURCE_CONTENT"})
        active.set_status(Status(StatusCode.ERROR, "FAKE_API_KEY_123"))
    exported = delegate.get_finished_spans()[0]
    assert exported.attributes == {
        "continuum.workflow.id": "cafe7a0b-4c94-49e9-97b9-44eb59c54a7c",
        "gen_ai.request.model": "scripted-fake-v1",
        "continuum.workflow.type": "other",
        "gen_ai.provider.name": "other",
    }
    assert exported.events == ()
    assert exported.status.description is None
    assert all(
        marker not in str(exported.to_json())
        for marker in ("SUPER_SECRET_PROMPT_VALUE", "PRIVATE_SOURCE_CONTENT", "FAKE_API_KEY_123")
    )


def test_metric_labels_reject_identifiers_and_bound_dynamic_values() -> None:
    forbidden = {
        "workflow_id",
        "step_id",
        "attempt_id",
        "agent_run_id",
        "tool_call_id",
        "trace_id",
        "customer_id",
        "workspace_id",
    }
    assert all(
        not (labels & forbidden)
        for labels in (*COUNTER_LABELS.values(), *HISTOGRAM_LABELS.values())
    )
    with pytest.raises(ValueError):
        validate_metric_labels("continuum_workflows_started", {"workflow_id": "abc"})
    assert validate_metric_labels(
        "continuum_workflows_started", {"workflow_type": "user-secret"}
    ) == {"workflow_type": "other"}


def test_disabled_metric_recording_is_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_ENABLED", "false")
    count("continuum_workflows_started", workflow_type="coding-demo")
    assert os.environ["OTEL_ENABLED"] == "false"


def test_bounded_error_classification() -> None:
    assert error_class(TimeoutError("secret")) == "timeout"
    assert error_class(ValueError("secret")) == "invalid_model_output"
    assert error_class(RuntimeError("secret")) == "other"
