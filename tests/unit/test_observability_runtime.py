"""Focused tests for bounded metrics, sampling, and safe model/tool observations."""

import asyncio
import importlib
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from uuid import uuid4

import pytest
from opentelemetry import metrics as otel_metrics
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider

from durable_agent_runtime.core.config import Settings
from durable_agent_runtime.core.logging import TraceCorrelationFilter, configure_logging
from durable_agent_runtime.observability import gauges, metrics, operations, runtime
from durable_agent_runtime.observability.privacy import safe_trace_attributes


class RecordingInstrument:
    def __init__(self) -> None:
        self.calls: list[tuple[str, float, dict[str, str] | None]] = []

    def add(self, value: int, labels: dict[str, str]) -> None:
        self.calls.append(("add", value, labels))

    def record(self, value: float, labels: dict[str, str]) -> None:
        self.calls.append(("record", value, labels))

    def set(self, value: int) -> None:
        self.calls.append(("set", value, None))


def test_metrics_record_bounded_values_and_ignore_telemetry_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_ENABLED", "true")
    instrument = RecordingInstrument()
    monkeypatch.setattr(metrics, "_instrument", lambda *_args: instrument)
    metrics.count("continuum_workflows_started", workflow_type="private-customer-name")
    metrics.duration("continuum_test_duration_seconds", -3, status="succeeded")
    metrics.gauge("continuum_active_workflows", 2)
    assert instrument.calls == [
        ("add", 1, {"workflow_type": "other"}),
        ("record", 0.0, {"status": "succeeded"}),
        ("set", 2, None),
    ]
    # A call-site label mistake is a telemetry error and cannot fail a workflow.
    metrics.count("continuum_workflows_started", workflow_id="private")
    metrics.duration("continuum_test_duration_seconds", 1, workflow_id="private")
    metrics.gauge("unknown_gauge", 1)


def test_metric_factory_creates_each_instrument_once(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[tuple[str, str]] = []

    class Meter:
        def create_counter(self, name: str) -> object:
            created.append(("counter", name))
            return object()

        def create_histogram(self, name: str) -> object:
            created.append(("histogram", name))
            return object()

        def create_gauge(self, name: str) -> object:
            created.append(("gauge", name))
            return object()

    monkeypatch.setattr(otel_metrics, "get_meter", lambda _name: Meter())
    metrics._instrument.cache_clear()
    try:
        metrics._instrument("continuum_factory_counter", "counter")
        metrics._instrument("continuum_factory_histogram", "histogram")
        metrics._instrument("continuum_factory_gauge", "gauge")
        metrics._instrument("continuum_factory_counter", "counter")
        assert created == [
            ("counter", "continuum_factory_counter"),
            ("histogram", "continuum_factory_histogram"),
            ("gauge", "continuum_factory_gauge"),
        ]
    finally:
        metrics._instrument.cache_clear()


def test_safe_attributes_validate_content_even_for_allowed_keys() -> None:
    result = safe_trace_attributes(
        {
            "continuum.workflow.id": "SUPER_SECRET_PROMPT_VALUE",
            "continuum.workflow.type": "PRIVATE_SOURCE_CONTENT",
            "gen_ai.request.model": "FAKE_API_KEY_123",
            "continuum.patch.sha256": "not-a-hash",
            "continuum.commit.sha": "not-a-sha",
            "continuum.executor.id": "x" * 201,
            "prompt": "SUPER_SECRET_PROMPT_VALUE",
            "continuum.event.duplicate": True,
        }
    )
    assert result == {
        "continuum.workflow.type": "other",
        "gen_ai.request.model": "other",
        "continuum.event.duplicate": True,
    }


def test_span_helper_and_error_category() -> None:
    with runtime.span("execution.run", {"prompt": "secret"}) as active:
        runtime.error(active, "timeout")
        assert not active.get_span_context().is_valid
    assert not runtime.enabled()
    assert Settings().otel_enabled is False


def test_structured_log_filter_preserves_trace_identity() -> None:
    record = logging.makeLogRecord({"msg": "durable workflow_id=abc"})
    correlation = TraceCorrelationFilter()
    assert correlation.filter(record)
    assert record.__dict__["trace_id"] == "-"
    tracer = TracerProvider().get_tracer(__name__)
    with tracer.start_as_current_span("workflow.create") as active:
        assert correlation.filter(record)
        assert record.__dict__["trace_id"] == f"{active.get_span_context().trace_id:032x}"
        assert record.__dict__["span_id"] == f"{active.get_span_context().span_id:016x}"
        assert record.msg == "durable workflow_id=abc"
    root = logging.getLogger()
    old_handlers, old_level = root.handlers[:], root.level
    try:
        configure_logging("WARNING")
        assert root.level == logging.WARNING
        assert any(
            isinstance(item, TraceCorrelationFilter)
            for handler in root.handlers
            for item in handler.filters
        )
    finally:
        root.handlers = old_handlers
        root.setLevel(old_level)


def test_configure_uses_bounded_async_export_and_service_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: dict[str, object] = {}

    class Provider:
        def __init__(self, *, resource: object) -> None:
            recorded["resource"] = resource

        def add_span_processor(self, processor: object) -> None:
            recorded["span_processor"] = processor

    def batch(_exporter: object, **options: object) -> object:
        recorded["batch_options"] = options
        return object()

    def meter_provider(**options: object) -> object:
        recorded["meter_options"] = options
        return object()

    monkeypatch.setattr(runtime, "_configured", False)
    monkeypatch.setattr(runtime, "TracerProvider", Provider)
    monkeypatch.setattr(runtime, "OTLPSpanExporter", lambda **_options: object())
    monkeypatch.setattr(runtime, "OTLPMetricExporter", lambda **_options: object())
    monkeypatch.setattr(runtime, "BatchSpanProcessor", batch)
    monkeypatch.setattr(
        runtime, "PeriodicExportingMetricReader", lambda *_args, **_options: object()
    )
    monkeypatch.setattr(runtime, "MeterProvider", meter_provider)
    monkeypatch.setattr(
        trace,
        "set_tracer_provider",
        lambda provider: recorded.update({"tracer_provider": provider}),
    )
    monkeypatch.setattr(
        otel_metrics,
        "set_meter_provider",
        lambda provider: recorded.update({"meter_provider": provider}),
    )
    try:
        runtime.configure("continuum-test", Settings(otel_enabled=True))
        assert recorded["batch_options"] == {
            "max_queue_size": 2048,
            "max_export_batch_size": 256,
            "schedule_delay_millis": 1000,
            "export_timeout_millis": 3000,
        }
        resource = recorded["resource"]
        assert isinstance(resource, Resource)
        assert resource.attributes["service.name"] == "continuum-test"
        assert recorded["meter_provider"] is not None
        runtime.configure("ignored-second-name", Settings(otel_enabled=True))
    finally:
        monkeypatch.setattr(runtime, "_configured", False)


@pytest.mark.asyncio
async def test_api_lifespan_starts_and_stops_low_frequency_sampling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api_module = importlib.import_module("durable_agent_runtime.api.app")
    events: list[str] = []

    async def fake_sampling(stop: asyncio.Event, _sessions: object) -> None:
        events.append("sample_started")
        await stop.wait()
        events.append("sample_stopped")

    class FakeEngine:
        async def dispose(self) -> None:
            events.append("engine_disposed")

    monkeypatch.setattr(api_module, "get_settings", lambda: Settings(otel_enabled=True))
    monkeypatch.setattr(api_module, "configure", lambda service, _settings: events.append(service))
    monkeypatch.setattr(api_module, "configure_logging", lambda _level: None)
    monkeypatch.setattr(api_module, "sampling_loop", fake_sampling)
    monkeypatch.setattr(api_module, "engine", FakeEngine())
    async with api_module.lifespan(api_module.app):
        await asyncio.sleep(0)
        assert "sample_started" in events
    assert events == [
        "continuum-api",
        "sample_started",
        "sample_stopped",
        "engine_disposed",
    ]


@pytest.mark.asyncio
async def test_mock_payments_lifespan_keeps_telemetry_outside_pool_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payments = importlib.import_module("durable_agent_runtime.mock_payments.app")
    events: list[str] = []

    class FakePool:
        async def close(self) -> None:
            events.append("pool_closed")

    async def create_pool(_dsn: str) -> FakePool:
        events.append("pool_created")
        return FakePool()

    previous_pool = getattr(payments.app.state, "pool", None)
    monkeypatch.setattr(payments, "configure", lambda service, _settings: events.append(service))
    monkeypatch.setattr(payments.asyncpg, "create_pool", create_pool)
    try:
        async with payments.lifespan(payments.app):
            assert isinstance(payments.app.state.pool, FakePool)
    finally:
        if previous_pool is None:
            delattr(payments.app.state, "pool")
        else:
            payments.app.state.pool = previous_pool
    assert events == ["continuum-mock-payments", "pool_created", "pool_closed"]


class FakeSession:
    def __init__(self) -> None:
        self.values = iter([5, 2, 1, 1, 3, 4])

    async def __aenter__(self) -> "FakeSession":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def scalar(self, _query: object) -> int:
        return next(self.values)


@pytest.mark.asyncio
async def test_gauge_sample_records_durable_depths(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: dict[str, int] = {}
    monkeypatch.setattr(gauges, "gauge", lambda name, value: recorded.update({name: value}))
    await gauges.sample(lambda: FakeSession())  # type: ignore[arg-type]
    assert recorded == {
        "continuum_active_workflows": 5,
        "continuum_pending_execution_attempts": 2,
        "continuum_running_execution_attempts": 1,
        "continuum_active_leases": 1,
        "continuum_unpublished_outbox_events": 3,
        "continuum_pending_approvals": 4,
    }


@pytest.mark.asyncio
async def test_gauge_loop_stops_after_sample(monkeypatch: pytest.MonkeyPatch) -> None:
    stop = asyncio.Event()
    sampled = 0

    async def fake_sample(_sessions: object) -> None:
        nonlocal sampled
        sampled += 1
        stop.set()

    monkeypatch.setattr(gauges, "sample", fake_sample)
    await gauges.sampling_loop(stop, object(), interval=0.01)  # type: ignore[arg-type]
    assert sampled == 1


def test_model_and_tool_metrics_reflect_success_and_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    counters: list[tuple[str, dict[str, str]]] = []
    histograms: list[tuple[str, dict[str, str]]] = []

    @contextmanager
    def fake_span(_name: str, _attributes: object) -> Iterator[trace.Span]:
        yield trace.NonRecordingSpan(trace.INVALID_SPAN_CONTEXT)

    monkeypatch.setattr(operations, "span", fake_span)
    monkeypatch.setattr(
        operations, "count", lambda name, _amount=1, **labels: counters.append((name, labels))
    )
    monkeypatch.setattr(
        operations,
        "duration",
        lambda name, _seconds, **labels: histograms.append((name, labels)),
    )
    with pytest.raises(TimeoutError):
        with operations.model_call(uuid4(), "fake", "scripted-fake-v1", 1):
            raise TimeoutError("secret")
    with operations.model_call(uuid4(), "fake", "scripted-fake-v1", 2) as active:
        operations.model_tokens(active, "fake", "scripted-fake-v1", 8, 3)
    with operations.tool_call(uuid4(), "read_refund_policy", "READ_ONLY"):
        pass
    with pytest.raises(RuntimeError):
        with operations.tool_call(uuid4(), "refund_customer", "IDEMPOTENCY_KEY_SUPPORTED"):
            raise RuntimeError("secret")
    assert counters == [
        (
            "continuum_model_calls",
            {"provider": "fake", "model": "scripted-fake-v1", "status": "failed"},
        ),
        (
            "continuum_model_tokens",
            {
                "provider": "fake",
                "model": "scripted-fake-v1",
                "direction": "input",
            },
        ),
        (
            "continuum_model_tokens",
            {
                "provider": "fake",
                "model": "scripted-fake-v1",
                "direction": "output",
            },
        ),
        (
            "continuum_model_calls",
            {"provider": "fake", "model": "scripted-fake-v1", "status": "succeeded"},
        ),
        (
            "continuum_tool_calls",
            {"tool_name": "read_refund_policy", "status": "succeeded"},
        ),
        (
            "continuum_tool_calls",
            {"tool_name": "refund_customer", "status": "failed"},
        ),
    ]
    assert [name for name, _ in histograms] == [
        "continuum_model_call_duration_seconds",
        "continuum_model_call_duration_seconds",
        "continuum_tool_call_duration_seconds",
        "continuum_tool_call_duration_seconds",
    ]
