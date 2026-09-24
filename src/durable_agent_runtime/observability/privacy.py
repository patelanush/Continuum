"""Allowlist applied immediately before any span leaves the process."""

from collections.abc import Mapping, Sequence
from re import fullmatch
from uuid import UUID

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.trace import Link
from opentelemetry.trace.status import Status

# Identifiers are appropriate for trace search, never for Prometheus labels.
TRACE_ATTRIBUTES = frozenset(
    {
        "continuum.workflow.id",
        "continuum.workflow.type",
        "continuum.step.id",
        "continuum.step.type",
        "continuum.execution_attempt.id",
        "continuum.execution_attempt.number",
        "continuum.agent_run.id",
        "continuum.agent_turn.id",
        "continuum.model_call.id",
        "continuum.agent_tool_call.id",
        "continuum.workspace.id",
        "continuum.sandbox.id",
        "continuum.approval.id",
        "continuum.event.id",
        "continuum.event.type",
        "continuum.event.duplicate",
        "continuum.event.result",
        "continuum.executor.id",
        "continuum.attempt.status",
        "continuum.recovered",
        "continuum.lease_lost",
        "continuum.tool.name",
        "continuum.tool.semantics",
        "continuum.tool.reconciled",
        "continuum.operation.sha256",
        "continuum.patch.sha256",
        "continuum.patch.file_count",
        "continuum.command.type",
        "continuum.command.exit_code",
        "continuum.command.timed_out",
        "continuum.commit.sha",
        "continuum.error.class",
        "continuum.publish.attempt",
        "continuum.outbox.batch_size",
        "continuum.approval.decision",
        "continuum.external.replayed",
        "continuum.test.passed",
        "continuum.test.failed",
        "messaging.system",
        "messaging.destination.name",
        "messaging.kafka.message.key",
        "messaging.kafka.partition",
        "messaging.kafka.offset",
        "messaging.operation.name",
        "http.request.method",
        "http.response.status_code",
        "http.route",
        "server.address",
        "server.port",
        "network.protocol.version",
        "db.system.name",
        "db.operation.name",
        "db.namespace",
        "gen_ai.system",
        "gen_ai.provider.name",
        "gen_ai.request.model",
        "gen_ai.usage.input_tokens",
        "gen_ai.usage.output_tokens",
    }
)

_UUID_ATTRIBUTES = frozenset(
    {
        "continuum.workflow.id",
        "continuum.step.id",
        "continuum.execution_attempt.id",
        "continuum.agent_run.id",
        "continuum.agent_turn.id",
        "continuum.model_call.id",
        "continuum.agent_tool_call.id",
        "continuum.workspace.id",
        "continuum.sandbox.id",
        "continuum.approval.id",
        "continuum.event.id",
        "messaging.kafka.message.key",
    }
)
_ENUM_ATTRIBUTES: dict[str, frozenset[str]] = {
    "continuum.workflow.type": frozenset(
        {"coding-demo", "support-demo", "faultlab", "phase3-demo", "demo", "other"}
    ),
    "continuum.step.type": frozenset(
        {"noop", "slow_noop", "mock_refund", "support_agent", "coding_agent", "other"}
    ),
    "continuum.event.type": frozenset({"step.ready", "other"}),
    "continuum.event.result": frozenset(
        {"scheduled", "already_scheduled", "duplicate", "stale", "dead_letter", "other"}
    ),
    "continuum.attempt.status": frozenset(
        {"succeeded", "failed", "expired", "lost_lease", "other"}
    ),
    "continuum.tool.name": frozenset(
        {
            "read_refund_policy",
            "refund_customer",
            "list_files",
            "read_file",
            "search_files",
            "apply_patch",
            "run_tests",
            "git_status",
            "git_diff",
            "other",
        }
    ),
    "continuum.tool.semantics": frozenset(
        {
            "READ_ONLY",
            "IDEMPOTENT",
            "IDEMPOTENCY_KEY_SUPPORTED",
            "RECONCILABLE",
            "idempotent",
            "other",
        }
    ),
    "continuum.command.type": frozenset(
        {
            "prepare",
            "fingerprint",
            "list_files",
            "read_file",
            "search_files",
            "apply_patch",
            "run_tests",
            "git_status",
            "git_diff",
            "git_commit",
            "other",
        }
    ),
    "continuum.approval.decision": frozenset({"approved", "rejected", "other"}),
    "continuum.error.class": frozenset(
        {
            "timeout",
            "provider_error",
            "invalid_model_output",
            "sandbox_unavailable",
            "sandbox_timeout",
            "sandbox_error",
            "command_failed",
            "execution_failed",
            "kafka_publish",
            "external_service_error",
            "other",
        }
    ),
    "messaging.destination.name": frozenset(
        {"continuum.step.ready.v1", "continuum.dead-letter.v1", "other"}
    ),
    "messaging.operation.name": frozenset({"send", "process", "other"}),
    "http.request.method": frozenset({"GET", "POST", "PUT", "PATCH", "DELETE", "other"}),
    "gen_ai.system": frozenset({"fake", "ollama", "other"}),
    "gen_ai.provider.name": frozenset({"fake", "ollama", "other"}),
    "gen_ai.request.model": frozenset(
        {"scripted-fake-v1", "scripted-coding-v1", "qwen2.5:3b", "qwen2.5-coder:3b", "other"}
    ),
}


def safe_trace_attributes(
    attributes: Mapping[str, object] | None,
) -> dict[str, str | bool | int | float]:
    if not attributes:
        return {}
    clean: dict[str, str | bool | int | float] = {}
    for name, value in attributes.items():
        if name in TRACE_ATTRIBUTES and isinstance(value, (str, bool, int, float)):
            if isinstance(value, str):
                if len(value) > 200:
                    continue
                if name in _UUID_ATTRIBUTES:
                    try:
                        UUID(value)
                    except ValueError:
                        continue
                elif name in {"continuum.patch.sha256", "continuum.operation.sha256"}:
                    if fullmatch(r"[0-9a-f]{64}", value) is None:
                        continue
                elif name == "continuum.commit.sha":
                    if fullmatch(r"[0-9a-f]{40}", value) is None:
                        continue
                elif name in _ENUM_ATTRIBUTES and value not in _ENUM_ATTRIBUTES[name]:
                    value = "other"
            clean[name] = value
    return clean


class RedactingSpanExporter(SpanExporter):
    """Strip unapproved attributes, events and status text before OTLP encoding."""

    def __init__(self, delegate: SpanExporter) -> None:
        self.delegate = delegate

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        clean = [
            ReadableSpan(
                name=span.name[:100],
                context=span.context,
                parent=span.parent,
                resource=span.resource,
                attributes=safe_trace_attributes(span.attributes),
                events=(),
                links=[
                    Link(link.context, safe_trace_attributes(link.attributes))
                    for link in span.links
                ],
                kind=span.kind,
                status=Status(span.status.status_code),
                start_time=span.start_time,
                end_time=span.end_time,
                instrumentation_scope=span.instrumentation_scope,
            )
            for span in spans
        ]
        return self.delegate.export(clean)

    def shutdown(self, timeout_millis: int = 30000) -> None:
        self.delegate.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self.delegate.force_flush(timeout_millis)
