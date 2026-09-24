"""Bounded operational metrics; IDs and user content are forbidden labels."""

import logging
from functools import lru_cache
from typing import Literal

from opentelemetry import metrics

logger = logging.getLogger(__name__)

COUNTER_LABELS: dict[str, frozenset[str]] = {
    "continuum_workflows_started": frozenset({"workflow_type"}),
    "continuum_workflows_completed": frozenset({"status", "workflow_type"}),
    "continuum_steps_completed": frozenset({"status", "step_type"}),
    "continuum_execution_attempts": frozenset({"status", "step_type"}),
    "continuum_recoveries": frozenset({"reason"}),
    "continuum_lease_expirations": frozenset(),
    "continuum_kafka_events_published": frozenset({"event_type"}),
    "continuum_kafka_events_consumed": frozenset({"event_type", "result"}),
    "continuum_kafka_duplicates": frozenset(),
    "continuum_dlq_messages": frozenset({"reason"}),
    "continuum_model_calls": frozenset({"provider", "model", "status"}),
    "continuum_tool_calls": frozenset({"tool_name", "status"}),
    "continuum_sandbox_commands": frozenset({"command_type", "status"}),
    "continuum_approvals": frozenset({"action_type", "decision"}),
    "continuum_agent_runs": frozenset({"status", "agent_type"}),
    "continuum_model_tokens": frozenset({"provider", "model", "direction"}),
    "continuum_heartbeats": frozenset({"result"}),
    "continuum_workspace_reconciliations": frozenset({"result"}),
    "continuum_git_commits": frozenset({"status"}),
}

HISTOGRAM_LABELS: dict[str, frozenset[str]] = {
    "continuum_workflow_duration_seconds": frozenset({"workflow_type", "status"}),
    "continuum_execution_attempt_duration_seconds": frozenset({"step_type", "status"}),
    "continuum_recovery_duration_seconds": frozenset(),
    "continuum_kafka_publish_delay_seconds": frozenset({"event_type"}),
    "continuum_execution_claim_latency_seconds": frozenset({"step_type"}),
    "continuum_model_call_duration_seconds": frozenset({"provider", "model", "status"}),
    "continuum_tool_call_duration_seconds": frozenset({"tool_name", "status"}),
    "continuum_sandbox_startup_duration_seconds": frozenset(),
    "continuum_sandbox_command_duration_seconds": frozenset({"command_type", "status"}),
    "continuum_test_duration_seconds": frozenset({"status"}),
}

GAUGES = frozenset(
    {
        "continuum_active_workflows",
        "continuum_pending_execution_attempts",
        "continuum_running_execution_attempts",
        "continuum_unpublished_outbox_events",
        "continuum_active_leases",
        "continuum_pending_approvals",
    }
)

_BOUNDED: dict[str, frozenset[str]] = {
    "status": frozenset({"succeeded", "failed", "expired", "cancelled", "timeout", "other"}),
    "result": frozenset(
        {
            "scheduled",
            "already_scheduled",
            "duplicate",
            "stale",
            "dead_letter",
            "ok",
            "error",
            "succeeded",
            "failed",
            "lost_lease",
            "other",
        }
    ),
    "decision": frozenset({"approved", "rejected", "cancelled", "other"}),
    "direction": frozenset({"input", "output"}),
    "workflow_type": frozenset({"coding-demo", "support-demo", "faultlab", "other"}),
    "step_type": frozenset({"noop", "slow_noop", "mock_refund", "support_agent", "coding_agent"}),
    "event_type": frozenset({"step.ready", "other"}),
    "provider": frozenset({"fake", "ollama", "other"}),
    "model": frozenset({"scripted-fake-v1", "scripted-coding-v1", "qwen2.5:3b", "other"}),
    "tool_name": frozenset(
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
    "command_type": frozenset(
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
    "reason": frozenset({"lease_expired", "invalid_event", "unknown", "other"}),
    "action_type": frozenset({"COMMIT_PATCH", "other"}),
    "agent_type": frozenset({"support_agent", "coding_agent", "other"}),
}


def validate_metric_labels(name: str, labels: dict[str, str]) -> dict[str, str]:
    allowed = COUNTER_LABELS.get(name, HISTOGRAM_LABELS.get(name))
    if allowed is None:
        if name in GAUGES and not labels:
            return {}
        raise ValueError(f"Unknown metric: {name}")
    if set(labels) != allowed:
        raise ValueError(f"Metric {name} requires exactly {sorted(allowed)}")
    return {
        key: value if key not in _BOUNDED or value in _BOUNDED[key] else "other"
        for key, value in labels.items()
    }


@lru_cache
def _instrument(name: str, kind: Literal["counter", "histogram", "gauge"]) -> object:
    meter = metrics.get_meter("durable_agent_runtime.observability")
    if kind == "counter":
        return meter.create_counter(name)
    if kind == "histogram":
        return meter.create_histogram(name)
    return meter.create_gauge(name)


def count(name: str, amount: int = 1, **labels: str) -> None:
    from durable_agent_runtime.observability.runtime import enabled

    if not enabled():
        return
    try:
        attributes = validate_metric_labels(name, labels)
        _instrument(name, "counter").add(amount, attributes)  # type: ignore[attr-defined]
    except Exception:
        logger.exception("telemetry metric recording failed")


def duration(name: str, seconds: float, **labels: str) -> None:
    from durable_agent_runtime.observability.runtime import enabled

    if not enabled():
        return
    try:
        attributes = validate_metric_labels(name, labels)
        _instrument(name, "histogram").record(max(0.0, seconds), attributes)  # type: ignore[attr-defined]
    except Exception:
        logger.exception("telemetry metric recording failed")


def gauge(name: str, value: int) -> None:
    from durable_agent_runtime.observability.runtime import enabled

    if not enabled():
        return
    try:
        validate_metric_labels(name, {})
        _instrument(name, "gauge").set(value)  # type: ignore[attr-defined]
    except Exception:
        logger.exception("telemetry metric recording failed")
