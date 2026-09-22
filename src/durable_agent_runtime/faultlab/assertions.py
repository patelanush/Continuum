"""Classify trials from PostgreSQL and independent payments state, not logs."""

from collections import Counter
from datetime import datetime
from typing import cast
from uuid import UUID

from durable_agent_runtime.db.models import (
    ExecutionAttempt,
    OutboxEvent,
    StateTransition,
    Workflow,
    WorkflowStep,
)
from durable_agent_runtime.faultlab.models import TrialResult
from durable_agent_runtime.faultlab.runtime import FaultLabRuntime


def elapsed_ms(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return round((end - start).total_seconds() * 1000, 3)


async def classify_workflow(
    runtime: FaultLabRuntime,
    trial: TrialResult,
    *,
    expected_attempts: int,
    expected_refunds: int = 0,
    customer_id: str | None = None,
    expected_outbox: int | None = None,
    expected_attempt_statuses: list[str] | None = None,
) -> None:
    if trial.workflow_id is None:
        raise AssertionError("Trial has no workflow ID")
    state = await runtime.snapshot(trial.workflow_id)
    workflow = cast(Workflow, state["workflow"])
    steps = cast(list[WorkflowStep], state["steps"])
    attempts = cast(list[ExecutionAttempt], state["attempts"])
    outbox = cast(list[OutboxEvent], state["outbox"])
    transitions = cast(list[StateTransition], state["transitions"])
    trial.actual_terminal_status = workflow.status.value
    trial.notes["step_count"] = len(steps)
    trial.notes["steps_succeeded"] = sum(step.status.value == "SUCCEEDED" for step in steps)
    trial.attempt_ids = [attempt.id for attempt in attempts]
    trial.executor_ids = sorted(
        {attempt.executor_id for attempt in attempts if attempt.executor_id is not None}
    )
    trial.attempt_count = len(attempts)
    trial.expected_side_effect_count = expected_refunds
    trial.lease_expiration_count = sum(attempt.status.value == "EXPIRED" for attempt in attempts)
    trial.workflow_elapsed_ms = elapsed_ms(workflow.started_at, workflow.completed_at)
    if outbox and attempts:
        first_event = min(outbox, key=lambda item: item.created_at)
        first_attempt = min(attempts, key=lambda item: item.created_at)
        trial.event_to_attempt_ms = elapsed_ms(first_event.created_at, first_attempt.created_at)
        trial.attempt_pending_to_claim_ms = elapsed_ms(
            first_attempt.created_at, first_attempt.started_at
        )
        trial.outbox_publish_delay_ms = elapsed_ms(first_event.created_at, first_event.published_at)
    transitions_by_key = Counter(
        (
            transition.entity_type.value,
            transition.entity_id,
            transition.from_status,
            transition.to_status,
        )
        for transition in transitions
    )
    trial.duplicate_transition_count = sum(
        count - 1 for count in transitions_by_key.values() if count > 1
    )
    outbox_by_step = Counter((event.step_id, event.event_type) for event in outbox)
    trial.outbox_duplicate_count = sum(count - 1 for count in outbox_by_step.values() if count > 1)
    if customer_id is not None:
        count = await runtime.refund_count(customer_id)
        trial.duplicate_side_effect_count = max(0, count - expected_refunds)
        trial.lost_side_effect_count = max(0, expected_refunds - count)
        if expected_refunds == 1 and trial.step_id is not None:
            key = f"continuum:{trial.step_id}"
            refund = await runtime.refund(key)
            if refund is not None:
                trial.external_side_effect_id = UUID(refund["refund_id"])
                if refund["idempotency_key"] != key or refund["customer_id"] != customer_id:
                    trial.failure_reason = "External refund identity did not match logical step"
    failures: list[str] = []
    if workflow.status.value != trial.expected_terminal_status:
        failures.append(
            f"workflow={workflow.status.value}, expected={trial.expected_terminal_status}"
        )
    if trial.expected_terminal_status == "SUCCEEDED" and any(
        step.status.value != "SUCCEEDED" for step in steps
    ):
        failures.append("non-succeeded step in succeeded workflow")
    if trial.attempt_count != expected_attempts:
        failures.append(f"attempts={trial.attempt_count}, expected={expected_attempts}")
    if expected_attempt_statuses is not None:
        actual_statuses = [attempt.status.value for attempt in attempts]
        if actual_statuses != expected_attempt_statuses:
            failures.append(
                f"attempt statuses={actual_statuses}, expected={expected_attempt_statuses}"
            )
    if any(attempt.status.value in {"PENDING", "RUNNING"} for attempt in attempts):
        failures.append("terminal workflow has active attempt")
    if workflow.status.value == "FAILED" and not any(
        step.status.value == "FAILED" for step in steps
    ):
        failures.append("failed workflow has no failed step")
    if trial.duplicate_transition_count:
        failures.append(f"duplicate transitions={trial.duplicate_transition_count}")
    if workflow.status.value == "SUCCEEDED":
        expected_transitions = 4 * len(steps) + 3
        if len(transitions) != expected_transitions:
            failures.append(f"transitions={len(transitions)}, expected={expected_transitions}")
    if trial.outbox_duplicate_count:
        failures.append(f"duplicate logical outbox events={trial.outbox_duplicate_count}")
    if any(event.published_at is None for event in outbox):
        failures.append("terminal workflow has unpublished outbox work")
    if [step.position for step in steps] != list(range(len(steps))):
        failures.append("invalid sequential step positions")
    if expected_outbox is not None and len(outbox) != expected_outbox:
        failures.append(f"outbox={len(outbox)}, expected={expected_outbox}")
    if trial.duplicate_side_effect_count or trial.lost_side_effect_count:
        failures.append(
            f"external effects duplicate={trial.duplicate_side_effect_count} "
            f"lost={trial.lost_side_effect_count}"
        )
    if trial.failure_reason:
        failures.append(trial.failure_reason)
    if trial.fault_injected_at is not None:
        expired = [attempt for attempt in attempts if attempt.status.value == "EXPIRED"]
        if expired:
            trial.fault_detection_latency_ms = elapsed_ms(
                trial.fault_injected_at, expired[0].completed_at
            )
            replacements = [
                attempt
                for attempt in attempts
                if attempt.step_id == expired[0].step_id
                and attempt.attempt_number > expired[0].attempt_number
            ]
            if replacements:
                trial.recovery_time_ms = elapsed_ms(
                    trial.fault_injected_at, replacements[0].started_at
                )
    trial.correct = not failures
    trial.recovered = (
        trial.recovery_required and trial.correct and workflow.status.value == "SUCCEEDED"
    )
    trial.failure_reason = "; ".join(failures) if failures else None
