from collections.abc import Mapping
from enum import StrEnum

from durable_agent_runtime.domain.enums import ExecutionAttemptStatus, StepStatus, WorkflowStatus
from durable_agent_runtime.domain.errors import InvalidStateTransition

WORKFLOW_TRANSITIONS: Mapping[WorkflowStatus, frozenset[WorkflowStatus]] = {
    WorkflowStatus.PENDING: frozenset({WorkflowStatus.RUNNING, WorkflowStatus.CANCELLED}),
    WorkflowStatus.RUNNING: frozenset(
        {WorkflowStatus.SUCCEEDED, WorkflowStatus.FAILED, WorkflowStatus.CANCELLED}
    ),
    WorkflowStatus.SUCCEEDED: frozenset(),
    WorkflowStatus.FAILED: frozenset(),
    WorkflowStatus.CANCELLED: frozenset(),
}

STEP_TRANSITIONS: Mapping[StepStatus, frozenset[StepStatus]] = {
    StepStatus.PENDING: frozenset({StepStatus.READY, StepStatus.CANCELLED}),
    StepStatus.READY: frozenset({StepStatus.RUNNING, StepStatus.CANCELLED}),
    StepStatus.RUNNING: frozenset({StepStatus.SUCCEEDED, StepStatus.FAILED, StepStatus.CANCELLED}),
    StepStatus.SUCCEEDED: frozenset(),
    StepStatus.FAILED: frozenset(),
    StepStatus.CANCELLED: frozenset(),
}

ATTEMPT_TRANSITIONS: Mapping[ExecutionAttemptStatus, frozenset[ExecutionAttemptStatus]] = {
    ExecutionAttemptStatus.PENDING: frozenset(
        {ExecutionAttemptStatus.RUNNING, ExecutionAttemptStatus.CANCELLED}
    ),
    ExecutionAttemptStatus.RUNNING: frozenset(
        {
            ExecutionAttemptStatus.SUCCEEDED,
            ExecutionAttemptStatus.FAILED,
            ExecutionAttemptStatus.EXPIRED,
            ExecutionAttemptStatus.CANCELLED,
        }
    ),
    ExecutionAttemptStatus.SUCCEEDED: frozenset(),
    ExecutionAttemptStatus.FAILED: frozenset(),
    ExecutionAttemptStatus.EXPIRED: frozenset(),
    ExecutionAttemptStatus.CANCELLED: frozenset(),
}


def _validate_transition[StatusT: StrEnum](
    entity: str, current: StatusT, target: StatusT, allowed: Mapping[StatusT, frozenset[StatusT]]
) -> None:
    if target not in allowed[current]:
        raise InvalidStateTransition(entity, current.value, target.value)


def validate_workflow_transition(current: WorkflowStatus, target: WorkflowStatus) -> None:
    _validate_transition("workflow", current, target, WORKFLOW_TRANSITIONS)


def validate_step_transition(current: StepStatus, target: StepStatus) -> None:
    _validate_transition("step", current, target, STEP_TRANSITIONS)


def validate_attempt_transition(
    current: ExecutionAttemptStatus, target: ExecutionAttemptStatus
) -> None:
    _validate_transition("execution attempt", current, target, ATTEMPT_TRANSITIONS)
