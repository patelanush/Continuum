import pytest

from durable_agent_runtime.domain.enums import StepStatus, WorkflowStatus
from durable_agent_runtime.domain.errors import InvalidStateTransition
from durable_agent_runtime.domain.state_machine import (
    STEP_TRANSITIONS,
    WORKFLOW_TRANSITIONS,
    validate_step_transition,
    validate_workflow_transition,
)


@pytest.mark.parametrize(
    ("current", "target"),
    [(current, target) for current, targets in WORKFLOW_TRANSITIONS.items() for target in targets],
)
def test_all_allowed_workflow_transitions(current: WorkflowStatus, target: WorkflowStatus) -> None:
    validate_workflow_transition(current, target)


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (current, target)
        for current in WorkflowStatus
        for target in WorkflowStatus
        if target not in WORKFLOW_TRANSITIONS[current]
    ],
)
def test_all_disallowed_workflow_transitions_raise(
    current: WorkflowStatus, target: WorkflowStatus
) -> None:
    with pytest.raises(InvalidStateTransition):
        validate_workflow_transition(current, target)


@pytest.mark.parametrize(
    ("current", "target"),
    [(current, target) for current, targets in STEP_TRANSITIONS.items() for target in targets],
)
def test_all_allowed_step_transitions(current: StepStatus, target: StepStatus) -> None:
    validate_step_transition(current, target)


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (current, target)
        for current in StepStatus
        for target in StepStatus
        if target not in STEP_TRANSITIONS[current]
    ],
)
def test_all_disallowed_step_transitions_raise(current: StepStatus, target: StepStatus) -> None:
    with pytest.raises(InvalidStateTransition):
        validate_step_transition(current, target)
