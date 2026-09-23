from collections.abc import Mapping
from enum import StrEnum

from durable_agent_runtime.domain.enums import (
    AgentRunStatus,
    AgentToolCallStatus,
    AgentTurnStatus,
    ApprovalStatus,
    CommandStatus,
    ExecutionAttemptStatus,
    ModelCallStatus,
    SandboxStatus,
    StepStatus,
    WorkflowStatus,
    WorkspaceStatus,
)
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


AGENT_RUN_TRANSITIONS: Mapping[AgentRunStatus, frozenset[AgentRunStatus]] = {
    AgentRunStatus.PENDING: frozenset({AgentRunStatus.RUNNING, AgentRunStatus.CANCELLED}),
    AgentRunStatus.RUNNING: frozenset(
        {AgentRunStatus.SUCCEEDED, AgentRunStatus.FAILED, AgentRunStatus.CANCELLED}
    ),
    AgentRunStatus.SUCCEEDED: frozenset(),
    AgentRunStatus.FAILED: frozenset(),
    AgentRunStatus.CANCELLED: frozenset(),
}
AGENT_TURN_TRANSITIONS: Mapping[AgentTurnStatus, frozenset[AgentTurnStatus]] = {
    AgentTurnStatus.PENDING_MODEL: frozenset(
        {AgentTurnStatus.TOOL_PENDING, AgentTurnStatus.COMPLETED, AgentTurnStatus.FAILED}
    ),
    AgentTurnStatus.TOOL_PENDING: frozenset({AgentTurnStatus.COMPLETED, AgentTurnStatus.FAILED}),
    AgentTurnStatus.COMPLETED: frozenset(),
    AgentTurnStatus.FAILED: frozenset(),
}
MODEL_CALL_TRANSITIONS: Mapping[ModelCallStatus, frozenset[ModelCallStatus]] = {
    ModelCallStatus.RUNNING: frozenset({ModelCallStatus.SUCCEEDED, ModelCallStatus.FAILED}),
    ModelCallStatus.SUCCEEDED: frozenset(),
    ModelCallStatus.FAILED: frozenset(),
}
AGENT_TOOL_CALL_TRANSITIONS: Mapping[AgentToolCallStatus, frozenset[AgentToolCallStatus]] = {
    AgentToolCallStatus.PENDING: frozenset(
        {AgentToolCallStatus.SUCCEEDED, AgentToolCallStatus.FAILED}
    ),
    AgentToolCallStatus.SUCCEEDED: frozenset(),
    AgentToolCallStatus.FAILED: frozenset(),
}


def validate_agent_run_transition(current: AgentRunStatus, target: AgentRunStatus) -> None:
    _validate_transition("agent run", current, target, AGENT_RUN_TRANSITIONS)


def validate_agent_turn_transition(current: AgentTurnStatus, target: AgentTurnStatus) -> None:
    _validate_transition("agent turn", current, target, AGENT_TURN_TRANSITIONS)


def validate_model_call_transition(current: ModelCallStatus, target: ModelCallStatus) -> None:
    _validate_transition("model call", current, target, MODEL_CALL_TRANSITIONS)


def validate_agent_tool_call_transition(
    current: AgentToolCallStatus, target: AgentToolCallStatus
) -> None:
    _validate_transition("agent tool call", current, target, AGENT_TOOL_CALL_TRANSITIONS)


WORKSPACE_TRANSITIONS: Mapping[WorkspaceStatus, frozenset[WorkspaceStatus]] = {
    WorkspaceStatus.PENDING: frozenset(
        {WorkspaceStatus.READY, WorkspaceStatus.FAILED, WorkspaceStatus.CANCELLED}
    ),
    WorkspaceStatus.READY: frozenset(
        {WorkspaceStatus.ACTIVE, WorkspaceStatus.FAILED, WorkspaceStatus.CANCELLED}
    ),
    WorkspaceStatus.ACTIVE: frozenset(
        {WorkspaceStatus.WAITING_APPROVAL, WorkspaceStatus.FAILED, WorkspaceStatus.CANCELLED}
    ),
    WorkspaceStatus.WAITING_APPROVAL: frozenset(
        {WorkspaceStatus.COMPLETED, WorkspaceStatus.FAILED, WorkspaceStatus.CANCELLED}
    ),
    WorkspaceStatus.COMPLETED: frozenset(),
    WorkspaceStatus.FAILED: frozenset(),
    WorkspaceStatus.CANCELLED: frozenset(),
}

SANDBOX_TRANSITIONS: Mapping[SandboxStatus, frozenset[SandboxStatus]] = {
    SandboxStatus.PENDING: frozenset({SandboxStatus.RUNNING, SandboxStatus.STOPPED}),
    SandboxStatus.RUNNING: frozenset({SandboxStatus.STOPPED}),
    SandboxStatus.STOPPED: frozenset(),
}

COMMAND_TRANSITIONS: Mapping[CommandStatus, frozenset[CommandStatus]] = {
    CommandStatus.RUNNING: frozenset(
        {
            CommandStatus.SUCCEEDED,
            CommandStatus.FAILED,
            CommandStatus.TIMED_OUT,
            CommandStatus.INTERRUPTED,
        }
    ),
    CommandStatus.SUCCEEDED: frozenset(),
    CommandStatus.FAILED: frozenset(),
    CommandStatus.TIMED_OUT: frozenset(),
    CommandStatus.INTERRUPTED: frozenset(),
}

APPROVAL_TRANSITIONS: Mapping[ApprovalStatus, frozenset[ApprovalStatus]] = {
    ApprovalStatus.PENDING: frozenset(
        {ApprovalStatus.APPROVED, ApprovalStatus.REJECTED, ApprovalStatus.CANCELLED}
    ),
    ApprovalStatus.APPROVED: frozenset(),
    ApprovalStatus.REJECTED: frozenset(),
    ApprovalStatus.CANCELLED: frozenset(),
}


def validate_workspace_transition(current: WorkspaceStatus, target: WorkspaceStatus) -> None:
    _validate_transition("coding workspace", current, target, WORKSPACE_TRANSITIONS)


def validate_sandbox_transition(current: SandboxStatus, target: SandboxStatus) -> None:
    _validate_transition("sandbox", current, target, SANDBOX_TRANSITIONS)


def validate_command_transition(current: CommandStatus, target: CommandStatus) -> None:
    _validate_transition("sandbox command", current, target, COMMAND_TRANSITIONS)


def validate_approval_transition(current: ApprovalStatus, target: ApprovalStatus) -> None:
    _validate_transition("approval", current, target, APPROVAL_TRANSITIONS)
