from uuid import uuid4

import pytest
from pydantic import ValidationError

from durable_agent_runtime.core.config import Settings
from durable_agent_runtime.domain.enums import ExecutionAttemptStatus
from durable_agent_runtime.domain.errors import InvalidStateTransition
from durable_agent_runtime.domain.state_machine import validate_attempt_transition
from durable_agent_runtime.execution.tools import (
    ExecutionContext,
    PermanentToolError,
    RetrySafety,
    can_retry_after_crash,
    execute_tool,
    operation_id,
    retry_safety,
)


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (ExecutionAttemptStatus.PENDING, ExecutionAttemptStatus.RUNNING),
        (ExecutionAttemptStatus.PENDING, ExecutionAttemptStatus.CANCELLED),
        (ExecutionAttemptStatus.RUNNING, ExecutionAttemptStatus.SUCCEEDED),
        (ExecutionAttemptStatus.RUNNING, ExecutionAttemptStatus.FAILED),
        (ExecutionAttemptStatus.RUNNING, ExecutionAttemptStatus.EXPIRED),
        (ExecutionAttemptStatus.RUNNING, ExecutionAttemptStatus.CANCELLED),
    ],
)
def test_valid_attempt_transition(
    current: ExecutionAttemptStatus, target: ExecutionAttemptStatus
) -> None:
    validate_attempt_transition(current, target)


@pytest.mark.parametrize("status", list(ExecutionAttemptStatus))
def test_self_transition_rejected(status: ExecutionAttemptStatus) -> None:
    with pytest.raises(InvalidStateTransition):
        validate_attempt_transition(status, status)


@pytest.mark.parametrize(
    "status",
    [
        ExecutionAttemptStatus.SUCCEEDED,
        ExecutionAttemptStatus.FAILED,
        ExecutionAttemptStatus.EXPIRED,
        ExecutionAttemptStatus.CANCELLED,
    ],
)
def test_terminal_attempt_cannot_restart(status: ExecutionAttemptStatus) -> None:
    with pytest.raises(InvalidStateTransition):
        validate_attempt_transition(status, ExecutionAttemptStatus.RUNNING)


def test_operation_id_is_stable_across_attempts() -> None:
    step_id = uuid4()
    first = ExecutionContext(uuid4(), step_id, uuid4(), 1)
    second = ExecutionContext(first.workflow_id, step_id, uuid4(), 2)
    assert first.operation_id == second.operation_id == operation_id(step_id)
    assert operation_id(uuid4()) != first.operation_id


def test_tool_retry_safety_is_explicit() -> None:
    assert retry_safety("noop") == RetrySafety.IDEMPOTENT
    assert retry_safety("slow_noop") == RetrySafety.IDEMPOTENT
    assert retry_safety("mock_refund") == RetrySafety.IDEMPOTENCY_KEY_SUPPORTED
    with pytest.raises(PermanentToolError, match="No executor"):
        retry_safety("unknown")
    assert can_retry_after_crash("mock_refund", 1, 3)
    assert not can_retry_after_crash("mock_refund", 3, 3)
    assert not can_retry_after_crash("unknown", 1, 3)


def test_lease_settings_reject_unsafe_heartbeat() -> None:
    with pytest.raises(ValidationError):
        Settings(executor_lease_seconds=2, executor_heartbeat_seconds=2)


@pytest.mark.asyncio
async def test_invalid_immutable_tool_input_is_permanent() -> None:
    context = ExecutionContext(uuid4(), uuid4(), uuid4(), 1)
    with pytest.raises(PermanentToolError) as error:
        await execute_tool("mock_refund", {"amount": -1}, context, payments_url="http://invalid")
    assert error.value.code == "INVALID_STEP_INPUT"
