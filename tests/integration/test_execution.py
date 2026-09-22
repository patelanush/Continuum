"""PostgreSQL attempt reservation, fencing, and atomic finalization tests."""

import asyncio
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select

from durable_agent_runtime.core.config import Settings
from durable_agent_runtime.db.models import ExecutionAttempt, OutboxEvent, StateTransition
from durable_agent_runtime.domain.enums import ExecutionAttemptStatus, StepStatus, WorkflowStatus
from durable_agent_runtime.events import StepReadyEvent
from durable_agent_runtime.execution.executor import execute_attempt
from durable_agent_runtime.schemas.workflows import WorkflowCreate
from durable_agent_runtime.services.execution import ExecutionService, LostLease
from durable_agent_runtime.services.workflows import WorkflowService
from durable_agent_runtime.worker.processor import process_step_ready
from tests.conftest import TestSession

pytestmark = pytest.mark.integration


async def create_scheduled(
    step_count: int = 2,
    *,
    step_type: str = "noop",
    step_input: dict[str, object] | None = None,
    max_attempts: int = 3,
) -> tuple[UUID, UUID]:
    command = WorkflowCreate.model_validate(
        {
            "workflow_type": "attempt-test",
            "steps": [
                {
                    "name": f"step-{i}",
                    "step_type": step_type,
                    "input": step_input or {},
                    "max_attempts": max_attempts,
                }
                for i in range(step_count)
            ],
        }
    )
    async with TestSession() as session:
        created = await WorkflowService(session).create_workflow(command)
        workflow_id = created.id
    async with TestSession() as session:
        await WorkflowService(session).start_workflow(workflow_id)
    async with TestSession() as session:
        row = await session.scalar(
            select(OutboxEvent).where(OutboxEvent.workflow_id == workflow_id)
        )
        assert row is not None and row.step_id is not None
        event = StepReadyEvent.from_outbox(row)
    async with TestSession() as session:
        assert (
            await process_step_ready(
                session, event, consumer_group="test-execution", worker_id="event-worker"
            )
            == "scheduled"
        )
    return workflow_id, event.step_id


async def claim(executor_id: str = "executor-a", lease_seconds: float = 20) -> ExecutionAttempt:
    async with TestSession() as session:
        attempt = await ExecutionService(session).claim_next(
            executor_id=executor_id, lease_seconds=lease_seconds
        )
    assert attempt is not None
    return attempt


async def attempts_for(workflow_id: UUID) -> list[ExecutionAttempt]:
    async with TestSession() as session:
        return list(
            await session.scalars(
                select(ExecutionAttempt)
                .where(ExecutionAttempt.workflow_id == workflow_id)
                .order_by(ExecutionAttempt.attempt_number)
            )
        )


async def test_claim_sets_lease_and_step_running() -> None:
    workflow_id, step_id = await create_scheduled()
    attempt = await claim()
    assert attempt.step_id == step_id
    assert attempt.status == ExecutionAttemptStatus.RUNNING
    assert attempt.executor_id == "executor-a"
    assert attempt.lease_token is not None
    assert attempt.lease_expires_at is not None
    assert attempt.started_at is not None
    async with TestSession() as session:
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        assert workflow.steps[0].status == StepStatus.RUNNING
        assert workflow.steps[0].attempt_count == 1


async def test_heartbeat_is_fenced_and_extends_lease() -> None:
    await create_scheduled()
    attempt = await claim(lease_seconds=10)
    assert attempt.lease_token is not None
    old_expiry = attempt.lease_expires_at
    async with TestSession() as session:
        assert not await ExecutionService(session).heartbeat(
            attempt.id, executor_id="executor-a", lease_token=uuid4(), lease_seconds=20
        )
    async with TestSession() as session:
        assert await ExecutionService(session).heartbeat(
            attempt.id,
            executor_id="executor-a",
            lease_token=attempt.lease_token,
            lease_seconds=20,
        )
    async with TestSession() as session:
        updated = await session.get(ExecutionAttempt, attempt.id)
        assert updated is not None and old_expiry is not None
        assert updated.lease_expires_at is not None
        assert updated.lease_expires_at > old_expiry


async def test_success_finalizes_attempt_and_next_step_atomically() -> None:
    workflow_id, _step_id = await create_scheduled()
    attempt = await claim()
    assert attempt.lease_token is not None
    async with TestSession() as session:
        await ExecutionService(session).finalize_success(
            attempt.id,
            executor_id="executor-a",
            lease_token=attempt.lease_token,
            output={"ok": True},
        )
    async with TestSession() as session:
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        assert workflow.status == WorkflowStatus.RUNNING
        assert workflow.current_step_position == 1
        assert [step.status for step in workflow.steps] == [StepStatus.SUCCEEDED, StepStatus.READY]
        assert workflow.steps[0].output == {"ok": True}
        assert (
            await session.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(OutboxEvent.workflow_id == workflow_id)
            )
            == 2
        )
        assert (
            await session.scalar(
                select(func.count())
                .select_from(StateTransition)
                .where(StateTransition.workflow_id == workflow_id)
            )
            == 8
        )


async def test_final_step_completes_workflow() -> None:
    workflow_id, _ = await create_scheduled(step_count=1)
    attempt = await claim()
    assert attempt.lease_token is not None
    async with TestSession() as session:
        await ExecutionService(session).finalize_success(
            attempt.id,
            executor_id="executor-a",
            lease_token=attempt.lease_token,
            output={"done": True},
        )
    async with TestSession() as session:
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        assert workflow.status == WorkflowStatus.SUCCEEDED
        assert workflow.steps[0].status == StepStatus.SUCCEEDED
        assert workflow.completed_at is not None
        assert (
            await session.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(OutboxEvent.workflow_id == workflow_id)
            )
            == 1
        )


async def test_failure_fails_workflow_and_attempt() -> None:
    workflow_id, _ = await create_scheduled()
    attempt = await claim()
    assert attempt.lease_token is not None
    async with TestSession() as session:
        await ExecutionService(session).finalize_failure(
            attempt.id,
            executor_id="executor-a",
            lease_token=attempt.lease_token,
            error_code="PERMANENT",
            error_detail="Rejected input",
        )
    async with TestSession() as session:
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        assert workflow.status == WorkflowStatus.FAILED
        assert workflow.steps[0].status == StepStatus.FAILED
        assert workflow.steps[0].error_code == "PERMANENT"
        failed_attempt = await session.get(ExecutionAttempt, attempt.id)
        assert failed_attempt is not None
        assert failed_attempt.status == ExecutionAttemptStatus.FAILED


async def test_unsupported_tool_is_permanent_executor_failure() -> None:
    workflow_id, _ = await create_scheduled(step_count=1, step_type="not-registered")
    attempt = await claim()
    assert (
        await execute_attempt(
            attempt, executor_id="executor-a", sessions=TestSession, settings=Settings()
        )
        == "failed"
    )
    async with TestSession() as session:
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        assert workflow.status == WorkflowStatus.FAILED
        assert workflow.steps[0].error_code == "UNSUPPORTED_STEP_TYPE"
        failed_attempt = await session.get(ExecutionAttempt, attempt.id)
        assert failed_attempt is not None
        assert failed_attempt.status == ExecutionAttemptStatus.FAILED


async def test_transient_http_failure_leaves_attempt_for_lease_recovery() -> None:
    workflow_id, _ = await create_scheduled(
        step_count=1,
        step_type="mock_refund",
        step_input={"customer_id": "offline-service", "amount": "1.00"},
    )
    attempt = await claim()
    assert (
        await execute_attempt(
            attempt,
            executor_id="executor-a",
            sessions=TestSession,
            settings=Settings(mock_payments_url="http://127.0.0.1:1"),
        )
        == "transient"
    )
    async with TestSession() as session:
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        assert workflow.status == WorkflowStatus.RUNNING
        assert workflow.steps[0].status == StepStatus.RUNNING
        same_attempt = await session.get(ExecutionAttempt, attempt.id)
        assert same_attempt is not None
        assert same_attempt.status == ExecutionAttemptStatus.RUNNING


async def test_wrong_token_cannot_finalize_or_mutate() -> None:
    workflow_id, _ = await create_scheduled()
    attempt = await claim()
    async with TestSession() as session:
        with pytest.raises(LostLease):
            await ExecutionService(session).finalize_success(
                attempt.id,
                executor_id="executor-a",
                lease_token=uuid4(),
                output={},
            )
    async with TestSession() as session:
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        assert workflow.steps[0].status == StepStatus.RUNNING
        assert workflow.current_step_position == 0


async def test_two_executors_cannot_claim_same_pending_attempt() -> None:
    await create_scheduled(step_count=1)

    async def one(executor_id: str) -> ExecutionAttempt | None:
        async with TestSession() as session:
            return await ExecutionService(session).claim_next(
                executor_id=executor_id, lease_seconds=20
            )

    claimed = await asyncio.gather(one("a"), one("b"))
    assert sum(attempt is not None for attempt in claimed) == 1


async def test_finalization_rollback_preserves_attempt_step_audit_outbox(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow_id, _ = await create_scheduled()
    attempt = await claim()
    assert attempt.lease_token is not None
    original = WorkflowService.complete_step_in_transaction

    async def fail_after_business(self: WorkflowService, *args: object, **kwargs: object) -> object:
        await original(self, *args, **kwargs)  # type: ignore[arg-type]
        raise RuntimeError("injected before finalization commit")

    monkeypatch.setattr(WorkflowService, "complete_step_in_transaction", fail_after_business)
    async with TestSession() as session:
        with pytest.raises(RuntimeError, match="injected"):
            await ExecutionService(session).finalize_success(
                attempt.id,
                executor_id="executor-a",
                lease_token=attempt.lease_token,
                output={"ok": True},
            )
    async with TestSession() as session:
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        assert workflow.current_step_position == 0
        assert workflow.steps[0].status == StepStatus.RUNNING
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1
        assert await session.scalar(select(func.count()).select_from(StateTransition)) == 6
        same_attempt = await session.get(ExecutionAttempt, attempt.id)
        assert same_attempt is not None
        assert same_attempt.status == ExecutionAttemptStatus.RUNNING
