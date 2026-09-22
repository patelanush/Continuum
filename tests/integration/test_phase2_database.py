import asyncio
from uuid import UUID

import pytest
from sqlalchemy import func, select

from durable_agent_runtime.db.models import (
    ConsumedEvent,
    ExecutionAttempt,
    OutboxEvent,
    StateTransition,
)
from durable_agent_runtime.domain.enums import StepStatus, WorkflowStatus
from durable_agent_runtime.events import StepReadyEvent
from durable_agent_runtime.schemas.workflows import WorkflowCreate
from durable_agent_runtime.services.execution import ExecutionService
from durable_agent_runtime.services.workflows import WorkflowService
from durable_agent_runtime.worker.processor import process_step_ready
from tests.conftest import TestSession

pytestmark = pytest.mark.integration


def command(count: int = 2) -> WorkflowCreate:
    return WorkflowCreate.model_validate(
        {
            "workflow_type": "phase2-test",
            "steps": [
                {"name": f"step-{position}", "step_type": "noop"} for position in range(count)
            ],
        }
    )


async def create_started(count: int = 2) -> tuple[UUID, StepReadyEvent]:
    async with TestSession() as session:
        workflow = await WorkflowService(session).create_workflow(command(count))
        workflow_id = workflow.id
    async with TestSession() as session:
        await WorkflowService(session).start_workflow(workflow_id)
    async with TestSession() as session:
        row = await session.scalar(
            select(OutboxEvent).where(OutboxEvent.workflow_id == workflow_id)
        )
        assert row is not None
        return workflow_id, StepReadyEvent.from_outbox(row)


async def outbox_count(workflow_id: UUID) -> int:
    async with TestSession() as session:
        count = await session.scalar(
            select(func.count())
            .select_from(OutboxEvent)
            .where(OutboxEvent.workflow_id == workflow_id)
        )
        return int(count or 0)


async def history_count(workflow_id: UUID) -> int:
    async with TestSession() as session:
        count = await session.scalar(
            select(func.count())
            .select_from(StateTransition)
            .where(StateTransition.workflow_id == workflow_id)
        )
        return int(count or 0)


async def test_start_creates_one_outbox_event_and_duplicate_start_does_not() -> None:
    workflow_id, event = await create_started()
    assert event.workflow_id == workflow_id
    assert event.event_type == "step.ready"
    assert await outbox_count(workflow_id) == 1
    async with TestSession() as session:
        await WorkflowService(session).start_workflow(workflow_id)
    assert await outbox_count(workflow_id) == 1


async def test_intermediate_creates_next_event_and_final_does_not() -> None:
    workflow_id, first = await create_started()
    async with TestSession() as session:
        await WorkflowService(session).mark_step_running(first.step_id)
    async with TestSession() as session:
        await WorkflowService(session).complete_step(first.step_id)
    assert await outbox_count(workflow_id) == 2
    async with TestSession() as session:
        rows = list(
            await session.scalars(
                select(OutboxEvent)
                .where(OutboxEvent.workflow_id == workflow_id)
                .order_by(OutboxEvent.created_at)
            )
        )
        assert rows[0].id == first.event_id
        assert rows[1].step_id != first.step_id
        next_step_id = rows[1].step_id
        assert next_step_id is not None
    async with TestSession() as session:
        await WorkflowService(session).mark_step_running(next_step_id)
    async with TestSession() as session:
        await WorkflowService(session).complete_step(next_step_id)
    assert await outbox_count(workflow_id) == 2


async def test_state_audit_outbox_roll_back_together(monkeypatch: pytest.MonkeyPatch) -> None:
    async with TestSession() as session:
        workflow = await WorkflowService(session).create_workflow(command())
        workflow_id = workflow.id
    original = WorkflowService._transition_step

    def fail_after_ready(self: WorkflowService, *args: object, **kwargs: object) -> None:
        original(self, *args, **kwargs)  # type: ignore[arg-type]
        if args[1] == StepStatus.READY:
            raise RuntimeError("injected after outbox insertion")

    monkeypatch.setattr(WorkflowService, "_transition_step", fail_after_ready)
    async with TestSession() as session:
        with pytest.raises(RuntimeError, match="injected"):
            await WorkflowService(session).start_workflow(workflow_id)
    async with TestSession() as session:
        persisted = await WorkflowService(session).get_workflow(workflow_id)
        assert persisted.status == WorkflowStatus.PENDING
        assert persisted.steps[0].status == StepStatus.PENDING
    assert await outbox_count(workflow_id) == 0
    assert await history_count(workflow_id) == 3


async def test_inbox_dedupe_and_lost_offset_ack_simulation() -> None:
    workflow_id, event = await create_started()
    async with TestSession() as session:
        assert (
            await process_step_ready(
                session, event, consumer_group="test-workers", worker_id="worker-a"
            )
            == "scheduled"
        )
    # The database committed, but this test deliberately does not commit any Kafka offset.
    # A second delivery of the same event_id must be a durable no-op.
    before_history = await history_count(workflow_id)
    async with TestSession() as session:
        assert (
            await process_step_ready(
                session, event, consumer_group="test-workers", worker_id="worker-b"
            )
            == "duplicate"
        )
    assert await history_count(workflow_id) == before_history
    assert await outbox_count(workflow_id) == 1
    async with TestSession() as session:
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        assert workflow.current_step_position == 0
        assert workflow.steps[0].status == StepStatus.READY
        assert await session.scalar(select(func.count()).select_from(ExecutionAttempt)) == 1
        inbox = list(
            await session.scalars(
                select(ConsumedEvent).where(ConsumedEvent.event_id == event.event_id)
            )
        )
        assert len(inbox) == 1


async def test_stale_distinct_event_is_recorded_without_mutation() -> None:
    workflow_id, event = await create_started()
    async with TestSession() as session:
        await WorkflowService(session).cancel_workflow(workflow_id)
    before_history = await history_count(workflow_id)
    async with TestSession() as session:
        assert (
            await process_step_ready(
                session, event, consumer_group="test-workers", worker_id="worker-a"
            )
            == "stale"
        )
    assert await history_count(workflow_id) == before_history
    assert await outbox_count(workflow_id) == 1


async def test_consumer_failure_rolls_back_inbox_and_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow_id, event = await create_started()
    original = ExecutionService.schedule_initial_attempt

    async def fail_after_execution(self: ExecutionService, step_id: UUID) -> str:
        await original(self, step_id)
        raise RuntimeError("injected after attempt insertion")

    monkeypatch.setattr(ExecutionService, "schedule_initial_attempt", fail_after_execution)
    async with TestSession() as session:
        with pytest.raises(RuntimeError, match="injected"):
            await process_step_ready(
                session, event, consumer_group="test-workers", worker_id="worker-a"
            )
    async with TestSession() as session:
        assert await session.scalar(select(func.count()).select_from(ConsumedEvent)) == 0
        assert await session.scalar(select(func.count()).select_from(ExecutionAttempt)) == 0
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        assert workflow.steps[0].status == StepStatus.READY
        assert workflow.steps[1].status == StepStatus.PENDING
    assert await outbox_count(workflow_id) == 1
    assert await history_count(workflow_id) == 5


async def test_concurrent_duplicate_consumers_insert_once() -> None:
    workflow_id, event = await create_started()

    async def consume(worker_id: str) -> str:
        async with TestSession() as session:
            return await process_step_ready(
                session, event, consumer_group="test-workers", worker_id=worker_id
            )

    results = await asyncio.gather(*(consume(f"worker-{index}") for index in range(4)))
    assert sorted(results) == ["duplicate", "duplicate", "duplicate", "scheduled"]
    assert await outbox_count(workflow_id) == 1
    async with TestSession() as session:
        assert await session.scalar(select(func.count()).select_from(ExecutionAttempt)) == 1


async def test_unsupported_step_type_is_durably_scheduled() -> None:
    command = WorkflowCreate.model_validate(
        {"workflow_type": "unsupported", "steps": [{"name": "tool", "step_type": "external"}]}
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
        assert row is not None
        event = StepReadyEvent.from_outbox(row)
    async with TestSession() as session:
        assert (
            await process_step_ready(
                session, event, consumer_group="test-workers", worker_id="worker-a"
            )
            == "scheduled"
        )
    async with TestSession() as session:
        pending = await WorkflowService(session).get_workflow(workflow_id)
        assert pending.status == WorkflowStatus.RUNNING
        assert pending.steps[0].status == StepStatus.READY
        assert await session.scalar(select(func.count()).select_from(ExecutionAttempt)) == 1
    assert await outbox_count(workflow_id) == 1
