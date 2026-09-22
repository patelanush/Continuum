import asyncio
from collections.abc import Callable
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from durable_agent_runtime.db.models import StateTransition, WorkflowStep
from durable_agent_runtime.domain.enums import EntityType, StepStatus, WorkflowStatus
from durable_agent_runtime.domain.errors import WorkflowConflict
from durable_agent_runtime.schemas.workflows import WorkflowCreate
from durable_agent_runtime.services.workflows import WorkflowService
from tests.conftest import TestSession


def command(step_count: int = 2) -> WorkflowCreate:
    return WorkflowCreate.model_validate(
        {
            "workflow_type": "demo",
            "input": {"task": "test"},
            "steps": [
                {"name": f"step-{position}", "step_type": "noop"} for position in range(step_count)
            ],
        }
    )


async def create_in_new_session(step_count: int = 2) -> tuple[Any, list[Any]]:
    async with TestSession() as session:
        workflow = await WorkflowService(session).create_workflow(command(step_count))
        return workflow.id, [step.id for step in workflow.steps]


@pytest.mark.integration
async def test_create_persists_ordered_pending_steps(session: AsyncSession) -> None:
    workflow = await WorkflowService(session).create_workflow(command(3))

    async with TestSession() as separate_session:
        persisted = await WorkflowService(separate_session).get_workflow(workflow.id)
        assert persisted.status == WorkflowStatus.PENDING
        assert persisted.version == 1
        assert [step.position for step in persisted.steps] == [0, 1, 2]
        assert {step.status for step in persisted.steps} == {StepStatus.PENDING}


@pytest.mark.integration
async def test_start_is_atomic_and_audited() -> None:
    workflow_id, _ = await create_in_new_session()
    async with TestSession() as session:
        workflow = await WorkflowService(session).start_workflow(workflow_id)
        assert workflow.status == WorkflowStatus.RUNNING
        assert workflow.current_step_position == 0
        assert workflow.started_at is not None
        assert workflow.steps[0].status == StepStatus.READY
        assert workflow.steps[1].status == StepStatus.PENDING

    async with TestSession() as session:
        history = await WorkflowService(session).get_workflow_history(workflow_id)
        pairs = [(item.from_status, item.to_status) for item in history]
        assert ("PENDING", "RUNNING") in pairs
        assert ("PENDING", "READY") in pairs


@pytest.mark.integration
async def test_intermediate_and_final_completion() -> None:
    workflow_id, step_ids = await create_in_new_session()
    async with TestSession() as session:
        await WorkflowService(session).start_workflow(workflow_id)
    async with TestSession() as session:
        running = await WorkflowService(session).mark_step_running(step_ids[0])
        assert running.steps[0].attempt_count == 1
    async with TestSession() as session:
        advanced = await WorkflowService(session).complete_step(step_ids[0], output={"one": 1})
        assert advanced.current_step_position == 1
        assert advanced.steps[0].status == StepStatus.SUCCEEDED
        assert advanced.steps[1].status == StepStatus.READY
    async with TestSession() as session:
        await WorkflowService(session).mark_step_running(step_ids[1])
    async with TestSession() as session:
        completed = await WorkflowService(session).complete_step(
            step_ids[1], output={"two": 2}, workflow_output={"done": True}
        )
        assert completed.status == WorkflowStatus.SUCCEEDED
        assert completed.output == {"done": True}
        assert completed.completed_at is not None
        assert completed.current_step_position == 1


@pytest.mark.integration
async def test_fail_running_step_fails_workflow() -> None:
    workflow_id, step_ids = await create_in_new_session()
    async with TestSession() as session:
        await WorkflowService(session).start_workflow(workflow_id)
    async with TestSession() as session:
        await WorkflowService(session).mark_step_running(step_ids[0])
    async with TestSession() as session:
        failed = await WorkflowService(session).fail_step(
            step_ids[0], error_code="BROKEN", error_detail="expected failure", reason="test"
        )
        assert failed.status == WorkflowStatus.FAILED
        assert failed.steps[0].status == StepStatus.FAILED
        assert failed.steps[0].error_code == "BROKEN"
        assert failed.completed_at is not None


@pytest.mark.integration
@pytest.mark.parametrize("start_first", [False, True])
async def test_cancel_pending_or_running_workflow(start_first: bool) -> None:
    workflow_id, _ = await create_in_new_session(3)
    if start_first:
        async with TestSession() as session:
            await WorkflowService(session).start_workflow(workflow_id)
    async with TestSession() as session:
        cancelled = await WorkflowService(session).cancel_workflow(workflow_id, reason="operator")
        assert cancelled.status == WorkflowStatus.CANCELLED
        assert all(step.status == StepStatus.CANCELLED for step in cancelled.steps)
        assert cancelled.completed_at is not None


@pytest.mark.integration
async def test_terminal_workflow_and_out_of_order_step_are_rejected() -> None:
    workflow_id, step_ids = await create_in_new_session()
    async with TestSession() as session:
        await WorkflowService(session).start_workflow(workflow_id)
    async with TestSession() as session:
        with pytest.raises(WorkflowConflict):
            await WorkflowService(session).mark_step_running(step_ids[1])
    async with TestSession() as session:
        await WorkflowService(session).cancel_workflow(workflow_id)
    async with TestSession() as session:
        with pytest.raises(WorkflowConflict):
            await WorkflowService(session).start_workflow(workflow_id)


@pytest.mark.integration
async def test_failure_rolls_back_all_mutations(monkeypatch: pytest.MonkeyPatch) -> None:
    workflow_id, step_ids = await create_in_new_session()
    async with TestSession() as session:
        await WorkflowService(session).start_workflow(workflow_id)
    async with TestSession() as session:
        await WorkflowService(session).mark_step_running(step_ids[0])

    async with TestSession() as session:
        service = WorkflowService(session)
        original: Callable[..., None] = service._append_transition

        def fail_on_success(*args: Any, **kwargs: Any) -> None:
            if args[4] == StepStatus.SUCCEEDED.value:
                raise RuntimeError("injected audit failure")
            original(*args, **kwargs)

        monkeypatch.setattr(service, "_append_transition", fail_on_success)
        with pytest.raises(RuntimeError, match="injected"):
            await service.complete_step(step_ids[0])

    async with TestSession() as session:
        persisted = await WorkflowService(session).get_workflow(workflow_id)
        assert persisted.steps[0].status == StepStatus.RUNNING
        assert persisted.steps[1].status == StepStatus.PENDING
        assert persisted.current_step_position == 0


@pytest.mark.integration
async def test_concurrent_start_writes_each_transition_once() -> None:
    workflow_id, _ = await create_in_new_session()

    async def start() -> WorkflowStatus:
        async with TestSession() as session:
            return (await WorkflowService(session).start_workflow(workflow_id)).status

    statuses = await asyncio.gather(*(start() for _ in range(6)))
    assert statuses == [WorkflowStatus.RUNNING] * 6

    async with TestSession() as session:
        transition_count = await session.scalar(
            select(func.count())
            .select_from(StateTransition)
            .where(
                StateTransition.workflow_id == workflow_id,
                StateTransition.from_status == "PENDING",
                StateTransition.to_status.in_(["RUNNING", "READY"]),
            )
        )
        assert transition_count == 2


@pytest.mark.integration
async def test_concurrent_completion_advances_once() -> None:
    workflow_id, step_ids = await create_in_new_session()
    async with TestSession() as session:
        await WorkflowService(session).start_workflow(workflow_id)
    async with TestSession() as session:
        await WorkflowService(session).mark_step_running(step_ids[0])

    async def complete() -> WorkflowStatus:
        async with TestSession() as session:
            return (await WorkflowService(session).complete_step(step_ids[0])).status

    await asyncio.gather(*(complete() for _ in range(6)))
    async with TestSession() as session:
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        assert workflow.current_step_position == 1
        assert workflow.steps[1].status == StepStatus.READY
        transitions = await WorkflowService(session).get_workflow_history(workflow_id)
        assert sum(item.to_status == "SUCCEEDED" for item in transitions) == 1
        assert (
            sum(item.to_status == "READY" and item.entity_id == step_ids[1] for item in transitions)
            == 1
        )


@pytest.mark.integration
async def test_unique_step_position_is_enforced() -> None:
    workflow_id, _ = await create_in_new_session()
    async with TestSession() as session:
        session.add(
            WorkflowStep(
                workflow_id=workflow_id,
                position=0,
                name="duplicate",
                step_type="noop",
                status=StepStatus.PENDING,
            )
        )
        with pytest.raises(IntegrityError):
            await session.commit()


@pytest.mark.integration
async def test_history_is_chronological_and_has_entity_types() -> None:
    workflow_id, _ = await create_in_new_session(1)
    async with TestSession() as session:
        await WorkflowService(session).start_workflow(workflow_id)
    async with TestSession() as session:
        history = await WorkflowService(session).get_workflow_history(workflow_id)
        assert [item.created_at for item in history] == sorted(item.created_at for item in history)
        assert {item.entity_type for item in history} == {EntityType.WORKFLOW, EntityType.STEP}
