import logging
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import func, select, true
from sqlalchemy.ext.asyncio import AsyncSession

from durable_agent_runtime.db.models import (
    ExecutionAttempt,
    OutboxEvent,
    StateTransition,
    Workflow,
    WorkflowStep,
)
from durable_agent_runtime.domain.enums import (
    EntityType,
    ExecutionAttemptStatus,
    StepStatus,
    WorkflowStatus,
)
from durable_agent_runtime.domain.errors import (
    InvariantViolation,
    StepNotFound,
    WorkflowConflict,
    WorkflowNotFound,
)
from durable_agent_runtime.domain.state_machine import (
    validate_attempt_transition,
    validate_step_transition,
    validate_workflow_transition,
)
from durable_agent_runtime.events import STEP_READY_TOPIC
from durable_agent_runtime.schemas.workflows import WorkflowCreate

logger = logging.getLogger(__name__)

TERMINAL_STEP_STATUSES = {StepStatus.SUCCEEDED, StepStatus.FAILED, StepStatus.CANCELLED}


class WorkflowService:
    """Owns workflow invariants and transaction boundaries."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_workflow(self, command: WorkflowCreate) -> Workflow:
        async with self.session.begin():
            workflow = Workflow(
                workflow_type=command.workflow_type,
                input=command.input,
                status=WorkflowStatus.PENDING,
            )
            workflow.steps = [
                WorkflowStep(
                    position=position,
                    name=step.name,
                    step_type=step.step_type,
                    input=step.input,
                    max_attempts=step.max_attempts,
                    status=StepStatus.PENDING,
                )
                for position, step in enumerate(command.steps)
            ]
            self.session.add(workflow)
            await self.session.flush()
            self._append_transition(
                EntityType.WORKFLOW, workflow.id, workflow.id, None, WorkflowStatus.PENDING.value
            )
            for step in workflow.steps:
                self._append_transition(
                    EntityType.STEP, step.id, workflow.id, None, StepStatus.PENDING.value
                )
        logger.info("workflow_created workflow_id=%s", workflow.id)
        return workflow

    async def get_workflow(self, workflow_id: UUID) -> Workflow:
        workflow = await self.session.scalar(select(Workflow).where(Workflow.id == workflow_id))
        if workflow is None:
            raise WorkflowNotFound(workflow_id)
        return workflow

    async def list_workflows(
        self, *, status: WorkflowStatus | None, limit: int, offset: int
    ) -> tuple[list[Workflow], int]:
        predicate = Workflow.status == status if status is not None else true()
        total = await self.session.scalar(
            select(func.count()).select_from(Workflow).where(predicate)
        )
        result = await self.session.scalars(
            select(Workflow)
            .where(predicate)
            .order_by(Workflow.created_at.desc(), Workflow.id.desc())
            .limit(limit)
            .offset(offset)
        )
        return list(result.unique()), int(total or 0)

    async def start_workflow(self, workflow_id: UUID) -> Workflow:
        async with self.session.begin():
            workflow, steps = await self._lock_workflow(workflow_id)
            if workflow.status == WorkflowStatus.RUNNING:
                return workflow
            if workflow.status != WorkflowStatus.PENDING:
                raise WorkflowConflict(
                    f"Workflow {workflow_id} cannot be started from {workflow.status}"
                )
            if not steps:
                raise InvariantViolation("A workflow must contain at least one step")
            first = steps[0]
            if first.position != 0 or first.status != StepStatus.PENDING:
                raise InvariantViolation("The first workflow step is not pending at position zero")

            now = datetime.now(UTC)
            self._transition_workflow(workflow, WorkflowStatus.RUNNING)
            workflow.started_at = now
            workflow.current_step_position = 0
            self._transition_step(first, StepStatus.READY)
        logger.info("workflow_started workflow_id=%s", workflow_id)
        return workflow

    async def mark_step_running(self, step_id: UUID) -> Workflow:
        async with self.session.begin():
            workflow = await self.mark_step_running_in_transaction(step_id)
        logger.info("step_running workflow_id=%s step_id=%s", workflow.id, step_id)
        return workflow

    async def mark_step_running_in_transaction(self, step_id: UUID) -> Workflow:
        """Apply a command within a caller-owned transaction (worker inbox path)."""
        workflow, steps, step = await self._lock_by_step(step_id)
        if step.status == StepStatus.RUNNING:
            return workflow
        self._assert_current_step(workflow, step)
        if step.status != StepStatus.READY:
            raise WorkflowConflict(f"Step {step_id} cannot run from {step.status}")
        self._transition_step(step, StepStatus.RUNNING)
        step.attempt_count += 1
        step.started_at = datetime.now(UTC)
        self._validate_single_active_step(steps, step)
        return workflow

    async def complete_step(
        self,
        step_id: UUID,
        *,
        output: dict[str, Any] | None = None,
        workflow_output: dict[str, Any] | None = None,
    ) -> Workflow:
        async with self.session.begin():
            workflow = await self.complete_step_in_transaction(
                step_id, output=output, workflow_output=workflow_output
            )
        logger.info("step_completed workflow_id=%s step_id=%s", workflow.id, step_id)
        return workflow

    async def complete_step_in_transaction(
        self,
        step_id: UUID,
        *,
        output: dict[str, Any] | None = None,
        workflow_output: dict[str, Any] | None = None,
        causation_id: UUID | None = None,
    ) -> Workflow:
        workflow, steps, step = await self._lock_by_step(step_id)
        if step.status == StepStatus.SUCCEEDED:
            return workflow
        self._assert_current_step(workflow, step)
        if step.status != StepStatus.RUNNING:
            raise WorkflowConflict(f"Step {step_id} cannot complete from {step.status}")

        self._transition_step(step, StepStatus.SUCCEEDED)
        step.output = output
        step.completed_at = datetime.now(UTC)
        next_position = step.position + 1
        if next_position < len(steps):
            # Release the partial unique active-step slot before activating the next step.
            # This flush remains inside the command transaction and cannot partially commit.
            await self.session.flush()
            next_step = steps[next_position]
            if next_step.position != next_position or next_step.status != StepStatus.PENDING:
                raise InvariantViolation("Next sequential step is not pending")
            self._transition_step(next_step, StepStatus.READY, causation_id=causation_id)
            workflow.current_step_position = next_position
            workflow.version += 1
        else:
            self._transition_workflow(workflow, WorkflowStatus.SUCCEEDED)
            workflow.output = workflow_output
            workflow.completed_at = datetime.now(UTC)
        return workflow

    async def fail_step(
        self, step_id: UUID, *, error_code: str, error_detail: str, reason: str | None = None
    ) -> Workflow:
        async with self.session.begin():
            workflow = await self.fail_step_in_transaction(
                step_id, error_code=error_code, error_detail=error_detail, reason=reason
            )
        logger.info(
            "step_failed workflow_id=%s step_id=%s error_code=%s", workflow.id, step_id, error_code
        )
        return workflow

    async def fail_step_in_transaction(
        self, step_id: UUID, *, error_code: str, error_detail: str, reason: str | None = None
    ) -> Workflow:
        workflow, _steps, step = await self._lock_by_step(step_id)
        self._assert_current_step(workflow, step)
        if step.status != StepStatus.RUNNING:
            raise WorkflowConflict(f"Step {step_id} cannot fail from {step.status}")
        self._transition_step(step, StepStatus.FAILED, reason=reason)
        step.error_code = error_code
        step.error_detail = error_detail
        step.completed_at = datetime.now(UTC)
        self._transition_workflow(workflow, WorkflowStatus.FAILED, reason=reason)
        workflow.completed_at = datetime.now(UTC)
        return workflow

    async def cancel_workflow(self, workflow_id: UUID, *, reason: str | None = None) -> Workflow:
        async with self.session.begin():
            workflow, steps = await self._lock_workflow(workflow_id)
            if workflow.status == WorkflowStatus.CANCELLED:
                return workflow
            if workflow.status not in {WorkflowStatus.PENDING, WorkflowStatus.RUNNING}:
                raise WorkflowConflict(
                    f"Workflow {workflow_id} cannot be cancelled from {workflow.status}"
                )
            self._transition_workflow(workflow, WorkflowStatus.CANCELLED, reason=reason)
            workflow.completed_at = datetime.now(UTC)
            for step in steps:
                if step.status not in TERMINAL_STEP_STATUSES:
                    self._transition_step(step, StepStatus.CANCELLED, reason=reason)
                    step.completed_at = datetime.now(UTC)
            attempts = await self.session.scalars(
                select(ExecutionAttempt)
                .where(
                    ExecutionAttempt.workflow_id == workflow_id,
                    ExecutionAttempt.status.in_(
                        [ExecutionAttemptStatus.PENDING, ExecutionAttemptStatus.RUNNING]
                    ),
                )
                .order_by(ExecutionAttempt.id)
                .with_for_update()
            )
            for attempt in attempts:
                validate_attempt_transition(attempt.status, ExecutionAttemptStatus.CANCELLED)
                attempt.status = ExecutionAttemptStatus.CANCELLED
                attempt.completed_at = datetime.now(UTC)
        logger.info("workflow_cancelled workflow_id=%s", workflow_id)
        return workflow

    async def get_workflow_history(self, workflow_id: UUID) -> list[StateTransition]:
        await self.get_workflow(workflow_id)
        transitions = await self.session.scalars(
            select(StateTransition)
            .where(StateTransition.workflow_id == workflow_id)
            .order_by(StateTransition.created_at, StateTransition.id)
        )
        return list(transitions)

    async def get_execution_attempts(self, workflow_id: UUID) -> list[ExecutionAttempt]:
        await self.get_workflow(workflow_id)
        rows = await self.session.scalars(
            select(ExecutionAttempt)
            .where(ExecutionAttempt.workflow_id == workflow_id)
            .order_by(ExecutionAttempt.created_at, ExecutionAttempt.attempt_number)
        )
        return list(rows)

    async def _lock_workflow(self, workflow_id: UUID) -> tuple[Workflow, list[WorkflowStep]]:
        workflow = await self.session.scalar(
            select(Workflow).where(Workflow.id == workflow_id).with_for_update()
        )
        if workflow is None:
            raise WorkflowNotFound(workflow_id)
        locked_steps = await self.session.scalars(
            select(WorkflowStep)
            .where(WorkflowStep.workflow_id == workflow_id)
            .order_by(WorkflowStep.position)
            .with_for_update()
        )
        return workflow, list(locked_steps)

    async def _lock_by_step(
        self, step_id: UUID
    ) -> tuple[Workflow, list[WorkflowStep], WorkflowStep]:
        workflow_id = await self.session.scalar(
            select(WorkflowStep.workflow_id).where(WorkflowStep.id == step_id)
        )
        if workflow_id is None:
            raise StepNotFound(step_id)
        workflow, steps = await self._lock_workflow(workflow_id)
        step = next((candidate for candidate in steps if candidate.id == step_id), None)
        if step is None:
            raise StepNotFound(step_id)
        return workflow, steps, step

    @staticmethod
    def _assert_current_step(workflow: Workflow, step: WorkflowStep) -> None:
        if workflow.status != WorkflowStatus.RUNNING:
            raise WorkflowConflict(f"Workflow {workflow.id} is not running")
        if workflow.current_step_position != step.position:
            raise WorkflowConflict(f"Step {step.id} is not the current workflow step")

    @staticmethod
    def _validate_single_active_step(steps: list[WorkflowStep], current: WorkflowStep) -> None:
        active = [
            step
            for step in steps
            if step.status in {StepStatus.READY, StepStatus.RUNNING} and step.id != current.id
        ]
        if active:
            raise InvariantViolation("More than one workflow step is ready or running")

    def _transition_workflow(
        self, workflow: Workflow, target: WorkflowStatus, *, reason: str | None = None
    ) -> None:
        previous = workflow.status
        validate_workflow_transition(previous, target)
        workflow.status = target
        workflow.version += 1
        self._append_transition(
            EntityType.WORKFLOW, workflow.id, workflow.id, previous.value, target.value, reason
        )

    def _transition_step(
        self,
        step: WorkflowStep,
        target: StepStatus,
        *,
        reason: str | None = None,
        causation_id: UUID | None = None,
    ) -> None:
        previous = step.status
        validate_step_transition(previous, target)
        step.status = target
        step.version += 1
        self._append_transition(
            EntityType.STEP, step.id, step.workflow_id, previous.value, target.value, reason
        )
        if target == StepStatus.READY:
            self.session.add(
                OutboxEvent(
                    event_type="step.ready",
                    schema_version=1,
                    workflow_id=step.workflow_id,
                    step_id=step.id,
                    correlation_id=step.workflow_id,
                    causation_id=causation_id,
                    payload={},
                    topic=STEP_READY_TOPIC,
                    message_key=str(step.workflow_id),
                    created_at=datetime.now(UTC),
                    publish_attempts=0,
                )
            )

    def _append_transition(
        self,
        entity_type: EntityType,
        entity_id: UUID,
        workflow_id: UUID,
        from_status: str | None,
        to_status: str,
        reason: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.session.add(
            StateTransition(
                entity_type=entity_type,
                entity_id=entity_id,
                workflow_id=workflow_id,
                from_status=from_status,
                to_status=to_status,
                reason=reason,
                details=details,
                created_at=datetime.now(UTC),
            )
        )
