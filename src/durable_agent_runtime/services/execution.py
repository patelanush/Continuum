"""Durable reserve/finalize operations; no external I/O belongs in this module."""

import logging
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from durable_agent_runtime.db.models import ExecutionAttempt, Workflow, WorkflowStep
from durable_agent_runtime.domain.enums import ExecutionAttemptStatus, StepStatus, WorkflowStatus
from durable_agent_runtime.domain.errors import WorkflowConflict
from durable_agent_runtime.domain.state_machine import validate_attempt_transition
from durable_agent_runtime.execution.tools import can_retry_after_crash
from durable_agent_runtime.services.workflows import WorkflowService

logger = logging.getLogger(__name__)


class LostLease(WorkflowConflict):
    """The caller no longer owns an attempt or its lease has expired."""


class ExecutionService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.workflows = WorkflowService(session)

    async def schedule_initial_attempt(self, step_id: UUID) -> str:
        """Caller owns the inbox transaction; unique DB indexes enforce one active attempt."""
        workflow, _steps, step = await self.workflows._lock_by_step(step_id)
        if (
            workflow.status != WorkflowStatus.RUNNING
            or workflow.current_step_position != step.position
            or step.status != StepStatus.READY
        ):
            return "stale"
        statement = (
            insert(ExecutionAttempt)
            .values(
                id=uuid4(),
                workflow_id=workflow.id,
                step_id=step_id,
                attempt_number=1,
                status=ExecutionAttemptStatus.PENDING,
            )
            .on_conflict_do_nothing()
            .returning(ExecutionAttempt.id)
        )
        created = await self.session.scalar(statement)
        return "scheduled" if created else "already_scheduled"

    async def claim_next(
        self, *, executor_id: str, lease_seconds: float
    ) -> ExecutionAttempt | None:
        """Lock workflow, steps, then attempt; SKIP LOCKED allows competing executors."""
        async with self.session.begin():
            candidates = (
                await self.session.execute(
                    select(ExecutionAttempt.id, ExecutionAttempt.workflow_id)
                    .where(
                        ExecutionAttempt.status == ExecutionAttemptStatus.PENDING,
                        (ExecutionAttempt.next_eligible_at.is_(None))
                        | (ExecutionAttempt.next_eligible_at <= func.clock_timestamp()),
                    )
                    .order_by(ExecutionAttempt.created_at, ExecutionAttempt.id)
                    .limit(30)
                )
            ).all()
            for attempt_id, workflow_id in candidates:
                workflow = await self.session.scalar(
                    select(Workflow)
                    .where(Workflow.id == workflow_id)
                    .with_for_update(skip_locked=True)
                )
                if workflow is None:
                    continue
                steps = list(
                    await self.session.scalars(
                        select(WorkflowStep)
                        .where(WorkflowStep.workflow_id == workflow_id)
                        .order_by(WorkflowStep.position)
                        .with_for_update()
                    )
                )
                attempt = await self.session.scalar(
                    select(ExecutionAttempt)
                    .where(ExecutionAttempt.id == attempt_id)
                    .with_for_update(skip_locked=True)
                )
                if attempt is None or attempt.status != ExecutionAttemptStatus.PENDING:
                    continue
                step = next((item for item in steps if item.id == attempt.step_id), None)
                if (
                    step is None
                    or workflow.status != WorkflowStatus.RUNNING
                    or workflow.current_step_position != step.position
                    or step.status not in {StepStatus.READY, StepStatus.RUNNING}
                    or (step.status == StepStatus.RUNNING and attempt.attempt_number == 1)
                ):
                    self._transition(attempt, ExecutionAttemptStatus.CANCELLED)
                    attempt.completed_at = await self._db_now()
                    continue
                now = await self._db_now()
                self._transition(attempt, ExecutionAttemptStatus.RUNNING)
                attempt.executor_id = executor_id
                attempt.lease_token = uuid4()
                attempt.started_at = now
                attempt.last_heartbeat_at = now
                attempt.lease_expires_at = now + timedelta(seconds=lease_seconds)
                if step.status == StepStatus.READY:
                    await self.workflows.mark_step_running_in_transaction(step.id)
                else:
                    step.attempt_count += 1
                    step.version += 1
                logger.info(
                    "process_type=executor operation=claimed workflow_id=%s step_id=%s "
                    "attempt_id=%s attempt_number=%s executor_id=%s",
                    workflow.id,
                    step.id,
                    attempt.id,
                    attempt.attempt_number,
                    executor_id,
                )
                return attempt
        return None

    async def heartbeat(
        self, attempt_id: UUID, *, executor_id: str, lease_token: UUID, lease_seconds: float
    ) -> bool:
        """Database-time compare-and-set; an expired or superseded lease cannot renew."""
        async with self.session.begin():
            now = await self._db_now()
            result = await self.session.scalar(
                update(ExecutionAttempt)
                .where(
                    ExecutionAttempt.id == attempt_id,
                    ExecutionAttempt.status == ExecutionAttemptStatus.RUNNING,
                    ExecutionAttempt.executor_id == executor_id,
                    ExecutionAttempt.lease_token == lease_token,
                    ExecutionAttempt.lease_expires_at > func.clock_timestamp(),
                )
                .values(
                    last_heartbeat_at=now,
                    lease_expires_at=now + timedelta(seconds=lease_seconds),
                )
                .returning(ExecutionAttempt.id)
            )
            return result is not None

    async def finalize_success(
        self,
        attempt_id: UUID,
        *,
        executor_id: str,
        lease_token: UUID,
        output: dict[str, Any],
    ) -> None:
        async with self.session.begin():
            attempt = await self._lock_owned(attempt_id, executor_id, lease_token)
            self._transition(attempt, ExecutionAttemptStatus.SUCCEEDED)
            attempt.output = output
            attempt.completed_at = await self._db_now()
            await self.workflows.complete_step_in_transaction(
                attempt.step_id, output=output, causation_id=attempt.id
            )
        logger.info(
            "process_type=executor operation=succeeded attempt_id=%s workflow_id=%s step_id=%s",
            attempt_id,
            attempt.workflow_id,
            attempt.step_id,
        )

    async def finalize_failure(
        self,
        attempt_id: UUID,
        *,
        executor_id: str,
        lease_token: UUID,
        error_code: str,
        error_detail: str,
    ) -> None:
        async with self.session.begin():
            attempt = await self._lock_owned(attempt_id, executor_id, lease_token)
            self._transition(attempt, ExecutionAttemptStatus.FAILED)
            attempt.error_code = error_code
            attempt.error_detail = error_detail[:2000]
            attempt.completed_at = await self._db_now()
            await self.workflows.fail_step_in_transaction(
                attempt.step_id, error_code=error_code, error_detail=error_detail[:2000]
            )

    async def recover_expired(self, *, limit: int = 20) -> int:
        """Multiple schedulers may run; workflow-first locks and rechecks fence races."""
        recovered = 0
        async with self.session.begin():
            candidates = (
                await self.session.execute(
                    select(ExecutionAttempt.id, ExecutionAttempt.workflow_id)
                    .where(
                        ExecutionAttempt.status == ExecutionAttemptStatus.RUNNING,
                        ExecutionAttempt.lease_expires_at < func.clock_timestamp(),
                    )
                    .order_by(ExecutionAttempt.lease_expires_at, ExecutionAttempt.id)
                    .limit(limit)
                )
            ).all()
            for attempt_id, workflow_id in candidates:
                workflow = await self.session.scalar(
                    select(Workflow)
                    .where(Workflow.id == workflow_id)
                    .with_for_update(skip_locked=True)
                )
                if workflow is None:
                    continue
                steps = list(
                    await self.session.scalars(
                        select(WorkflowStep)
                        .where(WorkflowStep.workflow_id == workflow_id)
                        .order_by(WorkflowStep.position)
                        .with_for_update()
                    )
                )
                attempt = await self.session.scalar(
                    select(ExecutionAttempt)
                    .where(
                        ExecutionAttempt.id == attempt_id,
                        ExecutionAttempt.status == ExecutionAttemptStatus.RUNNING,
                        ExecutionAttempt.lease_expires_at < func.clock_timestamp(),
                    )
                    .with_for_update(skip_locked=True)
                )
                if attempt is None:
                    continue
                self._transition(attempt, ExecutionAttemptStatus.EXPIRED)
                attempt.completed_at = await self._db_now()
                step = next((item for item in steps if item.id == attempt.step_id), None)
                if (
                    step is not None
                    and workflow.status == WorkflowStatus.RUNNING
                    and workflow.current_step_position == step.position
                    and step.status == StepStatus.RUNNING
                ):
                    if can_retry_after_crash(
                        step.step_type, attempt.attempt_number, step.max_attempts
                    ):
                        await self.session.flush()
                        self.session.add(
                            ExecutionAttempt(
                                workflow_id=workflow_id,
                                step_id=step.id,
                                attempt_number=attempt.attempt_number + 1,
                                status=ExecutionAttemptStatus.PENDING,
                            )
                        )
                    else:
                        error_code = (
                            "MAX_EXECUTION_ATTEMPTS_EXCEEDED"
                            if attempt.attempt_number >= step.max_attempts
                            else "UNSAFE_CRASH_RETRY"
                        )
                        await self.workflows.fail_step_in_transaction(
                            step.id,
                            error_code=error_code,
                            error_detail="Execution lease expired without a safe retry",
                        )
                recovered += 1
                logger.warning(
                    "process_type=recovery operation=expired attempt_id=%s workflow_id=%s "
                    "step_id=%s attempt_number=%s",
                    attempt.id,
                    workflow_id,
                    attempt.step_id,
                    attempt.attempt_number,
                )
        return recovered

    async def _lock_owned(
        self, attempt_id: UUID, executor_id: str, lease_token: UUID
    ) -> ExecutionAttempt:
        identity = (
            await self.session.execute(
                select(ExecutionAttempt.workflow_id, ExecutionAttempt.step_id).where(
                    ExecutionAttempt.id == attempt_id
                )
            )
        ).one_or_none()
        if identity is None:
            raise LostLease(f"Attempt {attempt_id} does not exist")
        workflow_id, step_id = identity
        workflow, _steps = await self.workflows._lock_workflow(workflow_id)
        attempt = await self.session.scalar(
            select(ExecutionAttempt).where(ExecutionAttempt.id == attempt_id).with_for_update()
        )
        if attempt is None or not await self._owns(attempt, executor_id, lease_token):
            raise LostLease(f"Attempt {attempt_id} lease is no longer owned")
        step = await self.session.get(WorkflowStep, step_id)
        if (
            step is None
            or workflow.status != WorkflowStatus.RUNNING
            or workflow.current_step_position != step.position
            or step.status != StepStatus.RUNNING
        ):
            raise LostLease(f"Attempt {attempt_id} step is no longer executable")
        return attempt

    async def _owns(self, attempt: ExecutionAttempt, executor_id: str, token: UUID) -> bool:
        return (
            attempt.status == ExecutionAttemptStatus.RUNNING
            and attempt.executor_id == executor_id
            and attempt.lease_token == token
            and attempt.lease_expires_at is not None
            and bool(
                await self.session.scalar(select(func.clock_timestamp() < attempt.lease_expires_at))
            )
        )

    async def _db_now(self) -> datetime:
        value = await self.session.scalar(select(func.clock_timestamp()))
        assert isinstance(value, datetime)
        return value

    @staticmethod
    def _transition(attempt: ExecutionAttempt, target: ExecutionAttemptStatus) -> None:
        validate_attempt_transition(attempt.status, target)
        attempt.status = target
