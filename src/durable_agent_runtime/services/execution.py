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
from durable_agent_runtime.observability.context import (
    current_traceparent,
    extract_traceparent,
    link_from_traceparent,
)
from durable_agent_runtime.observability.metrics import count, duration
from durable_agent_runtime.observability.runtime import span
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
                traceparent=current_traceparent(),
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
                with span(
                    "execution.claim",
                    {
                        "continuum.workflow.id": str(workflow.id),
                        "continuum.step.id": str(step.id),
                        "continuum.step.type": step.step_type,
                        "continuum.execution_attempt.id": str(attempt.id),
                        "continuum.execution_attempt.number": attempt.attempt_number,
                        "continuum.executor.id": executor_id,
                    },
                    context=extract_traceparent(attempt.traceparent),
                ):
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
            workflow = await self.workflows.complete_step_in_transaction(
                attempt.step_id, output=output, causation_id=attempt.id
            )
            step = await self.session.get(WorkflowStep, attempt.step_id)
            assert step is not None
            step_type = step.step_type
            workflow_duration = (
                (workflow.completed_at - workflow.started_at).total_seconds()
                if workflow.started_at and workflow.completed_at
                else None
            )
            workflow_finished = workflow.status == WorkflowStatus.SUCCEEDED
            workflow_type = workflow.workflow_type
        count("continuum_steps_completed", status="succeeded", step_type=step_type)
        if workflow_finished:
            count("continuum_workflows_completed", status="succeeded", workflow_type=workflow_type)
            if workflow_duration is not None:
                duration(
                    "continuum_workflow_duration_seconds",
                    workflow_duration,
                    workflow_type=workflow_type,
                    status="succeeded",
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
            workflow = await self.workflows.fail_step_in_transaction(
                attempt.step_id, error_code=error_code, error_detail=error_detail[:2000]
            )
            step = await self.session.get(WorkflowStep, attempt.step_id)
            assert step is not None
            step_type = step.step_type
            workflow_type = workflow.workflow_type
            workflow_duration = (
                (workflow.completed_at - workflow.started_at).total_seconds()
                if workflow.started_at and workflow.completed_at
                else None
            )
        count("continuum_steps_completed", status="failed", step_type=step_type)
        count("continuum_workflows_completed", status="failed", workflow_type=workflow_type)
        if workflow_duration is not None:
            duration(
                "continuum_workflow_duration_seconds",
                workflow_duration,
                workflow_type=workflow_type,
                status="failed",
            )

    async def recover_expired(self, *, limit: int = 20) -> int:
        """Multiple schedulers may run; workflow-first locks and rechecks fence races."""
        recovered = 0
        recorded: list[tuple[str, float, float]] = []
        failed_workflows: list[tuple[str, str, float | None]] = []
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
                with span(
                    "recovery.expire_attempt",
                    {
                        "continuum.workflow.id": str(workflow_id),
                        "continuum.step.id": str(attempt.step_id),
                        "continuum.execution_attempt.id": str(attempt.id),
                        "continuum.execution_attempt.number": attempt.attempt_number,
                        "continuum.attempt.status": "expired",
                    },
                    context=extract_traceparent(attempt.traceparent),
                    links=link_from_traceparent(attempt.traceparent),
                ):
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
                        with span(
                            "recovery.create_replacement",
                            {
                                "continuum.workflow.id": str(workflow_id),
                                "continuum.step.id": str(step.id),
                                "continuum.execution_attempt.number": attempt.attempt_number + 1,
                                "continuum.recovered": True,
                            },
                            context=extract_traceparent(attempt.traceparent),
                            links=link_from_traceparent(attempt.traceparent),
                        ):
                            self.session.add(
                                ExecutionAttempt(
                                    workflow_id=workflow_id,
                                    step_id=step.id,
                                    attempt_number=attempt.attempt_number + 1,
                                    traceparent=current_traceparent(),
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
                        failed_workflows.append(
                            (
                                workflow.workflow_type,
                                step.step_type,
                                (workflow.completed_at - workflow.started_at).total_seconds()
                                if workflow.started_at and workflow.completed_at
                                else None,
                            )
                        )
                recovered += 1
                recorded.append(
                    (
                        step.step_type if step is not None else "other",
                        (attempt.completed_at - attempt.started_at).total_seconds()
                        if attempt.started_at and attempt.completed_at
                        else 0.0,
                        (attempt.completed_at - attempt.lease_expires_at).total_seconds()
                        if attempt.completed_at and attempt.lease_expires_at
                        else 0.0,
                    )
                )
                logger.warning(
                    "process_type=recovery operation=expired attempt_id=%s workflow_id=%s "
                    "step_id=%s attempt_number=%s",
                    attempt.id,
                    workflow_id,
                    attempt.step_id,
                    attempt.attempt_number,
                )
        for step_type, elapsed, recovery_delay in recorded:
            count("continuum_lease_expirations")
            count("continuum_recoveries", reason="lease_expired")
            count("continuum_execution_attempts", status="expired", step_type=step_type)
            duration(
                "continuum_execution_attempt_duration_seconds",
                elapsed,
                status="expired",
                step_type=step_type,
            )
            duration("continuum_recovery_duration_seconds", recovery_delay)
        for workflow_type, step_type, failed_elapsed in failed_workflows:
            count("continuum_steps_completed", status="failed", step_type=step_type)
            count("continuum_workflows_completed", status="failed", workflow_type=workflow_type)
            if failed_elapsed is not None:
                duration(
                    "continuum_workflow_duration_seconds",
                    failed_elapsed,
                    workflow_type=workflow_type,
                    status="failed",
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
