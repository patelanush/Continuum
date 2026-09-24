"""Low-frequency gauges sampled from PostgreSQL durable state."""

import asyncio
import logging

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from durable_agent_runtime.db.models import ApprovalRequest, ExecutionAttempt, OutboxEvent, Workflow
from durable_agent_runtime.domain.enums import (
    ApprovalStatus,
    ExecutionAttemptStatus,
    WorkflowStatus,
)
from durable_agent_runtime.observability.metrics import gauge

logger = logging.getLogger(__name__)


async def sample(sessions: async_sessionmaker[AsyncSession]) -> None:
    async with sessions() as session:
        active = await session.scalar(
            select(func.count())
            .select_from(Workflow)
            .where(Workflow.status == WorkflowStatus.RUNNING)
        )
        pending = await session.scalar(
            select(func.count())
            .select_from(ExecutionAttempt)
            .where(ExecutionAttempt.status == ExecutionAttemptStatus.PENDING)
        )
        running = await session.scalar(
            select(func.count())
            .select_from(ExecutionAttempt)
            .where(ExecutionAttempt.status == ExecutionAttemptStatus.RUNNING)
        )
        leases = await session.scalar(
            select(func.count())
            .select_from(ExecutionAttempt)
            .where(
                ExecutionAttempt.status == ExecutionAttemptStatus.RUNNING,
                ExecutionAttempt.lease_expires_at > func.clock_timestamp(),
            )
        )
        outbox = await session.scalar(
            select(func.count()).select_from(OutboxEvent).where(OutboxEvent.published_at.is_(None))
        )
        approvals = await session.scalar(
            select(func.count())
            .select_from(ApprovalRequest)
            .where(ApprovalRequest.status == ApprovalStatus.PENDING)
        )
    for name, value in {
        "continuum_active_workflows": active,
        "continuum_pending_execution_attempts": pending,
        "continuum_running_execution_attempts": running,
        "continuum_active_leases": leases,
        "continuum_unpublished_outbox_events": outbox,
        "continuum_pending_approvals": approvals,
    }.items():
        gauge(name, int(value or 0))


async def sampling_loop(
    stop: asyncio.Event, sessions: async_sessionmaker[AsyncSession], interval: float = 15
) -> None:
    while not stop.is_set():
        try:
            await sample(sessions)
        except Exception:
            logger.exception("telemetry gauge sampling failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            pass
