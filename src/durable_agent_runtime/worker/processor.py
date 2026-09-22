"""Atomically deduplicate and execute one step-ready command."""

from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from durable_agent_runtime.db.models import ConsumedEvent, OutboxEvent
from durable_agent_runtime.events import StepReadyEvent
from durable_agent_runtime.services.execution import ExecutionService


class PermanentEventError(Exception):
    """A valid envelope does not match a durable event in PostgreSQL."""


async def process_step_ready(
    session: AsyncSession, event: StepReadyEvent, *, consumer_group: str, worker_id: str
) -> str:
    async with session.begin():
        source = await session.scalar(select(OutboxEvent).where(OutboxEvent.id == event.event_id))
        if (
            source is None
            or source.workflow_id != event.workflow_id
            or source.step_id != event.step_id
            or source.event_type != event.event_type
            or source.schema_version != event.schema_version
            or source.message_key != str(event.workflow_id)
        ):
            raise PermanentEventError("Event envelope does not match a durable outbox event")
        statement = (
            insert(ConsumedEvent)
            .values(
                id=uuid4(),
                consumer_group=consumer_group,
                event_id=event.event_id,
                workflow_id=event.workflow_id,
                step_id=event.step_id,
                event_type=event.event_type,
                worker_id=worker_id,
            )
            .on_conflict_do_nothing(index_elements=["consumer_group", "event_id"])
            .returning(ConsumedEvent.id)
        )
        inserted = await session.scalar(statement)
        if inserted is None:
            return "duplicate"
        return await ExecutionService(session).schedule_initial_attempt(event.step_id)
