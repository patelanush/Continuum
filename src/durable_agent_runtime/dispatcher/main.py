"""Publish durable outbox messages without holding database locks over broker I/O."""

import asyncio
import logging
import os
import signal
from datetime import UTC, datetime, timedelta
from time import time_ns
from uuid import UUID, uuid4

from aiokafka import AIOKafkaProducer
from opentelemetry.trace import SpanKind
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from durable_agent_runtime.core.config import get_settings
from durable_agent_runtime.core.logging import configure_logging
from durable_agent_runtime.db.models import OutboxEvent
from durable_agent_runtime.db.session import SessionFactory, engine
from durable_agent_runtime.events import StepReadyEvent, workflow_message_key
from durable_agent_runtime.observability.context import (
    current_traceparent,
    extract_traceparent,
    kafka_headers,
)
from durable_agent_runtime.observability.metrics import count, duration
from durable_agent_runtime.observability.runtime import configure, error, span

logger = logging.getLogger(__name__)


async def claim_batch(
    sessions: async_sessionmaker[AsyncSession], *, batch_size: int, lease_seconds: float
) -> list[tuple[OutboxEvent, UUID]]:
    """Short transaction; a crashed publisher's claim expires by database time."""
    async with sessions() as session, session.begin():
        now = await session.scalar(select(func.clock_timestamp()))
        assert now is not None
        rows = list(
            await session.scalars(
                select(OutboxEvent)
                .where(
                    OutboxEvent.published_at.is_(None),
                    (OutboxEvent.publish_lease_expires_at.is_(None))
                    | (OutboxEvent.publish_lease_expires_at < func.clock_timestamp()),
                )
                .order_by(OutboxEvent.created_at, OutboxEvent.id)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
        )
        claimed: list[tuple[OutboxEvent, UUID]] = []
        for row in rows:
            token = uuid4()
            row.publish_lease_token = token
            row.publish_lease_expires_at = now + timedelta(seconds=lease_seconds)
            row.publish_attempts += 1
            claimed.append((row, token))
        return claimed


async def finish_publish(
    sessions: async_sessionmaker[AsyncSession],
    event_id: UUID,
    token: UUID,
    *,
    error: str | None,
) -> bool:
    """Fenced finalization: a superseded publisher cannot update a newer claim."""
    values: dict[str, object] = {
        "publish_lease_token": None,
        "publish_lease_expires_at": None,
        "last_error": error,
    }
    if error is None:
        values["published_at"] = func.clock_timestamp()
    async with sessions() as session, session.begin():
        updated = await session.scalar(
            update(OutboxEvent)
            .where(
                OutboxEvent.id == event_id,
                OutboxEvent.publish_lease_token == token,
                OutboxEvent.published_at.is_(None),
            )
            .values(**values)
            .returning(OutboxEvent.id)
        )
        return updated is not None


async def dispatch_once(
    sessions: async_sessionmaker[AsyncSession],
    producer: AIOKafkaProducer,
    *,
    batch_size: int = 20,
    lease_seconds: float = 30,
) -> int:
    """At-least-once publish; each event ID survives an ack-before-finalize crash."""
    claim_started = time_ns()
    claimed = await claim_batch(sessions, batch_size=batch_size, lease_seconds=lease_seconds)
    if claimed:
        with span(
            "outbox.claim",
            {"continuum.outbox.batch_size": len(claimed)},
            start_time=claim_started,
        ):
            pass

    async def publish(row: OutboxEvent, token: UUID) -> None:
        attributes = {
            "continuum.workflow.id": str(row.workflow_id),
            "continuum.step.id": str(row.step_id),
            "continuum.event.id": str(row.id),
            "continuum.event.type": row.event_type,
            "continuum.publish.attempt": row.publish_attempts,
            "messaging.system": "kafka",
            "messaging.destination.name": row.topic,
            "messaging.kafka.message.key": str(row.workflow_id),
            "messaging.operation.name": "send",
        }
        with span(
            "kafka.publish",
            attributes,
            context=extract_traceparent(row.traceparent),
            kind=SpanKind.PRODUCER,
        ) as publish_span:
            try:
                event = StepReadyEvent.from_outbox(row)
                await asyncio.wait_for(
                    producer.send_and_wait(
                        row.topic,
                        key=workflow_message_key(row.workflow_id),
                        value=event.to_bytes(),
                        headers=kafka_headers(current_traceparent()),
                    ),
                    timeout=10,
                )
                # Test-only hook. FaultLab kills the dispatcher after the broker ack;
                # the claim then expires, so the same event ID is republished.
                if (
                    os.getenv("APP_ENV") == "faultlab"
                    and os.getenv("FAULTLAB_DISPATCHER_PAUSE_AFTER_ACK") == "1"
                ):
                    await asyncio.Event().wait()
            except Exception as exc:
                error(publish_span, "kafka_publish")
                with span("outbox.finalize", attributes):
                    await finish_publish(sessions, row.id, token, error=str(exc)[:1000])
                logger.warning(
                    "process_type=dispatcher operation=publish_failed event_id=%s workflow_id=%s "
                    "attempt=%s error=%s",
                    row.id,
                    row.workflow_id,
                    row.publish_attempts,
                    type(exc).__name__,
                )
            else:
                with span("outbox.finalize", attributes):
                    owned = await finish_publish(sessions, row.id, token, error=None)
                if owned:
                    count("continuum_kafka_events_published", event_type=row.event_type)
                    duration(
                        "continuum_kafka_publish_delay_seconds",
                        (datetime.now(UTC) - row.created_at).total_seconds(),
                        event_type=row.event_type,
                    )
                logger.info(
                    "process_type=dispatcher operation=published event_id=%s workflow_id=%s "
                    "topic=%s attempt=%s claim_owned=%s",
                    row.id,
                    row.workflow_id,
                    row.topic,
                    row.publish_attempts,
                    owned,
                )

    results = await asyncio.gather(
        *(publish(row, token) for row, token in claimed), return_exceptions=True
    )
    for result in results:
        if isinstance(result, BaseException):
            logger.error("process_type=dispatcher operation=finalize_failed error=%r", result)
    return len(claimed)


async def dispatch_loop(
    stop: asyncio.Event,
    sessions: async_sessionmaker[AsyncSession],
    producer: AIOKafkaProducer,
    *,
    poll_interval: float,
    lease_seconds: float = 30,
) -> None:
    while not stop.is_set():
        try:
            await dispatch_once(sessions, producer, lease_seconds=lease_seconds)
        except Exception:
            logger.exception("process_type=dispatcher operation=poll_failed")
        # Also pace unsuccessful immediate publishes (for example an invalid
        # outbox envelope); otherwise a poisoned row becomes a tight DB loop.
        try:
            await asyncio.wait_for(stop.wait(), timeout=poll_interval)
        except TimeoutError:
            pass


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    configure("continuum-dispatcher", settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    producer: AIOKafkaProducer | None = None
    while not stop.is_set():
        producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            acks="all",
            enable_idempotence=True,
            request_timeout_ms=10000,
        )
        try:
            await producer.start()
            break
        except Exception:
            logger.exception("process_type=dispatcher operation=connect_failed")
            await producer.stop()
            try:
                await asyncio.wait_for(stop.wait(), timeout=2)
            except TimeoutError:
                pass
    if stop.is_set() or producer is None:
        return
    try:
        await dispatch_loop(
            stop,
            SessionFactory,
            producer,
            poll_interval=settings.outbox_poll_interval,
            lease_seconds=settings.outbox_publish_lease_seconds,
        )
    finally:
        await producer.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run())
