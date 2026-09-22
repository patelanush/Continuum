"""Poll durable outbox rows and publish acknowledged Kafka messages."""

import asyncio
import logging
import signal
from datetime import UTC, datetime

from aiokafka import AIOKafkaProducer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from durable_agent_runtime.core.config import get_settings
from durable_agent_runtime.core.logging import configure_logging
from durable_agent_runtime.db.models import OutboxEvent
from durable_agent_runtime.db.session import SessionFactory, engine
from durable_agent_runtime.events import StepReadyEvent, workflow_message_key

logger = logging.getLogger(__name__)


async def dispatch_once(
    sessions: async_sessionmaker[AsyncSession], producer: AIOKafkaProducer, *, batch_size: int = 20
) -> int:
    """Publish a bounded batch; the row lock prevents competing dispatchers."""
    async with sessions() as session, session.begin():
        result = await session.scalars(
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.created_at, OutboxEvent.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        rows = list(result)
        for row in rows:
            row.publish_attempts += 1
            try:
                event = StepReadyEvent.from_outbox(row)
                await asyncio.wait_for(
                    producer.send_and_wait(
                        row.topic, key=workflow_message_key(row.workflow_id), value=event.to_bytes()
                    ),
                    timeout=10,
                )
            except Exception as exc:
                row.last_error = str(exc)[:1000]
                logger.warning(
                    "process_type=dispatcher operation=publish_failed event_id=%s workflow_id=%s "
                    "attempt=%s error=%s",
                    row.id,
                    row.workflow_id,
                    row.publish_attempts,
                    type(exc).__name__,
                )
            else:
                row.published_at = datetime.now(UTC)
                row.last_error = None
                logger.info(
                    "process_type=dispatcher operation=published event_id=%s workflow_id=%s "
                    "topic=%s attempt=%s",
                    row.id,
                    row.workflow_id,
                    row.topic,
                    row.publish_attempts,
                )
        return len(rows)


async def dispatch_loop(
    stop: asyncio.Event,
    sessions: async_sessionmaker[AsyncSession],
    producer: AIOKafkaProducer,
    *,
    poll_interval: float,
) -> None:
    while not stop.is_set():
        try:
            count = await dispatch_once(sessions, producer)
        except Exception:
            logger.exception("process_type=dispatcher operation=poll_failed")
            count = 0
        if count < 20:
            try:
                await asyncio.wait_for(stop.wait(), timeout=poll_interval)
            except TimeoutError:
                pass


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
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
            stop, SessionFactory, producer, poll_interval=settings.outbox_poll_interval
        )
    finally:
        await producer.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run())
