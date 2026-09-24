"""Kafka consumer with durable processing and manual offset acknowledgement."""

import asyncio
import hashlib
import logging
import os
import signal
from datetime import UTC, datetime
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.structs import ConsumerRecord
from opentelemetry.trace import SpanKind
from pydantic import BaseModel, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from durable_agent_runtime.core.config import get_settings
from durable_agent_runtime.core.logging import configure_logging
from durable_agent_runtime.db.session import SessionFactory, engine
from durable_agent_runtime.events import DEAD_LETTER_TOPIC, STEP_READY_TOPIC, StepReadyEvent
from durable_agent_runtime.observability.context import (
    extract_traceparent,
    traceparent_from_headers,
)
from durable_agent_runtime.observability.metrics import count
from durable_agent_runtime.observability.runtime import configure, span
from durable_agent_runtime.worker.processor import PermanentEventError, process_step_ready

logger = logging.getLogger(__name__)


class DeadLetterRecord(BaseModel):
    original_topic: str
    partition: int
    offset: int
    error_type: str
    error_message: str
    payload_sha256: str
    payload_bytes: int
    failed_at: datetime
    worker_id: str


async def handle_record(
    record: ConsumerRecord,
    producer: AIOKafkaProducer,
    *,
    worker_id: str,
    consumer_group: str,
    sessions: async_sessionmaker[AsyncSession] = SessionFactory,
) -> str:
    """Return only after DB commit or acknowledged DLQ publication."""
    with span(
        "kafka.consume",
        {
            "messaging.system": "kafka",
            "messaging.destination.name": record.topic,
            "messaging.kafka.partition": record.partition,
            "messaging.kafka.offset": record.offset,
            "messaging.operation.name": "process",
        },
        context=extract_traceparent(traceparent_from_headers(record.headers)),
        kind=SpanKind.CONSUMER,
    ) as consume_span:
        result = await _handle_record(
            record, producer, worker_id=worker_id, consumer_group=consumer_group, sessions=sessions
        )
        consume_span.set_attribute("continuum.event.result", result)
        consume_span.set_attribute("continuum.event.duplicate", result == "duplicate")
        return result


async def _handle_record(
    record: ConsumerRecord,
    producer: AIOKafkaProducer,
    *,
    worker_id: str,
    consumer_group: str,
    sessions: async_sessionmaker[AsyncSession],
) -> str:
    try:
        event = StepReadyEvent.from_bytes(record.value)
        if record.key != str(event.workflow_id).encode("ascii"):
            raise PermanentEventError("Kafka key does not match workflow_id")
        async with sessions() as session:
            with span(
                "inbox.dedupe",
                {
                    "continuum.event.id": str(event.event_id),
                    "continuum.event.type": event.event_type,
                    "continuum.workflow.id": str(event.workflow_id),
                    "continuum.step.id": str(event.step_id),
                },
            ):
                result = await process_step_ready(
                    session, event, consumer_group=consumer_group, worker_id=worker_id
                )
        count("continuum_kafka_events_consumed", event_type=event.event_type, result=result)
        if result == "duplicate":
            count("continuum_kafka_duplicates")
        logger.info(
            "process_type=worker worker_id=%s operation=%s event_id=%s workflow_id=%s "
            "step_id=%s topic=%s partition=%s offset=%s",
            worker_id,
            result,
            event.event_id,
            event.workflow_id,
            event.step_id,
            record.topic,
            record.partition,
            record.offset,
        )
        return result
    except (ValidationError, PermanentEventError) as exc:
        dead_letter = DeadLetterRecord(
            original_topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            error_type=type(exc).__name__,
            error_message=(
                "Invalid event envelope" if isinstance(exc, ValidationError) else str(exc)[:500]
            ),
            payload_sha256=hashlib.sha256(record.value).hexdigest(),
            payload_bytes=len(record.value),
            failed_at=datetime.now(UTC),
            worker_id=worker_id,
        )
        await asyncio.wait_for(
            producer.send_and_wait(
                DEAD_LETTER_TOPIC,
                key=record.key,
                value=dead_letter.model_dump_json().encode("utf-8"),
            ),
            timeout=10,
        )
        count("continuum_dlq_messages", reason="invalid_event")
        logger.warning(
            "process_type=worker worker_id=%s operation=dead_letter topic=%s partition=%s "
            "offset=%s error_type=%s",
            worker_id,
            record.topic,
            record.partition,
            record.offset,
            type(exc).__name__,
        )
        return "dead_letter"


async def consume_loop(
    stop: asyncio.Event,
    consumer: AIOKafkaConsumer,
    producer: AIOKafkaProducer,
    *,
    worker_id: str,
    consumer_group: str,
    sessions: async_sessionmaker[AsyncSession] = SessionFactory,
) -> None:
    while not stop.is_set():
        try:
            record = await asyncio.wait_for(consumer.getone(), timeout=1)
        except TimeoutError:
            continue
        except Exception:
            logger.exception("process_type=worker worker_id=%s operation=fetch_failed", worker_id)
            try:
                await asyncio.wait_for(stop.wait(), timeout=1)
            except TimeoutError:
                pass
            continue
        while not stop.is_set():
            try:
                await handle_record(
                    record,
                    producer,
                    worker_id=worker_id,
                    consumer_group=consumer_group,
                    sessions=sessions,
                )
                break
            except Exception:
                logger.exception(
                    "process_type=worker worker_id=%s operation=retry topic=%s "
                    "partition=%s offset=%s",
                    worker_id,
                    record.topic,
                    record.partition,
                    record.offset,
                )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=1)
                except TimeoutError:
                    pass
        if stop.is_set():
            continue
        # The database transaction (or DLQ acknowledgement) finished first. A failed
        # offset commit is safe to replay; it must not trap this worker after a rebalance.
        try:
            await consumer.commit(
                {TopicPartition(record.topic, record.partition): record.offset + 1}
            )
        except Exception:
            logger.exception(
                "process_type=worker worker_id=%s operation=offset_commit_failed "
                "topic=%s partition=%s offset=%s",
                worker_id,
                record.topic,
                record.partition,
                record.offset,
            )


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    configure("continuum-event-worker", settings)
    worker_id = f"{os.getenv('HOSTNAME', 'worker')}-{uuid4().hex[:8]}"
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    consumer: AIOKafkaConsumer | None = None
    producer: AIOKafkaProducer | None = None
    while not stop.is_set():
        consumer = AIOKafkaConsumer(
            STEP_READY_TOPIC,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_consumer_group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            acks="all",
            enable_idempotence=True,
        )
        try:
            await producer.start()
            await consumer.start()
            break
        except Exception:
            logger.exception("process_type=worker worker_id=%s operation=connect_failed", worker_id)
            await producer.stop()
            await consumer.stop()
            try:
                await asyncio.wait_for(stop.wait(), timeout=2)
            except TimeoutError:
                pass
    if stop.is_set() or consumer is None or producer is None:
        return

    logger.info("process_type=worker worker_id=%s operation=started", worker_id)
    try:
        await consume_loop(
            stop,
            consumer,
            producer,
            worker_id=worker_id,
            consumer_group=settings.kafka_consumer_group,
        )
    finally:
        await consumer.stop()
        await producer.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run())
