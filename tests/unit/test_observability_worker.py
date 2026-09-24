"""Event metrics reflect dedupe and DLQ handling without changing business outcomes."""

import asyncio
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.structs import ConsumerRecord
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import durable_agent_runtime.worker.main as worker
from durable_agent_runtime.events import STEP_READY_TOPIC, StepReadyEvent


class FakeProducer:
    def __init__(self) -> None:
        self.published: list[tuple[str, bytes | None, bytes]] = []

    async def send_and_wait(self, topic: str, *, key: bytes | None, value: bytes) -> None:
        self.published.append((topic, key, value))


class FakeSession:
    async def __aenter__(self) -> "FakeSession":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


def record(
    value: bytes, key: bytes, headers: list[tuple[str, bytes]] | None = None
) -> ConsumerRecord:
    return ConsumerRecord(
        topic=STEP_READY_TOPIC,
        partition=0,
        offset=12,
        timestamp=0,
        timestamp_type=0,
        key=key,
        value=value,
        checksum=None,
        serialized_key_size=len(key),
        serialized_value_size=len(value),
        headers=headers or [],
    )


@pytest.mark.asyncio
async def test_duplicate_event_counts_once_and_is_not_dead_lettered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = StepReadyEvent(
        event_id=uuid4(),
        event_type="step.ready",
        schema_version=1,
        occurred_at=datetime.now(UTC),
        workflow_id=uuid4(),
        step_id=uuid4(),
        correlation_id=uuid4(),
        causation_id=None,
    )
    observed: list[tuple[str, dict[str, str]]] = []

    async def duplicate(*_args: Any, **_kwargs: Any) -> str:
        return "duplicate"

    monkeypatch.setattr(worker, "process_step_ready", duplicate)
    monkeypatch.setattr(worker, "count", lambda name, **labels: observed.append((name, labels)))
    producer = FakeProducer()
    result = await worker.handle_record(
        record(event.to_bytes(), str(event.workflow_id).encode()),
        cast(AIOKafkaProducer, producer),
        worker_id="test-worker",
        consumer_group="test-group",
        sessions=cast(async_sessionmaker[AsyncSession], lambda: FakeSession()),
    )
    assert result == "duplicate"
    assert observed == [
        (
            "continuum_kafka_events_consumed",
            {"event_type": "step.ready", "result": "duplicate"},
        ),
        ("continuum_kafka_duplicates", {}),
    ]
    assert producer.published == []


@pytest.mark.asyncio
async def test_invalid_event_publishes_dlq_and_counts_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[tuple[str, dict[str, str]]] = []
    monkeypatch.setattr(worker, "count", lambda name, **labels: observed.append((name, labels)))
    producer = FakeProducer()
    result = await worker.handle_record(
        record(b"{invalid", b"workflow"),
        cast(AIOKafkaProducer, producer),
        worker_id="test-worker",
        consumer_group="test-group",
    )
    assert result == "dead_letter"
    assert len(producer.published) == 1
    assert producer.published[0][0] == "continuum.dead-letter.v1"
    assert observed == [("continuum_dlq_messages", {"reason": "invalid_event"})]


@pytest.mark.asyncio
async def test_worker_retries_processing_and_survives_offset_commit_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    message = record(b"payload", b"workflow")
    processed = 0

    class Consumer:
        async def getone(self) -> ConsumerRecord:
            return message

        async def commit(self, offsets: dict[TopicPartition, int]) -> None:
            assert offsets == {TopicPartition(STEP_READY_TOPIC, 0): 13}
            stop.set()
            raise RuntimeError("offset acknowledgement lost")

    async def process(*_args: Any, **_kwargs: Any) -> str:
        nonlocal processed
        processed += 1
        if processed == 1:
            raise RuntimeError("temporary database error")
        return "duplicate"

    monkeypatch.setattr(worker, "handle_record", process)
    await worker.consume_loop(
        stop,
        cast(AIOKafkaConsumer, Consumer()),
        cast(AIOKafkaProducer, FakeProducer()),
        worker_id="test-worker",
        consumer_group="test-group",
    )
    assert processed == 2
