"""Real Kafka and PostgreSQL tests on isolated per-test topics."""

import asyncio
import json
from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from sqlalchemy import func, select

from durable_agent_runtime.core.config import get_settings
from durable_agent_runtime.db.models import (
    ConsumedEvent,
    OutboxEvent,
    StateTransition,
    Workflow,
    WorkflowStep,
)
from durable_agent_runtime.dispatcher.main import dispatch_loop, dispatch_once
from durable_agent_runtime.domain.enums import StepStatus, WorkflowStatus
from durable_agent_runtime.events import StepReadyEvent
from durable_agent_runtime.schemas.workflows import WorkflowCreate
from durable_agent_runtime.services.workflows import WorkflowService
from durable_agent_runtime.worker.main import DeadLetterRecord, consume_loop, handle_record
from tests.conftest import TestSession

pytestmark = [pytest.mark.integration, pytest.mark.kafka]


@pytest_asyncio.fixture
async def topics() -> AsyncIterator[tuple[str, str]]:
    suffix = uuid4().hex[:12]
    ready = f"continuum.test.ready.{suffix}"
    dead = f"continuum.test.dead.{suffix}"
    admin = AIOKafkaAdminClient(bootstrap_servers=get_settings().kafka_bootstrap_servers)
    await admin.start()
    await admin.create_topics([NewTopic(ready, 3, 1), NewTopic(dead, 1, 1)])
    try:
        yield ready, dead
    finally:
        await admin.delete_topics([ready, dead])
        await admin.close()


@pytest_asyncio.fixture
async def producer() -> AsyncIterator[AIOKafkaProducer]:
    client = AIOKafkaProducer(
        bootstrap_servers=get_settings().kafka_bootstrap_servers,
        acks="all",
        enable_idempotence=True,
    )
    await client.start()
    try:
        yield client
    finally:
        await client.stop()


async def workflow(step_count: int, topic: str, monkeypatch: pytest.MonkeyPatch) -> UUID:
    monkeypatch.setattr("durable_agent_runtime.services.workflows.STEP_READY_TOPIC", topic)
    command = WorkflowCreate.model_validate(
        {
            "workflow_type": "kafka-test",
            "steps": [{"name": f"step-{i}", "step_type": "noop"} for i in range(step_count)],
        }
    )
    async with TestSession() as session:
        created = await WorkflowService(session).create_workflow(command)
        workflow_id = created.id
    async with TestSession() as session:
        await WorkflowService(session).start_workflow(workflow_id)
    return workflow_id


async def consumer(topic: str, group: str) -> AIOKafkaConsumer:
    client = AIOKafkaConsumer(
        topic,
        bootstrap_servers=get_settings().kafka_bootstrap_servers,
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await client.start()
    return client


async def event_rows(workflow_id: UUID) -> list[OutboxEvent]:
    async with TestSession() as session:
        return list(
            await session.scalars(
                select(OutboxEvent)
                .where(OutboxEvent.workflow_id == workflow_id)
                .order_by(OutboxEvent.created_at, OutboxEvent.id)
            )
        )


async def audit_count(workflow_id: UUID) -> int:
    async with TestSession() as session:
        return int(
            await session.scalar(
                select(func.count())
                .select_from(StateTransition)
                .where(StateTransition.workflow_id == workflow_id)
            )
            or 0
        )


async def test_dispatcher_publishes_acknowledged_event_with_stable_envelope(
    topics: tuple[str, str], producer: AIOKafkaProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    ready, _dead = topics
    workflow_id = await workflow(1, ready, monkeypatch)
    before = (await event_rows(workflow_id))[0]
    assert before.published_at is None
    assert await dispatch_once(TestSession, producer) == 1
    after = (await event_rows(workflow_id))[0]
    assert after.published_at is not None
    assert after.publish_attempts == 1
    client = await consumer(ready, f"test-{uuid4()}")
    try:
        record = await asyncio.wait_for(client.getone(), timeout=15)
        envelope = StepReadyEvent.from_bytes(record.value)
        assert record.key == str(workflow_id).encode("ascii")
        assert envelope.event_id == before.id
        assert envelope.event_type == "step.ready"
        assert envelope.schema_version == 1
        assert envelope.workflow_id == workflow_id
        assert envelope.step_id == before.step_id
    finally:
        await client.stop()


async def test_dispatcher_retries_same_event_after_publish_failure(
    topics: tuple[str, str], producer: AIOKafkaProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    ready, _dead = topics
    workflow_id = await workflow(1, ready, monkeypatch)
    original_id = (await event_rows(workflow_id))[0].id
    unavailable = AIOKafkaProducer(bootstrap_servers=get_settings().kafka_bootstrap_servers)
    # An unstarted real Kafka client fails its send; no fake broker acknowledgement is supplied.
    assert await dispatch_once(TestSession, unavailable) == 1
    after_failure = (await event_rows(workflow_id))[0]
    assert after_failure.id == original_id
    assert after_failure.published_at is None
    assert after_failure.publish_attempts == 1
    assert after_failure.last_error is not None
    assert await dispatch_once(TestSession, producer) == 1
    after_recovery = (await event_rows(workflow_id))[0]
    assert after_recovery.id == original_id
    assert after_recovery.published_at is not None
    assert after_recovery.publish_attempts == 2
    client = await consumer(ready, f"test-{uuid4()}")
    try:
        record = await asyncio.wait_for(client.getone(), timeout=15)
        assert StepReadyEvent.from_bytes(record.value).event_id == original_id
    finally:
        await client.stop()


async def test_duplicate_delivery_and_lost_offset_ack_are_harmless(
    topics: tuple[str, str], producer: AIOKafkaProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    ready, _dead = topics
    workflow_id = await workflow(2, ready, monkeypatch)
    first = (await event_rows(workflow_id))[0]
    await dispatch_once(TestSession, producer)
    group = f"test-{uuid4()}"
    client = await consumer(ready, group)
    try:
        original = await asyncio.wait_for(client.getone(), timeout=15)
        assert (
            await handle_record(
                original, producer, worker_id="worker-a", consumer_group=group, sessions=TestSession
            )
            == "processed"
        )
        # Deliberately omit the Kafka offset commit. Republish the exact envelope/event ID.
        await producer.send_and_wait(ready, key=original.key, value=original.value)
        duplicate = await asyncio.wait_for(client.getone(), timeout=15)
        assert duplicate.offset != original.offset
        assert (
            await handle_record(
                duplicate,
                producer,
                worker_id="worker-b",
                consumer_group=group,
                sessions=TestSession,
            )
            == "duplicate"
        )
        await client.commit(
            {TopicPartition(duplicate.topic, duplicate.partition): duplicate.offset + 1}
        )
        assert len(await event_rows(workflow_id)) == 2
        assert await audit_count(workflow_id) == 8
        async with TestSession() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(ConsumedEvent)
                    .where(ConsumedEvent.event_id == first.id)
                )
                == 1
            )
        await dispatch_once(TestSession, producer)
        next_record = await asyncio.wait_for(client.getone(), timeout=15)
        assert (
            await handle_record(
                next_record,
                producer,
                worker_id="worker-a",
                consumer_group=group,
                sessions=TestSession,
            )
            == "processed"
        )
        async with TestSession() as session:
            completed = await WorkflowService(session).get_workflow(workflow_id)
            assert completed.status == WorkflowStatus.SUCCEEDED
    finally:
        await client.stop()


async def test_stale_event_records_inbox_without_new_work(
    topics: tuple[str, str], producer: AIOKafkaProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    ready, _dead = topics
    workflow_id = await workflow(2, ready, monkeypatch)
    async with TestSession() as session:
        await WorkflowService(session).cancel_workflow(workflow_id)
    audit_before = await audit_count(workflow_id)
    await dispatch_once(TestSession, producer)
    group = f"test-{uuid4()}"
    client = await consumer(ready, group)
    try:
        record = await asyncio.wait_for(client.getone(), timeout=15)
        assert (
            await handle_record(
                record, producer, worker_id="worker-a", consumer_group=group, sessions=TestSession
            )
            == "stale"
        )
        assert await audit_count(workflow_id) == audit_before
        assert len(await event_rows(workflow_id)) == 1
    finally:
        await client.stop()


@pytest.mark.parametrize("invalid_version", [False, True])
async def test_malformed_message_reaches_dead_letter_topic(
    topics: tuple[str, str],
    producer: AIOKafkaProducer,
    monkeypatch: pytest.MonkeyPatch,
    invalid_version: bool,
) -> None:
    ready, dead = topics
    monkeypatch.setattr("durable_agent_runtime.worker.main.DEAD_LETTER_TOPIC", dead)
    dlq_consumer = await consumer(dead, f"dlq-{uuid4()}")
    input_consumer = await consumer(ready, f"input-{uuid4()}")
    try:
        if invalid_version:
            workflow_id = uuid4()
            payload = {
                "event_id": str(uuid4()),
                "event_type": "step.ready",
                "schema_version": 2,
                "occurred_at": "2026-09-22T10:00:00Z",
                "workflow_id": str(workflow_id),
                "step_id": str(uuid4()),
                "correlation_id": str(workflow_id),
                "causation_id": None,
                "payload": {},
            }
            raw = json.dumps(payload).encode()
        else:
            raw = b"not-json"
        await producer.send_and_wait(ready, key=b"broken", value=raw)
        record = await asyncio.wait_for(input_consumer.getone(), timeout=15)
        assert (
            await handle_record(
                record,
                producer,
                worker_id="worker-a",
                consumer_group="test-workers",
                sessions=TestSession,
            )
            == "dead_letter"
        )
        dlq_record = await asyncio.wait_for(dlq_consumer.getone(), timeout=15)
        dead_letter = DeadLetterRecord.model_validate_json(dlq_record.value)
        assert dead_letter.original_topic == ready
        assert dead_letter.partition == record.partition
        assert dead_letter.offset == record.offset
        assert dead_letter.error_type == "ValidationError"
        assert dead_letter.payload_bytes == len(record.value)
    finally:
        await input_consumer.stop()
        await dlq_consumer.stop()


async def test_three_workers_complete_twenty_sequential_workflows(
    topics: tuple[str, str], producer: AIOKafkaProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    ready, _dead = topics
    group = f"test-workers-{uuid4()}"
    clients = [await consumer(ready, group) for _ in range(3)]
    stop = asyncio.Event()

    tasks: list[asyncio.Task[None]] = []
    try:
        # getmany drives group assignment before the workload begins.
        async with asyncio.timeout(20):
            while sum(bool(client.assignment()) for client in clients) < 3:
                await asyncio.gather(*(client.getmany(timeout_ms=200) for client in clients))
        ids = [await workflow(5, ready, monkeypatch) for _ in range(20)]
        tasks = [
            asyncio.create_task(
                consume_loop(
                    stop,
                    client,
                    producer,
                    worker_id=f"worker-{i}",
                    consumer_group=group,
                    sessions=TestSession,
                )
            )
            for i, client in enumerate(clients)
        ]
        tasks.append(
            asyncio.create_task(dispatch_loop(stop, TestSession, producer, poll_interval=0.05))
        )
        async with asyncio.timeout(90):
            while True:
                for task in tasks:
                    if task.done():
                        task.result()
                async with TestSession() as session:
                    completed = await session.scalar(
                        select(func.count())
                        .select_from(Workflow)
                        .where(Workflow.id.in_(ids), Workflow.status == WorkflowStatus.SUCCEEDED)
                    )
                if completed == 20:
                    break
                await asyncio.sleep(0.1)
        async with TestSession() as session:
            steps = list(
                await session.scalars(select(WorkflowStep).where(WorkflowStep.workflow_id.in_(ids)))
            )
            assert len(steps) == 100
            assert all(step.status == StepStatus.SUCCEEDED for step in steps)
            workers = set(
                await session.scalars(
                    select(ConsumedEvent.worker_id).where(ConsumedEvent.workflow_id.in_(ids))
                )
            )
            assert len(workers) >= 2
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(OutboxEvent)
                    .where(OutboxEvent.workflow_id.in_(ids))
                )
                == 100
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(ConsumedEvent)
                    .where(ConsumedEvent.workflow_id.in_(ids))
                )
                == 100
            )
            assert await session.scalar(
                select(func.count())
                .select_from(StateTransition)
                .where(StateTransition.workflow_id.in_(ids))
            ) == 20 * (1 + 5 + 2 + 5 * 2 + 4 + 1)
    finally:
        stop.set()
        try:
            if tasks:
                await asyncio.gather(*tasks)
        finally:
            await asyncio.gather(*(client.stop() for client in clients))
