"""Fault scenarios with explicit injection points and durable assertions."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from aiokafka import AIOKafkaConsumer, TopicPartition
from sqlalchemy import func, select, update

from durable_agent_runtime.db.models import ConsumedEvent, ExecutionAttempt
from durable_agent_runtime.events import DEAD_LETTER_TOPIC, STEP_READY_TOPIC, StepReadyEvent
from durable_agent_runtime.faultlab import agent_scenarios, coding_scenarios
from durable_agent_runtime.faultlab.assertions import classify_workflow
from durable_agent_runtime.faultlab.models import TrialResult
from durable_agent_runtime.faultlab.runtime import FaultLabRuntime, wait_for
from durable_agent_runtime.services.execution import ExecutionService, LostLease
from durable_agent_runtime.worker.main import DeadLetterRecord, handle_record


@dataclass(frozen=True)
class Scenario:
    name: str
    description: str
    run: Callable[[FaultLabRuntime, TrialResult], Awaitable[None]]
    exclusive: bool = False
    version: int = 1


def inject(trial: TrialResult, fault_type: str, point: str) -> None:
    trial.fault_type = fault_type
    trial.fault_injection_point = point
    trial.fault_injected_at = datetime.now(UTC)
    trial.injected_failure_count += 1
    trial.recovery_required = True


def step_id(created: dict[str, Any]) -> UUID:
    return UUID(created["steps"][0]["id"])


async def finish(
    runtime: FaultLabRuntime,
    trial: TrialResult,
    *,
    expected_attempts: int,
    expected_refunds: int = 0,
    customer_id: str | None = None,
    expected_attempt_statuses: list[str] | None = None,
) -> None:
    assert trial.workflow_id is not None
    await runtime.wait_terminal(trial.workflow_id, wait_timeout=60)
    await classify_workflow(
        runtime,
        trial,
        expected_attempts=expected_attempts,
        expected_refunds=expected_refunds,
        customer_id=customer_id,
        expected_outbox=1,
        expected_attempt_statuses=expected_attempt_statuses,
    )


async def baseline(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    created = await runtime.create_start(
        [{"name": "noop", "step_type": "noop"}], label=str(trial.trial_id)
    )
    trial.workflow_id = UUID(created["id"])
    trial.step_id = step_id(created)
    await finish(runtime, trial, expected_attempts=1, expected_attempt_statuses=["SUCCEEDED"])


async def crash_during_pure(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    created = await runtime.create_start(
        [{"name": "slow", "step_type": "slow_noop", "input": {"duration_ms": 5000}}],
        label=str(trial.trial_id),
    )
    trial.workflow_id = UUID(created["id"])
    trial.step_id = step_id(created)
    first = await runtime.wait_attempt(trial.workflow_id)
    await runtime.assert_active_lease(UUID(first["id"]))
    inject(trial, "SIGKILL", "executor running pure work")
    killed = await runtime.docker.kill_owner(first["executor_id"])
    try:
        await finish(
            runtime,
            trial,
            expected_attempts=2,
            expected_attempt_statuses=["EXPIRED", "SUCCEEDED"],
        )
    finally:
        await runtime.docker.restart_container(killed)


async def crash_before_execution(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await runtime.docker.compose("stop", "executor")
    oneoff = ""
    try:
        oneoff = await runtime.docker.run_oneoff(
            "executor", variables={"FAULTLAB_PAUSE_AFTER_CLAIM": "1"}
        )
        created = await runtime.create_start(
            [{"name": "noop", "step_type": "noop"}], label=str(trial.trial_id)
        )
        trial.workflow_id = UUID(created["id"])
        trial.step_id = step_id(created)
        first = await runtime.wait_attempt(trial.workflow_id)
        await runtime.assert_active_lease(UUID(first["id"]))
        inject(trial, "SIGKILL", "after durable claim, before tool invocation")
        await runtime.docker.command("docker", "kill", "--signal=KILL", oneoff)
    finally:
        if oneoff:
            await runtime.docker.remove_oneoff(oneoff)
        await runtime.docker.compose("start", "executor")
    await finish(
        runtime, trial, expected_attempts=2, expected_attempt_statuses=["EXPIRED", "SUCCEEDED"]
    )


async def crash_after_side_effect(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    customer_id = f"faultlab-{trial.trial_id}"
    created = await runtime.create_start(
        [
            {
                "name": "refund",
                "step_type": "mock_refund",
                "input": {
                    "customer_id": customer_id,
                    "amount": "49.99",
                    "delay_after_commit_ms": 15000,
                },
            }
        ],
        label=str(trial.trial_id),
    )
    trial.workflow_id = UUID(created["id"])
    trial.step_id = step_id(created)
    first = await runtime.wait_attempt(trial.workflow_id)
    refund = await runtime.wait_refund(f"continuum:{trial.step_id}")
    await runtime.assert_active_lease(UUID(first["id"]))
    trial.notes["refund_committed_before_kill"] = refund["refund_id"]
    inject(trial, "SIGKILL", "after independent refund commit, before finalization")
    killed = await runtime.docker.kill_owner(first["executor_id"])
    try:
        await finish(
            runtime,
            trial,
            expected_attempts=2,
            expected_refunds=1,
            customer_id=customer_id,
            expected_attempt_statuses=["EXPIRED", "SUCCEEDED"],
        )
    finally:
        await runtime.docker.restart_container(killed)


async def missed_heartbeats(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await runtime.docker.compose("stop", "executor")
    oneoff = ""
    paused = False
    try:
        oneoff = await runtime.docker.run_oneoff("executor", variables={})
        created = await runtime.create_start(
            [{"name": "slow", "step_type": "slow_noop", "input": {"duration_ms": 5000}}],
            label=str(trial.trial_id),
        )
        trial.workflow_id = UUID(created["id"])
        trial.step_id = step_id(created)
        first = await runtime.wait_attempt(trial.workflow_id)
        await runtime.assert_active_lease(UUID(first["id"]))
        await runtime.docker.command("docker", "pause", oneoff)
        paused = True
        inject(trial, "PROCESS_PAUSED", "executor alive but cannot heartbeat")
        await runtime.docker.compose("start", "executor")
        await finish(
            runtime,
            trial,
            expected_attempts=2,
            expected_attempt_statuses=["EXPIRED", "SUCCEEDED"],
        )
    finally:
        if oneoff:
            if paused:
                await runtime.docker.command("docker", "unpause", oneoff)
            await runtime.docker.remove_oneoff(oneoff)
        await runtime.docker.compose("start", "executor")


async def response_lost(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    customer_id = f"faultlab-{trial.trial_id}"
    created = await runtime.create_start(
        [
            {
                "name": "refund",
                "step_type": "mock_refund",
                "input": {
                    "customer_id": customer_id,
                    "amount": "49.99",
                    "faultlab_mode": "error_after_commit",
                },
            }
        ],
        label=str(trial.trial_id),
    )
    trial.workflow_id = UUID(created["id"])
    trial.step_id = step_id(created)
    await runtime.wait_refund(f"continuum:{trial.step_id}")
    inject(trial, "HTTP_503_AFTER_COMMIT", "refund committed; first response unavailable")
    await finish(
        runtime,
        trial,
        expected_attempts=2,
        expected_refunds=1,
        customer_id=customer_id,
        expected_attempt_statuses=["EXPIRED", "SUCCEEDED"],
    )


async def timeout_before_effect(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    customer_id = f"faultlab-{trial.trial_id}"
    created = await runtime.create_start(
        [
            {
                "name": "refund",
                "step_type": "mock_refund",
                "input": {
                    "customer_id": customer_id,
                    "amount": "49.99",
                    "faultlab_mode": "timeout_before_commit",
                    "delay_before_commit_ms": 1000,
                    "faultlab_request_timeout_ms": 150,
                },
            }
        ],
        label=str(trial.trial_id),
    )
    trial.workflow_id = UUID(created["id"])
    trial.step_id = step_id(created)
    trial.expected_terminal_status = "FAILED"
    await runtime.wait_attempt(trial.workflow_id)
    inject(trial, "HTTP_READ_TIMEOUT", "every refund request times out before side effect")
    # Exhausting bounded attempts with no effect is the expected safe failure,
    # not a recovery-to-success trial.
    trial.recovery_required = False
    await finish(
        runtime,
        trial,
        expected_attempts=3,
        expected_refunds=0,
        customer_id=customer_id,
        expected_attempt_statuses=["EXPIRED", "EXPIRED", "EXPIRED"],
    )


async def unsafe_refund_retry(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    """Intentionally unsafe harness-only baseline; no Continuum runtime mode changes."""
    customer_id = f"unsafe-{trial.trial_id}"
    body = {"customer_id": customer_id, "amount": "49.99"}
    first_key = f"unsafe:{trial.trial_id}:1"
    first_request = asyncio.create_task(
        runtime.payments.post(
            "/refunds",
            headers={"Idempotency-Key": first_key},
            json={**body, "delay_after_commit_ms": 15000},
            timeout=20,
        )
    )
    try:
        first = await runtime.wait_refund(first_key)
        assert not first_request.done(), "Unsafe control missed the post-commit response window"
        inject(trial, "CLIENT_RESPONSE_LOST", "refund committed; client request cancelled")
    finally:
        first_request.cancel()
        await asyncio.gather(first_request, return_exceptions=True)
    second = await runtime.payments.post(
        "/refunds",
        headers={"Idempotency-Key": f"unsafe:{trial.trial_id}:2"},
        json=body,
    )
    second.raise_for_status()
    refund_ids = [first["refund_id"], second.json()["refund_id"]]
    count = await runtime.refund_count(customer_id)
    trial.expected_side_effect_count = 1
    trial.duplicate_side_effect_count = max(0, count - 1)
    trial.attempt_count = 2
    trial.recovery_required = False
    trial.notes["baseline"] = "TEST BASELINE - INTENTIONALLY UNSAFE new key per retry"
    trial.notes["refund_count"] = count
    trial.notes["refund_ids"] = refund_ids
    trial.correct = count == 2 and len(set(refund_ids)) == 2
    if not trial.correct:
        trial.failure_reason = f"unsafe baseline produced {count} refunds, expected 2"


async def _manual_recovery_setup(
    runtime: FaultLabRuntime, trial: TrialResult
) -> tuple[UUID, str, UUID]:
    created = await runtime.create_start(
        [{"name": "noop", "step_type": "noop"}], label=str(trial.trial_id)
    )
    trial.workflow_id = UUID(created["id"])
    trial.step_id = step_id(created)

    async def pending() -> dict[str, Any] | None:
        assert trial.workflow_id is not None
        return next(
            (
                item
                for item in await runtime.attempts(trial.workflow_id)
                if item["status"] == "PENDING"
            ),
            None,
        )

    await wait_for(pending)
    async with runtime.sessions() as session:
        claimed = await ExecutionService(session).claim_next(
            executor_id=f"faultlab-owner-{trial.trial_id}", lease_seconds=2
        )
    assert claimed is not None and claimed.lease_token is not None
    inject(trial, "LEASE_EXPIRED", "database-time lease expiry before finalization")
    async with runtime.sessions() as session, session.begin():
        await session.execute(
            update(ExecutionAttempt)
            .where(ExecutionAttempt.id == claimed.id)
            .values(lease_expires_at=func.clock_timestamp() - timedelta(seconds=1))
        )
    return claimed.id, claimed.executor_id or "", claimed.lease_token


async def stale_owner_finalize(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await runtime.docker.compose("stop", "executor")
    await runtime.docker.compose("stop", "recovery-scheduler")
    try:
        old_id, owner, token = await _manual_recovery_setup(runtime, trial)
        async with runtime.sessions() as session:
            assert await ExecutionService(session).recover_expired() == 1
        assert trial.workflow_id is not None
        replacement = await runtime.wait_attempt(trial.workflow_id, status="PENDING", number=2)
        assert replacement["attempt_number"] == 2
        try:
            async with runtime.sessions() as session:
                await ExecutionService(session).finalize_success(
                    old_id, executor_id=owner, lease_token=token, output={}
                )
        except LostLease:
            trial.notes["stale_finalize_rejected"] = True
        else:
            raise AssertionError("Stale lease finalized after replacement")
        async with runtime.sessions() as session:
            claimed = await ExecutionService(session).claim_next(
                executor_id=f"faultlab-new-{trial.trial_id}", lease_seconds=2
            )
        assert claimed is not None and claimed.lease_token is not None
        async with runtime.sessions() as session:
            await ExecutionService(session).finalize_success(
                claimed.id,
                executor_id=claimed.executor_id or "",
                lease_token=claimed.lease_token,
                output={},
            )
        await finish(
            runtime,
            trial,
            expected_attempts=2,
            expected_attempt_statuses=["EXPIRED", "SUCCEEDED"],
        )
    finally:
        await runtime.docker.compose("start", "executor")
        await runtime.docker.compose("start", "recovery-scheduler")


async def concurrent_recovery_race(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await runtime.docker.compose("stop", "executor")
    await runtime.docker.compose("stop", "recovery-scheduler")
    try:
        await _manual_recovery_setup(runtime, trial)

        async def scan() -> int:
            async with runtime.sessions() as session:
                return await ExecutionService(session).recover_expired()

        counts = await asyncio.gather(scan(), scan())
        assert sorted(counts) == [0, 1], f"competing recovery results: {counts}"
        assert trial.workflow_id is not None
        records = await runtime.attempts(trial.workflow_id)
        assert len(records) == 2
        assert [record["attempt_number"] for record in records] == [1, 2]
        async with runtime.sessions() as session:
            claimed = await ExecutionService(session).claim_next(
                executor_id=f"faultlab-recovery-{trial.trial_id}", lease_seconds=2
            )
        assert claimed is not None and claimed.lease_token is not None
        async with runtime.sessions() as session:
            await ExecutionService(session).finalize_success(
                claimed.id,
                executor_id=claimed.executor_id or "",
                lease_token=claimed.lease_token,
                output={},
            )
        trial.notes["concurrent_scan_results"] = counts
        await finish(
            runtime,
            trial,
            expected_attempts=2,
            expected_attempt_statuses=["EXPIRED", "SUCCEEDED"],
        )
    finally:
        await runtime.docker.compose("start", "executor")
        await runtime.docker.compose("start", "recovery-scheduler")


async def duplicate_kafka_delivery(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    created = await runtime.create_start(
        [{"name": "noop", "step_type": "noop"}], label=str(trial.trial_id)
    )
    trial.workflow_id = UUID(created["id"])
    trial.step_id = step_id(created)
    await runtime.wait_terminal(trial.workflow_id)
    event = (await runtime.outbox(trial.workflow_id))[0]
    envelope = StepReadyEvent.from_outbox(event)
    inject(trial, "KAFKA_REPUBLISH", "same event_id after original durable processing")
    metadata = await runtime.producer.send_and_wait(
        STEP_READY_TOPIC, key=str(trial.workflow_id).encode("ascii"), value=envelope.to_bytes()
    )
    await runtime.wait_committed_offset(STEP_READY_TOPIC, metadata.partition, metadata.offset)
    async with runtime.sessions() as session:
        count = await session.scalar(
            select(func.count())
            .select_from(ConsumedEvent)
            .where(
                ConsumedEvent.consumer_group == "continuum-workers-v1",
                ConsumedEvent.event_id == event.id,
            )
        )
    assert count == 1
    trial.kafka_redelivery_count = 1
    await classify_workflow(runtime, trial, expected_attempts=1, expected_outbox=1)


async def _next_event(
    consumer: AIOKafkaConsumer, event_id: UUID, *, wait_timeout: float = 90
) -> Any:
    async def probe() -> Any | None:
        try:
            record = await asyncio.wait_for(consumer.getone(), timeout=1)
        except TimeoutError:
            return None
        try:
            current = StepReadyEvent.from_bytes(record.value)
        except Exception:
            return None
        return record if current.event_id == event_id else None

    return await wait_for(probe, wait_timeout=wait_timeout)


async def lost_kafka_offset_ack(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await runtime.docker.compose("stop", "worker")
    consumer: AIOKafkaConsumer | None = None
    try:
        created = await runtime.create_start(
            [{"name": "noop", "step_type": "noop"}], label=str(trial.trial_id)
        )
        trial.workflow_id = UUID(created["id"])
        trial.step_id = step_id(created)
        event = (await runtime.outbox(trial.workflow_id))[0]
        consumer = AIOKafkaConsumer(
            STEP_READY_TOPIC,
            bootstrap_servers="127.0.0.1:19093",
            group_id="continuum-workers-v1",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await consumer.start()
        original = await _next_event(consumer, event.id)
        assert (
            await handle_record(
                original,
                runtime.producer,
                worker_id="faultlab-first",
                consumer_group="continuum-workers-v1",
                sessions=runtime.sessions,
            )
            == "scheduled"
        )
        inject(
            trial, "OFFSET_ACK_OMITTED", "PostgreSQL commit complete, Kafka offset not committed"
        )
        await consumer.stop()
        consumer = AIOKafkaConsumer(
            STEP_READY_TOPIC,
            bootstrap_servers="127.0.0.1:19093",
            group_id="continuum-workers-v1",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await consumer.start()
        redelivered = await _next_event(consumer, event.id)
        assert redelivered.offset == original.offset
        assert (
            await handle_record(
                redelivered,
                runtime.producer,
                worker_id="faultlab-second",
                consumer_group="continuum-workers-v1",
                sessions=runtime.sessions,
            )
            == "duplicate"
        )
        await consumer.commit(
            {TopicPartition(redelivered.topic, redelivered.partition): redelivered.offset + 1}
        )
        trial.kafka_redelivery_count = 1
    finally:
        if consumer is not None:
            await consumer.stop()
        await runtime.docker.compose("start", "worker")
    await finish(runtime, trial, expected_attempts=1)


async def kafka_outage(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await runtime.docker.compose("stop", "dispatcher")
    try:
        created = await runtime.create_start(
            [{"name": "noop", "step_type": "noop"}], label=str(trial.trial_id)
        )
        trial.workflow_id = UUID(created["id"])
        trial.step_id = step_id(created)
        rows = await runtime.outbox(trial.workflow_id)
        assert len(rows) == 1 and rows[0].published_at is None
        trial.notes["durable_unpublished_event_id"] = str(rows[0].id)
        await runtime.docker.compose("stop", "kafka")
        inject(trial, "KAFKA_STOP", "broker stopped after durable outbox commit")
        # Start the existing dispatcher container directly; Compose dependency
        # readiness would otherwise refuse to start it with Kafka stopped.
        dispatcher = (await runtime.docker.service_containers("dispatcher", include_stopped=True))[
            0
        ]
        await runtime.docker.restart_container(dispatcher)
        rows = await runtime.outbox(trial.workflow_id)
        assert rows[0].published_at is None
    finally:
        await runtime.docker.compose("start", "kafka")
        await runtime.docker.compose("up", "-d", "--wait", "kafka")
        await runtime.docker.compose("start", "dispatcher")
    await finish(runtime, trial, expected_attempts=1)


async def dispatcher_crash_after_ack(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await runtime.docker.compose("stop", "dispatcher")
    oneoff = ""
    observer = AIOKafkaConsumer(
        STEP_READY_TOPIC,
        bootstrap_servers="127.0.0.1:19093",
        group_id=f"faultlab-observer-{trial.trial_id}",
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )
    await observer.start()
    try:

        async def assigned() -> bool | None:
            return True if observer.assignment() else None

        await wait_for(assigned)
        oneoff = await runtime.docker.run_oneoff(
            "dispatcher", variables={"FAULTLAB_DISPATCHER_PAUSE_AFTER_ACK": "1"}
        )
        created = await runtime.create_start(
            [{"name": "noop", "step_type": "noop"}], label=str(trial.trial_id)
        )
        trial.workflow_id = UUID(created["id"])
        trial.step_id = step_id(created)
        original = (await runtime.outbox(trial.workflow_id))[0]
        first = await _next_event(observer, original.id)
        assert StepReadyEvent.from_bytes(first.value).event_id == original.id
        rows = await runtime.outbox(trial.workflow_id)
        assert rows[0].published_at is None
        inject(trial, "DISPATCHER_SIGKILL", "Kafka acknowledged; published_at not committed")
        await runtime.docker.command("docker", "kill", "--signal=KILL", oneoff)
        await runtime.docker.remove_oneoff(oneoff)
        oneoff = ""
        await runtime.docker.compose("start", "dispatcher")
        second = await _next_event(observer, original.id)
        assert second.offset != first.offset
        trial.kafka_redelivery_count = 1
        trial.notes["same_event_id_after_republish"] = str(original.id)
    finally:
        await observer.stop()
        if oneoff:
            await runtime.docker.remove_oneoff(oneoff)
        await runtime.docker.compose("start", "dispatcher")
    await finish(runtime, trial, expected_attempts=1)


async def malformed_kafka_event(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    observer = AIOKafkaConsumer(
        DEAD_LETTER_TOPIC,
        bootstrap_servers="127.0.0.1:19093",
        group_id=f"faultlab-dlq-{trial.trial_id}",
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )
    await observer.start()
    try:

        async def assigned() -> bool | None:
            return True if observer.assignment() else None

        await wait_for(assigned)
        # Assignment can precede the latest-offset reset. Resolve the position
        # before publishing so this observer cannot skip the injected record.
        for partition in observer.assignment():
            await observer.position(partition)
        inject(trial, "MALFORMED_JSON", "invalid event envelope on step-ready topic")
        injected = await runtime.producer.send_and_wait(
            STEP_READY_TOPIC, key=str(trial.trial_id).encode("ascii"), value=b"{not-json"
        )

        async def dlq() -> Any | None:
            try:
                record = await asyncio.wait_for(observer.getone(), timeout=1)
            except TimeoutError:
                return None
            dead = DeadLetterRecord.model_validate_json(record.value)
            return (
                record
                if dead.original_topic == STEP_READY_TOPIC
                and dead.partition == injected.partition
                and dead.offset == injected.offset
                and dead.error_type == "ValidationError"
                else None
            )

        dead = await wait_for(dlq)
        trial.notes["dlq_offset"] = dead.offset
    finally:
        await observer.stop()
    created = await runtime.create_start(
        [{"name": "noop", "step_type": "noop"}], label=str(trial.trial_id)
    )
    trial.workflow_id = UUID(created["id"])
    trial.step_id = step_id(created)
    await finish(runtime, trial, expected_attempts=1)


async def executor_restart(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    created = await runtime.create_start(
        [{"name": "slow", "step_type": "slow_noop", "input": {"duration_ms": 5000}}],
        label=str(trial.trial_id),
    )
    trial.workflow_id = UUID(created["id"])
    trial.step_id = step_id(created)
    first = await runtime.wait_attempt(trial.workflow_id)
    container = await runtime.docker.owner_container(first["executor_id"])
    inject(trial, "EXECUTOR_RESTART", "in-flight pure execution")
    trial.recovery_required = False  # SIGTERM drains in-flight work while heartbeating.
    await runtime.docker.command("docker", "restart", container)
    trial.notes["restart_mode"] = "graceful SIGTERM drain"
    await finish(runtime, trial, expected_attempts=1)


async def database_interruption(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    created = await runtime.create_start(
        [{"name": "slow", "step_type": "slow_noop", "input": {"duration_ms": 7000}}],
        label=str(trial.trial_id),
    )
    trial.workflow_id = UUID(created["id"])
    trial.step_id = step_id(created)
    await runtime.wait_attempt(trial.workflow_id)
    inject(trial, "POSTGRES_STOP", "active execution, before finalization")
    await runtime.docker.compose("stop", "postgres")
    try:
        # This is the controlled outage duration, not a synchronization guess.
        await asyncio.sleep(6.5)
    finally:
        await runtime.docker.compose("start", "postgres")
        await runtime.docker.compose("up", "-d", "--wait", "postgres")
    await finish(
        runtime, trial, expected_attempts=2, expected_attempt_statuses=["EXPIRED", "SUCCEEDED"]
    )


SCENARIOS: dict[str, Scenario] = {
    item.name: item
    for item in (
        Scenario("baseline", "No fault; one durable noop step", baseline),
        Scenario(
            "executor-crash-before-execution",
            "SIGKILL after durable claim and before invoking a tool",
            crash_before_execution,
            exclusive=True,
        ),
        Scenario(
            "executor-crash-during-pure-work",
            "SIGKILL during slow_noop",
            crash_during_pure,
            exclusive=True,
        ),
        Scenario(
            "executor-crash-after-side-effect",
            "SIGKILL after independent refund commit",
            crash_after_side_effect,
            exclusive=True,
        ),
        Scenario(
            "missed-heartbeats",
            "Keep executor alive while suppressing heartbeats",
            missed_heartbeats,
            exclusive=True,
        ),
        Scenario(
            "stale-owner-finalize",
            "Old lease token attempts finalization after replacement",
            stale_owner_finalize,
            exclusive=True,
        ),
        Scenario(
            "duplicate-kafka-delivery",
            "Republish the exact application event ID",
            duplicate_kafka_delivery,
        ),
        Scenario(
            "lost-kafka-offset-ack",
            "Commit PostgreSQL, omit Kafka offset, consume again",
            lost_kafka_offset_ack,
            exclusive=True,
        ),
        Scenario("kafka-outage", "Stop the real single Kafka broker", kafka_outage, exclusive=True),
        Scenario(
            "dispatcher-crash-after-kafka-ack",
            "Kill dispatcher after broker ack and before published_at",
            dispatcher_crash_after_ack,
            exclusive=True,
        ),
        Scenario(
            "malformed-kafka-event",
            "Poison message reaches DLQ; healthy work continues",
            malformed_kafka_event,
            exclusive=True,
        ),
        Scenario(
            "external-service-timeout-before-side-effect",
            "Real HTTP read timeout before refund commit; bounded attempts exhaust",
            timeout_before_effect,
        ),
        Scenario(
            "external-response-lost-after-side-effect",
            "Test-only 503 after refund commit, retry uses same key",
            response_lost,
        ),
        Scenario(
            "concurrent-recovery-race",
            "Two real PostgreSQL sessions recover the same lease",
            concurrent_recovery_race,
            exclusive=True,
        ),
        Scenario(
            "executor-restart",
            "Restart in-flight executor container",
            executor_restart,
            exclusive=True,
        ),
        Scenario(
            "database-interruption",
            "Stop Continuum PostgreSQL during execution; retain volumes",
            database_interruption,
            exclusive=True,
        ),
        Scenario(
            "unsafe-refund-retry-baseline",
            "Intentionally unsafe harness uses a new key on retry",
            unsafe_refund_retry,
        ),
        Scenario(
            "agent-model-timeout",
            "Failed model call is recorded before retry",
            agent_scenarios.model_timeout,
        ),
        Scenario(
            "agent-malformed-output",
            "Malformed decision cannot create a tool call",
            agent_scenarios.malformed_output,
        ),
        Scenario(
            "agent-crash-after-decision",
            "SIGKILL after durable refund decision",
            agent_scenarios.crash_after_decision,
            exclusive=True,
        ),
        Scenario(
            "agent-crash-after-side-effect",
            "SIGKILL after refund commit before tool checkpoint",
            agent_scenarios.crash_after_side_effect,
            exclusive=True,
        ),
        Scenario(
            "agent-crash-after-tool-result",
            "SIGKILL after durable tool result",
            agent_scenarios.crash_after_tool_result,
            exclusive=True,
        ),
        Scenario(
            "agent-crash-after-final",
            "SIGKILL after durable final answer",
            agent_scenarios.crash_after_final,
            exclusive=True,
        ),
        Scenario(
            "agent-max-turn-limit",
            "Bounded model/tool loop fails safely",
            agent_scenarios.max_turns,
        ),
        Scenario(
            "agent-unknown-tool",
            "Unknown model-selected tool fails closed",
            agent_scenarios.unknown_tool,
        ),
        Scenario(
            "coding-baseline",
            "Fixture patch, tests, approval, local commit",
            coding_scenarios.baseline,
            exclusive=True,
        ),
        Scenario(
            "coding-crash-after-decision",
            "Executor SIGKILL after durable coding decision",
            coding_scenarios.crash_after_decision,
            exclusive=True,
        ),
        Scenario(
            "coding-crash-after-patch",
            "Executor SIGKILL after patch before checkpoint",
            coding_scenarios.crash_after_patch,
            exclusive=True,
        ),
        Scenario(
            "coding-crash-after-tests",
            "Executor SIGKILL after tests before result",
            coding_scenarios.crash_after_tests,
            exclusive=True,
        ),
        Scenario(
            "coding-sandbox-killed",
            "Sandbox SIGKILL; workspace volume survives",
            coding_scenarios.sandbox_killed,
            exclusive=True,
        ),
        Scenario(
            "coding-executor-killed",
            "Executor SIGKILL mid-coding",
            coding_scenarios.executor_killed,
            exclusive=True,
        ),
        Scenario(
            "coding-crash-before-commit",
            "Executor SIGKILL after approval before commit",
            coding_scenarios.crash_before_commit,
            exclusive=True,
        ),
        Scenario(
            "coding-crash-after-commit",
            "Executor SIGKILL after Git commit before DB result",
            coding_scenarios.crash_after_commit,
            exclusive=True,
        ),
        Scenario(
            "coding-workspace-divergence",
            "Unexpected workspace edit is detected after executor replacement",
            coding_scenarios.workspace_divergence,
            exclusive=True,
        ),
        Scenario(
            "coding-path-traversal",
            "Structured file tool rejects traversal out of workspace",
            coding_scenarios.path_traversal,
            exclusive=True,
        ),
        Scenario(
            "coding-command-timeout",
            "Sandbox terminates a bounded test command",
            coding_scenarios.command_timeout,
            exclusive=True,
        ),
    )
}


CAMPAIGNS: dict[str, dict[str, int]] = {
    "smoke": {
        name: 1
        for name in SCENARIOS
        if name != "unsafe-refund-retry-baseline"
        and not name.startswith("agent-")
        and not name.startswith("coding-")
    },
    "ai-smoke": {name: 1 for name in SCENARIOS if name.startswith("agent-")},
    "coding-smoke": {name: 1 for name in SCENARIOS if name.startswith("coding-")},
    "side-effects": {
        "executor-crash-after-side-effect": 5,
        "external-response-lost-after-side-effect": 20,
        "unsafe-refund-retry-baseline": 20,
    },
    "reliability": {
        "baseline": 2200,
        "duplicate-kafka-delivery": 2200,
        "external-response-lost-after-side-effect": 500,
        "external-service-timeout-before-side-effect": 100,
        "executor-crash-before-execution": 5,
        "executor-crash-during-pure-work": 5,
        "executor-crash-after-side-effect": 10,
        "missed-heartbeats": 5,
        "stale-owner-finalize": 10,
        "lost-kafka-offset-ack": 10,
        "dispatcher-crash-after-kafka-ack": 5,
        "concurrent-recovery-race": 10,
        "kafka-outage": 3,
        "database-interruption": 2,
        "executor-restart": 5,
        "malformed-kafka-event": 5,
        "unsafe-refund-retry-baseline": 100,
    },
}
