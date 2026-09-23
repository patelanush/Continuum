"""Lease expiry and recovery use PostgreSQL time and durable locking."""

import asyncio
import os
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import text, update

from durable_agent_runtime.core.config import Settings
from durable_agent_runtime.db.models import ExecutionAttempt
from durable_agent_runtime.domain.enums import ExecutionAttemptStatus, StepStatus, WorkflowStatus
from durable_agent_runtime.execution.executor import execute_attempt, executor_loop
from durable_agent_runtime.execution.recovery import recovery_loop
from durable_agent_runtime.execution.tools import ExecutionContext, execute_tool, operation_id
from durable_agent_runtime.services.execution import ExecutionService, LostLease
from durable_agent_runtime.services.workflows import WorkflowService
from tests.conftest import TestSession
from tests.integration.test_execution import attempts_for, claim, create_scheduled

pytestmark = pytest.mark.integration


async def force_expiry(attempt_id: object) -> None:
    async with TestSession() as session, session.begin():
        await session.execute(
            update(ExecutionAttempt)
            .where(ExecutionAttempt.id == attempt_id)
            .values(lease_expires_at=text("clock_timestamp() - interval '1 second'"))
        )


async def test_expired_attempt_creates_one_replacement_and_fences_old_owner() -> None:
    workflow_id, _ = await create_scheduled(step_count=1)
    first = await claim(lease_seconds=10)
    assert first.lease_token is not None
    await force_expiry(first.id)
    async with TestSession() as session:
        assert await ExecutionService(session).recover_expired() == 1
    attempts = await attempts_for(workflow_id)
    assert [(a.attempt_number, a.status) for a in attempts] == [
        (1, ExecutionAttemptStatus.EXPIRED),
        (2, ExecutionAttemptStatus.PENDING),
    ]
    async with TestSession() as session:
        with pytest.raises(LostLease):
            await ExecutionService(session).finalize_success(
                first.id,
                executor_id="executor-a",
                lease_token=first.lease_token,
                output={"stale": True},
            )
    second = await claim(executor_id="executor-b")
    assert second.attempt_number == 2
    assert second.lease_token != first.lease_token
    assert (
        await execute_attempt(
            second, executor_id="executor-b", sessions=TestSession, settings=Settings()
        )
        == "succeeded"
    )
    async with TestSession() as session:
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        assert workflow.status == WorkflowStatus.SUCCEEDED
        assert workflow.steps[0].status == StepStatus.SUCCEEDED
        assert workflow.steps[0].attempt_count == 2


async def test_two_recovery_scans_create_one_replacement() -> None:
    workflow_id, _ = await create_scheduled(step_count=1)
    first = await claim()
    await force_expiry(first.id)

    async def scan() -> int:
        async with TestSession() as session:
            return await ExecutionService(session).recover_expired()

    results = await asyncio.gather(scan(), scan())
    assert sorted(results) == [0, 1]
    attempts = await attempts_for(workflow_id)
    assert [attempt.attempt_number for attempt in attempts] == [1, 2]


async def test_expired_lease_cannot_finalize_before_scheduler_runs() -> None:
    workflow_id, _ = await create_scheduled(step_count=1)
    first = await claim()
    assert first.lease_token is not None
    await force_expiry(first.id)
    async with TestSession() as session:
        with pytest.raises(LostLease):
            await ExecutionService(session).finalize_success(
                first.id,
                executor_id="executor-a",
                lease_token=first.lease_token,
                output={},
            )
    assert (await attempts_for(workflow_id))[0].status == ExecutionAttemptStatus.RUNNING


async def test_recovery_scheduler_loop_polls_and_stops() -> None:
    workflow_id, _ = await create_scheduled(step_count=1)
    first = await claim()
    await force_expiry(first.id)
    stop = asyncio.Event()
    task = asyncio.create_task(recovery_loop(stop, TestSession, poll_interval=0.02))
    try:
        async with asyncio.timeout(3):
            while True:
                if len(await attempts_for(workflow_id)) >= 2:
                    break
                await asyncio.sleep(0.02)
        assert [item.status for item in await attempts_for(workflow_id)] == [
            ExecutionAttemptStatus.EXPIRED,
            ExecutionAttemptStatus.PENDING,
        ]
    finally:
        stop.set()
        await task


async def test_max_attempts_exhaustion_fails_workflow() -> None:
    workflow_id, _ = await create_scheduled(step_count=1, max_attempts=1)
    first = await claim()
    await force_expiry(first.id)
    async with TestSession() as session:
        assert await ExecutionService(session).recover_expired() == 1
    async with TestSession() as session:
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        assert workflow.status == WorkflowStatus.FAILED
        assert workflow.steps[0].error_code == "MAX_EXECUTION_ATTEMPTS_EXCEEDED"
        assert len(await attempts_for(workflow_id)) == 1


async def test_unknown_tool_is_not_automatically_retried_after_crash() -> None:
    workflow_id, _ = await create_scheduled(step_count=1, step_type="unknown")
    first = await claim()
    await force_expiry(first.id)
    async with TestSession() as session:
        await ExecutionService(session).recover_expired()
    async with TestSession() as session:
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        assert workflow.status == WorkflowStatus.FAILED
        assert workflow.steps[0].error_code == "UNSAFE_CRASH_RETRY"


async def test_healthy_long_running_executor_heartbeats_past_original_expiry() -> None:
    workflow_id, _ = await create_scheduled(
        step_count=1, step_type="slow_noop", step_input={"duration_ms": 1300}
    )
    attempt = await claim(lease_seconds=0.45)
    original_expiry = attempt.lease_expires_at
    settings = Settings(executor_lease_seconds=0.45, executor_heartbeat_seconds=0.1)
    task = asyncio.create_task(
        execute_attempt(attempt, executor_id="executor-a", sessions=TestSession, settings=settings)
    )
    async with asyncio.timeout(5):
        while not task.done():
            async with TestSession() as session:
                await ExecutionService(session).recover_expired()
            await asyncio.sleep(0.08)
    assert await task == "succeeded"
    attempts = await attempts_for(workflow_id)
    assert len(attempts) == 1
    assert attempts[0].status == ExecutionAttemptStatus.SUCCEEDED
    assert attempts[0].last_heartbeat_at is not None
    assert original_expiry is not None and attempts[0].lease_expires_at is not None
    assert attempts[0].lease_expires_at > original_expiry


async def test_executor_loop_drains_inflight_work_after_stop_signal() -> None:
    workflow_id, _ = await create_scheduled(
        step_count=1, step_type="slow_noop", step_input={"duration_ms": 1500}
    )
    stop = asyncio.Event()
    settings = Settings(
        # Drain behavior is the invariant here; the separate heartbeat test uses
        # sub-second leases. Leave headroom for a heavily loaded Docker CI host.
        executor_lease_seconds=5,
        executor_heartbeat_seconds=0.25,
        executor_poll_interval_seconds=0.05,
    )
    task = asyncio.create_task(
        executor_loop(stop, TestSession, executor_id="draining-executor", settings=settings)
    )
    try:
        async with asyncio.timeout(6):
            while True:
                current = await attempts_for(workflow_id)
                if current and current[0].status == ExecutionAttemptStatus.RUNNING:
                    break
                await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(task, timeout=6)
        records = await attempts_for(workflow_id)
        assert len(records) == 1
        assert records[0].status == ExecutionAttemptStatus.SUCCEEDED
    finally:
        stop.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_cancel_fences_active_attempt_and_heartbeat() -> None:
    workflow_id, _ = await create_scheduled(step_count=1)
    attempt = await claim()
    assert attempt.lease_token is not None
    async with TestSession() as session:
        await WorkflowService(session).cancel_workflow(workflow_id)
    async with TestSession() as session:
        assert not await ExecutionService(session).heartbeat(
            attempt.id,
            executor_id="executor-a",
            lease_token=attempt.lease_token,
            lease_seconds=20,
        )
    async with TestSession() as session:
        with pytest.raises(LostLease):
            await ExecutionService(session).finalize_success(
                attempt.id,
                executor_id="executor-a",
                lease_token=attempt.lease_token,
                output={},
            )
    assert (await attempts_for(workflow_id))[0].status == ExecutionAttemptStatus.CANCELLED


async def test_external_refund_result_lost_then_recovered_without_second_side_effect() -> None:
    payments_url = os.getenv("MOCK_PAYMENTS_URL", "http://localhost:8001")
    customer = f"customer-{uuid4()}"
    step_input: dict[str, object] = {"customer_id": customer, "amount": "49.99"}
    workflow_id, step_id = await create_scheduled(
        step_count=1, step_type="mock_refund", step_input=step_input
    )
    first = await claim()
    async with httpx.AsyncClient(base_url=payments_url) as client:
        count_before = (await client.get("/refunds/count")).json()["count"]
    # The external operation succeeds, but Continuum deliberately does not finalize attempt 1.
    first_result = await execute_tool(
        "mock_refund",
        step_input,
        ExecutionContext(workflow_id, step_id, first.id, 1),
        payments_url=payments_url,
    )
    await force_expiry(first.id)
    async with TestSession() as session:
        assert await ExecutionService(session).recover_expired() == 1
    second = await claim(executor_id="executor-b")
    assert (
        await execute_attempt(
            second,
            executor_id="executor-b",
            sessions=TestSession,
            settings=Settings(mock_payments_url=payments_url),
        )
        == "succeeded"
    )
    async with httpx.AsyncClient(base_url=payments_url) as client:
        count_after = (await client.get("/refunds/count")).json()["count"]
        refund = (await client.get(f"/refunds/by-idempotency-key/{operation_id(step_id)}")).json()
    assert count_after == count_before + 1
    assert refund["refund_id"] == first_result["refund_id"]
    attempts = await attempts_for(workflow_id)
    assert [(attempt.attempt_number, attempt.status) for attempt in attempts] == [
        (1, ExecutionAttemptStatus.EXPIRED),
        (2, ExecutionAttemptStatus.SUCCEEDED),
    ]
    async with TestSession() as session:
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        assert workflow.status == WorkflowStatus.SUCCEEDED
