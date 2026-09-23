"""Durable agent checkpoints against PostgreSQL and the independent payments service."""

import asyncio
from typing import Any
from uuid import UUID

import httpx
import pytest
from httpx import AsyncClient
from sqlalchemy import func, select, text, update

from durable_agent_runtime.agent.decisions import DECISION_ADAPTER
from durable_agent_runtime.agent.providers import FakeModelProvider, ModelResult
from durable_agent_runtime.agent.runner import run_support_agent
from durable_agent_runtime.agent.service import AgentService
from durable_agent_runtime.agent.tools import execute_agent_tool
from durable_agent_runtime.core.config import Settings
from durable_agent_runtime.db.models import (
    AgentRun,
    AgentToolCall,
    AgentTurn,
    ExecutionAttempt,
    ModelCall,
)
from durable_agent_runtime.domain.enums import AgentRunStatus, ModelCallStatus, WorkflowStatus
from durable_agent_runtime.execution.executor import execute_attempt
from durable_agent_runtime.execution.tools import ExecutionContext
from durable_agent_runtime.services.execution import ExecutionService, LostLease
from durable_agent_runtime.services.workflows import WorkflowService
from tests.conftest import TestSession
from tests.integration.test_execution import claim, create_scheduled

pytestmark = pytest.mark.integration


def settings() -> Settings:
    return Settings(app_env="test", agent_provider="fake", executor_lease_seconds=60)


def support_input(customer_id: str, *, script: dict[str, Any] | None = None) -> dict[str, object]:
    payload: dict[str, object] = {
        "customer_id": customer_id,
        "amount": "49.99",
        "request": "I was charged twice. Please refund the duplicate charge.",
    }
    if script is not None:
        payload["fake_script"] = script
    return payload


async def records(
    step_id: UUID,
) -> tuple[AgentRun, list[AgentTurn], list[ModelCall], list[AgentToolCall]]:
    async with TestSession() as session:
        run = await session.scalar(select(AgentRun).where(AgentRun.step_id == step_id))
        assert run is not None
        turns = list(
            await session.scalars(
                select(AgentTurn)
                .where(AgentTurn.agent_run_id == run.id)
                .order_by(AgentTurn.turn_number)
            )
        )
        calls = list(
            await session.scalars(
                select(ModelCall)
                .where(ModelCall.agent_turn_id.in_([turn.id for turn in turns]))
                .order_by(ModelCall.id)
            )
        )
        tools = list(
            await session.scalars(
                select(AgentToolCall)
                .where(AgentToolCall.agent_run_id == run.id)
                .order_by(AgentToolCall.created_at, AgentToolCall.id)
            )
        )
        return run, turns, calls, tools


async def test_fake_support_agent_persists_three_turns_and_one_refund() -> None:
    workflow_id, step_id = await create_scheduled(
        step_count=1, step_type="support_agent", step_input=support_input("agent-basic")
    )
    attempt = await claim()
    assert (
        await execute_attempt(
            attempt, executor_id="executor-a", sessions=TestSession, settings=settings()
        )
        == "succeeded"
    )
    run, turns, calls, tools = await records(step_id)
    assert run.status == AgentRunStatus.SUCCEEDED
    assert [turn.decision_type for turn in turns] == ["tool_call", "tool_call", "final"]
    assert len(calls) == 3 and all(call.status == ModelCallStatus.SUCCEEDED for call in calls)
    assert [tool.tool_name for tool in tools] == ["read_refund_policy", "refund_customer"]
    assert tools[1].result is not None
    assert tools[1].result["idempotency_key"] == tools[1].operation_id
    async with TestSession() as session:
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        assert workflow.status == WorkflowStatus.SUCCEEDED
        assert workflow.steps[0].output is not None
        assert workflow.steps[0].output["agent_run_id"] == str(run.id)


async def test_timeout_and_malformed_calls_are_recorded_before_valid_decision() -> None:
    script: dict[str, list[dict[str, Any]]] = {
        "1": [
            {"fake_error": "timeout"},
            {"fake_error": "malformed"},
            {"type": "final", "response": "Done"},
        ]
    }
    _, step_id = await create_scheduled(
        step_count=1,
        step_type="support_agent",
        step_input=support_input("agent-retry", script=script),
    )
    attempt = await claim()
    assert (
        await execute_attempt(
            attempt, executor_id="executor-a", sessions=TestSession, settings=settings()
        )
        == "succeeded"
    )
    run, turns, calls, tools = await records(step_id)
    assert run.status == AgentRunStatus.SUCCEEDED
    assert len(turns) == 1 and not tools
    assert sorted(call.status for call in calls) == [
        ModelCallStatus.FAILED,
        ModelCallStatus.FAILED,
        ModelCallStatus.SUCCEEDED,
    ]
    assert {call.error_code for call in calls if call.status == ModelCallStatus.FAILED} == {
        "TimeoutError",
        "ValidationError",
    }


async def test_unknown_tool_never_executes_and_exhausts_model_attempts() -> None:
    script = {"1": [{"type": "tool_call", "tool_name": "shell", "arguments": {}}]}
    workflow_id, step_id = await create_scheduled(
        step_count=1,
        step_type="support_agent",
        step_input=support_input("agent-unknown", script=script),
    )
    attempt = await claim()
    assert (
        await execute_attempt(
            attempt, executor_id="executor-a", sessions=TestSession, settings=settings()
        )
        == "failed"
    )
    run, turns, calls, tools = await records(step_id)
    assert run.status == AgentRunStatus.FAILED
    assert len(turns) == 1 and len(calls) == 3 and not tools
    async with TestSession() as session:
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        assert workflow.status == WorkflowStatus.FAILED


async def test_persisted_final_answer_is_reused_after_outer_attempt_expiry() -> None:
    script = {"1": [{"type": "final", "response": "Durable final"}]}
    workflow_id, step_id = await create_scheduled(
        step_count=1,
        step_type="support_agent",
        step_input=support_input("agent-final", script=script),
    )
    attempt = await claim(lease_seconds=60)
    assert attempt.lease_token is not None
    context = ExecutionContext(
        workflow_id=workflow_id, step_id=step_id, attempt_id=attempt.id, attempt_number=1
    )
    first = await run_support_agent(
        support_input("agent-final", script=script),
        context,
        executor_id="executor-a",
        lease_token=attempt.lease_token,
        sessions=TestSession,
        settings=settings(),
    )
    assert first["final_response"] == "Durable final"
    # No outer finalization happened. A replay of the same ownership generation
    # must return the persisted final without another model call.
    second = await run_support_agent(
        support_input("agent-final", script=script),
        context,
        executor_id="executor-a",
        lease_token=attempt.lease_token,
        sessions=TestSession,
        settings=settings(),
    )
    assert second == first
    _, _, calls, _ = await records(step_id)
    assert len(calls) == 1


async def test_fenced_agent_checkpoint_rejects_wrong_outer_lease() -> None:
    _, step_id = await create_scheduled(
        step_count=1, step_type="support_agent", step_input=support_input("agent-fence")
    )
    attempt = await claim()
    context = ExecutionContext(
        workflow_id=attempt.workflow_id, step_id=step_id, attempt_id=attempt.id, attempt_number=1
    )
    with pytest.raises(LostLease):
        await run_support_agent(
            support_input("agent-fence"),
            context,
            executor_id="executor-a",
            lease_token=UUID(int=0),
            sessions=TestSession,
            settings=settings(),
        )
    async with TestSession() as session:
        assert await session.scalar(select(func.count()).select_from(AgentRun)) == 0


async def expire_and_replace(attempt: ExecutionAttempt) -> ExecutionAttempt:
    async with TestSession() as session:
        async with session.begin():
            await session.execute(
                update(ExecutionAttempt)
                .where(ExecutionAttempt.id == attempt.id)
                .values(lease_expires_at=func.clock_timestamp() - text("interval '1 second'"))
            )
    async with TestSession() as session:
        assert await ExecutionService(session).recover_expired() == 1
    replacement = await claim("executor-b", lease_seconds=60)
    assert replacement.attempt_number == 2
    return replacement


async def seed_refund_decision(
    attempt: ExecutionAttempt, step_id: UUID, provider: FakeModelProvider | None = None
) -> tuple[UUID, UUID, str]:
    assert attempt.lease_token is not None
    context = ExecutionContext(
        workflow_id=attempt.workflow_id, step_id=step_id, attempt_id=attempt.id, attempt_number=1
    )
    async with TestSession() as session:
        run_id = await AgentService(session, context, "executor-a", attempt.lease_token).ensure_run(
            provider="fake", model="scripted-fake-v1", max_turns=8
        )
    async with TestSession() as session:
        started = await AgentService(
            session, context, "executor-a", attempt.lease_token
        ).start_model_call(run_id, 3)
    assert started is not None
    call_id, request, _ = started
    raw = (
        await provider.generate(request, turn_number=1, attempt_number=1)
        if provider is not None
        else ModelResult(
            raw={
                "type": "tool_call",
                "tool_name": "refund_customer",
                "arguments": {"customer_id": "agent-crash", "amount": "49.99"},
            }
        )
    )
    decision = DECISION_ADAPTER.validate_python(raw.raw)
    async with TestSession() as session:
        await AgentService(session, context, "executor-a", attempt.lease_token).persist_decision(
            run_id, call_id, decision, raw, 1
        )
    _, _, calls, tools = await records(step_id)
    assert len(calls) == 1 and len(tools) == 1
    return run_id, tools[0].id, tools[0].operation_id


async def test_persisted_refund_decision_is_not_regenerated_on_replacement() -> None:
    script: dict[str, list[dict[str, Any]]] = {
        "1": [
            {
                "type": "tool_call",
                "tool_name": "refund_customer",
                "arguments": {"customer_id": "agent-crash", "amount": "49.99"},
            },
            {"type": "final", "response": "A contradictory retry"},
        ],
        "2": [{"type": "final", "response": "Refund completed"}],
    }
    provider = FakeModelProvider(script)
    workflow_id, step_id = await create_scheduled(
        step_count=1,
        step_type="support_agent",
        step_input=support_input("agent-crash", script=script),
    )
    first = await claim()
    run_id, tool_id, operation = await seed_refund_decision(first, step_id, provider)
    assert first.lease_token is not None
    _, before_turns, before_calls, _ = await records(step_id)
    contradictory = DECISION_ADAPTER.validate_python(
        {"type": "final", "response": "A contradictory retry"}
    )
    async with TestSession() as session:
        with pytest.raises(LostLease):
            await AgentService(
                session,
                ExecutionContext(
                    workflow_id=workflow_id, step_id=step_id, attempt_id=first.id, attempt_number=1
                ),
                "executor-a",
                first.lease_token,
            ).persist_decision(
                run_id,
                before_calls[0].id,
                contradictory,
                ModelResult(raw=contradictory.model_dump()),
                1,
            )
    assert before_turns[0].decision is not None
    replacement = await expire_and_replace(first)
    assert replacement.lease_token is not None
    output = await run_support_agent(
        support_input("agent-crash", script=script),
        ExecutionContext(
            workflow_id=workflow_id, step_id=step_id, attempt_id=replacement.id, attempt_number=2
        ),
        executor_id="executor-b",
        lease_token=replacement.lease_token,
        sessions=TestSession,
        settings=settings(),
        provider_override=provider,
    )
    async with TestSession() as session:
        await ExecutionService(session).finalize_success(
            replacement.id,
            executor_id="executor-b",
            lease_token=replacement.lease_token,
            output=output,
        )
    assert provider.invocations.count((1, 1)) == 1
    assert not any(turn == 1 and number > 1 for turn, number in provider.invocations)
    run, turns, calls, tools = await records(step_id)
    assert run.id == run_id and run.status == AgentRunStatus.SUCCEEDED
    assert tools[0].id == tool_id and tools[0].operation_id == operation
    assert len([call for call in calls if call.agent_turn_id == turns[0].id]) == 1
    assert turns[0].decision is not None and turns[0].decision["type"] == "tool_call"
    async with TestSession() as session:
        assert (
            await WorkflowService(session).get_workflow(workflow_id)
        ).status == WorkflowStatus.SUCCEEDED


async def test_refund_committed_before_result_checkpoint_is_reused() -> None:
    script = {"2": [{"type": "final", "response": "Refund completed"}]}
    _, step_id = await create_scheduled(
        step_count=1,
        step_type="support_agent",
        step_input=support_input("agent-crash", script=script),
    )
    first = await claim()
    run_id, tool_id, operation = await seed_refund_decision(first, step_id)
    result = await execute_agent_tool(
        "refund_customer",
        {"customer_id": "agent-crash", "amount": "49.99"},
        operation,
        payments_url=settings().mock_payments_url,
    )
    assert result["idempotency_key"] == operation
    replacement = await expire_and_replace(first)
    assert (
        await execute_attempt(
            replacement, executor_id="executor-b", sessions=TestSession, settings=settings()
        )
        == "succeeded"
    )
    run, turns, calls, tools = await records(step_id)
    assert run.id == run_id and len(calls) == 2 and len(turns) == 2
    assert tools[0].id == tool_id and tools[0].operation_id == operation
    assert tools[0].result is not None and tools[0].result["refund_id"] == result["refund_id"]
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings().mock_payments_url}/refunds/by-idempotency-key/{operation}"
        )
    assert response.status_code == 200 and response.json()["refund_id"] == result["refund_id"]


async def test_persisted_tool_result_is_not_reexecuted_after_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    script = {"2": [{"type": "final", "response": "Already refunded"}]}
    _, step_id = await create_scheduled(
        step_count=1,
        step_type="support_agent",
        step_input=support_input("agent-crash", script=script),
    )
    first = await claim()
    run_id, tool_id, operation = await seed_refund_decision(first, step_id)
    tool_result = await execute_agent_tool(
        "refund_customer",
        {"customer_id": "agent-crash", "amount": "49.99"},
        operation,
        payments_url=settings().mock_payments_url,
    )
    assert first.lease_token is not None
    context = ExecutionContext(
        workflow_id=first.workflow_id, step_id=step_id, attempt_id=first.id, attempt_number=1
    )
    async with TestSession() as session:
        await AgentService(session, context, "executor-a", first.lease_token).persist_tool_result(
            run_id, tool_id, tool_result
        )
    replacement = await expire_and_replace(first)

    async def forbidden_reexecution(*_args: object, **_kwargs: object) -> dict[str, Any]:
        raise AssertionError("Persisted tool result must not invoke the tool again")

    monkeypatch.setattr(
        "durable_agent_runtime.agent.runner.execute_agent_tool", forbidden_reexecution
    )
    assert (
        await execute_attempt(
            replacement, executor_id="executor-b", sessions=TestSession, settings=settings()
        )
        == "succeeded"
    )
    _, turns, calls, tools = await records(step_id)
    assert len(turns) == 2 and len(calls) == 2 and len(tools) == 1
    assert tools[0].result == tool_result


async def test_model_response_lost_before_persistence_may_change_on_retry() -> None:
    script: dict[str, list[dict[str, Any]]] = {
        "1": [
            {
                "type": "tool_call",
                "tool_name": "refund_customer",
                "arguments": {"customer_id": "agent-lost", "amount": "49.99"},
            },
            {"type": "final", "response": "No refund was committed"},
        ]
    }
    _, step_id = await create_scheduled(
        step_count=1,
        step_type="support_agent",
        step_input=support_input("agent-lost", script=script),
    )
    first = await claim()
    assert first.lease_token is not None
    context = ExecutionContext(
        workflow_id=first.workflow_id, step_id=step_id, attempt_id=first.id, attempt_number=1
    )
    async with TestSession() as session:
        run_id = await AgentService(session, context, "executor-a", first.lease_token).ensure_run(
            provider="fake", model="scripted-fake-v1", max_turns=8
        )
    async with TestSession() as session:
        started = await AgentService(
            session, context, "executor-a", first.lease_token
        ).start_model_call(run_id, 3)
    assert started is not None
    # The first response exists only in process memory; neither decision nor tool exists.
    provider = FakeModelProvider(script)
    _, request, _ = started
    lost_response = await provider.generate(request, turn_number=1, attempt_number=1)
    assert lost_response.raw["type"] == "tool_call"
    replacement = await expire_and_replace(first)
    assert (
        await execute_attempt(
            replacement, executor_id="executor-b", sessions=TestSession, settings=settings()
        )
        == "succeeded"
    )
    run, turns, calls, tools = await records(step_id)
    assert run.final_response == "No refund was committed"
    assert len(turns) == 1 and not tools
    assert sorted(call.status for call in calls) == [
        ModelCallStatus.FAILED,
        ModelCallStatus.SUCCEEDED,
    ]
    assert {call.error_code for call in calls} == {"INTERRUPTED", None}


async def test_max_turns_fails_without_unbounded_loop() -> None:
    script = {
        "1": [{"type": "tool_call", "tool_name": "read_refund_policy", "arguments": {}}],
        "2": [{"type": "tool_call", "tool_name": "read_refund_policy", "arguments": {}}],
    }
    workflow_id, step_id = await create_scheduled(
        step_count=1,
        step_type="support_agent",
        step_input=support_input("agent-loop", script=script),
    )
    attempt = await claim()
    config = Settings(app_env="test", agent_provider="fake", agent_max_turns=2)
    assert (
        await execute_attempt(
            attempt, executor_id="executor-a", sessions=TestSession, settings=config
        )
        == "failed"
    )
    run, turns, calls, tools = await records(step_id)
    assert run.error_code == "MAX_AGENT_TURNS_EXCEEDED"
    assert len(turns) == 2 and len(calls) == 2 and len(tools) == 2
    async with TestSession() as session:
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        assert workflow.steps[0].error_code == "MAX_AGENT_TURNS_EXCEEDED"


async def test_invalid_materialization_rolls_back_model_and_decision() -> None:
    _, step_id = await create_scheduled(
        step_count=1, step_type="support_agent", step_input=support_input("agent-rollback")
    )
    first = await claim()
    assert first.lease_token is not None
    context = ExecutionContext(
        workflow_id=first.workflow_id, step_id=step_id, attempt_id=first.id, attempt_number=1
    )
    async with TestSession() as session:
        run_id = await AgentService(session, context, "executor-a", first.lease_token).ensure_run(
            provider="fake", model="scripted-fake-v1", max_turns=8
        )
    async with TestSession() as session:
        started = await AgentService(
            session, context, "executor-a", first.lease_token
        ).start_model_call(run_id, 3)
    assert started is not None
    call_id, _, _ = started
    invalid = DECISION_ADAPTER.validate_python(
        {"type": "tool_call", "tool_name": "shell", "arguments": {}}
    )
    async with TestSession() as session:
        with pytest.raises(ValueError):
            await AgentService(session, context, "executor-a", first.lease_token).persist_decision(
                run_id, call_id, invalid, ModelResult(raw=invalid.model_dump()), 1
            )
    _, turns, calls, tools = await records(step_id)
    assert turns[0].decision is None and not tools
    assert calls[0].status == ModelCallStatus.RUNNING


async def test_read_only_agent_trace_endpoint(client: AsyncClient) -> None:
    workflow_id, _ = await create_scheduled(
        step_count=1,
        step_type="support_agent",
        step_input=support_input(
            "agent-trace", script={"1": [{"type": "final", "response": "Done"}]}
        ),
    )
    attempt = await claim()
    assert (
        await execute_attempt(
            attempt, executor_id="executor-a", sessions=TestSession, settings=settings()
        )
        == "succeeded"
    )
    response = await client.get(f"/api/v1/workflows/{workflow_id}/agent")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["status"] == "SUCCEEDED"
    assert payload[0]["turns"][0]["decision"]["type"] == "final"
    assert payload[0]["turns"][0]["model_calls"][0]["request_hash"]
    missing = await client.get(f"/api/v1/workflows/{UUID(int=0)}/agent")
    assert missing.status_code == 404


async def test_model_generation_longer_than_initial_lease_is_heartbeated() -> None:
    script = {"1": [{"type": "final", "response": "Healthy long call", "fake_delay_ms": 2800}]}
    workflow_id, step_id = await create_scheduled(
        step_count=1,
        step_type="support_agent",
        step_input=support_input("agent-heartbeat", script=script),
    )
    attempt = await claim(lease_seconds=1.5)
    initial_expiry = attempt.lease_expires_at
    assert initial_expiry is not None
    config = Settings(
        app_env="test",
        agent_provider="fake",
        executor_lease_seconds=1.5,
        executor_heartbeat_seconds=0.2,
    )
    execution = asyncio.create_task(
        execute_attempt(attempt, executor_id="executor-a", sessions=TestSession, settings=config)
    )
    deadline = asyncio.get_running_loop().time() + 4
    renewed = False
    while asyncio.get_running_loop().time() < deadline:
        async with TestSession() as session:
            current = await session.get(ExecutionAttempt, attempt.id)
            assert current is not None
            if current.lease_expires_at is not None and current.lease_expires_at > initial_expiry:
                renewed = True
                break
        await asyncio.sleep(0.02)
    assert renewed
    # Wait on database time until the original lease would have expired.
    while asyncio.get_running_loop().time() < deadline:
        async with TestSession() as session:
            past_original = await session.scalar(select(func.clock_timestamp() > initial_expiry))
        if past_original:
            break
        await asyncio.sleep(0.02)
    else:
        raise AssertionError("Database time did not pass the original lease")
    async with TestSession() as session:
        assert await ExecutionService(session).recover_expired() == 0
    assert await execution == "succeeded"
    run, _, _, _ = await records(step_id)
    assert run.status == AgentRunStatus.SUCCEEDED
    async with TestSession() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(ExecutionAttempt)
                .where(ExecutionAttempt.workflow_id == workflow_id)
            )
            == 1
        )


async def test_cancellation_prevents_first_model_call() -> None:
    workflow_id, step_id = await create_scheduled(
        step_count=1, step_type="support_agent", step_input=support_input("agent-cancel")
    )
    attempt = await claim()
    assert attempt.lease_token is not None
    context = ExecutionContext(
        workflow_id=workflow_id,
        step_id=step_id,
        attempt_id=attempt.id,
        attempt_number=1,
    )
    async with TestSession() as session:
        run_id = await AgentService(session, context, "executor-a", attempt.lease_token).ensure_run(
            provider="fake", model="scripted-fake-v1", max_turns=8
        )
    async with TestSession() as session:
        await WorkflowService(session).cancel_workflow(workflow_id)
    with pytest.raises(LostLease):
        await run_support_agent(
            support_input("agent-cancel"),
            context,
            executor_id="executor-a",
            lease_token=attempt.lease_token,
            sessions=TestSession,
            settings=settings(),
        )
    async with TestSession() as session:
        run = await session.get(AgentRun, run_id)
        assert run is not None and run.status == AgentRunStatus.CANCELLED
        assert await session.scalar(select(func.count()).select_from(ModelCall)) == 0


async def test_cancellation_closes_inflight_model_call() -> None:
    workflow_id, step_id = await create_scheduled(
        step_count=1, step_type="support_agent", step_input=support_input("agent-cancel-model")
    )
    attempt = await claim()
    assert attempt.lease_token is not None
    context = ExecutionContext(workflow_id, step_id, attempt.id, 1)
    async with TestSession() as session:
        service = AgentService(session, context, "executor-a", attempt.lease_token)
        run_id = await service.ensure_run(provider="fake", model="scripted-fake-v1", max_turns=8)
    async with TestSession() as session:
        started = await AgentService(
            session, context, "executor-a", attempt.lease_token
        ).start_model_call(run_id, 3)
    assert started is not None
    call_id, _, _ = started
    async with TestSession() as session:
        await WorkflowService(session).cancel_workflow(workflow_id)
    async with TestSession() as session:
        run = await session.get(AgentRun, run_id)
        call = await session.get(ModelCall, call_id)
        assert run is not None and run.status == AgentRunStatus.CANCELLED
        assert call is not None and call.status == ModelCallStatus.FAILED
        assert call.error_code == "WORKFLOW_CANCELLED"


async def test_outer_step_failure_closes_agent_run_and_inflight_call() -> None:
    workflow_id, step_id = await create_scheduled(
        step_count=1, step_type="support_agent", step_input=support_input("agent-outer-fail")
    )
    attempt = await claim()
    assert attempt.lease_token is not None
    context = ExecutionContext(workflow_id, step_id, attempt.id, 1)
    async with TestSession() as session:
        run_id = await AgentService(session, context, "executor-a", attempt.lease_token).ensure_run(
            provider="fake", model="scripted-fake-v1", max_turns=8
        )
    async with TestSession() as session:
        started = await AgentService(
            session, context, "executor-a", attempt.lease_token
        ).start_model_call(run_id, 3)
    assert started is not None
    call_id, _, _ = started
    async with TestSession() as session:
        await WorkflowService(session).fail_step(
            step_id, error_code="MAX_EXECUTION_ATTEMPTS_EXCEEDED", error_detail="lease exhausted"
        )
    async with TestSession() as session:
        workflow = await WorkflowService(session).get_workflow(workflow_id)
        run = await session.get(AgentRun, run_id)
        call = await session.get(ModelCall, call_id)
        assert workflow.status == WorkflowStatus.FAILED
        assert run is not None and run.status == AgentRunStatus.FAILED
        assert run.error_code == "MAX_EXECUTION_ATTEMPTS_EXCEEDED"
        assert call is not None and call.status == ModelCallStatus.FAILED
