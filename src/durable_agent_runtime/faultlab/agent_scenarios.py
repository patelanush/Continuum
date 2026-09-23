"""Phase 5 agent fault boundaries against the isolated real Compose stack."""

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select

from durable_agent_runtime.db.models import AgentRun, AgentToolCall, AgentTurn, ModelCall
from durable_agent_runtime.faultlab.assertions import classify_workflow
from durable_agent_runtime.faultlab.models import TrialResult
from durable_agent_runtime.faultlab.runtime import FaultLabRuntime, wait_for


def _inject(trial: TrialResult, fault_type: str, point: str) -> None:
    trial.fault_type = fault_type
    trial.fault_injection_point = point
    trial.fault_injected_at = datetime.now(UTC)
    trial.injected_failure_count += 1
    trial.recovery_required = True


def _input(
    trial: TrialResult,
    script: dict[str, list[dict[str, Any]]] | None = None,
    *,
    refund_delay_ms: int = 0,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "customer_id": f"agent-{trial.trial_id}",
        "amount": "49.99",
        "request": "I was charged twice. Refund the duplicate charge.",
    }
    if script is not None:
        value["fake_script"] = script
    if refund_delay_ms:
        value["faultlab_refund_delay_ms"] = refund_delay_ms
    return value


async def _start(
    runtime: FaultLabRuntime,
    trial: TrialResult,
    script: dict[str, list[dict[str, Any]]] | None = None,
    *,
    refund_delay_ms: int = 0,
) -> None:
    created = await runtime.create_start(
        [
            {
                "name": "support",
                "step_type": "support_agent",
                "input": _input(trial, script, refund_delay_ms=refund_delay_ms),
            }
        ],
        label=str(trial.trial_id),
    )
    trial.workflow_id = UUID(created["id"])
    trial.step_id = UUID(created["steps"][0]["id"])


async def _records(
    runtime: FaultLabRuntime, trial: TrialResult
) -> tuple[AgentRun | None, list[AgentTurn], list[ModelCall], list[AgentToolCall]]:
    assert trial.step_id is not None
    async with runtime.sessions() as session:
        run = await session.scalar(select(AgentRun).where(AgentRun.step_id == trial.step_id))
        if run is None:
            return None, [], [], []
        turns = list(
            await session.scalars(
                select(AgentTurn)
                .where(AgentTurn.agent_run_id == run.id)
                .order_by(AgentTurn.turn_number)
            )
        )
        calls = list(
            await session.scalars(
                select(ModelCall).where(ModelCall.agent_turn_id.in_([turn.id for turn in turns]))
            )
        )
        tools = list(
            await session.scalars(select(AgentToolCall).where(AgentToolCall.agent_run_id == run.id))
        )
        return run, turns, calls, tools


async def _wait_tool(runtime: FaultLabRuntime, trial: TrialResult, status: str) -> AgentToolCall:
    async def probe() -> AgentToolCall | None:
        _, _, _, tools = await _records(runtime, trial)
        return next(
            (
                item
                for item in tools
                if item.tool_name == "refund_customer" and item.status.value == status
            ),
            None,
        )

    return await wait_for(probe)


async def _finish(
    runtime: FaultLabRuntime,
    trial: TrialResult,
    *,
    attempts: int,
    refund: bool,
    expected_status: str = "SUCCEEDED",
) -> None:
    assert trial.workflow_id is not None
    trial.expected_terminal_status = expected_status
    await runtime.wait_terminal(trial.workflow_id, wait_timeout=65)
    await classify_workflow(
        runtime,
        trial,
        expected_attempts=attempts,
        expected_refunds=1 if refund else 0,
        customer_id=f"agent-{trial.trial_id}",
        expected_outbox=1,
        expected_attempt_statuses=["EXPIRED", "SUCCEEDED"] if attempts == 2 else None,
    )
    run, turns, calls, tools = await _records(runtime, trial)
    failures: list[str] = []
    if run is None:
        failures.append("agent run missing")
    elif run.status.value != expected_status:
        failures.append(f"agent run={run.status}, expected={expected_status}")
    if refund:
        refunds = [item for item in tools if item.tool_name == "refund_customer"]
        if len(refunds) != 1:
            failures.append(f"refund tool calls={len(refunds)}, expected=1")
        else:
            logical = refunds[0]
            if logical.status.value != "SUCCEEDED" or logical.result is None:
                failures.append("refund tool result is not durably completed")
            external = await runtime.refund(logical.operation_id)
            if external is None or external["idempotency_key"] != logical.operation_id:
                failures.append("stable tool operation was not found at payments service")
            elif (
                logical.result is not None and logical.result["refund_id"] != external["refund_id"]
            ):
                failures.append("persisted tool result differs from external refund")
            else:
                trial.external_side_effect_id = UUID(external["refund_id"])
                trial.notes["operation_id"] = logical.operation_id
                trial.notes["tool_call_id"] = str(logical.id)
    trial.notes["agent_run_id"] = str(run.id) if run else None
    trial.notes["agent_turn_count"] = len(turns)
    trial.notes["model_call_count"] = len(calls)
    trial.notes["agent_tool_count"] = len(tools)
    if failures:
        trial.correct = False
        trial.recovered = False
        trial.failure_reason = "; ".join(filter(None, [trial.failure_reason, *failures]))


async def model_timeout(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await _start(
        runtime,
        trial,
        {
            "1": [
                {"fake_error": "timeout"},
                {"type": "tool_call", "tool_name": "read_refund_policy", "arguments": {}},
            ]
        },
    )
    await _finish(runtime, trial, attempts=1, refund=True)
    _, _, calls, _ = await _records(runtime, trial)
    assert len(calls) == 4 and sum(call.status.value == "FAILED" for call in calls) == 1


async def malformed_output(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await _start(
        runtime,
        trial,
        {
            "1": [
                {"fake_error": "malformed"},
                {"type": "tool_call", "tool_name": "read_refund_policy", "arguments": {}},
            ]
        },
    )
    await _finish(runtime, trial, attempts=1, refund=True)
    _, _, calls, _ = await _records(runtime, trial)
    assert len(calls) == 4 and sum(call.status.value == "FAILED" for call in calls) == 1


async def crash_after_decision(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await runtime.docker.compose("stop", "executor")
    oneoff = ""
    try:
        oneoff = await runtime.docker.run_oneoff(
            "executor", variables={"FAULTLAB_AGENT_PAUSE_AFTER_DECISION": "1"}
        )
        await _start(runtime, trial)
        assert trial.workflow_id is not None
        await runtime.wait_attempt(trial.workflow_id)
        tool = await _wait_tool(runtime, trial, "PENDING")
        assert tool.result is None
        _, turns, calls, _ = await _records(runtime, trial)
        refund_turn = next(turn for turn in turns if turn.id == tool.agent_turn_id)
        original_calls = [call for call in calls if call.agent_turn_id == refund_turn.id]
        assert len(original_calls) == 1
        trial.notes["persisted_model_call_id"] = str(original_calls[0].id)
        _inject(
            trial, "SIGKILL", "after refund decision/tool identity commit, before external call"
        )
        await runtime.docker.command("docker", "kill", "--signal=KILL", oneoff)
        await runtime.docker.remove_oneoff(oneoff)
        oneoff = ""
    finally:
        if oneoff:
            await runtime.docker.remove_oneoff(oneoff)
        await runtime.docker.compose("start", "executor")
    await _finish(runtime, trial, attempts=2, refund=True)
    _, turns, calls, _ = await _records(runtime, trial)
    assert len([call for call in calls if call.agent_turn_id == refund_turn.id]) == 1


async def crash_after_side_effect(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await _start(runtime, trial, refund_delay_ms=15000)
    assert trial.workflow_id is not None
    first = await runtime.wait_attempt(trial.workflow_id)
    tool = await _wait_tool(runtime, trial, "PENDING")
    external = await runtime.wait_refund(tool.operation_id)
    assert tool.result is None
    trial.notes["refund_committed_before_kill"] = external["refund_id"]
    _inject(trial, "SIGKILL", "refund committed externally, agent tool result not persisted")
    killed = await runtime.docker.kill_owner(first["executor_id"])
    try:
        await _finish(runtime, trial, attempts=2, refund=True)
    finally:
        await runtime.docker.restart_container(killed)
    _, _, calls, tools = await _records(runtime, trial)
    assert len([item for item in calls if item.agent_turn_id == tool.agent_turn_id]) == 1
    assert next(item for item in tools if item.id == tool.id).operation_id == tool.operation_id


async def crash_after_tool_result(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await runtime.docker.compose("stop", "executor")
    oneoff = ""
    try:
        oneoff = await runtime.docker.run_oneoff(
            "executor", variables={"FAULTLAB_AGENT_PAUSE_AFTER_TOOL_RESULT": "1"}
        )
        await _start(runtime, trial)
        assert trial.workflow_id is not None
        await runtime.wait_attempt(trial.workflow_id)
        tool = await _wait_tool(runtime, trial, "SUCCEEDED")
        assert tool.result is not None
        _inject(trial, "SIGKILL", "tool result and next turn committed")
        await runtime.docker.command("docker", "kill", "--signal=KILL", oneoff)
        await runtime.docker.remove_oneoff(oneoff)
        oneoff = ""
    finally:
        if oneoff:
            await runtime.docker.remove_oneoff(oneoff)
        await runtime.docker.compose("start", "executor")
    await _finish(runtime, trial, attempts=2, refund=True)
    _, _, _, tools = await _records(runtime, trial)
    assert len([item for item in tools if item.tool_name == "refund_customer"]) == 1


async def crash_after_final(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await runtime.docker.compose("stop", "executor")
    oneoff = ""
    try:
        oneoff = await runtime.docker.run_oneoff(
            "executor", variables={"FAULTLAB_AGENT_PAUSE_AFTER_FINAL": "1"}
        )
        await _start(runtime, trial, {"1": [{"type": "final", "response": "Persisted answer"}]})
        assert trial.workflow_id is not None
        await runtime.wait_attempt(trial.workflow_id)

        async def finished() -> AgentRun | None:
            run, _, _, _ = await _records(runtime, trial)
            return run if run is not None and run.status.value == "SUCCEEDED" else None

        await wait_for(finished)
        _inject(trial, "SIGKILL", "final answer committed before outer step finalization")
        await runtime.docker.command("docker", "kill", "--signal=KILL", oneoff)
        await runtime.docker.remove_oneoff(oneoff)
        oneoff = ""
    finally:
        if oneoff:
            await runtime.docker.remove_oneoff(oneoff)
        await runtime.docker.compose("start", "executor")
    await _finish(runtime, trial, attempts=2, refund=False)
    _, turns, calls, _ = await _records(runtime, trial)
    assert len(turns) == 1 and len(calls) == 1


async def max_turns(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    cycle = {
        str(index): [{"type": "tool_call", "tool_name": "read_refund_policy", "arguments": {}}]
        for index in range(1, 9)
    }
    await _start(runtime, trial, cycle)
    await _finish(runtime, trial, attempts=1, refund=False, expected_status="FAILED")
    run, turns, _, _ = await _records(runtime, trial)
    assert run is not None and run.error_code == "MAX_AGENT_TURNS_EXCEEDED"
    assert len(turns) == 8


async def unknown_tool(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await _start(
        runtime, trial, {"1": [{"type": "tool_call", "tool_name": "shell", "arguments": {}}]}
    )
    await _finish(runtime, trial, attempts=1, refund=False, expected_status="FAILED")
    run, turns, calls, tools = await _records(runtime, trial)
    assert run is not None and run.error_code == "MODEL_ATTEMPTS_EXHAUSTED"
    assert len(turns) == 1 and len(calls) == 3 and not tools
