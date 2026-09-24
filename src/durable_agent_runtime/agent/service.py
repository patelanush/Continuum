"""Short, fenced transactions for replay-safe agent checkpoints."""

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from durable_agent_runtime.agent.decisions import (
    FinalDecision,
    ToolDecision,
    agent_operation_id,
    canonical_hash,
    validate_tool_arguments,
)
from durable_agent_runtime.agent.prompt import PROMPT_VERSION, prompt_for
from durable_agent_runtime.agent.providers import ModelResult
from durable_agent_runtime.coding.decisions import (
    TOOL_SEMANTICS as CODING_TOOL_SEMANTICS,
)
from durable_agent_runtime.coding.decisions import validate_coding_arguments
from durable_agent_runtime.db.models import (
    AgentRun,
    AgentToolCall,
    AgentTurn,
    ModelCall,
    WorkflowStep,
)
from durable_agent_runtime.domain.enums import (
    AgentRunStatus,
    AgentToolCallStatus,
    AgentTurnStatus,
    ModelCallStatus,
)
from durable_agent_runtime.domain.state_machine import (
    validate_agent_run_transition,
    validate_agent_tool_call_transition,
    validate_agent_turn_transition,
    validate_model_call_transition,
)
from durable_agent_runtime.execution.tools import ExecutionContext, PermanentToolError, RetrySafety
from durable_agent_runtime.observability.metrics import count
from durable_agent_runtime.services.execution import ExecutionService, LostLease


class AgentService:
    def __init__(
        self,
        session: AsyncSession,
        context: ExecutionContext,
        executor_id: str,
        lease_token: UUID,
    ) -> None:
        self.session = session
        self.context = context
        self.executor_id = executor_id
        self.lease_token = lease_token

    async def _fence(self) -> None:
        await ExecutionService(self.session)._lock_owned(
            self.context.attempt_id, self.executor_id, self.lease_token
        )

    @staticmethod
    def _run_status(run: AgentRun, target: AgentRunStatus) -> None:
        validate_agent_run_transition(run.status, target)
        run.status = target

    @staticmethod
    def _turn_status(turn: AgentTurn, target: AgentTurnStatus) -> None:
        validate_agent_turn_transition(turn.status, target)
        turn.status = target

    @staticmethod
    def _model_status(call: ModelCall, target: ModelCallStatus) -> None:
        validate_model_call_transition(call.status, target)
        call.status = target

    @staticmethod
    def _tool_status(call: AgentToolCall, target: AgentToolCallStatus) -> None:
        validate_agent_tool_call_transition(call.status, target)
        call.status = target

    async def ensure_run(
        self,
        *,
        provider: str,
        model: str,
        max_turns: int,
        agent_type: str = "support_agent",
        prompt_version: str = PROMPT_VERSION,
    ) -> UUID:
        async with self.session.begin():
            await self._fence()
            existing = await self.session.scalar(
                select(AgentRun).where(AgentRun.step_id == self.context.step_id).with_for_update()
            )
            if existing is not None:
                return existing.id
            step = await self.session.get(WorkflowStep, self.context.step_id)
            assert step is not None
            now = datetime.now(UTC)
            run = AgentRun(
                id=uuid4(),
                workflow_id=self.context.workflow_id,
                step_id=self.context.step_id,
                status=AgentRunStatus.PENDING,
                agent_type=agent_type,
                provider=provider,
                model=model,
                system_prompt_version=prompt_version,
                max_turns=max_turns,
                current_turn_number=1,
                started_at=now,
            )
            self.session.add(run)
            self._run_status(run, AgentRunStatus.RUNNING)
            request = self._request(step.input, [], prompt_version)
            self.session.add(self._new_turn(run.id, 1, request))
            return run.id

    async def snapshot(
        self, run_id: UUID
    ) -> tuple[AgentRunStatus, AgentTurn, AgentToolCall | None, str | None, str | None]:
        run = await self.session.get(AgentRun, run_id)
        assert run is not None
        turn = await self.session.scalar(
            select(AgentTurn).where(
                AgentTurn.agent_run_id == run_id,
                AgentTurn.turn_number == run.current_turn_number,
            )
        )
        assert turn is not None
        tool = await self.session.scalar(
            select(AgentToolCall).where(AgentToolCall.agent_turn_id == turn.id)
        )
        return run.status, turn, tool, run.final_response, run.error_code

    async def start_model_call(
        self, run_id: UUID, max_attempts: int
    ) -> tuple[UUID, dict[str, Any], int] | None:
        exhausted_agent_type: str | None = None
        started: tuple[UUID, dict[str, Any], int] | None = None
        async with self.session.begin():
            await self._fence()
            run = await self.session.scalar(
                select(AgentRun).where(AgentRun.id == run_id).with_for_update()
            )
            assert run is not None
            turn = await self.session.scalar(
                select(AgentTurn)
                .where(
                    AgentTurn.agent_run_id == run_id,
                    AgentTurn.turn_number == run.current_turn_number,
                )
                .with_for_update()
            )
            assert turn is not None
            if run.status != AgentRunStatus.RUNNING or turn.status != AgentTurnStatus.PENDING_MODEL:
                return None
            calls = list(
                await self.session.scalars(
                    select(ModelCall)
                    .where(ModelCall.agent_turn_id == turn.id)
                    .order_by(ModelCall.attempt_number)
                    .with_for_update()
                )
            )
            for previous in calls:
                if previous.status == ModelCallStatus.RUNNING:
                    self._model_status(previous, ModelCallStatus.FAILED)
                    previous.error_code = "INTERRUPTED"
                    previous.error_detail = "Executor lost before model decision was persisted"
                    previous.completed_at = datetime.now(UTC)
            if len(calls) >= max_attempts:
                self._turn_status(turn, AgentTurnStatus.FAILED)
                self._run_status(run, AgentRunStatus.FAILED)
                run.error_code = "MODEL_ATTEMPTS_EXHAUSTED"
                run.error_detail = "No valid structured decision within bounded model attempts"
                run.completed_at = datetime.now(UTC)
                exhausted_agent_type = run.agent_type
            else:
                request = dict(turn.model_request)
                invalid = next(
                    (
                        item
                        for item in reversed(calls)
                        if item.error_code in {"ValidationError", "ValueError"}
                    ),
                    None,
                )
                if invalid is not None:
                    request["messages"] = [
                        *request["messages"],
                        {
                            "role": "user",
                            "content": "Previous JSON decision was rejected. Correct it. "
                            f"Validation error: {invalid.error_detail}",
                        },
                    ]
                call = ModelCall(
                    id=uuid4(),
                    agent_turn_id=turn.id,
                    attempt_number=len(calls) + 1,
                    status=ModelCallStatus.RUNNING,
                    provider=run.provider,
                    model=run.model,
                    request=request,
                    request_hash=canonical_hash(request),
                    started_at=datetime.now(UTC),
                )
                self.session.add(call)
                started = call.id, request, call.attempt_number
        if exhausted_agent_type is not None:
            count("continuum_agent_runs", status="failed", agent_type=exhausted_agent_type)
        return started

    async def fail_model_call(
        self, call_id: UUID, code: str, detail: str, raw: dict[str, Any] | None = None
    ) -> None:
        async with self.session.begin():
            await self._fence()
            call = await self.session.scalar(
                select(ModelCall).where(ModelCall.id == call_id).with_for_update()
            )
            assert call is not None
            self._model_status(call, ModelCallStatus.FAILED)
            call.error_code = code
            call.error_detail = detail[:2000]
            call.raw_response = raw
            call.completed_at = datetime.now(UTC)

    async def fail_model_permanently(
        self, run_id: UUID, call_id: UUID, code: str, detail: str
    ) -> None:
        async with self.session.begin():
            await self._fence()
            run = await self.session.scalar(
                select(AgentRun).where(AgentRun.id == run_id).with_for_update()
            )
            assert run is not None
            turn = await self.session.scalar(
                select(AgentTurn)
                .where(
                    AgentTurn.agent_run_id == run_id,
                    AgentTurn.turn_number == run.current_turn_number,
                )
                .with_for_update()
            )
            assert turn is not None
            call = await self.session.scalar(
                select(ModelCall).where(ModelCall.id == call_id).with_for_update()
            )
            assert call is not None
            self._model_status(call, ModelCallStatus.FAILED)
            call.error_code = code
            call.error_detail = detail[:2000]
            call.completed_at = datetime.now(UTC)
            self._turn_status(turn, AgentTurnStatus.FAILED)
            self._run_status(run, AgentRunStatus.FAILED)
            run.error_code = code
            run.error_detail = detail[:2000]
            run.completed_at = datetime.now(UTC)
        count("continuum_agent_runs", status="failed", agent_type=run.agent_type)

    async def persist_decision(
        self,
        run_id: UUID,
        call_id: UUID,
        decision: ToolDecision | FinalDecision,
        result: ModelResult,
        latency_ms: int,
    ) -> None:
        async with self.session.begin():
            await self._fence()
            run = await self.session.scalar(
                select(AgentRun).where(AgentRun.id == run_id).with_for_update()
            )
            assert run is not None
            turn = await self.session.scalar(
                select(AgentTurn)
                .where(
                    AgentTurn.agent_run_id == run_id,
                    AgentTurn.turn_number == run.current_turn_number,
                )
                .with_for_update()
            )
            assert turn is not None
            if turn.decision is not None or turn.status != AgentTurnStatus.PENDING_MODEL:
                raise LostLease("Agent decision has already been materialized")
            call = await self.session.scalar(
                select(ModelCall).where(ModelCall.id == call_id).with_for_update()
            )
            assert call is not None and call.status == ModelCallStatus.RUNNING
            self._model_status(call, ModelCallStatus.SUCCEEDED)
            call.raw_response = result.raw
            call.validated_decision = decision.model_dump(mode="json")
            call.prompt_tokens = result.prompt_tokens
            call.completion_tokens = result.completion_tokens
            if result.prompt_tokens is not None and result.completion_tokens is not None:
                call.total_tokens = result.prompt_tokens + result.completion_tokens
            call.latency_ms = latency_ms
            call.completed_at = datetime.now(UTC)
            turn.decision = decision.model_dump(mode="json")
            turn.decision_type = decision.type
            if isinstance(decision, FinalDecision):
                self._turn_status(turn, AgentTurnStatus.COMPLETED)
                turn.final_response = decision.response
                turn.completed_at = datetime.now(UTC)
                self._run_status(run, AgentRunStatus.SUCCEEDED)
                run.final_response = decision.response
                run.completed_at = datetime.now(UTC)
            else:
                if run.agent_type == "coding_agent":
                    validate_coding_arguments(decision.tool_name, decision.arguments)
                else:
                    validate_tool_arguments(decision.tool_name, decision.arguments)
                arguments = decision.arguments
                tool_id = uuid4()
                if run.agent_type == "coding_agent":
                    semantics = CODING_TOOL_SEMANTICS[decision.tool_name]
                else:
                    semantics = (
                        RetrySafety.READ_ONLY
                        if decision.tool_name == "read_refund_policy"
                        else RetrySafety.IDEMPOTENCY_KEY_SUPPORTED
                    )
                self.session.add(
                    AgentToolCall(
                        id=tool_id,
                        agent_run_id=run_id,
                        agent_turn_id=turn.id,
                        tool_name=decision.tool_name,
                        arguments=arguments,
                        arguments_hash=canonical_hash(arguments),
                        status=AgentToolCallStatus.PENDING,
                        operation_id=agent_operation_id(tool_id),
                        tool_semantics=semantics.value,
                    )
                )
                self._turn_status(turn, AgentTurnStatus.TOOL_PENDING)
        if isinstance(decision, FinalDecision):
            count("continuum_agent_runs", status="succeeded", agent_type=run.agent_type)

    async def persist_tool_result(
        self, run_id: UUID, tool_id: UUID, result: dict[str, Any]
    ) -> None:
        max_turns_agent_type: str | None = None
        async with self.session.begin():
            await self._fence()
            run = await self.session.scalar(
                select(AgentRun).where(AgentRun.id == run_id).with_for_update()
            )
            assert run is not None
            tool = await self.session.scalar(
                select(AgentToolCall).where(AgentToolCall.id == tool_id).with_for_update()
            )
            assert tool is not None and tool.agent_run_id == run_id
            if tool.status == AgentToolCallStatus.SUCCEEDED:
                return
            if tool.status != AgentToolCallStatus.PENDING:
                raise LostLease("Agent tool is no longer pending")
            turn = await self.session.scalar(
                select(AgentTurn)
                .where(
                    AgentTurn.agent_run_id == run_id,
                    AgentTurn.turn_number == run.current_turn_number,
                )
                .with_for_update()
            )
            assert turn is not None
            assert tool.agent_turn_id == turn.id
            self._tool_status(tool, AgentToolCallStatus.SUCCEEDED)
            tool.result = result
            tool.completed_at = datetime.now(UTC)
            self._turn_status(turn, AgentTurnStatus.COMPLETED)
            turn.completed_at = datetime.now(UTC)
            if turn.turn_number >= run.max_turns:
                self._run_status(run, AgentRunStatus.FAILED)
                run.error_code = "MAX_AGENT_TURNS_EXCEEDED"
                run.error_detail = "Model did not return a final response by the turn limit"
                run.completed_at = datetime.now(UTC)
                max_turns_agent_type = run.agent_type
            else:
                prior_turns = list(
                    await self.session.scalars(
                        select(AgentTurn)
                        .where(AgentTurn.agent_run_id == run_id)
                        .order_by(AgentTurn.turn_number)
                    )
                )
                history: list[dict[str, Any]] = []
                for prior in prior_turns:
                    prior_tool = await self.session.scalar(
                        select(AgentToolCall).where(AgentToolCall.agent_turn_id == prior.id)
                    )
                    history.append(
                        {
                            "decision": prior.decision,
                            "tool_result": prior_tool.result if prior_tool else None,
                        }
                    )
                step = await self.session.get(WorkflowStep, run.step_id)
                assert step is not None
                next_number = turn.turn_number + 1
                request = self._request(step.input, history, run.system_prompt_version)
                self.session.add(self._new_turn(run_id, next_number, request))
                run.current_turn_number = next_number
        if max_turns_agent_type is not None:
            count("continuum_agent_runs", status="failed", agent_type=max_turns_agent_type)

    async def begin_tool(self, tool_id: UUID) -> None:
        async with self.session.begin():
            await self._fence()
            tool = await self.session.scalar(
                select(AgentToolCall).where(AgentToolCall.id == tool_id).with_for_update()
            )
            assert tool is not None
            turn = await self.session.get(AgentTurn, tool.agent_turn_id)
            if (
                turn is None
                or turn.decision is None
                or tool.agent_run_id != turn.agent_run_id
                or tool.tool_name != turn.decision.get("tool_name")
                or tool.arguments != turn.decision.get("arguments")
                or tool.arguments_hash != canonical_hash(tool.arguments)
                or tool.operation_id != agent_operation_id(tool.id)
            ):
                raise PermanentToolError(
                    "AGENT_TOOL_INVARIANT", "Durable tool call differs from accepted decision"
                )
            if tool.status != AgentToolCallStatus.PENDING:
                raise LostLease("Agent tool is no longer pending")
            if tool.started_at is None:
                tool.started_at = datetime.now(UTC)

    async def fail_tool(self, run_id: UUID, tool_id: UUID, code: str, detail: str) -> None:
        async with self.session.begin():
            await self._fence()
            run = await self.session.scalar(
                select(AgentRun).where(AgentRun.id == run_id).with_for_update()
            )
            assert run is not None
            tool = await self.session.scalar(
                select(AgentToolCall).where(AgentToolCall.id == tool_id).with_for_update()
            )
            assert tool is not None
            turn = await self.session.get(AgentTurn, tool.agent_turn_id)
            assert turn is not None
            self._tool_status(tool, AgentToolCallStatus.FAILED)
            tool.error_code = code
            tool.error_detail = detail[:2000]
            tool.completed_at = datetime.now(UTC)
            self._turn_status(turn, AgentTurnStatus.FAILED)
            self._run_status(run, AgentRunStatus.FAILED)
            run.error_code = code
            run.error_detail = detail[:2000]
            run.completed_at = datetime.now(UTC)
        count("continuum_agent_runs", status="failed", agent_type=run.agent_type)

    @staticmethod
    def _request(
        step_input: dict[str, Any], history: list[dict[str, Any]], prompt_version: str
    ) -> dict[str, Any]:
        keys = (
            ("repository", "task", "test_command")
            if prompt_version.startswith("coding-")
            else ("customer_id", "amount", "request")
        )
        clean_input = {key: step_input[key] for key in keys}
        messages: list[dict[str, str]] = [
            {"role": "system", "content": prompt_for(prompt_version)},
            {"role": "user", "content": json.dumps(clean_input, sort_keys=True)},
        ]
        for item in history:
            messages.append(
                {"role": "assistant", "content": json.dumps(item["decision"], sort_keys=True)}
            )
            if item["tool_result"] is not None:
                messages.append(
                    {
                        "role": "user",
                        "content": "Tool result: "
                        + json.dumps(item["tool_result"], sort_keys=True),
                    }
                )
        return {
            **clean_input,
            "messages": messages,
            "history": history,
            "system_prompt_version": prompt_version,
        }

    @staticmethod
    def _new_turn(run_id: UUID, number: int, request: dict[str, Any]) -> AgentTurn:
        return AgentTurn(
            id=uuid4(),
            agent_run_id=run_id,
            turn_number=number,
            status=AgentTurnStatus.PENDING_MODEL,
            model_request=request,
            model_request_hash=canonical_hash(request),
        )
