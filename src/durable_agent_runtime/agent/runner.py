"""Resume a durable agent inside a leased outer execution attempt."""

import asyncio
import logging
import os
from decimal import Decimal
from time import monotonic
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from durable_agent_runtime.agent.decisions import (
    DECISION_ADAPTER,
    FinalDecision,
    ToolDecision,
    validate_tool_arguments,
)
from durable_agent_runtime.agent.providers import (
    FakeModelProvider,
    ModelProvider,
    OllamaModelProvider,
    ProviderConfigurationError,
    ProviderUnavailable,
)
from durable_agent_runtime.agent.service import AgentService
from durable_agent_runtime.agent.tools import execute_agent_tool
from durable_agent_runtime.core.config import Settings
from durable_agent_runtime.db.models import AgentRun
from durable_agent_runtime.domain.enums import AgentRunStatus, AgentTurnStatus
from durable_agent_runtime.execution.tools import (
    ExecutionContext,
    PermanentToolError,
    TransientToolError,
)
from durable_agent_runtime.observability.operations import model_call, model_tokens, tool_call
from durable_agent_runtime.observability.runtime import span

logger = logging.getLogger(__name__)


class SupportInput(BaseModel):
    model_config = ConfigDict(extra="ignore")
    customer_id: str = Field(min_length=1)
    amount: Decimal = Field(gt=0, max_digits=12, decimal_places=2)
    request: str = Field(min_length=1)
    provider: str | None = None
    fake_script: dict[str, list[dict[str, Any]]] | None = None
    faultlab_refund_delay_ms: int = Field(default=0, ge=0, le=30_000)


def select_provider(step_input: SupportInput, settings: Settings) -> tuple[str, str, ModelProvider]:
    name = step_input.provider or settings.agent_provider
    if name == "fake":
        if settings.app_env not in {"faultlab", "test", "development"}:
            raise PermanentToolError(
                "INVALID_AGENT_PROVIDER", "Fake provider is test/development only"
            )
        return name, "scripted-fake-v1", FakeModelProvider(step_input.fake_script)
    if name == "ollama":
        return (
            name,
            settings.ollama_model,
            OllamaModelProvider(
                settings.ollama_base_url, settings.ollama_model, settings.model_timeout_seconds
            ),
        )
    raise PermanentToolError("INVALID_AGENT_PROVIDER", f"Unsupported provider: {name}")


async def _faultlab_pause(settings: Settings, name: str) -> None:
    if settings.app_env == "faultlab" and os.getenv(name) == "1":
        logger.warning("process_type=agent operation=faultlab_pause point=%s", name)
        await asyncio.Event().wait()


async def run_support_agent(
    raw_input: dict[str, Any],
    context: ExecutionContext,
    *,
    executor_id: str,
    lease_token: UUID,
    sessions: async_sessionmaker[AsyncSession],
    settings: Settings,
    provider_override: ModelProvider | None = None,
) -> dict[str, Any]:
    return await _run_support_agent(
        raw_input,
        context,
        executor_id=executor_id,
        lease_token=lease_token,
        sessions=sessions,
        settings=settings,
        provider_override=provider_override,
    )


async def _run_support_agent(
    raw_input: dict[str, Any],
    context: ExecutionContext,
    *,
    executor_id: str,
    lease_token: UUID,
    sessions: async_sessionmaker[AsyncSession],
    settings: Settings,
    provider_override: ModelProvider | None = None,
) -> dict[str, Any]:
    try:
        command = SupportInput.model_validate(raw_input)
    except ValidationError as exc:
        raise PermanentToolError("INVALID_AGENT_INPUT", str(exc)) from exc
    provider_name, model_name, provider = select_provider(command, settings)
    if provider_override is not None:
        provider = provider_override
    async with sessions() as session:
        run_id = await AgentService(session, context, executor_id, lease_token).ensure_run(
            provider=provider_name, model=model_name, max_turns=settings.agent_max_turns
        )
    with span(
        "agent.run",
        {
            "continuum.workflow.id": str(context.workflow_id),
            "continuum.step.id": str(context.step_id),
            "continuum.execution_attempt.id": str(context.attempt_id),
            "continuum.execution_attempt.number": context.attempt_number,
            "continuum.agent_run.id": str(run_id),
        },
    ):
        pass
    async with sessions() as session:
        persisted = await session.get(AgentRun, run_id)
        assert persisted is not None
        if persisted.provider != provider_name or persisted.model != model_name:
            if persisted.provider == "fake":
                provider = FakeModelProvider(command.fake_script)
            elif persisted.provider == "ollama":
                provider = OllamaModelProvider(
                    settings.ollama_base_url, persisted.model, settings.model_timeout_seconds
                )
            else:
                raise PermanentToolError(
                    "INVALID_AGENT_PROVIDER", "Persisted provider is unsupported"
                )
        if provider_override is not None:
            provider = provider_override
    while True:
        async with sessions() as session:
            run_status, turn, tool, final_response, error_code = await AgentService(
                session, context, executor_id, lease_token
            ).snapshot(run_id)
            turn_id, turn_number, turn_status = turn.id, turn.turn_number, turn.status
            tool_id = tool.id if tool else None
            tool_name = tool.tool_name if tool else None
            tool_arguments = tool.arguments if tool else None
            operation_id = tool.operation_id if tool else None
        if run_status == AgentRunStatus.SUCCEEDED:
            assert final_response is not None
            return {
                "final_response": final_response,
                "agent_run_id": str(run_id),
                "turn_count": turn_number,
            }
        if run_status in {AgentRunStatus.FAILED, AgentRunStatus.CANCELLED}:
            raise PermanentToolError(
                error_code or "AGENT_FAILED", f"Agent run {run_id} is {run_status}"
            )
        if turn_status == AgentTurnStatus.PENDING_MODEL:
            async with sessions() as session:
                started = await AgentService(
                    session, context, executor_id, lease_token
                ).start_model_call(run_id, settings.model_max_attempts)
            if started is None:
                continue
            call_id, request, model_attempt = started
            began = monotonic()
            raw_response: dict[str, Any] | None = None
            try:
                with span(
                    "agent.turn",
                    {
                        "continuum.agent_run.id": str(run_id),
                        "continuum.agent_turn.id": str(turn_id),
                    },
                ):
                    with model_call(call_id, provider_name, model_name, model_attempt) as active:
                        result = await asyncio.wait_for(
                            provider.generate(
                                request, turn_number=turn_number, attempt_number=model_attempt
                            ),
                            timeout=settings.model_timeout_seconds,
                        )
                        raw_response = result.raw
                        model_tokens(
                            active,
                            provider_name,
                            model_name,
                            result.prompt_tokens,
                            result.completion_tokens,
                        )
                        decision = DECISION_ADAPTER.validate_python(result.raw)
                        if isinstance(decision, ToolDecision):
                            validate_tool_arguments(decision.tool_name, decision.arguments)
            except ProviderConfigurationError as exc:
                async with sessions() as session:
                    await AgentService(
                        session, context, executor_id, lease_token
                    ).fail_model_permanently(run_id, call_id, "MODEL_CONFIGURATION_ERROR", str(exc))
                continue
            except (
                TimeoutError,
                ProviderUnavailable,
                ValidationError,
                ValueError,
                KeyError,
            ) as exc:
                async with sessions() as session:
                    await AgentService(session, context, executor_id, lease_token).fail_model_call(
                        call_id,
                        type(exc).__name__,
                        str(exc),
                        raw_response,
                    )
                logger.warning(
                    "process_type=agent operation=model_call_failed agent_run_id=%s "
                    "turn_number=%s model_attempt=%s error=%s",
                    run_id,
                    turn_number,
                    model_attempt,
                    type(exc).__name__,
                )
                continue
            async with sessions() as session:
                await AgentService(session, context, executor_id, lease_token).persist_decision(
                    run_id, call_id, decision, result, int((monotonic() - began) * 1000)
                )
            if isinstance(decision, ToolDecision) and decision.tool_name == "refund_customer":
                await _faultlab_pause(settings, "FAULTLAB_AGENT_PAUSE_AFTER_DECISION")
            if isinstance(decision, FinalDecision):
                await _faultlab_pause(settings, "FAULTLAB_AGENT_PAUSE_AFTER_FINAL")
            continue
        if turn_status == AgentTurnStatus.TOOL_PENDING:
            assert tool_id is not None and tool_name is not None
            assert tool_arguments is not None and operation_id is not None
            try:
                async with sessions() as session:
                    await AgentService(session, context, executor_id, lease_token).begin_tool(
                        tool_id
                    )
                with span(
                    "agent.turn",
                    {
                        "continuum.agent_run.id": str(run_id),
                        "continuum.agent_turn.id": str(turn_id),
                    },
                ):
                    with tool_call(tool_id, tool_name, "idempotent"):
                        tool_result = await execute_agent_tool(
                            tool_name,
                            tool_arguments,
                            operation_id,
                            payments_url=settings.mock_payments_url,
                            faultlab_delay_ms=(
                                command.faultlab_refund_delay_ms
                                if settings.app_env == "faultlab"
                                else 0
                            ),
                        )
            except PermanentToolError as exc:
                async with sessions() as session:
                    await AgentService(session, context, executor_id, lease_token).fail_tool(
                        run_id, tool_id, exc.code, str(exc)
                    )
                raise
            except TransientToolError:
                raise
            async with sessions() as session:
                await AgentService(session, context, executor_id, lease_token).persist_tool_result(
                    run_id, tool_id, tool_result
                )
            if tool_name == "refund_customer":
                await _faultlab_pause(settings, "FAULTLAB_AGENT_PAUSE_AFTER_TOOL_RESULT")
            continue
        if turn_status == AgentTurnStatus.COMPLETED:
            raise PermanentToolError(
                "AGENT_STATE_INVALID", "Running agent points at a completed turn"
            )
        raise PermanentToolError("AGENT_STATE_INVALID", f"Turn {turn_id} is {turn_status}")
