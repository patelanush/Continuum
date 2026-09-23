"""Crash-resumable coding agent over one persistent volume and disposable sandboxes."""

import asyncio
import logging
import os
from time import monotonic
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from durable_agent_runtime.agent.decisions import DECISION_ADAPTER, ToolDecision
from durable_agent_runtime.agent.providers import (
    FakeModelProvider,
    ModelProvider,
    OllamaModelProvider,
    ProviderConfigurationError,
    ProviderUnavailable,
)
from durable_agent_runtime.agent.service import AgentService
from durable_agent_runtime.coding.decisions import validate_coding_arguments
from durable_agent_runtime.coding.fake import fixture_script
from durable_agent_runtime.coding.sandbox import (
    DockerSandbox,
    SandboxOperationRejected,
    SandboxUnavailable,
)
from durable_agent_runtime.coding.service import CodingService
from durable_agent_runtime.core.config import Settings
from durable_agent_runtime.db.models import AgentRun, ApprovalRequest, CodingWorkspace
from durable_agent_runtime.domain.enums import (
    AgentRunStatus,
    AgentTurnStatus,
    ApprovalStatus,
    WorkspaceStatus,
)
from durable_agent_runtime.execution.tools import (
    ExecutionContext,
    PermanentToolError,
    TransientToolError,
)

logger = logging.getLogger(__name__)


class CodingInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    repository: Literal["fixture:discount_service"]
    task: str = Field(min_length=1, max_length=2000)
    test_command: Literal["pytest -q"]
    provider: Literal["fake", "ollama"] | None = None
    fake_script: dict[str, list[dict[str, Any]]] | None = None


def coding_provider(command: CodingInput, settings: Settings) -> tuple[str, str, ModelProvider]:
    name = command.provider or settings.agent_provider
    if name == "fake":
        if settings.app_env not in {"development", "test", "faultlab"}:
            raise PermanentToolError("INVALID_AGENT_PROVIDER", "Fake provider is test-only")
        return (
            name,
            "scripted-coding-v1",
            FakeModelProvider(command.fake_script or fixture_script()),
        )
    if name == "ollama":
        return (
            name,
            settings.ollama_model,
            OllamaModelProvider(
                settings.ollama_base_url, settings.ollama_model, settings.model_timeout_seconds
            ),
        )
    raise PermanentToolError("INVALID_AGENT_PROVIDER", "Unsupported coding provider")


async def faultlab_pause(settings: Settings, name: str) -> None:
    if settings.app_env == "faultlab" and os.getenv(name) == "1":
        logger.warning("process_type=coding_agent operation=faultlab_pause point=%s", name)
        await asyncio.Event().wait()


async def run_coding_agent(
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
        command = CodingInput.model_validate(raw_input)
    except ValidationError as exc:
        raise PermanentToolError("INVALID_CODING_INPUT", str(exc)) from exc
    provider_name, model_name, provider = coding_provider(command, settings)
    if provider_override is not None:
        provider = provider_override
    async with sessions() as session:
        run_id = await AgentService(session, context, executor_id, lease_token).ensure_run(
            provider=provider_name,
            model=model_name,
            max_turns=settings.coding_agent_max_turns,
            agent_type="coding_agent",
            prompt_version="coding-v2",
        )
    async with sessions() as session:
        persisted = await session.get(AgentRun, run_id)
        assert persisted is not None
        if persisted.agent_type != "coding_agent":
            raise PermanentToolError("AGENT_TYPE_MISMATCH", "Persisted agent type changed")
        if persisted.provider != provider_name or persisted.model != model_name:
            if persisted.provider == "fake":
                provider = FakeModelProvider(command.fake_script or fixture_script())
            elif persisted.provider == "ollama":
                provider = OllamaModelProvider(
                    settings.ollama_base_url, persisted.model, settings.model_timeout_seconds
                )
            else:
                raise PermanentToolError("INVALID_AGENT_PROVIDER", "Persisted provider unsupported")
        if provider_override is not None:
            provider = provider_override
    async with sessions() as session:
        workspace_id = await CodingService(
            session, context, executor_id, lease_token
        ).ensure_workspace(run_id, command.repository, ["pytest", "-q"], settings)
    async with sessions() as session:
        workspace = await session.get(CodingWorkspace, workspace_id)
        assert workspace is not None
        volume_name = workspace.volume_name
        initialize_if_missing = workspace.status == WorkspaceStatus.PENDING
    async with sessions() as session:
        sandbox_id = await CodingService(session, context, executor_id, lease_token).sandbox_intent(
            workspace_id, settings
        )
    sandbox = DockerSandbox(workspace_id, sandbox_id, volume_name, settings)
    started = False
    try:
        try:
            await sandbox.start()
            started = True
            async with sessions() as session:
                await CodingService(session, context, executor_id, lease_token).sandbox_started(
                    sandbox_id
                )
            prepared = await sandbox.call(
                "prepare",
                {
                    "repository": command.repository,
                    "initialize_if_missing": initialize_if_missing,
                },
            )
            async with sessions() as session:
                await CodingService(session, context, executor_id, lease_token).record_prepared(
                    workspace_id, prepared
                )
            async with sessions() as session:
                workspace = await CodingService(
                    session, context, executor_id, lease_token
                ).workspace(workspace_id)
                _status, _turn, pending_tool, _response, _error = await AgentService(
                    session, context, executor_id, lease_token
                ).snapshot(run_id)
                pending_patch = (
                    _turn.status == AgentTurnStatus.TOOL_PENDING
                    and pending_tool is not None
                    and pending_tool.tool_name == "apply_patch"
                )
                approval = await session.scalar(
                    select(ApprovalRequest).where(ApprovalRequest.workspace_id == workspace_id)
                )
                committed_before_response = (
                    workspace.status == WorkspaceStatus.WAITING_APPROVAL
                    and approval is not None
                    and approval.status == ApprovalStatus.APPROVED
                    and prepared["head_operation_id"] == approval.operation_id
                )
                if (
                    workspace.current_git_head != prepared["git_head"]
                    and not committed_before_response
                ):
                    raise PermanentToolError(
                        "WORKSPACE_RECONCILIATION_FAILED",
                        "Git HEAD differs from durable checkpoint",
                    )
                if workspace.tree_hash != prepared["tree_hash"] and not pending_patch:
                    raise PermanentToolError(
                        "WORKSPACE_RECONCILIATION_FAILED",
                        "Actual workspace differs from durable checkpoint",
                    )
        except SandboxOperationRejected as exc:
            raise PermanentToolError("WORKSPACE_RECONCILIATION_FAILED", str(exc)) from exc
        except (SandboxUnavailable, TimeoutError) as exc:
            raise TransientToolError("Sandbox preparation unavailable") from exc
        while True:
            async with sessions() as session:
                run_status, turn, tool, final_response, error_code = await AgentService(
                    session, context, executor_id, lease_token
                ).snapshot(run_id)
                turn_number, turn_status = turn.turn_number, turn.status
                tool_id = tool.id if tool else None
                tool_name = tool.tool_name if tool else None
                tool_arguments = tool.arguments if tool else None
                operation_id = tool.operation_id if tool else None
            if run_status == AgentRunStatus.SUCCEEDED:
                assert final_response is not None
                break
            if run_status in {AgentRunStatus.FAILED, AgentRunStatus.CANCELLED}:
                raise PermanentToolError(error_code or "AGENT_FAILED", "Coding agent failed")
            if turn_status == AgentTurnStatus.PENDING_MODEL:
                async with sessions() as session:
                    started_call = await AgentService(
                        session, context, executor_id, lease_token
                    ).start_model_call(run_id, settings.model_max_attempts)
                if started_call is None:
                    continue
                call_id, request, model_attempt = started_call
                raw_response: dict[str, Any] | None = None
                model_started = monotonic()
                try:
                    result = await asyncio.wait_for(
                        provider.generate(
                            request, turn_number=turn_number, attempt_number=model_attempt
                        ),
                        timeout=settings.model_timeout_seconds,
                    )
                    raw_response = result.raw
                    decision = DECISION_ADAPTER.validate_python(result.raw)
                    if isinstance(decision, ToolDecision):
                        validate_coding_arguments(decision.tool_name, decision.arguments)
                except ProviderConfigurationError as exc:
                    async with sessions() as session:
                        await AgentService(
                            session, context, executor_id, lease_token
                        ).fail_model_permanently(
                            run_id, call_id, "MODEL_CONFIGURATION_ERROR", str(exc)
                        )
                    continue
                except (
                    TimeoutError,
                    ProviderUnavailable,
                    ValidationError,
                    ValueError,
                    KeyError,
                ) as exc:
                    async with sessions() as session:
                        await AgentService(
                            session, context, executor_id, lease_token
                        ).fail_model_call(call_id, type(exc).__name__, str(exc), raw_response)
                    continue
                async with sessions() as session:
                    await AgentService(session, context, executor_id, lease_token).persist_decision(
                        run_id,
                        call_id,
                        decision,
                        result,
                        int((monotonic() - model_started) * 1000),
                    )
                if isinstance(decision, ToolDecision) and decision.tool_name == "apply_patch":
                    await faultlab_pause(settings, "FAULTLAB_CODING_PAUSE_AFTER_DECISION")
                continue
            if turn_status == AgentTurnStatus.TOOL_PENDING:
                assert tool_id is not None and tool_name is not None
                assert tool_arguments is not None and operation_id is not None
                async with sessions() as session:
                    await AgentService(session, context, executor_id, lease_token).begin_tool(
                        tool_id
                    )
                async with sessions() as session:
                    workspace = await CodingService(
                        session, context, executor_id, lease_token
                    ).workspace(workspace_id)
                    before_hashes = workspace.file_hashes
                    expected_tree_hash = workspace.tree_hash
                assert before_hashes is not None and expected_tree_hash is not None
                try:
                    if tool_name != "apply_patch":
                        actual = await sandbox.call("fingerprint", {})
                        if actual["tree_hash"] != expected_tree_hash:
                            raise SandboxOperationRejected(
                                "WORKSPACE_RECONCILIATION_FAILED: unexpected workspace state"
                            )
                    arguments = validate_coding_arguments(tool_name, tool_arguments)
                    if tool_name == "apply_patch":
                        arguments["before_file_hashes"] = before_hashes
                    if tool_name == "run_tests":
                        arguments["timeout_seconds"] = settings.coding_command_timeout_seconds
                    async with sessions() as session:
                        command_id = await CodingService(
                            session, context, executor_id, lease_token
                        ).begin_command(
                            workspace_id,
                            tool_id,
                            tool_name,
                            (settings.coding_command_timeout_seconds + 10) * 1000,
                        )
                    tool_result = await sandbox.call(
                        tool_name,
                        arguments,
                        timeout_seconds=settings.coding_command_timeout_seconds + 10,
                    )
                    if tool_name == "apply_patch":
                        await faultlab_pause(settings, "FAULTLAB_CODING_PAUSE_AFTER_PATCH")
                    if tool_name == "run_tests":
                        await faultlab_pause(settings, "FAULTLAB_CODING_PAUSE_AFTER_TESTS")
                    async with sessions() as session:
                        await CodingService(
                            session, context, executor_id, lease_token
                        ).finish_command(command_id, tool_result, operation_id=operation_id)
                    async with sessions() as session:
                        await AgentService(
                            session, context, executor_id, lease_token
                        ).persist_tool_result(run_id, tool_id, tool_result)
                    continue
                except SandboxOperationRejected as exc:
                    code = (
                        "WORKSPACE_RECONCILIATION_FAILED"
                        if "WORKSPACE_RECONCILIATION_FAILED" in str(exc)
                        else "CODING_TOOL_REJECTED"
                    )
                    async with sessions() as session:
                        await AgentService(session, context, executor_id, lease_token).fail_tool(
                            run_id, tool_id, code, str(exc)
                        )
                    raise PermanentToolError(code, str(exc)) from exc
                except (SandboxUnavailable, TimeoutError) as exc:
                    raise TransientToolError("Sandbox tool unavailable") from exc
            raise PermanentToolError("AGENT_STATE_INVALID", f"Turn is {turn_status}")

        async with sessions() as session:
            approval_id = await CodingService(
                session, context, executor_id, lease_token
            ).request_approval(workspace_id, final_response)
        await faultlab_pause(settings, "FAULTLAB_CODING_PAUSE_BEFORE_APPROVAL")
        while True:
            async with sessions() as session:
                approval = await session.get(ApprovalRequest, approval_id)
                assert approval is not None
                approval_status = approval.status
                operation_id = approval.operation_id
            if approval_status == ApprovalStatus.APPROVED:
                break
            if approval_status in {ApprovalStatus.REJECTED, ApprovalStatus.CANCELLED}:
                raise PermanentToolError("APPROVAL_REJECTED", "Local Git commit was not approved")
            await asyncio.sleep(0.5)
        async with sessions() as session:
            workspace = await CodingService(session, context, executor_id, lease_token).workspace(
                workspace_id
            )
            expected_tree_hash = workspace.tree_hash
            expected_git_head = workspace.current_git_head
        assert expected_tree_hash is not None and expected_git_head is not None
        await faultlab_pause(settings, "FAULTLAB_CODING_PAUSE_AFTER_APPROVAL")
        async with sessions() as session:
            commit_command_id = await CodingService(
                session, context, executor_id, lease_token
            ).begin_command(workspace_id, None, "git_commit", 35_000)
        try:
            commit_result = await sandbox.call(
                "git_commit",
                {
                    "operation_id": operation_id,
                    "expected_tree_hash": expected_tree_hash,
                    "expected_git_head": expected_git_head,
                    "message": "Fix checkout discount calculation",
                },
            )
        except SandboxOperationRejected as exc:
            raise PermanentToolError("COMMIT_RECONCILIATION_FAILED", str(exc)) from exc
        except (SandboxUnavailable, TimeoutError) as exc:
            raise TransientToolError("Sandbox commit unavailable") from exc
        await faultlab_pause(settings, "FAULTLAB_CODING_PAUSE_AFTER_COMMIT")
        async with sessions() as session:
            await CodingService(session, context, executor_id, lease_token).finish_command(
                commit_command_id, commit_result
            )
        async with sessions() as session:
            await CodingService(session, context, executor_id, lease_token).finalize_commit(
                workspace_id, approval_id, commit_result
            )
        return {
            "final_response": final_response,
            "agent_run_id": str(run_id),
            "workspace_id": str(workspace_id),
            "approval_id": str(approval_id),
            "commit_sha": commit_result["commit_sha"],
            "turn_count": turn_number,
        }
    except PermanentToolError:
        async with sessions() as session:
            await CodingService(session, context, executor_id, lease_token).mark_failed(
                workspace_id
            )
        raise
    finally:
        if started:
            await sandbox.stop()
        async with sessions() as session:
            await CodingService(session, context, executor_id, lease_token).sandbox_stopped(
                sandbox_id, "executor_exit"
            )
