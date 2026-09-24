"""PostgreSQL checkpoints plus real Docker coding sandbox and explicit approval."""

import asyncio
import hashlib
import json
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select

from durable_agent_runtime.coding.fake import fixture_script
from durable_agent_runtime.coding.sandbox import DockerSandbox, docker
from durable_agent_runtime.coding.service import ApprovalService
from durable_agent_runtime.core.config import Settings
from durable_agent_runtime.db.models import (
    AgentRun,
    AgentToolCall,
    ApprovalRequest,
    CodingWorkspace,
    ModelCall,
    SandboxCommand,
    SandboxExecution,
    WorkspaceCheckpoint,
)
from durable_agent_runtime.domain.enums import (
    ApprovalStatus,
    CommandStatus,
    WorkflowStatus,
    WorkspaceStatus,
)
from durable_agent_runtime.execution.executor import execute_attempt
from durable_agent_runtime.services.workflows import WorkflowService
from tests.conftest import TestSession
from tests.integration.test_agent import expire_and_replace
from tests.integration.test_execution import claim, create_scheduled

pytestmark = pytest.mark.integration
FIXTURE_TEST_SOURCE = (
    Path(__file__).resolve().parents[2] / "fixtures/coding/discount_service/tests/test_pricing.py"
).read_bytes()


def settings() -> Settings:
    return Settings(
        app_env="test",
        agent_provider="fake",
        sandbox_volume_prefix="continuum-test",
        executor_lease_seconds=60,
        executor_heartbeat_seconds=3,
        coding_command_timeout_seconds=15,
    )


def coding_input() -> dict[str, object]:
    return {
        "repository": "fixture:discount_service",
        "task": "Fix the discount calculation described in TASK.md and run the tests.",
        "test_command": "pytest -q",
        "provider": "fake",
    }


async def wait_for_approval(workflow_id: UUID) -> ApprovalRequest:
    async def probe() -> ApprovalRequest | None:
        async with TestSession() as session:
            return cast(
                ApprovalRequest | None,
                await session.scalar(
                    select(ApprovalRequest).where(ApprovalRequest.workflow_id == workflow_id)
                ),
            )

    async with asyncio.timeout(40):
        while True:
            approval = await probe()
            if approval is not None:
                return approval
            await asyncio.sleep(0.1)


async def wait_for_patch_effect(step_id: UUID) -> tuple[CodingWorkspace, str]:
    async with asyncio.timeout(40):
        while True:
            async with TestSession() as session:
                workspace = await session.scalar(
                    select(CodingWorkspace).where(CodingWorkspace.step_id == step_id)
                )
                sandbox = (
                    await session.scalar(
                        select(SandboxExecution)
                        .where(SandboxExecution.workspace_id == workspace.id)
                        .order_by(SandboxExecution.created_at.desc())
                    )
                    if workspace
                    else None
                )
                patch = await session.scalar(
                    select(AgentToolCall).where(AgentToolCall.tool_name == "apply_patch")
                )
            if workspace and sandbox and patch and workspace.tree_hash:
                try:
                    output = await docker(
                        "exec",
                        "-i",
                        sandbox.container_ref,
                        "python",
                        "/opt/sandbox_tool.py",
                        "fingerprint",
                        stdin=b"{}",
                    )
                    state = json.loads(output)["result"]
                except Exception:
                    state = None
                if state and state["tree_hash"] != workspace.tree_hash:
                    return workspace, sandbox.container_ref
            await asyncio.sleep(0.1)


async def test_fake_coding_agent_requires_approval_and_commits_once(client: AsyncClient) -> None:
    workflow_id, step_id = await create_scheduled(
        step_count=1, step_type="coding_agent", step_input=coding_input()
    )
    attempt = await claim(lease_seconds=60)
    task = asyncio.create_task(
        execute_attempt(
            attempt, executor_id="executor-a", sessions=TestSession, settings=settings()
        )
    )
    volume_name: str | None = None
    try:
        approval = await wait_for_approval(workflow_id)
        assert approval.status == ApprovalStatus.PENDING
        async with TestSession() as session:
            workspace = await session.scalar(
                select(CodingWorkspace).where(CodingWorkspace.step_id == step_id)
            )
            assert workspace is not None
            volume_name = workspace.volume_name
            assert workspace.status == WorkspaceStatus.WAITING_APPROVAL
            assert await session.scalar(select(func.count()).select_from(WorkspaceCheckpoint)) == 2
            tests = await session.scalar(
                select(SandboxCommand).where(SandboxCommand.command_type == "run_tests")
            )
            assert tests is not None and tests.exit_code == 0
            assert "6 passed" in (tests.stdout_excerpt or "")
            workflow = await WorkflowService(session).get_workflow(workflow_id)
            assert workflow.status == WorkflowStatus.RUNNING
        listed = await client.get("/api/v1/approvals", params={"status": "PENDING"})
        assert listed.status_code == 200
        assert str(approval.id) in {item["id"] for item in listed.json()}
        fetched = await client.get(f"/api/v1/approvals/{approval.id}")
        assert fetched.status_code == 200
        coding_view = await client.get(f"/api/v1/workflows/{workflow_id}/coding")
        assert coding_view.status_code == 200
        assert len(coding_view.json()) == 1
        approved = await client.post(
            f"/api/v1/approvals/{approval.id}/approve", json={"reason": "fixture reviewed"}
        )
        assert approved.status_code == 200
        assert approved.json()["status"] == "APPROVED"
        assert await asyncio.wait_for(task, timeout=20) == "succeeded"
        async with TestSession() as session:
            workspace = await session.scalar(
                select(CodingWorkspace).where(CodingWorkspace.step_id == step_id)
            )
            assert workspace is not None and workspace.status == WorkspaceStatus.COMPLETED
            assert workspace.final_git_head is not None
            persisted_approval = await session.get(ApprovalRequest, approval.id)
            assert persisted_approval is not None
            assert persisted_approval.commit_sha == workspace.final_git_head
            workflow = await WorkflowService(session).get_workflow(workflow_id)
            assert workflow.status == WorkflowStatus.SUCCEEDED
            assert await session.scalar(select(func.count()).select_from(AgentRun)) == 1
            assert await session.scalar(select(func.count()).select_from(ModelCall)) == 7
            assert await session.scalar(select(func.count()).select_from(AgentToolCall)) == 6
            assert await session.scalar(select(func.count()).select_from(CodingWorkspace)) == 1
            assert await session.scalar(select(func.count()).select_from(ApprovalRequest)) == 1
        diagnostic = DockerSandbox(workspace.id, uuid4(), volume_name, settings())
        await diagnostic.start()
        try:
            replay = await diagnostic.call(
                "git_commit",
                {
                    "operation_id": approval.operation_id,
                    "expected_tree_hash": workspace.tree_hash,
                    "expected_git_head": workspace.baseline_git_head,
                    "message": "Fix checkout discount calculation",
                },
            )
            assert replay["already_committed"] is True
            assert replay["commit_sha"] == workspace.final_git_head
        finally:
            await diagnostic.stop()
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if volume_name is not None:
            await docker("volume", "rm", volume_name)


async def test_rejected_approval_is_durable_and_never_commits(client: AsyncClient) -> None:
    workflow_id, step_id = await create_scheduled(
        step_count=1, step_type="coding_agent", step_input=coding_input()
    )
    attempt = await claim(lease_seconds=60)
    task = asyncio.create_task(
        execute_attempt(
            attempt, executor_id="executor-a", sessions=TestSession, settings=settings()
        )
    )
    volume_name: str | None = None
    try:
        approval = await wait_for_approval(workflow_id)
        async with TestSession() as session:
            workspace = await session.scalar(
                select(CodingWorkspace).where(CodingWorkspace.step_id == step_id)
            )
            assert workspace is not None
            volume_name = workspace.volume_name
        rejected = await client.post(f"/api/v1/approvals/{approval.id}/reject")
        assert rejected.status_code == 200 and rejected.json()["status"] == "REJECTED"
        repeated = await client.post(f"/api/v1/approvals/{approval.id}/reject")
        assert repeated.status_code == 200 and repeated.json()["status"] == "REJECTED"
        assert await asyncio.wait_for(task, timeout=20) == "failed"
        async with TestSession() as session:
            workspace = await session.scalar(
                select(CodingWorkspace).where(CodingWorkspace.step_id == step_id)
            )
            assert workspace is not None and workspace.final_git_head is None
            assert workspace.status == WorkspaceStatus.FAILED
            workflow = await WorkflowService(session).get_workflow(workflow_id)
            assert workflow.status == WorkflowStatus.FAILED
            commits = await session.scalar(
                select(func.count())
                .select_from(SandboxCommand)
                .where(SandboxCommand.command_type == "git_commit")
            )
            assert commits == 0
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if volume_name is not None:
            await docker("volume", "rm", volume_name)


async def test_timed_out_sandbox_tests_are_recorded_and_cannot_be_approved() -> None:
    source = FIXTURE_TEST_SOURCE
    script = fixture_script()
    script["5"] = [
        {
            "type": "tool_call",
            "tool_name": "apply_patch",
            "arguments": {
                "path": "tests/test_pricing.py",
                "expected_sha256": hashlib.sha256(source).hexdigest(),
                "replacement_text": "import time\ntime.sleep(3)\n" + source.decode(),
            },
        }
    ]
    input_value = {**coding_input(), "fake_script": script}
    workflow_id, step_id = await create_scheduled(
        step_count=1, step_type="coding_agent", step_input=input_value
    )
    attempt = await claim(lease_seconds=60)
    config = settings().model_copy(update={"coding_command_timeout_seconds": 1})
    async with TestSession() as session:
        initial = await session.scalar(
            select(CodingWorkspace).where(CodingWorkspace.step_id == step_id)
        )
        assert initial is None
    volume_name: str | None = None
    try:
        assert (
            await asyncio.wait_for(
                execute_attempt(
                    attempt, executor_id="executor-a", sessions=TestSession, settings=config
                ),
                timeout=35,
            )
            == "failed"
        )
        async with TestSession() as session:
            workspace = await session.scalar(
                select(CodingWorkspace).where(CodingWorkspace.step_id == step_id)
            )
            assert workspace is not None
            volume_name = workspace.volume_name
            assert workspace.status == WorkspaceStatus.FAILED
            timed = await session.scalar(
                select(SandboxCommand).where(SandboxCommand.command_type == "run_tests")
            )
            assert timed is not None and timed.status == CommandStatus.TIMED_OUT
            assert timed.exit_code is None
            assert await session.scalar(select(func.count()).select_from(ApprovalRequest)) == 0
            workflow = await WorkflowService(session).get_workflow(workflow_id)
            assert workflow.status == WorkflowStatus.FAILED
    finally:
        if volume_name is None:
            async with TestSession() as session:
                workspace = await session.scalar(
                    select(CodingWorkspace).where(CodingWorkspace.step_id == step_id)
                )
                if workspace is not None:
                    volume_name = workspace.volume_name
        if volume_name is not None:
            await docker("volume", "rm", volume_name)


async def test_invalid_python_model_patch_retries_before_any_file_effect() -> None:
    script = fixture_script()
    valid_patch = script["5"][0]
    invalid_patch = {
        **valid_patch,
        "arguments": {
            **valid_patch["arguments"],
            "replacement_text": '"""unterminated',
        },
    }
    script["5"] = [invalid_patch, valid_patch]
    workflow_id, step_id = await create_scheduled(
        step_count=1,
        step_type="coding_agent",
        step_input={**coding_input(), "fake_script": script},
    )
    attempt = await claim(lease_seconds=60)
    task = asyncio.create_task(
        execute_attempt(
            attempt, executor_id="executor-a", sessions=TestSession, settings=settings()
        )
    )
    volume_name: str | None = None
    try:
        approval = await wait_for_approval(workflow_id)
        async with TestSession() as session:
            workspace = await session.scalar(
                select(CodingWorkspace).where(CodingWorkspace.step_id == step_id)
            )
            assert workspace is not None
            volume_name = workspace.volume_name
            calls = list(await session.scalars(select(ModelCall).order_by(ModelCall.created_at)))
            failed = [call for call in calls if call.status.value == "FAILED"]
            assert len(failed) == 1 and failed[0].error_code == "ValueError"
            patch_tool_count = await session.scalar(
                select(func.count())
                .select_from(AgentToolCall)
                .where(AgentToolCall.tool_name == "apply_patch")
            )
            assert patch_tool_count == 1
            patch_command_count = await session.scalar(
                select(func.count())
                .select_from(SandboxCommand)
                .where(SandboxCommand.command_type == "apply_patch")
            )
            assert patch_command_count == 1
        async with TestSession() as session:
            await ApprovalService(session).decide(approval.id, ApprovalStatus.APPROVED)
        assert await asyncio.wait_for(task, timeout=25) == "succeeded"
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if volume_name is not None:
            await docker("volume", "rm", volume_name)


async def test_premature_final_is_recorded_as_failed_model_call_then_retried() -> None:
    script = fixture_script()
    script["2"] = [
        {"type": "final", "response": "The fix is complete."},
        script["2"][0],
    ]
    workflow_id, step_id = await create_scheduled(
        step_count=1,
        step_type="coding_agent",
        step_input={**coding_input(), "fake_script": script},
    )
    attempt = await claim(lease_seconds=60)
    task = asyncio.create_task(
        execute_attempt(
            attempt, executor_id="executor-a", sessions=TestSession, settings=settings()
        )
    )
    volume_name: str | None = None
    try:
        approval = await wait_for_approval(workflow_id)
        async with TestSession() as session:
            workspace = await session.scalar(
                select(CodingWorkspace).where(CodingWorkspace.step_id == step_id)
            )
            assert workspace is not None
            volume_name = workspace.volume_name
            calls = list(await session.scalars(select(ModelCall).order_by(ModelCall.created_at)))
            assert len(calls) == 8
            failed = [call for call in calls if call.status.value == "FAILED"]
            assert len(failed) == 1
            assert failed[0].error_detail is not None
            assert "Premature final decision" in failed[0].error_detail
            assert await session.scalar(select(func.count()).select_from(AgentToolCall)) == 6
        async with TestSession() as session:
            await ApprovalService(session).decide(approval.id, ApprovalStatus.APPROVED)
        assert await asyncio.wait_for(task, timeout=25) == "succeeded"
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if volume_name is not None:
            await docker("volume", "rm", volume_name)


async def test_repeated_premature_final_fails_without_approval_or_commit() -> None:
    workflow_id, step_id = await create_scheduled(
        step_count=1,
        step_type="coding_agent",
        step_input={
            **coding_input(),
            "fake_script": {"1": [{"type": "final", "response": "Already fixed."}]},
        },
    )
    attempt = await claim(lease_seconds=60)
    volume_name: str | None = None
    try:
        assert (
            await asyncio.wait_for(
                execute_attempt(
                    attempt,
                    executor_id="executor-a",
                    sessions=TestSession,
                    settings=settings(),
                ),
                timeout=30,
            )
            == "failed"
        )
        async with TestSession() as session:
            workspace = await session.scalar(
                select(CodingWorkspace).where(CodingWorkspace.step_id == step_id)
            )
            assert workspace is not None and workspace.status == WorkspaceStatus.FAILED
            volume_name = workspace.volume_name
            agent_run = await session.scalar(select(AgentRun).where(AgentRun.step_id == step_id))
            assert agent_run is not None and agent_run.status.value == "FAILED"
            assert await session.scalar(select(func.count()).select_from(ModelCall)) == 3
            assert await session.scalar(select(func.count()).select_from(ApprovalRequest)) == 0
            assert await session.scalar(select(func.count()).select_from(AgentToolCall)) == 0
            workflow = await WorkflowService(session).get_workflow(workflow_id)
            assert workflow.status == WorkflowStatus.FAILED
    finally:
        if volume_name is not None:
            await docker("volume", "rm", volume_name)


async def test_patch_effect_survives_outer_attempt_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FAULTLAB_CODING_PAUSE_AFTER_PATCH", "1")
    workflow_id, step_id = await create_scheduled(
        step_count=1, step_type="coding_agent", step_input=coding_input()
    )
    original = await claim(lease_seconds=60)
    config = settings().model_copy(update={"app_env": "faultlab"})
    task = asyncio.create_task(
        execute_attempt(original, executor_id="executor-a", sessions=TestSession, settings=config)
    )
    volume_name: str | None = None
    try:
        workspace, _container = await wait_for_patch_effect(step_id)
        volume_name = workspace.volume_name
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        monkeypatch.delenv("FAULTLAB_CODING_PAUSE_AFTER_PATCH")
        replacement = await expire_and_replace(original)
        resumed = asyncio.create_task(
            execute_attempt(
                replacement, executor_id="executor-b", sessions=TestSession, settings=settings()
            )
        )
        try:
            approval = await wait_for_approval(workflow_id)
            async with TestSession() as session:
                await ApprovalService(session).decide(approval.id, ApprovalStatus.APPROVED)
            assert await asyncio.wait_for(resumed, timeout=25) == "succeeded"
            async with TestSession() as session:
                assert await session.scalar(select(func.count()).select_from(AgentRun)) == 1
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(AgentToolCall)
                        .where(AgentToolCall.tool_name == "apply_patch")
                    )
                    == 1
                )
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(WorkspaceCheckpoint)
                        .where(WorkspaceCheckpoint.reason == "PATCH_APPLIED")
                    )
                    == 1
                )
                commands = list(
                    await session.scalars(
                        select(SandboxCommand)
                        .where(SandboxCommand.command_type == "apply_patch")
                        .order_by(SandboxCommand.started_at)
                    )
                )
                assert len(commands) == 2
                assert commands[-1].result is not None
                assert commands[-1].result["already_applied"] is True
                sandboxes = list(
                    await session.scalars(
                        select(SandboxExecution)
                        .where(SandboxExecution.workspace_id == workspace.id)
                        .order_by(SandboxExecution.created_at)
                    )
                )
                assert len(sandboxes) == 2
                assert sandboxes[0].status.value == "STOPPED"
        finally:
            if not resumed.done():
                resumed.cancel()
                await asyncio.gather(resumed, return_exceptions=True)
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if volume_name is not None:
            await docker("volume", "rm", volume_name)


async def test_commit_response_loss_reuses_existing_commit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FAULTLAB_CODING_PAUSE_AFTER_COMMIT", "1")
    workflow_id, step_id = await create_scheduled(
        step_count=1, step_type="coding_agent", step_input=coding_input()
    )
    original = await claim(lease_seconds=60)
    config = settings().model_copy(update={"app_env": "faultlab"})
    task = asyncio.create_task(
        execute_attempt(original, executor_id="executor-a", sessions=TestSession, settings=config)
    )
    volume_name: str | None = None
    try:
        approval = await wait_for_approval(workflow_id)
        async with TestSession() as session:
            workspace = await session.scalar(
                select(CodingWorkspace).where(CodingWorkspace.step_id == step_id)
            )
            assert workspace is not None
            volume_name = workspace.volume_name
        async with TestSession() as session:
            await ApprovalService(session).decide(approval.id, ApprovalStatus.APPROVED)
        async with asyncio.timeout(30):
            while True:
                async with TestSession() as session:
                    record = await session.scalar(
                        select(SandboxExecution)
                        .where(SandboxExecution.workspace_id == workspace.id)
                        .order_by(SandboxExecution.created_at.desc())
                    )
                    current = await session.get(ApprovalRequest, approval.id)
                assert record is not None and current is not None
                output = await docker(
                    "exec", record.container_ref, "git", "log", "-1", "--format=%B"
                )
                if approval.operation_id in output and current.commit_sha is None:
                    break
                await asyncio.sleep(0.1)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        monkeypatch.delenv("FAULTLAB_CODING_PAUSE_AFTER_COMMIT")
        replacement = await expire_and_replace(original)
        assert (
            await asyncio.wait_for(
                execute_attempt(
                    replacement, executor_id="executor-b", sessions=TestSession, settings=settings()
                ),
                timeout=25,
            )
            == "succeeded"
        )
        async with TestSession() as session:
            current = await session.get(ApprovalRequest, approval.id)
            assert current is not None and current.commit_sha is not None
            commands = list(
                await session.scalars(
                    select(SandboxCommand)
                    .where(SandboxCommand.command_type == "git_commit")
                    .order_by(SandboxCommand.started_at)
                )
            )
            assert len(commands) == 2
            assert commands[-1].result is not None
            assert commands[-1].result["already_committed"] is True
            assert commands[-1].result["commit_sha"] == current.commit_sha
            workflow = await WorkflowService(session).get_workflow(workflow_id)
            assert workflow.status == WorkflowStatus.SUCCEEDED
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if volume_name is not None:
            await docker("volume", "rm", volume_name)
