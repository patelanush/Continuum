"""Real-container coding faults against FaultLab's isolated Compose project."""

import asyncio
from datetime import UTC, datetime
from typing import cast
from uuid import UUID, uuid4

from sqlalchemy import func, select

from durable_agent_runtime.coding.fake import fixture_script
from durable_agent_runtime.coding.sandbox import DockerSandbox, docker
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
from durable_agent_runtime.faultlab.assertions import classify_workflow
from durable_agent_runtime.faultlab.models import TrialResult
from durable_agent_runtime.faultlab.runtime import FaultLabRuntime, wait_for


def _inject(trial: TrialResult, point: str) -> None:
    trial.fault_type = "SIGKILL"
    trial.fault_injection_point = point
    trial.fault_injected_at = datetime.now(UTC)
    trial.injected_failure_count += 1
    trial.recovery_required = True


async def _start(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    created = await runtime.create_start(
        [
            {
                "name": "fix-discount",
                "step_type": "coding_agent",
                "input": {
                    "repository": "fixture:discount_service",
                    "task": "Fix the discount bug described in TASK.md and run the tests.",
                    "test_command": "pytest -q",
                    "provider": "fake",
                },
            }
        ],
        label=str(trial.trial_id),
    )
    trial.workflow_id = UUID(created["id"])
    trial.step_id = UUID(created["steps"][0]["id"])


async def _workspace(runtime: FaultLabRuntime, trial: TrialResult) -> CodingWorkspace | None:
    async with runtime.sessions() as session:
        return cast(
            CodingWorkspace | None,
            await session.scalar(
                select(CodingWorkspace).where(CodingWorkspace.step_id == trial.step_id)
            ),
        )


async def _approval(runtime: FaultLabRuntime, trial: TrialResult) -> ApprovalRequest | None:
    async with runtime.sessions() as session:
        return cast(
            ApprovalRequest | None,
            await session.scalar(
                select(ApprovalRequest).where(ApprovalRequest.step_id == trial.step_id)
            ),
        )


async def _command(
    runtime: FaultLabRuntime, trial: TrialResult, command_type: str, *, completed: bool
) -> SandboxCommand | None:
    workspace = await _workspace(runtime, trial)
    if workspace is None:
        return None
    async with runtime.sessions() as session:
        statement = (
            select(SandboxCommand)
            .where(
                SandboxCommand.workspace_id == workspace.id,
                SandboxCommand.command_type == command_type,
            )
            .order_by(SandboxCommand.started_at.desc())
        )
        if completed:
            statement = statement.where(SandboxCommand.completed_at.is_not(None))
        return cast(SandboxCommand | None, await session.scalar(statement))


async def _tool(
    runtime: FaultLabRuntime, trial: TrialResult, tool_name: str
) -> AgentToolCall | None:
    async with runtime.sessions() as session:
        return cast(
            AgentToolCall | None,
            await session.scalar(
                select(AgentToolCall).where(
                    AgentToolCall.tool_name == tool_name,
                    AgentToolCall.agent_run_id.in_(
                        select(AgentRun.id).where(AgentRun.step_id == trial.step_id)
                    ),
                )
            ),
        )


async def _sandbox(runtime: FaultLabRuntime, trial: TrialResult) -> SandboxExecution | None:
    workspace = await _workspace(runtime, trial)
    if workspace is None:
        return None
    async with runtime.sessions() as session:
        return cast(
            SandboxExecution | None,
            await session.scalar(
                select(SandboxExecution)
                .where(SandboxExecution.workspace_id == workspace.id)
                .order_by(SandboxExecution.created_at.desc())
            ),
        )


async def _finish(runtime: FaultLabRuntime, trial: TrialResult, *, attempts: int) -> None:
    assert trial.workflow_id is not None
    approval = await wait_for(lambda: _approval(runtime, trial), wait_timeout=65)
    if approval.status.value == "PENDING":
        response = await runtime.api.post(f"/api/v1/approvals/{approval.id}/approve")
        response.raise_for_status()
    await runtime.wait_terminal(trial.workflow_id, wait_timeout=75)
    await classify_workflow(
        runtime,
        trial,
        expected_attempts=attempts,
        expected_outbox=1,
        expected_attempt_statuses=["EXPIRED", "SUCCEEDED"] if attempts == 2 else None,
    )
    workspace = await _workspace(runtime, trial)
    final_approval = await _approval(runtime, trial)
    failures: list[str] = []
    if workspace is None or workspace.status.value != "COMPLETED" or not workspace.final_git_head:
        failures.append("workspace did not complete with a local commit")
    if final_approval is None or final_approval.commit_sha != (
        workspace.final_git_head if workspace else None
    ):
        failures.append("approved commit SHA differs from workspace")
    test_record = await _command(runtime, trial, "run_tests", completed=True)
    if test_record is None or test_record.exit_code != 0:
        failures.append("no durable successful test execution")
    patch = await _tool(runtime, trial, "apply_patch")
    if patch is None or patch.status.value != "SUCCEEDED":
        failures.append("one durable patch tool result missing")
    if workspace is not None:
        async with runtime.sessions() as session:
            patch_count = await session.scalar(
                select(func.count())
                .select_from(WorkspaceCheckpoint)
                .where(
                    WorkspaceCheckpoint.workspace_id == workspace.id,
                    WorkspaceCheckpoint.reason == "PATCH_APPLIED",
                )
            )
            commit_count = await session.scalar(
                select(func.count())
                .select_from(SandboxCommand)
                .where(
                    SandboxCommand.workspace_id == workspace.id,
                    SandboxCommand.command_type == "git_commit",
                    SandboxCommand.status == "SUCCEEDED",
                )
            )
        if patch_count != 1:
            failures.append(f"patch checkpoints={patch_count}, expected=1")
        if commit_count != 1:
            failures.append(f"successful commit records={commit_count}, expected=1")
        trial.notes["workspace_id"] = str(workspace.id)
        trial.notes["commit_sha"] = workspace.final_git_head
        history_count = await runtime.docker.command(
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--user",
            "10001:10001",
            "--mount",
            f"type=volume,src={workspace.volume_name},dst=/workspace,readonly",
            "--workdir",
            "/workspace",
            "continuum-sandbox:phase6",
            "git",
            "rev-list",
            "--count",
            "HEAD",
        )
        if history_count != "2":
            failures.append(f"Git history has {history_count} commits, expected baseline plus one")
        trial.notes["agent_commit_count"] = int(history_count) - 1
    if failures:
        trial.correct = False
        trial.recovered = False
        trial.failure_reason = "; ".join(filter(None, [trial.failure_reason, *failures]))


async def baseline(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await _start(runtime, trial)
    await _finish(runtime, trial, attempts=1)


async def _paused_crash(
    runtime: FaultLabRuntime,
    trial: TrialResult,
    *,
    hook: str,
    boundary: str,
) -> None:
    await runtime.docker.compose("stop", "executor")
    oneoff = ""
    try:
        oneoff = await runtime.docker.run_oneoff("executor", variables={hook: "1"})
        await _start(runtime, trial)
        assert trial.workflow_id is not None
        await runtime.wait_attempt(trial.workflow_id)
        if boundary == "decision":
            await wait_for(lambda: _tool(runtime, trial, "apply_patch"))
            tool = await _tool(runtime, trial, "apply_patch")
            assert tool is not None and tool.result is None
        elif boundary == "patch":

            async def patch_applied() -> bool | None:
                record = await _sandbox(runtime, trial)
                if record is None:
                    return None
                try:
                    output = await runtime.docker.command(
                        "docker", "exec", record.container_ref, "git", "status", "--porcelain"
                    )
                except RuntimeError:
                    return None
                return True if output else None

            await wait_for(patch_applied)
        elif boundary == "tests":
            # The hook pauses after sandbox command return and before durable command result.
            record = await wait_for(lambda: _command(runtime, trial, "run_tests", completed=False))
            assert record.status.value == "RUNNING"
            await asyncio.sleep(1)
        elif boundary == "approval":
            approval = await wait_for(lambda: _approval(runtime, trial))
            decision = await runtime.api.post(f"/api/v1/approvals/{approval.id}/approve")
            decision.raise_for_status()
        elif boundary == "commit":
            approval = await wait_for(lambda: _approval(runtime, trial))
            decision = await runtime.api.post(f"/api/v1/approvals/{approval.id}/approve")
            decision.raise_for_status()

            async def committed() -> bool | None:
                record = await _sandbox(runtime, trial)
                if record is None:
                    return None
                try:
                    message = await runtime.docker.command(
                        "docker", "exec", record.container_ref, "git", "log", "-1", "--format=%B"
                    )
                except RuntimeError:
                    return None
                current = await _approval(runtime, trial)
                return (
                    True
                    if current and current.operation_id in message and not current.commit_sha
                    else None
                )

            await wait_for(committed)
        else:
            raise ValueError(boundary)
        _inject(trial, boundary)
        await runtime.docker.command("docker", "kill", "--signal=KILL", oneoff)
    finally:
        if oneoff:
            await runtime.docker.remove_oneoff(oneoff)
        await runtime.docker.compose("start", "executor")
    await _finish(runtime, trial, attempts=2)


async def crash_after_decision(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await _paused_crash(
        runtime, trial, hook="FAULTLAB_CODING_PAUSE_AFTER_DECISION", boundary="decision"
    )
    patch = await _tool(runtime, trial, "apply_patch")
    assert patch is not None
    async with runtime.sessions() as session:
        model_calls = await session.scalar(
            select(func.count())
            .select_from(ModelCall)
            .where(ModelCall.agent_turn_id == patch.agent_turn_id)
        )
    assert model_calls == 1
    trial.notes["persisted_patch_decision_model_calls"] = model_calls


async def crash_after_patch(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await _paused_crash(runtime, trial, hook="FAULTLAB_CODING_PAUSE_AFTER_PATCH", boundary="patch")
    workspace = await _workspace(runtime, trial)
    assert workspace is not None
    async with runtime.sessions() as session:
        commands = list(
            await session.scalars(
                select(SandboxCommand).where(
                    SandboxCommand.workspace_id == workspace.id,
                    SandboxCommand.command_type == "apply_patch",
                )
            )
        )
    assert len(commands) == 2 and commands[-1].result is not None
    assert commands[-1].result["already_applied"] is True


async def crash_after_tests(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await _paused_crash(runtime, trial, hook="FAULTLAB_CODING_PAUSE_AFTER_TESTS", boundary="tests")


async def crash_before_commit(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await _paused_crash(
        runtime, trial, hook="FAULTLAB_CODING_PAUSE_AFTER_APPROVAL", boundary="approval"
    )


async def crash_after_commit(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await _paused_crash(
        runtime, trial, hook="FAULTLAB_CODING_PAUSE_AFTER_COMMIT", boundary="commit"
    )


async def sandbox_killed(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await runtime.docker.compose("stop", "executor")
    oneoff = ""
    try:
        oneoff = await runtime.docker.run_oneoff(
            "executor", variables={"FAULTLAB_CODING_PAUSE_AFTER_DECISION": "1"}
        )
        await _start(runtime, trial)
        assert trial.workflow_id is not None
        await runtime.wait_attempt(trial.workflow_id)
        await wait_for(lambda: _tool(runtime, trial, "apply_patch"))
        workspace = await _workspace(runtime, trial)
        record = await _sandbox(runtime, trial)
        assert workspace is not None and record is not None
        _inject(trial, "sandbox container killed; logical volume retained")
        await runtime.docker.command("docker", "kill", "--signal=KILL", record.container_ref)
        await runtime.docker.command("docker", "kill", "--signal=KILL", oneoff)
    finally:
        if oneoff:
            await runtime.docker.remove_oneoff(oneoff)
        await runtime.docker.compose("start", "executor")
    await _finish(runtime, trial, attempts=2)
    recovered_workspace = await _workspace(runtime, trial)
    assert recovered_workspace is not None and workspace.id == recovered_workspace.id
    latest = await _sandbox(runtime, trial)
    assert latest is not None and latest.id != record.id


async def executor_killed(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    await runtime.docker.compose("stop", "executor")
    oneoff = ""
    try:
        oneoff = await runtime.docker.run_oneoff(
            "executor", variables={"FAULTLAB_CODING_PAUSE_AFTER_DECISION": "1"}
        )
        await _start(runtime, trial)
        assert trial.workflow_id is not None
        await runtime.wait_attempt(trial.workflow_id)
        await wait_for(lambda: _tool(runtime, trial, "apply_patch"))
        _inject(trial, "executor killed during coding agent")
        await runtime.docker.command("docker", "kill", "--signal=KILL", oneoff)
    finally:
        if oneoff:
            await runtime.docker.remove_oneoff(oneoff)
        await runtime.docker.compose("start", "executor")
    await _finish(runtime, trial, attempts=2)


async def workspace_divergence(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    """Unexplained external workspace change must fail closed on resume."""
    await runtime.docker.compose("stop", "executor")
    oneoff = ""
    try:
        oneoff = await runtime.docker.run_oneoff(
            "executor", variables={"FAULTLAB_CODING_PAUSE_AFTER_DECISION": "1"}
        )
        await _start(runtime, trial)
        assert trial.workflow_id is not None
        await runtime.wait_attempt(trial.workflow_id)
        await wait_for(lambda: _tool(runtime, trial, "apply_patch"))
        record = await _sandbox(runtime, trial)
        assert record is not None
        await runtime.docker.command(
            "docker",
            "exec",
            record.container_ref,
            "python",
            "-c",
            "from pathlib import Path; "
            "Path('/workspace/checkout/pricing.py').write_text('unexpected divergence\\n')",
        )
        _inject(trial, "external unexpected workspace mutation after durable checkpoint")
        # Correct behavior is a terminal fail-closed workflow, not recovery to success.
        trial.recovery_required = False
        await runtime.docker.command("docker", "kill", "--signal=KILL", oneoff)
    finally:
        if oneoff:
            await runtime.docker.remove_oneoff(oneoff)
        await runtime.docker.compose("start", "executor")
    assert trial.workflow_id is not None
    trial.expected_terminal_status = "FAILED"
    await runtime.wait_terminal(trial.workflow_id, wait_timeout=75)
    await classify_workflow(runtime, trial, expected_attempts=2, expected_outbox=1)
    assert await _approval(runtime, trial) is None
    workspace = await _workspace(runtime, trial)
    assert workspace is not None and workspace.final_git_head is None
    trial.notes["fail_closed"] = True


async def path_traversal(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    created = await runtime.create_start(
        [
            {
                "name": "reject-traversal",
                "step_type": "coding_agent",
                "input": {
                    "repository": "fixture:discount_service",
                    "task": "Inspect the repository.",
                    "test_command": "pytest -q",
                    "provider": "fake",
                    "fake_script": {
                        "1": [
                            {
                                "type": "tool_call",
                                "tool_name": "read_file",
                                "arguments": {"path": "../../etc/passwd"},
                            }
                        ],
                    },
                },
            }
        ],
        label=str(trial.trial_id),
    )
    trial.workflow_id = UUID(created["id"])
    trial.step_id = UUID(created["steps"][0]["id"])
    trial.expected_terminal_status = "FAILED"
    await runtime.wait_terminal(trial.workflow_id, wait_timeout=65)
    await classify_workflow(runtime, trial, expected_attempts=1, expected_outbox=1)
    assert await _approval(runtime, trial) is None
    tool = await _tool(runtime, trial, "read_file")
    assert tool is not None and tool.status.value == "FAILED"
    trial.notes["blocked_path"] = "../../etc/passwd"


async def command_timeout(runtime: FaultLabRuntime, trial: TrialResult) -> None:
    """A real Docker test command times out without leaving the container command alive."""
    workspace_id, sandbox_id = uuid4(), uuid4()
    volume_name = f"{runtime.docker.project}-ws-{workspace_id.hex}"
    sandbox = DockerSandbox(
        workspace_id,
        sandbox_id,
        volume_name,
        Settings(sandbox_volume_prefix=runtime.docker.project),
    )
    try:
        await sandbox.start()
        before = await sandbox.call(
            "prepare",
            {"repository": "fixture:discount_service", "initialize_if_missing": True},
        )
        patch = fixture_script()["5"][0]["arguments"]
        assert isinstance(patch, dict)
        await sandbox.call("apply_patch", {**patch, "before_file_hashes": before["file_hashes"]})
        test_file = await sandbox.call("read_file", {"path": "tests/test_pricing.py"})
        delayed = "import time\ntime.sleep(2)\n" + test_file["content"]
        await sandbox.call(
            "apply_patch",
            {
                "path": "tests/test_pricing.py",
                "expected_sha256": test_file["sha256"],
                "replacement_text": delayed,
                "before_file_hashes": (await sandbox.call("fingerprint", {}))["file_hashes"],
            },
        )
        result = await sandbox.call("run_tests", {"timeout_seconds": 1})
        assert result["timed_out"] is True and result["exit_code"] is None
        trial.correct = True
        trial.expected_terminal_status = "NOT_APPLICABLE"
        trial.notes["sandbox_command_status"] = "TIMED_OUT"
    finally:
        await sandbox.stop()
        await docker("volume", "rm", volume_name)
