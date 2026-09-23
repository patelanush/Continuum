"""Short, fenced transactions for logical workspaces and coding effects."""

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from durable_agent_runtime.core.config import Settings
from durable_agent_runtime.db.models import (
    ApprovalRequest,
    CodingWorkspace,
    SandboxCommand,
    SandboxExecution,
    Workflow,
    WorkspaceCheckpoint,
)
from durable_agent_runtime.domain.enums import (
    ApprovalStatus,
    CommandStatus,
    SandboxStatus,
    WorkflowStatus,
    WorkspaceStatus,
)
from durable_agent_runtime.domain.errors import ApprovalNotFound, WorkflowConflict
from durable_agent_runtime.domain.state_machine import (
    validate_approval_transition,
    validate_command_transition,
    validate_sandbox_transition,
    validate_workspace_transition,
)
from durable_agent_runtime.execution.tools import ExecutionContext, PermanentToolError
from durable_agent_runtime.services.execution import ExecutionService


class CodingService:
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

    async def ensure_workspace(
        self, run_id: UUID, repository: str, test_command: list[str], settings: Settings
    ) -> UUID:
        async with self.session.begin():
            await self._fence()
            existing = await self.session.scalar(
                select(CodingWorkspace)
                .where(CodingWorkspace.step_id == self.context.step_id)
                .with_for_update()
            )
            if existing is not None:
                if existing.agent_run_id != run_id or existing.repository_source != repository:
                    raise PermanentToolError(
                        "WORKSPACE_IDENTITY_MISMATCH", "Workspace identity changed"
                    )
                return existing.id
            workspace_id = uuid4()
            self.session.add(
                CodingWorkspace(
                    id=workspace_id,
                    workflow_id=self.context.workflow_id,
                    step_id=self.context.step_id,
                    agent_run_id=run_id,
                    status=WorkspaceStatus.PENDING,
                    repository_source=repository,
                    workspace_key=f"coding:{self.context.step_id}",
                    volume_name=f"{settings.sandbox_volume_prefix}-ws-{workspace_id.hex}",
                    test_command=test_command,
                )
            )
            return workspace_id

    async def sandbox_intent(self, workspace_id: UUID, settings: Settings) -> UUID:
        async with self.session.begin():
            await self._fence()
            sandbox_id = uuid4()
            self.session.add(
                SandboxExecution(
                    id=sandbox_id,
                    workspace_id=workspace_id,
                    execution_attempt_id=self.context.attempt_id,
                    container_ref=f"continuum-sb-{sandbox_id.hex}",
                    status=SandboxStatus.PENDING,
                    image=settings.sandbox_image,
                    resource_limits={"cpus": 1, "memory_mb": 512, "pids": 128},
                    network_mode="none",
                )
            )
            return sandbox_id

    async def sandbox_started(self, sandbox_id: UUID) -> None:
        async with self.session.begin():
            await self._fence()
            record = await self.session.get(SandboxExecution, sandbox_id, with_for_update=True)
            assert record is not None
            superseded = list(
                await self.session.scalars(
                    select(SandboxExecution)
                    .where(
                        SandboxExecution.workspace_id == record.workspace_id,
                        SandboxExecution.id != sandbox_id,
                        SandboxExecution.status != SandboxStatus.STOPPED,
                    )
                    .with_for_update()
                )
            )
            for old in superseded:
                validate_sandbox_transition(old.status, SandboxStatus.STOPPED)
                old.status = SandboxStatus.STOPPED
                old.stopped_at = datetime.now(UTC)
                old.exit_reason = "replaced_after_attempt_expiry"
            interrupted = list(
                await self.session.scalars(
                    select(SandboxCommand)
                    .where(
                        SandboxCommand.workspace_id == record.workspace_id,
                        SandboxCommand.status == CommandStatus.RUNNING,
                    )
                    .with_for_update()
                )
            )
            for command in interrupted:
                validate_command_transition(command.status, CommandStatus.INTERRUPTED)
                command.status = CommandStatus.INTERRUPTED
                command.completed_at = datetime.now(UTC)
            validate_sandbox_transition(record.status, SandboxStatus.RUNNING)
            record.status = SandboxStatus.RUNNING
            record.started_at = datetime.now(UTC)

    async def sandbox_stopped(self, sandbox_id: UUID, reason: str) -> None:
        async with self.session.begin():
            record = await self.session.get(SandboxExecution, sandbox_id, with_for_update=True)
            if record is not None and record.status != SandboxStatus.STOPPED:
                validate_sandbox_transition(record.status, SandboxStatus.STOPPED)
                record.status = SandboxStatus.STOPPED
                record.stopped_at = datetime.now(UTC)
                record.exit_reason = reason[:200]

    async def record_prepared(self, workspace_id: UUID, state: dict[str, Any]) -> None:
        async with self.session.begin():
            await self._fence()
            workspace = await self.session.get(CodingWorkspace, workspace_id, with_for_update=True)
            assert workspace is not None
            if workspace.status == WorkspaceStatus.PENDING:
                workspace.baseline_git_head = state["git_head"]
                workspace.current_git_head = state["git_head"]
                workspace.file_hashes = state["file_hashes"]
                workspace.tree_hash = state["tree_hash"]
                workspace.diff_hash = state["diff_hash"]
                checkpoint = WorkspaceCheckpoint(
                    id=uuid4(),
                    workspace_id=workspace_id,
                    sequence_number=1,
                    git_head=state["git_head"],
                    diff_hash=state["diff_hash"],
                    tree_hash=state["tree_hash"],
                    file_hashes=state["file_hashes"],
                    reason="PREPARED",
                )
                self.session.add(checkpoint)
                workspace.last_checkpoint_id = checkpoint.id
                validate_workspace_transition(workspace.status, WorkspaceStatus.READY)
                workspace.status = WorkspaceStatus.READY
                validate_workspace_transition(workspace.status, WorkspaceStatus.ACTIVE)
                workspace.status = WorkspaceStatus.ACTIVE
            elif workspace.tree_hash != state["tree_hash"]:
                # A patch may have reached the volume before its result/checkpoint was recorded.
                # Its persisted tool call must perform reconciliation before other work proceeds.
                pass

    async def begin_command(
        self, workspace_id: UUID, tool_id: UUID | None, operation: str, timeout_ms: int
    ) -> UUID:
        async with self.session.begin():
            await self._fence()
            if tool_id is not None or operation == "git_commit":
                conditions = [
                    SandboxCommand.workspace_id == workspace_id,
                    SandboxCommand.status == CommandStatus.RUNNING,
                ]
                if tool_id is not None:
                    conditions.append(SandboxCommand.agent_tool_call_id == tool_id)
                else:
                    conditions.append(SandboxCommand.command_type == operation)
                pending = list(
                    await self.session.scalars(
                        select(SandboxCommand).where(*conditions).with_for_update()
                    )
                )
                for previous in pending:
                    validate_command_transition(previous.status, CommandStatus.INTERRUPTED)
                    previous.status = CommandStatus.INTERRUPTED
                    previous.completed_at = datetime.now(UTC)
            command_id = uuid4()
            self.session.add(
                SandboxCommand(
                    id=command_id,
                    workspace_id=workspace_id,
                    agent_tool_call_id=tool_id,
                    command_type=operation,
                    argv=["python", "/opt/sandbox_tool.py", operation],
                    status=CommandStatus.RUNNING,
                    timeout_ms=timeout_ms,
                    started_at=datetime.now(UTC),
                )
            )
            return command_id

    async def finish_command(
        self, command_id: UUID, result: dict[str, Any], *, operation_id: str | None = None
    ) -> None:
        async with self.session.begin():
            await self._fence()
            command = await self.session.get(SandboxCommand, command_id, with_for_update=True)
            assert command is not None and command.status == CommandStatus.RUNNING
            status = CommandStatus.SUCCEEDED
            if result.get("timed_out"):
                status = CommandStatus.TIMED_OUT
            elif command.command_type == "run_tests" and result.get("exit_code") != 0:
                status = CommandStatus.FAILED
            validate_command_transition(command.status, status)
            command.status = status
            command.result = result
            command.exit_code = result.get("exit_code")
            command.stdout_excerpt = str(result.get("stdout", ""))[:8192]
            command.stderr_excerpt = str(result.get("stderr", ""))[:8192]
            command.duration_ms = result.get("duration_ms")
            command.completed_at = datetime.now(UTC)
            if command.command_type == "apply_patch":
                workspace = await self.session.get(
                    CodingWorkspace, command.workspace_id, with_for_update=True
                )
                assert workspace is not None
                state = result["fingerprint"]
                if workspace.tree_hash != state["tree_hash"]:
                    latest = await self.session.scalar(
                        select(WorkspaceCheckpoint)
                        .where(WorkspaceCheckpoint.workspace_id == workspace.id)
                        .order_by(WorkspaceCheckpoint.sequence_number.desc())
                        .limit(1)
                        .with_for_update()
                    )
                    assert latest is not None
                    checkpoint = WorkspaceCheckpoint(
                        id=uuid4(),
                        workspace_id=workspace.id,
                        sequence_number=latest.sequence_number + 1,
                        git_head=state["git_head"],
                        diff_hash=state["diff_hash"],
                        tree_hash=state["tree_hash"],
                        file_hashes=state["file_hashes"],
                        reason="PATCH_APPLIED",
                        operation_id=operation_id,
                    )
                    self.session.add(checkpoint)
                    workspace.last_checkpoint_id = checkpoint.id
                    workspace.tree_hash = state["tree_hash"]
                    workspace.diff_hash = state["diff_hash"]
                    workspace.file_hashes = state["file_hashes"]

    async def workspace(self, workspace_id: UUID) -> CodingWorkspace:
        workspace = await self.session.get(CodingWorkspace, workspace_id)
        assert workspace is not None
        return workspace

    async def mark_failed(self, workspace_id: UUID) -> None:
        async with self.session.begin():
            await self._fence()
            workspace = await self.session.get(CodingWorkspace, workspace_id, with_for_update=True)
            assert workspace is not None
            if workspace.status in {
                WorkspaceStatus.FAILED,
                WorkspaceStatus.COMPLETED,
                WorkspaceStatus.CANCELLED,
            }:
                return
            validate_workspace_transition(workspace.status, WorkspaceStatus.FAILED)
            workspace.status = WorkspaceStatus.FAILED

    async def request_approval(self, workspace_id: UUID, final_response: str) -> UUID:
        async with self.session.begin():
            await self._fence()
            workspace = await self.session.get(CodingWorkspace, workspace_id, with_for_update=True)
            assert workspace is not None
            existing = await self.session.scalar(
                select(ApprovalRequest).where(ApprovalRequest.workspace_id == workspace_id)
            )
            if existing is not None:
                return existing.id
            passing_test = await self.session.scalar(
                select(SandboxCommand)
                .where(
                    SandboxCommand.workspace_id == workspace_id,
                    SandboxCommand.command_type == "run_tests",
                    SandboxCommand.status == CommandStatus.SUCCEEDED,
                    SandboxCommand.exit_code == 0,
                )
                .order_by(SandboxCommand.started_at.desc())
                .limit(1)
            )
            if passing_test is None:
                raise PermanentToolError("TESTS_NOT_PASSED", "No passing sandbox test record")
            checkpoint = await self.session.scalar(
                select(WorkspaceCheckpoint)
                .where(
                    WorkspaceCheckpoint.workspace_id == workspace_id,
                    WorkspaceCheckpoint.reason == "PATCH_APPLIED",
                )
                .order_by(WorkspaceCheckpoint.sequence_number.desc())
                .limit(1)
            )
            if checkpoint is None:
                raise PermanentToolError("NO_CODING_CHANGE", "No durable patch checkpoint")
            if passing_test.started_at < checkpoint.created_at:
                raise PermanentToolError("TESTS_STALE", "Tests predate the latest patch")
            approval_id = uuid4()
            self.session.add(
                ApprovalRequest(
                    id=approval_id,
                    workflow_id=workspace.workflow_id,
                    step_id=workspace.step_id,
                    workspace_id=workspace_id,
                    action_type="COMMIT_PATCH",
                    status=ApprovalStatus.PENDING,
                    operation_id=f"continuum:git-commit:{approval_id}",
                    summary=final_response[:2000],
                    payload={
                        "diff_hash": workspace.diff_hash,
                        "tree_hash": workspace.tree_hash,
                        "test_command_id": str(passing_test.id),
                    },
                    requested_at=datetime.now(UTC),
                )
            )
            validate_workspace_transition(workspace.status, WorkspaceStatus.WAITING_APPROVAL)
            workspace.status = WorkspaceStatus.WAITING_APPROVAL
            return approval_id

    async def finalize_commit(
        self, workspace_id: UUID, approval_id: UUID, result: dict[str, Any]
    ) -> None:
        async with self.session.begin():
            await self._fence()
            workspace = await self.session.get(CodingWorkspace, workspace_id, with_for_update=True)
            approval = await self.session.get(ApprovalRequest, approval_id, with_for_update=True)
            assert workspace is not None and approval is not None
            if approval.status != ApprovalStatus.APPROVED:
                raise PermanentToolError("APPROVAL_REQUIRED", "Commit approval is not durable")
            if approval.commit_sha is not None:
                if approval.commit_sha != result["commit_sha"]:
                    raise PermanentToolError("COMMIT_RECONCILIATION_FAILED", "Commit SHA changed")
                return
            state = result["fingerprint"]
            if state["tree_hash"] != workspace.tree_hash:
                raise PermanentToolError(
                    "WORKSPACE_RECONCILIATION_FAILED", "Post-commit tree changed"
                )
            approval.commit_sha = result["commit_sha"]
            workspace.current_git_head = result["commit_sha"]
            workspace.final_git_head = result["commit_sha"]
            workspace.diff_hash = state["diff_hash"]
            validate_workspace_transition(workspace.status, WorkspaceStatus.COMPLETED)
            workspace.status = WorkspaceStatus.COMPLETED
            workspace.completed_at = datetime.now(UTC)


class ApprovalService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def decide(
        self, approval_id: UUID, target: ApprovalStatus, *, reason: str | None = None
    ) -> ApprovalRequest:
        if target not in {ApprovalStatus.APPROVED, ApprovalStatus.REJECTED}:
            raise ValueError("Only approve or reject commands are public")
        async with self.session.begin():
            identity = await self.session.get(ApprovalRequest, approval_id)
            if identity is None:
                raise ApprovalNotFound(approval_id)
            workflow = await self.session.scalar(
                select(Workflow).where(Workflow.id == identity.workflow_id).with_for_update()
            )
            assert workflow is not None
            request = await self.session.scalar(
                select(ApprovalRequest).where(ApprovalRequest.id == approval_id).with_for_update()
            )
            assert request is not None
            if request.status == target:
                return request
            if workflow.status != WorkflowStatus.RUNNING:
                raise WorkflowConflict("Cannot decide approval for a terminal workflow")
            validate_approval_transition(request.status, target)
            request.status = target
            request.decided_at = datetime.now(UTC)
            request.decision_reason = reason
            return request
