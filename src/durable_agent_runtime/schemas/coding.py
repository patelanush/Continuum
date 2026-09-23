"""Read-only coding workspace and explicit approval API representations."""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from durable_agent_runtime.domain.enums import ApprovalStatus, WorkspaceStatus


class ApprovalDecision(BaseModel):
    reason: str | None = Field(default=None, max_length=1000)


class ApprovalResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    workflow_id: UUID
    step_id: UUID
    workspace_id: UUID
    action_type: str
    status: ApprovalStatus
    summary: str
    payload: dict[str, Any]
    commit_sha: str | None
    requested_at: datetime
    decided_at: datetime | None
    decision_reason: str | None


class CodingWorkspaceResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    workflow_id: UUID
    step_id: UUID
    agent_run_id: UUID
    status: WorkspaceStatus
    repository_source: str
    baseline_git_head: str | None
    current_git_head: str | None
    final_git_head: str | None
    diff_hash: str | None
    tree_hash: str | None
    last_checkpoint_id: UUID | None
