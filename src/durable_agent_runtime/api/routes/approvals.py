"""Explicit local approval boundary; no arbitrary Git or status endpoints."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query
from sqlalchemy import select

from durable_agent_runtime.api.dependencies import DatabaseSession
from durable_agent_runtime.coding.service import ApprovalService
from durable_agent_runtime.db.models import ApprovalRequest, CodingWorkspace
from durable_agent_runtime.domain.enums import ApprovalStatus
from durable_agent_runtime.domain.errors import ApprovalNotFound, WorkflowNotFound
from durable_agent_runtime.observability.runtime import span
from durable_agent_runtime.schemas.coding import (
    ApprovalDecision,
    ApprovalResponse,
    CodingWorkspaceResponse,
)

router = APIRouter(prefix="/api/v1", tags=["coding approvals"])


@router.get("/approvals", response_model=list[ApprovalResponse])
async def list_approvals(
    session: DatabaseSession,
    status: Annotated[ApprovalStatus | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> object:
    query = select(ApprovalRequest).order_by(ApprovalRequest.requested_at.desc()).limit(limit)
    if status is not None:
        query = query.where(ApprovalRequest.status == status)
    return list(await session.scalars(query))


@router.get("/approvals/{approval_id}", response_model=ApprovalResponse)
async def get_approval(approval_id: UUID, session: DatabaseSession) -> object:
    request = await session.get(ApprovalRequest, approval_id)
    if request is None:
        raise ApprovalNotFound(approval_id)
    return request


@router.post("/approvals/{approval_id}/approve", response_model=ApprovalResponse)
async def approve(
    approval_id: UUID, session: DatabaseSession, body: ApprovalDecision | None = None
) -> object:
    with span("api.approval.approve", {"continuum.approval.id": str(approval_id)}):
        return await ApprovalService(session).decide(
            approval_id, ApprovalStatus.APPROVED, reason=body.reason if body else None
        )


@router.post("/approvals/{approval_id}/reject", response_model=ApprovalResponse)
async def reject(
    approval_id: UUID, session: DatabaseSession, body: ApprovalDecision | None = None
) -> object:
    with span("api.approval.reject", {"continuum.approval.id": str(approval_id)}):
        return await ApprovalService(session).decide(
            approval_id, ApprovalStatus.REJECTED, reason=body.reason if body else None
        )


@router.get("/workflows/{workflow_id}/coding", response_model=list[CodingWorkspaceResponse])
async def workflow_coding(workflow_id: UUID, session: DatabaseSession) -> object:
    from durable_agent_runtime.db.models import Workflow

    if await session.get(Workflow, workflow_id) is None:
        raise WorkflowNotFound(workflow_id)
    return list(
        await session.scalars(
            select(CodingWorkspace).where(CodingWorkspace.workflow_id == workflow_id)
        )
    )
