from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, Query, status

from durable_agent_runtime.api.dependencies import DatabaseSession
from durable_agent_runtime.domain.enums import WorkflowStatus
from durable_agent_runtime.schemas.agents import AgentRunTrace
from durable_agent_runtime.schemas.workflows import (
    AttemptResponse,
    CancelRequest,
    TransitionResponse,
    WorkflowCreate,
    WorkflowListResponse,
    WorkflowResponse,
)
from durable_agent_runtime.services.workflows import WorkflowService

router = APIRouter(prefix="/api/v1/workflows", tags=["workflows"])


@router.post("", response_model=WorkflowResponse, status_code=status.HTTP_201_CREATED)
async def create_workflow(command: WorkflowCreate, session: DatabaseSession) -> object:
    return await WorkflowService(session).create_workflow(command)


@router.get("", response_model=WorkflowListResponse)
async def list_workflows(
    session: DatabaseSession,
    workflow_status: Annotated[WorkflowStatus | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> WorkflowListResponse:
    items, total = await WorkflowService(session).list_workflows(
        status=workflow_status, limit=limit, offset=offset
    )
    return WorkflowListResponse(items=items, total=total, limit=limit, offset=offset)


@router.get("/{workflow_id}", response_model=WorkflowResponse)
async def get_workflow(workflow_id: UUID, session: DatabaseSession) -> object:
    return await WorkflowService(session).get_workflow(workflow_id)


@router.post("/{workflow_id}/start", response_model=WorkflowResponse)
async def start_workflow(workflow_id: UUID, session: DatabaseSession) -> object:
    return await WorkflowService(session).start_workflow(workflow_id)


@router.post("/{workflow_id}/cancel", response_model=WorkflowResponse)
async def cancel_workflow(
    workflow_id: UUID,
    session: DatabaseSession,
    command: Annotated[CancelRequest | None, Body()] = None,
) -> object:
    return await WorkflowService(session).cancel_workflow(
        workflow_id, reason=command.reason if command else None
    )


@router.get("/{workflow_id}/history", response_model=list[TransitionResponse])
async def workflow_history(workflow_id: UUID, session: DatabaseSession) -> object:
    return await WorkflowService(session).get_workflow_history(workflow_id)


@router.get("/{workflow_id}/attempts", response_model=list[AttemptResponse])
async def workflow_attempts(workflow_id: UUID, session: DatabaseSession) -> object:
    return await WorkflowService(session).get_execution_attempts(workflow_id)


@router.get("/{workflow_id}/agent", response_model=list[AgentRunTrace])
async def workflow_agent_trace(workflow_id: UUID, session: DatabaseSession) -> object:
    return await WorkflowService(session).get_agent_trace(workflow_id)
