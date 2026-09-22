from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from durable_agent_runtime.domain.enums import EntityType, StepStatus, WorkflowStatus


class StepCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    step_type: str = Field(min_length=1, max_length=100)
    input: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = Field(default=3, ge=1)

    @field_validator("name", "step_type")
    @classmethod
    def not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be blank")
        return value.strip()


class WorkflowCreate(BaseModel):
    workflow_type: str = Field(min_length=1, max_length=100)
    input: dict[str, Any] = Field(default_factory=dict)
    steps: list[StepCreate] = Field(min_length=1)

    @field_validator("workflow_type")
    @classmethod
    def workflow_type_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be blank")
        return value.strip()


class StepResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    position: int
    name: str
    step_type: str
    status: StepStatus
    input: dict[str, Any]
    output: dict[str, Any] | None
    attempt_count: int
    max_attempts: int
    error_code: str | None
    error_detail: str | None
    version: int
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    completed_at: datetime | None


class WorkflowResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    workflow_type: str
    status: WorkflowStatus
    input: dict[str, Any]
    output: dict[str, Any] | None
    current_step_position: int | None
    version: int
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    steps: list[StepResponse]


class WorkflowListResponse(BaseModel):
    items: list[WorkflowResponse]
    total: int
    limit: int
    offset: int


class CancelRequest(BaseModel):
    reason: str | None = Field(default=None, max_length=2000)


class TransitionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    entity_type: EntityType
    entity_id: UUID
    workflow_id: UUID
    from_status: str | None
    to_status: str
    reason: str | None
    details: dict[str, Any] | None
    created_at: datetime
