"""Read-only durable agent trajectory; raw prompts/responses are not exposed."""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from durable_agent_runtime.domain.enums import (
    AgentRunStatus,
    AgentToolCallStatus,
    AgentTurnStatus,
    ModelCallStatus,
)


class ModelCallSummary(BaseModel):
    id: UUID
    attempt_number: int
    status: ModelCallStatus
    provider: str
    model: str
    request_hash: str
    prompt_tokens: int | None
    completion_tokens: int | None
    latency_ms: int | None
    error_code: str | None


class AgentToolSummary(BaseModel):
    id: UUID
    tool_name: str
    arguments: dict[str, Any]
    arguments_hash: str
    status: AgentToolCallStatus
    operation_id: str
    tool_semantics: str
    result: dict[str, Any] | None
    error_code: str | None


class AgentTurnTrace(BaseModel):
    id: UUID
    turn_number: int
    status: AgentTurnStatus
    model_request_hash: str
    decision: dict[str, Any] | None
    final_response: str | None
    model_calls: list[ModelCallSummary]
    tool_call: AgentToolSummary | None


class AgentRunTrace(BaseModel):
    id: UUID
    step_id: UUID
    status: AgentRunStatus
    agent_type: str
    provider: str
    model: str
    system_prompt_version: str
    max_turns: int
    current_turn_number: int
    final_response: str | None
    error_code: str | None
    created_at: datetime
    completed_at: datetime | None
    turns: list[AgentTurnTrace]
