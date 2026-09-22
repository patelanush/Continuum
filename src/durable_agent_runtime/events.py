"""Versioned JSON messages shared by the dispatcher and workers."""

from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from durable_agent_runtime.db.models import OutboxEvent

STEP_READY_TOPIC = "continuum.step.ready.v1"
DEAD_LETTER_TOPIC = "continuum.dead-letter.v1"


class StepReadyEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    event_type: Literal["step.ready"]
    schema_version: Literal[1]
    occurred_at: datetime
    workflow_id: UUID
    step_id: UUID
    correlation_id: UUID
    causation_id: UUID | None
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("occurred_at")
    @classmethod
    def utc_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("occurred_at must include a timezone")
        return value.astimezone(UTC)

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> "StepReadyEvent":
        return cls.model_validate_json(raw)

    @classmethod
    def from_outbox(cls, row: OutboxEvent) -> "StepReadyEvent":
        return cls(
            event_id=row.id,
            event_type=row.event_type,
            schema_version=row.schema_version,
            occurred_at=row.created_at,
            workflow_id=row.workflow_id,
            step_id=row.step_id,
            correlation_id=row.correlation_id,
            causation_id=row.causation_id,
            payload=row.payload,
        )


def workflow_message_key(workflow_id: UUID) -> bytes:
    return str(workflow_id).encode("ascii")
