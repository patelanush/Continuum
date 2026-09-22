from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    desc,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from durable_agent_runtime.db.base import Base
from durable_agent_runtime.domain.enums import (
    EntityType,
    ExecutionAttemptStatus,
    StepStatus,
    WorkflowStatus,
)


def enum_values(
    enum_class: type[WorkflowStatus]
    | type[StepStatus]
    | type[EntityType]
    | type[ExecutionAttemptStatus],
) -> list[str]:
    return [item.value for item in enum_class]


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=lambda: datetime.now(UTC),
        nullable=False,
    )


class Workflow(TimestampMixin, Base):
    __tablename__ = "workflows"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    workflow_type: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[WorkflowStatus] = mapped_column(
        Enum(
            WorkflowStatus,
            name="workflow_status",
            native_enum=False,
            values_callable=enum_values,
            create_constraint=True,
        ),
        default=WorkflowStatus.PENDING,
        nullable=False,
        index=True,
    )
    input: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
    output: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    current_step_position: Mapped[int | None] = mapped_column(Integer, nullable=True)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    steps: Mapped[list[WorkflowStep]] = relationship(
        back_populates="workflow",
        cascade="all, delete-orphan",
        order_by="WorkflowStep.position",
        lazy="selectin",
    )

    __table_args__ = (
        CheckConstraint("char_length(workflow_type) > 0", name="workflow_type_nonempty"),
        CheckConstraint("version >= 1", name="workflow_version_positive"),
        CheckConstraint(
            "current_step_position IS NULL OR current_step_position >= 0",
            name="workflow_current_position_nonnegative",
        ),
        Index("ix_workflows_created_at_id", desc("created_at"), desc("id")),
    )


class WorkflowStep(TimestampMixin, Base):
    __tablename__ = "workflow_steps"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    workflow_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("workflows.id", ondelete="CASCADE"), nullable=False
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    step_type: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[StepStatus] = mapped_column(
        Enum(
            StepStatus,
            name="step_status",
            native_enum=False,
            values_callable=enum_values,
            create_constraint=True,
        ),
        default=StepStatus.PENDING,
        nullable=False,
        index=True,
    )
    input: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
    output: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3, nullable=False)
    error_code: Mapped[str | None] = mapped_column(String(100))
    error_detail: Mapped[str | None] = mapped_column(Text)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    workflow: Mapped[Workflow] = relationship(back_populates="steps")

    __table_args__ = (
        CheckConstraint("position >= 0", name="step_position_nonnegative"),
        CheckConstraint("char_length(name) > 0", name="step_name_nonempty"),
        CheckConstraint("char_length(step_type) > 0", name="step_type_nonempty"),
        CheckConstraint("attempt_count >= 0", name="step_attempt_count_nonnegative"),
        CheckConstraint("max_attempts >= 1", name="step_max_attempts_positive"),
        CheckConstraint("version >= 1", name="step_version_positive"),
        Index("uq_workflow_steps_workflow_position", workflow_id, position, unique=True),
        Index("ix_workflow_steps_workflow_status", workflow_id, status),
        Index(
            "uq_workflow_steps_one_active",
            workflow_id,
            unique=True,
            postgresql_where=status.in_([StepStatus.READY, StepStatus.RUNNING]),
        ),
    )


class StateTransition(Base):
    __tablename__ = "state_transitions"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    entity_type: Mapped[EntityType] = mapped_column(
        Enum(
            EntityType,
            name="entity_type",
            native_enum=False,
            values_callable=enum_values,
            create_constraint=True,
        ),
        nullable=False,
    )
    entity_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    workflow_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("workflows.id", ondelete="CASCADE"), nullable=False
    )
    from_status: Mapped[str | None] = mapped_column(String(30))
    to_status: Mapped[str] = mapped_column(String(30), nullable=False)
    reason: Mapped[str | None] = mapped_column(Text)
    details: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        CheckConstraint("char_length(to_status) > 0", name="transition_to_status_nonempty"),
        Index("ix_state_transitions_workflow_created", workflow_id, created_at, id),
        Index("ix_state_transitions_entity", entity_type, entity_id),
    )


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    workflow_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("workflows.id", ondelete="CASCADE"), nullable=False
    )
    step_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("workflow_steps.id", ondelete="CASCADE")
    )
    correlation_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    causation_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    topic: Mapped[str] = mapped_column(String(200), nullable=False)
    message_key: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint("schema_version >= 1", name="outbox_schema_version_positive"),
        CheckConstraint("publish_attempts >= 0", name="outbox_attempts_nonnegative"),
        Index(
            "ix_outbox_events_unpublished", created_at, id, postgresql_where=published_at.is_(None)
        ),
        Index("ix_outbox_events_workflow_id", workflow_id),
    )


class ConsumedEvent(Base):
    __tablename__ = "consumed_events"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    consumer_group: Mapped[str] = mapped_column(String(100), nullable=False)
    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    workflow_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("workflows.id", ondelete="CASCADE")
    )
    step_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("workflow_steps.id", ondelete="CASCADE")
    )
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    worker_id: Mapped[str] = mapped_column(String(200), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        Index("uq_consumed_events_group_event", consumer_group, event_id, unique=True),
        Index("ix_consumed_events_workflow_id", workflow_id),
    )


class ExecutionAttempt(TimestampMixin, Base):
    __tablename__ = "execution_attempts"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    workflow_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("workflows.id", ondelete="CASCADE"), nullable=False
    )
    step_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("workflow_steps.id", ondelete="CASCADE"), nullable=False
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[ExecutionAttemptStatus] = mapped_column(
        Enum(
            ExecutionAttemptStatus,
            name="execution_attempt_status",
            native_enum=False,
            values_callable=enum_values,
            create_constraint=True,
        ),
        nullable=False,
        default=ExecutionAttemptStatus.PENDING,
    )
    executor_id: Mapped[str | None] = mapped_column(String(200))
    lease_token: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_eligible_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    output: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    error_code: Mapped[str | None] = mapped_column(String(100))
    error_detail: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint("attempt_number >= 1", name="attempt_number_positive"),
        CheckConstraint(
            "status <> 'RUNNING' OR (executor_id IS NOT NULL AND lease_token IS NOT NULL "
            "AND lease_expires_at IS NOT NULL)",
            name="running_attempt_has_lease",
        ),
        Index("uq_execution_attempts_step_number", step_id, attempt_number, unique=True),
        Index(
            "uq_execution_attempts_one_active",
            step_id,
            unique=True,
            postgresql_where=status.in_(
                [ExecutionAttemptStatus.PENDING, ExecutionAttemptStatus.RUNNING]
            ),
        ),
        Index(
            "ix_execution_attempts_pending",
            next_eligible_at,
            "created_at",
            id,
            postgresql_where=status == ExecutionAttemptStatus.PENDING,
        ),
        Index(
            "ix_execution_attempts_expiring",
            lease_expires_at,
            id,
            postgresql_where=status == ExecutionAttemptStatus.RUNNING,
        ),
        Index("ix_execution_attempts_workflow_id", workflow_id),
    )
