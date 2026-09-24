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
    UniqueConstraint,
    desc,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from durable_agent_runtime.db.base import Base
from durable_agent_runtime.domain.enums import (
    AgentRunStatus,
    AgentToolCallStatus,
    AgentTurnStatus,
    ApprovalStatus,
    CommandStatus,
    EntityType,
    ExecutionAttemptStatus,
    ModelCallStatus,
    SandboxStatus,
    StepStatus,
    WorkflowStatus,
    WorkspaceStatus,
)


def enum_values(
    enum_class: type[WorkflowStatus]
    | type[StepStatus]
    | type[EntityType]
    | type[ExecutionAttemptStatus]
    | type[AgentRunStatus]
    | type[AgentTurnStatus]
    | type[ModelCallStatus]
    | type[AgentToolCallStatus]
    | type[WorkspaceStatus]
    | type[SandboxStatus]
    | type[CommandStatus]
    | type[ApprovalStatus],
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
    traceparent: Mapped[str | None] = mapped_column(String(55))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    topic: Mapped[str] = mapped_column(String(200), nullable=False)
    message_key: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_lease_token: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True))
    publish_lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint("schema_version >= 1", name="outbox_schema_version_positive"),
        CheckConstraint("publish_attempts >= 0", name="outbox_attempts_nonnegative"),
        CheckConstraint(
            "(publish_lease_token IS NULL) = (publish_lease_expires_at IS NULL)",
            name="ck_outbox_publish_lease_pair",
        ),
        Index(
            "ix_outbox_events_unpublished", created_at, id, postgresql_where=published_at.is_(None)
        ),
        Index("ix_outbox_events_workflow_id", workflow_id),
        Index(
            "ix_outbox_events_available",
            publish_lease_expires_at,
            created_at,
            postgresql_where=published_at.is_(None),
        ),
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
    traceparent: Mapped[str | None] = mapped_column(String(55))
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


class AgentRun(TimestampMixin, Base):
    __tablename__ = "agent_runs"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    workflow_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("workflows.id", ondelete="CASCADE"), nullable=False
    )
    step_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("workflow_steps.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[AgentRunStatus] = mapped_column(
        Enum(
            AgentRunStatus,
            name="agent_run_status",
            native_enum=False,
            values_callable=enum_values,
            create_constraint=True,
        ),
        nullable=False,
    )
    agent_type: Mapped[str] = mapped_column(String(100), nullable=False)
    provider: Mapped[str] = mapped_column(String(100), nullable=False)
    model: Mapped[str] = mapped_column(String(200), nullable=False)
    system_prompt_version: Mapped[str] = mapped_column(String(100), nullable=False)
    max_turns: Mapped[int] = mapped_column(Integer, nullable=False)
    current_turn_number: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    final_response: Mapped[str | None] = mapped_column(Text)
    error_code: Mapped[str | None] = mapped_column(String(100))
    error_detail: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint("step_id", name="uq_agent_runs_step_id"),
        CheckConstraint("max_turns >= 1", name="agent_run_max_turns_positive"),
        CheckConstraint("current_turn_number >= 1", name="agent_run_turn_positive"),
        Index("ix_agent_runs_workflow_id", workflow_id),
    )


class AgentTurn(TimestampMixin, Base):
    __tablename__ = "agent_turns"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    agent_run_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("agent_runs.id", ondelete="CASCADE"), nullable=False
    )
    turn_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[AgentTurnStatus] = mapped_column(
        Enum(
            AgentTurnStatus,
            name="agent_turn_status",
            native_enum=False,
            values_callable=enum_values,
            create_constraint=True,
        ),
        nullable=False,
    )
    model_request: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    model_request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    decision: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    decision_type: Mapped[str | None] = mapped_column(String(30))
    final_response: Mapped[str | None] = mapped_column(Text)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint("agent_run_id", "turn_number", name="uq_agent_turns_run_number"),
        CheckConstraint("turn_number >= 1", name="agent_turn_number_positive"),
        Index("ix_agent_turns_run_number", agent_run_id, turn_number),
    )


class ModelCall(TimestampMixin, Base):
    __tablename__ = "model_calls"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    agent_turn_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("agent_turns.id", ondelete="CASCADE"), nullable=False
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[ModelCallStatus] = mapped_column(
        Enum(
            ModelCallStatus,
            name="model_call_status",
            native_enum=False,
            values_callable=enum_values,
            create_constraint=True,
        ),
        nullable=False,
    )
    provider: Mapped[str] = mapped_column(String(100), nullable=False)
    model: Mapped[str] = mapped_column(String(200), nullable=False)
    request: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    raw_response: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    validated_decision: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    prompt_tokens: Mapped[int | None] = mapped_column(Integer)
    completion_tokens: Mapped[int | None] = mapped_column(Integer)
    total_tokens: Mapped[int | None] = mapped_column(Integer)
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    error_code: Mapped[str | None] = mapped_column(String(100))
    error_detail: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint("agent_turn_id", "attempt_number", name="uq_model_calls_turn_attempt"),
        CheckConstraint("attempt_number >= 1", name="model_call_attempt_positive"),
        Index("ix_model_calls_turn_attempt", agent_turn_id, attempt_number),
    )


class AgentToolCall(TimestampMixin, Base):
    __tablename__ = "agent_tool_calls"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    agent_run_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("agent_runs.id", ondelete="CASCADE"), nullable=False
    )
    agent_turn_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("agent_turns.id", ondelete="CASCADE"), nullable=False
    )
    tool_name: Mapped[str] = mapped_column(String(100), nullable=False)
    arguments: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    arguments_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[AgentToolCallStatus] = mapped_column(
        Enum(
            AgentToolCallStatus,
            name="agent_tool_call_status",
            native_enum=False,
            values_callable=enum_values,
            create_constraint=True,
        ),
        nullable=False,
    )
    operation_id: Mapped[str] = mapped_column(String(120), nullable=False)
    tool_semantics: Mapped[str] = mapped_column(String(40), nullable=False)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    error_code: Mapped[str | None] = mapped_column(String(100))
    error_detail: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint("agent_turn_id", name="uq_agent_tool_calls_turn_id"),
        UniqueConstraint("operation_id", name="uq_agent_tool_calls_operation_id"),
        Index("ix_agent_tool_calls_run_id", agent_run_id),
    )


class CodingWorkspace(TimestampMixin, Base):
    __tablename__ = "coding_workspaces"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    workflow_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("workflows.id", ondelete="CASCADE"), nullable=False
    )
    step_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("workflow_steps.id", ondelete="CASCADE"), nullable=False
    )
    agent_run_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("agent_runs.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[WorkspaceStatus] = mapped_column(
        Enum(
            WorkspaceStatus,
            name="workspace_status",
            native_enum=False,
            values_callable=enum_values,
            create_constraint=True,
        ),
        nullable=False,
    )
    repository_source: Mapped[str] = mapped_column(String(300), nullable=False)
    workspace_key: Mapped[str] = mapped_column(String(100), nullable=False)
    volume_name: Mapped[str] = mapped_column(String(160), nullable=False)
    baseline_git_head: Mapped[str | None] = mapped_column(String(40))
    current_git_head: Mapped[str | None] = mapped_column(String(40))
    final_git_head: Mapped[str | None] = mapped_column(String(40))
    diff_hash: Mapped[str | None] = mapped_column(String(64))
    tree_hash: Mapped[str | None] = mapped_column(String(64))
    file_hashes: Mapped[dict[str, str] | None] = mapped_column(JSONB)
    test_command: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    last_checkpoint_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint("step_id", name="uq_coding_workspaces_step_id"),
        UniqueConstraint("agent_run_id", name="uq_coding_workspaces_agent_run_id"),
        UniqueConstraint("workspace_key", name="uq_coding_workspaces_key"),
        UniqueConstraint("volume_name", name="uq_coding_workspaces_volume"),
        Index("ix_coding_workspaces_workflow_id", workflow_id),
    )


class WorkspaceCheckpoint(Base):
    __tablename__ = "workspace_checkpoints"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    workspace_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("coding_workspaces.id", ondelete="CASCADE"), nullable=False
    )
    sequence_number: Mapped[int] = mapped_column(Integer, nullable=False)
    git_head: Mapped[str] = mapped_column(String(40), nullable=False)
    diff_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    tree_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    file_hashes: Mapped[dict[str, str]] = mapped_column(JSONB, nullable=False)
    operation_id: Mapped[str | None] = mapped_column(String(120))
    reason: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "workspace_id", "sequence_number", name="uq_workspace_checkpoint_sequence"
        ),
        UniqueConstraint("workspace_id", "operation_id", name="uq_workspace_checkpoint_operation"),
        CheckConstraint("sequence_number >= 1", name="workspace_checkpoint_sequence_positive"),
        Index("ix_workspace_checkpoints_workspace_id", workspace_id),
    )


class SandboxExecution(Base):
    __tablename__ = "sandbox_executions"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    workspace_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("coding_workspaces.id", ondelete="CASCADE"), nullable=False
    )
    execution_attempt_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("execution_attempts.id", ondelete="CASCADE"),
        nullable=False,
    )
    container_ref: Mapped[str] = mapped_column(String(160), nullable=False)
    status: Mapped[SandboxStatus] = mapped_column(
        Enum(
            SandboxStatus,
            name="sandbox_status",
            native_enum=False,
            values_callable=enum_values,
            create_constraint=True,
        ),
        nullable=False,
    )
    image: Mapped[str] = mapped_column(String(200), nullable=False)
    resource_limits: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    network_mode: Mapped[str] = mapped_column(String(30), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    exit_reason: Mapped[str | None] = mapped_column(String(200))

    __table_args__ = (
        UniqueConstraint("container_ref", name="uq_sandbox_executions_container"),
        Index("ix_sandbox_executions_workspace_id", workspace_id),
    )


class SandboxCommand(Base):
    __tablename__ = "sandbox_commands"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    workspace_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("coding_workspaces.id", ondelete="CASCADE"), nullable=False
    )
    agent_tool_call_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("agent_tool_calls.id", ondelete="SET NULL")
    )
    command_type: Mapped[str] = mapped_column(String(100), nullable=False)
    argv: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    status: Mapped[CommandStatus] = mapped_column(
        Enum(
            CommandStatus,
            name="sandbox_command_status",
            native_enum=False,
            values_callable=enum_values,
            create_constraint=True,
        ),
        nullable=False,
    )
    exit_code: Mapped[int | None] = mapped_column(Integer)
    stdout_excerpt: Mapped[str | None] = mapped_column(Text)
    stderr_excerpt: Mapped[str | None] = mapped_column(Text)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    timeout_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint("timeout_ms > 0", name="sandbox_command_timeout_positive"),
        Index("ix_sandbox_commands_workspace_id", workspace_id),
        Index("ix_sandbox_commands_tool_call_id", agent_tool_call_id),
    )


class ApprovalRequest(Base):
    __tablename__ = "approval_requests"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    workflow_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("workflows.id", ondelete="CASCADE"), nullable=False
    )
    step_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("workflow_steps.id", ondelete="CASCADE"), nullable=False
    )
    workspace_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("coding_workspaces.id", ondelete="CASCADE"), nullable=False
    )
    action_type: Mapped[str] = mapped_column(String(40), nullable=False)
    status: Mapped[ApprovalStatus] = mapped_column(
        Enum(
            ApprovalStatus,
            name="approval_status",
            native_enum=False,
            values_callable=enum_values,
            create_constraint=True,
        ),
        nullable=False,
    )
    operation_id: Mapped[str] = mapped_column(String(120), nullable=False)
    traceparent: Mapped[str | None] = mapped_column(String(55))
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    commit_sha: Mapped[str | None] = mapped_column(String(40))
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    decision_reason: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        UniqueConstraint(
            "workspace_id", "action_type", name="uq_approval_requests_workspace_action"
        ),
        UniqueConstraint("operation_id", name="uq_approval_requests_operation_id"),
        Index("ix_approval_requests_status", status),
        Index("ix_approval_requests_workflow_id", workflow_id),
    )
