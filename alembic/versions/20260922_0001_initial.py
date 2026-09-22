"""Create Phase 1 workflow tables.

Revision ID: 20260922_0001
Revises: None
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260922_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

workflow_status = sa.Enum(
    "PENDING",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "CANCELLED",
    name="workflow_status",
    native_enum=False,
    create_constraint=True,
)
step_status = sa.Enum(
    "PENDING",
    "READY",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "CANCELLED",
    name="step_status",
    native_enum=False,
    create_constraint=True,
)
entity_type = sa.Enum(
    "workflow",
    "step",
    name="entity_type",
    native_enum=False,
    create_constraint=True,
)


def upgrade() -> None:
    op.create_table(
        "workflows",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("workflow_type", sa.String(length=100), nullable=False),
        sa.Column("status", workflow_status, nullable=False),
        sa.Column("input", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("output", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("current_step_position", sa.Integer(), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "char_length(workflow_type) > 0", name="ck_workflows_workflow_type_nonempty"
        ),
        sa.CheckConstraint("version >= 1", name="ck_workflows_workflow_version_positive"),
        sa.CheckConstraint(
            "current_step_position IS NULL OR current_step_position >= 0",
            name="ck_workflows_workflow_current_position_nonnegative",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_workflows"),
    )
    op.create_index("ix_workflows_status", "workflows", ["status"])
    op.create_index(
        "ix_workflows_created_at_id", "workflows", [sa.text("created_at DESC"), sa.text("id DESC")]
    )

    op.create_table(
        "workflow_steps",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("workflow_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("step_type", sa.String(length=100), nullable=False),
        sa.Column("status", step_status, nullable=False),
        sa.Column("input", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("output", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("error_code", sa.String(length=100), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("position >= 0", name="ck_workflow_steps_step_position_nonnegative"),
        sa.CheckConstraint("char_length(name) > 0", name="ck_workflow_steps_step_name_nonempty"),
        sa.CheckConstraint(
            "char_length(step_type) > 0", name="ck_workflow_steps_step_type_nonempty"
        ),
        sa.CheckConstraint(
            "attempt_count >= 0", name="ck_workflow_steps_step_attempt_count_nonnegative"
        ),
        sa.CheckConstraint(
            "max_attempts >= 1", name="ck_workflow_steps_step_max_attempts_positive"
        ),
        sa.CheckConstraint("version >= 1", name="ck_workflow_steps_step_version_positive"),
        sa.ForeignKeyConstraint(
            ["workflow_id"],
            ["workflows.id"],
            name="fk_workflow_steps_workflow_id_workflows",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_workflow_steps"),
    )
    op.create_index("ix_workflow_steps_status", "workflow_steps", ["status"])
    op.create_index(
        "ix_workflow_steps_workflow_status", "workflow_steps", ["workflow_id", "status"]
    )
    op.create_index(
        "uq_workflow_steps_workflow_position",
        "workflow_steps",
        ["workflow_id", "position"],
        unique=True,
    )
    op.create_index(
        "uq_workflow_steps_one_active",
        "workflow_steps",
        ["workflow_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('READY', 'RUNNING')"),
    )

    op.create_table(
        "state_transitions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("entity_type", entity_type, nullable=False),
        sa.Column("entity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("workflow_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("from_status", sa.String(length=30), nullable=True),
        sa.Column("to_status", sa.String(length=30), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("details", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "char_length(to_status) > 0", name="ck_state_transitions_transition_to_status_nonempty"
        ),
        sa.ForeignKeyConstraint(
            ["workflow_id"],
            ["workflows.id"],
            name="fk_state_transitions_workflow_id_workflows",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_state_transitions"),
    )
    op.create_index(
        "ix_state_transitions_workflow_created",
        "state_transitions",
        ["workflow_id", "created_at", "id"],
    )
    op.create_index(
        "ix_state_transitions_entity", "state_transitions", ["entity_type", "entity_id"]
    )


def downgrade() -> None:
    op.drop_table("state_transitions")
    op.drop_table("workflow_steps")
    op.drop_table("workflows")
