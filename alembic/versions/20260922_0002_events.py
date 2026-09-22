"""Add transactional outbox and durable consumer inbox.

Revision ID: 20260922_0002
Revises: 20260922_0001
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260922_0002"
down_revision: str | None = "20260922_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("schema_version", sa.Integer, nullable=False),
        sa.Column(
            "workflow_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("workflows.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "step_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("workflow_steps.id", ondelete="CASCADE"),
        ),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("causation_id", postgresql.UUID(as_uuid=True)),
        sa.Column("payload", postgresql.JSONB, nullable=False),
        sa.Column("topic", sa.String(200), nullable=False),
        sa.Column("message_key", sa.String(100), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("publish_attempts", sa.Integer, nullable=False),
        sa.Column("last_error", sa.Text),
        sa.CheckConstraint(
            "schema_version >= 1", name="ck_outbox_events_outbox_schema_version_positive"
        ),
        sa.CheckConstraint(
            "publish_attempts >= 0", name="ck_outbox_events_outbox_attempts_nonnegative"
        ),
    )
    op.create_index("ix_outbox_events_workflow_id", "outbox_events", ["workflow_id"])
    op.create_index(
        "ix_outbox_events_unpublished",
        "outbox_events",
        ["created_at", "id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_table(
        "consumed_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("consumer_group", sa.String(100), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "workflow_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("workflows.id", ondelete="CASCADE"),
        ),
        sa.Column(
            "step_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("workflow_steps.id", ondelete="CASCADE"),
        ),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("worker_id", sa.String(200), nullable=False),
        sa.Column(
            "processed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    )
    op.create_index(
        "uq_consumed_events_group_event",
        "consumed_events",
        ["consumer_group", "event_id"],
        unique=True,
    )
    op.create_index("ix_consumed_events_workflow_id", "consumed_events", ["workflow_id"])


def downgrade() -> None:
    op.drop_table("consumed_events")
    op.drop_table("outbox_events")
