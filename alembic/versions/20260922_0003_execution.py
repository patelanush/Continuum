"""Add durable execution attempts and lease ownership.

Revision ID: 20260922_0003
Revises: 20260922_0002
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260922_0003"
down_revision: str | None = "20260922_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "execution_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
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
            nullable=False,
        ),
        sa.Column("attempt_number", sa.Integer, nullable=False),
        sa.Column("status", sa.String(9), nullable=False),
        sa.Column("executor_id", sa.String(200)),
        sa.Column("lease_token", postgresql.UUID(as_uuid=True)),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True)),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("next_eligible_at", sa.DateTime(timezone=True)),
        sa.Column("output", postgresql.JSONB),
        sa.Column("error_code", sa.String(100)),
        sa.Column("error_detail", sa.Text),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "attempt_number >= 1", name="ck_execution_attempts_attempt_number_positive"
        ),
        sa.CheckConstraint(
            "status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','EXPIRED','CANCELLED')",
            name="execution_attempt_status",
        ),
        sa.CheckConstraint(
            "status <> 'RUNNING' OR (executor_id IS NOT NULL AND lease_token IS NOT NULL "
            "AND lease_expires_at IS NOT NULL)",
            name="ck_execution_attempts_running_attempt_has_lease",
        ),
    )
    op.create_index(
        "uq_execution_attempts_step_number",
        "execution_attempts",
        ["step_id", "attempt_number"],
        unique=True,
    )
    op.create_index(
        "uq_execution_attempts_one_active",
        "execution_attempts",
        ["step_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('PENDING', 'RUNNING')"),
    )
    op.create_index(
        "ix_execution_attempts_pending",
        "execution_attempts",
        ["next_eligible_at", "created_at", "id"],
        postgresql_where=sa.text("status = 'PENDING'"),
    )
    op.create_index(
        "ix_execution_attempts_expiring",
        "execution_attempts",
        ["lease_expires_at", "id"],
        postgresql_where=sa.text("status = 'RUNNING'"),
    )
    op.create_index("ix_execution_attempts_workflow_id", "execution_attempts", ["workflow_id"])


def downgrade() -> None:
    op.drop_table("execution_attempts")
