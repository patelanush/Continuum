"""Persist minimal W3C context at durable asynchronous boundaries.

Revision ID: 20260923_0007
Revises: d8f9c5a9b461
"""

import sqlalchemy as sa

from alembic import op

revision = "20260923_0007"
down_revision = "d8f9c5a9b461"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("outbox_events", sa.Column("traceparent", sa.String(55), nullable=True))
    op.add_column("execution_attempts", sa.Column("traceparent", sa.String(55), nullable=True))


def downgrade() -> None:
    op.drop_column("execution_attempts", "traceparent")
    op.drop_column("outbox_events", "traceparent")
