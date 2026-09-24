"""Correlate asynchronous human approval with its waiting workflow.

Revision ID: 20260923_0008
Revises: 20260923_0007
"""

import sqlalchemy as sa

from alembic import op

revision = "20260923_0008"
down_revision = "20260923_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("approval_requests", sa.Column("traceparent", sa.String(55), nullable=True))


def downgrade() -> None:
    op.drop_column("approval_requests", "traceparent")
