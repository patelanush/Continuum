"""Claim outbox rows briefly before broker I/O.

Revision ID: 20260922_0004
Revises: 20260922_0003
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260922_0004"
down_revision: str | None = "20260922_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("outbox_events", sa.Column("publish_lease_token", postgresql.UUID(as_uuid=True)))
    op.add_column(
        "outbox_events", sa.Column("publish_lease_expires_at", sa.DateTime(timezone=True))
    )
    op.create_index(
        "ix_outbox_events_available",
        "outbox_events",
        ["publish_lease_expires_at", "created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_check_constraint(
        "ck_outbox_publish_lease_pair",
        "outbox_events",
        "(publish_lease_token IS NULL) = (publish_lease_expires_at IS NULL)",
    )


def downgrade() -> None:
    op.drop_constraint("ck_outbox_publish_lease_pair", "outbox_events", type_="check")
    op.drop_index("ix_outbox_events_available", table_name="outbox_events")
    op.drop_column("outbox_events", "publish_lease_expires_at")
    op.drop_column("outbox_events", "publish_lease_token")
