"""outbox: unique (call_id, seq) so two emitters can never produce the same sequence number

Revision ID: 0003_outbox_unique_seq
Revises: 0002_slice1
Create Date: 2026-10-23
"""
from __future__ import annotations

from alembic import op

revision = "0003_outbox_unique_seq"
down_revision = "0002_slice1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("uq_webhook_deliveries_call_seq", "webhook_deliveries", ["call_id", "seq"], unique=True)


def downgrade() -> None:
    op.drop_index("uq_webhook_deliveries_call_seq", table_name="webhook_deliveries")
