"""slice 1: workers registry, webhook outbox, call.webhook_seq / call.worker_id

Revision ID: 0002_slice1
Revises: 0001_baseline
Create Date: 2026-10-22
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0002_slice1"
down_revision = "0001_baseline"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workers",
        sa.Column("worker_id", sa.String(128), primary_key=True),
        sa.Column("worker_url", sa.Text(), nullable=False),
        sa.Column("version", sa.String(32), nullable=True),
        sa.Column("active_sessions", sa.Integer(), nullable=True),
        sa.Column("max_sessions", sa.Integer(), nullable=True),
        sa.Column("draining", sa.Boolean(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(8), nullable=True),
    )
    op.create_index("ix_workers_last_seen_at", "workers", ["last_seen_at"])
    op.create_table(
        "webhook_deliveries",
        sa.Column("id", sa.String(40), primary_key=True),
        sa.Column("call_id", sa.String(40), nullable=False),
        sa.Column("seq", sa.Integer(), nullable=False),
        sa.Column("event", sa.String(32), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("status", sa.String(12), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_status_code", sa.Integer(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_webhook_deliveries_call_id", "webhook_deliveries", ["call_id"])
    op.create_index("ix_webhook_deliveries_status", "webhook_deliveries", ["status"])
    op.create_index("ix_webhook_deliveries_next_attempt_at", "webhook_deliveries", ["next_attempt_at"])
    op.add_column("calls", sa.Column("webhook_seq", sa.Integer(), nullable=True, server_default="0"))
    op.add_column("calls", sa.Column("worker_id", sa.String(128), nullable=True))


def downgrade() -> None:
    op.drop_column("calls", "worker_id")
    op.drop_column("calls", "webhook_seq")
    op.drop_index("ix_webhook_deliveries_next_attempt_at", table_name="webhook_deliveries")
    op.drop_index("ix_webhook_deliveries_status", table_name="webhook_deliveries")
    op.drop_index("ix_webhook_deliveries_call_id", table_name="webhook_deliveries")
    op.drop_table("webhook_deliveries")
    op.drop_index("ix_workers_last_seen_at", table_name="workers")
    op.drop_table("workers")
