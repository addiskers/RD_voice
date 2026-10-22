"""baseline: agent_versions, calls, numbers (the schema create_all produced before slice 1)

Revision ID: 0001_baseline
Revises:
Create Date: 2026-10-22
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0001_baseline"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_versions",
        sa.Column("agent_id", sa.String(128), primary_key=True),
        sa.Column("version", sa.Integer(), primary_key=True),
        sa.Column("config", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_table(
        "calls",
        sa.Column("id", sa.String(40), primary_key=True),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("direction", sa.String(16), nullable=False),
        sa.Column("provider", sa.String(24), nullable=False),
        sa.Column("to_number", sa.String(32), nullable=False),
        sa.Column("from_number", sa.String(32), nullable=True),
        sa.Column("provider_call_id", sa.String(80), nullable=True),
        sa.Column("request_uuid", sa.String(80), nullable=True),
        sa.Column("agent_id", sa.String(128), nullable=True),
        sa.Column("agent_version", sa.Integer(), nullable=True),
        sa.Column("agent_config", sa.JSON(), nullable=True),
        sa.Column("variables", sa.JSON(), nullable=True),
        sa.Column("call_metadata", sa.JSON(), nullable=True),
        sa.Column("webhook_url", sa.Text(), nullable=True),
        sa.Column("webhook_secret", sa.String(256), nullable=True),
        sa.Column("record", sa.Boolean(), nullable=True),
        sa.Column("max_duration_s", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ringing_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("answered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_s", sa.Float(), nullable=True),
        sa.Column("end_reason", sa.String(64), nullable=True),
        sa.Column("hangup_cause", sa.String(128), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("worker_url", sa.Text(), nullable=True),
        sa.Column("outcome", sa.JSON(), nullable=True),
        sa.Column("transcript", sa.JSON(), nullable=True),
        sa.Column("metrics", sa.JSON(), nullable=True),
        sa.Column("recording_path", sa.Text(), nullable=True),
        sa.Column("cost_usd", sa.Float(), nullable=True),
        sa.Column("ended_emitted", sa.Boolean(), nullable=True),
        sa.Column("last_seq", sa.Integer(), nullable=True),
    )
    op.create_index("ix_calls_status", "calls", ["status"])
    op.create_index("ix_calls_provider_call_id", "calls", ["provider_call_id"])
    op.create_index("ix_calls_request_uuid", "calls", ["request_uuid"])
    op.create_index("ix_calls_created_at", "calls", ["created_at"])
    op.create_table(
        "numbers",
        sa.Column("e164", sa.String(32), primary_key=True),
        sa.Column("provider", sa.String(24), nullable=False),
        sa.Column("agent_id", sa.String(128), nullable=True),
        sa.Column("agent_version", sa.Integer(), nullable=True),
        sa.Column("route_url", sa.Text(), nullable=True),
        sa.Column("webhook_url", sa.Text(), nullable=True),
        sa.Column("webhook_secret", sa.String(256), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("numbers")
    op.drop_index("ix_calls_created_at", table_name="calls")
    op.drop_index("ix_calls_request_uuid", table_name="calls")
    op.drop_index("ix_calls_provider_call_id", table_name="calls")
    op.drop_index("ix_calls_status", table_name="calls")
    op.drop_table("calls")
    op.drop_table("agent_versions")
