"""SQLAlchemy (async) models: agents (versioned), calls, numbers.

SQLite by default (``sqlite+aiosqlite:///./data/engine.db``); Postgres is a DATABASE_URL change
(``postgresql+asyncpg://...``). JSON columns keep the shared pydantic contracts as-is.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import JSON, Boolean, DateTime, Float, Integer, String, Text, UniqueConstraint
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


class Base(DeclarativeBase):
    pass


class AgentVersion(Base):
    __tablename__ = "agent_versions"
    agent_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    config: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Call(Base):
    __tablename__ = "calls"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    status: Mapped[str] = mapped_column(String(24), index=True)
    direction: Mapped[str] = mapped_column(String(16))
    provider: Mapped[str] = mapped_column(String(24))
    to_number: Mapped[str] = mapped_column(String(32))
    from_number: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    provider_call_id: Mapped[Optional[str]] = mapped_column(String(80), nullable=True, index=True)
    request_uuid: Mapped[Optional[str]] = mapped_column(String(80), nullable=True, index=True)

    agent_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    agent_version: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    agent_config: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)   # frozen for the call
    variables: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    call_metadata: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    webhook_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    webhook_secret: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    record: Mapped[bool] = mapped_column(Boolean, default=True)
    max_duration_s: Mapped[int] = mapped_column(Integer, default=600)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    ringing_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    answered_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_s: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    end_reason: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    hangup_cause: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    worker_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)      # set by the worker on start
    outcome: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    transcript: Mapped[list[Any]] = mapped_column(JSON, default=list)
    metrics: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    recording_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    cost_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ended_emitted: Mapped[bool] = mapped_column(Boolean, default=False)
    last_seq: Mapped[int] = mapped_column(Integer, default=0)            # last worker event seq applied
    webhook_seq: Mapped[int] = mapped_column(Integer, default=0)         # last outgoing webhook seq allocated
    worker_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)


class Worker(Base):
    __tablename__ = "workers"
    worker_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    worker_url: Mapped[str] = mapped_column(Text)
    version: Mapped[str] = mapped_column(String(32), default="")
    active_sessions: Mapped[int] = mapped_column(Integer, default=0)
    max_sessions: Mapped[int] = mapped_column(Integer, default=0)
    draining: Mapped[bool] = mapped_column(Boolean, default=False)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    status: Mapped[str] = mapped_column(String(8), default="live")      # live | dead


class WebhookDelivery(Base):
    __tablename__ = "webhook_deliveries"
    __table_args__ = (UniqueConstraint("call_id", "seq", name="uq_webhook_deliveries_call_seq"),)
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    call_id: Mapped[str] = mapped_column(String(40), index=True)
    seq: Mapped[int] = mapped_column(Integer)
    event: Mapped[str] = mapped_column(String(32))
    payload_json: Mapped[str] = mapped_column(Text)
    url: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(12), default="pending", index=True)   # pending|delivering|delivered|dead
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    last_status_code: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    delivered_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class PhoneNumber(Base):
    __tablename__ = "numbers"
    e164: Mapped[str] = mapped_column(String(32), primary_key=True)
    provider: Mapped[str] = mapped_column(String(24))
    agent_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    agent_version: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    route_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    webhook_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    webhook_secret: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


MIGRATIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "migrations")


BASELINE_REVISION = "0001_baseline"


async def _schema_state(url: str) -> tuple[set[str], Optional[str]]:
    """``(tables, current_revision)``; the revision is None when alembic_version is missing or empty."""
    from sqlalchemy import inspect, text
    engine = create_async_engine(url, future=True)
    try:
        async with engine.connect() as conn:
            tables = set(await conn.run_sync(lambda c: inspect(c).get_table_names()))
            revision = None
            if "alembic_version" in tables:
                revision = await conn.scalar(text("SELECT version_num FROM alembic_version"))
            return tables, revision
    finally:
        await engine.dispose()


def run_migrations(url: str) -> None:
    """Upgrade the schema at ``url`` to head (synchronous; call from a thread when a loop is running).

    A database created by the pre-slice-1 ``create_all`` has the baseline tables but no ``alembic_version``:
    it is stamped at the baseline first so the baseline migration does not try to recreate them."""
    import asyncio

    from alembic import command
    from alembic.config import Config

    cfg = Config()
    cfg.set_main_option("script_location", MIGRATIONS_DIR)
    cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    tables, revision = asyncio.run(_schema_state(url))
    if revision is None and {"agent_versions", "calls", "numbers"} <= tables:
        command.stamp(cfg, BASELINE_REVISION)
    command.upgrade(cfg, "head")


class Database:
    def __init__(self, url: str):
        self.url = url
        if url.startswith("sqlite"):
            path = url.split("///", 1)[-1]
            if path and path != ":memory:":
                os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        self.engine: AsyncEngine = create_async_engine(url, future=True)
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)

    async def init(self) -> None:
        import asyncio
        await asyncio.to_thread(run_migrations, self.url)

    async def close(self) -> None:
        await self.engine.dispose()

    def session(self) -> AsyncSession:
        return self.session_factory()
