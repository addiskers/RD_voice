import asyncio

from sqlalchemy import inspect

from engine.api.db import Database


def _tables(db: Database) -> set[str]:
    async def go():
        async with db.engine.connect() as conn:
            return set(await conn.run_sync(lambda c: inspect(c).get_table_names()))
    return asyncio.run(go())


def _cols(db: Database, table: str) -> set[str]:
    async def go():
        async with db.engine.connect() as conn:
            return {c["name"] for c in await conn.run_sync(lambda c: inspect(c).get_columns(table))}
    return asyncio.run(go())


def test_fresh_db_has_slice1_tables(tmp_path):
    db = Database(f"sqlite+aiosqlite:///{tmp_path / 'm.db'}")
    asyncio.run(db.init())
    names = _tables(db)
    assert {"agent_versions", "calls", "numbers", "workers", "webhook_deliveries", "alembic_version"} <= names
    assert {"webhook_seq", "worker_id"} <= _cols(db, "calls")
    assert {"worker_id", "worker_url", "active_sessions", "max_sessions", "draining", "last_seen_at", "status"} <= _cols(db, "workers")
    assert {"call_id", "seq", "event", "payload_json", "url", "status", "attempts", "next_attempt_at"} <= _cols(db, "webhook_deliveries")
    asyncio.run(db.close())


def test_init_is_idempotent(tmp_path):
    db = Database(f"sqlite+aiosqlite:///{tmp_path / 'm.db'}")
    asyncio.run(db.init())
    asyncio.run(db.init())
    assert "workers" in _tables(db)
    asyncio.run(db.close())
