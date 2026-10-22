import asyncio

import pytest
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


def _legacy_db(tmp_path, version_table_sql: str) -> str:
    """A Phase-0 database made by create_all: baseline tables, alembic_version missing or empty."""
    import sqlite3
    from alembic import command
    from alembic.config import Config
    from engine.api.db import MIGRATIONS_DIR

    path = tmp_path / "legacy.db"
    url = f"sqlite+aiosqlite:///{path}"
    cfg = Config()
    cfg.set_main_option("script_location", MIGRATIONS_DIR)
    cfg.set_main_option("sqlalchemy.url", url)
    command.upgrade(cfg, "0001_baseline")
    con = sqlite3.connect(path)
    con.execute(version_table_sql)
    con.commit()
    con.close()
    return url


@pytest.mark.parametrize("version_table_sql", ["DROP TABLE alembic_version", "DELETE FROM alembic_version"])
def test_legacy_create_all_db_is_stamped_and_upgraded(tmp_path, version_table_sql):
    db = Database(_legacy_db(tmp_path, version_table_sql))
    asyncio.run(db.init())          # must stamp the baseline, then apply 0002, not fail on CREATE TABLE
    names = _tables(db)
    assert {"workers", "webhook_deliveries", "alembic_version"} <= names
    assert {"webhook_seq", "worker_id"} <= _cols(db, "calls")
    asyncio.run(db.close())


def test_init_is_idempotent(tmp_path):
    db = Database(f"sqlite+aiosqlite:///{tmp_path / 'm.db'}")
    asyncio.run(db.init())
    asyncio.run(db.init())
    assert "workers" in _tables(db)
    asyncio.run(db.close())
