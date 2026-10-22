"""Health detail, Prometheus metrics on the API, and the SQLite copy command."""

import asyncio

from conftest import HI


def test_health_shape(client):
    h = client.get("/health").json()
    for key in ("status", "version", "active_calls", "db", "workers_live", "outbox_pending", "outbox_oldest_s"):
        assert key in h
    assert h["db"] == "ok" and h["status"] == "ok" and h["workers_live"] == 0 and h["outbox_pending"] == 0


def test_metrics_requires_token_and_exposes_gauges(client):
    assert client.get("/metrics").status_code == 401
    client.post("/internal/workers/heartbeat", headers=HI,
                json={"worker_id": "w1", "worker_url": "http://w1:8091", "max_sessions": 5, "active_sessions": 1})
    r = client.get("/metrics", headers=HI)
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/plain")
    text = r.text
    assert "gvox_engine_total 5.0" in text and "gvox_engine_free 4.0" in text
    assert 'gvox_workers{status="live"} 1.0' in text
    assert 'gvox_carrier_free{provider="plivo"}' in text and "gvox_outbox_pending 0.0" in text
    assert 'gvox_calls{status="in_progress"} 0.0' in text


def test_copy_sqlite(tmp_path):
    from engine.api.db import AgentVersion, Call, Database
    from engine.api.maintenance import copy_sqlite

    src_url = f"sqlite+aiosqlite:///{tmp_path / 'src.db'}"
    dst_url = f"sqlite+aiosqlite:///{tmp_path / 'dst.db'}"

    async def seed():
        db = Database(src_url)
        await db.init()
        async with db.session() as s:
            s.add(AgentVersion(agent_id="a", version=1, config={"name": "a"}))
            s.add(Call(id="call_1", status="completed", direction="outbound", provider="plivo", to_number="+91"))
            await s.commit()
        await db.close()

    async def check():
        db = Database(dst_url)
        await db.init()
        async with db.session() as s:
            return (await s.get(AgentVersion, ("a", 1))) is not None and (await s.get(Call, "call_1")) is not None

    asyncio.run(seed())
    assert asyncio.run(copy_sqlite(src_url, dst_url)) == 2
    assert asyncio.run(copy_sqlite(src_url, dst_url)) == 0      # idempotent
    assert asyncio.run(check())
