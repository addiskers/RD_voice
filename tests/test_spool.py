"""Worker event spool: on-disk, retried until the API acknowledges, replayed after a worker restart."""

import json

import httpx
import pytest

import engine.worker.spool as spool_mod
from engine.config import Settings
from engine.worker.spool import EventSpool, replay_pending


def _settings() -> Settings:
    return Settings(_env_file=None, internal_api_url="http://api", internal_token="t")


def _http(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
async def test_spool_persists_and_acks(tmp_path):
    posted = []

    def handler(r: httpx.Request) -> httpx.Response:
        assert r.url.path == "/internal/calls/call_a/events" and r.headers["x-internal-token"] == "t"
        posted.append(json.loads(r.content)["seq"])
        return httpx.Response(200)

    sp = EventSpool(_settings(), _http(handler), "call_a", directory=str(tmp_path))
    sp.start()
    for i in range(1, 4):
        sp.append({"type": "turn", "seq": i, "data": {"i": i}})
    assert await sp.wait_idle(2.0)
    assert posted == [1, 2, 3] and sp.pending == 0
    assert (tmp_path / "call_a.ack").read_text().strip() == "3"
    assert len((tmp_path / "call_a.jsonl").read_text(encoding="utf-8").splitlines()) == 3
    sp.mark_ended()
    assert await sp.wait_idle(2.0)
    await sp.aclose()
    assert not (tmp_path / "call_a.jsonl").exists() and not (tmp_path / "call_a.ack").exists()


@pytest.mark.asyncio
async def test_spool_retries_until_api_up(tmp_path, monkeypatch):
    monkeypatch.setattr(spool_mod, "BACKOFF_BASE_S", 0.01)
    calls = {"n": 0}
    posted = []

    def handler(r: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] <= 4:
            return httpx.Response(503)
        posted.append(json.loads(r.content)["seq"])
        return httpx.Response(200)

    sp = EventSpool(_settings(), _http(handler), "call_a", directory=str(tmp_path))
    sp.start()
    sp.append({"type": "turn", "seq": 1, "data": {}})
    sp.append({"type": "ended", "seq": 2, "data": {}})
    assert await sp.wait_idle(5.0)
    assert posted == [1, 2] and calls["n"] >= 6
    await sp.aclose()


@pytest.mark.asyncio
async def test_4xx_drops_event(tmp_path):
    sp = EventSpool(_settings(), _http(lambda r: httpx.Response(404)), "call_a", directory=str(tmp_path))
    sp.start()
    sp.append({"type": "turn", "seq": 1, "data": {}})
    assert await sp.wait_idle(2.0)
    assert sp.pending == 0 and (tmp_path / "call_a.ack").read_text().strip() == "1"
    await sp.aclose()


@pytest.mark.asyncio
async def test_replay_on_start_resends_unacked(tmp_path):
    lines = [json.dumps({"type": "turn", "seq": i, "data": {"i": i}}) for i in (1, 2, 3)]
    (tmp_path / "call_x.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (tmp_path / "call_x.ack").write_text("1")
    (tmp_path / "call_y.jsonl").write_text(lines[0] + "\n", encoding="utf-8")      # fully acked: just cleaned up
    (tmp_path / "call_y.ack").write_text("1")
    posted = []

    def handler(r: httpx.Request) -> httpx.Response:
        posted.append((r.url.path, json.loads(r.content)["seq"]))
        return httpx.Response(200)

    registry: dict = {}
    n = await replay_pending(_settings(), _http(handler), str(tmp_path), registry)
    assert n == 1 and "call_x" in registry
    sp = registry["call_x"]
    assert await sp.wait_idle(2.0)
    assert posted == [("/internal/calls/call_x/events", 2), ("/internal/calls/call_x/events", 3)]
    assert not (tmp_path / "call_y.jsonl").exists()
    assert "call_x" not in registry and not (tmp_path / "call_x.jsonl").exists()   # drained + cleaned up
    await sp.aclose()
