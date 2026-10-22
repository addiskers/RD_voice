"""Worker lifecycle: refusal at cap / while draining, drain to idle, drain timeout, heartbeat body."""

import asyncio
import json
import time

import httpx
import pytest

from engine.config import Settings
from engine.worker.lifecycle import WorkerLifecycle


def _s(**kw) -> Settings:
    return Settings(_env_file=None, internal_api_url="http://api", internal_token="t", **kw)


def _lc(settings: Settings, http=None, terminate=None) -> WorkerLifecycle:
    return WorkerLifecycle(settings, http, worker_id="w1", version="0.1.0", started_at=time.time(), terminate=terminate)


def test_should_refuse():
    lc = _lc(_s(worker_max_sessions=2))
    assert lc.should_refuse(1) is None and lc.should_refuse(2) == "capacity"
    lc.draining = True
    assert lc.should_refuse(0) == "draining"


@pytest.mark.asyncio
async def test_begin_drain_exits_when_idle():
    term = []
    lc = _lc(_s(), terminate=lambda: term.append(1))
    lc.poll_interval_s = 0.05

    async def end_fn(handle, reason):
        raise AssertionError("nothing to end")

    await asyncio.wait_for(lc.begin_drain(handles_fn=lambda: [], end_fn=end_fn, spool_pending_fn=lambda: 0), 3.0)
    assert lc.draining is True and term == [1]


@pytest.mark.asyncio
async def test_begin_drain_waits_for_spool():
    term = []
    pending = {"n": 2}
    lc = _lc(_s(), terminate=lambda: term.append(1))
    lc.poll_interval_s = 0.05

    async def shrink():
        await asyncio.sleep(0.2)
        pending["n"] = 0

    asyncio.create_task(shrink())
    await asyncio.wait_for(lc.begin_drain(handles_fn=lambda: [], end_fn=None, spool_pending_fn=lambda: pending["n"]), 3.0)
    assert term == [1] and pending["n"] == 0


@pytest.mark.asyncio
async def test_drain_timeout_ends_remaining_calls():
    ended, term = [], []
    handle = object()
    lc = _lc(_s(drain_timeout_seconds=1), terminate=lambda: term.append(1))
    lc.poll_interval_s = 0.05
    lc.grace_after_timeout_s = 0.2

    async def end_fn(h, reason):
        ended.append((h, reason))

    await asyncio.wait_for(lc.begin_drain(handles_fn=lambda: [handle], end_fn=end_fn, spool_pending_fn=lambda: 0), 10.0)
    assert ended == [(handle, "drain_timeout")] and term == [1]


@pytest.mark.asyncio
async def test_heartbeat_posts_expected_body():
    seen = []

    def handler(r: httpx.Request) -> httpx.Response:
        seen.append((r.url.path, r.headers.get("x-internal-token"), json.loads(r.content)))
        return httpx.Response(200)

    s = _s(worker_max_sessions=7, worker_heartbeat_seconds=0.05, worker_internal_url="http://w:8091")
    lc = _lc(s, http=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    task = asyncio.create_task(lc.heartbeat_loop(active_fn=lambda: 3, spool_pending_fn=lambda: 2))
    await asyncio.sleep(0.25)
    task.cancel()
    path, token, body = seen[0]
    assert path == "/internal/workers/heartbeat" and token == "t"
    assert body["worker_id"] == "w1" and body["worker_url"] == "http://w:8091" and body["version"] == "0.1.0"
    assert body["max_sessions"] == 7 and body["active_sessions"] == 3 and body["spool_pending"] == 2
    assert body["draining"] is False and body["started_at"]
    assert len(seen) >= 2
