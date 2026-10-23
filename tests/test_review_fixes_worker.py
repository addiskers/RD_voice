"""Regression tests for the review findings on the worker and crypto side (C5, I5, I6, I3 rotation, I8)."""

import asyncio
import math
import os
import struct
import time
import wave

import numpy as np
import pytest
from cryptography.fernet import Fernet

from conftest import H
from engine.api.crypto import SecretBox
from engine.config import Settings
from engine.worker.lifecycle import WorkerLifecycle, worker_dirs
from engine.worker.recording import StereoRecorder


def _sine(rate: int, seconds: float, freq: float, amp: int = 8000) -> bytes:
    n = int(rate * seconds)
    return struct.pack(f"<{n}h", *(int(amp * math.sin(2 * math.pi * freq * i / rate)) for i in range(n)))


# ----------------------------------------------------------------------------- C5: per-worker directories
def test_worker_dirs_are_per_worker(tmp_path):
    s = Settings(_env_file=None, spool_dir=str(tmp_path / "spool"), recordings_dir=str(tmp_path / "rec"))
    spool_a, staging_a = worker_dirs(s, "worker-a")
    spool_b, staging_b = worker_dirs(s, "worker-b")
    assert spool_a != spool_b and staging_a != staging_b
    assert spool_a == os.path.join(str(tmp_path / "spool"), "worker-a")
    assert staging_a == os.path.join(str(tmp_path / "rec"), "staging", "worker-a")
    assert os.path.isdir(spool_a) and os.path.isdir(staging_a)        # created on demand
    # a second worker starting up must not see the first worker's live files
    (tmp_path / "rec" / "staging" / "worker-a" / "call_x.caller.raw").write_bytes(b"\0" * 10)
    from engine.worker.recording import remove_stale_raw
    assert remove_stale_raw(staging_b) == 0
    assert (tmp_path / "rec" / "staging" / "worker-a" / "call_x.caller.raw").exists()


# ----------------------------------------------------------------------------- I5: drain grace for the proxy
@pytest.mark.asyncio
async def test_drain_keeps_accepting_streams_during_proxy_grace():
    s = Settings(_env_file=None, worker_max_sessions=5, drain_accept_grace_seconds=0.3, drain_timeout_seconds=5)
    lc = WorkerLifecycle(s, None, worker_id="w", version="t", started_at=time.time(), terminate=lambda: None)
    lc.poll_interval_s = 0.05
    lc.start_drain(handles_fn=lambda: [object()], end_fn=None, spool_pending_fn=lambda: 0)
    await asyncio.sleep(0.05)
    assert lc.draining is True
    assert lc.should_refuse(1) is None             # the proxy has not noticed yet: keep answering
    await asyncio.sleep(0.4)
    assert lc.should_refuse(1) == "draining"
    assert lc.should_refuse(5) == "capacity"        # the cap still applies inside the grace window
    lc._drain_task.cancel()


def test_compose_workers_do_not_restart_after_a_clean_drain():
    """A worker that exited by itself after draining must not be restarted on the old image."""
    services: dict[str, list[str]] = {}
    current = None
    for line in open("docker-compose.yml", encoding="utf-8"):
        if line.startswith("  ") and not line.startswith("   ") and line.strip().endswith(":"):
            current = line.strip()[:-1]
            services[current] = []
        elif current:
            services[current].append(line.strip())
    workers = {k: v for k, v in services.items() if k.startswith("worker")}
    assert set(workers) == {"worker", "worker-b"}
    for name, lines in workers.items():
        assert any(ln.startswith("restart: on-failure") for ln in lines), name


# ----------------------------------------------------------------------------- I6: recording finish
def test_finish_streams_long_recordings_in_chunks(tmp_path):
    rec = StereoRecorder(str(tmp_path / "long"), max_seconds=120)
    rec.add_caller(_sine(8000, 20.0, 1000.0), 8000)
    rec.add_agent(_sine(8000, 20.0, 440.0), 8000)
    assert rec.interleave_chunk_frames < 20 * 8000            # several chunks for a 20 s call
    path = rec.finish()
    with wave.open(path, "rb") as wf:
        assert wf.getnchannels() == 2 and wf.getframerate() == 8000
        data = np.frombuffer(wf.readframes(wf.getnframes()), dtype=np.int16).reshape(-1, 2)
    assert abs(len(data) - 20 * 8000) < 4000          # wall-clock placement: allow 0.5 s drift on a busy machine
    seg = data[8000 - 200: 8000 + 200]                       # across the first chunk boundary: no glitch
    assert np.max(np.abs(np.diff(seg[:, 0].astype(int)))) < 8000 * 0.8


@pytest.mark.asyncio
async def test_store_recording_runs_finish_off_the_event_loop(monkeypatch, tmp_path):
    from engine.worker import main as wmain

    class SlowRecorder:
        def finish(self):
            time.sleep(0.3)                                   # blocking work, like interleaving 10 minutes of audio
            p = tmp_path / "slow.wav"
            p.write_bytes(b"RIFF")
            return str(p)

    class Storage:
        async def put_file(self, local, key):
            pass

    monkeypatch.setattr(wmain.W, "storage", Storage())
    handle = wmain.CallHandle(call_id="call_slow", session=None, state=None, reporter=None, recorder=SlowRecorder())
    ticks = {"n": 0}

    async def ticker():
        while True:
            ticks["n"] += 1
            await asyncio.sleep(0.02)

    t = asyncio.create_task(ticker())
    key = await wmain._store_recording(handle)
    t.cancel()
    assert key and key.startswith("recordings/")
    assert ticks["n"] >= 8                                     # the loop kept running while finish() blocked


# ----------------------------------------------------------------------------- I3: key rotation
def test_secretbox_multi_key_decrypts_old_values_and_rotates():
    old, new = Fernet.generate_key().decode(), Fernet.generate_key().decode()
    token = SecretBox(old).encrypt("sk-live-0001")
    box = SecretBox(f"{new},{old}")                           # first key encrypts, every key decrypts
    assert box.decrypt(token) == "sk-live-0001"
    rotated = box.rotate(token)
    assert rotated != token and SecretBox(new).decrypt(rotated) == "sk-live-0001"
    with pytest.raises(Exception):
        SecretBox(new).decrypt(token)                          # the old token is unreadable without the old key
    assert box.rotate("plain") == "plain"


# ----------------------------------------------------------------------------- I8: log-only verification
def test_verify_log_only_allows_and_logs(tmp_path, caplog):
    from test_security import _client
    c = _client(tmp_path, carrier_verify_signatures=True, carrier_verify_log_only=True)
    try:
        call_id = c.post("/v1/calls", json={"to": "+919876543210", "agent": {"name": "s", "instructions": "x",
                         "llm": {"provider": "google", "model": "gemini-2.5-flash"}}}, headers=H).json()["call_id"]
        with caplog.at_level("WARNING"):
            r = c.get(f"/carrier/plivo/answer?call_id={call_id}&CallUUID=cu-1")
        assert r.status_code == 200 and "<Stream" in r.text
        assert any("signature" in m.lower() and "log-only" in m.lower() for m in caplog.messages)
    finally:
        c.__exit__(None, None, None)
