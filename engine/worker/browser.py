"""Transport C: browser WebSocket (the gvox-voice-proxy protocol) into the same AgentSession runtime.

Client -> worker:  {"type":"setup","auth_key":..,"agent_id":..|"agent":{...},"variables":{..},"sample_rate":16000}
                   {"type":"audio","data":"<base64 PCM16 mono LE>"}      {"type":"stop"}
Worker -> client:  {"type":"ready","call_id":..}  {"type":"audio","data":..}  {"type":"transcript","role":..,"text":..}
                   {"type":"interrupted"}  {"type":"turn_complete"}  {"type":"error","code":..,"message":..}
                   {"type":"metrics","turns":[{turn,eou_delay,stt_delay,llm_ttft,tts_ttfb,total}],"total_p50","total_p95",
                    "pickup_to_first_word"}   (pushed whenever the engine measures a turn)
                   {"type":"node","from":..,"to":..,"via":..}   (workflow step change)
                   {"type":"session_end","reason":..}
Browser sessions use VAD/STT endpointing (no turn-detector inference context) and are never recorded.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
from typing import Any, Optional

from aiohttp import web
from livekit import rtc
from livekit.agents.voice.io import AudioInput, AudioOutput, AudioOutputCapabilities

from engine.schemas.agent import AgentConfig
from engine.worker import audio as codec

logger = logging.getLogger("gvox.worker.browser")
PIPELINE_RATE = 16000


class BrowserAudioInput(AudioInput):
    def __init__(self, client_rate: int):
        super().__init__(label="browser-in")
        self._q: asyncio.Queue = asyncio.Queue()
        self._client_rate = client_rate
        self._closed = False
        self._rs = codec.StreamResampler(client_rate, PIPELINE_RATE)    # stateful: no artefacts at frame edges

    def push_pcm(self, pcm: bytes) -> None:
        if self._closed or not pcm:
            return
        pcm = self._rs.process(pcm)
        if not pcm:
            return
        self._q.put_nowait(rtc.AudioFrame(data=pcm, sample_rate=PIPELINE_RATE, num_channels=1,
                                          samples_per_channel=len(pcm) // 2))

    def close(self) -> None:
        self._closed = True
        self._q.put_nowait(None)

    async def __anext__(self) -> rtc.AudioFrame:
        frame = await self._q.get()
        if frame is None:
            raise StopAsyncIteration
        return frame


class BrowserAudioOutput(AudioOutput):
    def __init__(self, send_json, client_rate: int):
        super().__init__(label="browser-out", capabilities=AudioOutputCapabilities(pause=False), sample_rate=None)
        self._send = send_json
        self._client_rate = client_rate
        self._seg_started: Optional[float] = None
        self._pushed = 0.0
        self._finish_task: Optional[asyncio.Task] = None
        self._rs: dict[int, codec.StreamResampler] = {}     # one stateful resampler per source rate

    async def capture_frame(self, frame: rtc.AudioFrame) -> None:
        await super().capture_frame(frame)
        if self._seg_started is None:
            self._seg_started = time.monotonic()
            self._pushed = 0.0
            self.on_playback_started(created_at=time.time())
        pcm = bytes(frame.data)
        if frame.sample_rate != self._client_rate:
            rs = self._rs.get(frame.sample_rate)
            if rs is None:
                rs = self._rs[frame.sample_rate] = codec.StreamResampler(frame.sample_rate, self._client_rate)
            pcm = rs.process(pcm)
            if not pcm:
                return
        self._pushed += frame.duration
        try:
            await self._send({"type": "audio", "data": base64.b64encode(pcm).decode("ascii")})
        except Exception:
            pass

    def flush(self) -> None:
        super().flush()
        if self._seg_started is None:
            return
        started, pushed = self._seg_started, self._pushed
        self._seg_started = None

        async def _finish():
            remaining = started + pushed - time.monotonic()
            if remaining > 0:
                await asyncio.sleep(remaining)
            self.on_playback_finished(playback_position=pushed, interrupted=False)

        self._finish_task = asyncio.create_task(_finish())

    def clear_buffer(self) -> None:
        if self._finish_task and not self._finish_task.done():
            self._finish_task.cancel()
        played = 0.0
        if self._seg_started is not None:
            played = min(self._pushed, time.monotonic() - self._seg_started)
        if self._seg_started is not None or (self._finish_task and not self._finish_task.done()):
            self.on_playback_finished(playback_position=played, interrupted=True)
        self._seg_started = None
        self._finish_task = None
        asyncio.create_task(self._safe_send({"type": "interrupted"}))

    async def _safe_send(self, msg: dict[str, Any]) -> None:
        try:
            await self._send(msg)
        except Exception:
            pass


async def browser_ws_handler(request: web.Request) -> web.WebSocketResponse:
    from engine.worker.main import W, finalize, run_call
    from engine.worker.pipeline import make_session

    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    settings = W.settings

    async def send(msg: dict[str, Any]) -> None:
        if not ws.closed:
            await ws.send_str(json.dumps(msg))

    async def fail(code: str, message: str) -> web.WebSocketResponse:
        await send({"type": "error", "code": code, "message": message})
        await ws.close()
        return ws

    try:
        raw = await asyncio.wait_for(ws.receive_str(), timeout=10.0)
        setup = json.loads(raw)
    except Exception as e:
        return await fail("invalid_setup", f"setup message expected first: {e}")
    if setup.get("type") != "setup":
        return await fail("invalid_setup", "first message must be type 'setup'")
    key = str(setup.get("auth_key") or "")
    if key not in settings.browser_keys and key != settings.engine_api_key:
        return await fail("auth_failed", "invalid auth key")

    body: dict[str, Any] = {"variables": setup.get("variables") or {}, "metadata": {"transport": "browser"},
                            "webhook_url": setup.get("webhook_url")}
    if setup.get("agent"):
        body["agent"] = setup["agent"]
    elif setup.get("agent_id"):
        body["agent_id"] = setup["agent_id"]
        body["agent_version"] = setup.get("agent_version")
    elif setup.get("system_prompt"):     # legacy gvox-voice-proxy clients
        body["agent"] = {"name": "browser", "mode": "cascaded", "instructions": setup["system_prompt"],
                         "llm": {"provider": settings.default_llm_provider, "model": settings.default_llm_model}}
    else:
        return await fail("invalid_setup", "agent_id, agent or system_prompt is required")
    try:
        r = await W.http.post(f"{settings.internal_api_url.rstrip('/')}/internal/calls/browser", json=body,
                              headers={"X-Internal-Token": settings.internal_token}, timeout=10.0)
        if r.status_code != 200:
            return await fail("internal", f"engine API rejected the session: {r.status_code} {r.text[:200]}")
        runtime = r.json()
    except Exception as e:
        return await fail("internal", f"engine API unreachable: {e}")

    client_rate = int(setup.get("sample_rate") or 16000)
    audio_in = BrowserAudioInput(client_rate)
    audio_out = BrowserAudioOutput(send, client_rate)

    def session_factory(cfg: AgentConfig, state, mcp_servers):
        cfg = cfg.model_copy(deep=True)
        if cfg.turn.detector not in ("vad", "stt", "realtime") and not W.prewarmed.get("turn_detector"):
            cfg.turn.detector = "vad"       # the prewarmed detector carries its own inference executor
        return make_session(cfg, settings, prewarmed=W.prewarmed, mcp_servers=mcp_servers, userdata=state)

    def attach_io(session, _mixer):
        session.input.audio = audio_in
        session.output.audio = audio_out

    handle, rt, graph = await run_call(runtime, session_factory=session_factory, attach_io=attach_io,
                                       hangup_transport=lambda: asyncio.create_task(ws.close()), direction="browser")
    session = handle.session

    def _forward(etype: str, data: dict[str, Any]) -> None:
        if etype == "metrics":
            asyncio.create_task(send({"type": "metrics", **data}))
        elif etype == "transition":
            asyncio.create_task(send({"type": "node", "from": data.get("from"), "to": data.get("to"),
                                      "via": data.get("via")}))
        elif etype == "error":   # provider failure (non-fatal unless the session closes): show it, keep going
            asyncio.create_task(send({"type": "error", "code": "provider:" + (data.get("source") or "unknown"),
                                      "message": data.get("message", "")}))

    handle.reporter.listeners.append(_forward)

    @session.on("conversation_item_added")
    def _item(ev):
        item = ev.item
        text = getattr(item, "text_content", None)
        role = getattr(item, "role", None)
        if text and role in ("user", "assistant"):
            asyncio.create_task(send({"type": "transcript", "role": "user" if role == "user" else "agent",
                                      "text": text}))

    @session.on("agent_state_changed")
    def _state(ev):
        if ev.old_state == "speaking" and ev.new_state != "speaking":
            asyncio.create_task(send({"type": "turn_complete"}))

    await send({"type": "ready", "call_id": handle.call_id, "session_id": handle.call_id})
    await session.start(agent=rt.start_agent(session))

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except Exception:
                    continue
                t = data.get("type")
                if t == "audio":
                    try:
                        audio_in.push_pcm(base64.b64decode(data.get("data") or ""))
                    except Exception:
                        pass
                elif t == "stop":
                    break
            elif msg.type in (web.WSMsgType.CLOSE, web.WSMsgType.ERROR):
                break
    finally:
        audio_in.close()
        if not handle.finalized:
            if handle.end_reason:                      # an end is already in flight (tool / workflow / API)
                for _ in range(120):
                    if handle.finalized:
                        break
                    await asyncio.sleep(0.1)
            else:
                try:
                    await asyncio.wait_for(session.aclose(), timeout=8.0)
                except Exception:
                    pass
            await finalize(handle, handle.end_reason or "client_closed")
        try:
            await send({"type": "session_end", "reason": handle.end_reason or "client_closed"})
            await ws.close()
        except Exception:
            pass
    return ws
