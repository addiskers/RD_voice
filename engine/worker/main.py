"""Worker: carrier media WebSocket -> LiveKit AgentSession, one per call.

Built on Plivo Labs' ``agent-transport`` AudioStreamServer (Rust WebSocket endpoint speaking the
Plivo/Vobiz/EnableX <Stream> protocol, LiveKit audio I/O adapters, inference executor for the
turn detector, DTMF, Prometheus metrics). This module adds: call resolution against the engine
API, the pipeline factory, the workflow runtime, tools, event reporting, recording, live-call
control (hangup/say), the browser transport, and graceful end-of-call handling.

Run:  gvox-worker start | dev | debug | download-files      (from the project venv)
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx
from aiohttp import web

from engine import __version__
from engine.config import Settings, get_settings
from engine.schemas.agent import AgentConfig, Mode, ProviderRef
from engine.worker.events import CallReporter
from engine.worker.graph.runtime import GraphRuntime, NodeAgent
from engine.worker.lifecycle import WorkerLifecycle
from engine.worker.spool import EventSpool, replay_pending
from engine.worker.graph.validate import compile_workflow
from engine.worker.pipeline import make_session, prewarm_models
from engine.worker.recording import Mixer, RecordingAudioOutput, TeeAudioInput
from engine.worker.tools import CallState, build_mcp_servers, build_tools

logger = logging.getLogger("gvox.worker")

AGENT_END_REASONS = ("agent_end_call", "workflow_end", "closing_detected", "max_duration", "api_hangup", "transfer")


@dataclass
class CallHandle:
    call_id: str
    session: Any
    state: CallState
    reporter: CallReporter
    mixer: Optional[Mixer] = None
    end_reason: Optional[str] = None
    finalized: bool = False
    tasks: list[asyncio.Task] = field(default_factory=list)


class WorkerState:
    settings: Settings
    http: httpx.AsyncClient
    prewarmed: dict[str, Any] = {}
    active: dict[str, CallHandle] = {}
    spools: dict[str, EventSpool] = {}      # per-call event spools still delivering (incl. replayed ones)
    worker_id: str = ""
    lifecycle: WorkerLifecycle
    tasks: list[asyncio.Task] = []

    @classmethod
    def spool_pending(cls) -> int:
        return sum(s.pending for s in list(cls.spools.values()))


W = WorkerState()


# ----------------------------------------------------------------------------- engine API access
async def fetch_runtime(call_id: Optional[str], provider_call_id: Optional[str]) -> Optional[dict[str, Any]]:
    base = W.settings.internal_api_url.rstrip("/")
    headers = {"X-Internal-Token": W.settings.internal_token}
    urls = []
    if call_id:
        urls.append(f"{base}/internal/calls/{call_id}/runtime")
    if provider_call_id:
        urls.append(f"{base}/internal/calls/by-provider/{provider_call_id}/runtime")
    for url in urls:
        for attempt in range(4):           # the answer webhook and the stream race by a few ms
            try:
                r = await W.http.get(url, headers=headers, timeout=5.0)
                if r.status_code == 200:
                    return r.json()
                if r.status_code != 404:
                    logger.warning("runtime lookup %s -> %s", url, r.status_code)
                    break
            except Exception as e:
                logger.warning("runtime lookup failed (%s): %s", url, e)
            await asyncio.sleep(0.25 * (attempt + 1))
    return None


async def report_refusal(call_id: Optional[str], provider_call_id: Optional[str], reason: str) -> None:
    """Tell the API a stream was refused so it fails the call and drops the carrier leg (best effort)."""
    if not call_id:
        runtime = await fetch_runtime(None, provider_call_id)
        call_id = runtime.get("call_id") if runtime else None
    if not call_id:
        return
    try:
        await W.http.post(f"{W.settings.internal_api_url.rstrip('/')}/internal/calls/{call_id}/events",
                          json={"type": "refused", "data": {"reason": reason, "worker_id": W.worker_id}},
                          headers={"X-Internal-Token": W.settings.internal_token}, timeout=5.0)
    except Exception as e:
        logger.warning("could not report refusal for %s: %s", call_id, e)


# ----------------------------------------------------------------------------- one call
async def run_call(runtime: dict[str, Any], *, session_factory, attach_io, hangup_transport,
                   direction: str = "outbound") -> CallHandle:
    """Shared by the carrier and browser transports.

    session_factory(cfg, state, mcp_servers) -> AgentSession (already wired to its transport by attach_io)
    hangup_transport() -> tells the transport to drop the leg (fire-and-forget)
    """
    settings = W.settings
    call_id = runtime["call_id"]
    cfg = AgentConfig.model_validate(runtime["agent"])
    state = CallState(call_id=call_id, variables=dict(runtime.get("variables") or {}))
    spool = EventSpool(settings, W.http, call_id, directory=settings.spool_dir, registry=W.spools)
    reporter = CallReporter(settings, W.http, call_id, worker_url=settings.worker_internal_url, spool=spool,
                            worker_id=W.worker_id)
    state.report_tool = reporter.report_tool

    tools_by_id = build_tools(cfg.tools, W.http)
    mcp_servers = build_mcp_servers(cfg.tools)
    graph = compile_workflow(cfg.effective_workflow())
    rt = GraphRuntime(graph, cfg, state, tools_by_id, report_transition=reporter.report_transition)
    if cfg.mode == Mode.s2s:
        # speech-to-speech sessions have no text LLM; variable extraction (during and after the call) uses
        # the engine's default text model instead
        try:
            from engine.worker.pipeline import build_llm
            rt.extraction_llm = build_llm(ProviderRef(provider=settings.default_llm_provider,
                                                      model=settings.default_llm_model), settings)
        except Exception as e:
            logger.warning("extraction LLM unavailable for S2S: %s", e)

    session = session_factory(cfg, state, mcp_servers or None)
    mixer: Optional[Mixer] = None
    if runtime.get("record") and settings.record_calls and direction != "browser":
        mixer = Mixer(max_seconds=int(runtime.get("max_duration_s") or settings.call_max_seconds) + 60)
    attach_io(session, mixer)

    handle = CallHandle(call_id=call_id, session=session, state=state, reporter=reporter, mixer=mixer)
    handle.rt = rt   # finalize() runs post-call extraction through it
    W.active[call_id] = handle
    reporter.attach(session)
    reporter.start()
    reporter.started()

    async def _end_task(reason: str) -> None:
        # Runs detached from whatever asked for the end (a tool callback, a node's on_enter, the API):
        # awaiting session.aclose() from inside the session's own activity deadlocks (aclose waits for
        # that activity to finish), which left calls stuck "in_progress" with scheduling paused.
        try:
            cur = getattr(session, "current_speech", None)
            if cur is not None:
                await asyncio.wait_for(cur, timeout=8.0)
        except Exception:
            pass
        await asyncio.sleep(0.25)
        try:
            await asyncio.wait_for(session.aclose(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("session close timed out for %s; finalizing anyway", call_id)
        except Exception as e:
            logger.warning("session close failed for %s: %s", call_id, e)
        await finalize(handle, reason)
        try:
            hangup_transport()
        except Exception:
            pass

    async def request_end(reason: str) -> None:
        if handle.end_reason:
            return
        handle.end_reason = reason
        handle.tasks.append(asyncio.create_task(_end_task(reason), name="gvox-end"))

    state.request_end = request_end

    @session.on("close")
    def _on_close(ev):
        reason = handle.end_reason or f"session_{getattr(getattr(ev, 'reason', None), 'value', 'closed')}"
        handle.tasks.append(asyncio.create_task(finalize(handle, reason)))

    async def _max_duration():
        limit = int(runtime.get("max_duration_s") or settings.call_max_seconds)
        await asyncio.sleep(limit)
        if not handle.finalized:
            logger.info("max duration reached for %s", call_id)
            await state.end("max_duration")

    handle.tasks.append(asyncio.create_task(_max_duration()))
    return handle, rt, graph


async def finalize(handle: CallHandle, reason: str) -> None:
    if handle.finalized:
        return
    handle.finalized = True
    W.active.pop(handle.call_id, None)
    for t in handle.tasks:
        if not t.done() and t is not asyncio.current_task() and t.get_name() != "gvox-end":
            t.cancel()
    recording_path = None
    if handle.mixer is not None:
        path = os.path.join(W.settings.recordings_dir, f"{handle.call_id}.wav")
        recording_path = handle.mixer.write(path)
    rt = getattr(handle, "rt", None)
    if rt is not None and not handle.state.outcome and getattr(rt.cfg, "mode", None) == Mode.s2s:
        # speech-to-speech call ended without a finish tool (goodbye without the tool, caller hung up, client
        # closed): recover the caller's answers from the transcript with the text model, on every end path
        try:
            await asyncio.wait_for(rt.extract_all(handle.session.history, handle.session), timeout=8.0)
        except Exception as e:
            logger.warning("post-call extraction failed for %s: %s", handle.call_id, e)
    handle.reporter.ended(reason, recording_path=recording_path, outcome=handle.state.outcome,
                          hangup_carrier=reason in AGENT_END_REASONS,
                          error=("; ".join(handle.reporter.errors) if handle.reporter.errors else None))
    await handle.reporter.close()
    logger.info("call %s finalized (%s) after %.1fs, %d transcript turns", handle.call_id, reason,
                handle.reporter.at(), len(handle.reporter.transcript))


# ----------------------------------------------------------------------------- carrier transport (agent-transport)
def build_server():
    from agent_transport.audio_stream.livekit import AudioStreamServer, JobContext, JobProcess

    settings = get_settings()
    W.settings = settings
    import socket
    import time as _time
    W.worker_id = settings.worker_id or socket.gethostname()
    W.lifecycle = WorkerLifecycle(settings, None, worker_id=W.worker_id, version=__version__, started_at=_time.time())
    listen = f"{settings.worker_host}:{settings.worker_port}"
    from engine.worker.pipeline import register_plugins
    register_plugins()   # so `download-files` fetches VAD + turn-detector models for every shipped plugin

    class GvoxWorkerServer(AudioStreamServer):
        def _build_http_app(self) -> web.Application:
            app = super()._build_http_app()
            from engine.worker.browser import browser_ws_handler
            app.add_routes([
                web.get("/healthz", _healthz),
                web.post("/internal/calls/{call_id}/control", _control),
                web.post("/internal/drain", _drain),
                web.get("/browser", browser_ws_handler),
            ])
            app.on_startup.append(_on_http_startup)     # runs inside the event loop, after prewarm
            return app

    server = GvoxWorkerServer(listen_addr=listen, host=settings.worker_host, port=settings.worker_http_port,
                              plivo_auth_id=settings.plivo_auth_id, plivo_auth_token=settings.plivo_auth_token,
                              agent_name=settings.worker_agent_name)

    def prewarm(proc: JobProcess):
        W.http = httpx.AsyncClient(timeout=10.0)
        W.prewarmed = prewarm_models(settings)
        proc.userdata.update(W.prewarmed)
        logger.info("gvox-voice worker %s ready: media ws://%s, control http://%s:%d, api=%s", __version__, listen,
                    settings.worker_host, settings.worker_http_port, settings.internal_api_url)

    server.setup_fnc = prewarm

    @server.audio_stream_session()
    async def entrypoint(ctx: JobContext):
        headers = {k.lower(): v for k, v in (ctx.extra_headers or {}).items()}
        call_id = headers.get("x-call-id")
        refuse = W.lifecycle.should_refuse(len(W.active))
        if refuse:
            logger.warning("refusing stream (call_id=%s, provider uuid=%s): %s", call_id, ctx.plivo_call_uuid, refuse)
            await report_refusal(call_id, ctx.plivo_call_uuid, refuse)
            ctx.endpoint.hangup(ctx.session_id)
            return
        runtime = await fetch_runtime(call_id, ctx.plivo_call_uuid)
        if runtime is None:
            logger.error("no engine call for stream (call_id=%s, provider uuid=%s); hanging up", call_id,
                         ctx.plivo_call_uuid)
            ctx.endpoint.hangup(ctx.session_id)
            return
        if runtime.get("status") in ("completed", "failed", "cancelled", "busy", "no_answer"):
            logger.warning("call %s already terminal (%s); dropping stream", runtime["call_id"], runtime["status"])
            ctx.endpoint.hangup(ctx.session_id)
            return

        def session_factory(cfg, state, mcp_servers):
            return make_session(cfg, settings, prewarmed=ctx.proc.userdata if ctx.proc else W.prewarmed,
                                mcp_servers=mcp_servers, userdata=state)

        def attach_io(session, mixer):
            ctx.session = session                      # agent-transport wires its audio I/O + close hook
            if mixer is not None:
                session.input.audio = TeeAudioInput(session.input.audio, mixer)
                session.output.audio = RecordingAudioOutput(session.output.audio, mixer)

        handle, rt, graph = await run_call(runtime, session_factory=session_factory, attach_io=attach_io,
                                           hangup_transport=lambda: ctx.endpoint.hangup(ctx.session_id),
                                           direction=runtime.get("direction", "outbound"))

        async def _on_shutdown(*_args):
            await finalize(handle, handle.end_reason or "stream_ended")

        ctx.add_shutdown_callback(_on_shutdown)

        @ctx.on("dtmf_received")
        def _dtmf(digit):
            handle.reporter.post("turn", {"role": "user", "text": f"[DTMF {digit}]", "at": handle.reporter.at(),
                                          "extra": {"dtmf": digit}})

        logger.info("call %s: starting session (mode=%s, node=%s)", handle.call_id, rt.cfg.mode.value, graph.start_id)
        await handle.session.start(agent=rt.start_agent(handle.session), room=ctx.room)

    return server


# ----------------------------------------------------------------------------- control HTTP (API -> worker)
async def _on_http_startup(app: web.Application) -> None:
    """Loop-side start-up: replay event spools left by a previous worker process."""
    if getattr(W, "http", None) is None:
        W.http = httpx.AsyncClient(timeout=10.0)
    os.makedirs(W.settings.spool_dir, exist_ok=True)
    try:
        n = await replay_pending(W.settings, W.http, W.settings.spool_dir, W.spools)
        if n:
            logger.warning("replaying %d event spool(s) from a previous run", n)
    except Exception as e:
        logger.warning("spool replay failed: %s", e)
    W.lifecycle.http = W.http
    W.tasks.append(asyncio.create_task(W.lifecycle.heartbeat_loop(lambda: len(W.active), W.spool_pending),
                                       name="gvox-heartbeat"))


def _health_payload() -> dict[str, Any]:
    return {"status": "draining" if W.lifecycle.draining else "ok", "version": __version__,
            "worker_id": W.worker_id, "active_calls": len(W.active), "max_sessions": W.settings.worker_max_sessions,
            "draining": W.lifecycle.draining, "spool_pending": W.spool_pending()}


async def _healthz(request: web.Request) -> web.Response:
    # 503 while draining so a proxy health check stops routing new streams here; live streams are unaffected
    return web.json_response(_health_payload(), status=503 if W.lifecycle.draining else 200)


async def _end_handle(handle: CallHandle, reason: str) -> None:
    await handle.state.end(reason)


async def _drain(request: web.Request) -> web.Response:
    if request.headers.get("X-Internal-Token") != W.settings.internal_token:
        return web.json_response({"error": "unauthorized"}, status=401)
    W.lifecycle.start_drain(lambda: list(W.active.values()), _end_handle, W.spool_pending)
    return web.json_response({"draining": True, "active": len(W.active), "spool_pending": W.spool_pending()})


async def _control(request: web.Request) -> web.Response:
    if request.headers.get("X-Internal-Token") != W.settings.internal_token:
        return web.json_response({"error": "unauthorized"}, status=401)
    call_id = request.match_info["call_id"]
    handle = W.active.get(call_id)
    if handle is None:
        return web.json_response({"error": "call not live on this worker"}, status=404)
    body = await request.json()
    action = body.get("action")
    try:
        if action == "hangup":
            asyncio.create_task(handle.state.end("api_hangup"))
        elif action == "say":
            handle.session.say(str(body.get("text") or ""), allow_interruptions=bool(body.get("allow_interruptions", True)))
        elif action == "transfer":
            handle.state.transfer_to = str(body.get("to") or "")
            await handle.reporter.report_tool("transfer", {"transfer_requested": True, "to": handle.state.transfer_to,
                                                           "announce": body.get("announce")})
            if body.get("announce"):
                handle.session.say(str(body["announce"]), allow_interruptions=False)
            asyncio.create_task(handle.state.end("transfer"))
        else:
            return web.json_response({"error": f"unknown action '{action}'"}, status=400)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)
    return web.json_response({"ok": True})


def _patch_windows_signal_handlers() -> None:
    """agent-transport installs SIGINT/SIGTERM handlers on the event loop; Windows loops raise
    NotImplementedError for that. Make it a no-op there (Ctrl+C still stops the process)."""
    import sys

    if sys.platform != "win32":
        return
    import asyncio

    base = asyncio.base_events.BaseEventLoop
    if getattr(base, "_gvox_signal_patch", False):
        return
    base.add_signal_handler = lambda self, sig, callback, *args: None  # type: ignore[assignment]
    base.remove_signal_handler = lambda self, sig: False               # type: ignore[assignment]
    base._gvox_signal_patch = True  # type: ignore[attr-defined]


def main() -> None:
    settings = get_settings()
    logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    os.makedirs(settings.recordings_dir, exist_ok=True)
    _patch_windows_signal_handlers()
    server = build_server()
    server.run()


if __name__ == "__main__":
    main()
