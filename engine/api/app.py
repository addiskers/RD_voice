"""Engine REST API (FastAPI).

Surfaces:
  /v1/*          platform backend -> engine (Bearer ENGINE_API_KEY)
  /carrier/*     carrier webhooks: answer (returns <Stream> XML), ring, hangup
  /internal/*    worker -> engine (X-Internal-Token)
  /health, /v1/capacity
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from engine import __version__
from engine.api.db import Call, Database, PhoneNumber, iso
from engine.api.service import EngineService, ServiceError
from engine.api.webhooks import WebhookDispatcher
from engine.carriers.registry import CarrierRegistry
from engine.config import Settings, get_settings
from engine.schemas.agent import AgentConfig, AgentVersionOut
from engine.schemas.calls import CallCreate, CallCreated, CallRecord, CallStatus, Capacity, Direction

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- app state
class State:
    settings: Settings
    db: Database
    dispatcher: WebhookDispatcher
    carriers: CarrierRegistry
    service: EngineService


def build_app(settings: Optional[Settings] = None) -> FastAPI:
    settings = settings or get_settings()
    state = State()
    state.settings = settings

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO),
                            format="%(asctime)s %(levelname)s %(name)s: %(message)s")
        state.db = Database(settings.database_url)
        await state.db.init()
        state.dispatcher = WebhookDispatcher()
        await state.dispatcher.start()
        state.carriers = CarrierRegistry(settings)
        state.service = EngineService(settings, state.carriers, state.dispatcher)
        os.makedirs(settings.recordings_dir, exist_ok=True)
        logger.info("gvox-voice API %s up; carriers=%s; public=%s ws=%s", __version__,
                    ",".join(n for n in state.carriers.names() if state.carriers.get(n).configured()) or "none",
                    settings.public_http_url, settings.public_ws_url)

        async def _sweeper():
            import asyncio
            while True:
                await asyncio.sleep(60)
                try:
                    async with state.db.session() as session:
                        n = await state.service.sweep_stale(session)
                        if n:
                            logger.warning("swept %d stale call(s)", n)
                except Exception as e:
                    logger.warning("stale sweep failed: %s", e)

        import asyncio
        sweeper = asyncio.create_task(_sweeper())
        try:
            yield
        finally:
            sweeper.cancel()
            await state.dispatcher.close()
            await state.db.close()

    app = FastAPI(title="GlobalVox Voice Engine", version=__version__, lifespan=lifespan,
                  description="Carrier WebSocket -> LiveKit Agents conversation runtime, driven by a REST API.")
    app.state.engine = state

    # ----------------------------------------------------------------- deps
    async def db_session():
        async with state.db.session() as session:
            yield session

    async def require_api_key(authorization: str = Header(default="")):
        token = authorization.removeprefix("Bearer ").strip()
        if not settings.engine_api_key or token != settings.engine_api_key:
            raise HTTPException(401, "invalid or missing API key")

    async def require_internal(x_internal_token: str = Header(default="")):
        if x_internal_token != settings.internal_token:
            raise HTTPException(401, "invalid internal token")

    async def load_call(call_id: str, db: AsyncSession) -> Call:
        call = await db.get(Call, call_id)
        if call is None:
            raise HTTPException(404, f"call '{call_id}' not found")
        return call

    @app.exception_handler(ServiceError)
    async def _service_error(_: Request, exc: ServiceError):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    # ----------------------------------------------------------------- health / capacity
    @app.get("/health", tags=["ops"])
    async def health(db: AsyncSession = Depends(db_session)):
        active = await state.service.active_count(db)
        return {"status": "ok", "version": __version__, "active_calls": active}

    @app.get("/v1/capacity", response_model=Capacity, tags=["ops"], dependencies=[Depends(require_api_key)])
    async def capacity(db: AsyncSession = Depends(db_session)):
        active = await state.service.active_count(db)
        available = max(0, settings.max_concurrent_calls - active)
        return Capacity(active_calls=active, max_concurrent_calls=settings.max_concurrent_calls,
                        available=available, accepting=available > 0)

    # ----------------------------------------------------------------- agents
    @app.put("/v1/agents/{agent_id}", response_model=AgentVersionOut, tags=["agents"],
             dependencies=[Depends(require_api_key)])
    async def put_agent(agent_id: str, cfg: AgentConfig, db: AsyncSession = Depends(db_session)):
        row = await state.service.put_agent(db, agent_id, cfg)
        return AgentVersionOut(agent_id=row.agent_id, version=row.version, created_at=iso(row.created_at) or "",
                               config=AgentConfig.model_validate(row.config))

    @app.get("/v1/agents/{agent_id}", response_model=AgentVersionOut, tags=["agents"],
             dependencies=[Depends(require_api_key)])
    async def get_agent(agent_id: str, version: Optional[int] = None, db: AsyncSession = Depends(db_session)):
        row = await state.service.get_agent(db, agent_id, version)
        return AgentVersionOut(agent_id=row.agent_id, version=row.version, created_at=iso(row.created_at) or "",
                               config=AgentConfig.model_validate(row.config))

    @app.get("/v1/agents/{agent_id}/versions", tags=["agents"], dependencies=[Depends(require_api_key)])
    async def list_versions(agent_id: str, db: AsyncSession = Depends(db_session)):
        rows = await state.service.list_agent_versions(db, agent_id)
        return [{"agent_id": r.agent_id, "version": r.version, "created_at": iso(r.created_at)} for r in rows]

    @app.post("/v1/agents/validate", tags=["agents"], dependencies=[Depends(require_api_key)])
    async def validate_agent(cfg: AgentConfig):
        state.service.validate_agent(cfg)
        wf = cfg.effective_workflow()
        from engine.worker.graph.validate import compile_workflow
        graph = compile_workflow(wf)
        return {"ok": True, "nodes": len(wf.nodes), "edges": len(wf.edges),
                "required_variables": sorted(graph.required_variables)}

    # ----------------------------------------------------------------- numbers
    class NumberIn(BaseModel):
        provider: str = "plivo"
        agent_id: Optional[str] = None
        agent_version: Optional[int] = None
        route_url: Optional[str] = Field(default=None, description="Platform endpoint returning per-call variables")
        webhook_url: Optional[str] = None
        webhook_secret: Optional[str] = None

    @app.put("/v1/numbers/{e164}", tags=["numbers"], dependencies=[Depends(require_api_key)])
    async def put_number(e164: str, body: NumberIn, db: AsyncSession = Depends(db_session)):
        if body.agent_id:
            await state.service.get_agent(db, body.agent_id, body.agent_version)
        row = await db.get(PhoneNumber, e164)
        if row is None:
            row = PhoneNumber(e164=e164, provider=body.provider)
            db.add(row)
        row.provider = body.provider
        row.agent_id = body.agent_id
        row.agent_version = body.agent_version
        row.route_url = body.route_url
        row.webhook_url = body.webhook_url
        row.webhook_secret = body.webhook_secret
        await db.commit()
        base = settings.public_http_url.rstrip("/")
        return {"e164": e164, "provider": row.provider, "agent_id": row.agent_id, "agent_version": row.agent_version,
                "carrier_application": {"answer_url": f"{base}/carrier/{row.provider}/answer", "answer_method": "GET",
                                        "hangup_url": f"{base}/carrier/{row.provider}/hangup", "hangup_method": "POST"},
                "note": "Point the carrier Application for this number at answer_url/hangup_url."}

    @app.get("/v1/numbers", tags=["numbers"], dependencies=[Depends(require_api_key)])
    async def list_numbers(db: AsyncSession = Depends(db_session)):
        from sqlalchemy import select
        rows = await db.scalars(select(PhoneNumber))
        return [{"e164": r.e164, "provider": r.provider, "agent_id": r.agent_id, "agent_version": r.agent_version,
                 "route_url": r.route_url} for r in rows]

    # ----------------------------------------------------------------- calls
    @app.post("/v1/calls", response_model=CallCreated, status_code=201, tags=["calls"],
              dependencies=[Depends(require_api_key)])
    async def create_call(req: CallCreate, db: AsyncSession = Depends(db_session)):
        call = await state.service.create_call(db, req)
        return CallCreated(call_id=call.id, status=CallStatus(call.status), provider=call.provider,
                           provider_call_id=call.provider_call_id)

    @app.get("/v1/calls/{call_id}", response_model=CallRecord, response_model_by_alias=True, tags=["calls"],
             dependencies=[Depends(require_api_key)])
    async def get_call(call_id: str, db: AsyncSession = Depends(db_session)):
        return state.service.to_record(await load_call(call_id, db))

    @app.get("/v1/calls", tags=["calls"], dependencies=[Depends(require_api_key)])
    async def list_calls(limit: int = 50, status: Optional[str] = None, db: AsyncSession = Depends(db_session)):
        from sqlalchemy import select
        stmt = select(Call).order_by(Call.created_at.desc()).limit(min(limit, 500))
        if status:
            stmt = stmt.where(Call.status == status)
        rows = await db.scalars(stmt)
        return [state.service.to_record(c).model_dump(mode="json", by_alias=True, exclude={"transcript"}) for c in rows]

    @app.post("/v1/calls/{call_id}/hangup", tags=["calls"], dependencies=[Depends(require_api_key)])
    async def hangup_call(call_id: str, db: AsyncSession = Depends(db_session)):
        call = await load_call(call_id, db)
        forwarded = await _forward_control(call, {"action": "hangup"})
        if not forwarded:
            await state.service.hangup(db, call)
        return {"call_id": call.id, "status": call.status}

    class SayIn(BaseModel):
        text: str = Field(..., min_length=1)
        allow_interruptions: bool = True

    @app.post("/v1/calls/{call_id}/say", tags=["calls"], dependencies=[Depends(require_api_key)])
    async def say(call_id: str, body: SayIn, db: AsyncSession = Depends(db_session)):
        call = await load_call(call_id, db)
        if not await _forward_control(call, {"action": "say", **body.model_dump()}):
            raise HTTPException(409, "call is not live on a worker")
        return {"ok": True}

    class TransferIn(BaseModel):
        to: str = Field(..., min_length=3)
        announce: Optional[str] = None

    @app.post("/v1/calls/{call_id}/transfer", tags=["calls"], dependencies=[Depends(require_api_key)])
    async def transfer(call_id: str, body: TransferIn, db: AsyncSession = Depends(db_session)):
        call = await load_call(call_id, db)
        if not await _forward_control(call, {"action": "transfer", **body.model_dump()}):
            raise HTTPException(409, "call is not live on a worker")
        return {"ok": True}

    @app.get("/v1/calls/{call_id}/recording", tags=["calls"], dependencies=[Depends(require_api_key)])
    async def recording(call_id: str, db: AsyncSession = Depends(db_session)):
        call = await load_call(call_id, db)
        if not call.recording_path or not os.path.exists(call.recording_path):
            raise HTTPException(404, "no recording for this call")
        return FileResponse(call.recording_path, media_type="audio/wav", filename=f"{call.id}.wav")

    async def _forward_control(call: Call, command: dict[str, Any]) -> bool:
        """Send a live-call command to the worker that holds the call (registered on `started`)."""
        if not call.worker_url or call.status != CallStatus.in_progress.value:
            return False
        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.post(f"{call.worker_url.rstrip('/')}/internal/calls/{call.id}/control",
                                      json=command, headers={"X-Internal-Token": settings.internal_token})
                return r.status_code == 200
        except Exception as e:
            logger.warning("control forward to %s failed: %s", call.worker_url, e)
            return False

    # ----------------------------------------------------------------- carrier webhooks
    async def _params(request: Request) -> dict[str, Any]:
        params: dict[str, Any] = dict(request.query_params)
        if request.method == "POST":
            ctype = request.headers.get("content-type", "")
            try:
                if "json" in ctype:
                    body = await request.json()
                    if isinstance(body, dict):
                        params.update(body)
                else:
                    form = await request.form()
                    params.update({k: v for k, v in form.items()})
            except Exception:
                pass
        return params

    @app.api_route("/carrier/{provider}/answer", methods=["GET", "POST"], tags=["carrier"],
                   response_class=Response)
    async def carrier_answer(provider: str, request: Request, db: AsyncSession = Depends(db_session)):
        carrier = state.carriers.get(provider)
        params = await _params(request)
        info = carrier.parse_callback(params, "answer")
        call_id = params.get("call_id")
        if call_id:
            call = await db.get(Call, call_id)
            if call is None:
                return Response(carrier.hangup_xml(), media_type="application/xml")
            await state.service.on_answer(db, call, info)
        else:
            call = await state.service.create_inbound_call(db, carrier.name, info)
            if call is None:
                return Response(carrier.hangup_xml(), media_type="application/xml")
        xml = carrier.answer_xml(state.service.stream_url(call.id), {"X-Call-Id": call.id})
        return Response(xml, media_type="application/xml")

    @app.api_route("/carrier/{provider}/ring", methods=["GET", "POST"], tags=["carrier"])
    async def carrier_ring(provider: str, request: Request, db: AsyncSession = Depends(db_session)):
        carrier = state.carriers.get(provider)
        params = await _params(request)
        call = await db.get(Call, params.get("call_id", "")) if params.get("call_id") else None
        if call is not None:
            await state.service.on_ring(db, call, carrier.parse_callback(params, "ring"))
        return {"ok": True}

    @app.api_route("/carrier/{provider}/hangup", methods=["GET", "POST"], tags=["carrier"])
    async def carrier_hangup(provider: str, request: Request, db: AsyncSession = Depends(db_session)):
        carrier = state.carriers.get(provider)
        params = await _params(request)
        info = carrier.parse_callback(params, "hangup")
        call: Optional[Call] = None
        if params.get("call_id"):
            call = await db.get(Call, params["call_id"])
        if call is None and info.provider_call_id:
            from sqlalchemy import select
            call = await db.scalar(select(Call).where(Call.provider_call_id == info.provider_call_id))
        if call is not None:
            await state.service.on_hangup(db, call, info)
        return {"ok": True}

    # ----------------------------------------------------------------- internal (worker)
    @app.get("/internal/calls/{call_id}/runtime", tags=["internal"], dependencies=[Depends(require_internal)])
    async def runtime(call_id: str, db: AsyncSession = Depends(db_session)):
        call = await load_call(call_id, db)
        return state.service.runtime_payload(call)

    @app.get("/internal/calls/by-provider/{provider_call_id}/runtime", tags=["internal"],
             dependencies=[Depends(require_internal)])
    async def runtime_by_provider(provider_call_id: str, db: AsyncSession = Depends(db_session)):
        """The media stream's `start` frame carries the carrier CallUUID; the answer webhook stored it."""
        from sqlalchemy import select
        call = await db.scalar(select(Call).where(Call.provider_call_id == provider_call_id)
                               .order_by(Call.created_at.desc()).limit(1))
        if call is None:
            call = await db.scalar(select(Call).where(Call.request_uuid == provider_call_id)
                                   .order_by(Call.created_at.desc()).limit(1))
        if call is None:
            raise HTTPException(404, f"no call for provider id '{provider_call_id}'")
        return state.service.runtime_payload(call)

    class WorkerEvent(BaseModel):
        type: str
        seq: Optional[int] = None
        data: dict[str, Any] = Field(default_factory=dict)

    @app.post("/internal/calls/{call_id}/events", tags=["internal"], dependencies=[Depends(require_internal)])
    async def worker_event(call_id: str, ev: WorkerEvent, db: AsyncSession = Depends(db_session)):
        call = await load_call(call_id, db)
        await state.service.on_worker_event(db, call, ev.type, ev.data, ev.seq)
        return {"ok": True, "status": call.status}

    class BrowserCallIn(BaseModel):
        agent_id: Optional[str] = None
        agent_version: Optional[int] = None
        agent: Optional[AgentConfig] = None
        variables: dict[str, Any] = Field(default_factory=dict)
        metadata: dict[str, Any] = Field(default_factory=dict)
        webhook_url: Optional[str] = None

    @app.post("/internal/calls/browser", tags=["internal"], dependencies=[Depends(require_internal)])
    async def browser_call(body: BrowserCallIn, db: AsyncSession = Depends(db_session)):
        from engine.api.db import utcnow
        from engine.api.service import new_call_id
        from engine.schemas.agent import AgentRef
        if body.agent is not None:
            cfg = await state.service.resolve_agent(db, body.agent)
        elif body.agent_id:
            cfg = await state.service.resolve_agent(db, AgentRef(agent_id=body.agent_id, version=body.agent_version))
        else:
            raise HTTPException(422, "agent or agent_id required")
        call = Call(id=new_call_id(), status=CallStatus.in_progress.value, direction=Direction.browser.value,
                    provider="browser", to_number="browser", from_number=None, agent_id=cfg.id,
                    agent_version=cfg.version, agent_config=cfg.model_dump(mode="json"), variables=body.variables,
                    call_metadata=body.metadata, webhook_url=body.webhook_url, webhook_secret=settings.webhook_secret,
                    record=False, max_duration_s=cfg.max_duration_s or settings.call_max_seconds, answered_at=utcnow())
        db.add(call)
        await db.commit()
        return state.service.runtime_payload(call)

    # ----------------------------------------------------------------- test UI (static, local dev)
    _mount_ui(app, settings, state)
    return app


def _mount_ui(app: FastAPI, settings: Settings, state: State) -> None:
    import json
    from pathlib import Path

    from fastapi.responses import RedirectResponse
    from fastapi.staticfiles import StaticFiles

    root = Path(__file__).resolve().parents[2]
    ui_dir = root / "ui"
    examples = root / "examples"

    @app.get("/", include_in_schema=False)
    async def _root():
        return RedirectResponse("/ui/")

    @app.get("/ui/config.json", include_in_schema=False)
    async def _ui_config():
        keys = {p: bool(settings.provider_key(p)) for p in
                ("google", "deepgram", "cartesia", "elevenlabs", "sarvam", "anthropic", "openai", "groq")}
        carriers = [n for n in state.carriers.names() if state.carriers.get(n).configured()] if hasattr(state, "carriers") else []
        browser_keys = sorted(settings.browser_keys)
        tts_options: dict[str, dict[str, Any]] = {}
        if keys["cartesia"]:
            tts_options["cartesia"] = {"provider": "cartesia", "model": settings.default_tts_model,
                                       "voice": settings.default_tts_voice or None, "language": settings.default_tts_language}
        if keys["sarvam"]:
            tts_options["sarvam"] = {"provider": "sarvam", "model": settings.sarvam_tts_model,
                                     "voice": settings.sarvam_tts_speaker, "language": settings.sarvam_tts_language}
        if keys["elevenlabs"]:
            tts_options["elevenlabs"] = {"provider": "elevenlabs", "model": "eleven_flash_v2_5", "voice": None,
                                         "language": None}
        stt_options: dict[str, dict[str, Any]] = {}
        if keys["deepgram"]:
            stt_options["nova-3"] = {"provider": "deepgram", "model": "nova-3", "options": {"language": settings.deepgram_language}}
            stt_options["flux-general-multi"] = {"provider": "deepgram", "model": "flux-general-multi", "options": {}}
        if keys["sarvam"]:
            stt_options["sarvam"] = {"provider": "sarvam", "model": "saaras:v4", "options": {"language": settings.default_language}}
        llm_options: dict[str, dict[str, Any]] = {}
        if keys["google"]:
            llm_options["gemini-2.5-flash"] = {"provider": "google", "model": "gemini-2.5-flash",
                                               "options": {"temperature": 0.4, "thinking_budget": settings.default_llm_thinking_budget}}
            llm_options["gemini-2.5-flash-lite"] = {"provider": "google", "model": "gemini-2.5-flash-lite",
                                                    "options": {"temperature": 0.4, "thinking_budget": 0}}
        if keys["groq"]:
            # Groq's Llama 3.x endpoints are retired (404 on 2026-09-08); gpt-oss is the current fast tier
            llm_options["groq-gpt-oss-20b"] = {"provider": "groq", "model": "openai/gpt-oss-20b", "options": {"temperature": 0.4}}
            llm_options["groq-gpt-oss-120b"] = {"provider": "groq", "model": "openai/gpt-oss-120b", "options": {"temperature": 0.4}}
        if keys["anthropic"]:
            llm_options["claude-haiku-4-5"] = {"provider": "anthropic", "model": "claude-haiku-4-5", "options": {"temperature": 0.4}}
        default_stt_key = settings.default_stt_model if settings.default_stt_model in stt_options else (next(iter(stt_options)) if stt_options else "")
        default_llm_key = settings.default_llm_model if settings.default_llm_model in llm_options else (next(iter(llm_options)) if llm_options else "")
        return {
            "browser_ws_url": settings.browser_ws_url,
            "browser_key": browser_keys[0] if browser_keys else "",
            "providers": keys,
            "carriers": carriers,
            "cascaded_available": bool(stt_options) and bool(tts_options) and bool(llm_options),
            "stt_default": stt_options.get(default_stt_key) or {"provider": settings.default_stt_provider, "model": settings.default_stt_model,
                                                                 "options": {"language": settings.deepgram_language}},
            "llm_default": llm_options.get(default_llm_key) or {"provider": settings.default_llm_provider, "model": settings.default_llm_model},
            "stt_options": stt_options,
            "llm_options": llm_options,
            "default_stt": default_stt_key,
            "default_llm": default_llm_key,
            "tts_options": tts_options,
            "default_tts_provider": settings.default_tts_provider if settings.default_tts_provider in tts_options
            else (next(iter(tts_options)) if tts_options else ""),
            "default_realtime_model": settings.default_realtime_model,
            "default_realtime_voice": settings.default_realtime_voice,
            "default_llm_model": settings.default_llm_model,
            "default_language": settings.default_language,
            "public_http_url": settings.public_http_url,
            "version": __version__,
        }

    @app.get("/ui/example-agent", include_in_schema=False)
    async def _ui_example_agent():
        agent = json.loads((examples / "eo_rsvp_agent.json").read_text(encoding="utf-8"))
        agent["workflow"] = json.loads((examples / "eo_rsvp_workflow.json").read_text(encoding="utf-8"))
        cascaded_ok = bool(settings.provider_key("deepgram")) and any(
            settings.provider_key(p) for p in ("cartesia", "elevenlabs", "sarvam"))
        # speech-to-speech is the quality default (measured 2026-09-08: faster and far more natural than the
        # cascaded pipeline); cascaded stays available for BYO voices / deterministic per-step control
        agent["mode"] = "s2s" if settings.provider_key("google") else ("cascaded" if cascaded_ok else "s2s")
        agent["stt"] = {"provider": settings.default_stt_provider, "model": settings.default_stt_model,
                        "options": {"language": settings.deepgram_language}}
        agent["llm"] = {"provider": settings.default_llm_provider, "model": settings.default_llm_model,
                        "options": {"temperature": 0.4, "thinking_budget": settings.default_llm_thinking_budget}}
        # Indian callers answer in Hindi/Hinglish; Sarvam's bulbul speaks both, so prefer it for the EO example
        if settings.provider_key("sarvam"):
            agent["tts"] = {"provider": "sarvam", "model": settings.sarvam_tts_model,
                            "voice": settings.sarvam_tts_speaker, "language": settings.sarvam_tts_language}
        elif settings.provider_key("cartesia"):
            agent["tts"] = {"provider": "cartesia", "model": settings.default_tts_model,
                            "voice": settings.default_tts_voice or None, "language": settings.default_tts_language}
        agent["realtime"] = {"provider": "google", "model": settings.default_realtime_model,
                             "voice": settings.default_realtime_voice, "language": settings.default_language}
        return agent

    if ui_dir.is_dir():
        app.mount("/ui", StaticFiles(directory=str(ui_dir), html=True), name="ui")


app = build_app()


def main() -> None:
    import uvicorn
    s = get_settings()
    uvicorn.run("engine.api.app:app", host=s.api_host, port=s.api_port, log_level=s.log_level.lower())


if __name__ == "__main__":
    main()
