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
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

import hmac

from engine import __version__
from engine.api.crypto import SecretBox, encrypt_agent_config, mask_agent_config
from engine.api.db import Call, Database, PhoneNumber, iso
from engine.api.service import EngineService, ServiceError
from engine.api.webhooks import OutboxDispatcher
from engine.carriers.registry import CarrierRegistry
from engine.config import Settings, get_settings
from engine.storage import Storage, build_storage, is_storage_key
from engine.schemas.agent import AgentConfig, AgentVersionOut
from engine.schemas.calls import (CallCreate, CallCreated, CallRecord, CallStatus, Capacity, Direction,
                                  WorkerHeartbeat, WorkerOut)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- app state
class State:
    settings: Settings
    db: Database
    dispatcher: OutboxDispatcher
    carriers: CarrierRegistry
    service: EngineService
    storage: Storage
    box: SecretBox


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
        state.box = SecretBox(settings.engine_encryption_key)
        state.dispatcher = OutboxDispatcher(state.db, secret_resolver=state.box.decrypt)
        await state.dispatcher.start()
        state.carriers = CarrierRegistry(settings)
        state.service = EngineService(settings, state.carriers, state.dispatcher, box=state.box)
        os.makedirs(settings.recordings_dir, exist_ok=True)
        state.storage = build_storage(settings)
        logger.info("gvox-voice API %s up; carriers=%s; public=%s ws=%s", __version__,
                    ",".join(n for n in state.carriers.names() if state.carriers.get(n).configured()) or "none",
                    settings.public_http_url, settings.public_ws_url)

        async def _sweep_recordings():
            try:
                async with state.db.session() as session:
                    n = await state.service.sweep_recordings(session, state.storage)
                    if n:
                        logger.info("retention: deleted %d recording(s)", n)
            except Exception as e:
                logger.warning("recording retention sweep failed: %s", e)

        import asyncio
        stop = asyncio.Event()

        async def _sweeper():
            # cooperative stop: a cancel mid-query wedges aiosqlite's worker thread and blocks shutdown
            tick = 0
            await _sweep_recordings()
            while True:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=10)
                    return
                except asyncio.TimeoutError:
                    pass
                tick += 1
                if tick % 8640 == 0:           # once a day
                    await _sweep_recordings()
                try:
                    async with state.db.session() as session:
                        n = await state.service.sweep_workers(session)
                        if n:
                            logger.warning("failed %d call(s) on dead worker(s)", n)
                except Exception as e:
                    logger.warning("worker sweep failed: %s", e)
                if tick % 6:
                    continue
                try:
                    async with state.db.session() as session:
                        n = await state.service.sweep_stale(session)
                        if n:
                            logger.warning("swept %d stale call(s)", n)
                except Exception as e:
                    logger.warning("stale sweep failed: %s", e)

        sweeper = asyncio.create_task(_sweeper())
        try:
            yield
        finally:
            stop.set()
            try:
                await asyncio.wait_for(sweeper, timeout=15)
            except (asyncio.TimeoutError, Exception):
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
        if not settings.engine_api_key or not token or \
                not hmac.compare_digest(token.encode("utf-8"), settings.engine_api_key.encode("utf-8")):
            raise HTTPException(401, "invalid or missing API key")

    async def require_internal(x_internal_token: str = Header(default="")):
        if x_internal_token != settings.internal_token:
            raise HTTPException(401, "invalid internal token")

    async def load_call(call_id: str, db: AsyncSession) -> Call:
        call = await db.get(Call, call_id)
        if call is None:
            raise HTTPException(404, f"call '{call_id}' not found")
        return call

    state.require_api_key = require_api_key      # reused by the UI mount

    @app.exception_handler(ServiceError)
    async def _service_error(_: Request, exc: ServiceError):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail}, headers=exc.headers or None)

    # ----------------------------------------------------------------- health / capacity
    @app.get("/health", tags=["ops"])
    async def health(db: AsyncSession = Depends(db_session)):
        try:
            snap = await state.service.ops_snapshot(db)
            active = await state.service.active_count(db)
            db_ok = "ok"
        except Exception as e:
            logger.warning("health: database check failed: %s", e)
            return JSONResponse(status_code=503, content={"status": "degraded", "version": __version__, "db": "error",
                                                          "active_calls": None, "workers_live": None,
                                                          "outbox_pending": None, "outbox_oldest_s": None})
        return {"status": "ok", "version": __version__, "active_calls": active, "db": db_ok,
                "workers_live": snap["workers"]["live"], "outbox_pending": snap["outbox"]["pending"],
                "outbox_oldest_s": snap["outbox"]["oldest_s"]}

    # Prometheus gauges live in a per-app registry (several apps are built in one test process)
    from prometheus_client import CollectorRegistry, Gauge, generate_latest, CONTENT_TYPE_LATEST
    registry = CollectorRegistry()
    g_calls = Gauge("gvox_calls", "calls by status", ["status"], registry=registry)
    g_engine_free = Gauge("gvox_engine_free", "free conversation slots", registry=registry)
    g_engine_total = Gauge("gvox_engine_total", "total conversation slots", registry=registry)
    g_carrier_free = Gauge("gvox_carrier_free", "free carrier slots", ["provider"], registry=registry)
    g_carrier_total = Gauge("gvox_carrier_total", "carrier slot limit", ["provider"], registry=registry)
    g_outbox_pending = Gauge("gvox_outbox_pending", "webhook deliveries pending or in flight", registry=registry)
    g_outbox_dead = Gauge("gvox_outbox_dead", "webhook deliveries given up on", registry=registry)
    g_outbox_oldest = Gauge("gvox_outbox_oldest_seconds", "age of the oldest pending delivery", registry=registry)
    g_workers = Gauge("gvox_workers", "workers by status", ["status"], registry=registry)

    @app.get("/metrics", tags=["ops"], dependencies=[Depends(require_internal)], include_in_schema=False)
    async def metrics(db: AsyncSession = Depends(db_session)):
        snap = await state.service.ops_snapshot(db)
        for status in ("queued", "dialing", "ringing", "in_progress", "completed", "failed", "busy", "no_answer", "cancelled"):
            g_calls.labels(status=status).set(snap["calls"].get(status, 0))
        g_engine_free.set(snap["engine"]["free"])
        g_engine_total.set(snap["engine"]["total"])
        for provider, pool in snap["carriers"].items():
            g_carrier_free.labels(provider=provider).set(pool["free"])
            g_carrier_total.labels(provider=provider).set(pool["total"])
        g_outbox_pending.set(snap["outbox"]["pending"])
        g_outbox_dead.set(snap["outbox"]["dead"])
        g_outbox_oldest.set(snap["outbox"]["oldest_s"] or 0)
        for status, n in snap["workers"].items():
            g_workers.labels(status=status).set(n)
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    @app.get("/v1/capacity", response_model=Capacity, tags=["ops"], dependencies=[Depends(require_api_key)])
    async def capacity(db: AsyncSession = Depends(db_session)):
        view = await state.service.capacity(db)
        return Capacity(active_calls=view.legacy_active, max_concurrent_calls=view.engine_total,
                        available=view.available, accepting=view.available > 0,
                        engine={"free": view.engine_free, "total": view.engine_total},
                        carriers={k: p.as_dict() for k, p in view.carriers.items()}, workers=view.workers)

    # ----------------------------------------------------------------- workers
    @app.post("/internal/workers/heartbeat", tags=["internal"], dependencies=[Depends(require_internal)])
    async def worker_heartbeat(hb: WorkerHeartbeat, db: AsyncSession = Depends(db_session)):
        await state.service.upsert_worker(db, hb)
        return {"ok": True}

    @app.get("/v1/workers", response_model=list[WorkerOut], tags=["ops"], dependencies=[Depends(require_api_key)])
    async def list_workers(db: AsyncSession = Depends(db_session)):
        return await state.service.list_workers(db)

    @app.post("/v1/workers/{worker_id}/drain", tags=["ops"], dependencies=[Depends(require_api_key)])
    async def drain_worker(worker_id: str, db: AsyncSession = Depends(db_session)):
        ok = await state.service.drain_worker(db, worker_id)
        return {"worker_id": worker_id, "draining": True, "worker_acknowledged": ok}

    # ----------------------------------------------------------------- agents
    def _agent_out(row) -> AgentVersionOut:
        # provider keys are never returned in clear: masked to their last four characters
        return AgentVersionOut(agent_id=row.agent_id, version=row.version, created_at=iso(row.created_at) or "",
                               config=AgentConfig.model_validate(mask_agent_config(row.config, state.box)))

    @app.put("/v1/agents/{agent_id}", response_model=AgentVersionOut, tags=["agents"],
             dependencies=[Depends(require_api_key)])
    async def put_agent(agent_id: str, cfg: AgentConfig, db: AsyncSession = Depends(db_session)):
        return _agent_out(await state.service.put_agent(db, agent_id, cfg))

    @app.get("/v1/agents/{agent_id}", response_model=AgentVersionOut, tags=["agents"],
             dependencies=[Depends(require_api_key)])
    async def get_agent(agent_id: str, version: Optional[int] = None, db: AsyncSession = Depends(db_session)):
        return _agent_out(await state.service.get_agent(db, agent_id, version))

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
        row.webhook_secret = state.box.encrypt(body.webhook_secret) if body.webhook_secret else None
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

    @app.get("/v1/calls/{call_id}/cost", tags=["calls"], dependencies=[Depends(require_api_key)])
    async def call_cost(call_id: str, db: AsyncSession = Depends(db_session)):
        """The per-stage cost estimate recomputed with the current price list (the record keeps the one
        computed when the call ended)."""
        call = await load_call(call_id, db)
        if "usage_detail" not in (call.metrics or {}):     # {} is a call with no model usage, still priced (carrier)
            raise HTTPException(404, "no usage recorded for this call (it has not ended, or ran on an older worker)")
        return state.service.cost_for(call)

    @app.get("/v1/calls/{call_id}/deliveries", tags=["calls"], dependencies=[Depends(require_api_key)])
    async def list_deliveries(call_id: str, db: AsyncSession = Depends(db_session)):
        from sqlalchemy import select
        from engine.api.db import WebhookDelivery
        await load_call(call_id, db)
        rows = await db.scalars(select(WebhookDelivery).where(WebhookDelivery.call_id == call_id)
                                .order_by(WebhookDelivery.seq))
        return [{"id": r.id, "seq": r.seq, "event": r.event, "status": r.status, "attempts": r.attempts,
                 "last_status_code": r.last_status_code, "last_error": r.last_error, "created_at": iso(r.created_at),
                 "delivered_at": iso(r.delivered_at), "next_attempt_at": iso(r.next_attempt_at), "url": r.url}
                for r in rows]

    @app.post("/v1/calls/{call_id}/deliveries/{seq}/replay", tags=["calls"], dependencies=[Depends(require_api_key)])
    async def replay_delivery(call_id: str, seq: int, db: AsyncSession = Depends(db_session)):
        await load_call(call_id, db)
        if not await state.dispatcher.replay(db, call_id, seq):
            raise HTTPException(404, f"no delivery seq {seq} for call '{call_id}'")
        return {"ok": True}

    @app.get("/v1/calls/{call_id}/recording", tags=["calls"], dependencies=[Depends(require_api_key)])
    async def recording(call_id: str, db: AsyncSession = Depends(db_session)):
        call = await load_call(call_id, db)
        if not call.recording_path:
            raise HTTPException(404, "no recording for this call")
        if is_storage_key(call.recording_path):
            if not await state.storage.exists(call.recording_path):
                raise HTTPException(404, "no recording for this call")
            if settings.recording_url_mode == "signed":
                url = state.storage.signed_url(call.recording_path)
                if url:
                    return RedirectResponse(url, status_code=307)
            data = await state.storage.get_bytes(call.recording_path)
            return Response(data, media_type="audio/wav",
                            headers={"Content-Disposition": f'inline; filename="{call.id}.wav"'})
        if not os.path.exists(call.recording_path):      # legacy local file (pre-slice-1 or failed upload)
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
    async def _body_params(request: Request) -> dict[str, Any]:
        """POST body fields only (form or JSON); what Plivo includes in its V3 signature."""
        if request.method != "POST":
            return {}
        ctype = request.headers.get("content-type", "")
        try:
            if "json" in ctype:
                body = await request.json()
                return dict(body) if isinstance(body, dict) else {}
            form = await request.form()
            return {k: v for k, v in form.items()}
        except Exception:
            return {}

    async def _params(request: Request) -> dict[str, Any]:
        params: dict[str, Any] = dict(request.query_params)
        params.update(await _body_params(request))
        return params

    def _client_ip(request: Request) -> str:
        """Remote address; the first X-Forwarded-For hop is trusted only when the direct peer is private
        (the Caddy proxy in compose)."""
        import ipaddress
        host = request.client.host if request.client else ""
        fwd = request.headers.get("x-forwarded-for", "")
        if fwd:
            try:
                if ipaddress.ip_address(host).is_private or ipaddress.ip_address(host).is_loopback:
                    return fwd.split(",")[0].strip()
            except ValueError:
                pass
        return host

    def _carrier_or_404(provider: str):
        """Only carriers configured on this engine have callback routes (an unconfigured one would otherwise
        let its weaker verifier drive another carrier's calls)."""
        from engine.carriers.base import CarrierError
        try:
            carrier = state.carriers.get(provider)
        except CarrierError:
            raise HTTPException(404, f"unknown carrier '{provider}'")
        if not carrier.configured():
            raise HTTPException(404, f"carrier '{provider}' is not configured on this engine")
        return carrier

    async def _verify_carrier(carrier, request: Request) -> None:
        if not settings.carrier_verify_signatures:
            return
        url = settings.public_http_url.rstrip("/") + request.url.path
        if request.url.query:
            url += "?" + request.url.query
        ok = carrier.verify_callback(method=request.method, public_url=url, headers=request.headers,
                                     params=await _body_params(request), remote_ip=_client_ip(request))
        if not ok:
            if settings.carrier_verify_log_only:
                logger.warning("carrier %s callback signature FAILED verification (log-only mode: allowed) "
                               "from %s: %s %s", carrier.name, _client_ip(request), request.method, url)
                return
            raise HTTPException(403, "carrier signature invalid")

    @app.api_route("/carrier/{provider}/answer", methods=["GET", "POST"], tags=["carrier"],
                   response_class=Response)
    async def carrier_answer(provider: str, request: Request, db: AsyncSession = Depends(db_session)):
        carrier = _carrier_or_404(provider)
        await _verify_carrier(carrier, request)
        params = await _params(request)
        info = carrier.parse_callback(params, "answer")
        call_id = params.get("call_id")
        if call_id:
            call = await db.get(Call, call_id)
            if call is None or call.provider != carrier.name:      # a call is only driven through its own carrier
                return Response(carrier.hangup_xml(), media_type="application/xml")
            await state.service.on_answer(db, call, info)
        else:
            free, _total = await state.service.engine_slots(db)
            if free <= 0:
                await state.service.refuse_inbound(db, carrier.name, info)
                return Response(carrier.busy_xml(), media_type="application/xml")
            call = await state.service.create_inbound_call(db, carrier.name, info)
            if call is None:
                return Response(carrier.hangup_xml(), media_type="application/xml")
        xml = carrier.answer_xml(state.service.stream_url(call.id), {"X-Call-Id": call.id})
        return Response(xml, media_type="application/xml")

    @app.api_route("/carrier/{provider}/ring", methods=["GET", "POST"], tags=["carrier"])
    async def carrier_ring(provider: str, request: Request, db: AsyncSession = Depends(db_session)):
        carrier = _carrier_or_404(provider)
        await _verify_carrier(carrier, request)
        params = await _params(request)
        call = await db.get(Call, params.get("call_id", "")) if params.get("call_id") else None
        if call is not None and call.provider != carrier.name:
            raise HTTPException(404, "call not found for this provider")
        if call is not None:
            await state.service.on_ring(db, call, carrier.parse_callback(params, "ring"))
        return {"ok": True}

    @app.api_route("/carrier/{provider}/hangup", methods=["GET", "POST"], tags=["carrier"])
    async def carrier_hangup(provider: str, request: Request, db: AsyncSession = Depends(db_session)):
        carrier = _carrier_or_404(provider)
        await _verify_carrier(carrier, request)
        params = await _params(request)
        info = carrier.parse_callback(params, "hangup")
        call: Optional[Call] = None
        if params.get("call_id"):
            call = await db.get(Call, params["call_id"])
        if call is None and info.provider_call_id:
            from sqlalchemy import select
            call = await db.scalar(select(Call).where(Call.provider_call_id == info.provider_call_id,
                                                      Call.provider == carrier.name))
        if call is not None and call.provider != carrier.name:
            raise HTTPException(404, "call not found for this provider")
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
                    agent_version=cfg.version, agent_config=encrypt_agent_config(cfg.model_dump(mode="json"), state.box),
                    variables=body.variables, call_metadata=body.metadata, webhook_url=body.webhook_url,
                    webhook_secret=state.box.encrypt(settings.webhook_secret),
                    record=False, max_duration_s=cfg.max_duration_s or settings.call_max_seconds, answered_at=utcnow())
        db.add(call)
        await db.commit()
        return state.service.runtime_payload(call)

    # ----------------------------------------------------------------- test endpoints (ENABLE_TEST_ENDPOINTS)
    if settings.enable_test_endpoints:
        _mount_test_endpoints(app, settings, state, require_internal)

    # ----------------------------------------------------------------- test UI (static, local dev)
    _mount_ui(app, settings, state)
    return app


def _mount_test_endpoints(app: FastAPI, settings: Settings, state: State, require_internal) -> None:
    """Runs scripts/fake_carrier.py (3 scripted calls through the real worker endpoint) as a background job."""
    import asyncio
    import json
    import sys
    import uuid
    from pathlib import Path

    jobs: dict[str, dict[str, Any]] = {}
    script = Path(__file__).resolve().parents[2] / "scripts" / "fake_carrier.py"

    async def _run(job_id: str, calls: int, script_steps: str) -> None:
        cmd = [sys.executable, str(script), "--api", f"http://127.0.0.1:{settings.api_port}", "--key", settings.engine_api_key,
               "--internal-token", settings.internal_token, "--worker-ws", settings.worker_media_url,
               "--calls", str(calls), "--script", script_steps, "--json"]
        try:
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            out, err = await asyncio.wait_for(proc.communicate(), timeout=600)
            lines = [ln for ln in out.decode("utf-8", "replace").splitlines() if ln.startswith("{")]
            result = json.loads(lines[-1]) if lines else {"error": (err or out).decode("utf-8", "replace")[-800:]}
            jobs[job_id].update(status="done" if proc.returncode == 0 else "failed", result=result,
                                stderr=err.decode("utf-8", "replace")[-2000:])
        except Exception as e:
            jobs[job_id].update(status="failed", result={"error": str(e)})

    class SmokeIn(BaseModel):
        calls: int = Field(default=3, ge=1, le=25)
        script: str = "yes"

    @app.post("/v1/test/telephony-smoke", tags=["test"], dependencies=[Depends(require_internal)])
    async def start_smoke(body: SmokeIn):
        job_id = uuid.uuid4().hex[:10]
        jobs[job_id] = {"status": "running", "calls": body.calls, "script": body.script}
        asyncio.create_task(_run(job_id, body.calls, body.script))
        return {"job_id": job_id}

    @app.get("/v1/test/telephony-smoke/{job_id}", tags=["test"], dependencies=[Depends(require_internal)])
    async def smoke_status(job_id: str):
        job = jobs.get(job_id)
        if job is None:
            raise HTTPException(404, "unknown job")
        return job


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

    @app.get("/ui/config.json", include_in_schema=False, dependencies=[Depends(state.require_api_key)])
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


def _cli():
    import typer
    cli = typer.Typer(invoke_without_command=True, add_completion=False, help="GlobalVox Voice Engine API")

    @cli.callback()
    def _root(ctx: typer.Context) -> None:
        if ctx.invoked_subcommand is None:
            serve()

    @cli.command()
    def serve() -> None:
        """Run the API (default)."""
        import uvicorn
        s = get_settings()
        uvicorn.run("engine.api.app:app", host=s.api_host, port=s.api_port, log_level=s.log_level.lower())

    @cli.command()
    def migrate() -> None:
        """Upgrade the database schema to the latest migration and exit."""
        from engine.api.db import run_migrations
        run_migrations(get_settings().database_url)
        typer.echo("database is at head")

    @cli.command("encrypt-existing")
    def encrypt_existing() -> None:
        """Encrypt provider keys and webhook secrets that were stored before ENGINE_ENCRYPTION_KEY was set."""
        import asyncio
        from engine.api.maintenance import encrypt_existing_rows
        s = get_settings()
        if not s.engine_encryption_key:
            raise typer.BadParameter("ENGINE_ENCRYPTION_KEY is not set")
        n = asyncio.run(encrypt_existing_rows(s.database_url, SecretBox(s.engine_encryption_key)))
        typer.echo(f"encrypted {n} value(s)")

    @cli.command("copy-sqlite")
    def copy_sqlite(from_path: str = typer.Option(..., "--from", help="path of the SQLite file to copy from")) -> None:
        """Copy agents, numbers and calls from a Phase-0 SQLite file into DATABASE_URL."""
        import asyncio
        from engine.api.maintenance import copy_sqlite as _copy
        n = asyncio.run(_copy(f"sqlite+aiosqlite:///{from_path}", get_settings().database_url))
        typer.echo(f"copied {n} row(s)")

    return cli


def main() -> None:
    _cli()()


if __name__ == "__main__":
    main()
