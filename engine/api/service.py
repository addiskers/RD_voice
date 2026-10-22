"""Call lifecycle: create -> dial -> answer XML -> worker runtime -> events -> finalize -> webhooks.

All state transitions live here so the HTTP layer (app.py) stays thin. Terminal transitions are
idempotent: ``call.ended`` / ``call.failed`` is emitted exactly once per call.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import quote

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from engine.api.capacity import CapacityView, CarrierPool, CpsBudget
from engine.api.db import AgentVersion, Call, PhoneNumber, Worker, iso, utcnow
from engine.api.webhooks import OutboxDispatcher
from engine.carriers.base import CallbackInfo, CarrierError
from engine.carriers.registry import CarrierRegistry
from engine.config import Settings
from engine.schemas.agent import AgentConfig, AgentRef
from engine.schemas.calls import (TERMINAL_STATUSES, CallCreate, CallMetrics, CallRecord, CallStatus,
                                  Direction, TranscriptTurn, WorkerHeartbeat, WorkerOut)
from engine.schemas.webhooks import EventType
from engine.worker.graph.validate import validate_workflow

logger = logging.getLogger(__name__)

ACTIVE_STATUSES = (CallStatus.queued.value, CallStatus.dialing.value, CallStatus.ringing.value,
                   CallStatus.in_progress.value)


class ServiceError(Exception):
    def __init__(self, status_code: int, detail: Any, headers: Optional[dict[str, str]] = None):
        self.status_code = status_code
        self.detail = detail
        self.headers = headers or {}
        super().__init__(str(detail))


def new_call_id() -> str:
    return f"call_{uuid.uuid4().hex[:12]}"


def _aware(dt: Optional[datetime]) -> Optional[datetime]:
    """SQLite hands back naive datetimes; treat them as UTC so arithmetic with utcnow() works."""
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


class EngineService:
    def __init__(self, settings: Settings, carriers: CarrierRegistry, dispatcher: OutboxDispatcher):
        self.settings = settings
        self.carriers = carriers
        self.dispatcher = dispatcher
        self.cps = CpsBudget(settings.carrier_cps_limit)

    # ---------------------------------------------------------------- agents
    def validate_agent(self, cfg: AgentConfig) -> None:
        wf = cfg.effective_workflow()
        errors = validate_workflow(wf)
        if errors:
            raise ServiceError(422, {"workflow_errors": [str(e) for e in errors]})

    async def put_agent(self, db: AsyncSession, agent_id: str, cfg: AgentConfig) -> AgentVersion:
        self.validate_agent(cfg)
        latest = await db.scalar(select(func.max(AgentVersion.version)).where(AgentVersion.agent_id == agent_id))
        version = int(latest or 0) + 1
        cfg = cfg.model_copy(update={"id": agent_id, "version": version})
        row = AgentVersion(agent_id=agent_id, version=version, config=cfg.model_dump(mode="json"))
        db.add(row)
        await db.commit()
        return row

    async def get_agent(self, db: AsyncSession, agent_id: str, version: Optional[int] = None) -> AgentVersion:
        stmt = select(AgentVersion).where(AgentVersion.agent_id == agent_id)
        if version is not None:
            stmt = stmt.where(AgentVersion.version == version)
        stmt = stmt.order_by(AgentVersion.version.desc()).limit(1)
        row = await db.scalar(stmt)
        if row is None:
            raise ServiceError(404, f"agent '{agent_id}'" + (f" version {version}" if version else "") + " not found")
        return row

    async def list_agent_versions(self, db: AsyncSession, agent_id: str) -> list[AgentVersion]:
        rows = await db.scalars(select(AgentVersion).where(AgentVersion.agent_id == agent_id)
                                .order_by(AgentVersion.version.desc()))
        return list(rows)

    async def resolve_agent(self, db: AsyncSession, ref) -> AgentConfig:
        if isinstance(ref, AgentRef):
            row = await self.get_agent(db, ref.agent_id, ref.version)
            return AgentConfig.model_validate(row.config)
        cfg = ref
        self.validate_agent(cfg)
        return cfg

    # ---------------------------------------------------------------- capacity
    async def active_count(self, db: AsyncSession) -> int:
        n = await db.scalar(select(func.count()).select_from(Call).where(Call.status.in_(ACTIVE_STATUSES)))
        return int(n or 0)

    async def _count(self, db: AsyncSession, *statuses: str, provider: Optional[str] = None) -> int:
        stmt = select(func.count()).select_from(Call).where(Call.status.in_(statuses))
        if provider:
            stmt = stmt.where(Call.provider == provider)
        return int(await db.scalar(stmt) or 0)

    async def engine_slots(self, db: AsyncSession) -> tuple[int, int]:
        """``(free, total)`` conversation slots. With no worker ever registered (single-dev runs, tests) the
        static ``MAX_CONCURRENT_CALLS`` is the pool; otherwise live, non-draining workers are."""
        in_progress = await self._count(db, CallStatus.in_progress.value)
        any_worker = await db.scalar(select(func.count()).select_from(Worker))
        if not any_worker:
            total = self.settings.max_concurrent_calls
            return max(0, total - in_progress), total
        workers = [w for w in await self.live_workers(db) if not w.draining]
        total = sum(int(w.max_sessions or 0) for w in workers)
        reported = sum(int(w.active_sessions or 0) for w in workers)
        return max(0, total - max(reported, in_progress)), total

    async def carrier_pool(self, db: AsyncSession, provider: str) -> CarrierPool:
        total = self.settings.carrier_limit(provider)
        used = await self._count(db, CallStatus.dialing.value, CallStatus.ringing.value, CallStatus.in_progress.value,
                                 provider=provider)
        return CarrierPool(total=total, free=max(0, total - used), cps_free=self.cps.available(provider))

    async def capacity(self, db: AsyncSession) -> CapacityView:
        free, total = await self.engine_slots(db)
        pools = {}
        for name in self.carriers.names():
            if self.carriers.get(name).configured():
                pools[name] = await self.carrier_pool(db, name)
        workers = [w.model_dump(mode="json") for w in await self.list_workers(db)]
        return CapacityView(engine_total=total, engine_free=free, carriers=pools, workers=workers,
                            legacy_active=await self.active_count(db))

    # ---------------------------------------------------------------- workers
    async def upsert_worker(self, db: AsyncSession, hb: WorkerHeartbeat) -> Worker:
        row = await db.get(Worker, hb.worker_id)
        if row is None:
            row = Worker(worker_id=hb.worker_id, worker_url=hb.worker_url)
            db.add(row)
        row.worker_url = hb.worker_url
        row.version = hb.version or ""
        row.active_sessions = int(hb.active_sessions)
        row.max_sessions = int(hb.max_sessions)
        row.draining = bool(hb.draining)
        if hb.started_at:
            try:
                row.started_at = datetime.fromisoformat(hb.started_at)
            except ValueError:
                pass
        row.last_seen_at = utcnow()
        row.status = "live"
        await db.commit()
        return row

    def _worker_out(self, w: Worker) -> WorkerOut:
        seen = _aware(w.last_seen_at) or utcnow()
        return WorkerOut(worker_id=w.worker_id, worker_url=w.worker_url, version=w.version or "",
                         active_sessions=w.active_sessions or 0, max_sessions=w.max_sessions or 0,
                         draining=bool(w.draining), started_at=iso(w.started_at), status=w.status or "live",
                         seconds_since_seen=round(max(0.0, (utcnow() - seen).total_seconds()), 1),
                         last_seen_at=iso(w.last_seen_at) or "")

    async def list_workers(self, db: AsyncSession) -> list[WorkerOut]:
        rows = await db.scalars(select(Worker).order_by(Worker.worker_id))
        return [self._worker_out(w) for w in rows]

    async def live_workers(self, db: AsyncSession) -> list[Worker]:
        """Workers heard from recently (regardless of the sweeper having run)."""
        from datetime import timedelta
        cutoff = utcnow() - timedelta(seconds=self.settings.worker_dead_after_seconds)
        rows = await db.scalars(select(Worker).where(Worker.status == "live"))
        return [w for w in rows if (_aware(w.last_seen_at) or cutoff) >= cutoff]

    async def sweep_workers(self, db: AsyncSession) -> int:
        """Mark silent workers dead and fail the calls they were holding (carrier legs hung up)."""
        from datetime import timedelta
        cutoff = utcnow() - timedelta(seconds=self.settings.worker_dead_after_seconds)
        rows = await db.scalars(select(Worker).where(Worker.status == "live"))
        failed = 0
        for w in list(rows):
            if (_aware(w.last_seen_at) or cutoff) >= cutoff:
                continue
            w.status = "dead"
            await db.commit()
            logger.warning("worker %s (%s) is dead: no heartbeat for %.0fs", w.worker_id, w.worker_url,
                           self.settings.worker_dead_after_seconds)
            calls = await db.scalars(select(Call).where(Call.status == CallStatus.in_progress.value)
                                     .where((Call.worker_id == w.worker_id) | (Call.worker_url == w.worker_url)))
            for call in list(calls):
                await self.finalize(db, call, status=CallStatus.failed, reason="worker_lost",
                                    error="worker heartbeat lost")
                await self._hangup_carrier_leg(call)
                failed += 1
        return failed

    async def drain_worker(self, db: AsyncSession, worker_id: str) -> bool:
        w = await db.get(Worker, worker_id)
        if w is None:
            raise ServiceError(404, f"worker '{worker_id}' not found")
        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.post(f"{w.worker_url.rstrip('/')}/internal/drain",
                                      headers={"X-Internal-Token": self.settings.internal_token})
                ok = r.status_code == 200
        except Exception as e:
            logger.warning("drain request to %s failed: %s", w.worker_url, e)
            ok = False
        w.draining = True
        await db.commit()
        return ok

    async def _hangup_carrier_leg(self, call: Call) -> None:
        if not call.provider_call_id or call.direction == Direction.browser.value:
            return
        try:
            await self.carriers.get(call.provider).hangup(call.provider_call_id)
        except Exception as e:
            logger.warning("carrier hangup for %s failed: %s", call.id, e)

    # ---------------------------------------------------------------- urls
    def _callback_urls(self, provider: str, call_id: str) -> dict[str, str]:
        base = self.settings.public_http_url.rstrip("/")
        q = f"?call_id={quote(call_id)}"
        return {
            "answer_url": f"{base}/carrier/{provider}/answer{q}",
            "ring_url": f"{base}/carrier/{provider}/ring{q}",
            "hangup_url": f"{base}/carrier/{provider}/hangup{q}",
        }

    def stream_url(self, call_id: str) -> str:
        base = self.settings.public_ws_url.rstrip("/")
        return f"{base}/stream/{quote(call_id)}" if self.settings.stream_path_per_call else base

    def recording_url(self, call_id: str) -> str:
        return f"{self.settings.public_http_url.rstrip('/')}/v1/calls/{call_id}/recording"

    # ---------------------------------------------------------------- create outbound
    async def create_call(self, db: AsyncSession, req: CallCreate) -> Call:
        cfg = await self.resolve_agent(db, req.agent)
        carrier = self.carriers.get(req.provider)
        if not carrier.configured():
            raise ServiceError(503, f"carrier '{carrier.name}' is not configured on this engine")
        await self._check_capacity(db, carrier.name)

        call = Call(
            id=new_call_id(), status=CallStatus.queued.value, direction=Direction.outbound.value,
            provider=carrier.name, to_number=req.to, from_number=req.from_number or carrier.from_number,
            agent_id=cfg.id, agent_version=cfg.version, agent_config=cfg.model_dump(mode="json"),
            variables=req.variables, call_metadata=req.metadata, webhook_url=req.webhook_url,
            webhook_secret=req.webhook_secret or self.settings.webhook_secret,
            record=(req.record if req.record is not None else (cfg.record if cfg.record is not None
                                                                else self.settings.record_calls)),
            max_duration_s=req.max_duration_s or cfg.max_duration_s or self.settings.call_max_seconds,
        )
        db.add(call)
        await db.commit()
        await self._emit(db, call, EventType.call_queued, {"to": call.to_number, "provider": call.provider})

        urls = self._callback_urls(carrier.name, call.id)
        try:
            result = await carrier.dial(call.to_number, call.from_number or "", stream_url=self.stream_url(call.id),
                                        **urls)
            call.request_uuid = result.provider_call_id
            call.provider_call_id = result.provider_call_id
            call.status = CallStatus.dialing.value
            await db.commit()
        except CarrierError as e:
            logger.error("dial failed for %s: %s", call.id, e)
            await self.fail_call(db, call, reason="dial_failed", error=str(e))
            raise ServiceError(502, f"dial failed: {e}")
        return call

    async def _check_capacity(self, db: AsyncSession, provider: str) -> None:
        retry = {"Retry-After": "1"}
        free, _total = await self.engine_slots(db)
        if free <= 0:
            raise ServiceError(429, {"reason": "engine_full", "detail": "no free conversation slot; see GET /v1/capacity"},
                               headers=retry)
        pool = await self.carrier_pool(db, provider)
        if pool.free <= 0:
            raise ServiceError(429, {"reason": "carrier_full", "detail": f"{provider} concurrent-call limit reached"},
                               headers=retry)
        if not self.cps.try_acquire(provider):
            raise ServiceError(429, {"reason": "cps_exceeded", "detail": f"{provider} dials-per-second budget exhausted"},
                               headers=retry)

    # ---------------------------------------------------------------- inbound
    async def _lookup_number(self, db: AsyncSession, to_number: str) -> Optional[PhoneNumber]:
        return await db.get(PhoneNumber, to_number) or await db.get(PhoneNumber, "+" + to_number.lstrip("+"))

    async def refuse_inbound(self, db: AsyncSession, provider: str, info: CallbackInfo) -> Call:
        """No free engine slot: record the refusal as a failed inbound call (webhook if the number has one)."""
        number = await self._lookup_number(db, info.to_number)
        call = Call(
            id=new_call_id(), status=CallStatus.queued.value, direction=Direction.inbound.value, provider=provider,
            to_number=info.to_number, from_number=info.from_number, provider_call_id=info.provider_call_id or None,
            agent_id=number.agent_id if number else None, agent_version=number.agent_version if number else None,
            call_metadata={"inbound": True}, webhook_url=number.webhook_url if number else None,
            webhook_secret=(number.webhook_secret if number else None) or self.settings.webhook_secret,
            record=False, max_duration_s=self.settings.call_max_seconds,
        )
        db.add(call)
        await db.commit()
        await self.finalize(db, call, status=CallStatus.failed, reason="capacity",
                            error="no free conversation slot for an inbound call")
        return call

    async def create_inbound_call(self, db: AsyncSession, provider: str, info: CallbackInfo) -> Optional[Call]:
        number = await self._lookup_number(db, info.to_number)
        if number is None or not number.agent_id:
            logger.warning("inbound call to unknown/unrouted number %s", info.to_number)
            return None
        row = await self.get_agent(db, number.agent_id, number.agent_version)
        cfg = AgentConfig.model_validate(row.config)
        variables: dict[str, Any] = {"caller": info.from_number, "called": info.to_number}
        if number.route_url:
            variables.update(await self._fetch_route(number.route_url, info))
        call = Call(
            id=new_call_id(), status=CallStatus.in_progress.value, direction=Direction.inbound.value,
            provider=provider, to_number=info.to_number, from_number=info.from_number,
            provider_call_id=info.provider_call_id or None, agent_id=cfg.id, agent_version=cfg.version,
            agent_config=cfg.model_dump(mode="json"), variables=variables, call_metadata={"inbound": True},
            webhook_url=number.webhook_url, webhook_secret=number.webhook_secret or self.settings.webhook_secret,
            record=(cfg.record if cfg.record is not None else self.settings.record_calls),
            max_duration_s=cfg.max_duration_s or self.settings.call_max_seconds, answered_at=utcnow(),
        )
        db.add(call)
        await db.commit()
        await self._emit(db, call, EventType.call_answered, {"direction": "inbound", "from": info.from_number,
                                                          "to": info.to_number})
        return call

    async def _fetch_route(self, route_url: str, info: CallbackInfo) -> dict[str, Any]:
        """Ask the platform for per-call context (missed-call history, CRM fields) — optional."""
        try:
            import httpx
            async with httpx.AsyncClient(timeout=3.0) as client:
                r = await client.get(route_url, params={"to": info.to_number, "from": info.from_number},
                                     headers={"X-Internal-Token": self.settings.internal_token})
                if r.status_code == 200 and r.headers.get("content-type", "").startswith("application/json"):
                    data = r.json()
                    return data.get("variables", data) if isinstance(data, dict) else {}
        except Exception as e:  # never block the call on the platform
            logger.warning("route_url %s failed: %s", route_url, e)
        return {}

    # ---------------------------------------------------------------- carrier callbacks
    async def on_answer(self, db: AsyncSession, call: Call, info: CallbackInfo) -> None:
        if info.provider_call_id:
            call.provider_call_id = info.provider_call_id
        if call.status in (CallStatus.queued.value, CallStatus.dialing.value, CallStatus.ringing.value):
            call.status = CallStatus.in_progress.value
            call.answered_at = call.answered_at or utcnow()
            await db.commit()
            await self._emit(db, call, EventType.call_answered, {"from": info.from_number or call.from_number,
                                                              "to": info.to_number or call.to_number})

    async def on_ring(self, db: AsyncSession, call: Call, info: CallbackInfo) -> None:
        if info.provider_call_id:
            call.provider_call_id = info.provider_call_id
        if call.status in (CallStatus.queued.value, CallStatus.dialing.value):
            call.status = CallStatus.ringing.value
            call.ringing_at = utcnow()
            await db.commit()
            await self._emit(db, call, EventType.call_ringing, {})

    async def on_hangup(self, db: AsyncSession, call: Call, info: CallbackInfo) -> None:
        if info.provider_call_id:
            call.provider_call_id = info.provider_call_id
        call.hangup_cause = info.hangup_cause or call.hangup_cause
        if call.status in [s.value for s in TERMINAL_STATUSES]:
            await db.commit()
            return
        was_answered = call.answered_at is not None or call.status == CallStatus.in_progress.value
        carrier = self.carriers.get(call.provider)
        status = carrier.final_status(info, was_answered)
        if status == CallStatus.completed and not call.ended_emitted and call.worker_url:
            # the worker will report the rich `ended` event shortly; remember the carrier's view
            call.end_reason = call.end_reason or "carrier_hangup"
            call.ended_at = call.ended_at or utcnow()
            call.duration_s = info.duration_s or call.duration_s
            call.status = CallStatus.completed.value
            await db.commit()
            return
        await self.finalize(db, call, status=status, reason=call.end_reason or "carrier_hangup",
                            duration_s=info.duration_s)

    # ---------------------------------------------------------------- worker events
    async def on_worker_event(self, db: AsyncSession, call: Call, etype: str, data: dict[str, Any],
                              seq: Optional[int]) -> None:
        if seq is not None:
            if seq <= call.last_seq:
                return  # duplicate / out of order replay
            call.last_seq = seq
        if etype == "started":
            call.worker_url = data.get("worker_url") or call.worker_url
            call.worker_id = data.get("worker_id") or call.worker_id
            if call.status != CallStatus.in_progress.value and call.status not in [s.value for s in TERMINAL_STATUSES]:
                call.status = CallStatus.in_progress.value
                call.answered_at = call.answered_at or utcnow()
            await db.commit()
            return
        if etype == "refused":
            # the worker would not take the stream (at its cap or draining): fail the call, drop the carrier leg
            reason = str(data.get("reason") or "refused")
            await self.finalize(db, call, status=CallStatus.failed, reason=reason,
                                error=f"worker {data.get('worker_id') or ''} refused the stream ({reason})")
            await self._hangup_carrier_leg(call)
            return
        if etype == "turn":
            turns = list(call.transcript or [])
            turns.append(data)
            call.transcript = turns
            await db.commit()
            await self._emit(db, call, EventType.call_turn, data)
            return
        if etype == "tool":
            if data.get("record_outcome"):
                outcome = dict(call.outcome or {})
                outcome.update(data.get("result") or {})
                call.outcome = outcome
            await db.commit()
            await self._emit(db, call, EventType.call_tool, data)
            return
        if etype == "transition":
            await self._emit(db, call, EventType.call_transition, data)
            return
        if etype == "metrics":
            events = list((call.metrics or {}).get("provider_events") or [])
            call.metrics = {**data, "provider_events": events + list(data.get("provider_events") or [])} \
                if events and not data.get("provider_events") else data
            await db.commit()
            return
        if etype == "provider":
            metrics = dict(call.metrics or {})
            metrics["provider_events"] = list(metrics.get("provider_events") or []) + [data]
            call.metrics = metrics
            await db.commit()
            return
        if etype == "ended":
            if data.get("transcript") is not None:
                call.transcript = data["transcript"]
            if data.get("metrics") is not None:
                call.metrics = data["metrics"]
            if data.get("outcome"):
                outcome = dict(call.outcome or {})
                outcome.update(data["outcome"])
                call.outcome = outcome
            call.recording_path = data.get("recording_path") or call.recording_path
            call.cost_usd = data.get("cost_usd", call.cost_usd)
            await db.commit()   # keep the worker's transcript/metrics even when the call is already terminal
            reason = data.get("reason") or "agent_ended"
            # a call that ended normally stays "completed" even if the worker logged recoverable provider
            # errors along the way (they are kept in `error` for inspection); only an error-driven close fails it
            status = CallStatus.failed if (data.get("error") and reason.startswith("session_error")) else CallStatus.completed
            await self.finalize(db, call, status=status, reason=reason, error=data.get("error"),
                                duration_s=data.get("duration_s"))
            # the worker asks us to hang up the carrier leg when the agent ended the call
            if data.get("hangup_carrier") and call.provider_call_id and call.direction != Direction.browser.value:
                try:
                    await self.carriers.get(call.provider).hangup(call.provider_call_id)
                except Exception as e:
                    logger.warning("carrier hangup after agent end failed for %s: %s", call.id, e)
            return
        logger.debug("unknown worker event %s for %s", etype, call.id)

    # ---------------------------------------------------------------- housekeeping
    async def sweep_stale(self, db: AsyncSession, grace_s: int = 120) -> int:
        """Finalize calls that never received a terminal event (worker restart, lost stream): anything still
        active beyond its own max duration + grace. Keeps capacity accounting honest."""
        from datetime import timedelta
        now = utcnow()
        rows = await db.scalars(select(Call).where(Call.status.in_(ACTIVE_STATUSES)))
        n = 0
        for call in list(rows):
            created = _aware(call.created_at)
            if now - created > timedelta(seconds=(call.max_duration_s or self.settings.call_max_seconds) + grace_s):
                await self.finalize(db, call, status=CallStatus.failed, reason="stale_timeout",
                                    error="no terminal event from the worker within max_duration + grace")
                n += 1
        return n

    # ---------------------------------------------------------------- terminal
    async def fail_call(self, db: AsyncSession, call: Call, *, reason: str, error: str) -> None:
        await self.finalize(db, call, status=CallStatus.failed, reason=reason, error=error)

    async def finalize(self, db: AsyncSession, call: Call, *, status: CallStatus, reason: str,
                       error: Optional[str] = None, duration_s: Optional[float] = None) -> None:
        if call.ended_emitted:
            return
        call.status = status.value
        call.end_reason = reason
        call.error = error or call.error
        call.ended_at = call.ended_at or utcnow()
        if duration_s is not None:
            call.duration_s = duration_s
        elif call.answered_at:
            call.duration_s = max(0.0, (_aware(call.ended_at) - _aware(call.answered_at)).total_seconds())
        call.ended_emitted = True
        await db.commit()
        record = self.to_record(call)
        payload = record.model_dump(mode="json", by_alias=True)
        payload["recording_url"] = record.recording_url
        etype = EventType.call_failed if status == CallStatus.failed else EventType.call_ended
        await self._emit(db, call, etype, payload)

    async def hangup(self, db: AsyncSession, call: Call, reason: str = "api_hangup") -> None:
        if call.status in [s.value for s in TERMINAL_STATUSES]:
            return
        carrier_ok = False
        if call.provider_call_id and call.direction != Direction.browser.value:
            try:
                carrier_ok = await self.carriers.get(call.provider).hangup(call.provider_call_id)
            except Exception as e:
                logger.warning("carrier hangup failed for %s: %s", call.id, e)
        if call.status != CallStatus.in_progress.value or not carrier_ok:
            await self.finalize(db, call, status=CallStatus.cancelled if call.status != CallStatus.in_progress.value
                                else CallStatus.completed, reason=reason)

    # ---------------------------------------------------------------- views
    def to_record(self, call: Call) -> CallRecord:
        metrics = CallMetrics.model_validate(call.metrics or {}) if call.metrics else CallMetrics()
        return CallRecord(
            id=call.id, status=CallStatus(call.status), direction=Direction(call.direction), provider=call.provider,
            to=call.to_number, from_number=call.from_number, provider_call_id=call.provider_call_id,
            agent_id=call.agent_id, agent_version=call.agent_version, created_at=iso(call.created_at) or "",
            answered_at=iso(call.answered_at), ended_at=iso(call.ended_at), duration_s=call.duration_s,
            end_reason=call.end_reason, hangup_cause=call.hangup_cause, outcome=call.outcome or {},
            variables=call.variables or {}, metadata=call.call_metadata or {},
            transcript=[TranscriptTurn.model_validate(t) for t in (call.transcript or [])], metrics=metrics,
            recording_url=(self.recording_url(call.id) if call.recording_path else None), cost_usd=call.cost_usd,
            error=call.error,
        )

    def runtime_payload(self, call: Call) -> dict[str, Any]:
        """What the worker needs to run this call."""
        return {
            "call_id": call.id,
            "direction": call.direction,
            "provider": call.provider,
            "to": call.to_number,
            "from": call.from_number,
            "agent": call.agent_config,
            "variables": call.variables or {},
            "metadata": call.call_metadata or {},
            "record": bool(call.record),
            "max_duration_s": call.max_duration_s,
            "status": call.status,
        }

    async def _emit(self, db: AsyncSession, call: Call, etype: str, data: dict[str, Any]) -> None:
        if not call.webhook_url:
            return
        await self.dispatcher.enqueue(db, call, etype, data, agent_id=call.agent_id,
                                      agent_version=call.agent_version, metadata=call.call_metadata or {})


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
