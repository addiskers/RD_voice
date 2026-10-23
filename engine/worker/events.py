"""CallReporter: AgentSession events -> transcript + per-turn metrics -> engine API (ordered, seq'd)."""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

import httpx
from livekit.agents import AgentSession, llm as lk_llm

from engine.config import Settings
from engine.schemas.calls import CallMetrics, TranscriptTurn, TurnMetrics
from engine.worker.spool import EventSpool

logger = logging.getLogger(__name__)


class CallReporter:
    def __init__(self, settings: Settings, http: httpx.AsyncClient, call_id: str, *, worker_url: str = "",
                 spool: Optional[EventSpool] = None, worker_id: str = ""):
        self.settings = settings
        self.http = http
        self.call_id = call_id
        self.worker_url = worker_url
        self.worker_id = worker_id
        self.spool = spool          # None = in-process observers only (tests); the worker always passes one
        self.started_at = time.monotonic()
        self.seq = 0
        self.transcript: list[TranscriptTurn] = []
        self.metrics = CallMetrics()
        self._turn_index = 0
        self._current: Optional[TurnMetrics] = None
        self._first_agent_speech_at: Optional[float] = None
        self._last_user_stop: Optional[float] = None
        self._pending_user: dict[str, Any] = {}
        # in-process observers (e.g. the browser transport pushing live metrics to the page): fn(type, data)
        self.listeners: list[Any] = []
        self._closed = False
        self.node_id: Optional[str] = None
        self.errors: list[str] = []
        self._realtime = False
        self._session: Optional[AgentSession] = None
        self.current_providers: dict[str, str] = {}
        self.stt_failed = False          # the turn-driving STT (Flux) died: the call cannot commit turns
        # what each vendor will bill, keyed by (stage, provider, model): seconds, tokens, characters
        self._usage: dict[tuple[str, str, str], dict[str, Any]] = {}

    # ------------------------------------------------------------------ billable usage
    def _usage_key(self, kind: str, m: Any) -> tuple[str, str, str]:
        from engine.worker.pipeline import provider_id
        label = str(getattr(m, "label", "") or "")
        provider = self.current_providers.get(kind) or provider_id(type("L", (), {"label": label})())
        configured = (self.metrics.models.get(kind) or "").split(" ", 1)
        model = str(getattr(m, "model", "") or "")
        if not model and len(configured) == 2 and configured[0] == provider:
            model = configured[1]
        if not model and len(configured) == 2:
            model = configured[1] if provider == configured[0] else ""
        return kind, provider, model or "?"

    def record_usage(self, m: Any) -> None:
        """Called for every LiveKit metrics event; sums the billable quantities per stage, vendor and model."""
        mtype = getattr(m, "type", "")
        if mtype == "llm_metrics":
            key = self._usage_key("llm", m)
            u = self._usage.setdefault(key, {"requests": 0, "input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0,
                                             "thinking_tokens": 0})
            prompt = int(getattr(m, "prompt_tokens", 0) or 0)
            completion = int(getattr(m, "completion_tokens", 0) or 0)
            total = int(getattr(m, "total_tokens", 0) or 0)
            # Gemini reports thinking tokens only inside the total (billed as output, excluded from
            # completion_tokens); OpenAI-style providers break them out but already count them in completion.
            hidden = max(0, total - prompt - completion) if total else 0
            u["requests"] += 1
            u["input_tokens"] += prompt
            u["cached_input_tokens"] += int(getattr(m, "prompt_cached_tokens", 0) or 0)
            u["output_tokens"] += completion + hidden
            u["thinking_tokens"] += hidden or int(getattr(m, "reasoning_tokens", 0) or 0)
        elif mtype == "tts_metrics":
            key = self._usage_key("tts", m)
            u = self._usage.setdefault(key, {"requests": 0, "characters": 0, "cancelled_characters": 0, "audio_seconds": 0.0})
            chars = int(getattr(m, "characters_count", 0) or 0)
            u["requests"] += 1
            u["characters"] += chars                       # vendors bill the request even when playout is cancelled
            if getattr(m, "cancelled", False):
                u["cancelled_characters"] += chars
            else:
                u["audio_seconds"] = round(u["audio_seconds"] + float(getattr(m, "audio_duration", 0.0) or 0.0), 3)
        elif mtype == "stt_metrics":
            key = self._usage_key("stt", m)
            u = self._usage.setdefault(key, {"recognized_seconds": 0.0})
            u["recognized_seconds"] = round(u["recognized_seconds"] + float(getattr(m, "audio_duration", 0.0) or 0.0), 3)
        elif mtype == "realtime_model_metrics":
            key = self._usage_key("llm", m)
            key = ("realtime", key[1], key[2])
            u = self._usage.setdefault(key, {"requests": 0, "audio_input_tokens": 0, "text_input_tokens": 0,
                                             "cached_input_tokens": 0, "audio_output_tokens": 0, "text_output_tokens": 0})
            inp, out = getattr(m, "input_token_details", None), getattr(m, "output_token_details", None)
            u["requests"] += 1
            u["audio_input_tokens"] += int(getattr(inp, "audio_tokens", 0) or 0)
            u["text_input_tokens"] += int(getattr(inp, "text_tokens", 0) or 0)
            u["cached_input_tokens"] += int(getattr(inp, "cached_tokens", 0) or 0)
            u["audio_output_tokens"] += int(getattr(out, "audio_tokens", 0) or 0)
            u["text_output_tokens"] += int(getattr(out, "text_tokens", 0) or 0)

    def usage_detail(self, call_seconds: Optional[float] = None) -> dict[str, list[dict[str, Any]]]:
        """Billable quantities per stage. STT is billed on the audio streamed for the whole call (silence
        included), so its seconds are the call duration when known, with the recognized seconds alongside."""
        out: dict[str, list[dict[str, Any]]] = {}
        seen_stt = False
        for (kind, provider, model), u in self._usage.items():
            entry = {"provider": provider, "model": model, **u}
            if kind == "stt":
                seen_stt = True
                entry = {"provider": provider, "model": model,
                         "seconds": round(call_seconds if call_seconds is not None else u["recognized_seconds"], 3),
                         "recognized_seconds": u["recognized_seconds"]}
            out.setdefault(kind, []).append(entry)
        if not seen_stt and call_seconds is not None and not self._realtime and self.metrics.models.get("stt"):
            provider, _, model = self.metrics.models["stt"].partition(" ")
            out["stt"] = [{"provider": provider, "model": model, "seconds": round(call_seconds, 3), "recognized_seconds": 0.0}]
        return out

    def _new_turn(self, **kwargs: Any) -> TurnMetrics:
        """A TurnMetrics stamped with the vendors serving each stage right now (so failovers are visible)."""
        if self._session is not None:
            try:
                from engine.worker.pipeline import provider_labels
                self.current_providers = provider_labels(self._session)
            except Exception:
                pass
        p = self.current_providers
        previous = self.metrics.turns[-1].node if self.metrics.turns else None
        transition = bool(self.metrics.turns) and self.node_id != previous
        return TurnMetrics(stt_provider=p.get("stt"), llm_provider=p.get("llm"), tts_provider=p.get("tts"),
                           node=self.node_id, transition=transition, **kwargs)

    # ------------------------------------------------------------------ transport
    def start(self) -> None:
        if self.spool is not None:
            self.spool.start()

    def post(self, etype: str, data: dict[str, Any]) -> None:
        if self._closed:
            return
        self.seq += 1
        if self.spool is not None:
            self.spool.append({"type": etype, "seq": self.seq, "data": data})
        for fn in list(self.listeners):
            try:
                fn(etype, data)
            except Exception as e:  # observers must never affect the call
                logger.debug("reporter listener failed: %s", e)

    async def close(self) -> None:
        """No more events will be posted. Delivery continues in the spool until the API has acknowledged
        everything (a draining worker waits for that before it exits)."""
        if self._closed:
            return
        self._closed = True
        if self.spool is not None:
            self.spool.mark_ended()

    # ------------------------------------------------------------------ helpers
    def at(self) -> float:
        return round(time.monotonic() - self.started_at, 3)

    def snapshot(self) -> dict[str, Any]:
        self.metrics.recompute()
        return {"transcript": [t.model_dump(mode="json") for t in self.transcript],
                "metrics": self.metrics.model_dump(mode="json")}

    async def report_tool(self, name: str, data: dict[str, Any]) -> None:
        self.post("tool", {"name": name, "at": self.at(), "node": self.node_id, **data})

    async def report_transition(self, data: dict[str, Any]) -> None:
        self.node_id = data.get("to")
        self.post("transition", {"at": self.at(), **data})

    # ------------------------------------------------------------------ session wiring
    def attach(self, session: AgentSession) -> None:
        self._session = session
        try:
            self._realtime = isinstance(session.llm, lk_llm.RealtimeModel)
        except Exception:
            self._realtime = False
        try:
            from engine.worker.pipeline import model_labels
            self.metrics.models = model_labels(session)
        except Exception as e:
            logger.debug("model labels unavailable: %s", e)
        self._subscribe_availability(session)

        @session.on("conversation_item_added")
        def _item(ev):
            item = ev.item
            if isinstance(item, lk_llm.ChatMessage) and item.role in ("user", "assistant"):
                text = item.text_content or ""
                if not text.strip():
                    return
                rep = dict(getattr(item, "metrics", None) or {})
                if item.role == "user":
                    # wall-clock end of the caller's speech: the anchor for user-perceived latency
                    stop = rep.get("stopped_speaking_at")
                    if stop:
                        self._last_user_stop = float(stop)
                    self._pending_user = {"eou": rep.get("end_of_turn_delay"), "stt": rep.get("transcription_delay")}
                else:
                    ttft = rep.get("llm_node_ttft")
                    target = self._current or (self.metrics.turns[-1] if self.metrics.turns else None)
                    if ttft is not None and target is not None and target.llm_ttft is None:
                        target.llm_ttft = round(float(ttft), 3)
                        self.post("metrics", self.metrics.recompute().model_dump(mode="json"))
                turn = TranscriptTurn(role="user" if item.role == "user" else "agent", text=text, at=self.at(),
                                      node=self.node_id, interrupted=bool(item.interrupted),
                                      extra={k: round(float(v), 3) for k, v in rep.items() if isinstance(v, (int, float))
                                             and k in ("end_of_turn_delay", "transcription_delay", "llm_node_ttft",
                                                       "llm_node_ttfs")})
                self.transcript.append(turn)
                self.post("turn", turn.model_dump(mode="json"))

        @session.on("metrics_collected")
        def _metrics(ev):
            m = ev.metrics
            mtype = getattr(m, "type", "")
            try:
                self.record_usage(m)
            except Exception as e:
                logger.debug("usage accounting error: %s", e)
            try:
                if mtype == "eou_metrics":
                    self._turn_index += 1
                    self._current = self._new_turn(turn=self._turn_index, at=self.at(),
                                                   eou_delay=round(m.end_of_utterance_delay, 3),
                                                   stt_delay=round(m.transcription_delay, 3))
                    self.metrics.turns.append(self._current)
                elif mtype == "llm_metrics":
                    target = self._current or (self.metrics.turns[-1] if self.metrics.turns else None)
                    if target is None:
                        self._turn_index += 1
                        target = self._current = self._new_turn(turn=self._turn_index, at=self.at())
                        self.metrics.turns.append(target)
                    if target.llm_ttft is None and m.ttft is not None and m.ttft >= 0:
                        target.llm_ttft = round(m.ttft, 3)   # -1 = preemptive generation, not measured
                elif mtype == "tts_metrics":
                    target = self._current or (self.metrics.turns[-1] if self.metrics.turns else None)
                    if target is not None and target.tts_ttfb is None:
                        target.tts_ttfb = round(m.ttfb, 3)
                        self.post("metrics", self.metrics.recompute().model_dump(mode="json"))
                elif mtype == "realtime_model_metrics":
                    # speech-to-speech: the model's own ttft is ~0 (audio arrives with the response); the
                    # user-perceived total is measured from end-of-speech detection to agent audio instead
                    target = self._current or (self.metrics.turns[-1] if self.metrics.turns else None)
                    if target is not None and target.llm_ttft is None and m.ttft is not None and m.ttft > 0:
                        target.llm_ttft = round(m.ttft, 3)
            except Exception as e:
                logger.debug("metrics handling error: %s", e)

        @session.on("user_state_changed")
        def _user_state(ev):
            # end-of-speech detection (Silero VAD, or the realtime model's server VAD): the anchor for
            # speech-to-speech turns, where no STT end-of-turn metrics exist
            if ev.new_state == "listening" and ev.old_state == "speaking" and self._realtime:
                self._last_user_stop = float(getattr(ev, "created_at", time.time()))
                self._turn_index += 1
                self._current = self._new_turn(turn=self._turn_index, at=self.at())
                self.metrics.turns.append(self._current)

        @session.on("session_usage_updated")
        def _usage(ev):
            # LiveKit's own running totals (a dataclass in 1.8): kept beside usage_detail as a cross-check
            try:
                import dataclasses
                u = ev.usage
                self.metrics.usage = (u.model_dump(mode="json") if hasattr(u, "model_dump")
                                      else dataclasses.asdict(u) if dataclasses.is_dataclass(u) else {})
            except Exception:
                pass

        @session.on("agent_state_changed")
        def _agent_state(ev):
            if ev.new_state != "speaking":
                return
            if self._first_agent_speech_at is None:
                self._first_agent_speech_at = self.at()
                self.metrics.pickup_to_first_word = self._first_agent_speech_at
            # user-perceived response time: end of the caller's speech -> agent audio starts playing
            if self._last_user_stop and self._current is not None and self._current.total is None:
                total = float(ev.created_at) - self._last_user_stop
                if 0 < total < 30:
                    self._current.total = round(total, 3)
                self._last_user_stop = None
                self.post("metrics", self.metrics.recompute().model_dump(mode="json"))
                self._current = None

        @session.on("error")
        def _error(ev):
            try:
                self.errors.append(f"{type(ev.error).__name__}: {getattr(ev.error, 'message', ev.error)}")
            except Exception:
                self.errors.append(str(ev))
            logger.error("session error on %s: %s", self.call_id, self.errors[-1])
            # local listeners only (browser console): the API learns about errors from the 'ended' event
            source = getattr(ev, "source", None)
            try:
                from livekit.plugins import deepgram
                if isinstance(source, deepgram.STTv2) and not getattr(ev, "recoverable", False):
                    from engine.worker.breaker import BREAKER, FLUX_KEY
                    BREAKER.record_failure(FLUX_KEY)
                    self.stt_failed = True
            except Exception:
                pass
            payload = {"message": self.errors[-1][:300], "source": type(source).__name__ if source else "",
                       "recoverable": bool(getattr(ev, "recoverable", False))}
            for fn in list(self.listeners):
                try:
                    fn("error", payload)
                except Exception:
                    pass

    def _subscribe_availability(self, session: AgentSession) -> None:
        """FallbackAdapters emit availability changes; report them so the record shows who served the call."""
        for kind in ("stt", "llm", "tts"):
            comp = getattr(session, kind, None)
            if comp is None or not hasattr(comp, "_active_instance"):
                continue

            def _handler(ev, kind=kind):
                from engine.worker.pipeline import provider_id
                provider = provider_id(getattr(ev, kind, None))
                available = bool(getattr(ev, "available", False))
                logger.warning("%s provider %s is now %s on %s", kind, provider,
                               "available" if available else "UNAVAILABLE (failing over)", self.call_id)
                self.post("provider", {"kind": kind, "provider": provider, "available": available, "at": self.at()})

            try:
                comp.on(f"{kind}_availability_changed", _handler)
            except Exception as e:
                logger.debug("cannot subscribe to %s availability: %s", kind, e)

    def started(self) -> None:
        self.post("started", {"worker_url": self.worker_url, "worker_id": self.worker_id, "at": self.at()})

    def ended(self, reason: str, *, recording_path: Optional[str] = None, outcome: Optional[dict[str, Any]] = None,
              hangup_carrier: bool = False, error: Optional[str] = None) -> None:
        snap = self.snapshot()
        duration = self.at()
        self.post("ended", {"reason": reason, "duration_s": duration, "recording_path": recording_path,
                            "outcome": outcome or {}, "hangup_carrier": hangup_carrier, "error": error,
                            "errors": self.errors, "usage_detail": self.usage_detail(call_seconds=duration), **snap})
