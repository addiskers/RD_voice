"""Carrier adapter interface.

A carrier gives us: REST dial + hangup, an answer webhook that expects XML telling it to open a
bidirectional <Stream> WebSocket to the worker, and ring/hangup callbacks. Plivo is the
reference protocol; Vobiz is Plivo-compatible; EnableX passes the stream URL at dial time.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional
from xml.sax.saxutils import escape, quoteattr

import httpx

from engine.config import Settings
from engine.schemas.calls import CallStatus


class CarrierError(RuntimeError):
    pass


@dataclass
class DialResult:
    provider_call_id: Optional[str]
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class CallbackInfo:
    """Normalised view of an answer / ring / hangup callback."""
    event: str = ""                      # answer | ring | hangup | other
    provider_call_id: str = ""           # CallUUID
    request_uuid: str = ""               # RequestUUID (outbound dial handle)
    from_number: str = ""
    to_number: str = ""
    direction: str = ""                  # inbound | outbound
    call_status: str = ""                # ringing | in-progress | completed | busy | no-answer | failed ...
    answered: Optional[bool] = None      # True if the carrier reports an answer time
    hangup_cause: str = ""
    hangup_code: Optional[int] = None
    hangup_source: str = ""
    duration_s: Optional[float] = None
    raw: dict[str, Any] = field(default_factory=dict)


def _first(params: dict[str, Any], *keys: str, default: str = "") -> str:
    for k in keys:
        v = params.get(k)
        if v not in (None, ""):
            return str(v)
    return default


class Carrier(ABC):
    name: str = "base"

    def __init__(self, settings: Settings, http: Optional[httpx.AsyncClient] = None):
        self.settings = settings
        self._http = http

    @property
    def http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=20.0)
        return self._http

    # --- capabilities --------------------------------------------------------------------
    @property
    @abstractmethod
    def from_number(self) -> str: ...

    @abstractmethod
    def configured(self) -> bool: ...

    @abstractmethod
    async def dial(self, to: str, from_number: str, *, answer_url: str, ring_url: str, hangup_url: str,
                   stream_url: str, extra: Optional[dict[str, Any]] = None) -> DialResult: ...

    @abstractmethod
    async def hangup(self, provider_call_id: str) -> bool: ...

    # --- callback authentication -----------------------------------------------------------
    def verify_callback(self, *, method: str, public_url: str, headers: Mapping[str, str],
                        params: dict[str, Any], remote_ip: str) -> bool:
        """True when a carrier callback is genuine. ``public_url`` is the URL the carrier called (built from
        PUBLIC_HTTP_URL), ``params`` the POST body fields (empty for GET). Base: accept everything."""
        return True

    # --- protocol defaults (Plivo-style) ---------------------------------------------------
    def answer_xml(self, stream_url: str, extra_headers: Optional[dict[str, str]] = None) -> str:
        """XML that makes the carrier open the bidirectional media WebSocket to ``stream_url``."""
        eh = ""
        if extra_headers:
            pairs = ",".join(f"{k}={v}" for k, v in extra_headers.items())
            eh = f" extraHeaders={quoteattr(pairs)}"
        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            "<Response>\n"
            '  <Stream bidirectional="true" keepCallAlive="true" '
            f'contentType="audio/x-mulaw;rate=8000" audioTrack="inbound"{eh}>{escape(stream_url)}</Stream>\n'
            "</Response>"
        )

    def hangup_xml(self) -> str:
        return '<?xml version="1.0" encoding="UTF-8"?>\n<Response>\n  <Hangup/>\n</Response>'

    def busy_xml(self, text: str = "All our lines are busy right now. Please try again in a few minutes.") -> str:
        """Spoken refusal for an inbound call when the engine has no free slot."""
        return ('<?xml version="1.0" encoding="UTF-8"?>\n<Response>\n'
                f"  <Speak>{escape(text)}</Speak>\n  <Hangup/>\n</Response>")

    def parse_callback(self, params: dict[str, Any], event: str) -> CallbackInfo:
        answer_time = _first(params, "AnswerTime", "answer_time")
        code = _first(params, "HangupCauseCode", "hangup_cause_code")
        duration = _first(params, "Duration", "duration", "BillDuration")
        info = CallbackInfo(
            event=event,
            provider_call_id=_first(params, "CallUUID", "call_uuid", "callUUID", "CallSid"),
            request_uuid=_first(params, "RequestUUID", "request_uuid"),
            from_number=_first(params, "From", "from"),
            to_number=_first(params, "To", "to"),
            direction=_first(params, "Direction", "direction").lower(),
            call_status=_first(params, "CallStatus", "call_status", "Status").lower(),
            answered=(bool(answer_time) if (answer_time or event == "hangup") else None),
            hangup_cause=_first(params, "HangupCauseName", "HangupCause", "hangup_cause", "hangup_cause_name"),
            hangup_code=int(code) if code.isdigit() else None,
            hangup_source=_first(params, "HangupSource", "hangup_source"),
            duration_s=float(duration) if duration.replace(".", "", 1).isdigit() else None,
            raw=dict(params),
        )
        if event == "answer" and info.answered is None:
            info.answered = True
        return info

    def final_status(self, info: CallbackInfo, was_answered: bool) -> CallStatus:
        """Map a hangup callback to the call's terminal status."""
        if was_answered or info.answered:
            return CallStatus.completed
        cause = (info.hangup_cause or "").lower().replace("_", " ")
        status = (info.call_status or "").lower()
        code = info.hangup_code
        if code == 3010 or "busy" in cause or "busy" in status:
            return CallStatus.busy
        if code in (3000, 6010) or "no answer" in cause or "timeout" in cause or "no-answer" in status:
            return CallStatus.no_answer
        if "cancel" in cause or "cancel" in status:
            return CallStatus.cancelled
        return CallStatus.failed
