"""Carrier bidirectional <Stream> WebSocket protocol (Plivo; Vobiz and EnableX are compatible).

Inbound (carrier -> engine) JSON text frames:
  start : {"event":"start","start":{"streamId","callId","accountId","mediaFormat":{"encoding","sampleRate"}},
           "extra_headers":"k=v,k=v"}
  media : {"event":"media","media":{"payload":"<base64 mu-law>","track":"inbound"}}
  dtmf  : {"event":"dtmf","dtmf":{"digit":"5"}}
  playedStream / clearedAudio : acknowledgements (Vobiz names; Plivo sends similar)
  stop  : {"event":"stop", ...}
Outbound (engine -> carrier):
  playAudio  : {"event":"playAudio","streamId":..,"media":{"contentType":"audio/x-mulaw","sampleRate":8000,"payload":..}}
  clearAudio : {"event":"clearAudio","streamId":..}          <- barge-in: flush carrier playout
  checkpoint : {"event":"checkpoint","streamId":..,"name":..} <- ack when playout reaches this point
Parsing is defensive (snake/camel case, nested/top-level) because field casing differs between accounts.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.parse import unquote


@dataclass
class StreamStart:
    stream_id: str = ""
    call_id: str = ""
    account_id: str = ""
    encoding: str = "audio/x-mulaw"
    sample_rate: int = 8000
    extra_headers: dict[str, str] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)


def _dig(d: Any, *keys: str, default: Any = None) -> Any:
    for k in keys:
        if isinstance(d, dict) and d.get(k) not in (None, ""):
            return d[k]
    return default


def parse_extra_headers(value: Any) -> dict[str, str]:
    if isinstance(value, dict):
        return {str(k): unquote(str(v)) for k, v in value.items()}
    headers: dict[str, str] = {}
    if isinstance(value, str) and value:
        for pair in value.split(","):
            if "=" in pair:
                k, v = pair.split("=", 1)
                headers[k.strip()] = unquote(v.strip())
    return headers


def event_type(msg: dict[str, Any]) -> str:
    return str(msg.get("event") or msg.get("type") or "").lower()


def parse_start(msg: dict[str, Any]) -> StreamStart:
    start = msg.get("start") if isinstance(msg.get("start"), dict) else msg
    media_fmt = start.get("mediaFormat") or start.get("media_format") or {}
    eh = _dig(msg, "extra_headers", "extraHeaders") or _dig(start, "extra_headers", "extraHeaders")
    return StreamStart(
        stream_id=str(_dig(start, "streamId", "stream_id") or _dig(msg, "streamId", "stream_id") or ""),
        call_id=str(_dig(start, "callId", "call_id", "callUUID") or _dig(msg, "callId", "call_id") or ""),
        account_id=str(_dig(start, "accountId", "account_id") or ""),
        encoding=str(_dig(media_fmt, "encoding") or "audio/x-mulaw"),
        sample_rate=int(_dig(media_fmt, "sampleRate", "sample_rate", default=8000) or 8000),
        extra_headers=parse_extra_headers(eh),
        raw=msg,
    )


def media_payload(msg: dict[str, Any]) -> Optional[str]:
    media = msg.get("media")
    if isinstance(media, dict):
        return media.get("payload")
    return None


def media_track(msg: dict[str, Any]) -> str:
    media = msg.get("media")
    if isinstance(media, dict):
        return str(media.get("track") or "inbound").lower()
    return "inbound"


def dtmf_digit(msg: dict[str, Any]) -> Optional[str]:
    d = msg.get("dtmf")
    if isinstance(d, dict):
        return d.get("digit")
    return None


# --- outbound builders ---------------------------------------------------------------------------
def play_audio(payload_b64: str, stream_id: Optional[str] = None) -> str:
    frame: dict[str, Any] = {
        "event": "playAudio",
        "media": {"contentType": "audio/x-mulaw", "sampleRate": 8000, "payload": payload_b64},
    }
    if stream_id:
        frame["streamId"] = stream_id
    return json.dumps(frame)


def clear_audio(stream_id: Optional[str] = None) -> str:
    frame: dict[str, Any] = {"event": "clearAudio"}
    if stream_id:
        frame["streamId"] = stream_id
    return json.dumps(frame)


def checkpoint(name: str, stream_id: Optional[str] = None) -> str:
    frame: dict[str, Any] = {"event": "checkpoint", "name": name}
    if stream_id:
        frame["streamId"] = stream_id
    return json.dumps(frame)
