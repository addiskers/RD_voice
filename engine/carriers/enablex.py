"""EnableX adapter (stub pending schema verification against developer.enablex.io).

EnableX has no answer_url fetch: the bidirectional stream URL is passed at dial time
(``action_on_connect.stream``). The media WebSocket carries the same base64 mu-law frames as
Plivo. Field names below are taken from the EO prototype and MUST be verified on first live use.
"""

from __future__ import annotations

import base64
import logging
from typing import Any, Optional

from .base import Carrier, CarrierError, DialResult

logger = logging.getLogger(__name__)


class EnablexCarrier(Carrier):
    name = "enablex"

    @property
    def from_number(self) -> str:
        return self.settings.enablex_from_number

    def configured(self) -> bool:
        s = self.settings
        return bool(s.enablex_app_id and s.enablex_app_key and s.enablex_from_number)

    def _headers(self) -> dict[str, str]:
        tok = base64.b64encode(f"{self.settings.enablex_app_id}:{self.settings.enablex_app_key}".encode()).decode()
        return {"Authorization": f"Basic {tok}", "Content-Type": "application/json"}

    async def dial(self, to: str, from_number: str, *, answer_url: str, ring_url: str, hangup_url: str,
                   stream_url: str, extra: Optional[dict[str, Any]] = None) -> DialResult:
        if not self.configured():
            raise CarrierError("enablex credentials not configured")
        payload: dict[str, Any] = {
            "from": from_number or self.from_number,
            "to": to,
            "action_on_connect": {"stream": {"url": stream_url, "bidirectional": True}},
            "event_url": hangup_url,   # verify: EnableX event callback field name
        }
        if extra:
            payload.update(extra)
        url = self.settings.enablex_api_base.rstrip("/") + "/call"
        resp = await self.http.post(url, json=payload, headers=self._headers())
        if resp.status_code not in (200, 201, 202):
            raise CarrierError(f"enablex dial failed: HTTP {resp.status_code} {resp.text[:300]}")
        body = resp.json() if resp.content else {}
        result = body.get("result") if isinstance(body.get("result"), dict) else {}
        call_id = body.get("voice_id") or body.get("call_id") or result.get("voice_id")
        return DialResult(provider_call_id=str(call_id) if call_id else None, raw=body)

    async def hangup(self, provider_call_id: str) -> bool:
        if not provider_call_id:
            return False
        url = self.settings.enablex_api_base.rstrip("/") + f"/call/{provider_call_id}"
        resp = await self.http.delete(url, headers=self._headers())
        return resp.status_code in (200, 202, 204)
