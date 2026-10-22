"""Plivo Voice API adapter (REST: make call / hangup; XML answer with <Stream>)."""

from __future__ import annotations

import logging
from typing import Any, Optional

from .base import Carrier, CarrierError, DialResult

logger = logging.getLogger(__name__)


class PlivoCarrier(Carrier):
    name = "plivo"
    api_base = "https://api.plivo.com/v1/Account/{auth_id}/Call/"

    @property
    def auth_id(self) -> str:
        return self.settings.plivo_auth_id

    @property
    def auth_token(self) -> str:
        return self.settings.plivo_auth_token

    @property
    def from_number(self) -> str:
        return self.settings.plivo_from_number

    def configured(self) -> bool:
        return bool(self.auth_id and self.auth_token and self.from_number)

    def _call_url(self, suffix: str = "") -> str:
        return self.api_base.format(auth_id=self.auth_id) + suffix

    def _auth(self):
        return (self.auth_id, self.auth_token)

    def _headers(self) -> dict[str, str]:
        return {"Content-Type": "application/json"}

    def _fmt_number(self, number: str) -> str:
        return number  # Plivo accepts E.164 with '+'

    async def dial(self, to: str, from_number: str, *, answer_url: str, ring_url: str, hangup_url: str,
                   stream_url: str, extra: Optional[dict[str, Any]] = None) -> DialResult:
        if not self.configured():
            raise CarrierError(f"{self.name} credentials not configured")
        body: dict[str, Any] = {
            "from": self._fmt_number(from_number or self.from_number),
            "to": self._fmt_number(to),
            "answer_url": answer_url,
            "answer_method": "GET",
            "hangup_url": hangup_url,
            "hangup_method": "POST",
            "ring_url": ring_url,
            "ring_method": "POST",
        }
        if extra:
            body.update(extra)
        resp = await self.http.post(self._call_url(), json=body, auth=self._auth(), headers=self._headers())
        if resp.status_code not in (200, 201, 202):
            raise CarrierError(f"{self.name} dial failed: HTTP {resp.status_code} {resp.text[:300]}")
        data = resp.json() if resp.content else {}
        call_id = (data.get("request_uuid") or data.get("call_uuid") or data.get("RequestUUID")
                   or data.get("CallUUID"))
        logger.info("%s dial fired to %s (handle %s)", self.name, to, call_id)
        return DialResult(provider_call_id=str(call_id) if call_id else None, raw=data)

    async def hangup(self, provider_call_id: str) -> bool:
        if not provider_call_id:
            return False
        resp = await self.http.delete(self._call_url(f"{provider_call_id}/"), auth=self._auth(),
                                      headers=self._headers())
        ok = resp.status_code in (200, 204)
        if not ok:
            logger.warning("%s hangup %s -> HTTP %s %s", self.name, provider_call_id, resp.status_code,
                           resp.text[:200])
        return ok
