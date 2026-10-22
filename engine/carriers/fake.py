"""`fake` carrier: no network, for the Plivo-protocol test client (scripts/fake_carrier.py).

Enabled only with ENABLE_TEST_ENDPOINTS=true. ``dial`` returns a generated CallUUID; the client then plays
the carrier's part itself: it fetches the answer XML, opens the media WebSocket to the worker with that
CallUUID, and posts the hangup callback at the end. Streams for fake calls must carry the internal token in
the ``X-Test-Token`` extra header or the worker drops them."""

from __future__ import annotations

import uuid
from typing import Any, Optional

from .base import Carrier, DialResult


class FakeCarrier(Carrier):
    name = "fake"

    def __init__(self, settings, http=None):
        super().__init__(settings, http)
        self.hangups: list[str] = []

    @property
    def from_number(self) -> str:
        return "+910000000000"

    def configured(self) -> bool:
        return bool(self.settings.enable_test_endpoints)

    async def dial(self, to: str, from_number: str, *, answer_url: str, ring_url: str, hangup_url: str,
                   stream_url: str, extra: Optional[dict[str, Any]] = None) -> DialResult:
        return DialResult(provider_call_id=f"fake-{uuid.uuid4().hex[:12]}",
                          raw={"answer_url": answer_url, "hangup_url": hangup_url, "stream_url": stream_url})

    async def hangup(self, provider_call_id: str) -> bool:
        self.hangups.append(provider_call_id)
        return True

    def verify_callback(self, **kwargs) -> bool:
        return self.configured()        # only when ENABLE_TEST_ENDPOINTS=true; never a bypass in production
