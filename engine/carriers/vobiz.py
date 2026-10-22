"""Vobiz adapter: Plivo-compatible REST + <Stream> WebSocket.

Differences from Plivo (per Vobiz docs and Dograh's provider): X-Auth-ID / X-Auth-Token headers
instead of basic auth, JSON bodies, numbers WITHOUT the '+' prefix, ``call_uuid`` in the response.
"""

from __future__ import annotations

from .plivo import PlivoCarrier


class VobizCarrier(PlivoCarrier):
    name = "vobiz"

    @property
    def api_base(self) -> str:  # type: ignore[override]
        return self.settings.vobiz_api_base.rstrip("/") + "/Account/{auth_id}/Call/"

    @property
    def auth_id(self) -> str:
        return self.settings.vobiz_auth_id

    @property
    def auth_token(self) -> str:
        return self.settings.vobiz_auth_token

    @property
    def from_number(self) -> str:
        return self.settings.vobiz_from_number

    def _auth(self):
        return None  # header auth instead of basic auth

    def _headers(self) -> dict[str, str]:
        return {"Content-Type": "application/json", "X-Auth-ID": self.auth_id, "X-Auth-Token": self.auth_token}

    def _fmt_number(self, number: str) -> str:
        return number.lstrip("+")
