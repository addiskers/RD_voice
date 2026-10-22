from __future__ import annotations

from typing import Optional

import httpx

from engine.config import Settings

from .base import Carrier, CarrierError
from .enablex import EnablexCarrier
from .plivo import PlivoCarrier
from .vobiz import VobizCarrier

_CARRIERS: dict[str, type[Carrier]] = {
    PlivoCarrier.name: PlivoCarrier,
    VobizCarrier.name: VobizCarrier,
    EnablexCarrier.name: EnablexCarrier,
}


class CarrierRegistry:
    def __init__(self, settings: Settings, http: Optional[httpx.AsyncClient] = None):
        self.settings = settings
        self._http = http
        self._instances: dict[str, Carrier] = {}

    def names(self) -> list[str]:
        return list(_CARRIERS)

    def get(self, name: Optional[str]) -> Carrier:
        key = (name or self.settings.default_provider or "plivo").strip().lower()
        if key not in _CARRIERS:
            raise CarrierError(f"unknown provider '{key}' (known: {', '.join(_CARRIERS)})")
        if key not in self._instances:
            self._instances[key] = _CARRIERS[key](self.settings, self._http)
        return self._instances[key]
