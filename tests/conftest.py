"""Shared fixtures: an API TestClient on a fresh SQLite DB with a fake Plivo carrier."""

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from engine.api.app import build_app
from engine.carriers.base import Carrier, DialResult
from engine.config import Settings

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"
KEY = "test-key"
INTERNAL = "test-internal"


class FakePlivo(Carrier):
    name = "plivo"
    dials: list = []
    hangups: list = []

    @property
    def from_number(self):
        return "+918000000000"

    def configured(self):
        return True

    async def dial(self, to, from_number, *, answer_url, ring_url, hangup_url, stream_url, extra=None):
        self.dials.append({"to": to, "answer_url": answer_url, "hangup_url": hangup_url, "stream_url": stream_url})
        return DialResult(provider_call_id="rq-1", raw={})

    async def hangup(self, provider_call_id):
        self.hangups.append(provider_call_id)
        return True


@pytest.fixture
def client(tmp_path):
    settings = Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path / 'engine.db'}",
                        recordings_dir=str(tmp_path / "rec"), engine_api_key=KEY, internal_token=INTERNAL,
                        public_http_url="https://api.test", public_ws_url="wss://media.test",
                        plivo_auth_id="x", plivo_auth_token="y", plivo_from_number="+918000000000",
                        carrier_verify_signatures=False,   # unrelated tests post unsigned callbacks
                        carrier_cps="plivo=100")           # the CPS budget has its own tests (test_capacity)
    app = build_app(settings)
    with TestClient(app) as c:
        engine = app.state.engine
        engine.carriers._instances["plivo"] = FakePlivo(settings)
        FakePlivo.dials.clear()
        FakePlivo.hangups.clear()
        yield c


def _agent_body():
    agent = json.loads((EXAMPLES / "eo_rsvp_agent.json").read_text(encoding="utf-8"))
    agent["workflow"] = json.loads((EXAMPLES / "eo_rsvp_workflow.json").read_text(encoding="utf-8"))
    return agent


H = {"Authorization": f"Bearer {KEY}"}
HI = {"X-Internal-Token": INTERNAL}
