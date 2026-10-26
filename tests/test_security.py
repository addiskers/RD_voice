"""Security minimums: Plivo V3 callback verification, Vobiz IP allowlist, encrypted secrets, authenticated
config and worker metrics, constant-time API key compare."""

import base64
import hashlib
import hmac

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient as AioClient, TestServer
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from conftest import INTERNAL, KEY, H, _agent_body
from engine.api.app import build_app
from engine.api.crypto import SecretBox, encrypt_agent_config, mask_agent_config
from engine.carriers.base import DialResult
from engine.carriers.plivo import PlivoCarrier
from engine.carriers.signatures import ip_in_allowlist, plivo_v3_base_string, plivo_v3_signature, verify_plivo_v3
from engine.config import Settings


class VerifyingFakePlivo(PlivoCarrier):
    """No network, but the real Plivo signature verification (auth token from settings)."""

    async def dial(self, to, from_number, *, answer_url, ring_url, hangup_url, stream_url, extra=None):
        return DialResult(provider_call_id="rq-1", raw={})

    async def hangup(self, provider_call_id):
        return True

PARAMS = {"CallUuid": "4vbcpem8-0u46-x1ha-9af1-438vc92bf374", "Digits": "1234", "From": "+15551111111", "To": "+15555555555"}
DOC_BASE = ("https://example.com/abcd?foo=bar.CallUuid4vbcpem8-0u46-x1ha-9af1-438vc92bf374Digits1234"
            "From+15551111111To+15555555555")


def _client(tmp_path, **kw) -> TestClient:
    settings = Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path / 'sec.db'}", engine_api_key=KEY,
                        internal_token=INTERNAL, public_http_url="https://api.test", public_ws_url="wss://media.test",
                        plivo_auth_id="x", plivo_auth_token="y", plivo_from_number="+918000000000", **kw)
    app = build_app(settings)
    c = TestClient(app)
    c.__enter__()
    app.state.engine.carriers._instances["plivo"] = VerifyingFakePlivo(settings)
    return c


def test_plivo_v3_post_matches_documented_example():
    base = plivo_v3_base_string("POST", "https://example.com/abcd?foo=bar", PARAMS)
    assert base == DOC_BASE
    expected = base64.b64encode(hmac.new(b"t0k", (DOC_BASE + ".n1").encode(), hashlib.sha256).digest()).decode()
    assert plivo_v3_signature("t0k", base, "n1") == expected
    assert verify_plivo_v3("t0k", "POST", "https://example.com/abcd?foo=bar", "n1", expected + ",other", PARAMS)
    assert not verify_plivo_v3("t0k", "POST", "https://example.com/abcd?foo=bar", "n2", expected, PARAMS)
    assert not verify_plivo_v3("wrong", "POST", "https://example.com/abcd?foo=bar", "n1", expected, PARAMS)


def test_plivo_v3_get_merges_query_sorted():
    assert plivo_v3_base_string("GET", "https://x/a?b=2&a=1", {}) == "https://x/a?a=1&b=2"
    assert plivo_v3_base_string("GET", "https://x/a", {}) == "https://x/a"
    assert plivo_v3_base_string("GET", "https://x/a?b=2", {"a": "1"}) == "https://x/a?a=1&b=2"


def test_vobiz_ip_allowlist():
    assert ip_in_allowlist("10.1.2.3", "10.1.2.0/24, 1.1.1.1")
    assert ip_in_allowlist("1.1.1.1", "10.1.2.0/24, 1.1.1.1")
    assert not ip_in_allowlist("10.2.0.1", "10.1.2.0/24, 1.1.1.1")
    assert not ip_in_allowlist("10.2.0.1", "")


def _signed_headers(token: str, method: str, url: str, params: dict, nonce: str = "nonce-1") -> dict:
    sig = plivo_v3_signature(token, plivo_v3_base_string(method, url, params), nonce)
    return {"X-Plivo-Signature-V3": sig, "X-Plivo-Signature-V3-Nonce": nonce}


def test_plivo_v3_uses_public_url(tmp_path):
    c = _client(tmp_path, carrier_verify_signatures=True)
    try:
        call_id = c.post("/v1/calls", json={"to": "+919876543210", "agent": {"name": "s", "instructions": "x",
                         "llm": {"provider": "google", "model": "gemini-2.5-flash"}}}, headers=H).json()["call_id"]
        path = f"/carrier/plivo/answer?call_id={call_id}&CallUUID=cu-1&From=%2B918000000000&To=%2B919876543210"
        # Plivo signs the public URL it called, not the proxied internal host the request arrives on
        good = _signed_headers("y", "GET", "https://api.test" + path, {})
        r = c.get(path, headers=good)
        assert r.status_code == 200 and "<Stream" in r.text
        bad = _signed_headers("y", "GET", "https://api.test" + path, {}, nonce="other")
        bad["X-Plivo-Signature-V3-Nonce"] = "nonce-1"
        assert c.get(path, headers=bad).status_code == 403
        # POST callback: form fields are part of the base string
        hpath = f"/carrier/plivo/hangup?call_id={call_id}"
        form = {"CallUUID": "cu-1", "HangupCauseCode": "4000"}
        r = c.post(hpath, data=form, headers=_signed_headers("y", "POST", "https://api.test" + hpath, form))
        assert r.status_code == 200
        assert c.get(f"/v1/calls/{call_id}", headers=H).json()["status"] == "completed"
    finally:
        c.__exit__(None, None, None)


def test_unsigned_carrier_callback_403(tmp_path):
    c = _client(tmp_path, carrier_verify_signatures=True)
    try:
        assert c.get("/carrier/plivo/answer?call_id=x").status_code == 403
        assert c.post("/carrier/plivo/hangup?call_id=x", data={"CallUUID": "cu"}).status_code == 403
    finally:
        c.__exit__(None, None, None)


def test_secretbox_roundtrip_and_mask():
    box = SecretBox(Fernet.generate_key().decode())
    token = box.encrypt("example-key-abcdef123456")
    assert token.startswith("enc:v1:") and box.decrypt(token) == "example-key-abcdef123456"
    assert box.decrypt("plain-value") == "plain-value"
    assert SecretBox.mask("example-key-abcdef123456") == "****3456" and SecretBox.mask("short") == "****"
    plain = SecretBox("")
    assert plain.encrypt("v") == "v" and plain.decrypt("v") == "v"
    cfg = encrypt_agent_config({"stt": {"provider": "deepgram", "api_key": "example-key-1234"}, "llm": {"provider": "google"}}, box)
    assert cfg["stt"]["api_key"].startswith("enc:v1:") and "api_key" not in cfg["llm"]
    assert mask_agent_config(cfg, box)["stt"]["api_key"] == "****1234"


def test_agent_keys_encrypted_at_rest_and_masked_on_get(tmp_path):
    key = Fernet.generate_key().decode()
    c = _client(tmp_path, carrier_verify_signatures=False, engine_encryption_key=key)
    try:
        agent = _agent_body()
        agent["stt"] = {"provider": "deepgram", "model": "nova-3", "api_key": "example-key-1234"}
        assert c.put("/v1/agents/eo-rsvp", json=agent, headers=H).status_code == 200
        shown = c.get("/v1/agents/eo-rsvp", headers=H).json()["config"]["stt"]["api_key"]
        assert shown == "****1234"
        engine = c.app.state.engine

        async def raw():
            from engine.api.db import AgentVersion
            async with engine.db.session() as s:
                row = await s.get(AgentVersion, ("eo-rsvp", 1))
                return row.config["stt"]["api_key"]

        assert c.portal.call(raw).startswith("enc:v1:")
        call_id = c.post("/v1/calls", json={"to": "+919876543210", "agent": {"agent_id": "eo-rsvp"},
                                            "variables": {"first_name": "P"}, "webhook_url": "https://p/hook",
                                            "webhook_secret": "hook-secret-9999"}, headers=H).json()["call_id"]
        rt = c.get(f"/internal/calls/{call_id}/runtime", headers={"X-Internal-Token": INTERNAL}).json()
        assert rt["agent"]["stt"]["api_key"] == "example-key-1234"      # the worker needs the real key

        async def call_row():
            from engine.api.db import Call
            async with engine.db.session() as s:
                row = await s.get(Call, call_id)
                return row.webhook_secret, row.agent_config["stt"]["api_key"]

        secret, frozen_key = c.portal.call(call_row)
        assert secret.startswith("enc:v1:") and frozen_key.startswith("enc:v1:")
    finally:
        c.__exit__(None, None, None)


def test_ui_config_requires_key(client):
    assert client.get("/ui/config.json").status_code == 401
    assert client.get("/ui/config.json", headers=H).status_code == 200


def test_api_key_compare_rejects_prefix_and_empty(client):
    assert client.get("/v1/capacity", headers={"Authorization": f"Bearer {KEY}x"}).status_code == 401
    assert client.get("/v1/capacity", headers={"Authorization": "Bearer "}).status_code == 401


@pytest.mark.asyncio
async def test_worker_metrics_requires_internal_token():
    from engine.worker.main import internal_token_middleware

    async def metrics(request):
        return web.Response(text="gvox 1")

    app = web.Application(middlewares=[internal_token_middleware("tok", protected=("/metrics",))])
    app.router.add_get("/metrics", metrics)
    app.router.add_get("/healthz", metrics)
    async with AioClient(TestServer(app)) as c:
        assert (await c.get("/metrics")).status == 401
        assert (await c.get("/metrics", headers={"X-Internal-Token": "tok"})).status == 200
        assert (await c.get("/healthz")).status == 200
