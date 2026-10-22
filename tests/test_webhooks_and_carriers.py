import asyncio
import json
import time

import httpx
import pytest

from engine.api.webhooks import sign, verify
from engine.carriers.base import CallbackInfo
from engine.carriers.plivo import PlivoCarrier
from engine.carriers.vobiz import VobizCarrier
from engine.config import Settings
from engine.schemas.calls import CallStatus


def test_signature_roundtrip():
    body = b'{"a":1}'
    ts = str(int(time.time()))
    sig = sign("secret", ts, body)
    assert sig.startswith("v1=") and verify("secret", ts, body, sig)
    assert not verify("other", ts, body, sig)
    assert not verify("secret", str(int(time.time()) - 10_000), body, sign("secret", str(int(time.time()) - 10_000), body))


def _settings(**kw) -> Settings:
    base = dict(plivo_auth_id="MA_X", plivo_auth_token="tok", plivo_from_number="+918000000000",
                vobiz_auth_id="V", vobiz_auth_token="vt", vobiz_from_number="+918000000001")
    base.update(kw)
    return Settings(_env_file=None, **base)


def test_plivo_answer_xml_and_status_mapping():
    c = PlivoCarrier(_settings())
    xml = c.answer_xml("wss://media.example.com/stream/call_1", {"X-Call-Id": "call_1"})
    assert 'bidirectional="true"' in xml and 'keepCallAlive="true"' in xml
    assert ">wss://media.example.com/stream/call_1</Stream>" in xml
    assert 'extraHeaders="X-Call-Id=call_1"' in xml
    assert c.final_status(CallbackInfo(event="hangup", hangup_code=3010, hangup_cause="Busy"), False) == CallStatus.busy
    assert c.final_status(CallbackInfo(event="hangup", hangup_code=3000), False) == CallStatus.no_answer
    assert c.final_status(CallbackInfo(event="hangup", hangup_cause="Ring Timeout"), False) == CallStatus.no_answer
    assert c.final_status(CallbackInfo(event="hangup", hangup_cause="NORMAL_CLEARING"), True) == CallStatus.completed
    assert c.final_status(CallbackInfo(event="hangup", answered=True), False) == CallStatus.completed
    assert c.final_status(CallbackInfo(event="hangup", hangup_cause="Unallocated number"), False) == CallStatus.failed


def test_plivo_callback_parsing():
    c = PlivoCarrier(_settings())
    info = c.parse_callback({"CallUUID": "cu-1", "RequestUUID": "rq-1", "From": "+918000000000", "To": "+919876543210",
                             "Direction": "outbound", "HangupCauseCode": "4000", "HangupCauseName": "Normal Hangup",
                             "AnswerTime": "2026-09-07 10:00:00", "Duration": "63"}, "hangup")
    assert info.provider_call_id == "cu-1" and info.request_uuid == "rq-1"
    assert info.answered is True and info.hangup_code == 4000 and info.duration_s == 63.0
    info2 = c.parse_callback({"CallUUID": "cu-2", "HangupCauseCode": "3000"}, "hangup")
    assert info2.answered is False


@pytest.mark.asyncio
async def test_plivo_dial_uses_rest_and_returns_handle():
    seen = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("authorization", "")
        seen["body"] = json.loads(request.content)
        return httpx.Response(201, json={"message": "call fired", "request_uuid": "rq-42"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    c = PlivoCarrier(_settings(), http=client)
    res = await c.dial("+919876543210", "", answer_url="https://api/answer", ring_url="https://api/ring",
                       hangup_url="https://api/hangup", stream_url="wss://ws/stream/x")
    assert res.provider_call_id == "rq-42"
    assert seen["url"] == "https://api.plivo.com/v1/Account/MA_X/Call/"
    assert seen["auth"].startswith("Basic ")
    assert seen["body"]["from"] == "+918000000000" and seen["body"]["answer_method"] == "GET"
    assert seen["body"]["hangup_url"] == "https://api/hangup" and seen["body"]["ring_url"] == "https://api/ring"


@pytest.mark.asyncio
async def test_vobiz_dial_is_plivo_compatible_with_header_auth():
    seen = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["headers"] = dict(request.headers)
        seen["body"] = json.loads(request.content)
        return httpx.Response(201, json={"call_uuid": "vc-1"})

    c = VobizCarrier(_settings(), http=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    res = await c.dial("+919876543210", "", answer_url="a", ring_url="r", hangup_url="h", stream_url="s")
    assert res.provider_call_id == "vc-1"
    assert seen["url"] == "https://api.vobiz.ai/api/v1/Account/V/Call/"
    assert seen["headers"]["x-auth-id"] == "V" and "authorization" not in seen["headers"]
    assert seen["body"]["to"] == "919876543210" and seen["body"]["from"] == "918000000001"
