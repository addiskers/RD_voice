"""Plays the carrier against the real worker media endpoint, so every telephony drill runs locally.

For each call: POST /v1/calls (provider `fake`) -> GET the answer XML as the carrier would -> open the
worker's media WebSocket and speak the Plivo <Stream> protocol (20 ms mu-law frames, 8 kHz) -> play
scripted caller utterances after each agent turn -> stop -> POST the hangup callback -> read the call
record. Measures end-of-caller-speech -> first agent audio per turn (client side) and reads the engine's
own per-turn metrics.

    python scripts/fake_carrier.py --calls 1 --script yes            # one scripted call
    python scripts/fake_carrier.py --calls 25 --minutes 10           # load test (keeps calls going for 10 min)
    python scripts/fake_carrier.py --calls 3 --script yes,no --json  # machine-readable (used by /v1/test/telephony-smoke)

Needs the API started with ENABLE_TEST_ENDPOINTS=true and real vendor keys (it spends call minutes).
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import sys
import time
import wave
from typing import Any, Optional

import httpx
import websockets

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from engine.worker.audio import (SILENCE_FRAME_ULAW, pcm16_to_mulaw, telephony_degrade)  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
FRAME_S = 0.02
TURN_TIMEOUT_S = 30.0
QUIET_S = 1.2          # agent audio silent this long = agent turn finished


# ----------------------------------------------------------------------------- Plivo <Stream> frames
def start_frame(stream_id: str, call_uuid: str, extra_headers: dict[str, str]) -> str:
    eh = ",".join(f"{k}={v}" for k, v in extra_headers.items())
    return json.dumps({
        "event": "start", "sequenceNumber": "1", "streamId": stream_id,
        "start": {"streamId": stream_id, "callId": call_uuid, "accountId": "fake", "tracks": ["inbound"],
                  "mediaFormat": {"encoding": "audio/x-mulaw", "sampleRate": 8000}},
        "extra_headers": eh,
    })


def media_frame(mulaw: bytes, stream_id: str = "") -> str:
    msg: dict[str, Any] = {"event": "media", "media": {"payload": base64.b64encode(mulaw).decode("ascii"),
                                                      "track": "inbound"}}
    if stream_id:
        msg["streamId"] = stream_id
    return json.dumps(msg)


def stop_frame(stream_id: str) -> str:
    return json.dumps({"event": "stop", "streamId": stream_id, "stop": {"reason": "client"}})


def load_utterance(path: str) -> list[bytes]:
    """A WAV (any rate, mono PCM16) as a list of 20 ms mu-law frames at 8 kHz, phone-degraded."""
    with wave.open(path, "rb") as wf:
        assert wf.getnchannels() == 1 and wf.getsampwidth() == 2, f"{path}: need mono PCM16"
        rate, pcm = wf.getframerate(), wf.readframes(wf.getnframes())
    pcm8 = pcm if rate == 8000 else telephony_degrade(pcm, rate)
    mulaw = pcm16_to_mulaw(pcm8, 8000)
    return [mulaw[i:i + 160].ljust(160, b"\xff") for i in range(0, len(mulaw), 160)]


TTS_REFS = {
    "cartesia": {"provider": "cartesia", "model": "sonic-3", "language": "en"},
    "sarvam": {"provider": "sarvam", "model": os.environ.get("SARVAM_TTS_MODEL", "bulbul:v3"),
               "voice": os.environ.get("SARVAM_TTS_SPEAKER", "rahul"), "language": "en-IN"},
    "elevenlabs": {"provider": "elevenlabs", "model": "eleven_flash_v2_5"},
}


def apply_overrides(agent: dict[str, Any], args) -> dict[str, Any]:
    """Same vendor switches as browser_smoke.py so latency_bench.py can run its matrix on the phone path."""
    if agent.get("mode", "cascaded") != "cascaded":
        return agent
    if args.stt:
        agent["stt"] = ({"provider": "sarvam", "model": "saaras:v4", "options": {"language": "unknown"}} if args.stt == "sarvam"
                        else {"provider": "deepgram", "model": args.stt, "options": {"language": "multi"}})
    if args.llm:
        if args.llm.startswith("groq:"):
            agent["llm"] = {"provider": "groq", "model": args.llm.split(":", 1)[1], "options": {"temperature": 0.4}}
        else:
            opts = {"temperature": 0.4}
            if args.thinking >= 0 and "2.5" in args.llm:
                opts["thinking_budget"] = args.thinking
            agent["llm"] = {"provider": "google", "model": args.llm, "options": opts}
    if args.tts:
        agent["tts"] = TTS_REFS[args.tts]
    return agent


def example_agent(mode: str = "cascaded") -> tuple[dict[str, Any], dict[str, Any]]:
    ex = os.path.join(os.path.dirname(HERE), "examples")
    agent = json.load(open(os.path.join(ex, "eo_rsvp_agent.json"), encoding="utf-8"))
    agent["workflow"] = json.load(open(os.path.join(ex, "eo_rsvp_workflow.json"), encoding="utf-8"))
    agent["mode"] = mode
    variables = {"first_name": "Pratik", "event_name": "the AI First Mindset Workshop", "event_date": "Friday the eleventh",
                 "event_time": "7 pm", "event_venue": "DoubleTree Ahmedabad"}
    return agent, variables


# ----------------------------------------------------------------------------- one call
async def run_call(args, idx: int, utterances: list[list[bytes]], http: httpx.AsyncClient) -> dict[str, Any]:
    H = {"Authorization": f"Bearer {args.key}"}
    res: dict[str, Any] = {"index": idx, "ok": False, "events": [], "client_latency_s": [], "dropped": False}
    agent, variables = example_agent(args.mode) if args.agent == "example" else ({
        "name": "smoke", "mode": "cascaded", "language": "en-IN", "max_duration_s": 120,
        "instructions": "You are a test agent. Keep replies to one short sentence.",
        "greeting": "Hello, this is the GlobalVox test agent. Can you hear me?",
        "llm": {"provider": "google", "model": "gemini-2.5-flash-lite"}}, {})
    agent = apply_overrides(agent, args)
    body = {"to": f"+91000000{idx:04d}", "provider": "fake", "agent": agent, "variables": variables}
    if args.webhook_url:
        body["webhook_url"] = args.webhook_url
    r = await http.post(f"{args.api}/v1/calls", json=body, headers=H, timeout=20)
    if r.status_code != 201:
        res["error"] = f"create: {r.status_code} {r.text[:200]}"
        return res
    call_id, pcid = r.json()["call_id"], r.json()["provider_call_id"]
    res["call_id"] = call_id
    answer = await http.get(f"{args.api}/carrier/fake/answer", params={"call_id": call_id, "CallUUID": pcid,
                            "From": "+910000000000", "To": f"+91000000{idx:04d}", "Direction": "outbound"}, timeout=20)
    if "<Stream" not in answer.text:
        res["error"] = f"answer xml: {answer.status_code} {answer.text[:200]}"
        return res

    t0 = time.monotonic()
    stream_id = f"st-{call_id}"
    agent_audio = bytearray()
    state = {"last_agent_audio": 0.0, "agent_spoke_since_utterance": False, "speech_end": None, "utt": 0,
             "first_audio": None}
    ended = asyncio.Event()
    deadline = t0 + (args.minutes * 60 if args.minutes else 180)

    async with websockets.connect(f"{args.worker_ws}", max_size=None, open_timeout=10) as ws:
        await ws.send(start_frame(stream_id, pcid, {"X-Call-Id": call_id, "X-Test-Token": args.internal_token}))

        async def receiver():
            try:
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    ev = str(msg.get("event") or "").lower()
                    if ev == "playaudio":
                        payload = (msg.get("media") or {}).get("payload") or ""
                        agent_audio.extend(base64.b64decode(payload))
                        now = time.monotonic()
                        if state["first_audio"] is None:
                            state["first_audio"] = round(now - t0, 3)
                        if state["speech_end"] is not None:
                            lat = round(now - state["speech_end"], 3)
                            res["client_latency_s"].append(lat)
                            res["events"].append(f"reply#{len(res['client_latency_s'])} +{lat}s")
                            state["speech_end"] = None
                        state["last_agent_audio"] = now
                        state["agent_spoke_since_utterance"] = True
                    elif ev in ("clearaudio", "checkpoint"):
                        pass
            except Exception as e:
                res["events"].append(f"ws closed: {type(e).__name__}")
            finally:
                ended.set()

        async def sender():
            next_tick = time.monotonic()
            script_done_at: Optional[float] = None
            while not ended.is_set() and time.monotonic() < deadline:
                now = time.monotonic()
                quiet = now - state["last_agent_audio"] > QUIET_S
                due = (state["utt"] < len(utterances) and state["agent_spoke_since_utterance"] and quiet)
                if state["speech_end"] is not None and now - state["speech_end"] > TURN_TIMEOUT_S:
                    res["events"].append("turn timeout")
                    res["dropped"] = True
                    break
                if due:
                    await asyncio.sleep(0.6)
                    frames = utterances[state["utt"]]
                    res["events"].append(f"speak#{state['utt'] + 1}@{now - t0:.1f}s")
                    try:
                        for fr in frames:
                            await ws.send(media_frame(fr, stream_id))
                            await asyncio.sleep(FRAME_S)
                    except websockets.ConnectionClosed:
                        res["events"].append("stream closed by the engine")
                        return
                    state["utt"] += 1
                    state["speech_end"] = time.monotonic()
                    state["agent_spoke_since_utterance"] = False
                    next_tick = time.monotonic()
                    continue
                if state["utt"] >= len(utterances) and state["agent_spoke_since_utterance"] and quiet:
                    if args.minutes:          # load mode: loop the script until the time is up
                        state["utt"] = 0
                    else:
                        script_done_at = script_done_at or now
                        if now - script_done_at > 1.0:
                            break
                try:
                    await ws.send(media_frame(SILENCE_FRAME_ULAW, stream_id))
                except websockets.ConnectionClosed:
                    res["events"].append(f"stream closed by the engine@{time.monotonic() - t0:.1f}s")
                    return
                next_tick += FRAME_S
                await asyncio.sleep(max(0.0, next_tick - time.monotonic()))
            res["script_finished"] = True
            try:
                await ws.send(stop_frame(stream_id))
                await ws.close()
            except Exception:
                pass

        recv_task = asyncio.create_task(receiver())
        try:
            await asyncio.wait_for(sender(), timeout=(deadline - time.monotonic()) + 15)
        except asyncio.TimeoutError:
            res["events"].append("sender timeout")
        try:
            await asyncio.wait_for(recv_task, timeout=5)
        except Exception:
            recv_task.cancel()

    duration = round(time.monotonic() - t0, 1)
    if args.hangup_delay and not res.get("script_finished"):
        # the stream dropped before the script finished (worker died?): a real carrier keeps the call up
        # (keepCallAlive) until the caller hangs up; emulate that patience
        res["events"].append(f"stream dropped early; hangup in {args.hangup_delay}s")
        await asyncio.sleep(args.hangup_delay)
    await http.post(f"{args.api}/carrier/fake/hangup", params={"call_id": call_id},
                    data={"CallUUID": pcid, "HangupCauseCode": "4000", "HangupCauseName": "Normal Hangup",
                          "Duration": str(int(duration)), "AnswerTime": "now"}, timeout=20)
    record: dict[str, Any] = {}
    for _ in range(40):
        record = (await http.get(f"{args.api}/v1/calls/{call_id}", headers=H, timeout=20)).json()
        if record.get("status") in ("completed", "failed", "busy", "no_answer", "cancelled"):
            break
        await asyncio.sleep(0.25)
    turns = [t for t in (record.get("metrics") or {}).get("turns", []) if t.get("total") is not None]
    res.update(status=record.get("status"), end_reason=record.get("end_reason"), duration_s=duration,
               agent_audio_s=round(len(agent_audio) / 8000, 2), first_audio_s=state["first_audio"],
               engine_total_s=[t["total"] for t in turns], utterances_spoken=state["utt"],
               agent_turns=sum(1 for t in record.get("transcript", []) if t.get("role") == "agent"),
               user_turns=sum(1 for t in record.get("transcript", []) if t.get("role") == "user"),
               user_texts=[t.get("text", "") for t in record.get("transcript", []) if t.get("role") == "user"],
               stages=[{k: t.get(k) for k in ("turn", "eou_delay", "llm_ttft", "tts_ttfb", "total", "transition")}
                       for t in turns],
               models=(record.get("metrics") or {}).get("models"),
               recording_url=record.get("recording_url"))
    engine_ended = record.get("end_reason") in ("workflow_end", "agent_end_call", "closing_detected", "max_duration")
    res["ok"] = (record.get("status") == "completed" and not res["dropped"] and res["agent_audio_s"] > 0.5
                 and (not utterances or res["user_turns"] >= 1) and (res.get("script_finished") or engine_ended))
    return res


def pct(values: list[float], p: float) -> Optional[float]:
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    k = max(0, min(len(vals) - 1, int(round((p / 100.0) * (len(vals) - 1)))))
    return round(vals[k], 3)


async def run(args) -> dict[str, Any]:
    names = [s.strip() for s in args.script.split(",") if s.strip()] if args.script else []
    utterances = []
    for name in names:
        path = name if os.path.exists(name) else os.path.join(args.wav_dir, f"tel_user_{name}.wav")
        if not os.path.exists(path):
            path = os.path.join(args.wav_dir, f"user_{name}.wav")
        utterances.append(load_utterance(path))
    async with httpx.AsyncClient() as http:
        results = await asyncio.gather(*(run_call(args, i + 1, utterances, http) for i in range(args.calls)),
                                       return_exceptions=True)
    calls = [r if isinstance(r, dict) else {"ok": False, "error": repr(r), "client_latency_s": [], "engine_total_s": []}
             for r in results]
    client_lat = [v for c in calls for v in c.get("client_latency_s", [])]
    engine_tot = [v for c in calls for v in c.get("engine_total_s", [])]
    return {
        "calls": len(calls), "completed": sum(1 for c in calls if c.get("status") == "completed"),
        "ok": all(c.get("ok") for c in calls), "dropped": sum(1 for c in calls if c.get("dropped")),
        "client_p50": pct(client_lat, 50), "client_p95": pct(client_lat, 95),
        "engine_total_p50": pct(engine_tot, 50), "engine_total_p95": pct(engine_tot, 95),
        "turns_measured": len(client_lat), "per_call": calls,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", default=os.environ.get("ENGINE_URL", "http://127.0.0.1:8080"))
    ap.add_argument("--key", default=os.environ.get("ENGINE_API_KEY", "dev-engine-key"))
    ap.add_argument("--internal-token", default=os.environ.get("INTERNAL_TOKEN", "dev-internal-token"))
    ap.add_argument("--worker-ws", default=os.environ.get("WORKER_MEDIA_URL", "ws://127.0.0.1:8090"))
    ap.add_argument("--calls", type=int, default=1)
    ap.add_argument("--minutes", type=float, default=0.0, help="load mode: keep each call going this long")
    ap.add_argument("--script", default="yes", help="comma-separated utterances: yes,no,nokids or WAV paths")
    ap.add_argument("--agent", default="example", choices=["example", "prompt"])
    ap.add_argument("--mode", default="cascaded", choices=["cascaded", "s2s"])
    ap.add_argument("--wav-dir", default=os.path.join(HERE, "assets"))
    ap.add_argument("--stt", default="", help="nova-3 | flux-general-multi | sarvam (cascaded agents)")
    ap.add_argument("--llm", default="", help="gemini-2.5-flash-lite | gemini-2.5-flash | groq:openai/gpt-oss-20b")
    ap.add_argument("--tts", default="", choices=["", "cartesia", "sarvam", "elevenlabs"])
    ap.add_argument("--thinking", type=int, default=0)
    ap.add_argument("--webhook-url", default="", help="register a webhook on each call (e.g. scripts/webhook_sink.py)")
    ap.add_argument("--hangup-delay", type=float, default=0.0,
                    help="seconds to keep the call up after the stream drops early (worker-loss drill)")
    ap.add_argument("--json", action="store_true", help="print only the JSON result")
    args = ap.parse_args()
    out = asyncio.run(run(args))
    if args.json:
        print(json.dumps(out))
    else:
        print(f"calls {out['calls']}  completed {out['completed']}  dropped {out['dropped']}  turns {out['turns_measured']}")
        print(f"client latency  p50 {out['client_p50']}  p95 {out['client_p95']}   (end of caller speech -> first agent audio)")
        print(f"engine total    p50 {out['engine_total_p50']}  p95 {out['engine_total_p95']}")
        for c in out["per_call"]:
            print(f"  #{c.get('index')} {c.get('call_id')} {c.get('status')} {c.get('end_reason')} "
                  f"agent_turns={c.get('agent_turns')} user_turns={c.get('user_turns')} "
                  f"{'OK' if c.get('ok') else 'FAIL'} {c.get('error', '')} {' | '.join(c.get('events', [])[:6])}")
        print("TELEPHONY SMOKE", "PASS" if out["ok"] else "FAIL")
    return 0 if out["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
