"""Headless check of the browser transport: open the worker's /browser WebSocket, start a session with a
single-prompt agent, send silence for a few seconds and report what came back (greeting audio, transcript).

    python scripts/browser_smoke.py [--seconds 12] [--mode s2s|cascaded]
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import time

import websockets


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=os.environ.get("BROWSER_WS_URL", "ws://localhost:8091/browser"))
    ap.add_argument("--key", default=os.environ.get("BROWSER_KEY", "demo-browser-key"))
    ap.add_argument("--seconds", type=float, default=12.0)
    ap.add_argument("--mode", default="s2s")
    ap.add_argument("--example", action="store_true", help="use the EO RSVP workflow example instead of a prompt")
    ap.add_argument("--tts", default="cartesia", choices=["cartesia", "sarvam", "elevenlabs"])
    ap.add_argument("--speak-wav", default=None, help="16 kHz mono PCM16 WAV streamed as the caller after the greeting")
    ap.add_argument("--speak-after", type=float, default=None, help="seconds after start to begin the utterance "
                    "(default: right after the first agent turn completes)")
    ap.add_argument("--llm", default="gemini-2.5-flash-lite",
                    help="gemini-2.5-flash | gemini-2.5-flash-lite | groq:openai/gpt-oss-20b | groq:openai/gpt-oss-120b")
    ap.add_argument("--stt", default="nova-3", help="nova-3 | flux-general-multi | flux-general-en | sarvam")
    ap.add_argument("--thinking", type=int, default=0, help="Gemini 2.5 thinking budget (0 = off, -1 = model default)")
    ap.add_argument("--json-only", action="store_true", help="print only the JSON result (for latency_bench.py)")
    ap.add_argument("--flat", action="store_true", help="run the workflow flattened (one LLM call per turn)")
    args = ap.parse_args()
    tts_refs = {
        "cartesia": {"provider": "cartesia", "model": "sonic-3", "language": "en"},
        "sarvam": {"provider": "sarvam", "model": os.environ.get("SARVAM_TTS_MODEL", "bulbul:v3"),
                   "voice": os.environ.get("SARVAM_TTS_SPEAKER", "rahul"), "language": "en-IN"},
        "elevenlabs": {"provider": "elevenlabs", "model": "eleven_flash_v2_5"},
    }

    model = os.environ.get("DEFAULT_REALTIME_MODEL", "gemini-3.1-flash-live-preview")
    variables: dict = {}
    if args.example:
        import json as _json
        from pathlib import Path
        ex = Path(__file__).resolve().parents[1] / "examples"
        agent = _json.loads((ex / "eo_rsvp_agent.json").read_text(encoding="utf-8"))
        agent["workflow"] = _json.loads((ex / "eo_rsvp_workflow.json").read_text(encoding="utf-8"))
        agent["mode"] = args.mode
        agent["realtime"] = {"provider": "google", "model": model, "voice": "Aoede", "language": "en-IN"}
        agent["record"] = False
        if args.flat:
            agent["workflow_mode"] = "flat"
        variables = {"first_name": "Pratik", "event_name": "the AI First Mindset Workshop", "event_date": "Friday the eleventh",
                     "event_time": "7 pm", "event_venue": "DoubleTree Ahmedabad"}
    else:
        agent = {
            "name": "smoke", "mode": args.mode, "language": "en-IN",
            "instructions": "You are a test agent. Keep replies to one short sentence.",
            "greeting": "Hello, this is the GlobalVox test agent. Can you hear me?",
            "llm": {"provider": "google", "model": "gemini-2.5-flash"},
            "realtime": {"provider": "google", "model": model, "voice": "Aoede", "language": "en-IN"},
            "max_duration_s": 60, "record": False,
        }
    if args.mode == "cascaded":
        if args.stt == "sarvam":
            # saarika = same-language transcription (Hindi stays Hindi); 'unknown' lets Sarvam detect the language
            agent["stt"] = {"provider": "sarvam", "model": "saarika:v2.5", "options": {"language": "unknown"}}
        else:
            agent["stt"] = {"provider": "deepgram", "model": args.stt, "options": {"language": "multi"}}
        if args.llm.startswith("groq:"):
            agent["llm"] = {"provider": "groq", "model": args.llm.split(":", 1)[1], "options": {"temperature": 0.4}}
        else:
            llm_opts = {"temperature": 0.4}
            if args.thinking >= 0 and "2.5" in args.llm:
                llm_opts["thinking_budget"] = args.thinking
            agent["llm"] = {"provider": "google", "model": args.llm, "options": llm_opts}
        agent["tts"] = tts_refs[args.tts]
    stats = {"audio_bytes": 0, "audio_msgs": 0, "transcripts": [], "events": [], "call_id": None}
    t0 = time.monotonic()
    first_audio = None
    silence = base64.b64encode(bytes(640)).decode()  # 20 ms of PCM16 @ 16 kHz
    utterances: list[list[str]] = []          # one or more WAVs (comma-separated), spoken one per agent turn
    if args.speak_wav:
        import wave
        for path in args.speak_wav.split(","):
            with wave.open(path.strip(), "rb") as wf:
                assert wf.getframerate() == 16000 and wf.getnchannels() == 1, "need 16 kHz mono PCM16"
                pcm = wf.readframes(wf.getnframes())
            utterances.append([base64.b64encode(pcm[i:i + 640]).decode() for i in range(0, len(pcm) - 639, 640)])
    speech_frames = utterances[0] if utterances else []
    first_turn_done = asyncio.Event()
    turn_state = {"turn_complete_since_speech": False, "last_agent_audio": 0.0}
    ended = asyncio.Event()

    async with websockets.connect(args.url, max_size=None) as ws:
        await ws.send(json.dumps({"type": "setup", "auth_key": args.key, "agent": agent, "sample_rate": 16000,
                                  "variables": variables}))

        async def sender():
            idx = 0
            while time.monotonic() - t0 < args.seconds and not ended.is_set():
                now = time.monotonic()
                quiet = now - turn_state["last_agent_audio"] > 1.2
                due = idx < len(utterances) and (
                    (args.speak_after is not None and idx == 0 and now - t0 >= args.speak_after)
                    or (turn_state["turn_complete_since_speech"] and quiet))
                if due:
                    await asyncio.sleep(0.6)               # a natural pause after the agent finishes
                    frames = utterances[idx]
                    stats["events"].append(f"speaking#{idx + 1}@{time.monotonic() - t0:.2f}s ({len(frames) * 0.02:.1f}s)")
                    for fr in frames:
                        await ws.send(json.dumps({"type": "audio", "data": fr}))
                        await asyncio.sleep(0.02)
                    idx += 1
                    turn_state["turn_complete_since_speech"] = False
                    turn_state["speech_end"] = time.monotonic()
                    stats["events"].append(f"spoke#{idx}@{time.monotonic() - t0:.2f}s")
                    continue
                await ws.send(json.dumps({"type": "audio", "data": silence}))
                await asyncio.sleep(0.02)
            if not ended.is_set():
                await ws.send(json.dumps({"type": "stop"}))

        send_task = asyncio.create_task(sender())
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=args.seconds + 10)
                m = json.loads(raw)
                t = m.get("type")
                if t == "audio":
                    stats["audio_msgs"] += 1
                    stats["audio_bytes"] += len(base64.b64decode(m["data"]))
                    now = time.monotonic()
                    if turn_state.get("speech_end") is not None:
                        # client-observed reply latency: last caller frame sent -> first agent audio received
                        lat = now - turn_state["speech_end"]
                        stats.setdefault("reply_latency_s", []).append(round(lat, 3))
                        stats["events"].append(f"reply#{len(stats['reply_latency_s'])}@{now - t0:.2f}s +{lat:.2f}s after speech end")
                        turn_state["speech_end"] = None
                    turn_state["last_agent_audio"] = now
                    if first_audio is None:
                        first_audio = now - t0
                elif t == "transcript":
                    stats["transcripts"].append(f"{m['role']}: {m['text']}")
                elif t == "ready":
                    stats["call_id"] = m.get("call_id")
                    stats["events"].append(f"ready@{time.monotonic() - t0:.2f}s")
                elif t == "metrics":
                    done = [x for x in m.get("turns", []) if x.get("total") is not None]
                    if done:
                        stats["events"].append(f"metrics@{time.monotonic() - t0:.2f}s total={done[-1]['total']}")
                elif t == "node":
                    stats["events"].append(f"node@{time.monotonic() - t0:.2f}s {m.get('to')}")
                elif t in ("interrupted", "turn_complete", "error", "session_end"):
                    stats["events"].append(f"{t}@{time.monotonic() - t0:.2f}s {m.get('message', '') or m.get('reason', '')}")
                    if t == "turn_complete":
                        first_turn_done.set()
                        turn_state["turn_complete_since_speech"] = True
                    if t == "session_end":
                        ended.set()
                        stats["end_reason"] = m.get("reason")
                        break
        except (asyncio.TimeoutError, websockets.ConnectionClosed) as e:
            stats["events"].append(f"closed: {e}")
        finally:
            send_task.cancel()

    secs = stats["audio_bytes"] / (2 * 16000)
    ok = stats["call_id"] is not None and secs > 0.5
    users = [t for t in stats["transcripts"] if t.startswith("user:")]
    agents = [t for t in stats["transcripts"] if t.startswith("agent:")]
    if utterances:
        ok = ok and len(users) >= len(utterances) and len(agents) >= len(utterances) + 1
    result = {**stats, "agent_audio_seconds": round(secs, 2),
              "first_audio_after_s": round(first_audio, 2) if first_audio else None,
              "user_turns": len(users), "agent_turns": len(agents), "ok": ok}
    if args.json_only:
        print(json.dumps(result))
    else:
        print(json.dumps(result, indent=2))
        if speech_frames:
            print(f"conversation check: user turns={len(users)} agent turns={len(agents)}")
        print("SMOKE", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
