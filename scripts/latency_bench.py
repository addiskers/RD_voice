"""Latency bench: run the synthesized-caller loop N times per configuration and report per-stage p50/p95.

    python scripts/latency_bench.py --runs 3 --tts cartesia --llm gemini-2.5-flash,groq:llama-3.3-70b-versatile --stt nova-3,flux-general-multi

Each configuration runs scripts/browser_smoke.py (headless browser transport) and reads the call record from the
engine API for the stage metrics. `total` = end of the caller's speech -> agent audio starts (user-perceived).
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
import statistics
import subprocess
import sys

import httpx

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable


def pct(values: list[float], p: float) -> float | None:
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    k = max(0, min(len(vals) - 1, int(round((p / 100.0) * (len(vals) - 1)))))
    return vals[k]


def run_once(args, llm: str, stt: str, tts: str) -> dict:
    cmd = [PY, os.path.join(HERE, "browser_smoke.py"), "--mode", "cascaded", "--tts", tts, "--llm", llm, "--stt", stt,
           "--thinking", str(args.thinking), "--speak-wav", args.wav, "--seconds", str(args.seconds), "--json-only"]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=args.seconds + 60)
    line = [ln for ln in out.stdout.splitlines() if ln.startswith("{")]
    if not line:
        return {"ok": False, "error": (out.stderr or out.stdout)[-400:]}
    res = json.loads(line[-1])
    if res.get("call_id"):
        try:
            r = httpx.get(f"{args.api}/v1/calls/{res['call_id']}", headers={"Authorization": f"Bearer {args.key}"},
                          timeout=10).json()
            res["metrics"] = r.get("metrics", {})
            res["transcript"] = [(t["role"], t["text"]) for t in r.get("transcript", [])]
        except Exception as e:
            res["error"] = f"record fetch failed: {e}"
    return res


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--llm", default="gemini-2.5-flash")
    ap.add_argument("--stt", default="nova-3")
    ap.add_argument("--tts", default="cartesia")
    ap.add_argument("--thinking", type=int, default=0)
    ap.add_argument("--wav", default=os.path.join(HERE, "assets", "user_yes.wav"))
    ap.add_argument("--seconds", type=float, default=24.0)
    ap.add_argument("--api", default=os.environ.get("ENGINE_URL", "http://127.0.0.1:8080"))
    ap.add_argument("--key", default=os.environ.get("ENGINE_API_KEY", "dev-engine-key"))
    args = ap.parse_args()

    configs = list(itertools.product(args.llm.split(","), args.stt.split(","), args.tts.split(",")))
    rows = []
    for llm, stt, tts in configs:
        stages: dict[str, list[float]] = {"total": [], "eou": [], "stt": [], "llm_ttft": [], "tts_ttfb": [],
                                          "first_word": []}
        oks = 0
        exact = 0
        for i in range(args.runs):
            res = run_once(args, llm, stt, tts)
            oks += 1 if res.get("ok") else 0
            m = res.get("metrics") or {}
            # the reply turn is the one with a total (the greeting has no user turn before it)
            turns = [t for t in m.get("turns", []) if t.get("total") is not None]
            if turns:
                t = turns[-1]
                stages["total"].append(t["total"])
                for k, src in (("eou", "eou_delay"), ("stt", "stt_delay"), ("llm_ttft", "llm_ttft"), ("tts_ttfb", "tts_ttfb")):
                    if t.get(src) is not None:
                        stages[k].append(t[src])
            if m.get("pickup_to_first_word") is not None:
                stages["first_word"].append(m["pickup_to_first_word"])
            user_text = " ".join(x[1] for x in res.get("transcript", []) if x[0] == "user").lower()
            if "hear you" in user_text:
                exact += 1
            print(f"  [{llm} | {stt} | {tts}] run {i + 1}: ok={res.get('ok')} total={turns[-1]['total'] if turns else None} "
                  f"ttft={turns[-1].get('llm_ttft') if turns else None} err={res.get('error', '')[:80]}", flush=True)
        row = {"llm": llm, "stt": stt, "tts": tts, "runs": args.runs, "ok": oks, "stt_ok": exact,
               **{f"{k}_p50": pct(v, 50) for k, v in stages.items()},
               **{f"{k}_p95": pct(v, 95) for k, v in stages.items()}}
        rows.append(row)

    def f(v):
        return "  —  " if v is None else f"{v:5.2f}"

    print("\nconfig                                              | ok | stt | total p50/p95 | eou  | stt  | ttft | ttfb | first")
    for r in rows:
        name = f"{r['llm']} | {r['stt']} | {r['tts']}"
        print(f"{name:52s}| {r['ok']}/{r['runs']} | {r['stt_ok']}/{r['runs']} | {f(r['total_p50'])}/{f(r['total_p95'])} | "
              f"{f(r['eou_p50'])} | {f(r['stt_p50'])} | {f(r['llm_ttft_p50'])} | {f(r['tts_ttfb_p50'])} | {f(r['first_word_p50'])}")
    with open(os.path.join(HERE, "..", "data", "latency_bench.json"), "w", encoding="utf-8") as fh:
        json.dump(rows, fh, indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
