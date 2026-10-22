"""Direct LLM time-to-first-token from this machine (no LiveKit), to pick the fastest model for the voice loop.

    python scripts/llm_ttft_check.py
"""

from __future__ import annotations

import asyncio
import os
import statistics
import time

from dotenv import load_dotenv

SYSTEM = "You are a phone agent. Reply in one short sentence."
USER = "Yes, I can hear you clearly. What is this call about?"


async def gemini(model: str, budget: int | None, n: int) -> list[float]:
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=os.environ["GOOGLE_API_KEY"])
    cfg = types.GenerateContentConfig(system_instruction=SYSTEM, temperature=0.4,
                                      thinking_config=(types.ThinkingConfig(thinking_budget=budget) if budget is not None else None))
    out = []
    for _ in range(n):
        t0 = time.perf_counter()
        first = None
        async for chunk in await client.aio.models.generate_content_stream(model=model, contents=USER, config=cfg):
            if first is None and chunk.text:
                first = time.perf_counter() - t0
        out.append(first or -1)
    return out


async def openai_compat(model: str, base_url: str, key: str, n: int) -> list[float]:
    import openai

    client = openai.AsyncOpenAI(api_key=key, base_url=base_url)
    out = []
    for _ in range(n):
        t0 = time.perf_counter()
        first = None
        try:
            stream = await client.chat.completions.create(
                model=model, temperature=0.4, stream=True,
                messages=[{"role": "system", "content": SYSTEM}, {"role": "user", "content": USER}])
            async for chunk in stream:
                if first is None and chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
                    first = time.perf_counter() - t0
        except Exception as e:
            print(f"    {model}: ERROR {str(e)[:160]}")
            return [-1]
        out.append(first or -1)
    return out


async def main() -> int:
    load_dotenv()
    n = 3
    rows = []
    for model, budget in (("gemini-2.5-flash", 0), ("gemini-2.5-flash", None), ("gemini-2.5-flash-lite", 0),
                          ("gemini-2.0-flash", None)):
        try:
            t = await gemini(model, budget, n)
            rows.append((f"{model} (thinking {'off' if budget == 0 else 'default'})", t))
        except Exception as e:
            rows.append((f"{model}", [-1]))
            print(f"    {model}: ERROR {str(e)[:160]}")
    if os.environ.get("GROQ_API_KEY"):
        for model in ("llama-3.3-70b-versatile", "llama-3.1-8b-instant", "openai/gpt-oss-20b"):
            t = await openai_compat(model, "https://api.groq.com/openai/v1", os.environ["GROQ_API_KEY"], n)
            rows.append((f"groq {model}", t))
    print(f"\n{'model':45s} ttft p50   samples")
    for name, t in rows:
        good = [x for x in t if x > 0]
        print(f"{name:45s} {statistics.median(good) if good else float('nan'):6.2f} s   {[round(x, 2) for x in t]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
