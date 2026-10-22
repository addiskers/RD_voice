"""Tiny webhook receiver: verifies the HMAC signature and prints every event.

    python scripts/webhook_sink.py --port 9000 --secret dev-webhook-secret
Point POST /v1/calls ... "webhook_url": "http://<host>:9000/hook" (use a tunnel for a remote engine).
"""

from __future__ import annotations

import argparse
import json

import uvicorn
from fastapi import FastAPI, Header, Request

from engine.api.webhooks import verify

app = FastAPI()
SECRET = "dev-webhook-secret"


@app.post("/hook")
async def hook(request: Request, x_gvox_signature: str = Header(default=""),
               x_gvox_timestamp: str = Header(default=""), x_gvox_event: str = Header(default="")):
    body = await request.body()
    ok = verify(SECRET, x_gvox_timestamp, body, x_gvox_signature)
    data = json.loads(body)
    print(f"[{x_gvox_event}] signature={'ok' if ok else 'BAD'} call={data.get('call_id')} seq={data.get('seq')}")
    if x_gvox_event in ("call.ended", "call.failed", "call.tool"):
        print(json.dumps(data.get("data"), indent=2, ensure_ascii=False)[:3000])
    return {"ok": True}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--secret", default=SECRET)
    args = ap.parse_args()
    SECRET = args.secret
    uvicorn.run(app, host="0.0.0.0", port=args.port)
