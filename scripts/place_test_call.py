"""Place a test call through the engine API from a laptop.

    python scripts/place_test_call.py +919876543210 --agent-file examples/eo_rsvp_agent.json \
        --workflow-file examples/eo_rsvp_workflow.json --var first_name=Pratik \
        --webhook-url https://<your sink>/hook

Environment: ENGINE_URL (default http://localhost:8080), ENGINE_API_KEY.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

import httpx


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("to")
    ap.add_argument("--agent-id", help="Registered agent id (PUT /v1/agents/{id} first)")
    ap.add_argument("--agent-file", help="Inline agent config JSON")
    ap.add_argument("--workflow-file", help="Workflow JSON merged into the inline agent config")
    ap.add_argument("--var", action="append", default=[], help="name=value template variable")
    ap.add_argument("--provider", default=None)
    ap.add_argument("--webhook-url", default=None)
    ap.add_argument("--mode", choices=["cascaded", "s2s"], default=None, help="override the agent mode")
    ap.add_argument("--wait", action="store_true", help="poll until the call ends and print the record")
    args = ap.parse_args()

    base = os.environ.get("ENGINE_URL", "http://localhost:8080").rstrip("/")
    key = os.environ.get("ENGINE_API_KEY", "dev-engine-key")
    headers = {"Authorization": f"Bearer {key}"}

    if args.agent_id:
        agent: dict = {"agent_id": args.agent_id}
    elif args.agent_file:
        agent = json.load(open(args.agent_file, encoding="utf-8"))
        if args.workflow_file:
            agent["workflow"] = json.load(open(args.workflow_file, encoding="utf-8"))
        if args.mode:
            agent["mode"] = args.mode
    else:
        print("--agent-id or --agent-file required", file=sys.stderr)
        return 2

    variables = dict(v.split("=", 1) for v in args.var if "=" in v)
    body = {"to": args.to, "agent": agent, "variables": variables, "provider": args.provider,
            "webhook_url": args.webhook_url, "metadata": {"source": "place_test_call"}}
    r = httpx.post(f"{base}/v1/calls", json=body, headers=headers, timeout=30)
    print(r.status_code, r.text)
    if r.status_code != 201:
        return 1
    call_id = r.json()["call_id"]
    if not args.wait:
        return 0
    while True:
        time.sleep(3)
        c = httpx.get(f"{base}/v1/calls/{call_id}", headers=headers, timeout=10).json()
        print(f"  status={c['status']} turns={len(c.get('transcript', []))}")
        if c["status"] in ("completed", "failed", "busy", "no_answer", "cancelled"):
            print(json.dumps(c, indent=2, ensure_ascii=False))
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
