"""Webhook events (engine -> platform).

Every delivery is a POST with JSON body ``WebhookEvent`` and headers:
  X-GVox-Signature: v1=<hex hmac-sha256(secret, f"{timestamp}.{body}")>
  X-GVox-Timestamp: <unix seconds>
  X-GVox-Event: <event type>
  X-GVox-Delivery: <event id>
Consumers must be idempotent on (call_id, seq).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class EventType:
    call_queued = "call.queued"
    call_ringing = "call.ringing"
    call_answered = "call.answered"
    call_turn = "call.turn"           # one transcript turn (user or agent)
    call_tool = "call.tool"           # a tool was invoked (name, arguments, result)
    call_transition = "call.transition"  # workflow node change
    call_ended = "call.ended"
    call_failed = "call.failed"

    ALL = (call_queued, call_ringing, call_answered, call_turn, call_tool,
           call_transition, call_ended, call_failed)


class WebhookEvent(BaseModel):
    id: str = Field(..., description="Unique delivery id")
    type: str
    call_id: str
    seq: int = Field(..., description="Monotonic per call; consumers dedupe on (call_id, seq)")
    timestamp: str
    agent_id: str | None = None
    agent_version: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict, description="The metadata passed at call creation")
    data: dict[str, Any] = Field(default_factory=dict)
