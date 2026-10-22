"""Call contract: create request, live record, per-turn metrics."""

from __future__ import annotations

from enum import Enum
from typing import Any, Optional, Union

from pydantic import BaseModel, Field, model_validator

from .agent import AgentConfig, AgentRef


class CallStatus(str, Enum):
    queued = "queued"
    dialing = "dialing"
    ringing = "ringing"
    in_progress = "in_progress"
    completed = "completed"
    failed = "failed"
    busy = "busy"
    no_answer = "no_answer"
    cancelled = "cancelled"


TERMINAL_STATUSES = {CallStatus.completed, CallStatus.failed, CallStatus.busy,
                     CallStatus.no_answer, CallStatus.cancelled}


class Direction(str, Enum):
    outbound = "outbound"
    inbound = "inbound"
    browser = "browser"


class CallCreate(BaseModel):
    to: str = Field(..., min_length=3, description="E.164 destination, e.g. +919876543210")
    from_number: Optional[str] = Field(default=None, alias="from")
    provider: Optional[str] = Field(default=None, description="plivo | vobiz | enablex (engine default if omitted)")
    agent: Union[AgentRef, AgentConfig]
    variables: dict[str, Any] = Field(default_factory=dict, description="{{template}} values for prompts")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Echoed back on every webhook")
    webhook_url: Optional[str] = None
    webhook_secret: Optional[str] = Field(default=None, description="Per-call HMAC secret (engine default if omitted)")
    record: Optional[bool] = None
    max_duration_s: Optional[int] = Field(default=None, ge=10, le=3600)

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def _normalise(self) -> "CallCreate":
        self.to = self.to.strip()
        return self


class TurnMetrics(BaseModel):
    """One agent turn, all values in seconds (None when the stage does not apply, e.g. S2S)."""
    turn: int
    eou_delay: Optional[float] = Field(default=None, description="end of user speech -> turn committed")
    stt_delay: Optional[float] = Field(default=None, description="final transcript delay")
    llm_ttft: Optional[float] = Field(default=None, description="LLM time to first token")
    tts_ttfb: Optional[float] = Field(default=None, description="TTS time to first byte")
    total: Optional[float] = Field(default=None, description="end of user speech -> first agent audio")
    at: float = Field(default=0.0, description="seconds since call start")


class CallMetrics(BaseModel):
    turns: list[TurnMetrics] = Field(default_factory=list)
    total_p50: Optional[float] = None
    total_p95: Optional[float] = None
    llm_ttft_p50: Optional[float] = None
    tts_ttfb_p50: Optional[float] = None
    eou_p50: Optional[float] = None
    pickup_to_first_word: Optional[float] = None
    usage: dict[str, Any] = Field(default_factory=dict, description="tokens / characters / seconds per provider")

    def recompute(self) -> "CallMetrics":
        def pct(values: list[float], p: float) -> Optional[float]:
            vals = sorted(v for v in values if v is not None)
            if not vals:
                return None
            k = max(0, min(len(vals) - 1, int(round((p / 100.0) * (len(vals) - 1)))))
            return round(vals[k], 3)

        self.total_p50 = pct([t.total for t in self.turns], 50)
        self.total_p95 = pct([t.total for t in self.turns], 95)
        self.llm_ttft_p50 = pct([t.llm_ttft for t in self.turns], 50)
        self.tts_ttfb_p50 = pct([t.tts_ttfb for t in self.turns], 50)
        self.eou_p50 = pct([t.eou_delay for t in self.turns], 50)
        return self


class TranscriptTurn(BaseModel):
    role: str = Field(..., description="user | agent | tool | system")
    text: str
    at: float = Field(default=0.0, description="seconds since call start")
    node: Optional[str] = None
    interrupted: bool = False
    extra: dict[str, Any] = Field(default_factory=dict)


class CallRecord(BaseModel):
    id: str
    status: CallStatus
    direction: Direction
    provider: str
    to: str
    from_number: Optional[str] = Field(default=None, alias="from")
    provider_call_id: Optional[str] = None
    agent_id: Optional[str] = None
    agent_version: Optional[int] = None
    created_at: str
    answered_at: Optional[str] = None
    ended_at: Optional[str] = None
    duration_s: Optional[float] = None
    end_reason: Optional[str] = None
    hangup_cause: Optional[str] = None
    outcome: dict[str, Any] = Field(default_factory=dict, description="record_outcome tool payloads, extraction vars")
    variables: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    transcript: list[TranscriptTurn] = Field(default_factory=list)
    metrics: CallMetrics = Field(default_factory=CallMetrics)
    recording_url: Optional[str] = None
    cost_usd: Optional[float] = None
    error: Optional[str] = None

    model_config = {"populate_by_name": True}


class CallCreated(BaseModel):
    call_id: str
    status: CallStatus
    provider: str
    provider_call_id: Optional[str] = None


class Capacity(BaseModel):
    active_calls: int
    max_concurrent_calls: int
    available: int
    accepting: bool
