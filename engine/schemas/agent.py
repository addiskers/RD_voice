"""Agent configuration contract (platform -> engine).

An agent is: a pipeline (cascaded STT/LLM/TTS or a speech-to-speech realtime model), turn-taking
settings, tools, knowledge documents, and either a single prompt or a workflow graph. Versions are
immutable: a call pins ``agent_id + version`` so builder edits never change a live conversation.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator

from .workflow import Workflow, single_prompt_workflow


class Mode(str, Enum):
    cascaded = "cascaded"   # STT -> LLM -> TTS (text is visible before it is spoken)
    s2s = "s2s"             # speech-to-speech realtime model (e.g. Gemini Live)


class ProviderRef(BaseModel):
    provider: str = Field(..., description="deepgram | sarvam | google | anthropic | openai | groq | cartesia | elevenlabs ...")
    model: Optional[str] = None
    api_key: Optional[str] = Field(default=None, description="BYOK; falls back to the engine's key")
    options: dict[str, Any] = Field(default_factory=dict, description="Provider-specific constructor kwargs")


class VoiceRef(ProviderRef):
    voice: Optional[str] = Field(default=None, description="Voice id / speaker name for TTS or realtime")
    language: Optional[str] = None
    speed: Optional[float] = None


class TurnConfig(BaseModel):
    detector: str = Field(default="multilingual", description="multilingual | english | vad | stt | realtime")
    min_endpointing_delay: float = 0.25          # the semantic detector extends this when the caller is mid-sentence
    max_endpointing_delay: float = 2.0           # hard cap: a caller who pauses this long mid-sentence gets a reply anyway
    allow_interruptions: bool = True
    interruption_mode: str = Field(default="adaptive", description="adaptive | vad")
    min_interruption_duration: float = 0.5
    min_interruption_words: int = 0
    false_interruption_timeout: float = 2.0
    resume_false_interruption: bool = True
    preemptive_generation: bool = True
    preemptive_tts: bool = Field(default=True, description="Also synthesize the speculative reply before the turn is confirmed")
    user_away_timeout: Optional[float] = Field(default=15.0, description="Seconds of silence before a nudge")


class ToolType(str, Enum):
    http = "http"
    mcp = "mcp"
    builtin = "builtin"


class ToolDef(BaseModel):
    id: str = Field(..., min_length=1)
    type: ToolType
    name: str = Field(..., min_length=1, description="Function name the LLM sees (a-z, 0-9, _)")
    description: str = ""
    parameters: dict[str, Any] = Field(default_factory=lambda: {"type": "object", "properties": {}},
                                       description="JSON schema of the arguments")
    # http
    url: Optional[str] = None
    method: str = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    timeout_s: float = 8.0
    speak_while_running: Optional[str] = Field(default=None, description="Filler line spoken during a slow tool")
    # mcp
    mcp_url: Optional[str] = None
    mcp_headers: dict[str, str] = Field(default_factory=dict)
    # builtin: record_outcome | schedule_callback | hold | transfer | end_call
    builtin: Optional[str] = None


class GuardConfig(BaseModel):
    enabled: bool = True
    max_sentences: int = Field(default=4, ge=1, le=12, description="Cap per agent turn (cascaded mode only)")
    single_closing: bool = True
    closing_markers: list[str] = Field(default_factory=lambda: [
        "see you", "look forward", "have a great", "have a good", "thank you for your time",
        "thank you for letting", "thanks for letting", "thank you for confirming", "take care",
        "anything else i can help", "count you in", "goodbye", "bye for now",
    ])
    suppress_repeats: bool = True


class AgentConfig(BaseModel):
    id: Optional[str] = None
    version: Optional[int] = None
    name: str = "agent"
    mode: Mode = Mode.cascaded
    language: str = "en-IN"

    stt: Optional[ProviderRef] = None
    llm: Optional[ProviderRef] = None
    tts: Optional[VoiceRef] = None
    realtime: Optional[VoiceRef] = Field(default=None, description="Speech-to-speech model (mode=s2s)")

    turn: TurnConfig = Field(default_factory=TurnConfig)
    guards: GuardConfig = Field(default_factory=GuardConfig)

    instructions: Optional[str] = Field(default=None, description="Single-prompt agent (no workflow)")
    greeting: Optional[str] = Field(default=None, description="Spoken verbatim at call start (single-prompt agents)")
    workflow: Optional[Workflow] = None
    workflow_mode: str = Field(default="nodes", description="nodes = one agent per step with handoffs (deterministic, "
                               "one extra LLM call per transition); flat = whole graph in one prompt (one LLM call per turn)")

    tools: list[ToolDef] = Field(default_factory=list)
    documents: list[str] = Field(default_factory=list, description="Knowledge document ids attached to the agent")

    max_duration_s: Optional[int] = Field(default=None, ge=10, le=3600)
    record: Optional[bool] = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check(self) -> "AgentConfig":
        if self.mode == Mode.cascaded:
            if self.llm is None:
                raise ValueError("cascaded mode requires 'llm'")
        else:
            if self.realtime is None:
                raise ValueError("s2s mode requires 'realtime'")
        if not self.instructions and self.workflow is None:
            raise ValueError("either 'instructions' or 'workflow' is required")
        return self

    def effective_workflow(self) -> Workflow:
        if self.workflow is not None:
            return self.workflow
        return single_prompt_workflow(self.instructions or "", greeting=self.greeting,
                                      tool_uuids=[t.id for t in self.tools] or None)

    def tool(self, tool_id: str) -> Optional[ToolDef]:
        return next((t for t in self.tools if t.id == tool_id), None)


class AgentRef(BaseModel):
    agent_id: str
    version: Optional[int] = Field(default=None, description="Defaults to the latest version")


class AgentVersionOut(BaseModel):
    agent_id: str
    version: int
    created_at: str
    config: AgentConfig
