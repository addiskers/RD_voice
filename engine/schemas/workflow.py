"""Workflow JSON contract (builder -> engine).

Field names deliberately match Dograh's ReactFlow DTO (BSD-2) so a builder UI ported from it
emits this schema unchanged: node types ``startCall`` / ``agentNode`` / ``globalNode`` /
``endCall``; node data with ``prompt``, ``allow_interrupt``, ``add_global_prompt``, extraction
variables, ``tool_uuids`` / ``document_uuids``, greeting; edges with ``label`` (-> tool name),
``condition`` (-> tool description) and ``transition_speech``.

Structural validation (one start node, at most one global node, end nodes without outgoing
edges, edge endpoints exist, unique transition names) lives in ``engine.worker.graph.validate``.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator


class NodeType(str, Enum):
    startCall = "startCall"
    agentNode = "agentNode"
    globalNode = "globalNode"
    endCall = "endCall"


class VariableType(str, Enum):
    string = "string"
    number = "number"
    boolean = "boolean"


class ExtractionVariable(BaseModel):
    name: str = Field(..., min_length=1, description="snake_case identifier used downstream")
    type: VariableType = VariableType.string
    prompt: Optional[str] = Field(default=None, description="Per-variable hint describing what to look for")


class NodeData(BaseModel):
    """Union of the per-type node data; type-specific rules are enforced in validation."""

    name: str = Field(..., min_length=1)
    prompt: Optional[str] = Field(default=None, description="System prompt for this node; supports {{vars}}")
    allow_interrupt: bool = True
    add_global_prompt: bool = True

    extraction_enabled: bool = False
    extraction_prompt: Optional[str] = None
    extraction_variables: Optional[list[ExtractionVariable]] = None

    tool_uuids: Optional[list[str]] = Field(default=None, description="Tool ids this node may invoke")
    document_uuids: Optional[list[str]] = Field(default=None, description="Knowledge documents this node may use")

    # startCall: spoken verbatim at call start. endCall: a fixed closing line spoken without an LLM call.
    greeting: Optional[str] = Field(default=None, description="Spoken verbatim (start greeting / fixed closing); supports {{vars}}")
    # endCall only: outcome fields the engine records on entering the node (e.g. {"status": "no"}); values may use
    # {{vars}} including extracted variables — replaces a record_outcome tool call on the closing path
    outcome: Optional[dict[str, Any]] = None
    greeting_type: Optional[str] = Field(default="text", description="'text' (TTS) or 'audio' (recording id)")
    greeting_recording_id: Optional[str] = None
    delayed_start: bool = False
    delayed_start_duration: Optional[float] = Field(default=None, ge=0.0, le=10.0)

    # legacy flags from Dograh's DTO; ignored (type decides start/end) but accepted
    is_start: bool = False
    is_end: bool = False


class Position(BaseModel):
    x: float = 0.0
    y: float = 0.0


class Node(BaseModel):
    id: str = Field(..., min_length=1)
    type: NodeType
    data: NodeData
    position: Optional[Position] = None


class EdgeData(BaseModel):
    label: str = Field(..., min_length=1, description="Becomes the transition tool name")
    condition: str = Field(..., min_length=1, description="When to take this edge; becomes the tool description")
    transition_speech: Optional[str] = Field(default=None, description="Spoken verbatim while switching nodes")
    transition_speech_type: Optional[str] = "text"
    transition_speech_recording_id: Optional[str] = None

    def function_name(self) -> str:
        """Dograh-compatible tool name derived from the label (a-z, 0-9, '_')."""
        name = re.sub(r"[^a-z0-9]+", "_", self.label.strip().lower()).strip("_")
        if not name:
            name = "transition"
        if name[0].isdigit():
            name = f"go_{name}"
        return name[:60]


class Edge(BaseModel):
    id: str = Field(..., min_length=1)
    source: str
    target: str
    data: EdgeData


class Workflow(BaseModel):
    nodes: list[Node]
    edges: list[Edge]
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _referential_integrity(self) -> "Workflow":
        ids = [n.id for n in self.nodes]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate node ids")
        id_set = set(ids)
        for e in self.edges:
            if e.source not in id_set:
                raise ValueError(f"edge {e.id}: source node '{e.source}' does not exist")
            if e.target not in id_set:
                raise ValueError(f"edge {e.id}: target node '{e.target}' does not exist")
        return self

    # convenience accessors ---------------------------------------------------------------
    def node(self, node_id: str) -> Node:
        for n in self.nodes:
            if n.id == node_id:
                return n
        raise KeyError(node_id)

    def out_edges(self, node_id: str) -> list[Edge]:
        return [e for e in self.edges if e.source == node_id]

    @property
    def start_node(self) -> Optional[Node]:
        return next((n for n in self.nodes if n.type == NodeType.startCall), None)

    @property
    def global_node(self) -> Optional[Node]:
        return next((n for n in self.nodes if n.type == NodeType.globalNode), None)


# {{ variable }} / {{ variable | default }} placeholders (same pattern as Dograh's template renderer)
TEMPLATE_VAR_PATTERN = re.compile(r"\{\{\s*([^|\s}]+)(?:\s*\|\s*([^:}]+)(?::([^}]+))?)?\s*\}\}")


def extract_template_variables(text: str) -> set[str]:
    """Top-level variable names referenced in ``text`` that have no default."""
    found: set[str] = set()
    for m in TEMPLATE_VAR_PATTERN.finditer(text or ""):
        var = m.group(1).strip()
        if "." in var or m.group(2) is not None:
            continue
        found.add(var)
    return found


def render_template(text: str, variables: dict[str, Any]) -> str:
    """Replace ``{{var}}`` / ``{{var | default}}`` / ``{{a.b}}`` with values; unknown vars become ''."""

    def lookup(path: str) -> Any:
        cur: Any = variables
        for part in path.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return None
        return cur

    def repl(m: re.Match) -> str:
        val = lookup(m.group(1).strip())
        if val is None or val == "":
            default = m.group(2)
            if default is not None:
                d = default.strip()
                if d.lower() in ("default", "fallback") and m.group(3) is not None:
                    return m.group(3).strip()
                return d
            return ""
        return str(val)

    return TEMPLATE_VAR_PATTERN.sub(repl, text or "")


def single_prompt_workflow(instructions: str, greeting: Optional[str] = None,
                           tool_uuids: Optional[list[str]] = None) -> Workflow:
    """A one-node workflow for 'just a prompt + tools' agents (Retell/Vapi style)."""
    return Workflow(
        nodes=[Node(id="start", type=NodeType.startCall,
                    data=NodeData(name="Start", prompt=instructions, greeting=greeting,
                                  tool_uuids=tool_uuids))],
        edges=[],
    )
