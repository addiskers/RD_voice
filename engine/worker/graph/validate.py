"""Structural validation + compilation of a Workflow (rules ported from Dograh's WorkflowGraph).

Rules:
  - exactly one startCall node, at most one globalNode
  - globalNode has no edges; endCall has no outgoing edges; every non-start node has >= 1 incoming
  - startCall has no incoming edges
  - transition tool names derived from edge labels are unique per source node
  - prompted nodes (start/agent/end) need a prompt (globalNode too)
Cycles are ALLOWED (a conversation may loop back), as in Dograh.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Optional

from engine.schemas.workflow import (Edge, Node, NodeType, Workflow, extract_template_variables)


@dataclass
class WorkflowError:
    kind: str            # workflow | node | edge
    id: Optional[str]
    message: str

    def __str__(self) -> str:
        where = f"{self.kind} {self.id}" if self.id else self.kind
        return f"{where}: {self.message}"


class WorkflowValidationError(ValueError):
    def __init__(self, errors: list[WorkflowError]):
        self.errors = errors
        super().__init__("; ".join(str(e) for e in errors))


@dataclass
class CompiledEdge:
    edge: Edge
    function_name: str
    target: str


@dataclass
class CompiledGraph:
    workflow: Workflow
    start_id: str
    global_prompt: Optional[str]
    nodes: dict[str, Node]
    out_edges: dict[str, list[CompiledEdge]] = field(default_factory=dict)
    required_variables: set[str] = field(default_factory=set)

    def node(self, node_id: str) -> Node:
        return self.nodes[node_id]

    def is_end(self, node_id: str) -> bool:
        return self.nodes[node_id].type == NodeType.endCall


def validate_workflow(wf: Workflow) -> list[WorkflowError]:
    errors: list[WorkflowError] = []
    counts = Counter(n.type for n in wf.nodes)

    if counts[NodeType.startCall] != 1:
        errors.append(WorkflowError("workflow", None, "workflow must have exactly one startCall node"))
    if counts[NodeType.globalNode] > 1:
        errors.append(WorkflowError("workflow", None, "workflow can have at most one globalNode"))

    in_deg: Counter = Counter()
    out_deg: Counter = Counter()
    for e in wf.edges:
        out_deg[e.source] += 1
        in_deg[e.target] += 1

    for n in wf.nodes:
        if n.type == NodeType.startCall and in_deg[n.id]:
            errors.append(WorkflowError("node", n.id, "startCall cannot have incoming edges"))
        if n.type == NodeType.globalNode and (in_deg[n.id] or out_deg[n.id]):
            errors.append(WorkflowError("node", n.id, "globalNode cannot have edges"))
        if n.type == NodeType.endCall and out_deg[n.id]:
            errors.append(WorkflowError("node", n.id, "endCall cannot have outgoing edges"))
        if n.type in (NodeType.agentNode, NodeType.endCall) and in_deg[n.id] == 0:
            errors.append(WorkflowError("node", n.id, f"{n.type.value} must have at least one incoming edge"))
        if not (n.data.prompt or "").strip():
            if n.type == NodeType.startCall and (n.data.greeting or "").strip():
                pass  # a greeting-only start node is acceptable
            else:
                errors.append(WorkflowError("node", n.id, "prompt is required"))
        if n.data.extraction_enabled and not n.data.extraction_variables:
            errors.append(WorkflowError("node", n.id, "extraction_enabled but no extraction_variables"))

    # unique transition names per source node
    seen: dict[tuple[str, str], str] = {}
    for e in wf.edges:
        key = (e.source, e.data.function_name())
        if key in seen:
            errors.append(WorkflowError("edge", e.id,
                                        f"transition name '{key[1]}' duplicates edge {seen[key]} on the same node"))
        else:
            seen[key] = e.id
    return errors


def compile_workflow(wf: Workflow) -> CompiledGraph:
    errors = validate_workflow(wf)
    if errors:
        raise WorkflowValidationError(errors)

    nodes = {n.id: n for n in wf.nodes}
    start = wf.start_node
    assert start is not None
    glob = wf.global_node
    graph = CompiledGraph(workflow=wf, start_id=start.id,
                          global_prompt=(glob.data.prompt if glob else None), nodes=nodes)
    for e in wf.edges:
        graph.out_edges.setdefault(e.source, []).append(
            CompiledEdge(edge=e, function_name=e.data.function_name(), target=e.target))

    for n in wf.nodes:
        if n.data.prompt:
            graph.required_variables |= extract_template_variables(n.data.prompt)
        if n.data.greeting:
            graph.required_variables |= extract_template_variables(n.data.greeting)
    for e in wf.edges:
        if e.data.transition_speech:
            graph.required_variables |= extract_template_variables(e.data.transition_speech)
    return graph
