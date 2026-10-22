import json
from pathlib import Path

import pytest

from engine.schemas.agent import AgentConfig, Mode, ProviderRef
from engine.schemas.workflow import (Edge, EdgeData, Node, NodeData, NodeType, Workflow, extract_template_variables,
                                     render_template, single_prompt_workflow)
from engine.worker.graph.validate import WorkflowValidationError, compile_workflow, validate_workflow

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


def _wf(nodes, edges):
    return Workflow(nodes=nodes, edges=edges)


def test_example_workflow_validates_and_compiles():
    wf = Workflow.model_validate(json.loads((EXAMPLES / "eo_rsvp_workflow.json").read_text(encoding="utf-8")))
    assert validate_workflow(wf) == []
    graph = compile_workflow(wf)
    assert graph.start_id == "identity"
    assert graph.global_prompt and "EO Gujarat" in graph.global_prompt
    assert {e.function_name for e in graph.out_edges["rsvp"]} == {"said_yes", "said_no_or_callback"}
    assert graph.required_variables >= {"first_name", "event_name", "event_date"}
    assert graph.is_end("closing_yes") and not graph.is_end("kids")


def test_example_agent_config_loads():
    cfg = AgentConfig.model_validate(json.loads((EXAMPLES / "eo_rsvp_agent.json").read_text(encoding="utf-8"))
                                     | {"workflow": json.loads((EXAMPLES / "eo_rsvp_workflow.json").read_text(encoding="utf-8"))})
    assert cfg.mode == Mode.cascaded
    assert cfg.tool("record_outcome").builtin == "record_outcome"
    assert cfg.effective_workflow().start_node.id == "identity"


def test_edge_function_name_is_tool_safe():
    e = EdgeData(label="Said YES!! (clearly)", condition="c")
    assert e.function_name() == "said_yes_clearly"
    assert EdgeData(label="2nd option", condition="c").function_name().startswith("go_")


def test_validation_errors():
    start = Node(id="s", type=NodeType.startCall, data=NodeData(name="s", prompt="p"))
    agent = Node(id="a", type=NodeType.agentNode, data=NodeData(name="a", prompt="p"))
    end = Node(id="e", type=NodeType.endCall, data=NodeData(name="e", prompt="p"))
    # agent node with no incoming edge + end node with an outgoing edge
    wf = _wf([start, agent, end], [Edge(id="x", source="e", target="a", data=EdgeData(label="l", condition="c"))])
    msgs = [str(e) for e in validate_workflow(wf)]
    assert any("endCall cannot have outgoing" in m for m in msgs)
    with pytest.raises(WorkflowValidationError):
        compile_workflow(wf)
    # two start nodes
    wf2 = _wf([start, Node(id="s2", type=NodeType.startCall, data=NodeData(name="s2", prompt="p"))], [])
    assert any("exactly one startCall" in str(e) for e in validate_workflow(wf2))
    # duplicate transition names on one node
    wf3 = _wf([start, agent, end], [
        Edge(id="1", source="s", target="a", data=EdgeData(label="go", condition="c")),
        Edge(id="2", source="s", target="e", data=EdgeData(label="GO", condition="c")),
    ])
    assert any("duplicates edge" in str(e) for e in validate_workflow(wf3))


def test_referential_integrity():
    start = Node(id="s", type=NodeType.startCall, data=NodeData(name="s", prompt="p"))
    with pytest.raises(ValueError):
        _wf([start], [Edge(id="x", source="s", target="missing", data=EdgeData(label="l", condition="c"))])


def test_cycles_are_allowed():
    start = Node(id="s", type=NodeType.startCall, data=NodeData(name="s", prompt="p"))
    a = Node(id="a", type=NodeType.agentNode, data=NodeData(name="a", prompt="p"))
    b = Node(id="b", type=NodeType.agentNode, data=NodeData(name="b", prompt="p"))
    wf = _wf([start, a, b], [
        Edge(id="1", source="s", target="a", data=EdgeData(label="to a", condition="c")),
        Edge(id="2", source="a", target="b", data=EdgeData(label="to b", condition="c")),
        Edge(id="3", source="b", target="a", data=EdgeData(label="back", condition="c")),
    ])
    assert validate_workflow(wf) == []


def test_single_prompt_agent_and_mode_rules():
    cfg = AgentConfig(instructions="Be helpful.", llm=ProviderRef(provider="google", model="gemini-2.5-flash"))
    wf = cfg.effective_workflow()
    assert len(wf.nodes) == 1 and wf.nodes[0].type == NodeType.startCall
    with pytest.raises(ValueError):
        AgentConfig(instructions="x")                       # cascaded needs llm
    with pytest.raises(ValueError):
        AgentConfig(mode=Mode.s2s, instructions="x")        # s2s needs realtime
    with pytest.raises(ValueError):
        AgentConfig(llm=ProviderRef(provider="google"))     # no prompt / workflow


def test_templates():
    assert extract_template_variables("Hi {{first_name}}, {{event.name}} {{x | default:none}}") == {"first_name"}
    assert render_template("Hi {{first_name}}!", {"first_name": "Pratik"}) == "Hi Pratik!"
    assert render_template("{{a.b}} {{missing}} {{v | fallback:zzz}}", {"a": {"b": 1}}) == "1  zzz"
    wf = single_prompt_workflow("p", greeting="Hello {{name}}")
    assert compile_workflow(wf).required_variables == {"name"}
