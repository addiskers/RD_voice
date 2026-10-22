"""Workflow graph runtime on LiveKit agent handoffs.

node   -> ``NodeAgent`` (instructions = global prompt + node prompt rendered with {{vars}}, node tools)
edge   -> a function tool named after the edge label; calling it runs the node's variable
          extraction, speaks the transition text, and hands off to the target NodeAgent
endCall-> the closing reply is generated, then the call is ended
Guards -> in cascaded mode the LLM text is gated sentence by sentence before TTS.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, AsyncIterable, Optional

from livekit.agents import Agent, ChatContext, RunContext, function_tool, llm as lk_llm

from engine.schemas.agent import AgentConfig, Mode
from engine.schemas.workflow import Node, NodeType, render_template
from engine.worker.graph.validate import CompiledGraph
from engine.worker.guards import GuardPolicy, SentenceGate
from engine.worker.tools import CallState

logger = logging.getLogger(__name__)


class GraphRuntime:
    """Holds everything the NodeAgents need; one instance per call."""

    def __init__(self, graph: CompiledGraph, cfg: AgentConfig, state: CallState, tools_by_id: dict[str, Any],
                 report_transition=None):
        self.graph = graph
        self.cfg = cfg
        self.state = state
        self.tools_by_id = tools_by_id
        self.report_transition = report_transition
        self.guard_policy = GuardPolicy.from_config(cfg.guards)
        self.guards_active = cfg.mode == Mode.cascaded and cfg.guards.enabled

    def variables(self) -> dict[str, Any]:
        return {**self.state.variables, **self.state.extracted, "outcome": self.state.outcome}

    def render(self, text: Optional[str]) -> str:
        return render_template(text or "", self.variables())

    LANGUAGE_POLICY = ("Language: mirror the caller. If they speak Hindi or Hinglish, reply in natural Hinglish "
                       "(Hindi words in Devanagari, English terms as they are); if they speak English, reply in "
                       "English. Keep names, dates, times and venue details exactly as given.")

    def instructions_for(self, node: Node) -> str:
        parts: list[str] = []
        if node.data.add_global_prompt and self.graph.global_prompt:
            parts.append(self.render(self.graph.global_prompt))
        parts.append(self.render(node.data.prompt))
        parts.append(self.LANGUAGE_POLICY)
        parts.append("Never invent events, dates, venues, offers or facts that are not in your instructions.")
        if getattr(self.state, "language_hint", ""):
            parts.append(self.state.language_hint)
        if self.graph.out_edges.get(node.id):
            names = ", ".join(e.function_name for e in self.graph.out_edges[node.id])
            parts.append(f"When the condition of a transition is met, call exactly one of these tools: {names}. "
                         "Make the call silently: say nothing in that turn (no acknowledgement, no thanks, no "
                         "goodbye) — the next step will speak. If the caller's most recent message ALREADY satisfies "
                         "one of these transitions (for example they already said no, or already confirmed), do not "
                         "ask again and do not reply: call that transition immediately.")
        if node.type == NodeType.endCall:
            parts.append("This is the closing step. Say one short closing line only, then stop.")
        return "\n\n".join(p for p in parts if p)

    def tools_for(self, node: Node) -> list[Any]:
        ids = list(node.data.tool_uuids or [])
        tools = [self.tools_by_id[i] for i in ids if i in self.tools_by_id]
        for ce in self.graph.out_edges.get(node.id, []):
            tools.append(self._transition_tool(node, ce))
        return tools

    # ---------------------------------------------------------------- flattened form (S2S models)
    def ordered_nodes(self) -> list[Node]:
        """Start node first, then breadth-first along edges, then any unreachable nodes."""
        seen: list[str] = []
        queue = [self.graph.start_id]
        while queue:
            nid = queue.pop(0)
            if nid in seen:
                continue
            seen.append(nid)
            queue.extend(ce.target for ce in self.graph.out_edges.get(nid, []))
        for n in self.graph.workflow.nodes:
            if n.id not in seen and n.type != NodeType.globalNode:
                seen.append(n.id)
        return [self.graph.nodes[i] for i in seen]

    def flatten_instructions(self) -> str:
        """One prompt for the whole graph, for speech-to-speech models that cannot switch
        instructions mid-session (per-step handoffs are replaced by explicit step guidance)."""
        parts: list[str] = []
        if self.graph.global_prompt:
            parts.append(self.render(self.graph.global_prompt))
        parts.append(self.LANGUAGE_POLICY)
        parts.append("Never invent events, dates, venues, offers or facts that are not in your instructions.")
        nodes = self.ordered_nodes()
        if len(nodes) > 1:
            parts.append("You run a multi-step conversation. Follow the steps in order and move to the next step "
                         "only when its condition is met; if the caller's answer already decides a later step, skip "
                         "ahead. Never mention step names or numbers to the caller. Ask each question at most once; "
                         "once the caller has answered, do not confirm or re-ask it, move on. Offer nothing that the "
                         "steps do not mention (no callbacks, alternatives or extra help) unless the caller asks.")
        finish_names = [self.finish_tool_name(n) for n in nodes if n.type == NodeType.endCall and n.data.outcome]
        if finish_names:
            parts.append("CALL RULE: you cannot end the call by yourself. The call ends only when you call one of "
                         f"these tools: {', '.join(finish_names)}. At a closing step, call its tool FIRST, then say "
                         "the closing line. Never say goodbye without calling the tool.")
        for i, n in enumerate(nodes, 1):
            title = f"STEP {i} — {n.data.name}" + (" (closing step)" if n.type == NodeType.endCall else "")
            body = self.render(n.data.prompt).strip()
            nxt = [f"- If {ce.edge.data.condition.strip()} → go to '{self.graph.node(ce.target).data.name}'."
                   for ce in self.graph.out_edges.get(n.id, [])]
            block = title + "\n" + body
            if nxt:
                block += "\nNext:\n" + "\n".join(nxt)
            if n.type == NodeType.endCall:
                if n.data.outcome:
                    block += (f"\nFIRST call the {self.finish_tool_name(n)} tool (it records the outcome and ends "
                              "the call once you finish speaking), THEN say the closing line.")
                else:
                    block += "\nAfter the closing line, call the end_call tool."
            vars_ = n.data.extraction_variables or []
            if vars_:
                block += ("\nCapture with record_outcome when known: "
                          + ", ".join(f"{v.name} ({v.prompt or v.type.value})" for v in vars_))
            parts.append(block)
        parts.append(self.LANGUAGE_POLICY)   # repeated last: recency matters for speech-to-speech models
        return "\n\n".join(p for p in parts if p)

    @staticmethod
    def finish_tool_name(node: Node) -> str:
        return "finish_" + re.sub(r"[^a-z0-9_]+", "_", node.id.lower()).strip("_")

    def _finish_tool(self, node: Node):
        """Flat mode stand-in for entering an endCall node: records the node's declared outcome and ends the
        call after the current speech, so speech-to-speech models need exactly one tool call to close."""
        runtime = self
        schema = {"name": self.finish_tool_name(node),
                  "description": f"Closing step '{node.data.name}': records outcome {json.dumps(node.data.outcome)} "
                                 "and ends the call after your closing line. Call it once, at this step only.",
                  "parameters": {"type": "object", "properties": {}}}

        @function_tool(raw_schema=schema)
        async def _finish(raw_arguments: dict[str, Any], context: RunContext[CallState]):
            import asyncio
            runtime.state.outcome.update({k: runtime.render(v) if isinstance(v, str) else v
                                          for k, v in (node.data.outcome or {}).items()})
            runtime.state.node_id = node.id
            if runtime.report_transition:
                try:
                    await runtime.report_transition({"from": None, "to": node.id, "via": schema["name"],
                                                     "extracted": dict(runtime.state.extracted)})
                except Exception:
                    pass

            from engine.worker.tools import end_after_agent_speech
            # the closing line is spoken AFTER this tool returns (a new generation on S2S models): end the
            # call when that line has played out, not when the tool-call speech handle completes
            end_after_agent_speech(context.session, runtime.state, "workflow_end")
            return "Outcome recorded. Say the closing line now (one sentence) and nothing after it."

        return _finish

    def all_node_tools(self) -> list[Any]:
        ids: list[str] = []
        for n in self.graph.workflow.nodes:
            for t in n.data.tool_uuids or []:
                if t not in ids:
                    ids.append(t)
        tools = [self.tools_by_id[i] for i in ids if i in self.tools_by_id]
        for n in self.graph.workflow.nodes:
            if n.type == NodeType.endCall and n.data.outcome:
                tools.append(self._finish_tool(n))
        if not any(getattr(getattr(t, "info", None), "name", "") == "end_call" for t in tools):
            from engine.schemas.agent import ToolDef, ToolType
            from engine.worker.tools import BUILTINS
            tools.append(BUILTINS["end_call"](ToolDef(id="end_call", type=ToolType.builtin, builtin="end_call",
                                                      name="end_call",
                                                      description="End the call after the closing line.")))
        return tools

    def start_agent(self, session: Any) -> Agent:
        caps = realtime_caps(session)
        if caps is not None and not caps.mutable_instructions:
            logger.info("realtime model cannot change instructions mid-session: running the workflow flattened")
            self._install_closing_net(session)
            return FlatAgent(self)
        if getattr(self.cfg, "workflow_mode", "nodes") == "flat":
            logger.info("workflow_mode=flat: one prompt, one LLM call per turn")
            self._install_closing_net(session)
            return FlatAgent(self)
        return NodeAgent(self, self.graph.start_id)

    closing_grace_s = 3.0

    def _install_closing_net(self, session: Any) -> None:
        """Flat/S2S safety net: models sometimes say the goodbye but skip the finish tool (seen with
        gemini-3.1-flash-live-preview on a Hindi call). When an agent line matches a closing marker and no
        finish tool fires, the call ends after the line plays out plus a grace period, and the outcome is
        extracted from the transcript at the end."""
        import asyncio
        rt = self
        pending: dict[str, Any] = {"task": None}

        def cancel() -> None:
            t = pending.get("task")
            if t is not None and not t.done():
                t.cancel()
            pending["task"] = None

        async def _net() -> None:
            try:
                for _ in range(600):                      # let the closing line finish playing (≤ 60 s)
                    if getattr(session, "agent_state", "") != "speaking":
                        break
                    await asyncio.sleep(0.1)
                await asyncio.sleep(rt.closing_grace_s)
                if rt.state.end_requested or getattr(session, "user_state", "") == "speaking":
                    return
                logger.info("closing line spoken without a finish tool: ending the call (closing_detected)")
                rt.state.closing_detected = True
                await rt.state.end("closing_detected")
            except asyncio.CancelledError:
                pass

        @session.on("conversation_item_added")
        def _item(ev):
            item = ev.item
            if not isinstance(item, lk_llm.ChatMessage):
                return
            if item.role == "user":
                cancel()                                  # the caller kept talking: not a closing after all
                return
            if item.role != "assistant" or rt.state.end_requested:
                return
            if rt.guard_policy.is_closing(item.text_content or ""):
                cancel()
                pending["task"] = asyncio.create_task(_net())

    async def extract_all(self, chat_ctx: Optional[ChatContext], session: Any) -> None:
        """Post-call extraction for every node that declares variables (used when a flat/S2S call ended
        without its finish tool, so the record still carries the caller's answer)."""

        class _Ctx:
            def __init__(self, s, u):
                self.session = s
                self.userdata = u

        for node in self.graph.workflow.nodes:
            if node.data.extraction_enabled and node.data.extraction_variables:
                try:
                    await self.extract(node, chat_ctx, _Ctx(session, self.state))  # type: ignore[arg-type]
                except Exception as e:
                    logger.warning("post-call extraction failed on %s: %s", node.id, e)

    def _transition_tool(self, node: Node, ce):
        runtime = self
        target_id = ce.target
        speech = ce.edge.data.transition_speech
        schema = {"name": ce.function_name, "description": ce.edge.data.condition,
                  "parameters": {"type": "object", "properties": {}}}

        @function_tool(raw_schema=schema)
        async def _transition(raw_arguments: dict[str, Any], context: RunContext[CallState]):
            agent = context.session.current_agent
            chat_ctx = agent.chat_ctx if isinstance(agent, Agent) else None
            if node.data.extraction_enabled and node.data.extraction_variables and chat_ctx is not None:
                # extraction is an extra LLM call: keep it off the reply path unless the next node's text
                # needs the extracted values right away
                import asyncio
                target = runtime.graph.node(target_id)
                texts = [target.data.prompt or "", target.data.greeting or "", json.dumps(target.data.outcome or {})]
                needs_now = any(("{{" + v.name) in t or ("{{ " + v.name) in t
                                for v in node.data.extraction_variables for t in texts)
                coro = runtime.extract(node, chat_ctx, context)
                if needs_now:
                    await coro
                else:
                    runtime.__dict__.setdefault("_bg_tasks", []).append(asyncio.ensure_future(coro))
            if speech:
                try:
                    await speak(context.session, runtime.render(speech))
                except Exception as e:
                    logger.warning("transition speech failed: %s", e)
            if runtime.report_transition:
                try:
                    await runtime.report_transition({"from": node.id, "to": target_id, "via": ce.function_name,
                                                     "extracted": dict(runtime.state.extracted)})
                except Exception:
                    pass
            runtime.state.node_id = target_id
            # bare handoff: LiveKit then expects no tool reply, so the ONLY generation is the new node's
            # on_enter (returning a message here made LiveKit generate twice and Gemini answered one of
            # them with an empty candidate -> "no response generated" retries, 8 s gaps, failed records)
            return NodeAgent(runtime, target_id, chat_ctx=agent.chat_ctx if isinstance(agent, Agent) else None)

        return _transition

    async def extract(self, node: Node, chat_ctx: Optional[ChatContext], context: RunContext[CallState]) -> None:
        """Structured extraction of the node's variables from the conversation so far (cascaded only)."""
        if not node.data.extraction_enabled or not node.data.extraction_variables or chat_ctx is None:
            return
        activity_llm = None
        try:
            activity_llm = context.session.current_agent.llm if hasattr(context.session.current_agent, "llm") else None
        except Exception:
            activity_llm = None
        if activity_llm is None or not isinstance(activity_llm, lk_llm.LLM):
            # speech-to-speech sessions have no text LLM: use the engine's default text model when wired
            activity_llm = getattr(self, "extraction_llm", None)
        if activity_llm is None or not isinstance(activity_llm, lk_llm.LLM):
            return
        convo = []
        for item in chat_ctx.items[-30:]:
            if isinstance(item, lk_llm.ChatMessage) and item.role in ("user", "assistant") and item.text_content:
                convo.append(f"{'Caller' if item.role == 'user' else 'Agent'}: {item.text_content}")
        spec = "\n".join(f"- {v.name} ({v.type.value}): {v.prompt or ''}" for v in node.data.extraction_variables)
        prompt = (f"{node.data.extraction_prompt or 'Extract the following variables from the conversation.'}\n"
                  f"Variables:\n{spec}\n\nReturn ONLY a JSON object with these keys; use null when unknown.")
        ctx = ChatContext.empty()
        ctx.add_message(role="system", content=prompt)
        ctx.add_message(role="user", content="Conversation:\n" + "\n".join(convo))
        try:
            text = ""
            async with activity_llm.chat(chat_ctx=ctx) as stream:
                async for chunk in stream:
                    if isinstance(chunk, str):
                        text += chunk
                    elif getattr(chunk, "delta", None) and chunk.delta.content:
                        text += chunk.delta.content
            data = _parse_json(text)
            for v in node.data.extraction_variables:
                if v.name in data and data[v.name] is not None:
                    self.state.extracted[v.name] = data[v.name]
            self.state.outcome.setdefault("extracted", {}).update(
                {k: self.state.extracted[k] for k in (v.name for v in node.data.extraction_variables)
                 if k in self.state.extracted})
        except Exception as e:
            logger.warning("extraction failed on node %s: %s", node.id, e)


def is_realtime(session: Any) -> bool:
    try:
        return isinstance(session.llm, lk_llm.RealtimeModel)
    except Exception:
        return False


def realtime_caps(session: Any):
    try:
        return session.llm.capabilities if is_realtime(session) else None
    except Exception:
        return None


def realtime_session(session: Any):
    try:
        return session.current_agent._get_activity_or_raise().realtime_llm_session
    except Exception:
        return None


async def trigger_text(session: Any, text: str, *, wait_s: float = 6.0) -> bool:
    """Make a speech-to-speech model speak now by sending a completed user text turn on the live
    Gemini session (the same 'speak first' trigger the EO stack uses with gemini-3.1-flash-live-preview).
    Needed for models whose chat context is immutable mid-session, where ``generate_reply`` is refused.
    Sent directly on the provider session: content queued through the plugin before the connection
    settles is silently ignored by the model (verified 2026-09-07)."""
    import asyncio

    rt = realtime_session(session)
    if rt is None:
        logger.warning("realtime trigger skipped: no realtime session yet")
        return False
    active = None
    deadline = asyncio.get_event_loop().time() + wait_s
    while asyncio.get_event_loop().time() < deadline:
        active = getattr(rt, "_active_session", None)
        if active is not None:
            break
        await asyncio.sleep(0.05)
    if active is None:
        logger.warning("realtime trigger skipped: provider session not connected within %.1fs", wait_s)
        return False
    await asyncio.sleep(0.1)
    try:
        from google.genai import types

        await active.send_client_content(
            turns=[types.Content(role="user", parts=[types.Part(text=text)])], turn_complete=True)
        logger.info("realtime trigger sent (%d chars)", len(text))
        return True
    except Exception as e:
        logger.warning("realtime trigger failed: %s", e)
        return False


async def speak(session: Any, text: str, *, allow_interruptions: bool = True):
    """Speak ``text`` verbatim. Cascaded mode uses TTS (``say``); speech-to-speech models cannot
    synthesize arbitrary text, so the line is requested through the model instead."""
    text = (text or "").strip()
    if not text:
        return None
    caps = realtime_caps(session)
    if caps is not None:
        line = f"Say exactly this to the caller, nothing more: \"{text}\""
        if caps.mutable_chat_context:
            return session.generate_reply(instructions=line, allow_interruptions=allow_interruptions)
        await trigger_text(session, f"[System: {line}]")
        return None
    return session.say(text, allow_interruptions=allow_interruptions)


async def reply_now(session: Any, instructions: Optional[str] = None, *, allow_interruptions: bool = True):
    """Ask the model to take a turn now (agent speaks first / after a step change)."""
    caps = realtime_caps(session)
    if caps is not None and not caps.mutable_chat_context:
        await trigger_text(session, "[System: the call is connected. Speak now, following your instructions for "
                           "this step. " + (instructions or "") + "]")
        return None
    if instructions:
        return session.generate_reply(instructions=instructions, allow_interruptions=allow_interruptions)
    return session.generate_reply(allow_interruptions=allow_interruptions)


def _parse_json(text: str) -> dict[str, Any]:
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        return {}
    try:
        data = json.loads(m.group(0))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


_DEVANAGARI = re.compile(r"[ऀ-ॿ]")
_GUJARATI = re.compile(r"[઀-૿]")


def language_hint(text: str) -> str:
    """Per-turn language nudge derived from the caller's transcript script (empty for Latin-only text)."""
    if _DEVANAGARI.search(text):
        return ("The caller speaks Hindi: anything you say must be natural Hinglish in Devanagari script (names, brand "
                "names and technical terms stay in English). This does not require speaking: when a transition tool "
                "applies, call it silently.")
    if _GUJARATI.search(text):
        return ("The caller speaks Gujarati: anything you say must be natural Gujarati (names and brand names stay in "
                "English). This does not require speaking: when a transition tool applies, call it silently.")
    return ""


class GuardedAgentMixin:
    """Shared by NodeAgent and FlatAgent: text guards before TTS + closing bookkeeping."""

    runtime: GraphRuntime

    async def on_user_turn_completed(self, turn_ctx: ChatContext, new_message: lk_llm.ChatMessage) -> None:
        self.runtime.state.prev_turn_closing = False      # the caller spoke: a fresh closing is allowed again
        hint = language_hint(new_message.text_content or "")
        if hint:  # deterministic nudge: small models sometimes ignore the standing language policy
            turn_ctx.add_message(role="system", content=hint)
            # also remembered on the call so a step entered by handoff right after this message inherits it
            # (turn_ctx is a per-generation copy; the next node's on_enter reply never sees it otherwise)
            setattr(self.runtime.state, "language_hint", hint)
        # knowledge retrieval hook: documents attached to the node/agent are searched and injected before
        # the LLM answers (retriever wired in a later increment)
        retriever = getattr(self.runtime, "retriever", None)
        node = getattr(self, "node", None)
        docs = list(node.data.document_uuids) if node is not None and node.data.document_uuids else \
            list(self.runtime.cfg.documents)
        if retriever and docs:
            try:
                snippets = await retriever(new_message.text_content or "", docs)
                if snippets:
                    turn_ctx.add_message(role="system", content="Relevant knowledge:\n" + "\n".join(snippets))
            except Exception as e:
                logger.warning("retrieval failed: %s", e)

    async def llm_node(self, chat_ctx: ChatContext, tools: list[Any], model_settings: Any):
        rt = self.runtime
        if not rt.guards_active:
            return Agent.default.llm_node(self, chat_ctx, tools, model_settings)  # type: ignore[arg-type]
        return self._guarded(Agent.default.llm_node(self, chat_ctx, tools, model_settings))  # type: ignore[arg-type]

    async def _guarded(self, source: AsyncIterable[Any]):
        gate = SentenceGate(self.runtime.guard_policy)
        # cross-turn: if the previous agent turn (with no caller speech in between, e.g. the turn that
        # triggered a handoff into a closing node) already delivered a closing line, drop another one
        if getattr(self.runtime.state, "prev_turn_closing", False):
            gate.state.closing_said = True
        async for chunk in source:
            if isinstance(chunk, str):
                for s in gate.feed(chunk):
                    yield s + " "
                continue
            delta = getattr(chunk, "delta", None)
            if delta is not None and delta.content:
                kept = " ".join(gate.feed(delta.content))
                delta.content = (kept + " ") if kept else ""
            yield chunk
        for s in gate.flush():
            yield s
        spoke_closing = gate.state.closing_said and any(self.runtime.guard_policy.is_closing(s) for s in gate.state.spoken)
        self.runtime.state.prev_turn_closing = bool(spoke_closing or getattr(self.runtime.state, "prev_turn_closing", False)
                                                    and not gate.state.spoken)
        if gate.dropped:
            logger.info("guards dropped %d sentence(s): %s", len(gate.dropped), gate.dropped)
        # flat mode has no end nodes: once an outcome is recorded and a closing line was spoken, end the call
        # after playout even if the model forgets to call end_call
        if getattr(self, "flat", False) and spoke_closing and self.runtime.state.outcome.get("status") \
                and not self.runtime.state.end_requested:
            import asyncio
            asyncio.get_event_loop().create_task(self.runtime.state.end("workflow_end"))


class NodeAgent(GuardedAgentMixin, Agent):
    def __init__(self, runtime: GraphRuntime, node_id: str, chat_ctx: Optional[ChatContext] = None):
        self.runtime = runtime
        self.node = runtime.graph.node(node_id)
        kwargs: dict[str, Any] = {}
        if chat_ctx is not None:
            kwargs["chat_ctx"] = chat_ctx
        if not self.node.data.allow_interrupt and self.node.type != NodeType.endCall:
            kwargs["turn_handling"] = {"interruption": {"enabled": False}}
        super().__init__(id=f"node:{node_id}", instructions=runtime.instructions_for(self.node),
                         tools=runtime.tools_for(self.node), **kwargs)

    async def on_enter(self) -> None:
        rt = self.runtime
        rt.state.node_id = self.node.id
        if self.node.type == NodeType.endCall and self.node.data.outcome:
            # deterministic outcome declared on the end node: recorded by the engine, no tool round-trip
            rt.state.outcome.update({k: rt.render(v) if isinstance(v, str) else v
                                     for k, v in self.node.data.outcome.items()})
        if self.node.type == NodeType.endCall and (self.node.data.greeting or "").strip():
            # fixed closing line: no LLM round-trip, the call ends right after it is spoken
            h = await speak(self.session, rt.render(self.node.data.greeting), allow_interruptions=False)
            if h is not None:
                await h
            await rt.extract_end(self)
            await rt.state.end("workflow_end")
            return
        if self.node.type == NodeType.startCall:
            greeting = rt.render(self.node.data.greeting) if self.node.data.greeting else ""
            if self.node.data.delayed_start and self.node.data.delayed_start_duration:
                import asyncio
                await asyncio.sleep(min(10.0, float(self.node.data.delayed_start_duration)))
            if greeting:
                await speak(self.session, greeting, allow_interruptions=self.node.data.allow_interrupt)
                return
        handle = await reply_now(self.session, allow_interruptions=self.node.data.allow_interrupt
                                 if self.node.type != NodeType.endCall else False)
        if self.node.type == NodeType.endCall:
            if handle is not None:
                await handle
            await rt.extract_end(self)
            await rt.state.end("workflow_end")


class FlatAgent(GuardedAgentMixin, Agent):
    """The whole workflow as one agent: speech-to-speech models with immutable instructions, or
    ``workflow_mode="flat"`` on the cascaded pipeline (one LLM call per turn)."""

    flat = True

    def __init__(self, runtime: GraphRuntime):
        self.runtime = runtime
        self.node = runtime.graph.node(runtime.graph.start_id)
        super().__init__(id="flat", instructions=runtime.flatten_instructions(), tools=runtime.all_node_tools())

    async def on_enter(self) -> None:
        rt = self.runtime
        rt.state.node_id = self.node.id
        if self.node.data.delayed_start and self.node.data.delayed_start_duration:
            import asyncio
            await asyncio.sleep(min(10.0, float(self.node.data.delayed_start_duration)))
        greeting = rt.render(self.node.data.greeting) if self.node.data.greeting else ""
        if greeting:
            await speak(self.session, greeting, allow_interruptions=self.node.data.allow_interrupt)
        else:
            await reply_now(self.session, allow_interruptions=self.node.data.allow_interrupt)


async def _noop(*_a, **_k):
    return None


# endCall nodes may declare extraction too; run it after the closing line
async def _extract_end(self: GraphRuntime, agent: NodeAgent) -> None:
    import asyncio
    pending = [t for t in self.__dict__.get("_bg_tasks", []) if not t.done()]
    if pending:  # background extractions from earlier transitions must land before the record is finalized
        try:
            await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), 8)
        except Exception as e:
            logger.warning("background extraction did not finish: %s", e)
    if not agent.node.data.extraction_enabled:
        return

    class _Ctx:  # minimal RunContext stand-in for extraction
        def __init__(self, session, userdata):
            self.session = session
            self.userdata = userdata

    try:
        await self.extract(agent.node, agent.chat_ctx, _Ctx(agent.session, self.state))  # type: ignore[arg-type]
    except Exception as e:
        logger.warning("end-node extraction failed: %s", e)


GraphRuntime.extract_end = _extract_end  # type: ignore[attr-defined]
