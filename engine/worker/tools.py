"""Tools available to agents: HTTP tools, MCP servers and the engine built-ins.

Every tool is a LiveKit ``function_tool`` built from a ``ToolDef`` (JSON schema) at call start.
Business outcomes never live in the engine: ``record_outcome`` stores the payload on the call
record and the platform receives it as a ``call.tool`` webhook.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

import httpx
from livekit.agents import RunContext, function_tool

from engine.schemas.agent import ToolDef, ToolType
from engine.schemas.workflow import render_template

logger = logging.getLogger(__name__)

EndRequest = Callable[[str], Awaitable[None]]
ToolReport = Callable[[str, dict[str, Any]], Awaitable[None]]


@dataclass
class CallState:
    """Shared per-call state (``session.userdata``)."""
    call_id: str
    variables: dict[str, Any] = field(default_factory=dict)
    outcome: dict[str, Any] = field(default_factory=dict)
    node_id: Optional[str] = None
    started_at: float = field(default_factory=time.monotonic)
    end_requested: bool = False
    end_reason: Optional[str] = None
    hold_until: float = 0.0
    transfer_to: Optional[str] = None
    extracted: dict[str, Any] = field(default_factory=dict)
    prev_turn_closing: bool = False
    request_end: Optional[EndRequest] = None
    report_tool: Optional[ToolReport] = None

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.started_at

    async def end(self, reason: str) -> None:
        if self.end_requested:
            return
        self.end_requested = True
        self.end_reason = reason
        if self.request_end:
            await self.request_end(reason)

    async def report(self, name: str, data: dict[str, Any]) -> None:
        if self.report_tool:
            try:
                await self.report_tool(name, data)
            except Exception as e:  # never break the conversation on reporting
                logger.warning("tool report failed: %s", e)


def _schema(td: ToolDef) -> dict[str, Any]:
    params = td.parameters or {"type": "object", "properties": {}}
    if "type" not in params:
        params = {"type": "object", **params}
    return {"name": td.name, "description": td.description or td.name, "parameters": params}


# ----------------------------------------------------------------------------- built-ins
def _builtin_record_outcome(td: ToolDef):
    schema = _schema(td)
    if not schema["parameters"].get("properties"):
        schema["parameters"] = {"type": "object", "properties": {"status": {"type": "string"},
                                                                  "note": {"type": "string"}},
                                "required": ["status"]}

    @function_tool(raw_schema=schema)
    async def record_outcome(raw_arguments: dict[str, Any], context: RunContext[CallState]) -> str:
        state = context.userdata
        state.outcome.update({k: v for k, v in raw_arguments.items() if v is not None})
        await state.report(td.name, {"record_outcome": True, "arguments": raw_arguments, "result": dict(state.outcome)})
        return "Recorded. Continue naturally; do not repeat what was just said."

    return record_outcome


def end_after_agent_speech(session: Any, state: "CallState", reason: str, *, wait_start_s: float = 6.0,
                           max_speech_s: float = 40.0) -> None:
    """End the call once the agent's next/current line has finished playing.

    State-based on purpose: with speech-to-speech models the reply that follows a tool call is a NEW
    generation (a new speech handle), so a done-callback on the handle that carried the tool call fires
    before the goodbye is spoken and the closing line gets cut (seen with gemini-3.1-flash-live-preview)."""

    async def _run() -> None:
        loop = asyncio.get_event_loop()
        t0 = loop.time()
        try:
            while loop.time() - t0 < wait_start_s and getattr(session, "agent_state", "") != "speaking":
                if state.end_requested:
                    return
                await asyncio.sleep(0.1)
            while loop.time() - t0 < max_speech_s and getattr(session, "agent_state", "") == "speaking":
                await asyncio.sleep(0.1)
            await asyncio.sleep(0.3)
        except Exception:
            pass
        await state.end(reason)

    asyncio.get_event_loop().create_task(_run())


def _builtin_end_call(td: ToolDef):
    schema = _schema(td)
    schema.setdefault("description", "End the call once the closing line has been spoken.")

    @function_tool(raw_schema=schema)
    async def end_call(raw_arguments: dict[str, Any], context: RunContext[CallState]) -> str:
        state = context.userdata
        reason = str(raw_arguments.get("reason") or "agent_end_call")
        # the goodbye (or the line that called us) finishes playing before we hang up
        end_after_agent_speech(context.session, state, reason)
        await state.report(td.name, {"arguments": raw_arguments})
        return "Say a short goodbye now if you have not already; nothing else."

    return end_call


def _builtin_hold(td: ToolDef):
    schema = _schema(td)
    schema["parameters"] = schema["parameters"] if schema["parameters"].get("properties") else {
        "type": "object", "properties": {"seconds": {"type": "integer", "description": "How long to wait, default 30"}}}

    @function_tool(raw_schema=schema)
    async def hold(raw_arguments: dict[str, Any], context: RunContext[CallState]) -> str:
        secs = min(120, int(raw_arguments.get("seconds") or 30))
        context.userdata.hold_until = time.monotonic() + secs
        return f"Stay silent and wait up to {secs} seconds; do not speak until the caller talks again."

    return hold


def _builtin_schedule_callback(td: ToolDef):
    schema = _schema(td)
    if not schema["parameters"].get("properties"):
        schema["parameters"] = {"type": "object", "properties": {"when": {"type": "string",
                                                                          "description": "Requested time in words or ISO 8601"},
                                                                 "note": {"type": "string"}}, "required": ["when"]}

    @function_tool(raw_schema=schema)
    async def schedule_callback(raw_arguments: dict[str, Any], context: RunContext[CallState]) -> str:
        state = context.userdata
        state.outcome["callback_time"] = raw_arguments.get("when")
        state.outcome.setdefault("status", "callback")
        await state.report(td.name, {"record_outcome": True, "arguments": raw_arguments,
                                     "result": {"callback_time": raw_arguments.get("when"), "status": "callback"}})
        return "Callback noted. Confirm the time briefly."

    return schedule_callback


def _builtin_transfer(td: ToolDef):
    schema = _schema(td)
    if not schema["parameters"].get("properties"):
        schema["parameters"] = {"type": "object", "properties": {"to": {"type": "string"}, "reason": {"type": "string"}}}

    @function_tool(raw_schema=schema)
    async def transfer(raw_arguments: dict[str, Any], context: RunContext[CallState]) -> str:
        state = context.userdata
        state.transfer_to = str(raw_arguments.get("to") or "")
        await state.report(td.name, {"transfer_requested": True, "arguments": raw_arguments})
        return "Tell the caller you are connecting them, then stop speaking."

    return transfer


BUILTINS = {
    "record_outcome": _builtin_record_outcome,
    "end_call": _builtin_end_call,
    "hold": _builtin_hold,
    "schedule_callback": _builtin_schedule_callback,
    "transfer": _builtin_transfer,
}


# ----------------------------------------------------------------------------- HTTP tools
def _http_tool(td: ToolDef, http: httpx.AsyncClient):
    schema = _schema(td)

    @function_tool(raw_schema=schema)
    async def http_tool(raw_arguments: dict[str, Any], context: RunContext[CallState]) -> str:
        state = context.userdata
        tpl_vars = {**state.variables, "call_id": state.call_id, "args": raw_arguments}
        url = render_template(td.url or "", tpl_vars)
        headers = {k: render_template(v, tpl_vars) for k, v in (td.headers or {}).items()}
        if td.speak_while_running:
            try:
                context.session.say(td.speak_while_running, add_to_chat_ctx=False)
            except Exception:
                pass
        payload = {"arguments": raw_arguments, "call_id": state.call_id, "variables": state.variables}
        try:
            if td.method.upper() == "GET":
                resp = await http.get(url, params=raw_arguments, headers=headers, timeout=td.timeout_s)
            else:
                resp = await http.request(td.method.upper(), url, json=payload, headers=headers, timeout=td.timeout_s)
            body = resp.text[:4000]
            await state.report(td.name, {"http": True, "url": url, "status": resp.status_code, "arguments": raw_arguments,
                                         "result": _try_json(body)})
            if resp.status_code >= 400:
                return f"The tool failed with HTTP {resp.status_code}. Tell the caller you could not do that right now."
            return body or "Done."
        except Exception as e:
            logger.warning("http tool %s failed: %s", td.name, e)
            await state.report(td.name, {"http": True, "url": url, "error": str(e), "arguments": raw_arguments})
            return "The tool is unavailable right now. Apologise briefly and continue."

    return http_tool


def _try_json(text: str) -> Any:
    try:
        return json.loads(text)
    except Exception:
        return text


# ----------------------------------------------------------------------------- assembly
def build_tools(defs: list[ToolDef], http: httpx.AsyncClient) -> dict[str, Any]:
    """ToolDef list -> {tool_id: livekit tool}."""
    out: dict[str, Any] = {}
    for td in defs:
        try:
            if td.type == ToolType.builtin:
                factory = BUILTINS.get((td.builtin or td.name).lower())
                if factory is None:
                    logger.warning("unknown builtin tool '%s'", td.builtin)
                    continue
                out[td.id] = factory(td)
            elif td.type == ToolType.http:
                if not td.url:
                    logger.warning("http tool '%s' has no url", td.name)
                    continue
                out[td.id] = _http_tool(td, http)
            elif td.type == ToolType.mcp:
                continue  # MCP servers are attached at the session level (see build_mcp_servers)
        except Exception as e:
            logger.error("failed to build tool %s: %s", td.id, e)
    return out


def build_mcp_servers(defs: list[ToolDef]) -> list[Any]:
    servers: list[Any] = []
    for td in defs:
        if td.type != ToolType.mcp or not td.mcp_url:
            continue
        try:
            from livekit.agents import mcp
            servers.append(mcp.MCPServerHTTP(url=td.mcp_url, headers=td.mcp_headers or None))
        except Exception as e:
            logger.error("failed to build MCP server %s: %s", td.id, e)
    return servers
