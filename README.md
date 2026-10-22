# gvox-voice — GlobalVox Voice Engine

The agent backend of the GlobalVox voice platform: a carrier's bidirectional `<Stream>` WebSocket
(Plivo, Vobiz, EnableX) lands on a worker that runs the conversation with **LiveKit Agents**
(cascaded STT → LLM → TTS with guards, or a speech-to-speech model), executes the builder's workflow
graph and tools in-process, and reports everything back through a REST API and signed webhooks.
The platform backend never touches telephony: it calls `POST /v1/calls` and listens to webhooks.

```
platform backend ──POST /v1/calls──► api (FastAPI, :8080) ──REST dial──► Plivo / Vobiz / EnableX
                 ◄──signed webhooks──   │ answer/ring/hangup URLs  ◄──────────────┘
                                        │ runtime config / events
                                        ▼
                                 worker (:8090 media WS, :8091 control) ◄── <Stream> WSS (Door A)
                                 LiveKit AgentSession · graph runtime · tools · recording
browser test page ───────────── ws://worker:8091/browser (Transport C, VAD endpointing)
```

## Layout
- `engine/api/` — REST API (`/v1/*`), carrier webhooks (`/carrier/{provider}/answer|ring|hangup`), internal endpoints for the worker, webhook dispatcher (HMAC-SHA256, retries), SQLAlchemy models (SQLite by default, Postgres via `DATABASE_URL`).
- `engine/carriers/` — Plivo (reference), Vobiz (Plivo-compatible), EnableX (stub; verify field names before use).
- `engine/worker/` — `main.py` (agent-transport `AudioStreamServer` entrypoint + control HTTP), `pipeline.py` (AgentConfig → AgentSession, provider maps), `graph/` (workflow validation + runtime on agent handoffs), `tools.py` (HTTP tools, MCP, built-ins), `guards.py` (single-closing / repeat / sentence cap gate before TTS), `events.py` (transcript + per-turn metrics → API), `recording.py` (caller+agent WAV mix), `browser.py` (Transport C), `audio.py` / `stream_protocol.py` (codecs, `<Stream>` frames — fallback transport).
- `engine/schemas/` — the contracts shared with the platform team: `AgentConfig`, Workflow JSON (Dograh-compatible node/edge schema), `CallCreate`/`CallRecord`, webhook events.
- `examples/` — the EO Gujarat RSVP flow as a 5-node graph + agent config. `scripts/` — test call, webhook sink, OpenAPI export.

## Local run (Windows, project venv only)
```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev,plivo-transport]"
copy .env.example .env      # fill keys
.\.venv\Scripts\python.exe -m pytest -q                       # offline tests
.\.venv\Scripts\gvox-api.exe                                   # API on :8080
.\.venv\Scripts\gvox-worker.exe download-files                 # once: VAD + turn-detector models
.\.venv\Scripts\gvox-worker.exe dev                            # media WS :8090, control :8091
```
Browser test (no telephony): connect a WebSocket to `ws://localhost:8091/browser`, send
`{"type":"setup","auth_key":"demo-browser-key","agent":{...},"variables":{"first_name":"Pratik"}}`, then
`{"type":"audio","data":"<base64 PCM16 16 kHz>"}` frames; you receive `audio`, `transcript`, `interrupted`, `turn_complete`.

## Phase-0 runbook (real calls, India)
1. **Host in Mumbai** (GCP `asia-south1`). Two public endpoints: HTTPS for the API (`PUBLIC_HTTP_URL`) and WSS for the
   worker media port (`PUBLIC_WS_URL`, e.g. nginx/Caddy terminating TLS in front of `:8090`; for a quick test
   `cloudflared tunnel --url http://localhost:8080` and a second tunnel for `:8090`).
2. `.env`: carrier creds (`PLIVO_*` and/or `VOBIZ_*`), model keys (Deepgram, Google, Cartesia/ElevenLabs/Sarvam),
   `ENGINE_API_KEY`, `INTERNAL_TOKEN`, `WEBHOOK_SECRET`. Keep `STREAM_PATH_PER_CALL=false` (the agent-transport endpoint
   accepts connections on its base URL; calls are matched by the carrier CallUUID).
3. Start `gvox-api` and `gvox-worker start`. `GET /health` on the API and `GET /healthz` on `:8091` must be 200.
4. Register the agent: `PUT /v1/agents/eo-rsvp` with `examples/eo_rsvp_agent.json` + `"workflow": <examples/eo_rsvp_workflow.json>`
   (or pass it inline per call). `POST /v1/agents/validate` reports the `{{variables}}` the workflow expects.
5. Outbound: `python scripts/place_test_call.py +91XXXXXXXXXX --agent-id eo-rsvp --var first_name=Pratik
   --var event_name="AI First Mindset Workshop" --var event_date="Friday the eleventh" --var event_time="7 pm"
   --var event_venue="DoubleTree" --webhook-url https://<sink>/hook --wait`.
6. Inbound: `PUT /v1/numbers/+91XXXXXXXXXX {"provider":"plivo","agent_id":"eo-rsvp"}` and point the number's Plivo
   Application at the returned `answer_url` (GET) / `hangup_url` (POST).
7. Compare per-turn metrics (`GET /v1/calls/{id}` → `metrics.turns[]`: `eou_delay`, `llm_ttft`, `tts_ttfb`, `total`)
   against the EO stack's `TURN LATENCY` log. Switch `"mode": "s2s"` on the agent to A/B Gemini Live.

## Contract for the platform team
- `POST /v1/calls` (Bearer `ENGINE_API_KEY`) → `{call_id}`; `GET /v1/calls/{id}`; `POST /v1/calls/{id}/hangup|say|transfer`;
  `PUT /v1/agents/{id}` (versioned, immutable per call); `PUT /v1/numbers/{e164}`; `GET /v1/capacity` (pace campaigns on it).
- Webhooks: `call.queued`, `call.ringing`, `call.answered`, `call.turn`, `call.tool` (`record_outcome` payloads live here),
  `call.transition`, `call.ended` / `call.failed` (full record: transcript, metrics, outcome, recording URL).
  Headers `X-GVox-Signature: v1=<hmac-sha256(secret, "<ts>.<body>")>`, `X-GVox-Timestamp`, `X-GVox-Event`, `X-GVox-Delivery`.
  Dedupe on `(call_id, seq)`.
- `python scripts/export_openapi.py openapi.json` produces the OpenAPI document.

## Test console (local UI)
With the API and worker running, open **http://localhost:8080/ui/** (API key `dev-engine-key` in the header box):
- **Talk**: start a browser-mic session with the EO RSVP workflow, a single prompt, or the JSON from the Agent tab;
  live transcript, interruptions, a **live Latency card** (engine-measured end-of-speech → agent audio per reply with
  the STT/LLM/TTS breakdown, session p50/p95, and a client-side stopwatch cross-check), and the call record afterwards.
- **Phone call**: `POST /v1/calls` from the page (needs public URLs; the page warns when `PUBLIC_HTTP_URL` is localhost),
  live status polling, hang up, operator "say".
- **Calls**: recent calls, transcript, per-turn latency table, recording player. **Agent JSON**: validate / save versions.
`python scripts/browser_smoke.py [--example]` is the headless version of the Talk tab (used as the local smoke test).

## Cascaded pipeline (verified locally 2026-09-07)
Default when Deepgram + a TTS key are present: **Deepgram nova-3 (`language=multi`) → Gemini 2.5 Flash → Cartesia sonic-3**,
with **Sarvam bulbul:v3 (speaker `rahul`, en-IN)** selectable as the TTS in the Talk tab (`DEEPGRAM_LANGUAGE`,
`DEFAULT_TTS_*`, `SARVAM_TTS_*` in `.env`). Headless full-loop check:
`python scripts/browser_smoke.py --mode cascaded --tts cartesia|sarvam --speak-wav scripts/assets/user_yes.wav --seconds 26`
(greeting → synthesized caller utterance → STT → LLM reply → TTS; `scripts/make_utterance.py` regenerates the WAV).
Per-turn metrics on the call record: `eou_delay` (end of speech → turn committed), `stt_delay`, `llm_ttft` (absent when
preemptive generation started before the turn ended), `tts_ttfb`, and `total` = end of the caller's speech → agent audio
starts (user-perceived), plus `pickup_to_first_word`.

## Latency (measured from a laptop in India, 2026-09-08; `scripts/latency_bench.py --runs 2`)
`total` = end of the caller's speech → agent audio starts (user-perceived), p50 over the runs; STT column = exact
transcription of the synthesized utterance.

| LLM | STT / turn detection | TTS | total p50 | eou | llm first token | tts first byte |
|---|---|---|---|---|---|---|
| Gemini 2.5 Flash (thinking on, before) | nova-3 + semantic detector | Cartesia | 1.54 s | 0.47 | 1.18 | 0.12 |
| Gemini 2.5 Flash (thinking off) | nova-3 + semantic detector | Cartesia | 1.35 s | 0.47 | 1.09 | 0.10 |
| Gemini 2.5 Flash (thinking off) | Flux end-of-turn | Cartesia | 1.16 s | 0.25 | 1.02 | 0.10 |
| Gemini 2.5 Flash-Lite | nova-3 + semantic detector | Cartesia | 0.95 s | 0.46 | 0.73 | 0.12 |
| **Gemini 2.5 Flash-Lite (default)** | **Flux end-of-turn (default)** | Cartesia | **0.82 s** | 0.25 | 0.67 | 0.11 |
| Groq `openai/gpt-oss-20b` | nova-3 + semantic detector | Cartesia | 1.00 s | 0.46 | 0.71 | 0.11 |
| **Groq `openai/gpt-oss-20b`** | **Flux end-of-turn** | Cartesia | **0.67 s** (p95 0.82) | 0.26 | 0.57 | 0.11 |

Direct first-token from here (`scripts/llm_ttft_check.py`): 2.5 Flash 1.09 s (1.41 with thinking), Flash-Lite 0.71 s,
Groq gpt-oss-20b 0.56 s. Provider round-trips: Deepgram 258 ms, Gemini 298 ms, Groq 303 ms, Cartesia 28 ms,
Sarvam 37 ms. Groq's Llama 3.x endpoints are retired (404); use `openai/gpt-oss-20b` / `-120b`.
Levers in code: `thinking_budget=0` (Gemini 2.5), `min_endpointing_delay` 0.25 s / `max_endpointing_delay` 2.0 s,
`preemptive_tts`, Flux STT → `turn_detection="stt"` with `eot_threshold` 0.6 and `eot_timeout_ms` 1800.
A turn whose endpointing hits the cap means the caller paused mid-sentence (Flux/the detector would not call it
finished); the Latency card labels these "max wait". Live-mic finding 2026-09-08: with Deepgram's default 3 s
timeout such turns measured 3.0 s. Hosting in Mumbai removes most of the ~250–300 ms per hop to Deepgram/Gemini/Groq.
Both fast LLMs run the EO workflow correctly (identity → transition tool → invitation); the reply inside a
multi-node workflow costs one extra LLM round-trip at each transition (1.0 s Groq / 1.55 s Flash-Lite measured).

### Workflow latency rules (from live-mic sessions, 2026-09-08)
- **Every hop is an LLM call.** A "no" said during the invitation that has to travel invite → rsvp → closing costs
  three generations (7.6 s measured). Connect decision-bearing nodes directly to their outcomes (the EO example now has
  invite → kids / closing_no and identity → closing_no edges); the engine also tells each node to transition
  immediately when the caller's last message already decides it.
- **Fixed closings are free.** An `endCall` node with a `greeting` speaks it verbatim (TTS only, ~0.3 s) and ends the
  call; leave it empty when the closing must mirror the caller's language.
- **Record outcomes on the node, not with a tool.** `endCall` nodes take `"outcome": {"status": "no"}`; the engine
  records it on entry (values may use `{{vars}}`), so the closing turn needs no `record_outcome` round-trip.
  Extraction when leaving a node runs in the background unless the next node's prompt uses the extracted variables.
- **Vendor outages don't silence a call.** `TTS_FALLBACK_PROVIDER` (default cartesia) wraps the chosen voice in
  LiveKit's FallbackAdapter; when Sarvam ran out of credits (HTTP 402, 2026-09-08) calls continued on Cartesia after
  a ~3 s first-turn switch. Provider errors reach the test console as a red `provider:` status line.
- **Language mirroring is nudged per turn.** Besides the standing policy, a caller transcript in Devanagari or
  Gujarati script adds a one-line system hint for that turn (`language_hint`), because Flash-Lite occasionally
  answered Hindi in English.
- **Flat mode is not a shortcut.** `workflow_mode: "flat"` (one prompt for the whole graph) measured no faster with
  Flash-Lite: the longer prompt raises first-token latency and tool calls still cost a round-trip each.
- **STT for Hindi/Hinglish**: Flux `flux-general-multi` commits in ~0.25 s on clean audio (1.3 s on a noisy laptop mic);
  Sarvam saarika transcribes Hindi more cleanly but finalises in ~1.5 s. Groq gpt-oss-20b ignored the language policy
  (answered Hindi in English, invented a date) — keep Gemini 2.5 Flash-Lite for Hinglish.

## Which mode to use (measured 2026-09-08, laptop in India, same flows, client-side clock)

| flow / turn | speech-to-speech (gemini-3.1-flash-live) | cascaded (Flux → Flash-Lite → Cartesia) |
|---|---|---|
| English "yes" → invitation reply | 1.6 s | 2.0 s |
| English "no kids" → closing (3 steps away) | 1.2 s | 4.5 s |
| Hindi "yes" → invitation reply | 2.2 s | 1.8–2.7 s |
| Hindi "no" → closing | 2.5 s | 1.4–2.5 s |
| voice quality | native prosody, human-like, Hinglish natural | TTS reads text; flat unless a good voice is picked |
| greeting after connect | 4–5 s (Gemini session setup) | 1.5–3 s |

Recommendation: **speech-to-speech is the primary engine for quality**; the test console defaults to it. The
cascaded pipeline remains for bring-your-own voices, strict per-step control and vendor independence.
Speech-to-speech specifics the engine handles for you: the workflow runs flattened (one prompt, `finish_<node>`
tools record each closing outcome and end the call), a closing-line safety net ends the call when the model says
goodbye without calling its tool (`end_reason=closing_detected`), and the caller's answers are then extracted from
the transcript with the default text model. Known: Gemini's input transcription can mislabel Hindi audio (a Hindi
"no" was transcribed as Japanese while the model still understood it); the record's transcript is for reference.

## Speech-to-speech notes (Gemini Live, verified locally 2026-09-07)
- `gemini-3.1-flash-live-preview` (the EO production model) cannot change instructions/tools mid-session and refuses
  LiveKit's `generate_reply`; the engine therefore runs a workflow **flattened into one agent** on it (all steps in one
  prompt, all tools registered up front) and makes it speak first by sending a completed user text turn directly on the
  Gemini session (`trigger_text`, same trick as EO). Per-node handoffs apply on models with mutable instructions
  (e.g. `gemini-2.5-flash-native-audio-preview-12-2025`) and in cascaded mode.
- The `language` parameter is only passed for 3.1 (2.5 native-audio rejects `en-IN` with API error 1007).
- Cascaded mode needs STT + TTS keys (Deepgram/Sarvam + Cartesia/ElevenLabs/Sarvam); without them the UI defaults to S2S.

## Notes / limits (increment 1)
- Carrier hangup after the agent ends the call is issued by the API (`hangup_carrier` on the `ended` event) and,
  for Plivo, also by agent-transport. Vobiz/EnableX hangups go through the API only.
- Browser transport uses VAD endpointing (the semantic turn detector runs only inside agent-transport sessions).
- EnableX REST field names are unverified; Plivo and Vobiz follow the documented Plivo Voice API shape.
- Knowledge-base retrieval hook exists (`NodeAgent.on_user_turn_completed`) but no vector store is wired yet.
