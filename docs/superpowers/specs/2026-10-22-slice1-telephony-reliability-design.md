# Slice 1: telephony reliability on the carrier WebSocket path

Date: 2026-10-22. Status: draft for review. Scope owner: Aditya.

## Intent

gvox-voice must run the first outbound campaign of about 5,000 calls a day with 20 calls in progress at
once, on the existing cloud vendors and the existing Plivo / Vobiz `<Stream>` WebSocket path, without
dropping live calls on deploys, losing webhooks on restarts, or lying to the dialer about free capacity.
It must also measure itself on the audio a real caller produces (8 kHz G.711), not on the 16 kHz browser path.

This slice is the groundwork for everything that follows (per-language routing, cost ledger, data capture
for own models, LiveKit SIP). It changes no contract the platform team already depends on; every change is
additive.

### Why these and not others

A review on 2026-10-22 found that at this load the engine fails before any vendor does: one worker process
holds every call and is killed instantly on SIGTERM; webhook ordering and retry state live in API memory;
capacity is one static number counted on outbound only; only TTS has a fallback; recordings are a mono mix
held in RAM; the latency bench runs over the browser path. Those are the items here. Multi-worker load
balancing, SIP, self-hosted models and analytics are explicitly later.

## Goals

1. A deploy never drops a live call. A worker can be told to drain, stops taking new streams, finishes its
   calls, and exits; a second worker takes new calls meanwhile.
2. Webhooks survive an API restart and are delivered at least once, in `seq` order per call, with a
   visible delivery log and a dead-letter state.
3. Capacity is correct for a dialer: engine slots and carrier slots are tracked separately, ringing calls
   do not consume engine slots, inbound is refused when full, and `GET /v1/capacity` reports headroom
   the platform can pace on.
4. STT and LLM fail over to a second vendor mid-call the way TTS already does.
5. Recordings are two-channel 8 kHz WAV, written during the call rather than buffered, stored in object
   storage, and deleted by a retention policy.
6. Every latency and accuracy number comes from the telephony path: a fake-carrier client that speaks the
   Plivo stream protocol drives the real worker endpoint with band-limited, G.711-companded audio, and the
   same client runs a 25-call load test.
7. Postgres is the production database with migrations. SQLite stays for tests and single-developer runs.
8. The minimum security a customer questionnaire will ask about on the carrier path: carrier callbacks are
   signature-checked, vendor keys and webhook secrets are encrypted at rest, and no unauthenticated endpoint
   returns a credential.
9. A dead worker is detected within 30 s; its calls are marked failed, the carrier legs are hung up, and
   `call.failed` is sent.

## Non-goals

- Load-aware assignment of calls to workers. A reverse proxy in front of N workers with round-robin or a
  blue/green upstream switch is enough at 20 concurrent. Worker registry and heartbeats are in scope only so
  the API knows which workers are alive and can compute capacity.
- SIP, LiveKit server, LiveKit SIP, agent-transport replacement.
- Per-language provider routing, mid-call language switching, text normalisation, noise suppression.
- Cost ledger, OpenTelemetry, structured JSON logs, post-call analytics. (Slice 2.)
- Campaign dialer, pacing, retries, calling hours, DND scrubbing. The platform backend owns the dialer and
  paces on `GET /v1/capacity`. (Assumption, see "Calls made".)
- Kubernetes. Docker Compose remains the deployment unit.
- Consent disclosure line, PII masking, retention for transcripts. (Slice 2, except recording retention
  which is here because the disk fills in weeks.)

## Anti-goals

- Anything that changes the shape of existing webhooks, `POST /v1/calls`, `GET /v1/calls/{id}` or the
  Agent JSON. New fields may be added; nothing is renamed or removed.
- Any change to the conversation runtime (`engine/worker/graph/`, `guards.py`, `tools.py`) beyond what
  failover and two-channel recording strictly need.
- A fix that works only on Linux or only on Windows. Developers run Windows; production is Linux containers.
- Performance work that improves the browser path. The phone path is the one that matters.

## Constraints

- Python 3.11 to 3.13, LiveKit Agents 1.8, agent-transport 0.2.2 (Plivo Labs' Rust media server). The
  worker's media WebSocket on `:8090` is owned by agent-transport; it installs its own SIGTERM handling that
  hangs up every session and calls `os._exit(0)`. Drain has to be designed around that.
- Carriers connect to one `PUBLIC_WS_URL` (`STREAM_PATH_PER_CALL=false`). Any multi-worker arrangement sits
  behind a reverse proxy on that URL.
- The API already routes control commands to the worker that reported `started` via `call.worker_url`.
- Carrier audio is `audio/x-mulaw;rate=8000` in 20 ms frames. The pipeline is fed 8 kHz PCM by
  agent-transport. Deepgram, Flux, Cartesia, ElevenLabs and Sarvam all accept or emit 8 kHz natively.
- Plivo India defaults: 50 concurrent calls, 2 calls per second outbound. Vendor TTS tiers cap concurrent
  streams (Cartesia and ElevenLabs by plan). These are configuration, not code.
- Hosting: GCP `asia-south1` (Mumbai). Object storage is S3-compatible (GCS with interoperability or MinIO
  locally).
- Offline tests must keep passing without vendor keys. Live tests are marked `integration`.

## The design, as decided

### 1. Worker lifecycle: registry, heartbeat, drain

- Each worker gets a stable `worker_id` (env, default hostname) and reports to the API every 5 s:
  `worker_id`, `worker_url`, `version`, `active_sessions`, `max_sessions`, `draining`, `started_at`.
  The API keeps a `workers` table; a worker not heard from for 20 s is `dead`.
- `MAX_CONCURRENT_CALLS` becomes per worker (`WORKER_MAX_SESSIONS`, default 20). A worker at its cap
  refuses a new stream: it closes the WebSocket immediately and the API marks the call `failed` with
  `end_reason=capacity`, hangs up the carrier leg, and sends `call.failed`.
- Drain: `POST /internal/drain` on the worker control port sets `draining=true`; the heartbeat carries it;
  the worker closes any new stream connection; it exits on its own when `active_sessions` reaches 0 or
  `DRAIN_TIMEOUT_SECONDS` (default 900) elapses, whichever first, after hanging up the remainder.
  The container entrypoint maps SIGTERM to this drain sequence before agent-transport can see the signal.
  The builder decides the mechanism (signal handler installed first, a supervising wrapper process, or an
  agent-transport hook if 0.2.2 exposes one); the acceptance test is the same in all cases.
- Deploy procedure documented in the README: start worker B on a new port, switch the proxy upstream,
  drain A, stop A when its heartbeat says 0 sessions. Compose gets a second worker service definition for
  this; the proxy (nginx or Caddy) config is part of the repo.
- Dead worker handling in the API sweeper: for every call `in_progress` on a dead worker, mark `failed`,
  `end_reason=worker_lost`, hang up the carrier leg, send `call.failed`.
- A draining worker does not exit while its event spool (section 3) still holds unacknowledged events;
  it keeps retrying until the API acknowledges them or `DRAIN_TIMEOUT_SECONDS` elapses, in which case the
  spool is written to disk under `data/spool/` and replayed on the next worker start.

### 2. Capacity model

- Two pools, both from the DB, both exposed:
  - **Engine slots**: `sum(max_sessions - active_sessions)` over live, non-draining workers. Counted
    against: calls `in_progress`, plus calls `answered` whose stream has not attached yet (a short window).
  - **Carrier slots**, per provider: `CARRIER_MAX_CONCURRENT[provider]` (Plivo default 50) minus calls
    `dialing | ringing | in_progress` on that provider; plus a calls-per-second budget
    `CARRIER_CPS[provider]` (Plivo default 2) enforced with a token bucket in the API.
- `POST /v1/calls` returns 429 with `Retry-After` and a JSON `reason` (`engine_full`, `carrier_full`,
  `cps_exceeded`) when any pool is exhausted. Browser sessions count as engine slots.
- `GET /v1/capacity` returns `{engine: {free, total}, carriers: {plivo: {free, total, cps_free}}, workers: [...]}`.
  The old top-level `free` field stays for compatibility and equals the minimum of the pools.
- Inbound when engine slots are 0: the answer XML returns a short `<Speak>` "all lines are busy" plus
  `<Hangup/>`; the call is recorded as `failed`, `end_reason=capacity`.

### 3. Webhook outbox

- New table `webhook_deliveries`: `id`, `call_id`, `seq`, `event`, `payload_json`, `url`, `status`
  (`pending | delivering | delivered | dead`), `attempts`, `next_attempt_at`, `last_status_code`,
  `last_error`, `created_at`, `delivered_at`. `seq` is allocated in the same transaction that inserts the
  row, from a per-call counter column on `calls`.
- The dispatcher is a loop in the API process that claims due rows (`SELECT ... FOR UPDATE SKIP LOCKED`
  on Postgres; a simple select on SQLite) and sends them. Per-call ordering: a row is eligible only if no
  earlier `seq` for the same call is `pending` or `delivering`. Retry schedule unchanged (0.5, 2, 8, 30,
  120 s); after the last attempt the row is `dead`. 4xx other than 408/429 goes `dead` immediately.
- `GET /v1/calls/{id}/deliveries` lists rows; `POST /v1/calls/{id}/deliveries/{seq}/replay` resets a row to
  `pending`. Both behind the engine API key.
- The in-memory per-call queue and the process-wide `seq` counter are removed.
- **Worker-side spool.** Today the worker's event reporter gives up on an event after three tries over
  about three seconds and abandons the queue ten seconds after a call ends, so an API restart loses turn,
  tool and even `ended` events before any outbox sees them. The reporter becomes a per-worker spool that
  retries until the API acknowledges each event (exponential backoff capped at 10 s, no attempt limit),
  keeps `ended` events after the call object is gone, and survives a worker restart via the on-disk spool
  above. "No gap" in acceptance 3 therefore includes events generated while the API was down.
- **Single API process.** The CPS token bucket, the sweeper and the outbox dispatcher assume exactly one
  API process, which is what `gvox-api` runs today. This is a documented constraint of the slice; running
  API replicas is out of scope and would need a DB-backed bucket and dispatcher leases (the
  `SKIP LOCKED` claim is kept so that change is small later).

### 4. STT and LLM failover

- `STT_FALLBACK_PROVIDER` and `LLM_FALLBACK_PROVIDER` (env defaults; per-agent override in Agent JSON
  under `stt.fallback` / `llm.fallback`), wired through LiveKit's `stt.FallbackAdapter` and
  `llm.FallbackAdapter` exactly as `TTS_FALLBACK_PROVIDER` is today. Empty = off.
- Availability changes are logged and emitted on the existing provider status path so the console and
  the call record show which vendor served which turn.
- Defaults: STT fallback = `sarvam` when a Sarvam key exists, LLM fallback = `groq` when a Groq key
  exists; otherwise off.
- **Flux conflict (pending Aditya's answer, see "Calls made").** The default STT is Deepgram Flux, whose
  own end-of-turn events drive turn detection (`turn_detection="stt"`). LiveKit's `stt.FallbackAdapter`
  cannot carry end-of-turn, and Sarvam does not emit it, so wrapping Flux in the adapter would either
  silently move every call to VAD plus semantic endpointing (about 0.2 s slower per turn) or leave a call
  unable to commit turns after failing over. Recommended resolution, written here as the design unless
  overridden: keep Flux as the primary with its own endpointing; add a per-provider circuit breaker in the
  worker so that once Flux fails for any call, new calls start on nova-3 wrapped in the fallback adapter
  until Flux is healthy again; for the call in which Flux fails, the engine swaps that session to the
  fallback STT with VAD plus semantic endpointing for the rest of the call if LiveKit 1.8 allows swapping
  the recognition path mid-session, otherwise the call is ended with `end_reason=stt_failed` and
  `call.failed` and the breaker protects the next calls. Acceptance 5 for STT runs with nova-3 as the
  primary; a second drill with Flux as primary verifies the breaker.

### 5. Recording

- Two channels: left = caller, right = agent. 8 kHz, 16-bit PCM WAV. Frames are appended to a temp file
  during the call; the header is finalised at the end. Nothing is held in memory beyond one frame.
- On call end the file is uploaded to the bucket at `recordings/{yyyy}/{mm}/{dd}/{call_id}.wav` and the
  object key is stored in `recording_path`. `GET /v1/calls/{id}/recording` streams from the bucket (or
  returns a signed URL when `RECORDING_URL_MODE=signed`). Local disk is only the staging area.
- `RECORDING_RETENTION_DAYS` (default 90). The API sweeper deletes expired objects nightly and nulls
  `recording_path`. `RECORD_CALLS` remains the global switch; a per-agent `record: false` overrides it.
- Interruption handling keeps the current behaviour (drop the unplayed tail of agent audio).

### 6. Telephony-path measurement and load test

- A new carrier provider `fake` in the registry: `dial` returns a generated CallUUID and never touches a
  network; `hangup` is a no-op. It exists so test calls have real call records.
- `scripts/fake_carrier.py`: opens N WebSocket connections to the worker media port speaking the Plivo
  stream protocol (reusing `stream_protocol.py`), identifies each by `X-Call-Id`, plays WAV utterances as
  paced 20 ms mu-law frames, records agent audio, and measures end-of-utterance to first agent audio per
  turn. It can run a scripted dialogue (greeting wait, play "yes", wait, play "no") and reports p50/p95.
- Test audio preparation: `make_utterance.py` gains `--telephony`, which band-limits to 300–3400 Hz,
  resamples to 8 kHz with a proper resampler, and round-trips through mu-law before saving.
- `latency_bench.py` and `browser_smoke.py` gain `--path telephony`, which uses the fake carrier instead
  of the browser transport. The README's latency table is replaced by numbers from this path.
- Load test: `fake_carrier.py --calls 25 --minutes 10` against the reference box (4 vCPU, 8 GB).
  Pass criteria in "Acceptance".
- The STT plugin for carrier sessions is configured at 8000 Hz so the LiveKit base class does not upsample
  before Deepgram or Flux. Browser sessions keep 16 kHz.
- `engine/worker/audio.py` linear-interpolation resampling is replaced by LiveKit's `rtc.AudioResampler`
  (or `soxr`) wherever it is used (browser transport, recording). The mu-law tables stay.

### 7. Database

- Postgres is the default in `docker-compose.yml` (a `postgres` service with a volume); `DATABASE_URL`
  for SQLite remains documented for local runs and is what the offline tests use.
- Alembic is introduced with a baseline migration equal to today's `create_all` schema, then one migration
  per change in this slice (`workers`, `webhook_deliveries`, new columns on `calls`). `create_all` is no
  longer called at startup; `gvox-api migrate` runs migrations, and the compose entrypoint runs it first.
- **Existing data.** The engine has not served a paying customer yet; the SQLite file and local recordings
  from Phase 0 are treated as test data. A one-off `gvox-api copy-sqlite --from <path>` copies agents,
  numbers and calls into Postgres for convenience. Recordings on local disk are not backfilled into the
  bucket; their `recording_path` keeps working through the API's local-disk fallback until retention
  removes them.

### 8. Security minimums

- Carrier callbacks: Plivo requests are verified with Plivo's signature header (V3 with fallback to V2)
  using the auth token; Vobiz the same if it signs, otherwise an IP allowlist from config; unverifiable
  requests get 403. EnableX stays a stub.
- BYOK provider keys and webhook secrets are encrypted at rest with a symmetric key from
  `ENGINE_ENCRYPTION_KEY` (Fernet). `GET /v1/agents/{id}` returns them masked. A one-off
  `gvox-api encrypt-existing` command migrates rows.
- `/ui/config.json` requires the engine API key; the browser key is no longer served unauthenticated.
- `/metrics` on the worker is bound to the internal interface or requires the internal token.
- API keys are compared in constant time.
- **Carrier media WebSocket.** The stream endpoint on `:8090` is owned by agent-transport and accepts any
  client that speaks the protocol, with an attacker-chosen `X-Call-Id`. In scope: the reverse proxy in
  front of `PUBLIC_WS_URL` allows only the carriers' published media IP ranges (configured per provider in
  `.env`), and the fake carrier used by the acceptance tests connects on a separate internal listener or
  path that is never exposed by the proxy and requires the internal token in its start frame. Both
  mechanisms are proxy and config work; agent-transport is not modified.

### 9. Health and operations

- API `/health` reports DB reachability, live worker count, outbox backlog, oldest pending delivery age.
- API exposes Prometheus `/metrics` (internal-token protected): calls by status, engine and carrier
  headroom, outbox backlog, deliveries dead, workers live/draining.
- A short `docs/runbook.md`: deploy with drain, what to do when a worker dies, how to replay webhooks,
  how to read capacity, how to run the load test.

## Acceptance

All run on the reference box with the `fake` carrier unless stated.

1. **Load**: 25 concurrent fake-carrier calls for 10 minutes, cascaded pipeline. Zero dropped calls,
   p95 end-of-utterance-to-first-audio within 25% of the single-call p95 on the same box, worker CPU
   below 70% sustained.
2. **Drain drill**: with 20 live calls on worker A, start B, switch the proxy, drain A. All 20 calls finish
   on A, new calls land on B, A exits by itself. Zero `call.failed`.
3. **API restart**: kill the API with 20 live calls and a webhook sink that is slow; keep it down for
   60 s, long enough for several turns and at least one call to end. Calls continue; after restart every
   event, including those generated while the API was down, is delivered with no gap or duplicate `seq`
   per call, and the calls that ended during the outage are `ended`, not `failed`.
4. **Worker loss**: kill worker A with live calls. Within 30 s each call is `failed`/`worker_lost`, the
   carrier `hangup` was called, `call.failed` delivered.
5. **Vendor outage**: with nova-3 as the primary STT, block Deepgram at the network; STT fails over within
   one turn and the call continues. Same for the LLM. With Flux as primary, block Deepgram: the affected
   call either continues on the fallback with VAD endpointing or ends with `stt_failed`, and the next call
   starts on nova-3 plus fallback without a Flux attempt.
6. **Capacity**: with workers at cap, `POST /v1/calls` returns 429 with the right reason and
   `GET /v1/capacity` shows 0 engine headroom; an inbound call hears the busy line and is recorded as
   `failed`/`capacity`. CPS budget: 10 dials in one second yield 2 accepted, 8 `cps_exceeded`.
7. **Recording**: every completed call has a two-channel 8 kHz WAV in the bucket with caller left and
   agent right; a call older than the retention window has none.
8. **Telephony bench**: `latency_bench.py --path telephony` produces a table; its p50 is the number in
   the README.
9. **Security**: an unsigned Plivo callback gets 403; agent GET shows masked keys; `/ui/config.json`
   without the API key gets 401.
10. **Offline tests** cover capacity arithmetic, outbox ordering and retry transitions, signature
    verification, the resampler (sine sweep round-trip SNR), and the fake-carrier protocol framing.
    `pytest -q` passes with no vendor keys.

## Left to the builder

- Exact drain mechanism around agent-transport's signal handling.
- Table column types, index choices, Alembic layout.
- Resampler library (LiveKit's `rtc.AudioResampler` is the default choice).
- S3 client library and bucket layout details beyond the key pattern above.
- Proxy choice (nginx vs Caddy) and its config.
- Metric names and label sets.
- How `fake_carrier.py` paces frames and detects agent silence to time turns.
- Whether the outbox dispatcher uses polling or LISTEN/NOTIFY on Postgres.
- Spool format on disk and the acknowledgement protocol between worker and API (an event id echoed back
  is enough).
- Whether the drain and worker-loss drills can be rehearsed on a Windows developer box; if not, they run
  only on the Linux reference box and the plan says so.

## Calls made (not decided by Aditya)

- The platform backend owns the campaign dialer; the engine provides capacity and 429s only.
- Load-aware worker assignment is out; a proxy with blue/green is the deploy story.
- Postgres default in compose; SQLite kept for tests.
- Recording retention default 90 days; recordings staged on disk then uploaded, not streamed to the bucket.
- STT fallback default Sarvam, LLM fallback default Groq, only when keys are present.
- Inbound-when-full plays a busy line instead of silently hanging up.
- Reference box for acceptance is 4 vCPU / 8 GB, matching LiveKit's own agent-server sizing guidance.
- Worker events are spooled and retried without limit so an API restart loses nothing; a draining worker
  waits for acknowledgements.
- The API is one process for this slice; replicas are out of scope.
- Phase-0 SQLite data is test data: copied by a one-off command, recordings not backfilled.
- The carrier media WebSocket is protected by a proxy IP allowlist; the fake carrier uses an internal
  listener.
- Flux stays the primary STT with a circuit breaker for failover between calls; mid-call failover is
  best-effort (pending Aditya: see the open question on latency versus failover).

## Resolved on 2026-10-22 (Aditya delegated these: "do best")

1. **Flux stays primary.** Circuit breaker between calls is guaranteed; mid-call failover is best-effort
   as described in section 4. nova-3 plus semantic detector remains one config change away.
2. **Acceptance runs locally first, then on the deploy box.** There is no Linux box yet; Aditya deploys
   after local testing on Windows. Therefore: drain is driven by the control endpoint (works on Windows)
   and the SIGTERM mapping is a thin Linux-only entrypoint wrapper verified in Docker; worker-loss is
   rehearsed by killing the worker process; object storage has a `local` backend (a directory) for
   development and an `s3` backend for production, selected by `RECORDING_STORAGE`; the 25-call load test
   is a documented command for the deploy box, while the local run uses 3 calls for 2 minutes against real
   vendor keys to prove the path. Acceptance item 1's full numbers are produced on the deploy box.
3. **One release, built in dependency order.** No campaign date was given, so the plan is sequenced so
   that drain, outbox and spool, capacity, Postgres and dead-worker handling land first and the slice can
   be cut there if a date appears.

### 10. Minimal local test UI

The existing console at `/ui/` (Talk, Phone call, Calls, Agent JSON) gains one **Ops** tab, kept minimal:

- **Workers**: id, url, version, active / max sessions, draining flag, seconds since last heartbeat, and a
  **Drain** button that calls `POST /v1/workers/{id}/drain` (API proxies to the worker's control port).
- **Capacity**: the two pools from `GET /v1/capacity` (engine free/total, per-carrier free/total/cps),
  refreshed every 5 s.
- **Deliveries**: for the call selected in the Calls tab, the outbox rows with status, attempts, last
  status code, and a **Replay** button per row.
- **Per-turn provider**: the Calls tab's per-turn latency table gains a column showing which STT / LLM /
  TTS provider served the turn (from the failover status events), so a failover drill is visible.
- **Telephony smoke**: a button that runs the fake-carrier smoke (3 calls, scripted dialogue) through the
  API (`POST /v1/test/telephony-smoke`, internal-token protected, only enabled when `ENABLE_TEST_ENDPOINTS=true`)
  and shows per-turn latency when done. This is the local stand-in for acceptance items 1 and 8.

No new framework; the console stays plain HTML/JS as today.
