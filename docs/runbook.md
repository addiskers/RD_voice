# gvox-voice runbook

Operations notes for the engine after slice 1 (2026-10-23). Commands assume the project venv on Windows
(`.\.venv\Scripts\...`) or the Compose stack in production.

## What runs where

| Process | Port | Does |
|---|---|---|
| `gvox-api` | 8080 | REST API, carrier callbacks, webhook outbox, worker registry, sweepers, `/metrics` (internal token) |
| `gvox-worker` | 8090 media WebSocket, 8091 control | one conversation per carrier stream, event spool, heartbeat, drain |
| Caddy (compose) | 8080 / 8090 public | TLS termination, media-IP allowlist, blue/green upstreams |
| Postgres (compose) | internal | the database; SQLite only for single-developer runs |

A worker heartbeats every 5 s (`POST /internal/workers/heartbeat`); the API marks it dead after 20 s of
silence and fails its live calls as `worker_lost` (carrier legs are hung up).

## Deploy without dropping calls (blue/green)

1. `docker compose --profile standby up -d --build worker-b` (new image; `worker` keeps serving).
2. Confirm both are live: `GET /v1/workers` shows `worker` and `worker-b` with `status: live`.
3. Drain the old one: `POST /v1/workers/worker/drain` (API key). Its `/healthz` turns 503, so Caddy sends new
   streams to `worker-b`; live calls on `worker` finish; the worker exits by itself when idle and its event
   spool is acknowledged (`DRAIN_TIMEOUT_SECONDS`, default 900 s, then it ends what is left).
4. `docker compose up -d --build worker` and drain `worker-b` the same way (or leave both running).

`docker stop` / `compose down` also drain: `deploy/worker-entrypoint.sh` maps SIGTERM to the drain request;
keep `stop_grace_period` above the drain timeout. On Windows there is no SIGTERM mapping: call
`POST /internal/drain` on the worker's control port with `X-Internal-Token`.

## A worker died

- `GET /v1/workers` shows it `dead`; within 30 s its calls are `failed` / `worker_lost` and `call.failed`
  webhooks went out. Nothing to do for those calls.
- On restart the worker replays `data/spool/*.jsonl` (events the API never acknowledged) and deletes stale
  `*.caller.raw` / `*.agent.raw` recording files. Check the log for `replaying N event spool(s)`.
- If the worker died with calls, the recordings of those calls are lost (only finished calls are uploaded).

## Webhooks

- Every event is a row in `webhook_deliveries` before it is sent. `GET /v1/calls/{id}/deliveries` lists
  them with `status` (`pending | delivering | delivered | dead`), `attempts`, `last_status_code`.
- Retries: 0.5 s, 2 s, 8 s, 30 s, 120 s, then `dead`. A 4xx other than 408/429 is `dead` immediately.
- Replay one: `POST /v1/calls/{id}/deliveries/{seq}/replay`. Backlog: `/health` → `outbox_pending`,
  `outbox_oldest_s`; `/metrics` → `gvox_outbox_pending`, `gvox_outbox_dead`.
- Ordering is per call: a dead row blocks later seqs of that call until replayed.

## Capacity

`GET /v1/capacity` returns two pools. `engine.free` is conversation slots across live, non-draining
workers (`WORKER_MAX_SESSIONS` each; with no worker registered the static `MAX_CONCURRENT_CALLS`).
`carriers.<provider>.free` is the carrier's concurrent-call limit (`CARRIER_MAX_CONCURRENT`) minus calls
dialing, ringing or in progress; `cps_free` is the dials-per-second budget (`CARRIER_CPS`). `POST /v1/calls`
answers 429 with `detail.reason` = `engine_full | carrier_full | cps_exceeded` and `Retry-After: 1`; a
dialer should pace on `available` and back off on 429. Inbound calls with no engine slot hear a busy line
and are recorded as `failed` / `capacity`.

## Carrier callbacks and media

- `CARRIER_VERIFY_SIGNATURES=true` (default): Plivo callbacks must carry a valid `X-Plivo-Signature-V3`
  computed over `PUBLIC_HTTP_URL` + path + query, so `PUBLIC_HTTP_URL` must be exactly the URL the carrier
  calls. Vobiz: V3 signature when present, else the IP allowlist `VOBIZ_CALLBACK_IPS` (comma-separated
  IPs/CIDRs). With verification on and neither configured, Vobiz callbacks are rejected (403).
- The media WebSocket on :8090 accepts any client that speaks the protocol. In compose, set
  `CARRIER_MEDIA_IPS` (space-separated IPs/CIDRs) to the carrier's published media ranges; Caddy answers 403
  to everything else. The default allows all.

## Secrets

- `ENGINE_ENCRYPTION_KEY` (a Fernet key: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`)
  encrypts bring-your-own provider keys inside agent configs and webhook secrets. Without it values are
  stored in plaintext and the API logs a warning at start.
- Existing rows: `gvox-api encrypt-existing` after setting the key.
- Rotation: `ENGINE_ENCRYPTION_KEY` accepts a comma-separated list; the first key encrypts and every key
  decrypts. Set `ENGINE_ENCRYPTION_KEY=<new>,<old>`, restart the API, run `gvox-api encrypt-existing`
  (it re-encrypts every value with the new key), then remove the old key.
- `GET /v1/agents/{id}` returns keys masked (`****1234`); only the worker runtime payload carries them.
  Saving a config that still contains masked keys keeps the previous version's real keys.
- `CARRIER_VERIFY_LOG_ONLY=true` verifies callback signatures but only logs failures: use it for the first
  day on a new carrier account to confirm the signature scheme before enforcing.

## Recordings

Two channels (left caller, right agent), 8 kHz, written during the call, uploaded on call end to
`RECORDING_STORAGE` (`local` directory or `s3` bucket) under `recordings/YYYY/MM/DD/<call_id>.wav`.
`GET /v1/calls/{id}/recording` streams it (or redirects to a signed URL with `RECORDING_URL_MODE=signed` on
S3). Retention: `RECORDING_RETENTION_DAYS` (90); the sweeper deletes older objects daily and at start.

## Cost estimates

The worker sums billable quantities from LiveKit's metrics events (STT seconds, LLM tokens uncached/cached/
output, TTS characters including cancelled synthesis, realtime audio/text tokens) and sends them on the
`ended` event as `usage_detail`. The API prices them with `engine/pricing.json` (list prices with source URLs
and the date read; `carrier_inr_per_minute` for Plivo/Vobiz with 30-second pulses) and stores the result in
`metrics.cost` and `cost_usd`. `GET /v1/calls/{id}/cost` recomputes with the current price list.

- Contracted rates: copy `engine/pricing.json`, edit, set `PRICING_FILE=/path/to/copy.json`.
- Exchange rate: `USD_INR=<rate>` (default is the value in the pricing file).
- A model with no entry shows `no price on file` and `incomplete: true`; the total is then a lower bound.
  Add the key `<provider>/<model>` (as shown in the call's `models` row) to the pricing file.
- Review the prices whenever a vendor changes its page; `as_of` in the file and in every cost payload says
  when they were last read.

## Testing the phone path locally

Start the API with `ENABLE_TEST_ENDPOINTS=true` and the worker; then:

```
python scripts/fake_carrier.py --calls 1 --script yes,no          # one scripted call, prints latency
python scripts/fake_carrier.py --calls 25 --minutes 10            # load test (acceptance item 1)
python scripts/latency_bench.py --runs 3 --path telephony         # stage breakdown on the 8 kHz path
```

The fake carrier plays Plivo: it opens the worker's media WebSocket directly (`WORKER_MEDIA_URL`, never
through the proxy), sends band-limited G.711 audio, and posts the hangup callback. It spends real vendor
minutes. The console's **Ops** tab has a button for the 3-call smoke (internal token required).

## Last local verification

2026-10-23, Windows 11 laptop, project venv, real vendor keys, SQLite, local recording storage, API with
`ENABLE_TEST_ENDPOINTS=true`, one worker, `scripts/fake_carrier.py` as the carrier.

| Drill | Result |
|---|---|
| 3 scripted calls (EO workflow, yes / no) with a webhook sink | 3/3 completed, 6 turns; client p50 **2.62 s**, p95 2.82 s; engine `total` p50 2.78 s; 10 deliveries per call, seq 1..10 contiguous, all delivered, HMAC verified by the sink |
| API killed for ~30 s during 2 live calls, then restarted | worker spool retried until acknowledged; both calls `completed` / `workflow_end` with full transcripts; 10/10 deliveries each, contiguous, including `call.ended` |
| worker killed during a live call | call `failed` / `worker_lost` 21 s after the kill; restarted worker removed the orphaned raw recording files |
| drain with 2 live calls, third call attempted during drain | `/healthz` 503 while draining; third stream refused and its call `failed` / `draining`; both live calls finished (59 s, 62 s); worker exited by itself 52 s later |
| unsigned Plivo callbacks | `answer` and `hangup` both 403 |
| single-prompt agent (gemini-2.5-flash-lite), 2 concurrent calls, 1 minute | client p50 ≈ 1.2 s, engine `total` p50 1.31 s on the 8 kHz path |

The drills above ran before the whole-branch review; the review's fixes (outbox recovery after a crash,
carrier-route isolation, capacity during drain, worker re-incarnation, per-worker data directories, drain
grace, key rotation, log-only verification) are covered by `tests/test_review_fixes*.py` and were not
re-drilled live.

Not run locally (needs the deploy box): the 25-call × 10-minute load test (acceptance item 1), the Linux
SIGTERM-to-drain entrypoint (`deploy/worker-entrypoint.sh`), the S3 recording backend, Postgres end to end
(the suite and drills ran on SQLite), Vobiz callbacks, and the browser click-through of the Ops tab (its
endpoints were exercised over HTTP).

Cost breakdown (2026-10-23): two fake-carrier calls; the worker's billed quantities matched LiveKit's own
session totals for LLM input tokens and TTS characters exactly, and TTS characters equalled the agent
transcript length. Gemini thinking tokens (369 of 522 output tokens on the thinking-enabled call) are
counted by the worker but missing from LiveKit's summary.

Found and fixed during verification: agent-transport 0.2.2 under LiveKit Agents 1.8 crashed every carrier
call in `AgentSession.start` (`engine/worker/main.py::patch_stub_job`); Gemini rejects the LLM fallback
adapter's default 5 s deadline (now 15 s); a call the carrier had already hung up was never finalized when
its worker died (now `completed` / `worker_lost`); a Phase-0 SQLite file (no `alembic_version`) is stamped at
the baseline before upgrading.
