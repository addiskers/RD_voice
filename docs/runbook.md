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
- Existing rows: `gvox-api encrypt-existing` after setting the key. Rotation: decrypt with the old key is
  not automatic; set the new key, run `encrypt-existing` **before** removing the old one is not possible
  with a single key, so rotate by exporting agents, changing the key, and re-importing.
- `GET /v1/agents/{id}` returns keys masked (`****1234`); only the worker runtime payload carries them.

## Recordings

Two channels (left caller, right agent), 8 kHz, written during the call, uploaded on call end to
`RECORDING_STORAGE` (`local` directory or `s3` bucket) under `recordings/YYYY/MM/DD/<call_id>.wav`.
`GET /v1/calls/{id}/recording` streams it (or redirects to a signed URL with `RECORDING_URL_MODE=signed` on
S3). Retention: `RECORDING_RETENTION_DAYS` (90); the sweeper deletes older objects daily and at start.

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

Filled in by the verification pass (see docs/superpowers/plans/2026-10-22-slice1-telephony-reliability.md, Task 14).
