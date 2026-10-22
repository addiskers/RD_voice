# Slice 1: Telephony Reliability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the carrier-WebSocket telephony path survive a 20-concurrent, 5,000-calls-a-day campaign: no dropped calls on deploy, durable webhooks, a truthful capacity model, STT/LLM failover, two-channel 8 kHz recordings in object storage, telephony-path measurement, Postgres, and the security minimums.

**Architecture:** Everything is additive around the existing FastAPI API process and the single agent-transport worker process. The API gains a `workers` registry, a database-backed webhook outbox, a two-pool capacity model and a storage abstraction; the worker gains a disk spool for events, heartbeat/drain lifecycle, fallback adapters, a stereo recorder and an 8 kHz input path. A `fake` carrier plus a Plivo-protocol client script lets every acceptance drill run locally against the real worker endpoint.

**Tech Stack:** Python 3.11–3.13, FastAPI, SQLAlchemy async + Alembic, LiveKit Agents 1.8 (`stt.FallbackAdapter`, `llm.FallbackAdapter`, `rtc.AudioResampler`), agent-transport 0.2.2 (unmodified), cryptography (Fernet), prometheus-client, boto3 (S3 backend only), Docker Compose with Postgres and Caddy.

**Spec:** `docs/superpowers/specs/2026-10-22-slice1-telephony-reliability-design.md`

## Global Constraints

- Python `>=3.11,<3.14`; `livekit-agents~=1.8`; `agent-transport==0.2.2` is a dependency and is never modified or forked.
- No existing REST field, webhook field or Agent JSON field is renamed or removed; new fields only.
- `engine/worker/graph/`, `engine/worker/guards.py`, `engine/worker/tools.py` are not changed except where failover or two-channel recording strictly requires it.
- Carriers connect to one `PUBLIC_WS_URL`; audio is `audio/x-mulaw;rate=8000`, 20 ms frames.
- Exactly one API process; the CPS bucket, sweeper and outbox dispatcher may be in-process state.
- Developers run Windows; production is Linux containers. Nothing may work on only one. Drain is driven by `POST /internal/drain`; the SIGTERM mapping is a Linux-only entrypoint script.
- `pytest -q` passes with no vendor keys and no network. Live checks are scripts or `@pytest.mark.integration`.
- Dev commands use the project venv: `.\.venv\Scripts\python.exe -m pytest -q`, `.\.venv\Scripts\python.exe -m ruff check engine tests scripts`.
- Secrets: never read `.env`; tests build `Settings(_env_file=None, ...)`.

## Review Focus

1. **Plivo callback arriving through the proxy** with an internal `http://` host: the V3 signature must be computed over `PUBLIC_HTTP_URL` plus path and query, or every real call is rejected with 403. Pinned by `test_plivo_v3_uses_public_url` (Task 10).
2. **Devanagari transcript in a webhook payload**: the outbox must persist and re-serialize UTF-8 so the HMAC still verifies after a restart. Pinned by `test_outbox_unicode_payload_signature_verifies` (Task 4).
3. **A worker comes back and replays `ended` for a call the API already failed as `worker_lost`**: the transcript must be stored but the terminal status must not flip. Pinned by `test_late_ended_after_worker_lost_is_ignored` (Task 2).
4. **A worker is redeployed with a different `WORKER_MAX_SESSIONS`**: capacity total must follow the latest heartbeat, not the first. Pinned by `test_heartbeat_updates_max_sessions` (Task 2).
5. **Worker crash mid-call leaves half-written raw recording files**: the next start must not crash and must not upload garbage. Pinned by `test_finish_tolerates_missing_agent_file` and `test_stale_raw_files_removed_on_start` (Task 9).

---

### Task 1: Settings, dependencies, Alembic and the new tables

**Files:**
- Modify: `pyproject.toml` (dependencies), `engine/config.py`, `engine/api/db.py`
- Create: `alembic.ini`, `engine/api/migrations/env.py`, `engine/api/migrations/script.py.mako`, `engine/api/migrations/versions/0001_baseline.py`, `engine/api/migrations/versions/0002_slice1.py`
- Test: `tests/test_migrations.py`

**Interfaces:**
- Produces: `Settings` fields (exact names and defaults):
  `worker_id: str = ""`, `worker_max_sessions: int = 20`, `worker_heartbeat_seconds: float = 5.0`, `worker_dead_after_seconds: float = 20.0`, `drain_timeout_seconds: int = 900`,
  `carrier_max_concurrent: str = "plivo=50,vobiz=50,enablex=50,fake=1000"`, `carrier_cps: str = "plivo=2,vobiz=2,enablex=2,fake=100"`,
  `stt_fallback_provider: str = "sarvam"`, `llm_fallback_provider: str = "groq"`,
  `recording_storage: str = "local"`, `recording_s3_bucket: str = ""`, `recording_s3_endpoint: str = ""`, `recording_s3_region: str = "ap-south-1"`, `recording_s3_access_key: str = ""`, `recording_s3_secret_key: str = ""`, `recording_retention_days: int = 90`, `recording_url_mode: str = "stream"`,
  `spool_dir: str = "./data/spool"`, `engine_encryption_key: str = ""`, `carrier_verify_signatures: bool = True`, `vobiz_callback_ips: str = ""`, `enable_test_endpoints: bool = False`.
- Produces: ORM classes `Worker` (`worker_id` PK String(128), `worker_url` Text, `version` String(32), `active_sessions` Integer, `max_sessions` Integer, `draining` Boolean, `started_at` DateTime(tz), `last_seen_at` DateTime(tz) index, `status` String(8) default `"live"`), `WebhookDelivery` (`id` String(40) PK, `call_id` String(40) index, `seq` Integer, `event` String(32), `payload_json` Text, `url` Text, `status` String(12) index default `"pending"`, `attempts` Integer default 0, `next_attempt_at` DateTime(tz) index, `last_status_code` Integer nullable, `last_error` Text nullable, `created_at`, `delivered_at` nullable), and new `Call` columns `webhook_seq: int default 0`, `worker_id: Optional[str]`.
- Produces: `Database.init()` now runs `alembic upgrade head` programmatically; `engine.api.db.run_migrations(url: str) -> None`.

- [ ] **Step 1: Add dependencies**

In `pyproject.toml` `dependencies`, add `"alembic>=1.13"`, `"cryptography>=42"`, `"prometheus-client>=0.20"`. Add optional extra `s3 = ["boto3>=1.34"]`. Install with `.\.venv\Scripts\python.exe -m pip install -e ".[dev,plivo-transport,s3]"`.

- [ ] **Step 2: Write the failing test**

```python
# tests/test_migrations.py
import asyncio
from sqlalchemy import inspect
from engine.api.db import Database

def _tables(db: Database) -> set[str]:
    async def go():
        async with db.engine.connect() as conn:
            return set(await conn.run_sync(lambda c: inspect(c).get_table_names()))
    return asyncio.run(go())

def test_fresh_db_has_slice1_tables(tmp_path):
    db = Database(f"sqlite+aiosqlite:///{tmp_path / 'm.db'}")
    asyncio.run(db.init())
    names = _tables(db)
    assert {"agent_versions", "calls", "numbers", "workers", "webhook_deliveries", "alembic_version"} <= names
    cols = asyncio.run(_cols(db, "calls"))
    assert {"webhook_seq", "worker_id"} <= cols

def test_init_is_idempotent(tmp_path):
    db = Database(f"sqlite+aiosqlite:///{tmp_path / 'm.db'}")
    asyncio.run(db.init()); asyncio.run(db.init())
    assert "workers" in _tables(db)
```
(`_cols` mirrors `_tables` with `inspect(c).get_columns(name)` names.)

- [ ] **Step 3: Run test to verify it fails**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_migrations.py -v`
Expected: FAIL (`workers` not in tables).

- [ ] **Step 4: Add the Settings fields** listed in Interfaces to `engine/config.py` under new comment sections `# --- workers`, `# --- carrier limits`, `# --- failover`, `# --- recordings`, `# --- security`, `# --- test`. Add `def carrier_limit(self, provider: str) -> int` and `def carrier_cps_limit(self, provider: str) -> int` that parse the `name=int,name=int` strings (unknown provider → 50 and 2).

- [ ] **Step 5: Add the ORM classes and columns** in `engine/api/db.py` exactly as in Interfaces. Add `run_migrations(url: str) -> None` that builds an `alembic.config.Config` pointing `script_location` at `engine/api/migrations` and sets `sqlalchemy.url`, then calls `alembic.command.upgrade(cfg, "head")`. `Database.init()` calls `await asyncio.to_thread(run_migrations, self.url)` (store `self.url` in `__init__`) and no longer calls `create_all`.

- [ ] **Step 6: Create the Alembic environment**

`alembic.ini` at repo root with `script_location = engine/api/migrations`. `env.py` uses the async template: `create_async_engine(config.get_main_option("sqlalchemy.url"))`, `connection.run_sync(do_run_migrations)`, `target_metadata = Base.metadata`, `render_as_batch=True` (needed for SQLite column adds). Because `Database.init()` runs inside a running loop via a thread, `env.py` must call `asyncio.run(run_async_migrations())` only when no loop is running in that thread (it never is, inside `to_thread`).

`0001_baseline.py`: the three existing tables exactly as today's models (copy types and indexes from `db.py` before your edits). `0002_slice1.py`: `workers`, `webhook_deliveries`, and `op.add_column` for `calls.webhook_seq` (server_default `"0"`) and `calls.worker_id`.

- [ ] **Step 7: Run the full suite**

Run: `.\.venv\Scripts\python.exe -m pytest -q`
Expected: all pass, including the new file. (Existing tests build fresh SQLite DBs per test; migrations run in each.)

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml alembic.ini engine/config.py engine/api/db.py engine/api/migrations tests/test_migrations.py
git commit -m "feat(db): alembic migrations, workers and webhook_deliveries tables, slice-1 settings"
```

---

### Task 2: Worker registry, heartbeats and dead-worker handling (API side)

**Files:**
- Modify: `engine/api/service.py`, `engine/api/app.py`, `engine/schemas/calls.py`
- Test: `tests/test_workers.py`

**Interfaces:**
- Produces (schemas, `engine/schemas/calls.py`): `class WorkerHeartbeat(BaseModel)` with `worker_id: str`, `worker_url: str`, `version: str = ""`, `active_sessions: int = 0`, `max_sessions: int`, `draining: bool = False`, `started_at: Optional[str] = None`, `spool_pending: int = 0`; `class WorkerOut(BaseModel)` with those fields plus `status: str`, `seconds_since_seen: float`, `last_seen_at: str`.
- Produces (service): `async upsert_worker(db, hb: WorkerHeartbeat) -> Worker`; `async list_workers(db) -> list[WorkerOut]`; `async sweep_workers(db) -> int` (marks dead, fails their calls, returns calls failed); `async drain_worker(db, worker_id: str) -> bool` (POSTs `{worker_url}/internal/drain` with `X-Internal-Token`, sets `draining=True`); `on_worker_event` handles `etype == "refused"` (data `{"reason": "capacity"|"draining"}`) by `finalize(failed, reason)` plus carrier hangup, and stores `data["worker_id"]` on `started`.
- Produces (routes): `POST /internal/workers/heartbeat` (internal token) → `{"ok": true}`; `GET /v1/workers` → `list[WorkerOut]`; `POST /v1/workers/{worker_id}/drain` → `{"worker_id", "draining": true}` or 404.
- Produces (app lifespan): the sweeper loop runs every 10 s, calling `sweep_workers` each tick and `sweep_stale` every 6th tick.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_workers.py  (reuse the `client`, H, HI, FakePlivo fixtures by importing from tests.test_api)
def _hb(client, **kw):
    body = {"worker_id": "w1", "worker_url": "http://w1:8091", "max_sessions": 20, "active_sessions": 0}
    body.update(kw)
    return client.post("/internal/workers/heartbeat", json=body, headers=HI)

def test_heartbeat_upserts_and_lists(client):
    assert _hb(client).status_code == 200
    ws = client.get("/v1/workers", headers=H).json()
    assert ws[0]["worker_id"] == "w1" and ws[0]["status"] == "live" and ws[0]["seconds_since_seen"] < 5

def test_heartbeat_updates_max_sessions(client):
    _hb(client, max_sessions=20); _hb(client, max_sessions=8)
    assert client.get("/v1/workers", headers=H).json()[0]["max_sessions"] == 8

def test_dead_worker_fails_its_calls_and_hangs_up(client):
    # start a call bound to w1, backdate the heartbeat, sweep, expect failed/worker_lost and a carrier hangup
    ... call_id = (create outbound call, answer it, post started with worker_id="w1", worker_url="http://w1:8091")
    backdate workers.last_seen_at by 60 s via engine.db.session(); n = portal.call(service.sweep_workers)
    assert n == 1
    rec = client.get(f"/v1/calls/{call_id}", headers=H).json()
    assert rec["status"] == "failed" and rec["end_reason"] == "worker_lost"
    assert FakePlivo.hangups == ["cu-1"]          # add `hangups` list to FakePlivo.hangup

def test_late_ended_after_worker_lost_is_ignored(client):
    ... after the previous flow, post `ended` with a transcript of 2 turns
    rec = ...; assert rec["status"] == "failed" and len(rec["transcript"]) == 2

def test_refused_event_fails_call(client):
    ... create+answer a call, post {"type":"refused","data":{"reason":"capacity"}}
    assert rec["status"] == "failed" and rec["end_reason"] == "capacity" and FakePlivo.hangups[-1] == "cu-1"

def test_drain_proxies_to_worker(client, monkeypatch):
    _hb(client)
    seen = {}
    async def fake_post(self, url, **kw): seen["url"] = url; return httpx.Response(200, json={"draining": True})
    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    assert client.post("/v1/workers/w1/drain", headers=H).json()["draining"] is True
    assert seen["url"] == "http://w1:8091/internal/drain"
    assert client.get("/v1/workers", headers=H).json()[0]["draining"] is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_workers.py -v` → FAIL with 404 on the heartbeat route.

- [ ] **Step 3: Implement** the schemas, the four service methods, the `refused` and `started` handling, the three routes, and the 10 s sweeper cadence. `sweep_workers` selects workers with `last_seen_at < now - worker_dead_after_seconds` and `status == "live"`, sets `status="dead"`, then for calls where `status == in_progress` and (`worker_id == w.worker_id` or `worker_url == w.worker_url`) calls `finalize(status=failed, reason="worker_lost", error="worker heartbeat lost")` and `carriers.get(call.provider).hangup(call.provider_call_id)` inside try/except. `seconds_since_seen` uses `_aware()`.

- [ ] **Step 4: Run tests** → PASS. Run the whole suite → PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/api/service.py engine/api/app.py engine/schemas/calls.py tests/test_workers.py tests/test_api.py
git commit -m "feat(api): worker registry, heartbeats, dead-worker sweep, refused events"
```

---

### Task 3: Capacity model (two pools, CPS budget, 429 reasons, inbound busy)

**Files:**
- Create: `engine/api/capacity.py`
- Modify: `engine/api/service.py`, `engine/api/app.py`, `engine/schemas/calls.py`, `engine/carriers/base.py`
- Test: `tests/test_capacity.py`

**Interfaces:**
- Consumes: `Worker` rows and `Settings.carrier_limit/carrier_cps_limit` (Task 1), `WorkerHeartbeat` route (Task 2).
- Produces (`engine/api/capacity.py`): `def parse_limits(spec: str) -> dict[str, int]`; `class CpsBudget` with `__init__(self, rate_for: Callable[[str], int])` and `def try_acquire(self, provider: str, now: float | None = None) -> bool` (token bucket, capacity = rate, refill rate tokens/s); `@dataclass class CarrierPool: total: int; free: int; cps_free: int`; `@dataclass class CapacityView: engine_total: int; engine_free: int; carriers: dict[str, CarrierPool]; workers: list[dict]; legacy_active: int`.
- Produces (service): `async engine_slots(db) -> tuple[int, int]` returning `(free, total)`: if the `workers` table is empty → `total = settings.max_concurrent_calls`, `free = total - count(in_progress)`; else `total = sum(max_sessions)` over live non-draining workers and `free = max(0, total - max(sum(active_sessions), count(in_progress)))`; `async carrier_pool(db, provider) -> CarrierPool` where used = count of `dialing|ringing|in_progress` for that provider; `async capacity(db) -> CapacityView`; `self.cps = CpsBudget(settings.carrier_cps_limit)`; `async refuse_inbound(db, provider, info) -> Call` (creates a `failed`/`capacity` inbound call row and emits `call.failed` when the number has a webhook).
- Produces: `ServiceError(status_code, detail, headers: dict | None = None)`; `create_call` raises `ServiceError(429, {"reason": "engine_full"|"carrier_full"|"cps_exceeded", "detail": str}, headers={"Retry-After": "1"})`.
- Produces (schema): `Capacity` gains `engine: dict`, `carriers: dict`, `workers: list` (all default empty) while keeping `active_calls`, `max_concurrent_calls`, `available`, `accepting` (`available` = `min(engine_free, min carrier free)`).
- Produces (carrier): `Carrier.busy_xml(self, text: str = "All our lines are busy right now. Please try again in a few minutes.") -> str` returning `<Response><Speak>…</Speak><Hangup/></Response>`.
- Produces (route): inbound answer when `engine_free <= 0` returns `busy_xml` and records the refusal.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_capacity.py
from engine.api.capacity import CpsBudget, parse_limits

def test_parse_limits():
    assert parse_limits("plivo=50, vobiz=10") == {"plivo": 50, "vobiz": 10}

def test_cps_budget_allows_rate_per_second():
    b = CpsBudget(lambda p: 2)
    assert [b.try_acquire("plivo", now=100.0) for _ in range(3)] == [True, True, False]
    assert b.try_acquire("plivo", now=100.5) is True          # half a token refilled… rate 2/s → 1 token
    assert b.try_acquire("plivo", now=100.5) is False

def test_engine_full_returns_429_reason(client):           # heartbeat w1 with max_sessions=1, active_sessions=1
    r = client.post("/v1/calls", json=BODY, headers=H)
    assert r.status_code == 429 and r.json()["detail"]["reason"] == "engine_full" and r.headers["retry-after"] == "1"

def test_ringing_does_not_consume_engine_slot(client):     # heartbeat max 1 active 0; create call (dialing) → second create → carrier pool still free, engine free 1 → 201
def test_carrier_full_and_cps(tmp_path):                   # Settings(carrier_max_concurrent="plivo=1", carrier_cps="plivo=1") → 2nd call 429 carrier_full; with "plivo=5", two creates in the same second → second is cps_exceeded
def test_capacity_shape(client):                           # GET /v1/capacity has engine.free/total, carriers.plivo.free/total/cps_free, workers list, and the four legacy fields
def test_capacity_legacy_without_workers(client):          # no heartbeat → engine.total == settings.max_concurrent_calls
def test_inbound_busy_when_full(client):                   # heartbeat max 1 active 1; PUT number; GET answer inbound → "<Speak" and "<Hangup" in xml; latest call status failed/capacity
```

- [ ] **Step 2: Run** → FAIL (import error).

- [ ] **Step 3: Implement** `capacity.py`, the service methods, the 429 path (check order: engine, carrier, cps), the `ServiceError.headers` propagation in the exception handler, the `Capacity` fields, `busy_xml`, and the inbound branch in `carrier_answer`. Update `test_api.py::test_capacity_limit` only if its assertions depend on the old `accepting` semantics (they should still hold).

- [ ] **Step 4: Run** `tests/test_capacity.py` and the full suite → PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/api/capacity.py engine/api/service.py engine/api/app.py engine/schemas/calls.py engine/carriers/base.py tests/test_capacity.py
git commit -m "feat(api): two-pool capacity model with CPS budget, 429 reasons and inbound busy line"
```

---

### Task 4: Webhook outbox (database-backed dispatcher)

**Files:**
- Modify: `engine/api/webhooks.py`, `engine/api/service.py`, `engine/api/app.py`
- Test: `tests/test_outbox.py`, update `tests/test_webhooks_and_carriers.py::test_dispatcher_delivers_in_order_and_retries`

**Interfaces:**
- Consumes: `WebhookDelivery` table, `Call.webhook_seq` (Task 1).
- Produces: `class OutboxDispatcher` with `__init__(self, db: Database, *, timeout_s: float = 10.0, poll_interval_s: float = 0.5, http: httpx.AsyncClient | None = None)`, `async start()`, `async close()`, `def wake()`, `async enqueue(self, session: AsyncSession, call: Call, event_type: str, data: dict, *, secret: str) -> WebhookDelivery` (increments `call.webhook_seq`, builds `WebhookEvent` with that seq, inserts the row with `next_attempt_at=now`, commits, calls `wake()`), `async deliver_due(self) -> int` (one pass; used by tests), `async replay(self, session, call_id: str, seq: int) -> bool`.
- Behaviour: per call, only the lowest pending `seq` is eligible and only if no lower seq is `delivering`; retry delays `RETRY_DELAYS_S` indexed by `attempts`; `dead` after the last delay or on 4xx other than 408/429; rows claimed by setting `status="delivering"` in one UPDATE … WHERE status='pending' (Postgres: `with_for_update(skip_locked=True)` on the select).
- Produces (service): `_emit(self, db, call, etype, data)` now calls `enqueue` (every caller passes its `db`); `WebhookDispatcher` and `Delivery` are removed; `sign`/`verify`/`RETRY_DELAYS_S` stay.
- Produces (routes): `GET /v1/calls/{call_id}/deliveries` → list of `{id, seq, event, status, attempts, last_status_code, last_error, created_at, delivered_at, next_attempt_at}`; `POST /v1/calls/{call_id}/deliveries/{seq}/replay` → `{"ok": true}` or 404.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_outbox.py
@pytest.mark.asyncio
async def test_enqueue_allocates_seq_per_call(tmp_db, call_rows):   # two calls, three enqueues on A, one on B → seqs [1,2,3] and [1]
@pytest.mark.asyncio
async def test_delivers_in_order_and_retries(tmp_db):               # MockTransport fails first attempt of seq 2 with 500; after deliver_due loops: received == [1,2,3]; row 2 attempts == 2
@pytest.mark.asyncio
async def test_4xx_is_dead_immediately_except_408_429(tmp_db):      # 404 → dead after 1 attempt; 429 → pending with attempts 1
@pytest.mark.asyncio
async def test_dead_after_final_attempt_and_replay(tmp_db):         # always-500 sink, RETRY_DELAYS_S patched to (0,0); status dead; replay() → pending; then 200 sink → delivered
@pytest.mark.asyncio
async def test_survives_restart(tmp_db):                            # enqueue 2 rows with no dispatcher running; new OutboxDispatcher(db).deliver_due() delivers both in order
@pytest.mark.asyncio
async def test_outbox_unicode_payload_signature_verifies(tmp_db):   # data {"text": "हाँ, मैं आ रहा हूँ"}; sink verifies X-GVox-Signature over the raw body bytes and sees the same text
def test_deliveries_endpoints(client):                              # full flow via TestClient with a sink: GET lists rows with status delivered; replay a seq → 200 and attempts increments
```
`tmp_db` fixture: `Database(sqlite tmp)` with `await db.init()`.

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement** `OutboxDispatcher`, delete the in-memory classes, change `_emit` and all its call sites (`create_call`, `create_inbound_call`, `on_answer`, `on_ring`, `on_worker_event`, `finalize`, `refuse_inbound`) to pass `db`, wire `state.dispatcher = OutboxDispatcher(state.db)` in `lifespan`, add the two routes. Rewrite the old dispatcher test to the new API (same assertions).

- [ ] **Step 4: Run** the full suite → PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/api/webhooks.py engine/api/service.py engine/api/app.py tests/test_outbox.py tests/test_webhooks_and_carriers.py
git commit -m "feat(api): database-backed webhook outbox with per-call ordering, retries, dead-letter and replay"
```

---

### Task 5: Worker event spool (retry until acknowledged, survives restarts)

**Files:**
- Create: `engine/worker/spool.py`
- Modify: `engine/worker/events.py`, `engine/worker/main.py`
- Test: `tests/test_spool.py`

**Interfaces:**
- Produces: `class EventSpool` with `__init__(self, settings: Settings, http: httpx.AsyncClient, call_id: str, *, directory: str)`, `def append(self, event: dict) -> None` (appends one JSON line to `<directory>/<call_id>.jsonl` and nudges the sender), `def start(self) -> None`, `def mark_ended(self) -> None` (no more appends will come), `@property pending(self) -> int`, `async wait_idle(self, timeout: float | None) -> bool`, `async aclose(self) -> None`; module function `async def replay_pending(settings, http, directory: str, registry: dict[str, "EventSpool"]) -> int`.
- Sender semantics: POST `/internal/calls/{call_id}/events` per event in file order; 2xx → acked (write seq to `<call_id>.ack`); 4xx except 408/429 → acked and logged; 5xx, 408, 429, network error → retry with `min(10.0, 0.5 * 2**n)` seconds, forever. When `mark_ended()` was called and `pending == 0`, delete both files and remove itself from the registry.
- Produces (worker state): `W.spools: dict[str, EventSpool]`; `CallReporter.__init__` gains `spool: EventSpool`; `post()` calls `spool.append`; `start()` calls `spool.start()`; `close()` calls `spool.mark_ended()` and returns without waiting.
- Produces (startup): `build_server().prewarm` calls `replay_pending(settings, W.http, settings.spool_dir, W.spools)` once.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_spool.py  (httpx.MockTransport sinks; Settings(_env_file=None, internal_api_url="http://api"))
@pytest.mark.asyncio
async def test_spool_persists_and_acks(tmp_path):       # append 3 events; sink 200; wait_idle(2) True; .ack file contains 3; after mark_ended files are gone
@pytest.mark.asyncio
async def test_spool_retries_until_api_up(tmp_path):     # sink returns 503 for the first 4 calls then 200; patch backoff base to 0.01; all delivered in order; attempts > 3
@pytest.mark.asyncio
async def test_4xx_drops_event(tmp_path):                # sink 404 → pending 0, event acked
@pytest.mark.asyncio
async def test_replay_on_start_resends_unacked(tmp_path): # write a .jsonl with seq 1..3 and .ack "1" by hand; replay_pending → sink receives seq 2,3 only
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement** `spool.py`; rewire `CallReporter` (remove `_queue`, `_pump`, `_task`); construct the spool in `run_call` (`EventSpool(settings, W.http, call_id, directory=settings.spool_dir)`), register in `W.spools`; call `replay_pending` in `prewarm`. Create `settings.spool_dir` on worker start.

- [ ] **Step 4: Run** `tests/test_spool.py`, `tests/test_worker_offline.py` (the reporter listener test must still pass) → PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/worker/spool.py engine/worker/events.py engine/worker/main.py tests/test_spool.py
git commit -m "feat(worker): on-disk event spool that retries until the API acknowledges"
```

---

### Task 6: Worker lifecycle: id, heartbeat, cap refusal, drain, deploy files

**Files:**
- Create: `engine/worker/lifecycle.py`, `deploy/worker-entrypoint.sh`, `deploy/Caddyfile`
- Modify: `engine/worker/main.py`, `docker-compose.yml`, `Dockerfile`
- Test: `tests/test_lifecycle.py`

**Interfaces:**
- Consumes: heartbeat route (Task 2), `EventSpool.pending` and `W.spools` (Task 5).
- Produces: `class WorkerLifecycle` with `__init__(self, settings, http, *, worker_id: str, version: str, started_at: float, terminate: Callable[[], None] | None = None)`, attributes `draining: bool`, `worker_id`, methods `def should_refuse(self, active: int) -> Optional[str]` (returns `"draining"`, `"capacity"` or `None`), `async heartbeat_loop(self, active_fn: Callable[[], int], spool_pending_fn: Callable[[], int]) -> None` (POST every `worker_heartbeat_seconds`), `async begin_drain(self, handles_fn: Callable[[], list], end_fn: Callable[[Any, str], Awaitable[None]], spool_pending_fn) -> None` (sets `draining`, polls every 1 s until `len(handles_fn()) == 0 and spool_pending_fn() == 0` or `drain_timeout_seconds` elapsed; on timeout calls `end_fn(handle, "drain_timeout")` for each, waits ≤10 s, then calls `terminate()`), `def default_terminate() -> None` (`os.kill(os.getpid(), signal.SIGTERM)` on posix, `os._exit(0)` on win32).
- Produces (worker): `W.lifecycle`; `worker_id = settings.worker_id or socket.gethostname()`; `started` event data gains `worker_id`; entrypoint calls `W.lifecycle.should_refuse(len(W.active))` before `fetch_runtime` and, when refusing, posts `{"type": "refused", "data": {"reason": r, "worker_id": ...}}` to the API and `ctx.endpoint.hangup(ctx.session_id)`; routes `web.post("/internal/drain", _drain)` (token; returns `{"draining": true, "active": n, "spool_pending": m}`) and `/healthz` returns `{"status","version","worker_id","active_calls","max_sessions","draining","spool_pending"}`.
- Produces (deploy): `deploy/worker-entrypoint.sh` traps TERM, POSTs `/internal/drain` to `localhost:$WORKER_HTTP_PORT`, then `wait`s for the child (which self-terminates when idle); `Dockerfile` copies `deploy/` and uses the script when `ROLE=worker`; `docker-compose.yml` gains `postgres` (image `postgres:16`, volume), `caddy` (ports 8080/8090 published, `deploy/Caddyfile` mounted), `worker` and `worker-b` (profile `standby`, `WORKER_ID=worker-b`), `stop_grace_period: 16m` on workers, and `DATABASE_URL=postgresql+asyncpg://gvox:gvox@postgres:5432/gvox`; `deploy/Caddyfile` reverse-proxies `:8080 → api:8080` and `:8090 → worker:8090 worker-b:8090` with `lb_policy first` and `health_uri /` on 8091, plus a `@carriers remote_ip {$CARRIER_MEDIA_IPS}` matcher on :8090 that denies everything else when the env var is set.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_lifecycle.py
def test_should_refuse():
    lc = WorkerLifecycle(settings(worker_max_sessions=2), http=None, worker_id="w", version="t", started_at=0.0)
    assert lc.should_refuse(1) is None and lc.should_refuse(2) == "capacity"
    lc.draining = True
    assert lc.should_refuse(0) == "draining"

@pytest.mark.asyncio
async def test_begin_drain_exits_when_idle():        # handles_fn returns [] → terminate called within 2 s; draining True
@pytest.mark.asyncio
async def test_drain_timeout_ends_remaining_calls(): # settings(drain_timeout_seconds=1); handles_fn returns [h]; end_fn records ("drain_timeout"); terminate called
@pytest.mark.asyncio
async def test_heartbeat_posts_expected_body():      # MockTransport captures one POST to /internal/workers/heartbeat with worker_id, max_sessions=settings.worker_max_sessions, draining False; cancel loop
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement** `lifecycle.py`, wire it into `main.py` (`prewarm` starts the heartbeat task; `_drain` route starts `begin_drain` as a task once), the refusal branch in the entrypoint, the healthz fields. Write the deploy files and compose changes. Keep `_patch_windows_signal_handlers` as is.

- [ ] **Step 4: Run** tests → PASS. Manual check on Windows: start API and worker, `curl -X POST -H "X-Internal-Token: dev-internal-token" http://localhost:8091/internal/drain` with no calls → worker process exits within 2 s; `GET /v1/workers` showed it live before and `dead` 20 s after.

- [ ] **Step 5: Commit**

```bash
git add engine/worker/lifecycle.py engine/worker/main.py deploy docker-compose.yml Dockerfile tests/test_lifecycle.py
git commit -m "feat(worker): heartbeat, per-worker cap, drain to idle, blue/green compose with Caddy and Postgres"
```

---

### Task 7: STT and LLM failover, Flux circuit breaker, provider events

**Files:**
- Create: `engine/worker/breaker.py`
- Modify: `engine/schemas/agent.py`, `engine/schemas/calls.py`, `engine/worker/pipeline.py`, `engine/worker/events.py`, `engine/worker/main.py`, `engine/api/service.py`
- Test: `tests/test_failover.py`

**Interfaces:**
- Produces (schema): `ProviderRef.fallback: Optional[str] = None`; `TurnMetrics` gains `stt_provider: Optional[str] = None`, `llm_provider: Optional[str] = None`, `tts_provider: Optional[str] = None`; `CallMetrics.provider_events: list[dict] = []`.
- Produces (`breaker.py`): `class ProviderBreaker` with `__init__(self, open_seconds: float = 60.0, threshold: int = 1)`, `record_failure(key: str, now: float | None = None)`, `record_success(key)`, `is_open(key, now=None) -> bool`; module singleton `BREAKER = ProviderBreaker()`. Key for Flux: `"deepgram:flux"`.
- Produces (pipeline): `def resolve_fallback(kind: Literal["stt","llm"], ref: ProviderRef, settings) -> Optional[str]` (per-agent `ref.fallback` else `settings.{kind}_fallback_provider`; `None` when empty, equal to the primary, or unkeyed); `build_stt(ref, settings, language, *, vad=None, sample_rate: int = 16000)` returns `stt.FallbackAdapter([primary, fallback], vad=vad)` when a fallback resolves and the primary is not Flux; `build_llm(ref, settings)` returns `llm.FallbackAdapter([primary, fallback])` when one resolves; `def is_flux(ref: ProviderRef, settings) -> bool`; in `make_session`, when `is_flux(stt_ref)` and `BREAKER.is_open("deepgram:flux")`, substitute `ProviderRef(provider="deepgram", model="nova-3", fallback=stt_ref.fallback, options=stt_ref.options, api_key=stt_ref.api_key)`. `stt_provides_turns` must look through a `FallbackAdapter` (it never wraps Flux, so it may return False for adapters).
- Produces (reporter): `attach()` subscribes to `stt_availability_changed`, `llm_availability_changed`, `tts_availability_changed` on the session's components when present, posts `{"type": "provider", "data": {"kind", "provider", "available", "at"}}`, keeps `self.current_providers: dict[str, str]`, and stamps `stt_provider/llm_provider/tts_provider` onto each `TurnMetrics` it creates. On `session.on("error")` with an STTv2 source: `BREAKER.record_failure("deepgram:flux")` and `self.stt_failed = True`.
- Produces (worker): in `_on_close`, when `handle.reporter.stt_failed` and no `end_reason`, the reason is `"stt_failed"`.
- Produces (API): `on_worker_event` with `etype == "provider"` appends `data` to `call.metrics["provider_events"]` and commits.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_failover.py  (Settings(_env_file=None, deepgram_api_key="d", sarvam_api_key="s", google_api_key="g", groq_api_key="q", cartesia_api_key="c"))
def test_stt_fallback_adapter_built_when_keyed():      # build_stt(ProviderRef(provider="deepgram", model="nova-3")) is stt.FallbackAdapter
def test_flux_not_wrapped():                            # build_stt(model="flux-general-multi") is deepgram.STTv2
def test_breaker_substitutes_nova3_when_open():         # BREAKER.record_failure("deepgram:flux"); make_session(cfg with flux) → session.stt is FallbackAdapter; BREAKER reset afterwards
def test_llm_fallback_adapter_built():                  # build_llm(ProviderRef(provider="google", model="gemini-2.5-flash-lite")) is llm.FallbackAdapter
def test_no_fallback_without_key():                     # settings without sarvam key → plain deepgram.STT
def test_breaker_opens_and_recovers():                  # is_open False; record_failure(now=0) → True at 10; False at 61
def test_turn_metrics_carry_provider_fields():          # TurnMetrics(turn=1, stt_provider="deepgram").model_dump()["stt_provider"] == "deepgram"
def test_api_stores_provider_events(client):            # post {"type":"provider","data":{...}} → GET call metrics.provider_events has 1 item
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement** as specified. `tests/test_worker_offline.py::test_make_session_constructs_providers_without_network` must still pass; adjust its expectations only if it asserts a concrete STT class.

- [ ] **Step 4: Run** → PASS (full suite).

- [ ] **Step 5: Commit**

```bash
git add engine/worker/breaker.py engine/schemas/agent.py engine/schemas/calls.py engine/worker/pipeline.py engine/worker/events.py engine/worker/main.py engine/api/service.py tests/test_failover.py
git commit -m "feat(worker): STT/LLM fallback adapters, Flux circuit breaker, per-turn provider attribution"
```

---

### Task 8: Audio: proper resampler, telephony degradation, 8 kHz STT input

**Files:**
- Modify: `engine/worker/audio.py`, `engine/worker/browser.py`, `engine/worker/pipeline.py`, `engine/worker/main.py`
- Test: `tests/test_audio_telephony.py`

**Interfaces:**
- Produces (`audio.py`): `class StreamResampler` with `__init__(self, src_rate: int, dst_rate: int)` (wraps `rtc.AudioResampler(src, dst, quality=AudioResamplerQuality.HIGH)`; identity when rates match), `def process(self, pcm: bytes) -> bytes`, `def flush(self) -> bytes`; `resample_pcm16(pcm, src_rate, dst_rate)` keeps its signature and is now `StreamResampler(src, dst).process(pcm) + flush()`; `def bandpass_pcm16(pcm: bytes, rate: int, low_hz: float = 300.0, high_hz: float = 3400.0, taps: int = 101) -> bytes` (windowed-sinc FIR, numpy); `def telephony_degrade(pcm16: bytes, rate: int) -> bytes` returning 8 kHz PCM16 after `bandpass_pcm16` → resample to 8000 → `pcm16_to_mulaw` → `mulaw_to_pcm16`.
- Produces (pipeline): `make_session(..., input_sample_rate: int = 16000)` passes `sample_rate=input_sample_rate` into `build_stt` which forwards it to `deepgram.STT` / `deepgram.STTv2` / `sarvam.STT` via `_construct` (dropped where unsupported).
- Produces (worker): carrier entrypoint calls `make_session(..., input_sample_rate=8000)`; browser keeps 16000. `browser.py` uses one `StreamResampler` per direction instead of `codec.resample_pcm16` per frame.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_audio_telephony.py
def _snr_db(ref, out): ...  # numpy, aligned by trimming group delay via cross-correlation
def test_stream_resampler_roundtrip_snr():
    tone = _sine_pcm(8000, 1.0, freq=1000.0)
    up = StreamResampler(8000, 16000); down = StreamResampler(16000, 8000)
    back = down.process(up.process(tone) + up.flush()) + down.flush()
    assert _snr_db(tone, back) >= 35.0

def test_stream_resampler_is_stateful_across_frames():   # feeding 50 × 20 ms frames == feeding 1 s at once (byte-equal after flush)
def test_telephony_degrade_bandlimits():                  # 16 kHz input = 1 kHz + 3.8 kHz tones; output 8 kHz; FFT bin power at 3.8 kHz ≥ 20 dB below 1 kHz
def test_make_session_passes_input_sample_rate():        # make_session(cfg nova-3, settings with keys, input_sample_rate=8000).stt (unwrap FallbackAdapter) has sample_rate == 8000
```
Keep `tests/test_audio_protocol_guards.py::test_resample_pcm16` passing (length and identity assertions).

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement**. The FallbackAdapter in Task 7 exposes its STTs via a private list; for the test, add `def unwrap_stt(obj) -> Any` in `pipeline.py` returning the first underlying STT when `obj` is a `FallbackAdapter`.

- [ ] **Step 4: Run** → PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/worker/audio.py engine/worker/browser.py engine/worker/pipeline.py engine/worker/main.py tests/test_audio_telephony.py
git commit -m "feat(audio): soxr-backed resampler, telephony band-limit helper, 8 kHz STT input on the carrier path"
```

---

### Task 9: Two-channel recording, object storage, retention

**Files:**
- Create: `engine/storage.py`
- Modify: `engine/worker/recording.py`, `engine/worker/main.py`, `engine/api/app.py`, `engine/api/service.py`
- Test: `tests/test_recording_storage.py`

**Interfaces:**
- Produces (`storage.py`): `class Storage(Protocol)` with `async put_file(self, local_path: str, key: str) -> None`, `async get_bytes(self, key: str) -> bytes`, `async exists(self, key: str) -> bool`, `async delete(self, key: str) -> None`, `def signed_url(self, key: str, expires_s: int = 900) -> Optional[str]`; `class LocalStorage(root: str)`; `class S3Storage(bucket, endpoint, region, access_key, secret_key)` (boto3 in `asyncio.to_thread`, imported lazily); `def build_storage(settings) -> Storage`; `def recording_key(call_id: str, when: datetime) -> str` → `recordings/YYYY/MM/DD/<call_id>.wav`.
- Produces (`recording.py`): `class StereoRecorder` with `__init__(self, base_path: str, max_seconds: int)`, `add_caller(pcm: bytes, rate: int)`, `add_agent(pcm: bytes, rate: int)`, `agent_interrupted()`, `def finish(self) -> Optional[str]` (interleaves `<base>.caller.raw` and `<base>.agent.raw` into `<base>.wav`, 2 channels, 8 kHz, 16-bit; removes the raws; returns the wav path or `None` when empty), `frames_in`, `frames_out`; `TeeAudioInput(source, recorder)` and `RecordingAudioOutput(next_in_chain, recorder)` keep their class names; `def remove_stale_raw(dir: str) -> int`. `Mixer` is deleted.
- Produces (worker `finalize`): `path = recorder.finish()`; `key = recording_key(call_id, utcnow())`; `await storage.put_file(path, key)`; remove the local file; `reporter.ended(..., recording_path=key)`. On upload failure keep the local file and send `recording_path=path` (legacy local).
- Produces (API): `GET /v1/calls/{id}/recording`: if `recording_path.startswith("recordings/")` → `StreamingResponse` from `storage.get_bytes` (or `RedirectResponse(signed_url)` when `recording_url_mode == "signed"` and the backend returns one), else legacy `FileResponse` when the path exists; `service.sweep_recordings(db, storage) -> int` deletes objects for calls with `ended_at < now - recording_retention_days` and `recording_path` set, nulls the column; the sweeper runs it on startup and every 24 h.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_recording_storage.py
def test_stereo_recorder_writes_two_channels(tmp_path):   # add_caller 1 kHz tone 0.5 s at 16 kHz; add_agent 440 Hz 0.5 s at 24 kHz; finish → wave nchannels 2, framerate 8000; left energy >> right in first… (agent starts at cursor≥now, so check both channels non-silent)
def test_agent_interrupt_truncates_tail(tmp_path):        # add_agent 2 s; agent_interrupted() immediately; finish → right channel length ≈ elapsed, not 2 s
def test_finish_tolerates_missing_agent_file(tmp_path):   # only caller frames → finish returns a stereo wav with silent right channel
def test_stale_raw_files_removed_on_start(tmp_path):      # create x.caller.raw; remove_stale_raw(dir) == 1
@pytest.mark.asyncio
async def test_local_storage_put_get_delete(tmp_path):    # put_file, exists True, get_bytes equal, delete, exists False
def test_recording_key():                                 # recording_key("call_a", datetime(2026,10,22,...)) == "recordings/2026/10/22/call_a.wav"
def test_retention_sweep_deletes_old(client, tmp_path):   # call with recording_path key and ended_at 100 days ago; object exists in LocalStorage; sweep → 1, object gone, GET recording → 404
def test_recording_endpoint_streams_from_storage(client): # put a wav under the key; GET /v1/calls/{id}/recording → 200 audio/wav body equals bytes
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement**. `StereoRecorder` places caller audio at `int((monotonic - t0) * 8000)` samples (pad with zeros when behind, overwrite when overlapping) into the caller raw file using `seek`; agent audio at `max(cursor, now_idx)` appended, `agent_interrupted()` truncates the agent raw at `now_idx`. Both channels use a `StreamResampler` (Task 8) per channel. `W.storage = build_storage(settings)` in `prewarm`; `state.storage` in the API lifespan; `remove_stale_raw(settings.recordings_dir)` on worker start.

- [ ] **Step 4: Run** → PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/storage.py engine/worker/recording.py engine/worker/main.py engine/api/app.py engine/api/service.py tests/test_recording_storage.py
git commit -m "feat(recording): two-channel 8 kHz WAV written during the call, object storage backends, retention sweep"
```

---

### Task 10: Security minimums

**Files:**
- Create: `engine/carriers/signatures.py`, `engine/api/crypto.py`
- Modify: `engine/carriers/base.py`, `engine/carriers/plivo.py`, `engine/carriers/vobiz.py`, `engine/api/app.py`, `engine/api/service.py`, `engine/worker/main.py`, `ui/app.js`
- Test: `tests/test_security.py`

**Interfaces:**
- Produces (`signatures.py`, a port of plivo-python `signature_v3.py`, MIT): `def plivo_v3_base_string(method: str, url: str, params: dict[str, Any]) -> str` (GET: scheme+netloc+path, then `?` + sorted `k=v&k=v` of query∪params; POST: scheme+netloc+path, `?query` if present, then `.` + sorted `kv` pairs concatenated, no separators; list values sorted and repeated), `def plivo_v3_signature(auth_token: str, base_string: str, nonce: str) -> str` (base64 of HMAC-SHA256 over `f"{base_string}.{nonce}"`, stripped), `def verify_plivo_v3(auth_token: str, method: str, url: str, nonce: str, signature_header: str, params: dict[str, Any]) -> bool` (true when the computed value is in `signature_header.split(",")`, compared with `hmac.compare_digest`), `def ip_in_allowlist(ip: str, allowlist: str) -> bool` (comma-separated IPs/CIDRs).
- Produces (carriers): `Carrier.verify_callback(self, *, method: str, public_url: str, headers: Mapping[str, str], params: dict[str, Any], remote_ip: str) -> bool`; base returns `True`; `PlivoCarrier` verifies `X-Plivo-Signature-V3` + `X-Plivo-Signature-V3-Nonce` with its auth token, falls back to V2 (`X-Plivo-Signature-V2`, base64 HMAC over `url_without_query + nonce`) when only V2 headers are present, otherwise `False`; `VobizCarrier` uses the V3 path when the headers exist, else `ip_in_allowlist(remote_ip, settings.vobiz_callback_ips)`; `FakeCarrier` (Task 11) returns `True`.
- Produces (API): `_verify_carrier(carrier, request, params)` runs before any processing in the three carrier routes when `settings.carrier_verify_signatures`; the URL passed is `settings.public_http_url.rstrip("/") + request.url.path + ("?" + request.url.query if request.url.query else "")`; failure → `HTTPException(403, "carrier signature invalid")`. `require_api_key` uses `hmac.compare_digest`. `/ui/config.json` gets `dependencies=[Depends(require_api_key)]`.
- Produces (`crypto.py`): `class SecretBox` with `__init__(self, key: str)` (empty key → passthrough mode that logs one warning), `encrypt(self, value: str) -> str` (prefix `enc:v1:`), `decrypt(self, value: str) -> str` (passthrough when unprefixed), `is_encrypted(value) -> bool`, `@staticmethod mask(value: str) -> str` (`"****" + last 4`, or `"****"` when shorter than 8); `def encrypt_agent_config(cfg: dict, box) -> dict` / `decrypt_agent_config(cfg: dict, box) -> dict` / `mask_agent_config(cfg: dict) -> dict` operating on `stt.api_key`, `llm.api_key`, `tts.api_key`, `realtime.api_key`.
- Produces (service): `self.box = SecretBox(settings.engine_encryption_key)`; `put_agent` stores the encrypted config; `get_agent` returns the stored row (encrypted); API `GET /v1/agents/{id}` and `/versions` return masked configs; `resolve_agent`, `runtime_payload` and the browser path decrypt; `create_call`/`put_number` encrypt `webhook_secret`; `_emit` decrypts before signing. CLI `gvox-api encrypt-existing` re-encrypts plaintext rows; `gvox-api` becomes a Typer app with `serve` (default when no subcommand), `migrate`, `encrypt-existing`, `copy-sqlite --from PATH`.
- Produces (worker): aiohttp middleware in `GvoxWorkerServer._build_http_app` returning 401 on `/metrics` without `X-Internal-Token`.
- Produces (UI): `loadConfig()` fetches `/ui/config.json` through `api()`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_security.py
def test_plivo_v3_post_matches_documented_example():
    # Plivo docs example: url https://example.com/abcd?foo=bar, params CallUuid/Digits/From/To (sorted), nonce "n1", token "t0k"
    base = plivo_v3_base_string("POST", "https://example.com/abcd?foo=bar", PARAMS)
    assert base == "https://example.com/abcd?foo=bar.CallUuid4vbcpem8-0u46-x1ha-9af1-438vc92bf374Digits1234From+15551111111To+15555555555"
    expected = base64.b64encode(hmac.new(b"t0k", (base + ".n1").encode(), hashlib.sha256).digest()).decode()
    assert plivo_v3_signature("t0k", base, "n1") == expected
    assert verify_plivo_v3("t0k", "POST", "https://example.com/abcd?foo=bar", "n1", expected + ",other", PARAMS)

def test_plivo_v3_get_merges_query_sorted():   # GET https://x/a?b=2&a=1 with params {} → base "https://x/a?a=1&b=2"
def test_plivo_v3_uses_public_url(client):      # settings public_http_url="https://api.test"; sign the answer URL "https://api.test/carrier/plivo/answer?call_id=…" with token "y"; request via TestClient (host testserver) with the headers → 200; wrong nonce → 403
def test_unsigned_carrier_callback_403(client):  # GET /carrier/plivo/answer?call_id=… without headers → 403 when verification on
def test_vobiz_ip_allowlist():                  # ip_in_allowlist("10.1.2.3", "10.1.2.0/24, 1.1.1.1") True; "10.2.0.1" False
def test_secretbox_roundtrip_and_mask():        # Fernet key; encrypt→decrypt identity; encrypted startswith "enc:v1:"; mask("sk-abcdef123456") == "****3456"
def test_agent_keys_encrypted_at_rest_and_masked_on_get(client_with_key):  # PUT agent with stt.api_key "dg-secret-key-1234" → DB row config stt.api_key startswith enc:v1:; GET shows "****1234"; /internal/calls/{id}/runtime returns the plaintext
def test_ui_config_requires_key(client):        # GET /ui/config.json → 401; with H → 200
```
The existing `tests/test_api.py` fixture must set `carrier_verify_signatures=False` so unrelated tests keep posting unsigned callbacks; `test_security.py` builds its own client with verification on.

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement** as specified. Note `request.client.host` is the remote IP; honour `X-Forwarded-For`'s first hop only when `request.client.host` is a private address (the Caddy proxy).

- [ ] **Step 4: Run** → PASS (full suite).

- [ ] **Step 5: Commit**

```bash
git add engine/carriers/signatures.py engine/api/crypto.py engine/carriers engine/api/app.py engine/api/service.py engine/worker/main.py ui/app.js tests/test_security.py tests/test_api.py
git commit -m "feat(security): Plivo V3 callback verification, encrypted BYOK keys and secrets, authenticated config and metrics"
```

---

### Task 11: Fake carrier, Plivo-protocol test client, telephony bench

**Files:**
- Create: `engine/carriers/fake.py`, `scripts/fake_carrier.py`, `scripts/assets/tel_user_yes.wav`, `scripts/assets/tel_user_no.wav`, `scripts/assets/tel_user_nokids.wav`
- Modify: `engine/carriers/registry.py`, `engine/worker/main.py`, `engine/api/app.py`, `scripts/make_utterance.py`, `scripts/latency_bench.py`, `scripts/browser_smoke.py` (only `--path` dispatch)
- Test: `tests/test_fake_carrier.py`

**Interfaces:**
- Produces (`fake.py`): `class FakeCarrier(Carrier)` with `name = "fake"`, `from_number = "+910000000000"`, `configured()` → `settings.enable_test_endpoints`, `dial()` → `DialResult(provider_call_id=f"fake-{uuid4().hex[:12]}")`, `hangup()` → `True` and appends to `self.hangups`, `verify_callback()` → `True`. Registered in `registry._CARRIERS`.
- Produces (worker entrypoint): when `runtime["provider"] == "fake"` and the start frame's `X-Test-Token` extra header != `settings.internal_token` → hang up and return.
- Produces (`scripts/fake_carrier.py`): `python scripts/fake_carrier.py [--api URL] [--key KEY] [--internal-token T] [--worker-ws ws://127.0.0.1:8090] [--calls N] [--minutes M] [--script yes,no] [--agent example|prompt] [--wav-dir scripts/assets] [--json]`. Per call: `POST /v1/calls` (provider `fake`, webhook none) → `GET {answer_url}&CallUUID=<provider_call_id>` → assert `<Stream` → open WS → send `start` frame (`streamId`, `callId=<provider_call_id>`, `mediaFormat {encoding: audio/x-mulaw, sampleRate: 8000}`, `extra_headers: "X-Call-Id=<call_id>,X-Test-Token=<token>"`) → stream 20 ms mu-law frames: silence until the agent's first `playAudio` burst ends (no audio for 1.2 s), then the next scripted WAV (8 kHz mono), repeat → after the script, `{"event":"stop"}` and close → `POST {hangup_url}` with `CallUUID`, `Duration`, `HangupCauseCode=4000` → `GET /v1/calls/{id}`. Measures client-side `speech_end → first playAudio` per turn and reads `metrics.turns[].total` from the record. Reports a table (calls, completed, dropped, p50/p95 client latency, p50/p95 engine total) and exits 1 when any call is not `completed` or any turn timed out (30 s).
- Produces: `make_utterance.py --telephony IN16K.wav OUT8K.wav` (uses `telephony_degrade`) and `make_utterance.py "text" out.wav --telephony` (synthesize then degrade); the three `tel_*.wav` assets generated from the existing 16 kHz assets.
- Produces: `latency_bench.py --path telephony|browser` (default `browser` until the README table is replaced in Task 12, then `telephony`); telephony runs call `fake_carrier.py --calls 1 --json`.
- Produces (API, gated by `enable_test_endpoints`): `POST /v1/test/telephony-smoke` (internal token) starts `fake_carrier.py --calls 3 --script yes --json` as a subprocess and returns `{"job_id"}`; `GET /v1/test/telephony-smoke/{job_id}` returns `{"status": "running"|"done"|"failed", "result": {...}}`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_fake_carrier.py
def test_fake_dial_answer_and_hangup(client_test_endpoints):   # Settings(enable_test_endpoints=True); POST /v1/calls provider fake → 201 dialing, provider_call_id startswith "fake-"; GET answer → "<Stream"; POST hangup → completed/busy mapping as Plivo
def test_fake_not_configured_by_default(client):                # provider fake → 503
def test_fake_carrier_frames():                                 # from scripts.fake_carrier import start_frame, media_frame; parse_start(json.loads(start_frame(...))).extra_headers["X-Call-Id"] == "call_1"; media_payload(json.loads(media_frame(b"\xff"*160))) decodes to 160 bytes
def test_fake_carrier_rejected_without_test_token():            # unit test of the predicate `should_reject_fake(runtime, headers, settings) -> bool` exported from main.py
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement**. `scripts/fake_carrier.py` must be importable (`start_frame`, `media_frame`, `run(args) -> dict` at module level; `main()` under `__main__`). Generate the `tel_*.wav` assets with the new `make_utterance.py --telephony`.

- [ ] **Step 4: Run tests** → PASS. Live verification (needs `.env` keys, costs about 3 call-minutes): start API with `ENABLE_TEST_ENDPOINTS=true` and the worker, run `python scripts/fake_carrier.py --calls 1 --script yes`. Expected: table with 1 completed call, 2 agent turns, non-null p50; the call shows in `GET /v1/calls` with a two-channel recording.

- [ ] **Step 5: Commit**

```bash
git add engine/carriers/fake.py engine/carriers/registry.py engine/worker/main.py engine/api/app.py scripts tests/test_fake_carrier.py
git commit -m "feat(test): fake carrier, Plivo-protocol load/smoke client, telephony audio assets and bench path"
```

---

### Task 12: Health, API metrics, runbook, README, data copy

**Files:**
- Modify: `engine/api/app.py`, `engine/api/service.py`, `README.md`, `.env.example`
- Create: `docs/runbook.md`
- Test: `tests/test_ops.py`

**Interfaces:**
- Produces: `GET /health` → `{"status", "version", "active_calls", "db": "ok"|"error", "workers_live": int, "outbox_pending": int, "outbox_oldest_s": float|null}`; `GET /metrics` (internal token) → Prometheus text with gauges `gvox_calls{status=…}`, `gvox_engine_free`, `gvox_engine_total`, `gvox_carrier_free{provider=…}`, `gvox_outbox_pending`, `gvox_outbox_dead`, `gvox_workers{status=…}`, refreshed by the sweeper tick via `service.refresh_metrics(db)`.
- Produces: `gvox-api copy-sqlite --from PATH` copies `agent_versions`, `numbers`, `calls` rows into the configured `DATABASE_URL`.
- Produces (docs): `docs/runbook.md` with sections: deploy with drain (blue/green on Compose), worker died, replay webhooks, read capacity, run the load test, rotate the encryption key, configure carrier IP allowlists; README: new env vars, Postgres default, test commands (`fake_carrier.py`, `latency_bench.py --path telephony`), and the latency table replaced by a placeholder row stating it is produced by the telephony bench on the deploy box.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_ops.py
def test_health_shape(client):          # keys present; db == "ok"
def test_metrics_requires_token_and_exposes_gauges(client):  # /metrics → 401; with HI → 200 and "gvox_engine_total" in text
def test_copy_sqlite(tmp_path):         # build two sqlite DBs; insert one agent and one call in src via Database; run copy_sqlite(src_url, dst_url); dst has both rows
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement**; write the runbook and README changes.

- [ ] **Step 4: Run** → PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/api/app.py engine/api/service.py README.md .env.example docs/runbook.md tests/test_ops.py
git commit -m "feat(ops): health detail, Prometheus metrics, runbook, sqlite copy command"
```

---

### Task 13: Minimal local test UI (Ops tab)

**Files:**
- Modify: `ui/index.html`, `ui/app.js`, `ui/style.css`

**Interfaces:**
- Consumes: `GET /v1/workers`, `POST /v1/workers/{id}/drain` (Task 2), `GET /v1/capacity` (Task 3), `GET /v1/calls/{id}/deliveries`, replay (Task 4), `metrics.turns[].*_provider` (Task 7), `POST/GET /v1/test/telephony-smoke` (Task 11, internal token entered in a field, stored in localStorage).
- Produces: nav button `data-tab="ops"`; section `#tab-ops` with cards **Workers** (table + Drain button per row), **Capacity** (engine and per-carrier pools), **Deliveries** (for `state.selectedCallId`, set by `showCall`; Replay per row), **Telephony smoke** (token input, Run button, result table); 5 s refresh while the tab is active; Calls tab per-turn table gains a `providers` column rendering `stt/llm/tts` short names.

- [ ] **Step 1: Implement** the markup, the `refreshOps()` poller, the handlers, and the column.

- [ ] **Step 2: Verify manually**: with API and worker running, open `http://localhost:8080/ui/`, Ops tab shows the worker live with 0/20, capacity engine 20/20; click Drain → the worker row shows draining then disappears after it exits; select a call in Calls → Deliveries lists its rows; Replay re-sends one; with `ENABLE_TEST_ENDPOINTS=true` the smoke button returns a 3-call table.

- [ ] **Step 3: Commit**

```bash
git add ui
git commit -m "feat(ui): Ops tab with workers, capacity, deliveries and telephony smoke"
```

---

### Task 14: Verification pass

**Files:** none new.

- [ ] **Step 1: Full offline suite and lint**

Run: `.\.venv\Scripts\python.exe -m pytest -q` → all pass. Run: `.\.venv\Scripts\python.exe -m ruff check engine tests scripts` → clean.

- [ ] **Step 2: Local drills (Windows, real keys, small)**

1. Start API (`ENABLE_TEST_ENDPOINTS=true`, `CARRIER_VERIFY_SIGNATURES=true`) and worker. `python scripts/fake_carrier.py --calls 3 --script yes,no` → 3 completed, p50 reported. Save the output in the final report.
2. Restart drill: run `--calls 2 --minutes 1`, kill the API for 30 s mid-run, restart; afterwards `GET /v1/calls/{id}/deliveries` for both calls shows contiguous `seq` and the calls are `completed`.
3. Worker-loss drill: run `--calls 1`, kill the worker process; within 30 s the call is `failed`/`worker_lost`.
4. Drain drill: run `--calls 2 --minutes 1`, POST `/internal/drain`; both calls finish, then the worker exits.
5. Security: an unsigned `curl` to `/carrier/plivo/answer?call_id=x` → 403.

- [ ] **Step 3: Record outcomes** in `docs/runbook.md` under "Last local verification" with the date and the three numbers (p50, p95, calls completed), and list what was not run locally (25-call load test, Linux SIGTERM wrapper, S3 backend).

- [ ] **Step 4: Commit**

```bash
git add docs/runbook.md
git commit -m "docs: record local verification of slice 1"
```
