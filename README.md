# Notify Queue

Distributed delayed job & notification delivery system — schedules
email/SMS/push notifications for a future time or delay, guarantees
exactly-once delivery across multiple concurrent worker instances, supports
priority ordering, per-recipient rate limiting, exponential-backoff retries
with a dead-letter cap, idempotent submission, webhook status callbacks, and
job-status/metrics endpoints.

See **[DESIGN.md](./DESIGN.md)** for architecture, the exactly-once
guarantee in detail, and simplifying assumptions.

## Stack

Node.js · Express · **BullMQ** (Redis-backed priority queue + distributed
locking) · PostgreSQL (durable job state / idempotency) · Redis (queue +
rate-limit counters) · Jest (tests)

## Prerequisites

- Node.js 18+
- Docker (for Postgres + Redis) — or your own local Postgres/Redis instances

## 1. Start Postgres + Redis

```bash
docker compose up -d
```

This starts Postgres on `5432` (db `notifyqueue`, user/pass `notify`/`notify`)
and Redis on `6379`, matching the defaults in `.env.example`.

(If you already have Postgres/Redis running locally, just point `.env` at
them instead — no code changes needed.)

## 2. Install dependencies & configure environment

```bash
npm install
cp .env.example .env
```

## 3. Run migrations

```bash
npm run migrate
```

This creates the `jobs` and `webhook_logs` tables (see
`migrations/001_init.sql`).

## 4. (Optional) Seed sample data

```bash
npm run seed
```

Schedules a handful of sample jobs across channels/priorities, including a
deliberately duplicated idempotency key so you can see de-duplication happen
in the logs.

## 5. Start the API

```bash
npm start
```

Listens on `http://localhost:3000` (configurable via `PORT`).

## 6. Start worker instances — **run more than one to see exactly-once in action**

Open separate terminals (or background processes) and run:

```bash
# terminal 2
WORKER_ID=worker-1 npm run worker

# terminal 3
WORKER_ID=worker-2 npm run worker

# terminal 4 — add as many as you like, they all read from the same queue
WORKER_ID=worker-3 npm run worker
```

Every instance connects to the same Redis-backed BullMQ queue and competes
for the same jobs; BullMQ's per-job lock plus the DB-level atomic claim
(see DESIGN.md §4) guarantee only one of them ever actually sends a given job.
Watch the logs — you'll see `job ... already claimed by another
worker/attempt - skipping` on the losers of any race.

`WORKER_CONCURRENCY` (in `.env`) controls how many jobs a single worker
process handles in parallel internally, independent of how many worker
*processes* you run.

## API reference

### `POST /jobs` — schedule a notification

```bash
curl -X POST http://localhost:3000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "recipient": "alice@example.com",
    "channel": "email",
    "payload": { "template": "welcome", "name": "Alice" },
    "delaySeconds": 30,
    "priority": 2,
    "idempotencyKey": "welcome-alice-2026-07-08"
  }'
```

Either `sendAt` (ISO timestamp) or `delaySeconds` is required.
`priority` is 1 (highest) – 10 (lowest), default 5.
`idempotencyKey` is optional — one is generated for you if omitted, but you
won't get de-duplication across retries unless you supply your own.

Resubmitting the same `idempotencyKey` returns the original job
(`"deduplicated": true`, HTTP 200) instead of creating/scheduling a new one.

### `GET /jobs/:id` — check job status

```bash
curl http://localhost:3000/jobs/<job-id>
```

Returns `status` (`pending | processing | sent | failed | dead_letter`),
`attempts`, `lastError`, timestamps, etc.

### `GET /metrics` — lightweight counts

```bash
curl http://localhost:3000/metrics
```

```json
{ "pending": 3, "processing": 1, "sent": 12, "failed": 0, "dead_lettered": 1, "total": 17 }
```

### `POST /webhook/mock` — mocked webhook receiver

The system itself calls this endpoint (configurable via `WEBHOOK_URL`)
whenever a job's status changes to `sent`, `failed`, or `dead_lettered`. It
just logs what it receives — point `WEBHOOK_URL` at a real endpoint to see
the same callback delivered elsewhere.

## Running the tests

```bash
npm test
```

Requires Postgres + Redis reachable via `.env` (same as running the app).
Includes:

- `tests/concurrency.test.js` — **the key test**: spins up 25 concurrent
  claim attempts against the *same* job row (simulating many worker
  instances racing) and asserts exactly one succeeds and the job is never
  double-processed.
- `tests/idempotency.test.js` — duplicate idempotency keys, including under
  concurrent submission, only ever create one row.
- `tests/rateLimiter.test.js` — per-recipient hourly cap is enforced.

## Configuration reference (`.env`)

| Variable | Meaning | Default |
|---|---|---|
| `PORT` | API port | `3000` |
| `DATABASE_URL` | Postgres connection string | see `.env.example` |
| `REDIS_URL` | Redis connection string | `redis://localhost:6379` |
| `WORKER_CONCURRENCY` | Jobs processed in parallel per worker process | `5` |
| `SIMULATED_FAILURE_RATE` | Mock sender random failure rate (0–1) | `0.3` |
| `MAX_ATTEMPTS` | Default retry cap before dead-letter | `5` |
| `BASE_BACKOFF_MS` | Base delay for exponential backoff | `2000` |
| `RATE_LIMIT_MAX_PER_HOUR` | Max sends per recipient per hour | `5` |
| `WEBHOOK_URL` | Where status-change callbacks are POSTed | mocked local endpoint |

## Project structure

```
notify-queue/
├── DESIGN.md
├── README.md
├── docker-compose.yml
├── package.json
├── .env.example
├── migrations/
│   └── 001_init.sql
├── seed.js
├── src/
│   ├── server.js        # Express API bootstrap
│   ├── worker.js         # worker process (run many instances of this)
│   ├── queue.js          # BullMQ queue + connection setup
│   ├── db.js             # Postgres pool + migration runner
│   ├── config.js
│   ├── models/
│   │   └── job.js        # DB access, idempotency, atomic claim
│   ├── services/
│   │   ├── sender.js      # mock notification sender
│   │   ├── rateLimiter.js # per-recipient hourly cap (Redis)
│   │   └── webhook.js     # status-change callback dispatcher
│   └── routes/
│       ├── jobs.js
│       ├── webhook.js
│       └── metrics.js
└── tests/
    ├── concurrency.test.js
    ├── idempotency.test.js
    └── rateLimiter.test.js
```
