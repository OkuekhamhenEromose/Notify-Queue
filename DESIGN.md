# DESIGN.md — Notify Queue

Distributed delayed job & notification delivery system.
Built for the Expert Listing Backend Engineer assessment (Stage One).

## 1. Goals recap

- Schedule notification jobs (email/SMS/push) for a future time or after a delay.
- Multiple concurrent worker processes must be able to poll/deliver without ever
  double-sending the same job (exactly-once).
- High-priority jobs must jump ahead of lower-priority jobs that are also due.
- Failed sends retry with exponential backoff up to a cap, then move to a dead-letter state.
- Duplicate submissions (same idempotency key) must not schedule/send twice.
- Per-recipient rate limiting — excess jobs queue rather than fail.
- Webhook callback on every status change; job-status and metrics endpoints.

## 2. High-level architecture

```
                 ┌─────────────┐
   POST /jobs ─▶ │   API (Express)  │──▶ Postgres (jobs table, source of truth)
                 └─────────────┘         │
                        │                └─▶ enqueue into BullMQ (Redis)
                        ▼
                 ┌─────────────────────────────┐
                 │   Redis (BullMQ)             │
                 │  - delayed set (send_at)     │
                 │  - priority-ordered ready set │
                 │  - per-job lock while active   │
                 └─────────────────────────────┘
                        ▲            ▲
              worker-1  │            │  worker-2 ... worker-N
              (poll, claim, send, ack/retry/dead-letter)
```

Two data stores, two different jobs:

- **Postgres** is the durable **source of truth** for job state: what was
  requested, its idempotency key, current status, attempt count, last error.
  It's what the status/metrics endpoints read from, and what an operator would
  query during an incident.
- **Redis (via BullMQ)** is the **scheduling and coordination layer**: it holds
  the delayed/ready sets, applies priority ordering, and — critically — owns
  the distributed lock that makes concurrent worker polling safe.

This separation is deliberate: Redis is fast but only *ordering/coordination*
memory (jobs can be safely reconstructed/replayed from Postgres if Redis were
ever flushed in a real system, though that recovery path is out of scope here
— see "Simplifying assumptions" below).

## 3. Why BullMQ instead of hand-rolled polling

The assessment explicitly asks for "proper distributed locking or claim-based
job assignment, no relying on a single-process-only assumption." I chose to
build on **BullMQ** (Redis-backed queue library) rather than write my own
`SELECT ... FOR UPDATE SKIP LOCKED`-style poller, for a concrete reason: BullMQ
already implements the hard part — atomic claim + lease renewal — using
battle-tested Lua scripts, and exposes exactly the primitives the assessment
requires (priority, delay, attempts+backoff, per-job locking). Reimplementing
that from scratch would mean re-solving a well-known problem with more room
for subtle race-condition bugs, for no functional gain. A senior engineering
call here is knowing when *not* to build your own distributed lock.

That said, I did **not** rely on BullMQ alone. See below.

## 4. Exactly-once delivery — the three layers

This is the part I'd expect to be pushed hardest on in the presentation, so
I built it in three independent, defense-in-depth layers:

**Layer 1 — Idempotency key (Postgres unique constraint).**
`jobs.idempotency_key` has a `UNIQUE` constraint. `POST /jobs` does an
`INSERT ... RETURNING`; if two requests race with the same key, only one
`INSERT` wins — the other gets a `23505` unique-violation, which the model
layer catches and turns into "return the existing row" instead of an error.
This guarantees a logical job is *scheduled* exactly once, independent of
anything that happens later in the pipeline.

**Layer 2 — BullMQ's per-job distributed lock (the primary guarantee for
concurrent workers).**
When a `Worker` instance picks a job off the queue, BullMQ atomically marks it
"active" in Redis and attaches a lock token to it (via Lua script, so the
check-and-set is atomic). While the job is active, that lock is periodically
renewed by the worker holding it; no other `Worker` instance — in the same
process or a completely separate one — can pick up the same job id while the
lock is held. This is what actually prevents two concurrent worker instances
from running the processor for the same job at the same time; it's the
mechanism the assessment is really testing for.

**Layer 3 — DB-level compare-and-swap claim (belt-and-braces).**
Independent of BullMQ, the processor also does:
```sql
UPDATE jobs SET status = 'processing', attempts = attempts + 1
WHERE id = $1 AND status IN ('pending', 'claimed')
RETURNING *;
```
Only the first caller to run this for a given row gets a returned row; any
other caller (e.g. if a lock-duration misconfiguration or a manual re-drive
ever caused two processor invocations for the same job) gets zero rows back
and aborts *before* calling the sender. This means even a bug or
misconfiguration in the queue layer can't cause a real duplicate send — the
final send call is gated by an atomic database transition, not by trusting
the queue's lock alone.

**Where the race would happen without these layers:** with naive polling
(e.g. `SELECT * FROM jobs WHERE status='pending' AND send_at <= now()` from N
workers, then each does `UPDATE ... WHERE id = X` in a separate step), two
workers can both read the same row as "pending" in the gap between the SELECT
and the UPDATE, and both proceed to call the sender before either UPDATE
lands. The fix in both BullMQ's internals and in my Layer 3 is the same
principle: make the *read-and-transition* a single atomic operation
(`UPDATE ... WHERE status = 'pending' RETURNING *`), never split it into a
separate check-then-set across two round trips.

The concurrency test in `/tests/concurrency.test.js` exercises Layer 3
directly: 25 concurrent callers try to claim the same job row; exactly one
succeeds, and `attempts` is incremented exactly once — never double-counted.

## 5. Priority queueing

BullMQ's `priority` option (lower number = processed first) is passed straight
through from the API's `priority` field (1 = highest, 10 = lowest — chosen to
match BullMQ's own convention so no inversion/mapping bugs can sneak in).
When multiple jobs are simultaneously due, BullMQ serves them out of Redis in
priority order before falling back to FIFO within the same priority. The seed
script schedules a priority-1 SMS OTP alongside a priority-3 email so you can
watch the OTP get served first even though it was submitted second.

## 6. Retry, exponential backoff, and dead-letter

Each job is added to BullMQ with `attempts: maxAttempts` and
`backoff: { type: 'exponential', delay: BASE_BACKOFF_MS }`. On a thrown error
inside the processor:

- If `attemptsMade < maxAttempts`: I update Postgres (`status='pending'`,
  `last_error` set) and re-throw, letting BullMQ apply the exponential delay
  and re-queue automatically.
- If `attemptsMade >= maxAttempts`: instead of letting BullMQ retry again, I
  mark the Postgres row `status='dead_letter'`, log the terminal error, fire
  the `dead_lettered` webhook event, and return normally (no re-throw) so
  BullMQ doesn't schedule yet another attempt for an already-terminal job.

Dead-lettered jobs are not deleted — they stay in Postgres with
`status='dead_letter'` and their full error history (`last_error`), so an
operator can inspect and manually re-drive them later (re-drive endpoint is
out of scope for Stage One, but the schema supports it trivially).

## 7. Idempotency vs. exactly-once — the difference

These solve two different problems and I kept them separate on purpose:
- **Idempotency key** = don't schedule the same *logical* job twice, even if
  the client's HTTP request is retried (e.g. a flaky network causes the
  client to POST twice).
- **Exactly-once delivery** = once a job *is* scheduled, never send it twice,
  even with N workers polling concurrently.

## 8. Rate limiting (queue, don't fail)

Implemented as a Redis fixed-window counter, `ratelimit:<recipient>:<hourBucket>`,
incremented only on a **successful** send (not on every attempt, so failed
attempts don't unfairly consume a recipient's quota). Before sending, the
processor checks `canSendNow`; if the recipient is at/over the cap, the job is
moved back into the delayed set with `job.moveToDelayed(...)` and the
processor returns without treating it as a completed or failed attempt — it
simply gets re-checked later. This satisfies "excess jobs should queue, not
fail" without consuming a retry attempt or dead-lettering a job just because
its recipient is temporarily over quota.

*Trade-off:* this is a fixed-window limiter, not a sliding-log — it's simpler
and O(1) per check, at the cost of allowing a small burst right at a window
boundary (e.g. N sends at 12:59 and another N at 1:00). For a notification
system this trade-off is reasonable; a sliding-window or token-bucket
implementation would be a straightforward follow-up if stricter guarantees
were required.

## 9. Webhook callbacks

`services/webhook.js` POSTs a small JSON payload (`event`, `jobId`, `status`,
`attempts`, `timestamp`) to `WEBHOOK_URL` whenever a job transitions to
`sent`, `failed` (a retryable attempt failure), or `dead_lettered`. Every call
is also logged to a `webhook_logs` table for auditability. Failures to deliver
the webhook itself are caught and logged, never thrown — a flaky receiver
must never affect the job's own retry/delivery state.

`POST /webhook/mock` is included as the mocked receiver so the whole loop is
runnable locally without any external dependency.

## 10. Simplifying assumptions (and why)

- **Single Postgres instance, no read replicas / sharding.** Fine at the scale
  of an assessment; noted in §11 as the first thing that would need to change
  at real scale.
- **BullMQ/Redis persistence relies on Redis's own AOF/RDB.** I did not build
  a reconciliation job that re-hydrates Redis from Postgres if Redis data were
  lost. In production I'd add a periodic sweep that re-enqueues any Postgres
  row with `status IN ('pending','claimed')` whose `bullmq_job_id` can't be
  found in Redis — omitted here to keep the assessment focused on the core
  concurrency/exactly-once requirements.
- **Sender is a stub** with a configurable random failure rate
  (`SIMULATED_FAILURE_RATE`), as explicitly permitted by the brief.
  Real channel adapters (SES/SNS/FCM/etc.) would sit behind the same
  `services/sender.js` interface.
- **Rate limiter is fixed-window**, not sliding-log/token-bucket (see §8).
- **No authentication/authorization** on the API — out of scope for a
  backend-logic assessment; would add API keys or JWT in front of Express in
  a real deployment.
- **Webhook retries are not themselves retried with backoff** — a failed
  webhook delivery is logged and dropped. In production this would likely be
  its own small BullMQ queue with its own retry policy, decoupled from the
  notification job's own retry state.
- **Single Redis instance**, not Redis Cluster/Sentinel — acceptable for this
  exercise; noted as a scaling concern below.

## 11. Scaling to millions of jobs / thousands of workers — what breaks first

1. **Single Redis instance becomes the bottleneck first.** BullMQ's locking
   and priority ordering all funnel through one Redis node. At very high
   throughput, Redis CPU/single-threaded command processing (not memory) is
   usually the first ceiling. Fix: Redis Cluster with jobs partitioned across
   multiple named queues (e.g. by channel or by shard of recipient hash), each
   backed by its own Redis keyspace/node, with workers subscribing to a subset.
2. **Postgres write throughput** on the `jobs` table (every job does at least
   an INSERT, a claim UPDATE, and a status UPDATE) would need connection
   pooling (already using `pg.Pool`, but would need PgBouncer at scale),
   partitioning the `jobs` table by time (e.g. monthly partitions on
   `created_at`), and moving `webhook_logs` to an append-only store (or
   Kafka) rather than a relational table if webhook volume is very high.
3. **Thundering herd on `send_at`.** Millions of jobs scheduled for the exact
   same instant (e.g. a mass campaign) would all become "due" simultaneously;
   BullMQ's priority set would need many workers pulling from it concurrently,
   and the DB claim UPDATE could become a hotspot on the same rows'
   surrounding index pages. Fix: pre-shard scheduled jobs across N queues by
   a hash of recipient or job id, so no single Redis sorted-set key or DB
   index range absorbs the entire spike.
4. **Rate limiter under massive recipient cardinality** — the fixed-window
   Redis counter approach is fine at any recipient count since it's O(1) per
   key, but very high write volume of small INCR/EXPIRE commands would push
   toward the same single-Redis-node ceiling as #1, reinforcing the need to
   shard Redis.
5. **Worker fan-out** — going from tens to thousands of concurrent workers is
   mostly a matter of horizontal scaling (more processes/containers, `WORKER_CONCURRENCY`
   tuned per instance), but each worker maintains a Redis connection and
   periodic lock-renewal traffic; at very large fleets this connection count
   itself becomes something to manage (connection pooling / Redis max-clients
   tuning, or moving to a queue system explicitly designed for that fan-out,
   e.g. Kafka consumer groups instead of Redis-based locking).

In short: the architecture here would hold up correctly (not incorrectly —
just slowly) well past the assessment's scale, and the first real wall is
Redis being a single coordination point; the fix is horizontal partitioning
of the queue itself, not a change to the locking/exactly-once strategy, which
remains correct at any scale.
