-- 001_init.sql
-- Source of truth for job state. BullMQ/Redis owns scheduling & locking;
-- Postgres owns durable state, idempotency, and auditability.

CREATE EXTENSION IF NOT EXISTS pgcrypto; -- for gen_random_uuid()

CREATE TABLE IF NOT EXISTS jobs (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key   TEXT UNIQUE NOT NULL,
    recipient         TEXT NOT NULL,
    channel           TEXT NOT NULL CHECK (channel IN ('email', 'sms', 'push')),
    payload           JSONB NOT NULL DEFAULT '{}'::jsonb,
    send_at           TIMESTAMPTZ NOT NULL,
    priority          SMALLINT NOT NULL DEFAULT 5 CHECK (priority BETWEEN 1 AND 10),
    status            TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending','claimed','processing','sent','failed','dead_letter')),
    attempts          INT NOT NULL DEFAULT 0,
    max_attempts      INT NOT NULL DEFAULT 5,
    last_error        TEXT,
    bullmq_job_id     TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_recipient ON jobs (recipient);
CREATE INDEX IF NOT EXISTS idx_jobs_send_at ON jobs (send_at);

-- Audit log of every webhook callback the system fired (status-change notifications)
CREATE TABLE IF NOT EXISTS webhook_logs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id      UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    event       TEXT NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

