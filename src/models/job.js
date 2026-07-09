const { pool } = require('../db');

/**
 * Create a job row, or return the existing one if the idempotency_key
 * was already used. This is the FIRST layer of exactly-once protection:
 * a unique constraint at the database level means two concurrent API
 * requests with the same idempotency_key can race, but only one INSERT
 * will win; the loser is caught and we fetch+return the winner's row.
 */
async function createJob({ idempotencyKey, recipient, channel, payload, sendAt, priority, maxAttempts }) {
  try {
    const { rows } = await pool.query(
      `INSERT INTO jobs (idempotency_key, recipient, channel, payload, send_at, priority, max_attempts)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       RETURNING *`,
      [idempotencyKey, recipient, channel, payload, sendAt, priority, maxAttempts]
    );
    return { job: rows[0], created: true };
  } catch (err) {
    if (err.code === '23505') {
      // unique_violation on idempotency_key -> job already exists
      const existing = await getJobByIdempotencyKey(idempotencyKey);
      return { job: existing, created: false };
    }
    throw err;
  }
}

async function getJobByIdempotencyKey(key) {
  const { rows } = await pool.query('SELECT * FROM jobs WHERE idempotency_key = $1', [key]);
  return rows[0] || null;
}

async function getJobById(id) {
  const { rows } = await pool.query('SELECT * FROM jobs WHERE id = $1', [id]);
  return rows[0] || null;
}

async function setBullJobId(id, bullmqJobId) {
  await pool.query('UPDATE jobs SET bullmq_job_id = $1, updated_at = now() WHERE id = $2', [bullmqJobId, id]);
}

/**
 * SECOND layer of exactly-once protection (defense-in-depth on top of the
 * BullMQ per-job lock): atomically transition a job from pending/failed
 * into "processing" using a conditional UPDATE. If two workers somehow
 * invoked the processor for the same DB row concurrently (e.g. during a
 * BullMQ lock-renewal edge case or a manual re-drive), only one UPDATE
 * affects a row (WHERE status guards it) - the other gets 0 rows back
 * and knows to bail out without sending.
 */
async function claimJob(id) {
  const { rows } = await pool.query(
    `UPDATE jobs
     SET status = 'processing', attempts = attempts + 1, updated_at = now()
     WHERE id = $1 AND status IN ('pending', 'claimed')
     RETURNING *`,
    [id]
  );
  return rows[0] || null;
}

async function markSent(id) {
  const { rows } = await pool.query(
    `UPDATE jobs SET status = 'sent', updated_at = now() WHERE id = $1 RETURNING *`,
    [id]
  );
  return rows[0];
}

async function markFailedAttempt(id, errorMessage) {
  const { rows } = await pool.query(
    `UPDATE jobs SET status = 'pending', last_error = $2, updated_at = now() WHERE id = $1 RETURNING *`,
    [id, errorMessage]
  );
  return rows[0];
}

async function markDeadLetter(id, errorMessage) {
  const { rows } = await pool.query(
    `UPDATE jobs SET status = 'dead_letter', last_error = $2, updated_at = now() WHERE id = $1 RETURNING *`,
    [id, errorMessage]
  );
  return rows[0];
}

async function getMetrics() {
  const { rows } = await pool.query(
    `SELECT status, COUNT(*)::int AS count FROM jobs GROUP BY status`
  );
  const base = { pending: 0, claimed: 0, processing: 0, sent: 0, failed: 0, dead_letter: 0 };
  for (const row of rows) base[row.status] = row.count;
  return base;
}

async function logWebhookEvent(jobId, event, payload) {
  await pool.query(
    `INSERT INTO webhook_logs (job_id, event, payload) VALUES ($1, $2, $3)`,
    [jobId, event, payload]
  );
}

module.exports = {
  createJob,
  getJobByIdempotencyKey,
  getJobById,
  setBullJobId,
  claimJob,
  markSent,
  markFailedAttempt,
  markDeadLetter,
  getMetrics,
  logWebhookEvent,
};
