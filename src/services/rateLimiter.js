const { connection } = require('../queue');
const config = require('../config');

/**
 * Fixed-window rate limiter keyed by recipient: "ratelimit:<recipient>:<hourBucket>".
 * We use the current hour as the bucket key, so the counter naturally expires
 * and resets every hour (TTL set on first increment in the bucket).
 *
 * This is intentionally simple (fixed window, not sliding-log) - see DESIGN.md
 * for the trade-off note.
 */
function bucketKey(recipient) {
  const hourBucket = Math.floor(Date.now() / (60 * 60 * 1000));
  return `ratelimit:${recipient}:${hourBucket}`;
}

/**
 * Returns true if the recipient is currently UNDER the per-hour cap
 * (i.e. safe to send now). Does NOT increment - call `recordSend` only
 * once the send actually happens, so retries don't unfairly consume quota.
 */
async function canSendNow(recipient) {
  const key = bucketKey(recipient);
  const current = await connection.get(key);
  return !current || parseInt(current, 10) < config.rateLimitMaxPerHour;
}

async function recordSend(recipient) {
  const key = bucketKey(recipient);
  const multi = connection.multi();
  multi.incr(key);
  multi.expire(key, 3600);
  await multi.exec();
}

module.exports = { canSendNow, recordSend };
