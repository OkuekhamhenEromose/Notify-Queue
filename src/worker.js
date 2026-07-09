const { Worker } = require('bullmq');
const { connection, QUEUE_NAME } = require('./queue');
const config = require('./config');
const jobModel = require('./models/job');
const sender = require('./services/sender');
const rateLimiter = require('./services/rateLimiter');
const webhook = require('./services/webhook');

const WORKER_ID = process.env.WORKER_ID || `worker-${process.pid}`;

/**
 * Processor for a single BullMQ job. Multiple Worker instances (in this
 * process, or in separate processes/machines) can all be running this
 * function concurrently against the same Redis-backed queue.
 *
 * ---- Exactly-once delivery: where the guarantees come from ----
 * 1. BullMQ per-job distributed lock: when a worker fetches a job, BullMQ
 *    (via Redis Lua scripts) marks it "active" and attaches a lock token.
 *    No other worker can fetch the same job while the lock is held; the
 *    lock is auto-renewed while this function is still running and
 *    released atomically on completion/failure. This is what actually
 *    prevents two workers from concurrently processing the same job.
 * 2. DB-level compare-and-swap claim (jobModel.claimJob): belt-and-braces
 *    defense in depth. Even if the queue layer were misconfigured (e.g.
 *    lock duration too short causing a double pickup), the conditional
 *    UPDATE ... WHERE status IN ('pending','claimed') means only the
 *    first caller can flip status -> 'processing'; the second gets 0
 *    rows and aborts before calling the sender.
 * 3. Idempotency key unique constraint (in jobModel.createJob): prevents
 *    the same logical job from ever being scheduled twice in the first
 *    place, regardless of how many times the client retries the POST.
 */
async function processNotification(job, token) {
  const { jobId, recipient, channel, payload } = job.data;

  // Rate limiting: "queue, don't fail". If the recipient is over their
  // per-hour cap, push this job back with a delay and do NOT count it as
  // a failed attempt (moveToDelayed does not touch attemptsMade).
  const allowed = await rateLimiter.canSendNow(recipient);
  if (!allowed) {
    const retryInMs = 60 * 1000; // re-check in a minute
    console.log(`[${WORKER_ID}] rate limit hit for ${recipient}, delaying job ${jobId}`);
    await job.moveToDelayed(Date.now() + retryInMs, token);
    // Throwing this special error tells BullMQ the job was manually
    // moved and should not be treated as completed or failed.
    throw new Error('DelayedError:rate-limited');
  }

  // DB-level claim: defense-in-depth atomic transition.
  const claimed = await jobModel.claimJob(jobId);
  if (!claimed) {
    console.log(`[${WORKER_ID}] job ${jobId} already claimed by another worker/attempt - skipping`);
    return { skipped: true };
  }

  try {
    await sender.send({ recipient, channel, payload });
    await rateLimiter.recordSend(recipient);
    const updated = await jobModel.markSent(jobId);
    await webhook.notifyStatusChange(updated, 'sent');
    console.log(`[${WORKER_ID}] job ${jobId} sent successfully`);
    return { sent: true };
  } catch (err) {
    const attemptsMade = job.attemptsMade + 1; // this attempt
    const maxAttempts = job.opts.attempts || config.maxAttempts;

    if (attemptsMade >= maxAttempts) {
      const updated = await jobModel.markDeadLetter(jobId, err.message);
      await webhook.notifyStatusChange(updated, 'dead_lettered');
      console.log(`[${WORKER_ID}] job ${jobId} exceeded max attempts (${maxAttempts}) -> dead-lettered`);
      // Do not rethrow: we handled terminal failure ourselves so BullMQ
      // does not schedule yet another backoff retry for an already
      // dead-lettered job.
      return { deadLettered: true };
    }

    const updated = await jobModel.markFailedAttempt(jobId, err.message);
    await webhook.notifyStatusChange(updated, 'failed');
    console.log(`[${WORKER_ID}] job ${jobId} attempt ${attemptsMade}/${maxAttempts} failed: ${err.message}`);
    throw err; // let BullMQ apply exponential backoff and re-queue
  }
}

function startWorker() {
  const worker = new Worker(QUEUE_NAME, processNotification, {
    connection,
    concurrency: config.workerConcurrency,
  });

  worker.on('completed', (job, result) => {
    if (result && result.skipped) {
      console.log(`[${WORKER_ID}] job ${job.id} skipped (already handled elsewhere)`);
    }
  });

  worker.on('failed', (job, err) => {
    if (err.message === 'DelayedError:rate-limited') return; // expected control-flow signal
    console.error(`[${WORKER_ID}] job ${job?.id} failed:`, err.message);
  });

  console.log(`[${WORKER_ID}] worker started, concurrency=${config.workerConcurrency}`);
  return worker;
}

if (require.main === module) {
  startWorker();
}

module.exports = { startWorker, processNotification };
