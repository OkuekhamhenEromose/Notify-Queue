const { Queue, QueueEvents } = require('bullmq');
const IORedis = require('ioredis');
const config = require('./config');

// BullMQ requires this option on the ioredis connection.
const connection = new IORedis(config.redisUrl, { maxRetriesPerRequest: null });

const QUEUE_NAME = 'notifications';

const notificationQueue = new Queue(QUEUE_NAME, { connection });
const queueEvents = new QueueEvents(QUEUE_NAME, { connection });

/**
 * BullMQ priority: LOWER number = processed first.
 * Our API accepts priority 1 (highest) - 10 (lowest), which already
 * matches BullMQ's convention, so we pass it straight through.
 */
async function enqueueNotification(job) {
  const delay = Math.max(0, new Date(job.send_at).getTime() - Date.now());
  const bullJob = await notificationQueue.add(
    'deliver',
    {
      jobId: job.id,
      recipient: job.recipient,
      channel: job.channel,
      payload: job.payload,
    },
    {
      jobId: job.id, // reuse our own UUID as the BullMQ job id -> BullMQ itself also de-dupes on this id
      delay,
      priority: job.priority,
      attempts: job.max_attempts,
      backoff: {
        type: 'exponential',
        delay: config.baseBackoffMs,
      },
      removeOnComplete: { age: 3600 },
      removeOnFail: false, // keep failed jobs around so we can inspect / move to dead-letter
    }
  );
  return bullJob;
}

module.exports = { connection, notificationQueue, queueEvents, QUEUE_NAME, enqueueNotification };
