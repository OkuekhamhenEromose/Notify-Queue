const fetch = require('node-fetch');
const config = require('../config');
const { logWebhookEvent } = require('../models/job');

/**
 * Calls the (mocked) webhook receiver whenever a job's status changes.
 * Failures here are logged but never thrown - a flaky webhook receiver
 * must not affect delivery guarantees or retry state of the job itself.
 */
async function notifyStatusChange(job, event) {
  const body = {
    event, // 'sent' | 'failed' | 'dead_lettered'
    jobId: job.id,
    recipient: job.recipient,
    channel: job.channel,
    status: job.status,
    attempts: job.attempts,
    timestamp: new Date().toISOString(),
  };

  try {
    await logWebhookEvent(job.id, event, body);
    await fetch(config.webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  } catch (err) {
    console.error(`[webhook] Failed to deliver callback for job ${job.id}:`, err.message);
  }
}

module.exports = { notifyStatusChange };
