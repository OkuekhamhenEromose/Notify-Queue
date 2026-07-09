const express = require('express');
const { v4: uuidv4, validate: isUuid } = require('uuid');
const jobModel = require('../models/job');
const { enqueueNotification } = require('../queue');

const router = express.Router();

const VALID_CHANNELS = ['email', 'sms', 'push'];

/**
 * POST /jobs
 * Schedule a notification job.
 * Body: {
 *   recipient, channel, payload,
 *   sendAt?  (ISO timestamp)      -- mutually exclusive with delaySeconds
 *   delaySeconds? (number)
 *   priority? (1 highest - 10 lowest, default 5)
 *   idempotencyKey? (string, auto-generated if omitted)
 *   maxAttempts? (default from env)
 * }
 */
router.post('/', async (req, res) => {
  try {
    const {
      recipient,
      channel,
      payload = {},
      sendAt,
      delaySeconds,
      priority = 5,
      idempotencyKey,
      maxAttempts,
    } = req.body;

    if (!recipient || !channel) {
      return res.status(400).json({ error: 'recipient and channel are required' });
    }
    if (!VALID_CHANNELS.includes(channel)) {
      return res.status(400).json({ error: `channel must be one of ${VALID_CHANNELS.join(', ')}` });
    }
    if (priority < 1 || priority > 10) {
      return res.status(400).json({ error: 'priority must be between 1 (highest) and 10 (lowest)' });
    }
    if (!sendAt && delaySeconds === undefined) {
      return res.status(400).json({ error: 'either sendAt or delaySeconds is required' });
    }

    const resolvedSendAt = sendAt
      ? new Date(sendAt)
      : new Date(Date.now() + Number(delaySeconds) * 1000);

    if (isNaN(resolvedSendAt.getTime())) {
      return res.status(400).json({ error: 'invalid sendAt' });
    }

    const key = idempotencyKey || uuidv4();

    const config = require('../config');
    const { job, created } = await jobModel.createJob({
      idempotencyKey: key,
      recipient,
      channel,
      payload,
      sendAt: resolvedSendAt,
      priority,
      maxAttempts: maxAttempts || config.maxAttempts,
    });

    if (created) {
      const bullJob = await enqueueNotification(job);
      await jobModel.setBullJobId(job.id, bullJob.id);
    }

    return res.status(created ? 201 : 200).json({
      id: job.id,
      idempotencyKey: job.idempotency_key,
      status: job.status,
      sendAt: job.send_at,
      priority: job.priority,
      deduplicated: !created,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'internal error scheduling job' });
  }
});

/**
 * GET /jobs/:id  - check status of a single job
 */
router.get('/:id', async (req, res) => {
  const { id } = req.params;
  if (!isUuid(id)) return res.status(400).json({ error: 'invalid job id' });

  const job = await jobModel.getJobById(id);
  if (!job) return res.status(404).json({ error: 'job not found' });

  return res.json({
    id: job.id,
    idempotencyKey: job.idempotency_key,
    recipient: job.recipient,
    channel: job.channel,
    status: job.status,
    priority: job.priority,
    attempts: job.attempts,
    maxAttempts: job.max_attempts,
    sendAt: job.send_at,
    lastError: job.last_error,
    createdAt: job.created_at,
    updatedAt: job.updated_at,
  });
});

module.exports = router;
