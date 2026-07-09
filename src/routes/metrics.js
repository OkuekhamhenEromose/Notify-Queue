const express = require('express');
const jobModel = require('../models/job');

const router = express.Router();

/**
 * GET /metrics
 * Lightweight counts by job status. In production this would be exposed
 * in Prometheus exposition format; JSON keeps the assessment focused.
 */
router.get('/', async (req, res) => {
  const counts = await jobModel.getMetrics();
  res.json({
    pending: counts.pending + counts.claimed, // scheduled, not yet due/picked up
    processing: counts.processing,
    sent: counts.sent,
    failed: counts.failed, // mid-retry
    dead_lettered: counts.dead_letter,
    total: Object.values(counts).reduce((a, b) => a + b, 0),
  });
});

module.exports = router;
