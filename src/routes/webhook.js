const express = require('express');

const router = express.Router();

/**
 * POST /webhook/mock
 * Stand-in for a customer's webhook receiver. In real life this would be
 * an external URL; here we just log what we received so it's visible
 * during the demo/presentation.
 */
router.post('/mock', (req, res) => {
  console.log('[webhook:mock] received callback:', JSON.stringify(req.body));
  res.status(200).json({ received: true });
});

module.exports = router;
