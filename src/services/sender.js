const config = require('../config');

/**
 * Stub/mock sender. Simulates network latency and a configurable random
 * failure rate (SIMULATED_FAILURE_RATE) so retry/backoff/dead-letter logic
 * has something real to exercise in dev and tests.
 */
async function send({ recipient, channel, payload }) {
  await new Promise((resolve) => setTimeout(resolve, 50 + Math.random() * 150));

  if (Math.random() < config.simulatedFailureRate) {
    throw new Error(`Simulated ${channel} delivery failure for ${recipient}`);
  }

  console.log(`[sender] Delivered ${channel} to ${recipient}:`, JSON.stringify(payload));
  return { deliveredAt: new Date().toISOString() };
}

module.exports = { send };
