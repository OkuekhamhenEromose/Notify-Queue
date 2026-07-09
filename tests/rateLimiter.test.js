const { connection } = require('../src/queue');
const rateLimiter = require('../src/services/rateLimiter');
const config = require('../src/config');

describe('Rate limiter', () => {
  afterAll(async () => {
    await connection.quit();
  });

  test('allows sends under the per-hour cap and blocks once the cap is reached', async () => {
    const recipient = `rate-test-${Date.now()}@example.com`;

    for (let i = 0; i < config.rateLimitMaxPerHour; i++) {
      expect(await rateLimiter.canSendNow(recipient)).toBe(true);
      await rateLimiter.recordSend(recipient);
    }

    expect(await rateLimiter.canSendNow(recipient)).toBe(false);
  });
});
