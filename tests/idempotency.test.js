const { pool } = require('../src/db');
const jobModel = require('../src/models/job');

describe('Idempotency', () => {
  afterAll(async () => {
    await pool.end();
  });

  test('submitting the same idempotency key twice returns the same job and only creates one row', async () => {
    const key = `test-idem-${Date.now()}`;
    const input = {
      idempotencyKey: key,
      recipient: 'idem-test@example.com',
      channel: 'email',
      payload: { hello: 'world' },
      sendAt: new Date(Date.now() + 60000),
      priority: 5,
      maxAttempts: 5,
    };

    const first = await jobModel.createJob(input);
    const second = await jobModel.createJob(input);

    expect(first.created).toBe(true);
    expect(second.created).toBe(false);
    expect(second.job.id).toBe(first.job.id);

    const { rows } = await pool.query('SELECT COUNT(*)::int AS count FROM jobs WHERE idempotency_key = $1', [key]);
    expect(rows[0].count).toBe(1);
  });

  test('concurrent submissions with the same idempotency key still result in exactly one row', async () => {
    const key = `test-idem-concurrent-${Date.now()}`;
    const input = {
      idempotencyKey: key,
      recipient: 'idem-concurrent@example.com',
      channel: 'sms',
      payload: {},
      sendAt: new Date(Date.now() + 60000),
      priority: 2,
      maxAttempts: 5,
    };

    const results = await Promise.all(
      Array.from({ length: 10 }, () => jobModel.createJob(input))
    );

    const createdCount = results.filter((r) => r.created).length;
    expect(createdCount).toBe(1);

    const ids = new Set(results.map((r) => r.job.id));
    expect(ids.size).toBe(1);

    const { rows } = await pool.query('SELECT COUNT(*)::int AS count FROM jobs WHERE idempotency_key = $1', [key]);
    expect(rows[0].count).toBe(1);
  });
});
