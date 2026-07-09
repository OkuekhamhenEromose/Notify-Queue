const { pool } = require('../src/db');
const jobModel = require('../src/models/job');

/**
 * This test simulates the exact race the assessment calls out: multiple
 * worker instances polling/claiming the SAME due job at the same time.
 * It exercises the DB-level atomic claim (jobModel.claimJob) directly -
 * the same primitive the real worker uses as defense-in-depth alongside
 * the BullMQ per-job lock. Only one of N concurrent claim attempts for
 * the same job may succeed.
 */
describe('Exactly-once claim under concurrency', () => {
  afterAll(async () => {
    await pool.end();
  });

  test('only one of many concurrent claimJob calls succeeds for the same job', async () => {
    const { job } = await jobModel.createJob({
      idempotencyKey: `test-concurrency-${Date.now()}`,
      recipient: 'race-test@example.com',
      channel: 'email',
      payload: {},
      sendAt: new Date(),
      priority: 5,
      maxAttempts: 5,
    });

    const CONCURRENT_WORKERS = 25;
    const attempts = await Promise.all(
      Array.from({ length: CONCURRENT_WORKERS }, () => jobModel.claimJob(job.id))
    );

    const successfulClaims = attempts.filter((r) => r !== null);
    expect(successfulClaims.length).toBe(1);

    // Simulate that "winning" worker completing delivery.
    await jobModel.markSent(successfulClaims[0].id);

    const finalRow = await jobModel.getJobById(job.id);
    expect(finalRow.status).toBe('sent');
    expect(finalRow.attempts).toBe(1); // only incremented once, never double-counted
  });

  test('a job already sent cannot be re-claimed by a late/duplicate worker', async () => {
    const { job } = await jobModel.createJob({
      idempotencyKey: `test-already-sent-${Date.now()}`,
      recipient: 'late-worker@example.com',
      channel: 'push',
      payload: {},
      sendAt: new Date(),
      priority: 5,
      maxAttempts: 5,
    });

    const claimed = await jobModel.claimJob(job.id);
    await jobModel.markSent(claimed.id);

    // A "late" duplicate poller tries to claim the same job after it was
    // already sent - it must not be able to.
    const lateClaim = await jobModel.claimJob(job.id);
    expect(lateClaim).toBeNull();
  });
});
