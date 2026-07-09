/**
 * seed.js
 * Populates the database + queue with a handful of sample jobs covering:
 *  - different channels (email, sms, push)
 *  - different priorities (so you can watch high-priority jump the queue)
 *  - an immediate-due job and a delayed one
 *  - a deliberately duplicated idempotency key (to demonstrate de-dupe)
 *
 * Usage: npm run seed   (make sure `npm run migrate` has been run first,
 * and postgres/redis are up, e.g. via `docker compose up -d`)
 */
const { v4: uuidv4 } = require('uuid');
const jobModel = require('./src/models/job');
const { enqueueNotification } = require('./src/queue');
const { pool } = require('./src/db');

const samples = [
  {
    idempotencyKey: 'seed-welcome-email-1',
    recipient: 'alice@example.com',
    channel: 'email',
    payload: { template: 'welcome', name: 'Alice' },
    sendAt: new Date(Date.now() + 2000),
    priority: 3,
  },
  {
    idempotencyKey: 'seed-otp-sms-1',
    recipient: '+2348000000001',
    channel: 'sms',
    payload: { code: '123456' },
    sendAt: new Date(Date.now() + 1000),
    priority: 1, // highest priority - should jump ahead of the email above
  },
  {
    idempotencyKey: 'seed-push-promo-1',
    recipient: 'device-token-abc',
    channel: 'push',
    payload: { title: 'Sale!', body: '20% off today' },
    sendAt: new Date(Date.now() + 5000),
    priority: 8, // low priority
  },
  {
    idempotencyKey: 'seed-digest-email-1',
    recipient: 'bob@example.com',
    channel: 'email',
    payload: { template: 'weekly-digest' },
    sendAt: new Date(Date.now() + 15000), // delayed job, due later
    priority: 5,
  },
];

async function run() {
  console.log('Seeding sample jobs...');

  for (const sample of samples) {
    const { job, created } = await jobModel.createJob({
      ...sample,
      maxAttempts: 5,
    });
    if (created) {
      const bullJob = await enqueueNotification(job);
      await jobModel.setBullJobId(job.id, bullJob.id);
      console.log(`created job ${job.id} (${job.channel} -> ${job.recipient}, priority ${job.priority})`);
    } else {
      console.log(`job with idempotency key ${sample.idempotencyKey} already existed - skipped`);
    }
  }

  // Demonstrate idempotency: resubmitting the exact same key should NOT
  // create a second job or enqueue a second delivery.
  const dupe = samples[0];
  const { job, created } = await jobModel.createJob({ ...dupe, maxAttempts: 5 });
  console.log(
    `duplicate submission of "${dupe.idempotencyKey}" -> created=${created}, same id returned=${job.id}`
  );

  console.log('Seeding complete.');
}

run()
  .then(() => pool.end())
  .then(() => process.exit(0))
  .catch((err) => {
    console.error('Seed failed:', err);
    process.exit(1);
  });
