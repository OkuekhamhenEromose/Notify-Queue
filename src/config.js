require('dotenv').config();

module.exports = {
  port: parseInt(process.env.PORT || '3000', 10),
  databaseUrl: process.env.DATABASE_URL || 'postgres://notify:notify@localhost:5432/notifyqueue',
  redisUrl: process.env.REDIS_URL || 'redis://localhost:6379',
  workerConcurrency: parseInt(process.env.WORKER_CONCURRENCY || '5', 10),
  simulatedFailureRate: parseFloat(process.env.SIMULATED_FAILURE_RATE || '0.3'),
  maxAttempts: parseInt(process.env.MAX_ATTEMPTS || '5', 10),
  baseBackoffMs: parseInt(process.env.BASE_BACKOFF_MS || '2000', 10),
  rateLimitMaxPerHour: parseInt(process.env.RATE_LIMIT_MAX_PER_HOUR || '5', 10),
  webhookUrl: process.env.WEBHOOK_URL || 'http://localhost:3000/webhook/mock',
};
