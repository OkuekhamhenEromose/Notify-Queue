const express = require('express');
const config = require('./config');
const jobsRouter = require('./routes/jobs');
const webhookRouter = require('./routes/webhook');
const metricsRouter = require('./routes/metrics');

function createApp() {
  const app = express();
  app.use(express.json());

  app.get('/health', (req, res) => res.json({ ok: true }));
  app.use('/jobs', jobsRouter);
  app.use('/webhook', webhookRouter);
  app.use('/metrics', metricsRouter);

  return app;
}

if (require.main === module) {
  const app = createApp();
  app.listen(config.port, () => {
    console.log(`Notify Queue API listening on port ${config.port}`);
  });
}

module.exports = { createApp };
