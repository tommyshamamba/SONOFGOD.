const express = require('express');
const cors = require('cors');
const path = require('node:path');
const fs = require('node:fs');
require('dotenv').config();

function createApp(env = process.env) {
  const app = express();
  app.disable('x-powered-by');
  app.use(cors({ origin: (env.CORS_ORIGIN || 'http://localhost:3000').split(',') }));
  app.use(express.json({ limit: '16kb' }));
  const config = { appName: env.APP_NAME || 'k8s-demo', logLevel: env.LOG_LEVEL || 'info', featureFlags: {} };
  try { config.featureFlags = JSON.parse(env.FEATURE_FLAGS || '{}'); }
  catch { throw new Error('FEATURE_FLAGS must be valid JSON'); }
  if (!config.featureFlags || typeof config.featureFlags !== 'object' || Array.isArray(config.featureFlags)) throw new Error('FEATURE_FLAGS must be an object');
  app.get('/health', (req, res) => res.json({ status: 'healthy' }));
  app.get('/ready', (req, res) => res.json({ ready: true }));
  app.get('/api/data', (req, res) => res.json({ message: 'Hello from Kubernetes demo!', environment: env.NODE_ENV || 'development', podName: env.POD_NAME || 'local-process', nodeName: env.NODE_NAME || 'local-machine' }));
  app.get('/api/config', (req, res) => res.json(config));
  app.get('/api/secret', (req, res) => res.json({ apiKey: env.API_KEY ? '***MASKED***' : 'not set', dbPassword: env.DB_PASSWORD ? '***MASKED***' : 'not set' }));
  const build = path.join(__dirname, '../frontend/build');
  if (fs.existsSync(path.join(build, 'index.html'))) {
    app.use(express.static(build));
    app.get('/{*splat}', (req, res, next) => req.path.startsWith('/api/') ? next() : res.sendFile(path.join(build, 'index.html')));
  }
  app.use((req, res) => res.status(404).json({ error: 'Not found' }));
  return app;
}
if (require.main === module) createApp().listen(process.env.PORT || 3200, process.env.HOST || '127.0.0.1', () => console.log(`Kubernetes local demo: http://127.0.0.1:${process.env.PORT || 3200}`));
module.exports = { createApp };
