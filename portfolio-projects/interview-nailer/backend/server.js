require('dotenv').config();

const fs = require('fs');
const path = require('path');
const express = require('express');
const cors = require('cors');
const rateLimit = require('express-rate-limit');

const { port, nodeEnv, clientUrls, storageMode, aiMode } = require('./config/env');
const store = require('./services/store');
const authRoutes = require('./routes/auth');
const resumeRoutes = require('./routes/resume');
const answerRoutes = require('./routes/answers');
const sessionRoutes = require('./routes/sessions');

const app = express();

function normalizeOrigin(value) {
  return (value || '').trim().replace(/\/+$/, '').toLowerCase();
}

const allowedOrigins = new Set(
  [
    ...clientUrls,
    `http://localhost:${port}`,
    `http://127.0.0.1:${port}`,
  ]
    .map(normalizeOrigin)
    .filter(Boolean)
);

app.use(cors({
  credentials: true,
  origin(origin, callback) {
    const normalizedOrigin = normalizeOrigin(origin);

    if (!origin || clientUrls.length === 0 || allowedOrigins.has(normalizedOrigin)) {
      callback(null, true);
      return;
    }

    callback(new Error(`Origin ${origin} is not allowed by CORS.`));
  },
}));
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

const aiLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 20,
  message: { error: 'Too many requests. Slow down and practice one answer at a time.' },
});
app.use('/api/answers', aiLimiter);
app.use('/api/sessions', aiLimiter);

app.use('/api/auth', authRoutes);
app.use('/api/resume', resumeRoutes);
app.use('/api/answers', answerRoutes);
app.use('/api/sessions', sessionRoutes);

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    version: '1.0.0',
    service: 'Interview Nailer API',
    node_env: nodeEnv,
    storage_mode: storageMode,
    ai_mode: aiMode,
  });
});

const frontendBuildDir = path.join(__dirname, '..', 'frontend', 'build');
const frontendIndexPath = path.join(frontendBuildDir, 'index.html');
const hasFrontendBuild = fs.existsSync(frontendIndexPath);

if (hasFrontendBuild) {
  app.use(express.static(frontendBuildDir));

  app.get('*', (req, res, next) => {
    if (req.path.startsWith('/api/')) {
      next();
      return;
    }

    res.sendFile(frontendIndexPath);
  });
}
app.use((req, res) => res.status(404).json({ error: 'Route not found' }));

app.use((err, req, res, next) => {
  console.error(err.stack || err.message);
  res.status(500).json({ error: err.message || 'Internal server error' });
});

async function start() {
  await store.initialize();

  await new Promise((resolve, reject) => {
    const server = app.listen(port, () => {
      console.log(`Interview Nailer API started on port ${port}`);
      console.log(`Environment : ${nodeEnv}`);
      console.log(`Storage mode: ${storageMode}`);
      console.log(`AI mode     : ${aiMode}`);
      console.log(`Health check: http://localhost:${port}/health`);
      resolve();
    });

    server.once('error', reject);
  });
}

start().catch((error) => {
  if (error?.code === 'EADDRINUSE') {
    console.error(`Failed to start Interview Nailer API: port ${port} is already in use.`);
    console.error('If another Interview Nailer backend is already running, keep using the current frontend link.');
    console.error('Otherwise, change PORT in backend/.env and REACT_APP_API_BASE_URL in frontend/.env to the same free port, then restart both servers.');
  } else {
    console.error('Failed to start Interview Nailer API:', error.message);
  }
  process.exitCode = 1;
});
