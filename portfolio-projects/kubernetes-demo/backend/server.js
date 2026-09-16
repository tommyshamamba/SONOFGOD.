const express = require('express');
const cors = require('cors');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'healthy', timestamp: new Date().toISOString() });
});

// API endpoint
app.get('/api/data', (req, res) => {
  res.json({
    message: 'Hello from Kubernetes!',
    environment: process.env.NODE_ENV || 'development',
    podName: process.env.POD_NAME || 'unknown',
    nodeName: process.env.NODE_NAME || 'unknown'
  });
});

// ConfigMap data endpoint
app.get('/api/config', (req, res) => {
  res.json({
    appName: process.env.APP_NAME || 'k8s-demo',
    logLevel: process.env.LOG_LEVEL || 'info',
    featureFlags: process.env.FEATURE_FLAGS || '{}'
  });
});

// Secret data endpoint (masked)
app.get('/api/secret', (req, res) => {
  res.json({
    apiKey: process.env.API_KEY ? '***MASKED***' : 'not set',
    dbPassword: process.env.DB_PASSWORD ? '***MASKED***' : 'not set'
  });
});

app.listen(PORT, () => {
  console.log(`Backend server running on port ${PORT}`);
});
