const path = require('path');

const storageMode = process.env.STORAGE_MODE || (process.env.DATABASE_URL ? 'postgres' : 'file');
const aiMode = process.env.AI_MODE || (process.env.ANTHROPIC_API_KEY ? 'anthropic' : 'mock');
const renderExternalUrl = process.env.RENDER_EXTERNAL_URL || '';
const configuredClientUrls = process.env.CLIENT_URL || 'http://localhost:3000';

module.exports = {
  port: Number(process.env.PORT || 5000),
  nodeEnv: process.env.NODE_ENV || 'development',
  clientUrls: [configuredClientUrls, renderExternalUrl]
    .join(',')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean),
  jwtSecret: process.env.JWT_SECRET || 'dev-only-secret-change-me',
  jwtExpiresIn: process.env.JWT_EXPIRES_IN || '7d',
  databaseUrl: process.env.DATABASE_URL || '',
  storageMode: storageMode === 'postgres' ? 'postgres' : 'file',
  aiMode: aiMode === 'anthropic' ? 'anthropic' : 'mock',
  storeFile: process.env.STORE_FILE || path.join(__dirname, '..', 'data', 'store.json'),
};
