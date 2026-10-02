const path = require('path');

function loadConfig(env = process.env) {
  const nodeEnv = env.NODE_ENV || 'development';
  const production = nodeEnv === 'production';
  const storageMode = env.STORAGE_MODE || (env.DATABASE_URL ? 'postgres' : 'file');
  const aiMode = env.AI_MODE || (env.ANTHROPIC_API_KEY ? 'anthropic' : 'mock');
  const jwtSecret = env.JWT_SECRET || 'dev-only-secret-change-me';
  const port = Number(env.PORT || 5000);
  const trustProxyHops = Number(env.TRUST_PROXY_HOPS || 0);
  if (!['file', 'postgres'].includes(storageMode)) throw new Error('STORAGE_MODE must be file or postgres.');
  if (!['mock', 'anthropic'].includes(aiMode)) throw new Error('AI_MODE must be mock or anthropic.');
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('PORT must be a valid TCP port.');
  if (!Number.isInteger(trustProxyHops) || trustProxyHops < 0 || trustProxyHops > 5) throw new Error('TRUST_PROXY_HOPS must be between 0 and 5.');
  if (storageMode === 'postgres' && !/^postgres(ql)?:\/\//.test(env.DATABASE_URL || '')) {
    throw new Error('PostgreSQL mode requires a DATABASE_URL.');
  }
  if (aiMode === 'anthropic' && (!env.ANTHROPIC_API_KEY || /your_|placeholder/i.test(env.ANTHROPIC_API_KEY))) {
    throw new Error('Anthropic mode requires a real ANTHROPIC_API_KEY.');
  }
  if (production && (jwtSecret.length < 32 || /change.?me|dev-only|your.?secret|placeholder/i.test(jwtSecret))) {
    throw new Error('Production requires a random JWT_SECRET of at least 32 characters.');
  }
  if (production && storageMode !== 'postgres') throw new Error('Production requires PostgreSQL; file storage supports one local process only.');
  if (production && aiMode === 'mock' && env.ALLOW_MOCK_AI !== 'true') {
    throw new Error('Set ALLOW_MOCK_AI=true explicitly to deploy a labeled mock demo.');
  }
  const clientUrls = [env.CLIENT_URL || 'http://localhost:3001,http://127.0.0.1:3001,http://localhost:3000,http://127.0.0.1:3000,http://localhost:5000,http://127.0.0.1:5000', env.RENDER_EXTERNAL_URL || '']
    .join(',').split(',').map((value) => value.trim()).filter(Boolean);
  for (const origin of clientUrls) {
    const parsed = new URL(origin);
    if (!['http:', 'https:'].includes(parsed.protocol) || parsed.pathname !== '/' || parsed.search || parsed.hash) {
      throw new Error('CLIENT_URL must contain HTTP(S) origins, without paths.');
    }
  }
  if (production && !clientUrls.some((url) => url.startsWith('https://'))) {
    throw new Error('Production requires a HTTPS CLIENT_URL or RENDER_EXTERNAL_URL.');
  }
  if (env.DATABASE_SSL && !['true', 'false'].includes(env.DATABASE_SSL)) throw new Error('DATABASE_SSL must be true or false.');
  return {
    port, nodeEnv, clientUrls, jwtSecret, storageMode, aiMode, trustProxyHops,
    jwtExpiresIn: env.JWT_EXPIRES_IN || '7d',
    databaseUrl: env.DATABASE_URL || '',
    databaseSsl: env.DATABASE_SSL ? env.DATABASE_SSL === 'true' : production,
    anthropicModel: env.ANTHROPIC_MODEL || 'claude-sonnet-4-20250514',
    storeFile: path.resolve(env.STORE_FILE || path.join(__dirname, '..', 'data', 'store.json')),
  };
}

module.exports = { ...loadConfig(), loadConfig };
