const { test } = require('node:test');
const assert = require('node:assert/strict');
const { getEnv } = require('../src/config/env');

test('configuration rejects invalid modes and unsafe production defaults', () => {
  assert.throws(() => getEnv({ APP_MODE: 'typo' }), /APP_MODE/);
  assert.throws(() => getEnv({ NODE_ENV: 'production', APP_MODE: 'prototype' }), /database mode/);
  assert.throws(() => getEnv({ NODE_ENV: 'production', APP_MODE: 'database', DATABASE_URL: 'postgres://localhost/test', JWT_SECRET: 'short' }), /JWT_SECRET/);
  const production = { NODE_ENV: 'production', APP_MODE: 'database', DATABASE_URL: 'postgres://localhost/test', JWT_SECRET: 'test-only-secret-32-characters-long', DEMO_CREDENTIALS_ENABLED: 'true' };
  assert.throws(() => getEnv(production), /demo credentials/);
  assert.equal(getEnv({ ...production, DEMO_CREDENTIALS_ENABLED: 'false' }).demoCredentialsEnabled, false);
});
