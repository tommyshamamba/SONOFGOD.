const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { createProductionApp } = require('../src/create-production-app');
const db = require('../src/config/db');

const url = process.env.TEST_DATABASE_URL;
let server, base;
before(async () => {
  if (!url) return;
  assert.match(new URL(url).pathname, /^\/pesa_test_[a-z0-9_]+$/, 'Use a dedicated pesa_test_ database');
  const app = createProductionApp({ appMode: 'database', databaseUrl: url,
    jwtSecret: 'integration-test-only-not-for-deployment', jwtExpiresIn: '1h', demoCredentialsEnabled: true });
  server = await new Promise(resolve => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  base = `http://127.0.0.1:${server.address().port}`;
});
after(async () => {
  if (server) await new Promise(resolve => server.close(resolve));
  await db.closePool();
});

async function login(email, password) {
  const response = await fetch(`${base}/api/auth/login`, { method: 'POST',
    headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ email, password }) });
  assert.equal(response.status, 200);
  const body = await response.json();
  assert.ok(body.token);
  return { Authorization: `Bearer ${body.token}` };
}

test('real database contains all migrations and seeded assets', { skip: !url }, async () => {
  const versions = await db.query(url, 'SELECT COUNT(*)::int AS count FROM schema_migrations');
  assert.equal(versions.rows[0].count, 3);
  const assets = await db.query(url, 'SELECT COUNT(*)::int AS count FROM assets');
  assert.ok(assets.rows[0].count > 0);
});

test('database API authentication and read endpoints', { skip: !url }, async () => {
  assert.equal((await fetch(`${base}/api/assets`)).status, 401);
  const headers = await login('finance@bankdrc.cd', 'Finance123!');
  for (const endpoint of ['/api/health','/api/meta','/api/dashboard','/api/assets?page=1&pageSize=3','/api/reconciliation','/api/parallel-runs/latest']) {
    const response = await fetch(base + endpoint, { headers });
    assert.equal(response.status, 200, endpoint);
    assert.ok(await response.json());
  }
});

test('auditor cannot approve depreciation through database API', { skip: !url }, async () => {
  const headers = await login('auditor@bankdrc.cd', 'Audit123!');
  const response = await fetch(`${base}/api/depreciation/approve`, { method: 'POST',
    headers: { ...headers, 'Content-Type': 'application/json' }, body: '{}' });
  assert.equal(response.status, 403);
});

test('transaction failure rolls back writes on the real server', { skip: !url }, async () => {
  await assert.rejects(db.withTransaction(url, async client => {
    await client.query('CREATE TABLE integration_rollback_probe (id int)');
    await client.query('INSERT INTO integration_rollback_probe VALUES (1)');
    throw new Error('deliberate rollback');
  }), /deliberate rollback/);
  const result = await db.query(url, "SELECT to_regclass('public.integration_rollback_probe') AS name");
  assert.equal(result.rows[0].name, null);
});
