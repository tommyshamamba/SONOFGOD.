const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { createProductionApp } = require('../src/create-production-app');
const db = require('../src/config/db');
const { randomUUID } = require('node:crypto');

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

async function post(endpoint, headers, body, method = 'POST') {
  return fetch(base + endpoint, { method, headers: { ...headers, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
}

test('asset writes persist, reject duplicates and enforce branch boundaries', { skip: !url }, async () => {
  const finance = await login('finance@bankdrc.cd', 'Finance123!');
  const admin = await login('admin@bankdrc.cd', 'Admin123!');
  const id = `TEST-${randomUUID()}`;
  const asset = { assetId: id, tagCode: id, name: 'Integration workstation', categoryKey: 'COMPUTER_EQUIPMENT',
    branchCode: 'HQ-KIN', currency: 'USD', acquisitionCost: 1000, residualValue: 50,
    capitalisationDate: '2026-01-01', usefulLifeMonths: 48, depreciationMethod: 'SLM' };
  assert.equal((await post('/api/assets', finance, {})).status, 400);
  assert.equal((await post('/api/assets', admin, asset)).status, 403);
  const created = await post('/api/assets', finance, asset);
  assert.equal(created.status, 200, await created.text());
  assert.equal((await post('/api/assets', finance, asset)).status, 409);
  assert.equal((await post(`/api/assets/${id}`, admin, { ...asset, name: 'Forbidden change' }, 'PATCH')).status, 403);
  assert.equal((await post(`/api/assets/${id}`, finance, { ...asset, name: 'Updated workstation' }, 'PATCH')).status, 200);
  const persisted = await db.query(url, 'SELECT name FROM assets WHERE asset_id=$1', [id]);
  assert.equal(persisted.rows[0].name, 'Updated workstation');
  const audit = await db.query(url, "SELECT action FROM audit_logs WHERE entity_id=$1 AND action IN ('ASSET_CREATE','ASSET_UPDATE')", [id]);
  assert.equal(audit.rowCount, 2);
});

test('maker/checker and concurrent depreciation requests post exactly once', { skip: !url }, async () => {
  const finance = await login('finance@bankdrc.cd', 'Finance123!');
  const makerId = (await db.query(url, "SELECT id FROM users WHERE email='finance@bankdrc.cd'")).rows[0].id;
  const checkerId = randomUUID();
  await db.query(url, `INSERT INTO users (id, email, password_hash, name, role, is_active)
    SELECT $1, $2, password_hash, 'Integration checker', 'finance_admin', true FROM users WHERE id=$3`,
    [checkerId, `checker-${checkerId}@example.test`, makerId]);
  const jwt = require('jsonwebtoken');
  const checker = { Authorization: `Bearer ${jwt.sign({ sub: checkerId, role: 'finance_admin' }, 'integration-test-only-not-for-deployment')}` };
  const year = 2100 + Math.floor(Math.random() * 7000);
  const period = `${year}-01`;
  assert.equal((await post('/api/depreciation/run', finance, { period: 'bad' })).status, 400);
  const created = await post('/api/depreciation/run', finance, { period });
  const body = await created.json();
  assert.equal(created.status, 200, JSON.stringify(body));
  const runId = body.currentRun.id;
  assert.ok(runId);
  assert.equal((await post('/api/depreciation/approve', finance, { runId })).status, 409);
  const before = (await db.query(url, 'SELECT SUM(accumulated_depreciation)::text AS total FROM assets')).rows[0].total;
  const charge = (await db.query(url, "SELECT COALESCE(SUM(depreciation_charge),0)::text AS total FROM depreciation_lines WHERE depreciation_run_id=$1 AND posting_status='PENDING'", [runId])).rows[0].total;
  const responses = await Promise.all([post('/api/depreciation/approve', checker, { runId }), post('/api/depreciation/approve', checker, { runId })]);
  assert.deepEqual(responses.map(r => r.status).sort(), [200, 409]);
  const after = (await db.query(url, 'SELECT SUM(accumulated_depreciation)::text AS total FROM assets')).rows[0].total;
  assert.ok(Math.abs(Number(after) - Number(before) - Number(charge)) < 0.01);
  assert.equal((await db.query(url, "SELECT id FROM audit_logs WHERE action='DEPRECIATION_APPROVE' AND entity_id=$1", [runId])).rowCount, 1);
  assert.equal((await post('/api/depreciation/run', finance, { period })).status, 409);
  const retried = await Promise.all([post('/api/depreciation/retry-failures', checker, { runId }), post('/api/depreciation/retry-failures', checker, { runId })]);
  assert.deepEqual(retried.map(r => r.status).sort(), [200, 409]);
});

test('disabled users cannot reuse a previously issued token', { skip: !url }, async () => {
  const headers = await login('auditor@bankdrc.cd', 'Audit123!');
  await db.query(url, "UPDATE users SET is_active=false WHERE email='auditor@bankdrc.cd'");
  try { assert.equal((await fetch(base + '/api/meta', { headers })).status, 401); }
  finally { await db.query(url, "UPDATE users SET is_active=true WHERE email='auditor@bankdrc.cd'"); }
});
