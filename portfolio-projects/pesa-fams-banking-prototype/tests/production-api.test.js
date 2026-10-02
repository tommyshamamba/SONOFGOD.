const { test } = require('node:test');
const assert = require('node:assert/strict');
const jwt = require('jsonwebtoken');
const systemService = require('../src/services/systemService');
const { createProductionApp } = require('../src/create-production-app');

async function withServer(run) {
  const secret = 'isolated-api-test-secret-not-for-production';
  const app = createProductionApp({ jwtSecret: secret, demoCredentialsEnabled: false });
  const server = await new Promise(resolve => {
    const instance = app.listen(0, '127.0.0.1', () => resolve(instance));
  });
  const base = `http://127.0.0.1:${server.address().port}`;
  const headers = {
    Authorization: `Bearer ${jwt.sign({ sub: 'test-user' }, secret, { algorithm: 'HS256' })}`,
    'Content-Type': 'application/json',
  };
  try { await run(base, headers); }
  finally { await new Promise(resolve => server.close(resolve)); }
}

test('database API accepts nonempty job payloads and validates retry options', async (t) => {
  t.mock.method(systemService, 'getCurrentUserContext', async () => ({ user: { id: 'test-user', role: 'it_admin' } }));
  const queued = [];
  t.mock.method(systemService, 'enqueueJob', async (_env, _user, input) => {
    queued.push(input);
    return { id: 'queued-job', ...input };
  });
  await withServer(async (base, headers) => {
    const input = { jobType: 'monthly-depreciation', payload: { period: '2026-03', details: { source: 'test' } }, maxAttempts: 3 };
    const response = await fetch(base + '/api/jobs/enqueue', { method: 'POST', headers, body: JSON.stringify(input) });
    assert.equal(response.status, 200);
    assert.deepEqual((await response.json()).payload, input.payload);
    for (const invalid of [{ maxAttempts: 0 }, { maxAttempts: 11 }, { runAfter: 'not-a-date' }, { payload: [] }]) {
      const rejected = await fetch(base + '/api/jobs/enqueue', { method: 'POST', headers, body: JSON.stringify({ ...input, ...invalid }) });
      assert.equal(rejected.status, 400);
    }
    assert.equal(queued.length, 1);
  });
});

test('database API rejects fractional, negative and oversized page parameters', async (t) => {
  t.mock.method(systemService, 'getCurrentUserContext', async () => ({ user: { id: 'test-user', role: 'finance_admin' } }));
  const list = t.mock.method(systemService, 'listAssets', async () => ({ items: [] }));
  await withServer(async (base, headers) => {
    for (const query of ['page=-1', 'page=1.5', 'pageSize=101', 'pageSize=0']) {
      const response = await fetch(base + '/api/assets?' + query, { headers });
      assert.equal(response.status, 400, query);
    }
    assert.equal(list.mock.callCount(), 0);
  });
});

test('database API returns JSON for malformed requests and missing endpoints', async () => {
  await withServer(async (base, headers) => {
    const malformed = await fetch(base + '/api/auth/login', { method: 'POST', headers, body: '{"password":' });
    assert.equal(malformed.status, 400);
    assert.deepEqual(await malformed.json(), { error: 'Invalid request data.' });
    const missing = await fetch(base + '/api/missing');
    assert.equal(missing.status, 404);
    assert.deepEqual(await missing.json(), { error: 'Route not found.' });
  });
});
