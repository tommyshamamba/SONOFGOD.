const { test, after } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { once } = require('node:events');
const { createApp, readConfig } = require('../server');
const { FileStore } = require('../store');

const password = 'correct horse battery staple';
const account = `0x${'1'.repeat(40)}`;
const testFolders = [];
after(() => { for (const folder of testFolders) fs.rmSync(folder, { recursive: true, force: true }); });
function temporary(t) {
  const folder = path.join(__dirname, '..', '.test-data', randomUUID());
  fs.mkdirSync(folder, { recursive: true });
  testFolders.push(folder);
  return path.join(folder, 'store.json');
}
async function start(filename, overrides = {}) {
  const { env: extraEnv = {}, ...options } = overrides;
  const service = createApp({ env: { DATA_FILE: filename, DEMO_MODE: 'true', JWT_SECRET: 'test-signing-key-not-used-outside-tests', ...extraEnv }, ...options });
  const server = service.app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const base = `http://127.0.0.1:${server.address().port}`;
  return {
    async request(url, method = 'GET', body, token, apiKey) {
      const response = await fetch(`${base}${url}`, { method, headers: {
        'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}), ...(apiKey ? { 'X-API-Key': apiKey } : {}),
      }, ...(body ? { body: JSON.stringify(body) } : {}) });
      return { status: response.status, body: await response.json(), headers: response.headers };
    },
    async close() { await new Promise(resolve => server.close(resolve)); await service.close(); },
  };
}
async function register(client, email = 'owner@example.com') {
  const response = await client.request('/api/auth/register', 'POST', { email, password });
  assert.equal(response.status, 201);
  return response.body.token;
}
async function key(client, token) {
  const response = await client.request('/api/keys', 'POST', { name: 'Testing' }, token);
  assert.equal(response.status, 201);
  return response.body;
}

test('accounts, hashed keys, usage and revocations persist across application restart', async t => {
  const filename = temporary(t);
  let client = await start(filename);
  t.after(() => client.close());
  const token = await register(client, 'Owner@Example.com');
  const created = await key(client, token);
  assert.equal((await client.request(`/api/v1/ethereum/balance/${account}`, 'GET', undefined, undefined, created.apiKey)).status, 200);
  const saved = fs.readFileSync(filename, 'utf8');
  assert.ok(!saved.includes(created.apiKey));
  assert.ok(!saved.includes(password));
  await client.close();
  client = await start(filename);
  const login = await client.request('/api/auth/login', 'POST', { email: 'owner@example.com', password });
  assert.equal(login.status, 200);
  const listed = await client.request('/api/keys', 'GET', undefined, login.body.token);
  assert.equal(listed.body.keys[0].requests, 1);
  assert.equal(listed.body.keys[0].id, created.id);
  assert.equal(listed.body.keys[0].digest, undefined);
  assert.equal((await client.request('/api/v1/usage', 'GET', undefined, undefined, created.apiKey)).status, 200);
  assert.equal((await client.request(`/api/keys/${created.id}`, 'DELETE', undefined, login.body.token)).status, 200);
  await client.close();
  client = await start(filename);
  assert.equal((await client.request('/api/v1/usage', 'GET', undefined, undefined, created.apiKey)).status, 401);
});

test('key ownership and exact IDs prevent unauthorized or prefix revocation', async t => {
  const client = await start(temporary(t)); t.after(() => client.close());
  const owner = await register(client);
  const other = await register(client, 'other@example.com');
  const created = await key(client, owner);
  assert.deepEqual((await client.request('/api/keys', 'GET', undefined, other)).body.keys, []);
  assert.equal((await client.request(`/api/keys/${created.id}`, 'DELETE', undefined, other)).status, 404);
  assert.equal((await client.request(`/api/keys/${created.id.slice(0, 8)}`, 'DELETE', undefined, owner)).status, 404);
  assert.equal((await client.request('/api/v1/usage', 'GET', undefined, undefined, created.apiKey)).status, 200);
  assert.equal((await client.request('/api/keys')).status, 401);
});

test('concurrent duplicate registration produces a single account', async t => {
  const client = await start(temporary(t)); t.after(() => client.close());
  const responses = await Promise.all([1, 2].map(() => client.request('/api/auth/register', 'POST', { email: 'same@example.com', password })));
  assert.deepEqual(responses.map(result => result.status).sort(), [201, 409]);
  assert.equal((await client.request('/api/auth/login', 'POST', { email: 'same@example.com', password: 'incorrect password' })).status, 401);
  assert.equal((await client.request('/api/auth/register', 'POST', { email: 'bad', password: 'short' })).status, 400);
});

test('limiter outages fail closed; quotas produce 429 with retry header and recover', async t => {
  let mode = 'outage';
  const limiter = { consume: async () => {
    if (mode === 'outage') throw new Error('redis://credential:password@private-host');
    if (mode === 'quota') throw { msBeforeNext: 1500 };
  } };
  const client = await start(temporary(t), { limiter }); t.after(() => client.close());
  const created = await key(client, await register(client));
  const request = () => client.request('/api/v1/usage', 'GET', undefined, undefined, created.apiKey);
  const unavailable = await request();
  assert.equal(unavailable.status, 503);
  assert.ok(!JSON.stringify(unavailable.body).includes('password'));
  assert.equal((await client.request('/ready')).status, 503);
  mode = 'quota';
  const quota = await request();
  assert.equal(quota.status, 429);
  assert.equal(quota.headers.get('Retry-After'), '2');
  mode = 'ready';
  assert.equal((await request()).status, 200);
  assert.equal((await client.request('/ready')).status, 200);
});

test('provider failures and deadlines return bounded sanitized errors, readiness and pending status', async t => {
  let mode = 'outage';
  let calls = 0;
  const provider = {
    getBlockNumber: async () => { if (mode === 'outage') throw new Error('private RPC secret'); return 1; },
    getBalance: () => { calls++; if (mode === 'timeout') return new Promise(() => {}); throw new Error('private RPC secret'); },
    getTransaction: async hash => mode === 'missing' ? null : ({ hash, from: account, to: account, value: 0n, blockNumber: null }),
    getTransactionReceipt: async () => null,
  };
  const client = await start(temporary(t), { providers: { ethereum: provider }, env: { RPC_TIMEOUT_MS: '30' } }); t.after(() => client.close());
  const created = await key(client, await register(client));
  const request = route => client.request(`/api/v1/ethereum/${route}`, 'GET', undefined, undefined, created.apiKey);
  assert.equal((await client.request('/ready')).status, 503);
  const failed = await request(`balance/${account}`);
  assert.equal(failed.status, 502);
  assert.ok(!JSON.stringify(failed.body).includes('secret'));
  mode = 'timeout';
  assert.equal((await request(`balance/${account}`)).status, 504);
  assert.equal((await request('balance/not-an-address')).status, 400);
  assert.equal(calls, 2);
  const tx = await request(`transaction/0x${'a'.repeat(64)}`);
  assert.equal(tx.body.status, 'pending');
  mode = 'missing';
  assert.equal((await request(`transaction/0x${'a'.repeat(64)}`)).status, 404);
  assert.equal((await client.request('/ready')).status, 200);
});

test('offline demo is explicitly labeled and broadcasting is disabled', async t => {
  const client = await start(temporary(t)); t.after(() => client.close());
  const created = await key(client, await register(client));
  assert.equal((await client.request('/health')).body.mode, 'demo');
  assert.equal((await client.request('/ready')).status, 200);
  const result = await client.request(`/api/v1/ethereum/balance/${account}`, 'GET', undefined, undefined, created.apiKey);
  assert.equal(result.body.mode, 'demo');
  assert.equal(result.body.balance, '1.0');
  const chains = await client.request('/api/v1/chains');
  assert.equal(chains.body.endpoints, undefined);
  assert.equal((await client.request('/api/v1/ethereum/broadcast', 'POST', { signedTx: '0x1234' }, undefined, created.apiKey)).status, 403);
});

test('storage rejects concurrent writers and corrupt snapshots without overwriting data', t => {
  const filename = temporary(t);
  const first = new FileStore(filename);
  assert.throws(() => new FileStore(filename), { code: 'EEXIST' });
  first.close();
  fs.writeFileSync(filename, '{bad json');
  assert.throws(() => new FileStore(filename), SyntaxError);
  assert.equal(fs.readFileSync(filename, 'utf8'), '{bad json');
  assert.equal(fs.existsSync(`${filename}.lock`), false);
});

test('production rejects default secrets and incomplete configuration before opening storage', () => {
  assert.throws(() => readConfig({ NODE_ENV: 'production' }), /JWT_SECRET/);
  assert.throws(() => readConfig({ NODE_ENV: 'production', JWT_SECRET: 'your-super-secret-jwt-key-change-in-production' }), /JWT_SECRET/);
  assert.throws(() => readConfig({ NODE_ENV: 'production', JWT_SECRET: 'a'.repeat(48) }), /DATA_FILE/);
});

test('OS storage lock releases after a forced process kill and preserves the committed snapshot', { timeout: 15000 }, async t => {
  const { spawn } = require('node:child_process');
  const filename = temporary(t);
  const program = `const { FileStore } = require(${JSON.stringify(require.resolve('../store'))}); const store = new FileStore(process.argv[1]); store.addUser({ userId: 'crash-test', email: 'crash@example.invalid' }); require('node:fs').writeSync(1, 'READY\\n'); setInterval(() => {}, 1000);`;
  const child = spawn(process.execPath, ['-e', program, filename], { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
  let exited = false;
  let stderr = '';
  child.stderr.on('data', chunk => { stderr += chunk; });
  const exit = new Promise((resolve, reject) => {
    child.once('exit', (...args) => { exited = true; resolve(args); });
    child.once('error', reject);
  });
  // Handle rejection immediately; the readiness promise below reports it too.
  exit.catch(() => {});
  t.after(async () => { if (!exited) child.kill('SIGKILL'); await exit.catch(() => {}); });
  let timer;
  try {
    await new Promise((resolve, reject) => {
      let output = '';
      child.stdout.on('data', chunk => { output += chunk; if (output.includes('READY')) resolve(); });
      child.once('error', reject);
      child.once('exit', () => reject(new Error(`Store worker exited before ready: ${stderr}`)));
      timer = setTimeout(() => reject(new Error(`Store worker readiness timeout: ${stderr}`)), 10000);
    });
  } finally { clearTimeout(timer); }
  assert.throws(() => new FileStore(filename), { code: 'EEXIST' });
  assert.equal(child.kill('SIGKILL'), true);
  await exit;
  const reopened = new FileStore(filename);
  try { assert.equal(reopened.findUser('crash@example.invalid').userId, 'crash-test'); }
  finally { reopened.close(); }
  assert.throws(() => reopened.addUser({ userId: 'late', email: 'late@example.invalid' }), /closed/);
});

test('legacy PID locks are never automatically deleted while an older service may be running', t => {
  const filename = temporary(t);
  fs.writeFileSync(`${filename}.lock`, String(process.pid));
  assert.throws(() => new FileStore(filename), { code: 'EEXIST' });
  assert.equal(fs.readFileSync(`${filename}.lock`, 'utf8'), String(process.pid));
});
