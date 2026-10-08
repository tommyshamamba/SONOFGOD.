const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { createApp } = require('../server');
test('local endpoints work and never disclose configured secrets', async () => {
  const server = await new Promise(resolve => { const s = createApp({ API_KEY: 'private-key', DB_PASSWORD: 'private-password', FEATURE_FLAGS: '{"enabled":true}' }).listen(0, '127.0.0.1', () => resolve(s)); });
  const url = `http://127.0.0.1:${server.address().port}`;
  try {
    for (const route of ['/health', '/ready', '/api/data', '/api/config', '/api/secret']) {
      const response = await fetch(url + route);
      assert.equal(response.status, 200);
      assert.doesNotMatch(await response.text(), /private-key|private-password/);
    }
    assert.deepEqual((await (await fetch(url + '/api/config')).json()).featureFlags, { enabled: true });
    assert.equal((await fetch(url + '/api/unknown')).status, 404);
  } finally { await new Promise(resolve => server.close(resolve)); }
});
test('invalid feature configuration fails startup', () => {
  assert.throws(() => createApp({ FEATURE_FLAGS: 'broken' }), /valid JSON/);
  assert.throws(() => createApp({ FEATURE_FLAGS: '[]' }), /object/);
});
test('built frontend serves SPA routes while preserving API and method boundaries', async (t) => {
  const build = path.join(__dirname, '../../frontend/build');
  const index = path.join(build, 'index.html');
  const cleanup = [];
  t.after(() => { for (const remove of cleanup.reverse()) remove(); });
  if (!fs.existsSync(build)) {
    fs.mkdirSync(build);
    cleanup.push(() => fs.rmdirSync(build));
  }
  if (!fs.existsSync(index)) {
    fs.writeFileSync(index, '<!doctype html><title>SPA regression fixture</title>');
    cleanup.push(() => fs.rmSync(index));
  }
  const html = fs.readFileSync(index, 'utf8');
  const asset = `/regression-${randomUUID()}.txt`;
  fs.writeFileSync(path.join(build, asset), 'static asset');
  cleanup.push(() => fs.rmSync(path.join(build, asset)));
  const server = await new Promise(resolve => { const s = createApp({}).listen(0, '127.0.0.1', () => resolve(s)); });
  t.after(() => new Promise(resolve => server.close(resolve)));
  const url = `http://127.0.0.1:${server.address().port}`;
  for (const route of ['/', '/dashboard', '/dashboard/details']) {
    const response = await fetch(url + route);
    assert.equal(response.status, 200);
    assert.match(response.headers.get('content-type'), /text\/html/);
    assert.equal(await response.text(), html);
  }
  assert.equal(await (await fetch(url + asset)).text(), 'static asset');
  assert.equal((await fetch(url + '/dashboard', { method: 'HEAD' })).status, 200);
  assert.equal((await (await fetch(url + '/api/data')).json()).message, 'Hello from Kubernetes demo!');
  for (const [route, method] of [['/api/unknown', 'GET'], ['/api/unknown/nested', 'GET'], ['/dashboard', 'POST']]) {
    const response = await fetch(url + route, { method });
    assert.equal(response.status, 404);
    assert.deepEqual(await response.json(), { error: 'Not found' });
  }
});
