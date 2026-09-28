const { test } = require('node:test');
const assert = require('node:assert/strict');
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
