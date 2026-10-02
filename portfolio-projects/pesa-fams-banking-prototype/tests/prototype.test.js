const test = require("node:test");
const assert = require("node:assert/strict");
const { createApp } = require("../src/app");

async function withServer(run) {
  const app = createApp();
  const server = await new Promise((resolve) => {
    const instance = app.listen(0, () => resolve(instance));
  });
  const baseUrl = `http://127.0.0.1:${server.address().port}`;
  try {
    await run(baseUrl);
  } finally {
    await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  }
}

test("health endpoint returns status", async () => {
  await withServer(async (baseUrl) => {
    const response = await fetch(`${baseUrl}/api/health`);
    const payload = await response.json();
    assert.equal(response.status, 200);
    assert.equal(payload.status, "ok");
  });
});

test("IT prepares a batch and a separate finance checker approves simulated posting once", async () => {
  await withServer(async (baseUrl) => {
    const login = await fetch(`${baseUrl}/api/auth/login`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ email: "finance@bankdrc.cd", password: "Finance123!" }) });
    const token = (await login.json()).token;
    const makerLogin = await fetch(`${baseUrl}/api/auth/login`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ email: "it@bankdrc.cd", password: "ITAdmin123!" }) });
    const makerToken = (await makerLogin.json()).token;

    const run = await fetch(`${baseUrl}/api/depreciation/run`, { method: "POST", headers: { Authorization: `Bearer ${makerToken}` } });
    assert.equal(run.status, 200);
    assert.equal((await run.json()).currentRun.runByUserId, 'usr-it');

    const approve = await fetch(`${baseUrl}/api/depreciation/approve`, { method: "POST", headers: { Authorization: `Bearer ${token}` } });
    const payload = await approve.json();
    assert.equal(approve.status, 200);
    assert.equal(payload.currentRun.status, "POSTED");
    assert.match(payload.currentRun.summary, /simulated/i);
    assert.doesNotMatch(payload.currentRun.summary, /Posted to Finacle/);
    assert.equal((await fetch(`${baseUrl}/api/depreciation/approve`, { method: "POST", headers: { Authorization: `Bearer ${token}` } })).status, 409);
    assert.equal((await fetch(`${baseUrl}/api/depreciation/run`, { method: "POST", headers: { Authorization: `Bearer ${makerToken}` } })).status, 409);
  });
});

test("prototype rejects self-approval even when the maker has finance permission", async () => {
  await withServer(async (baseUrl) => {
    const login = await fetch(`${baseUrl}/api/auth/login`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ email: "finance@bankdrc.cd", password: "Finance123!" }) });
    const headers = { Authorization: `Bearer ${(await login.json()).token}` };
    assert.equal((await fetch(`${baseUrl}/api/depreciation/run`, { method: 'POST', headers })).status, 200);
    const approval = await fetch(`${baseUrl}/api/depreciation/approve`, { method: 'POST', headers });
    assert.equal(approval.status, 409);
    assert.match((await approval.json()).error, /preparer cannot approve/i);
    const run = await (await fetch(`${baseUrl}/api/depreciation`, { headers })).json();
    assert.equal(run.currentRun.status, 'PENDING_APPROVAL');
    assert.equal(run.currentRun.approvedBy, null);
  });
});

test("prototype advertises supported role actions without enabling database-only controls", async () => {
  await withServer(async (baseUrl) => {
    for (const [email, password, allowed] of [
      ['it@bankdrc.cd', 'ITAdmin123!', ['depreciation.run', 'reconciliation.run']],
      ['finance@bankdrc.cd', 'Finance123!', ['depreciation.approve', 'assets.verify']],
      ['auditor@bankdrc.cd', 'Audit123!', ['reports.export', 'audit.read']],
    ]) {
      const response = await fetch(`${baseUrl}/api/auth/login`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ email, password }) });
      const login = await response.json();
      const headers = { Authorization: `Bearer ${login.token}` };
      const meta = await (await fetch(`${baseUrl}/api/meta`, { headers })).json();
      assert.equal(meta.mode, 'prototype');
      assert.deepEqual(meta.user.permissions, login.user.permissions);
      for (const permission of allowed) assert.ok(meta.user.permissions.includes(permission));
      for (const unsupported of ['assets.create', 'assets.import', 'admin.users.manage', 'depreciation.retry_failures', 'approvals.decide']) {
        assert.ok(!meta.user.permissions.includes(unsupported));
      }
      if (email.startsWith('auditor')) {
        assert.ok(!meta.user.permissions.includes('depreciation.run'));
        for (const endpoint of ['/api/depreciation/run', '/api/depreciation/approve', '/api/reconciliation/run', '/api/verification/submit']) {
          assert.equal((await fetch(baseUrl + endpoint, { method: 'POST', headers })).status, 403, endpoint);
        }
      }
    }
  });
});

test("auditor cannot approve depreciation", async () => {
  await withServer(async (baseUrl) => {
    const login = await fetch(`${baseUrl}/api/auth/login`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ email: "auditor@bankdrc.cd", password: "Audit123!" }) });
    const token = (await login.json()).token;
    const response = await fetch(`${baseUrl}/api/depreciation/approve`, { method: "POST", headers: { Authorization: `Bearer ${token}` } });
    assert.equal(response.status, 403);
  });
});

test("operations user only sees their branch assets", async () => {
  await withServer(async (baseUrl) => {
    const login = await fetch(`${baseUrl}/api/auth/login`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ email: "operations@bankdrc.cd", password: "Ops123!" }) });
    const token = (await login.json()).token;
    const response = await fetch(`${baseUrl}/api/assets?page=1&pageSize=20`, { headers: { Authorization: `Bearer ${token}` } });
    const payload = await response.json();
    assert.equal(response.status, 200);
    assert.ok(payload.items.length > 0);
    assert.ok(payload.items.every((asset) => asset.branchCode === "BR-GOM"));
  });
});

test("branch administration is limited to its assigned branch", async () => {
  await withServer(async (baseUrl) => {
    const login = await fetch(`${baseUrl}/api/auth/login`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ email: 'admin@bankdrc.cd', password: 'Admin123!' }) });
    const headers = { Authorization: `Bearer ${(await login.json()).token}` };
    const assets = await (await fetch(`${baseUrl}/api/assets?page=1&pageSize=20`, { headers })).json();
    assert.ok(assets.items.length > 0);
    assert.ok(assets.items.every(asset => asset.branchCode === 'BR-LUB'));
    const meta = await (await fetch(`${baseUrl}/api/meta`, { headers })).json();
    assert.deepEqual(meta.branches.map(branch => branch.code), ['BR-LUB']);
  });
});
