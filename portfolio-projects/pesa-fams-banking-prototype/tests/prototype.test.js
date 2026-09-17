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

test("finance admin can login and approve depreciation", async () => {
  await withServer(async (baseUrl) => {
    const login = await fetch(`${baseUrl}/api/auth/login`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ email: "finance@bankdrc.cd", password: "Finance123!" }) });
    const token = (await login.json()).token;

    const run = await fetch(`${baseUrl}/api/depreciation/run`, { method: "POST", headers: { Authorization: `Bearer ${token}` } });
    assert.equal(run.status, 200);

    const approve = await fetch(`${baseUrl}/api/depreciation/approve`, { method: "POST", headers: { Authorization: `Bearer ${token}` } });
    const payload = await approve.json();
    assert.equal(approve.status, 200);
    assert.equal(payload.currentRun.status, "POSTED");
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
