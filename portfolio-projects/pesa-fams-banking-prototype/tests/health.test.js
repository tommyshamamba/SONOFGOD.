const test = require("node:test");
const assert = require("node:assert/strict");

const db = require("../src/config/db");
const { createProductionApp } = require("../src/create-production-app");

async function withServer(app, run) {
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

test("database health endpoint reports up when ping succeeds", async () => {
  const originalPing = db.pingDatabase;
  db.pingDatabase = async () => true;

  try {
    const app = createProductionApp({
      appMode: "database",
      databaseUrl: "postgresql://example",
      jwtSecret: "test-secret",
      demoCredentialsEnabled: true
    });

    await withServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/api/health`);
      const payload = await response.json();

      assert.equal(response.status, 200);
      assert.equal(payload.status, "ok");
      assert.equal(payload.database.status, "up");
    });
  } finally {
    db.pingDatabase = originalPing;
  }
});

test("database health endpoint reports degraded when ping fails", async () => {
  const originalPing = db.pingDatabase;
  db.pingDatabase = async () => {
    throw new Error("connection refused");
  };

  try {
    const app = createProductionApp({
      appMode: "database",
      databaseUrl: "postgresql://example",
      jwtSecret: "test-secret",
      demoCredentialsEnabled: true
    });

    await withServer(app, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/api/health`);
      const payload = await response.json();

      assert.equal(response.status, 503);
      assert.equal(payload.status, "degraded");
      assert.equal(payload.database.status, "down");
    });
  } finally {
    db.pingDatabase = originalPing;
  }
});
