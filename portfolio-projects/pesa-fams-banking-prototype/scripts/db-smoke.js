const { getEnv } = require("../src/config/env");

async function requestJson(url, options = {}) {
  let response;
  try {
    response = await fetch(url, {
      headers: {
        "Content-Type": "application/json",
        ...(options.headers || {})
      },
      ...options
    });
  } catch (error) {
    throw new Error(`Could not reach ${url}. Start the API first with \`node src/server.js\`.`);
  }

  const contentType = response.headers.get("content-type") || "";
  const body = contentType.includes("application/json") ? await response.json() : await response.text();
  return { response, body };
}

async function main() {
  const env = getEnv();
  const baseUrl = `http://localhost:${env.port}`;

  console.log(`Running smoke checks against ${baseUrl}`);

  const health = await requestJson(`${baseUrl}/api/health`);
  if (!health.response.ok) {
    throw new Error(`Health check failed with ${health.response.status}: ${JSON.stringify(health.body)}`);
  }
  console.log("[ok] /api/health");

  const login = await requestJson(`${baseUrl}/api/auth/login`, {
    method: "POST",
    body: JSON.stringify({
      email: "finance@bankdrc.cd",
      password: "Finance123!"
    })
  });
  if (!login.response.ok || !login.body.token) {
    throw new Error(`Login failed with ${login.response.status}: ${JSON.stringify(login.body)}`);
  }
  console.log("[ok] /api/auth/login");

  const token = login.body.token;
  const authHeaders = { Authorization: `Bearer ${token}` };

  const checks = [
    "/api/meta",
    "/api/dashboard",
    "/api/assets?page=1&pageSize=3",
    "/api/reconciliation",
    "/api/parallel-runs/latest"
  ];

  for (const endpoint of checks) {
    const result = await requestJson(`${baseUrl}${endpoint}`, { headers: authHeaders });
    if (!result.response.ok) {
      throw new Error(`${endpoint} failed with ${result.response.status}: ${JSON.stringify(result.body)}`);
    }
    console.log(`[ok] ${endpoint}`);
  }

  console.log("Smoke checks passed.");
}

main().catch((error) => {
  console.error(error.message || error);
  process.exitCode = 1;
});
