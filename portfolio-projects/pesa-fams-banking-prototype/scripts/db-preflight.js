const fs = require("node:fs");
const net = require("node:net");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const { getEnv } = require("../src/config/env");

function commandExists(command) {
  const lookup = process.platform === "win32" ? "where.exe" : "which";
  const result = spawnSync(lookup, [command], { stdio: "ignore" });
  return result.status === 0;
}

function parseConnection(databaseUrl) {
  const url = new URL(databaseUrl);
  return {
    host: url.hostname || "localhost",
    port: Number(url.port || 5432),
    database: url.pathname.replace(/^\//, "") || "postgres"
  };
}

function canReachTcp(host, port, timeoutMs = 1500) {
  return new Promise((resolve) => {
    const socket = net.createConnection({ host, port });
    const finish = (status) => {
      socket.removeAllListeners();
      socket.destroy();
      resolve(status);
    };

    socket.setTimeout(timeoutMs);
    socket.on("connect", () => finish(true));
    socket.on("timeout", () => finish(false));
    socket.on("error", () => finish(false));
  });
}

async function main() {
  const env = getEnv();
  const envPath = path.join(process.cwd(), ".env");
  const checks = [];

  checks.push({
    label: ".env file",
    ok: fs.existsSync(envPath),
    detail: fs.existsSync(envPath) ? envPath : "Missing .env file."
  });

  checks.push({
    label: "APP_MODE",
    ok: env.appMode === "database",
    detail: `Current mode: ${env.appMode}`
  });

  checks.push({
    label: "DATABASE_URL",
    ok: Boolean(env.databaseUrl),
    detail: env.databaseUrl ? "Configured" : "DATABASE_URL is missing."
  });

  if (env.databaseUrl) {
    const connection = parseConnection(env.databaseUrl);
    checks.push({
      label: "Database target",
      ok: true,
      detail: `${connection.host}:${connection.port}/${connection.database}`
    });

    const tcpReachable = await canReachTcp(connection.host, connection.port);
    checks.push({
      label: "TCP connection",
      ok: tcpReachable,
      detail: tcpReachable ? "PostgreSQL port is reachable." : "PostgreSQL port is not reachable yet."
    });
  }

  checks.push({
    label: "Docker CLI",
    ok: commandExists("docker"),
    detail: commandExists("docker") ? "docker is installed." : "docker command not found."
  });

  checks.push({
    label: "Node dependencies",
    ok: ["express", "pg", "jsonwebtoken", "zod"].every((name) => {
      try {
        require.resolve(name);
        return true;
      } catch (_error) {
        return false;
      }
    }),
    detail: "express, pg, jsonwebtoken, and zod resolve successfully."
  });

  const failing = checks.filter((check) => !check.ok);

  console.log("PESA FAMS database preflight");
  for (const check of checks) {
    console.log(`${check.ok ? "[ok]" : "[x]"} ${check.label}: ${check.detail}`);
  }

  if (failing.length > 0) {
    console.error("\nPreflight did not pass. Fix the failing checks, then run `node scripts/db-up.js`.");
    process.exitCode = 1;
    return;
  }

  console.log("\nPreflight passed. You can run `node scripts/db-up.js` next.");
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
