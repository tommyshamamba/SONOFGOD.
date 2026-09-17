const { spawnSync } = require("node:child_process");
const { setTimeout: delay } = require("node:timers/promises");

const { getEnv } = require("../src/config/env");
const { pingDatabase, closePool } = require("../src/config/db");

function commandExists(command) {
  const lookup = process.platform === "win32" ? "where.exe" : "which";
  const result = spawnSync(lookup, [command], { stdio: "ignore" });
  return result.status === 0;
}

function runStep(label, command, args) {
  console.log(`\n== ${label} ==`);
  const result = spawnSync(command, args, {
    stdio: "inherit",
    shell: false
  });

  if (result.status !== 0) {
    throw new Error(`${label} failed.`);
  }
}

async function waitForDatabase(databaseUrl, maxAttempts = 30) {
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      await pingDatabase(databaseUrl);
      console.log(`Database is reachable after ${attempt} check(s).`);
      return;
    } catch (_error) {
      await delay(2000);
    }
  }

  throw new Error("Database did not become reachable in time.");
}

async function main() {
  const env = getEnv();

  if (env.appMode !== "database") {
    throw new Error("APP_MODE must be set to database before bringing up the local stack.");
  }

  if (!env.databaseUrl) {
    throw new Error("DATABASE_URL is required.");
  }

  if (!commandExists("docker")) {
    throw new Error("Docker is not installed or is not on PATH. Install Docker Desktop or start a local PostgreSQL server first.");
  }

  runStep("Starting PostgreSQL container", "docker", ["compose", "up", "-d"]);
  console.log("\nWaiting for PostgreSQL to accept connections...");
  await waitForDatabase(env.databaseUrl);
  runStep("Applying migrations", process.execPath, ["scripts/migrate.js"]);
  runStep("Seeding database", process.execPath, ["scripts/seed.js"]);

  console.log("\nLocal database stack is ready.");
  console.log("Next steps:");
  console.log("  1. node src/server.js");
  console.log("  2. node scripts/db-smoke.js");
  console.log("  3. node scripts/worker.js");
}

main()
  .catch((error) => {
    console.error(error.message || error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });
