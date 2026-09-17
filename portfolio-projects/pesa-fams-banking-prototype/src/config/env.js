const path = require("node:path");
const dotenv = require("dotenv");

let loaded = false;

function loadEnv() {
  if (loaded) return;
  dotenv.config({ path: path.join(process.cwd(), ".env"), quiet: true });
  loaded = true;
}

function boolFromEnv(value, fallback = false) {
  if (value == null || value === "") return fallback;
  return String(value).toLowerCase() === "true";
}

function getEnv(overrides = {}) {
  loadEnv();

  return {
    appMode: overrides.APP_MODE || process.env.APP_MODE || (process.env.DATABASE_URL ? "database" : "prototype"),
    host: overrides.HOST || process.env.HOST || "0.0.0.0",
    port: Number(overrides.PORT || process.env.PORT || 3100),
    databaseUrl: overrides.DATABASE_URL || process.env.DATABASE_URL || "",
    jwtSecret: overrides.JWT_SECRET || process.env.JWT_SECRET || "prototype-secret",
    jwtExpiresIn: overrides.JWT_EXPIRES_IN || process.env.JWT_EXPIRES_IN || "8h",
    demoCredentialsEnabled: boolFromEnv(overrides.DEMO_CREDENTIALS_ENABLED || process.env.DEMO_CREDENTIALS_ENABLED, true),
    workerId: overrides.JOB_WORKER_ID || process.env.JOB_WORKER_ID || "local-worker-1"
  };
}

module.exports = {
  getEnv
};
