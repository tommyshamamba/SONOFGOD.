const os = require("node:os");

const { createApp } = require("./app");
const { createProductionApp } = require("./create-production-app");
const { getEnv } = require("./config/env");

const env = getEnv();

if (env.appMode === "database" && !env.databaseUrl) {
  throw new Error("APP_MODE=database requires DATABASE_URL to be set.");
}

const app = env.appMode === "database" ? createProductionApp(env) : createApp();

function isWildcardHost(host) {
  return host === "0.0.0.0" || host === "::";
}

function collectLanUrls(host, port) {
  const urls = new Set();
  urls.add(`http://localhost:${port}`);

  if (!isWildcardHost(host)) {
    if (host !== "127.0.0.1" && host !== "::1") {
      urls.add(`http://${host}:${port}`);
    }
    return Array.from(urls);
  }

  const interfaces = os.networkInterfaces();
  for (const group of Object.values(interfaces)) {
    for (const entry of group || []) {
      if (entry.family !== "IPv4" || entry.internal) continue;
      urls.add(`http://${entry.address}:${port}`);
    }
  }

  return Array.from(urls);
}

app.listen(env.port, env.host, () => {
  const mode = env.appMode === "database" ? "database" : "prototype";
  const urls = collectLanUrls(env.host, env.port);

  console.log(`PESA FAMS server running in ${mode} mode`);
  console.log(`Bound host: ${env.host}:${env.port}`);
  console.log("Available URLs:");
  for (const url of urls) {
    console.log(`  ${url}`);
  }
  console.log("Use one of the LAN URLs above when opening the app from another PC on the same network.");
});
