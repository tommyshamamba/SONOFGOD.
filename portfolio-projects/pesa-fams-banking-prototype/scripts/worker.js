const { getEnv } = require("../src/config/env");
const { withTransaction, closePool } = require("../src/config/db");
const jobRepository = require("../src/repositories/jobRepository");
const userRepository = require("../src/repositories/userRepository");
const systemService = require("../src/services/systemService");

async function processNextJob(env) {
  const claimed = await withTransaction(env.databaseUrl, async (client) => jobRepository.claimNextJob(client, env.workerId));
  if (!claimed) {
    console.log("No pending jobs available.");
    return false;
  }

  const financeUser = await userRepository.findUserByEmail(env.databaseUrl, "finance@bankdrc.cd");
  const itUser = await userRepository.findUserByEmail(env.databaseUrl, "it@bankdrc.cd");

  try {
    if (claimed.job_type === "monthly-depreciation") {
      await systemService.runDepreciation(env, { id: financeUser.id, role: financeUser.role }, claimed.payload.period);
    } else if (claimed.job_type === "daily-reconciliation") {
      await systemService.runReconciliation(env, { id: itUser.id, role: itUser.role }, claimed.payload.period);
    } else if (claimed.job_type === "parallel-run-compare") {
      await systemService.createParallelRun(env, { id: financeUser.id, role: financeUser.role }, claimed.payload);
    }

    await withTransaction(env.databaseUrl, async (client) => {
      await jobRepository.markJobComplete(client, claimed.id);
    });
    console.log(`Completed job ${claimed.id} (${claimed.job_type})`);
    return true;
  } catch (error) {
    await withTransaction(env.databaseUrl, async (client) => {
      await jobRepository.markJobFailed(client, claimed.id, error.message);
    });
    console.error(`Job ${claimed.id} failed:`, error.message);
    return true;
  }
}

async function main() {
  const env = getEnv();
  if (!env.databaseUrl) {
    throw new Error("DATABASE_URL is required to run the job worker.");
  }

  let processed = true;
  while (processed) {
    processed = await processNextJob(env);
  }
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });
