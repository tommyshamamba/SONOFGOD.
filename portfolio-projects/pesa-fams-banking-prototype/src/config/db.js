const { Pool } = require("pg");

let pool;

function getPool(databaseUrl) {
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for database mode.");
  }

  if (!pool) {
    pool = new Pool({
      connectionString: databaseUrl
    });
  }

  return pool;
}

async function query(databaseUrl, text, params = []) {
  const activePool = getPool(databaseUrl);
  return activePool.query(text, params);
}

async function pingDatabase(databaseUrl) {
  const result = await query(databaseUrl, "SELECT 1 AS ok");
  return result.rows[0]?.ok === 1;
}

async function withTransaction(databaseUrl, callback) {
  const client = await getPool(databaseUrl).connect();
  try {
    await client.query("BEGIN");
    const result = await callback(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function closePool() {
  if (pool) {
    const current = pool;
    pool = null;
    await current.end();
  }
}

module.exports = {
  getPool,
  query,
  pingDatabase,
  withTransaction,
  closePool
};
