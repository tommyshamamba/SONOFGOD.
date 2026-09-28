const { Pool } = require('pg');

const { databaseUrl, databaseSsl, storageMode } = require('./env');

let pool = null;

if (storageMode === 'postgres') {
  pool = new Pool({
    connectionString: databaseUrl,
    ssl: databaseSsl ? { rejectUnauthorized: true } : false,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
  });

  pool.on('error', (err) => {
    console.error('Unexpected DB client error', err);
  });
}

async function query(text, params) {
  if (!pool) {
    throw new Error('PostgreSQL storage mode is not enabled.');
  }

  return pool.query(text, params);
}

module.exports = { query, pool };
