require('dotenv').config();

const fs = require('fs');
const path = require('path');

const db = require('../config/db');
const { storageMode } = require('../config/env');

async function main() {
  if (storageMode !== 'postgres') {
    console.log('Database init skipped because STORAGE_MODE is not set to postgres.');
    return;
  }

  const schemaPath = path.join(__dirname, '..', 'config', 'schema.sql');
  const sql = fs.readFileSync(schemaPath, 'utf8');
  await db.query(sql);
  console.log('Database schema applied successfully.');
}

main()
  .catch((error) => {
    console.error('Database init failed:', error.message);
    process.exitCode = 1;
  })
  .finally(async () => {
    if (db.pool) {
      await db.pool.end();
    }
  });
