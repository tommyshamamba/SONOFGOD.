const { query } = require("../config/db");

async function createImportBatch(client, input) {
  const result = await client.query(
    `
      INSERT INTO import_batches (
        source_type,
        status,
        summary,
        created_by_user_id
      )
      VALUES ($1, $2, $3::jsonb, $4)
      RETURNING *
    `,
    [input.sourceType, input.status, JSON.stringify(input.summary || {}), input.createdByUserId || null]
  );
  return result.rows[0];
}

async function insertImportBatchRows(client, batchId, rows) {
  for (const row of rows) {
    await client.query(
      `
        INSERT INTO import_batch_rows (
          import_batch_id,
          row_number,
          asset_public_id,
          tag_code,
          status,
          message,
          payload
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
      `,
      [
        batchId,
        row.rowNumber,
        row.assetPublicId || null,
        row.tagCode || null,
        row.status,
        row.message || null,
        JSON.stringify(row.payload || {})
      ]
    );
  }
}

async function listImportBatchRows(databaseUrl, batchId) {
  const result = await query(
    databaseUrl,
    `
      SELECT *
      FROM import_batch_rows
      WHERE import_batch_id = $1
      ORDER BY row_number ASC
    `,
    [batchId]
  );
  return result.rows;
}

async function findImportBatchById(databaseUrl, batchId) {
  const result = await query(
    databaseUrl,
    `
      SELECT *
      FROM import_batches
      WHERE id = $1
      LIMIT 1
    `,
    [batchId]
  );
  return result.rows[0] || null;
}

async function updateImportBatchStatus(client, batchId, status, summary) {
  const result = await client.query(
    `
      UPDATE import_batches
      SET
        status = $2,
        summary = $3::jsonb,
        imported_at = CASE WHEN $2 = 'IMPORTED' THEN NOW() ELSE imported_at END
      WHERE id = $1
      RETURNING *
    `,
    [batchId, status, JSON.stringify(summary || {})]
  );
  return result.rows[0];
}

async function updateImportBatchRowStatus(client, rowId, status, message = "") {
  const result = await client.query(
    `
      UPDATE import_batch_rows
      SET
        status = $2,
        message = $3
      WHERE id = $1
      RETURNING *
    `,
    [rowId, status, message || null]
  );
  return result.rows[0];
}

async function listImportBatches(databaseUrl, limit = 12) {
  const result = await query(
    databaseUrl,
    `
      SELECT
        ib.*,
        u.name AS created_by_name
      FROM import_batches ib
      LEFT JOIN users u ON u.id = ib.created_by_user_id
      ORDER BY ib.created_at DESC
      LIMIT $1
    `,
    [limit]
  );
  return result.rows;
}

module.exports = {
  createImportBatch,
  insertImportBatchRows,
  listImportBatchRows,
  findImportBatchById,
  updateImportBatchStatus,
  updateImportBatchRowStatus,
  listImportBatches
};
