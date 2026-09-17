const { query } = require("../config/db");

async function createParallelRun(client, input) {
  const result = await client.query(
    `
      INSERT INTO parallel_run_sessions (
        period,
        source_name,
        source_snapshot,
        system_snapshot,
        variance_summary,
        status,
        created_by_user_id
      )
      VALUES ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, $6, $7)
      RETURNING *
    `,
    [
      input.period,
      input.sourceName,
      JSON.stringify(input.sourceSnapshot),
      JSON.stringify(input.systemSnapshot),
      JSON.stringify(input.varianceSummary),
      input.status,
      input.createdByUserId
    ]
  );
  return result.rows[0];
}

async function getLatestParallelRun(databaseUrl) {
  const result = await query(
    databaseUrl,
    `
      SELECT *
      FROM parallel_run_sessions
      ORDER BY created_at DESC
      LIMIT 1
    `
  );
  return result.rows[0] || null;
}

module.exports = {
  createParallelRun,
  getLatestParallelRun
};
