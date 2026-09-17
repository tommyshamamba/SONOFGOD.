const { query } = require("../config/db");

async function enqueueJob(client, input) {
  const result = await client.query(
    `
      INSERT INTO job_queue (
        job_type,
        payload,
        status,
        run_after,
        max_attempts
      )
      VALUES ($1, $2::jsonb, 'PENDING', COALESCE($3, NOW()), COALESCE($4, 3))
      RETURNING *
    `,
    [input.jobType, JSON.stringify(input.payload || {}), input.runAfter || null, input.maxAttempts || 3]
  );
  return result.rows[0];
}

async function listPendingJobs(databaseUrl) {
  const result = await query(
    databaseUrl,
    `
      SELECT *
      FROM job_queue
      WHERE status = 'PENDING'
        AND run_after <= NOW()
      ORDER BY run_after ASC, created_at ASC
    `
  );
  return result.rows;
}

async function claimNextJob(client, workerId) {
  const result = await client.query(
    `
      UPDATE job_queue
      SET
        status = 'RUNNING',
        locked_at = NOW(),
        locked_by = $1,
        attempts = attempts + 1,
        updated_at = NOW()
      WHERE id = (
        SELECT id
        FROM job_queue
        WHERE status = 'PENDING'
          AND run_after <= NOW()
        ORDER BY run_after ASC, created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      RETURNING *
    `,
    [workerId]
  );
  return result.rows[0] || null;
}

async function markJobComplete(client, jobId) {
  await client.query(
    `
      UPDATE job_queue
      SET
        status = 'COMPLETED',
        completed_at = NOW(),
        updated_at = NOW()
      WHERE id = $1
    `,
    [jobId]
  );
}

async function markJobFailed(client, jobId, errorMessage) {
  await client.query(
    `
      UPDATE job_queue
      SET
        status = CASE WHEN attempts >= max_attempts THEN 'FAILED' ELSE 'PENDING' END,
        last_error = $2,
        updated_at = NOW()
      WHERE id = $1
    `,
    [jobId, errorMessage]
  );
}

module.exports = {
  enqueueJob,
  listPendingJobs,
  claimNextJob,
  markJobComplete,
  markJobFailed
};
