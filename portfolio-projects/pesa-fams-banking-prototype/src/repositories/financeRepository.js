const { query } = require("../config/db");

async function listDepreciationRuns(databaseUrl) {
  const result = await query(
    databaseUrl,
    `
      SELECT
        dr.id,
        dr.period,
        dr.status,
        dr.run_by_user_id,
        dr.total_assets_processed,
        dr.total_assets_skipped,
        dr.total_depreciation_usd,
        dr.total_depreciation_cdf,
        dr.exchange_rate_used,
        dr.failure_count,
        dr.summary,
        dr.gl_batch_reference,
        dr.approved_at,
        dr.posted_at,
        run_user.name AS run_by_name,
        approve_user.name AS approved_by_name
      FROM depreciation_runs dr
      LEFT JOIN users run_user ON run_user.id = dr.run_by_user_id
      LEFT JOIN users approve_user ON approve_user.id = dr.approved_by_user_id
      ORDER BY dr.period DESC
      LIMIT 12
    `
  );
  return result.rows;
}

async function findDepreciationRunById(databaseUrl, runId) {
  const result = await query(
    databaseUrl,
    `
      SELECT *
      FROM depreciation_runs
      WHERE id = $1
      LIMIT 1
    `,
    [runId]
  );
  return result.rows[0] || null;
}

async function listDepreciationLines(databaseUrl, runId) {
  const result = await query(
    databaseUrl,
    `
      SELECT *
      FROM depreciation_lines
      WHERE depreciation_run_id = $1
      ORDER BY asset_id ASC
    `,
    [runId]
  );
  return result.rows;
}

async function listDepreciationLinesWithAssetDetail(databaseUrl, runId) {
  const result = await query(
    databaseUrl,
    `
      SELECT
        dl.*,
        a.asset_id AS asset_public_id,
        a.tag_code,
        a.name AS asset_name,
        b.code AS branch_code,
        b.name AS branch_name
      FROM depreciation_lines dl
      INNER JOIN assets a ON a.id = dl.asset_id
      INNER JOIN branches b ON b.id = a.branch_id
      WHERE dl.depreciation_run_id = $1
      ORDER BY a.asset_id ASC
    `,
    [runId]
  );
  return result.rows;
}

async function getLatestExchangeRate(databaseUrl, currency = "USD") {
  const result = await query(
    databaseUrl,
    `
      SELECT *
      FROM exchange_rates
      WHERE currency = $1
      ORDER BY period DESC
      LIMIT 1
    `,
    [currency]
  );
  return result.rows[0] || null;
}

async function getAssetBalancesByGlCode(databaseUrl) {
  const result = await query(
    databaseUrl,
    `
      SELECT
        ga.code AS gl_code,
        ga.name AS label,
        COALESCE(SUM(CASE WHEN a.currency = 'USD' THEN a.net_book_value ELSE 0 END), 0)::numeric AS fams_usd,
        COALESCE(SUM(CASE WHEN a.currency = 'CDF' THEN a.net_book_value ELSE 0 END), 0)::numeric AS fams_cdf
      FROM assets a
      LEFT JOIN gl_accounts ga ON ga.id = a.gl_asset_account_id
      GROUP BY ga.code, ga.name
      ORDER BY ga.code ASC
    `
  );
  return result.rows;
}

async function getGlBalancesForPeriod(databaseUrl, period) {
  const result = await query(
    databaseUrl,
    `
      SELECT
        gl_code,
        label,
        COALESCE(SUM(CASE WHEN currency = 'USD' THEN balance ELSE 0 END), 0)::numeric AS gl_usd,
        COALESCE(SUM(CASE WHEN currency = 'CDF' THEN balance ELSE 0 END), 0)::numeric AS gl_cdf
      FROM gl_balances
      WHERE period = $1
      GROUP BY gl_code, label
      ORDER BY gl_code ASC
    `,
    [period]
  );
  return result.rows;
}

async function listDepreciableAssets(databaseUrl) {
  const result = await query(
    databaseUrl,
    `
      SELECT
        a.id,
        a.asset_id,
        a.currency,
        a.acquisition_cost,
        a.residual_value,
        a.net_book_value,
        a.useful_life_months,
        a.capitalisation_date,
        a.depreciation_method,
        a.status
      FROM assets a
      ORDER BY a.asset_id ASC
    `
  );
  return result.rows;
}

async function insertDepreciationRun(client, input) {
  const result = await client.query(
    `
      INSERT INTO depreciation_runs (
        period,
        status,
        exchange_rate_used,
        total_assets_processed,
        total_assets_skipped,
        total_depreciation_usd,
        total_depreciation_cdf,
        failure_count,
        run_by_user_id,
        summary
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      ON CONFLICT (period)
      DO UPDATE SET
        status = EXCLUDED.status,
        exchange_rate_used = EXCLUDED.exchange_rate_used,
        total_assets_processed = EXCLUDED.total_assets_processed,
        total_assets_skipped = EXCLUDED.total_assets_skipped,
        total_depreciation_usd = EXCLUDED.total_depreciation_usd,
        total_depreciation_cdf = EXCLUDED.total_depreciation_cdf,
        failure_count = EXCLUDED.failure_count,
        run_by_user_id = EXCLUDED.run_by_user_id,
        summary = EXCLUDED.summary,
        updated_at = NOW(),
        approved_by_user_id = NULL,
        approved_at = NULL,
        posted_at = NULL,
        gl_batch_reference = NULL
      RETURNING *
    `,
    [
      input.period,
      input.status,
      input.exchangeRateUsed,
      input.totalAssetsProcessed,
      input.totalAssetsSkipped,
      input.totalDepreciationUSD,
      input.totalDepreciationCDF,
      input.failureCount,
      input.runByUserId,
      input.summary
    ]
  );
  return result.rows[0];
}

async function replaceDepreciationLines(client, runId, lines) {
  await client.query(`DELETE FROM depreciation_lines WHERE depreciation_run_id = $1`, [runId]);
  for (const line of lines) {
    await client.query(
      `
        INSERT INTO depreciation_lines (
          depreciation_run_id,
          asset_id,
          opening_nbv,
          depreciation_charge,
          closing_nbv,
          currency,
          posting_status,
          failure_reason
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      `,
      [runId, line.assetId, line.openingNBV, line.depreciationCharge, line.closingNBV, line.currency, line.postingStatus, line.failureReason]
    );
  }
}

async function applyDepreciationToAssets(client, lines) {
  for (const line of lines.filter((item) => item.depreciationCharge > 0 && item.postingStatus !== "FAILED")) {
    await client.query(
      `
        UPDATE assets
        SET
          accumulated_depreciation = accumulated_depreciation + $2,
          net_book_value = $3,
          updated_at = NOW()
        WHERE id = $1
      `,
      [line.assetId, line.depreciationCharge, line.closingNBV]
    );
  }
}

async function approveDepreciationRun(client, runId, userId, batchReference, summary) {
  const result = await client.query(
    `
      UPDATE depreciation_runs
      SET
        status = 'POSTED',
        approved_by_user_id = $2,
        approved_at = NOW(),
        posted_at = NOW(),
        gl_batch_reference = $3,
        summary = $4,
        updated_at = NOW()
      WHERE id = $1
      RETURNING *
    `,
    [runId, userId, batchReference, summary]
  );
  return result.rows[0];
}

async function updateDepreciationRunStatus(client, runId, status, summary, userId = null) {
  const result = await client.query(
    `
      UPDATE depreciation_runs
      SET
        status = $2,
        summary = $3,
        approved_by_user_id = CASE WHEN $2 = 'REJECTED' THEN $4 ELSE approved_by_user_id END,
        updated_at = NOW()
      WHERE id = $1
      RETURNING *
    `,
    [runId, status, summary, userId]
  );
  return result.rows[0];
}

async function markDepreciationLinesPosted(client, runId, assetIds, postingReference) {
  if (!assetIds.length) return [];
  const result = await client.query(
    `
      UPDATE depreciation_lines
      SET
        posting_status = 'POSTED',
        posting_reference = $3,
        posted_at = NOW(),
        failure_reason = NULL,
        updated_at = NOW()
      WHERE depreciation_run_id = $1
        AND asset_id = ANY($2::uuid[])
        AND posting_status = 'PENDING'
      RETURNING *
    `,
    [runId, assetIds, postingReference]
  );
  return result.rows;
}

async function retryFailedDepreciationLines(client, runId, assetIds, postingReference) {
  if (!assetIds.length) return [];
  const result = await client.query(
    `
      UPDATE depreciation_lines
      SET
        posting_status = 'POSTED',
        posting_reference = $3,
        posted_at = NOW(),
        retry_count = retry_count + 1,
        failure_reason = NULL,
        updated_at = NOW()
      WHERE depreciation_run_id = $1
        AND asset_id = ANY($2::uuid[])
        AND posting_status = 'FAILED'
      RETURNING *
    `,
    [runId, assetIds, postingReference]
  );
  return result.rows;
}

async function updateDepreciationRunAfterRetry(client, runId, failureCount, summary) {
  const result = await client.query(
    `
      UPDATE depreciation_runs
      SET
        failure_count = $2,
        summary = $3,
        updated_at = NOW()
      WHERE id = $1
      RETURNING *
    `,
    [runId, failureCount, summary]
  );
  return result.rows[0];
}

async function getLatestReconciliation(databaseUrl) {
  const runResult = await query(
    databaseUrl,
    `
      SELECT *
      FROM reconciliation_runs
      ORDER BY created_at DESC
      LIMIT 1
    `
  );
  const run = runResult.rows[0] || null;
  if (!run) return null;

  const lineResult = await query(
    databaseUrl,
    `
      SELECT *
      FROM reconciliation_lines
      WHERE reconciliation_run_id = $1
      ORDER BY gl_code ASC
    `,
    [run.id]
  );

  return {
    run,
    lines: lineResult.rows
  };
}

async function createReconciliationRun(client, input) {
  const result = await client.query(
    `
      INSERT INTO reconciliation_runs (
        period,
        status,
        fams_balance_usd,
        fams_balance_cdf,
        gl_balance_usd,
        gl_balance_cdf,
        variance_usd,
        variance_cdf,
        discrepancy_count,
        run_by_user_id
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      RETURNING *
    `,
    [
      input.period,
      input.status,
      input.famsBalanceUSD,
      input.famsBalanceCDF,
      input.glBalanceUSD,
      input.glBalanceCDF,
      input.varianceUSD,
      input.varianceCDF,
      input.discrepancyCount,
      input.runByUserId
    ]
  );
  return result.rows[0];
}

async function insertReconciliationLines(client, runId, lines) {
  for (const line of lines) {
    await client.query(
      `
        INSERT INTO reconciliation_lines (
          reconciliation_run_id,
          gl_code,
          label,
          fams_usd,
          gl_usd,
          variance_usd,
          fams_cdf,
          gl_cdf,
          variance_cdf,
          status
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      `,
      [runId, line.glCode, line.label, line.famsUSD, line.glUSD, line.varianceUSD, line.famsCDF, line.glCDF, line.varianceCDF, line.status]
    );
  }
}

module.exports = {
  listDepreciationRuns,
  findDepreciationRunById,
  listDepreciationLines,
  listDepreciationLinesWithAssetDetail,
  getLatestExchangeRate,
  getAssetBalancesByGlCode,
  getGlBalancesForPeriod,
  listDepreciableAssets,
  insertDepreciationRun,
  replaceDepreciationLines,
  applyDepreciationToAssets,
  approveDepreciationRun,
  updateDepreciationRunStatus,
  markDepreciationLinesPosted,
  retryFailedDepreciationLines,
  updateDepreciationRunAfterRetry,
  getLatestReconciliation,
  createReconciliationRun,
  insertReconciliationLines
};
