const { query } = require("../config/db");

function buildAssetFilters(filters, branchIds = [], startIndex = 1) {
  const clauses = [];
  const params = [];
  let index = startIndex;

  if (branchIds.length > 0) {
    clauses.push(`a.branch_id = ANY($${index}::uuid[])`);
    params.push(branchIds);
    index += 1;
  }

  if (filters.search) {
    clauses.push(`(
      lower(a.asset_id) LIKE $${index}
      OR lower(a.tag_code) LIKE $${index}
      OR lower(a.name) LIKE $${index}
      OR lower(b.name) LIKE $${index}
    )`);
    params.push(`%${filters.search.toLowerCase()}%`);
    index += 1;
  }

  if (filters.status) {
    clauses.push(`a.status = $${index}`);
    params.push(filters.status);
    index += 1;
  }

  if (filters.branchCode) {
    clauses.push(`b.code = $${index}`);
    params.push(filters.branchCode);
    index += 1;
  }

  if (filters.categoryKey) {
    clauses.push(`a.category_key = $${index}`);
    params.push(filters.categoryKey);
    index += 1;
  }

  return {
    where: clauses.length ? `WHERE ${clauses.join(" AND ")}` : "",
    params,
    nextIndex: index
  };
}

async function listAssets(databaseUrl, filters, branchIds) {
  const page = Math.max(Number(filters.page) || 1, 1);
  const pageSize = Math.min(Math.max(Number(filters.pageSize) || 12, 1), 100);
  const offset = (page - 1) * pageSize;
  const built = buildAssetFilters(filters, branchIds);

  const rows = await query(
    databaseUrl,
    `
      SELECT
        a.id,
        a.asset_id,
        a.tag_code,
        a.name,
        a.description,
        a.category_key,
        p.label AS category,
        b.id AS branch_id,
        b.code AS branch_code,
        b.name AS branch_name,
        b.city,
        b.province,
        a.currency,
        a.acquisition_cost,
        a.residual_value,
        a.capitalisation_date,
        a.useful_life_months,
        a.depreciation_method,
        a.accumulated_depreciation,
        a.impairment_loss,
        a.net_book_value,
        a.status,
        ga.code AS gl_asset_account,
        ge.code AS gl_depreciation_expense_account,
        gd.code AS gl_accumulated_depreciation_account,
        a.purchase_order_ref,
        a.warranty_expiry_date,
        a.last_verified_at,
        a.created_at,
        a.updated_at
      FROM assets a
      INNER JOIN branches b ON b.id = a.branch_id
      INNER JOIN asset_category_policies p ON p.category_key = a.category_key
      LEFT JOIN gl_accounts ga ON ga.id = a.gl_asset_account_id
      LEFT JOIN gl_accounts ge ON ge.id = a.gl_depreciation_expense_account_id
      LEFT JOIN gl_accounts gd ON gd.id = a.gl_accumulated_depreciation_account_id
      ${built.where}
      ORDER BY a.updated_at DESC, a.asset_id ASC
      LIMIT $${built.nextIndex}
      OFFSET $${built.nextIndex + 1}
    `,
    [...built.params, pageSize, offset]
  );

  const countResult = await query(
    databaseUrl,
    `
      SELECT COUNT(*)::int AS total
      FROM assets a
      INNER JOIN branches b ON b.id = a.branch_id
      ${built.where}
    `,
    built.params
  );

  return {
    items: rows.rows,
    total: countResult.rows[0].total,
    page,
    pageSize,
    totalPages: Math.max(Math.ceil(countResult.rows[0].total / pageSize), 1)
  };
}

async function getAssetByPublicId(databaseUrl, publicId, branchIds) {
  const branchFilter = branchIds.length ? "AND a.branch_id = ANY($2::uuid[])" : "";
  const params = branchIds.length ? [publicId, branchIds] : [publicId];
  const result = await query(
    databaseUrl,
    `
      SELECT
        a.*,
        p.label AS category,
        b.code AS branch_code,
        b.name AS branch_name,
        b.city,
        b.province,
        ga.code AS gl_asset_account,
        ge.code AS gl_depreciation_expense_account,
        gd.code AS gl_accumulated_depreciation_account
      FROM assets a
      INNER JOIN branches b ON b.id = a.branch_id
      INNER JOIN asset_category_policies p ON p.category_key = a.category_key
      LEFT JOIN gl_accounts ga ON ga.id = a.gl_asset_account_id
      LEFT JOIN gl_accounts ge ON ge.id = a.gl_depreciation_expense_account_id
      LEFT JOIN gl_accounts gd ON gd.id = a.gl_accumulated_depreciation_account_id
      WHERE (a.id::text = $1 OR a.asset_id = $1)
      ${branchFilter}
      LIMIT 1
    `,
    params
  );
  return result.rows[0] || null;
}

async function findAssetByTagCode(databaseUrl, tagCode, branchIds) {
  const branchFilter = branchIds.length ? "AND a.branch_id = ANY($2::uuid[])" : "";
  const params = branchIds.length ? [tagCode, branchIds] : [tagCode];
  const result = await query(
    databaseUrl,
    `
      SELECT
        a.*,
        p.label AS category,
        b.code AS branch_code,
        b.name AS branch_name,
        b.city,
        b.province,
        ga.code AS gl_asset_account,
        ge.code AS gl_depreciation_expense_account,
        gd.code AS gl_accumulated_depreciation_account
      FROM assets a
      INNER JOIN branches b ON b.id = a.branch_id
      INNER JOIN asset_category_policies p ON p.category_key = a.category_key
      LEFT JOIN gl_accounts ga ON ga.id = a.gl_asset_account_id
      LEFT JOIN gl_accounts ge ON ge.id = a.gl_depreciation_expense_account_id
      LEFT JOIN gl_accounts gd ON gd.id = a.gl_accumulated_depreciation_account_id
      WHERE lower(a.tag_code) = lower($1)
      ${branchFilter}
      LIMIT 1
    `,
    params
  );
  return result.rows[0] || null;
}

async function getDashboardMetrics(databaseUrl, branchIds) {
  const filter = branchIds.length ? "WHERE branch_id = ANY($1::uuid[])" : "";
  const params = branchIds.length ? [branchIds] : [];
  const result = await query(
    databaseUrl,
    `
      SELECT
        COUNT(*)::int AS total_assets,
        COUNT(DISTINCT branch_id)::int AS total_locations,
        COALESCE(SUM(CASE WHEN currency = 'USD' THEN net_book_value ELSE 0 END), 0)::numeric AS total_usd_nbv,
        COALESCE(SUM(CASE WHEN currency = 'CDF' THEN net_book_value ELSE 0 END), 0)::numeric AS total_cdf_nbv,
        COUNT(*) FILTER (WHERE status IN ('PENDING', 'TRANSFERRED', 'HELD_FOR_SALE', 'IMPAIRED'))::int AS pending_actions
      FROM assets
      ${filter}
    `,
    params
  );
  return result.rows[0];
}

async function listStatusBreakdown(databaseUrl, branchIds) {
  const filter = branchIds.length ? "WHERE branch_id = ANY($1::uuid[])" : "";
  const result = await query(
    databaseUrl,
    `
      SELECT status, COUNT(*)::int AS count
      FROM assets
      ${filter}
      GROUP BY status
      ORDER BY status ASC
    `,
    branchIds.length ? [branchIds] : []
  );
  return result.rows;
}

async function listBranchSummary(databaseUrl, branchIds) {
  const filter = branchIds.length ? "WHERE a.branch_id = ANY($1::uuid[])" : "";
  const result = await query(
    databaseUrl,
    `
      SELECT
        b.code AS branch_code,
        b.name AS branch_name,
        b.city,
        COUNT(a.id)::int AS asset_count,
        COUNT(a.id) FILTER (WHERE a.status = 'ACTIVE')::int AS active_count,
        COUNT(a.id) FILTER (WHERE a.status IN ('PENDING', 'TRANSFERRED', 'HELD_FOR_SALE'))::int AS pending_actions,
        COALESCE(
          ROUND(
            (
              COUNT(a.id) FILTER (WHERE a.last_verified_at >= CURRENT_DATE - INTERVAL '90 days')::numeric
              / NULLIF(COUNT(a.id), 0)
            ) * 100
          ),
          0
        )::int AS verification_coverage
      FROM assets a
      INNER JOIN branches b ON b.id = a.branch_id
      ${filter}
      GROUP BY b.code, b.name, b.city
      ORDER BY asset_count DESC, branch_name ASC
      LIMIT 10
    `,
    branchIds.length ? [branchIds] : []
  );
  return result.rows;
}

async function recordVerification(client, input) {
  await client.query(
    `
      INSERT INTO verification_events (
        asset_id,
        branch_id,
        verified_by_user_id,
        outcome,
        notes,
        verified_at,
        is_offline_synced,
        synced_at
      )
      VALUES ($1, $2, $3, $4, $5, NOW(), $6, NOW())
    `,
    [input.assetId, input.branchId, input.verifiedByUserId, input.outcome, input.notes || null, input.isOfflineSynced ?? true]
  );

  const updatedAsset = await client.query(
    `
      UPDATE assets
      SET
        last_verified_at = CURRENT_DATE,
        status = CASE WHEN $2 = 'NOT_FOUND' THEN 'IMPAIRED' ELSE status END,
        updated_at = NOW()
      WHERE id = $1
      RETURNING *
    `,
    [input.assetId, input.outcome]
  );

  return updatedAsset.rows[0];
}

async function findAssetsByAssetIdsOrTagCodes(databaseUrl, assetIds = [], tagCodes = []) {
  const assetIdList = Array.from(new Set(assetIds.filter(Boolean)));
  const tagCodeList = Array.from(new Set(tagCodes.filter(Boolean).map((value) => String(value).toLowerCase())));
  if (!assetIdList.length && !tagCodeList.length) return [];

  const result = await query(
    databaseUrl,
    `
      SELECT
        id,
        asset_id,
        tag_code,
        branch_id
      FROM assets
      WHERE
        (cardinality($1::text[]) > 0 AND asset_id = ANY($1::text[]))
        OR (cardinality($2::text[]) > 0 AND lower(tag_code) = ANY($2::text[]))
    `,
    [assetIdList, tagCodeList]
  );

  return result.rows;
}

async function getAssetRowById(client, assetId) {
  const result = await client.query(
    `
      SELECT
        a.id,
        a.asset_id,
        a.tag_code,
        a.name,
        a.description,
        a.category_key,
        p.label AS category,
        b.id AS branch_id,
        b.code AS branch_code,
        b.name AS branch_name,
        b.city,
        b.province,
        a.currency,
        a.acquisition_cost,
        a.residual_value,
        a.capitalisation_date,
        a.useful_life_months,
        a.depreciation_method,
        a.accumulated_depreciation,
        a.impairment_loss,
        a.net_book_value,
        a.status,
        ga.code AS gl_asset_account,
        ge.code AS gl_depreciation_expense_account,
        gd.code AS gl_accumulated_depreciation_account,
        a.purchase_order_ref,
        a.warranty_expiry_date,
        a.last_verified_at,
        a.created_at,
        a.updated_at
      FROM assets a
      INNER JOIN branches b ON b.id = a.branch_id
      INNER JOIN asset_category_policies p ON p.category_key = a.category_key
      LEFT JOIN gl_accounts ga ON ga.id = a.gl_asset_account_id
      LEFT JOIN gl_accounts ge ON ge.id = a.gl_depreciation_expense_account_id
      LEFT JOIN gl_accounts gd ON gd.id = a.gl_accumulated_depreciation_account_id
      WHERE a.id = $1
      LIMIT 1
    `,
    [assetId]
  );
  return result.rows[0] || null;
}

async function getDefaultAccountsForCategory(databaseUrl, categoryKey) {
  const result = await query(
    databaseUrl,
    `
      SELECT
        MAX(CASE WHEN account_type = 'asset' THEN id END) AS gl_asset_account_id,
        MAX(CASE WHEN account_type = 'depreciation_expense' THEN id END) AS gl_depreciation_expense_account_id,
        MAX(CASE WHEN account_type = 'accumulated_depreciation' THEN id END) AS gl_accumulated_depreciation_account_id
      FROM gl_accounts
      WHERE category_key = $1
        AND is_active = TRUE
    `,
    [categoryKey]
  );
  return result.rows[0] || null;
}

async function createAsset(client, input) {
  const result = await client.query(
    `
      INSERT INTO assets (
        asset_id,
        tag_code,
        name,
        description,
        category_key,
        branch_id,
        currency,
        acquisition_cost,
        residual_value,
        capitalisation_date,
        useful_life_months,
        depreciation_method,
        accumulated_depreciation,
        impairment_loss,
        net_book_value,
        status,
        gl_asset_account_id,
        gl_depreciation_expense_account_id,
        gl_accumulated_depreciation_account_id,
        purchase_order_ref,
        warranty_expiry_date,
        created_by_user_id,
        updated_by_user_id
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
        $21, $22, $23
      )
      RETURNING id
    `,
    [
      input.assetId,
      input.tagCode,
      input.name,
      input.description || null,
      input.categoryKey,
      input.branchId,
      input.currency,
      input.acquisitionCost,
      input.residualValue,
      input.capitalisationDate,
      input.usefulLifeMonths,
      input.depreciationMethod,
      input.accumulatedDepreciation ?? 0,
      input.impairmentLoss ?? 0,
      input.netBookValue,
      input.status,
      input.glAssetAccountId || null,
      input.glDepreciationExpenseAccountId || null,
      input.glAccumulatedDepreciationAccountId || null,
      input.purchaseOrderRef || null,
      input.warrantyExpiryDate || null,
      input.createdByUserId || null,
      input.updatedByUserId || null
    ]
  );

  return getAssetRowById(client, result.rows[0].id);
}

async function updateAsset(client, assetId, input) {
  await client.query(
    `
      UPDATE assets
      SET
        name = $2,
        description = $3,
        category_key = $4,
        branch_id = $5,
        currency = $6,
        acquisition_cost = $7,
        residual_value = $8,
        capitalisation_date = $9,
        useful_life_months = $10,
        depreciation_method = $11,
        net_book_value = $12,
        status = $13,
        gl_asset_account_id = $14,
        gl_depreciation_expense_account_id = $15,
        gl_accumulated_depreciation_account_id = $16,
        purchase_order_ref = $17,
        warranty_expiry_date = $18,
        updated_by_user_id = $19,
        updated_at = NOW()
      WHERE id = $1
    `,
    [
      assetId,
      input.name,
      input.description || null,
      input.categoryKey,
      input.branchId,
      input.currency,
      input.acquisitionCost,
      input.residualValue,
      input.capitalisationDate,
      input.usefulLifeMonths,
      input.depreciationMethod,
      input.netBookValue,
      input.status,
      input.glAssetAccountId || null,
      input.glDepreciationExpenseAccountId || null,
      input.glAccumulatedDepreciationAccountId || null,
      input.purchaseOrderRef || null,
      input.warrantyExpiryDate || null,
      input.updatedByUserId || null
    ]
  );

  return getAssetRowById(client, assetId);
}

async function listAssetAttachments(databaseUrl, assetId) {
  const result = await query(
    databaseUrl,
    `
      SELECT
        aa.*,
        u.name AS uploaded_by_name
      FROM asset_attachments aa
      LEFT JOIN users u ON u.id = aa.uploaded_by_user_id
      WHERE aa.asset_id = $1
      ORDER BY aa.created_at DESC
    `,
    [assetId]
  );
  return result.rows;
}

async function insertAssetAttachment(client, input) {
  const result = await client.query(
    `
      INSERT INTO asset_attachments (
        asset_id,
        attachment_type,
        file_name,
        reference_url,
        note,
        uploaded_by_user_id
      )
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING *
    `,
    [
      input.assetId,
      input.attachmentType,
      input.fileName,
      input.referenceUrl || null,
      input.note || null,
      input.uploadedByUserId || null
    ]
  );
  return result.rows[0];
}

async function getDashboardAlerts(databaseUrl, branchIds) {
  const branchFilter = branchIds.length ? "WHERE branch_id = ANY($1::uuid[])" : "";
  const branchParams = branchIds.length ? [branchIds] : [];

  const [unverified, fullyDepreciated, pendingApprovals, failedPostingLines] = await Promise.all([
    query(
      databaseUrl,
      `
        SELECT COUNT(*)::int AS count
        FROM assets
        ${branchFilter ? `${branchFilter} AND` : "WHERE"}
          (last_verified_at IS NULL OR last_verified_at < CURRENT_DATE - INTERVAL '90 days')
      `,
      branchParams
    ),
    query(
      databaseUrl,
      `
        SELECT COUNT(*)::int AS count
        FROM assets
        ${branchFilter ? `${branchFilter} AND` : "WHERE"}
          depreciation_method <> 'NONE'
          AND net_book_value <= residual_value + 1
      `,
      branchParams
    ),
    query(
      databaseUrl,
      `
        SELECT COUNT(*)::int AS count
        FROM approval_requests
        WHERE status = 'PENDING'
          AND requested_at < NOW() - INTERVAL '48 hours'
      `
    ),
    query(
      databaseUrl,
      `
        SELECT COUNT(*)::int AS count
        FROM depreciation_lines dl
        INNER JOIN assets a ON a.id = dl.asset_id
        INNER JOIN depreciation_runs dr ON dr.id = dl.depreciation_run_id
        ${branchIds.length ? "WHERE a.branch_id = ANY($1::uuid[]) AND" : "WHERE"}
          dr.period = (SELECT period FROM depreciation_runs ORDER BY period DESC LIMIT 1)
          AND dl.posting_status = 'FAILED'
      `,
      branchParams
    )
  ]);

  return {
    overdueApprovals: pendingApprovals.rows[0]?.count || 0,
    failedPostingLines: failedPostingLines.rows[0]?.count || 0,
    fullyDepreciatedAssets: fullyDepreciated.rows[0]?.count || 0,
    unverifiedAssets: unverified.rows[0]?.count || 0
  };
}

async function listVerificationQueue(databaseUrl, branchIds, limit = 12) {
  const result = await query(
    databaseUrl,
    `
      SELECT
        a.id,
        a.asset_id,
        a.tag_code,
        a.name,
        a.status,
        a.last_verified_at,
        b.code AS branch_code,
        b.name AS branch_name,
        b.city
      FROM assets a
      INNER JOIN branches b ON b.id = a.branch_id
      ${branchIds.length ? "WHERE a.branch_id = ANY($1::uuid[]) AND" : "WHERE"}
        (a.last_verified_at IS NULL OR a.last_verified_at < CURRENT_DATE - INTERVAL '90 days')
      ORDER BY a.last_verified_at NULLS FIRST, a.asset_id ASC
      LIMIT $${branchIds.length ? 2 : 1}
    `,
    branchIds.length ? [branchIds, limit] : [limit]
  );
  return result.rows;
}

module.exports = {
  listAssets,
  getAssetByPublicId,
  findAssetByTagCode,
  findAssetsByAssetIdsOrTagCodes,
  getDefaultAccountsForCategory,
  createAsset,
  updateAsset,
  listAssetAttachments,
  insertAssetAttachment,
  getDashboardMetrics,
  getDashboardAlerts,
  listStatusBreakdown,
  listBranchSummary,
  listVerificationQueue,
  recordVerification
};
