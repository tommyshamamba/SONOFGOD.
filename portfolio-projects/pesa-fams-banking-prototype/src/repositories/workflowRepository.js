const { query } = require("../config/db");

async function listWorkflowRequests(databaseUrl, branchIds) {
  const filter = branchIds.length ? "WHERE w.asset_branch_id = ANY($1::uuid[])" : "";
  const result = await query(
    databaseUrl,
    `
      SELECT
        w.id,
        w.workflow_type,
        w.status,
        w.notes,
        w.created_at,
        w.to_branch_id,
        b_to.name AS to_branch_name,
        a.id AS asset_row_id,
        a.asset_id,
        a.tag_code,
        a.name AS asset_name,
        a.status AS asset_status,
        branch.code AS branch_code,
        branch.name AS branch_name
      FROM workflow_requests w
      INNER JOIN assets a ON a.id = w.asset_id
      INNER JOIN branches branch ON branch.id = a.branch_id
      LEFT JOIN branches b_to ON b_to.id = w.to_branch_id
      ${filter}
      ORDER BY w.created_at DESC
    `,
    branchIds.length ? [branchIds] : []
  );
  return result.rows;
}

async function findWorkflowById(databaseUrl, workflowId) {
  const result = await query(
    databaseUrl,
    `
      SELECT *
      FROM workflow_requests
      WHERE id = $1
      LIMIT 1
    `,
    [workflowId]
  );
  return result.rows[0] || null;
}

async function advanceWorkflow(client, workflowId, nextStatus, decidedByUserId) {
  const result = await client.query(
    `
      UPDATE workflow_requests
      SET
        status = $2,
        decided_by_user_id = $3,
        decided_at = NOW(),
        updated_at = NOW()
      WHERE id = $1
      RETURNING *
    `,
    [workflowId, nextStatus, decidedByUserId]
  );
  return result.rows[0];
}

async function createWorkflowRequest(client, input) {
  const result = await client.query(
    `
      INSERT INTO workflow_requests (
        workflow_type,
        status,
        asset_id,
        asset_branch_id,
        to_branch_id,
        requested_by_user_id,
        notes,
        payload
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
      RETURNING *
    `,
    [
      input.workflowType,
      input.status,
      input.assetId,
      input.assetBranchId,
      input.toBranchId || null,
      input.requestedByUserId || null,
      input.notes || null,
      JSON.stringify(input.payload || {})
    ]
  );
  return result.rows[0];
}

async function updateAssetBranch(client, assetId, branchId, status) {
  const result = await client.query(
    `
      UPDATE assets
      SET
        branch_id = $2,
        status = $3,
        updated_at = NOW()
      WHERE id = $1
      RETURNING *
    `,
    [assetId, branchId, status]
  );
  return result.rows[0];
}

async function updateAssetStatus(client, assetId, status) {
  const result = await client.query(
    `
      UPDATE assets
      SET
        status = $2,
        updated_at = NOW()
      WHERE id = $1
      RETURNING *
    `,
    [assetId, status]
  );
  return result.rows[0];
}

module.exports = {
  listWorkflowRequests,
  findWorkflowById,
  advanceWorkflow,
  createWorkflowRequest,
  updateAssetBranch,
  updateAssetStatus
};
