const { query } = require("../config/db");

async function listApprovalRequests(databaseUrl, filters = {}, branchIds = []) {
  const clauses = [];
  const params = [];
  let index = 1;

  if (filters.status) {
    clauses.push(`ar.status = $${index}`);
    params.push(filters.status);
    index += 1;
  }

  if (filters.entityType) {
    clauses.push(`ar.entity_type = $${index}`);
    params.push(filters.entityType);
    index += 1;
  }

  if (branchIds.length > 0) {
    clauses.push(`ar.branch_id = ANY($${index}::uuid[])`);
    params.push(branchIds);
    index += 1;
  }

  const where = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
  const result = await query(
    databaseUrl,
    `
      SELECT
        ar.*,
        branch.code AS branch_code,
        branch.name AS branch_name,
        requester.name AS requested_by_name,
        requester.role AS requested_by_role,
        approver.name AS approved_by_name,
        rejecter.name AS rejected_by_name
      FROM approval_requests ar
      LEFT JOIN branches branch ON branch.id = ar.branch_id
      LEFT JOIN users requester ON requester.id = ar.requested_by_user_id
      LEFT JOIN users approver ON approver.id = ar.approved_by_user_id
      LEFT JOIN users rejecter ON rejecter.id = ar.rejected_by_user_id
      ${where}
      ORDER BY
        CASE ar.status
          WHEN 'PENDING' THEN 1
          WHEN 'REJECTED' THEN 2
          WHEN 'APPROVED' THEN 3
          ELSE 4
        END,
        ar.requested_at DESC
      LIMIT 80
    `,
    params
  );
  return result.rows;
}

async function findApprovalRequestById(databaseUrl, approvalId) {
  const result = await query(
    databaseUrl,
    `
      SELECT
        ar.*,
        branch.code AS branch_code,
        branch.name AS branch_name,
        requester.name AS requested_by_name,
        requester.role AS requested_by_role,
        approver.name AS approved_by_name,
        rejecter.name AS rejected_by_name
      FROM approval_requests ar
      LEFT JOIN branches branch ON branch.id = ar.branch_id
      LEFT JOIN users requester ON requester.id = ar.requested_by_user_id
      LEFT JOIN users approver ON approver.id = ar.approved_by_user_id
      LEFT JOIN users rejecter ON rejecter.id = ar.rejected_by_user_id
      WHERE ar.id = $1
      LIMIT 1
    `,
    [approvalId]
  );
  return result.rows[0] || null;
}

async function findPendingApprovalForEntity(databaseUrl, entityType, entityId, actionType = null) {
  const result = await query(
    databaseUrl,
    `
      SELECT *
      FROM approval_requests
      WHERE entity_type = $1
        AND entity_id = $2
        AND status = 'PENDING'
        AND ($3::text IS NULL OR action_type = $3)
      ORDER BY requested_at DESC
      LIMIT 1
    `,
    [entityType, entityId, actionType]
  );
  return result.rows[0] || null;
}

async function createApprovalRequest(client, input) {
  const result = await client.query(
    `
      INSERT INTO approval_requests (
        entity_type,
        entity_id,
        action_type,
        title,
        detail,
        branch_id,
        requested_by_user_id,
        payload
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
      RETURNING *
    `,
    [
      input.entityType,
      input.entityId,
      input.actionType,
      input.title,
      input.detail,
      input.branchId || null,
      input.requestedByUserId,
      JSON.stringify(input.payload || {})
    ]
  );
  return result.rows[0];
}

async function cancelPendingApprovals(client, entityType, entityId, actionType = null) {
  await client.query(
    `
      UPDATE approval_requests
      SET
        status = 'CANCELLED',
        decision_notes = COALESCE(decision_notes, 'Superseded by a newer submission.'),
        rejected_at = NOW()
      WHERE entity_type = $1
        AND entity_id = $2
        AND status = 'PENDING'
        AND ($3::text IS NULL OR action_type = $3)
    `,
    [entityType, entityId, actionType]
  );
}

async function approveApprovalRequest(client, approvalId, userId, notes = "") {
  const result = await client.query(
    `
      UPDATE approval_requests
      SET
        status = 'APPROVED',
        approved_by_user_id = $2,
        approved_at = NOW(),
        decision_notes = $3
      WHERE id = $1
      RETURNING *
    `,
    [approvalId, userId, notes]
  );
  return result.rows[0];
}

async function rejectApprovalRequest(client, approvalId, userId, notes = "") {
  const result = await client.query(
    `
      UPDATE approval_requests
      SET
        status = 'REJECTED',
        rejected_by_user_id = $2,
        rejected_at = NOW(),
        decision_notes = $3
      WHERE id = $1
      RETURNING *
    `,
    [approvalId, userId, notes]
  );
  return result.rows[0];
}

module.exports = {
  listApprovalRequests,
  findApprovalRequestById,
  findPendingApprovalForEntity,
  createApprovalRequest,
  cancelPendingApprovals,
  approveApprovalRequest,
  rejectApprovalRequest
};
