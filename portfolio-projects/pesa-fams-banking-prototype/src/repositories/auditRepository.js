const { query } = require("../config/db");

async function insertAuditLog(client, entry) {
  const result = await client.query(
    `
      INSERT INTO audit_logs (
        actor_user_id,
        actor_name,
        actor_role,
        branch_id,
        action,
        entity_type,
        entity_id,
        detail,
        metadata
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
      RETURNING *
    `,
    [
      entry.actorUserId || null,
      entry.actorName || null,
      entry.actorRole || null,
      entry.branchId || null,
      entry.action,
      entry.entityType,
      entry.entityId,
      entry.detail,
      JSON.stringify(entry.metadata || {})
    ]
  );
  return result.rows[0];
}

async function listAuditLogs(databaseUrl, filters, branchIds) {
  const clauses = [];
  const params = [];
  let index = 1;

  if (filters.action) {
    clauses.push(`action = $${index}`);
    params.push(filters.action);
    index += 1;
  }

  if (filters.user) {
    clauses.push(`lower(actor_name) LIKE $${index}`);
    params.push(`%${filters.user.toLowerCase()}%`);
    index += 1;
  }

  if (branchIds.length) {
    clauses.push(`branch_id = ANY($${index}::uuid[])`);
    params.push(branchIds);
    index += 1;
  }

  const where = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
  const result = await query(
    databaseUrl,
    `
      SELECT
        id,
        created_at AS timestamp,
        actor_name AS user,
        actor_role AS role,
        action,
        entity_id,
        detail,
        metadata
      FROM audit_logs
      ${where}
      ORDER BY created_at DESC
      LIMIT 80
    `,
    params
  );
  return result.rows;
}

async function listRecentAuditLogs(databaseUrl, branchIds, limit = 10) {
  const where = branchIds.length ? "WHERE branch_id = ANY($1::uuid[])" : "";
  const params = branchIds.length ? [branchIds, limit] : [limit];
  const limitIndex = branchIds.length ? 2 : 1;
  const result = await query(
    databaseUrl,
    `
      SELECT
        id,
        created_at AS timestamp,
        actor_name AS user,
        action,
        detail
      FROM audit_logs
      ${where}
      ORDER BY created_at DESC
      LIMIT $${limitIndex}
    `,
    params
  );
  return result.rows;
}

module.exports = {
  insertAuditLog,
  listAuditLogs,
  listRecentAuditLogs
};
