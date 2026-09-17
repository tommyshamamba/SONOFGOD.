const { query } = require("../config/db");

async function findUserByEmail(databaseUrl, email) {
  const result = await query(
    databaseUrl,
    `
      SELECT u.*, b.code AS branch_code, b.name AS branch_name
      FROM users u
      LEFT JOIN branches b ON b.id = u.home_branch_id
      WHERE lower(u.email) = lower($1)
      LIMIT 1
    `,
    [email]
  );
  return result.rows[0] || null;
}

async function findUserById(databaseUrl, userId) {
  const result = await query(
    databaseUrl,
    `
      SELECT u.*, b.code AS branch_code, b.name AS branch_name
      FROM users u
      LEFT JOIN branches b ON b.id = u.home_branch_id
      WHERE u.id = $1
      LIMIT 1
    `,
    [userId]
  );
  return result.rows[0] || null;
}

async function listUserBranchAccess(databaseUrl, userId) {
  const result = await query(
    databaseUrl,
    `
      SELECT uba.branch_id, b.code, b.name, b.city, b.province, uba.access_type
      FROM user_branch_access uba
      INNER JOIN branches b ON b.id = uba.branch_id
      WHERE uba.user_id = $1
      ORDER BY b.name ASC
    `,
    [userId]
  );
  return result.rows;
}

async function listBranches(databaseUrl) {
  const result = await query(
    databaseUrl,
    `
      SELECT id, code, name, city, province, is_head_office, is_active
      FROM branches
      WHERE is_active = TRUE
      ORDER BY is_head_office DESC, name ASC
    `
  );
  return result.rows;
}

async function findBranchByCode(databaseUrl, code) {
  const result = await query(
    databaseUrl,
    `
      SELECT id, code, name, city, province, is_head_office, is_active
      FROM branches
      WHERE code = $1
      LIMIT 1
    `,
    [code]
  );
  return result.rows[0] || null;
}

async function listUsers(databaseUrl) {
  const result = await query(
    databaseUrl,
    `
      SELECT
        u.id,
        u.email,
        u.name,
        u.role,
        u.is_active,
        u.home_branch_id,
        u.last_login_at,
        u.password_changed_at,
        u.phone_number,
        b.code AS branch_code,
        b.name AS branch_name,
        COALESCE(
          json_agg(
            json_build_object(
              'branchId', uba.branch_id,
              'code', branch.code,
              'name', branch.name,
              'accessType', uba.access_type
            )
            ORDER BY branch.name
          ) FILTER (WHERE uba.id IS NOT NULL),
          '[]'::json
        ) AS branch_access
      FROM users u
      LEFT JOIN branches b ON b.id = u.home_branch_id
      LEFT JOIN user_branch_access uba ON uba.user_id = u.id
      LEFT JOIN branches branch ON branch.id = uba.branch_id
      GROUP BY u.id, b.code, b.name
      ORDER BY u.name ASC
    `
  );
  return result.rows;
}

async function createUser(client, input) {
  const result = await client.query(
    `
      INSERT INTO users (
        email,
        password_hash,
        name,
        role,
        home_branch_id,
        is_active,
        phone_number,
        password_changed_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
      RETURNING *
    `,
    [
      input.email,
      input.passwordHash,
      input.name,
      input.role,
      input.homeBranchId || null,
      input.isActive ?? true,
      input.phoneNumber || null
    ]
  );
  return result.rows[0];
}

async function updateUser(client, userId, input) {
  const result = await client.query(
    `
      UPDATE users
      SET
        name = $2,
        role = $3,
        home_branch_id = $4,
        is_active = $5,
        phone_number = $6,
        updated_at = NOW()
      WHERE id = $1
      RETURNING *
    `,
    [
      userId,
      input.name,
      input.role,
      input.homeBranchId || null,
      input.isActive,
      input.phoneNumber || null
    ]
  );
  return result.rows[0];
}

async function replaceUserBranchAccess(client, userId, branchIds, homeBranchId = null) {
  await client.query(`DELETE FROM user_branch_access WHERE user_id = $1`, [userId]);

  const accessIds = Array.from(new Set([...(branchIds || []), homeBranchId].filter(Boolean)));
  for (const branchId of accessIds) {
    await client.query(
      `
        INSERT INTO user_branch_access (user_id, branch_id, access_type)
        VALUES ($1, $2, $3)
      `,
      [userId, branchId, branchId === homeBranchId ? "primary" : "secondary"]
    );
  }
}

async function updatePassword(client, userId, passwordHash) {
  const result = await client.query(
    `
      UPDATE users
      SET
        password_hash = $2,
        password_changed_at = NOW(),
        updated_at = NOW()
      WHERE id = $1
      RETURNING id, email, name, role
    `,
    [userId, passwordHash]
  );
  return result.rows[0];
}

async function updateLastLogin(databaseUrl, userId) {
  await query(
    databaseUrl,
    `
      UPDATE users
      SET
        last_login_at = NOW(),
        updated_at = NOW()
      WHERE id = $1
    `,
    [userId]
  );
}

module.exports = {
  findUserByEmail,
  findUserById,
  listUserBranchAccess,
  listBranches,
  findBranchByCode,
  listUsers,
  createUser,
  updateUser,
  replaceUserBranchAccess,
  updatePassword,
  updateLastLogin
};
