const { getEnv } = require("../src/config/env");
const { withTransaction, closePool } = require("../src/config/db");
const { branches, categoryProfiles, demoUsers } = require("../src/data/referenceData");
const { buildBaseData } = require("../src/data/generator");
const { hashPassword } = require("../src/security/password");

function monthLabel(date) {
  return date.toISOString().slice(0, 7);
}

async function truncateAll(client) {
  await client.query(`
    TRUNCATE TABLE
      audit_logs,
      approval_requests,
      job_queue,
      parallel_run_sessions,
      verification_events,
      import_batch_rows,
      import_batches,
      reconciliation_lines,
      reconciliation_runs,
      depreciation_lines,
      depreciation_runs,
      workflow_requests,
      asset_attachments,
      assets,
      user_branch_access,
      users,
      exchange_rates,
      gl_balances,
      gl_accounts,
      asset_category_policies,
      branches
    RESTART IDENTITY CASCADE
  `);
}

async function seedBranches(client) {
  const map = new Map();
  for (const branch of branches) {
    const result = await client.query(
      `
        INSERT INTO branches (code, name, city, province, is_head_office)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, code, name
      `,
      [branch.code, branch.name, branch.city, branch.province, Boolean(branch.isHeadOffice)]
    );
    map.set(branch.code, result.rows[0]);
  }
  return map;
}

async function seedCategoryPolicies(client) {
  for (const category of categoryProfiles) {
    await client.query(
      `
        INSERT INTO asset_category_policies (
          category_key,
          label,
          default_depreciation_method,
          default_useful_life_months,
          residual_rate
        )
        VALUES ($1, $2, $3, $4, $5)
      `,
      [category.key, category.label, category.method, category.usefulLifeMonths, category.residualRate]
    );
  }
}

async function seedGlAccounts(client) {
  const map = new Map();
  for (const category of categoryProfiles) {
    const prefix = category.key.slice(0, 3);
    const accountRows = [
      { code: `15-${100 + map.size}-${prefix}`, name: `${category.label} Asset`, type: "asset" },
      { code: `61-${200 + map.size}-${prefix}`, name: `${category.label} Depreciation Expense`, type: "depreciation_expense" },
      { code: `18-${300 + map.size}-${prefix}`, name: `${category.label} Accumulated Depreciation`, type: "accumulated_depreciation" }
    ];

    const categoryAccounts = {};
    for (const account of accountRows) {
      const result = await client.query(
        `
          INSERT INTO gl_accounts (code, name, account_type, category_key)
          VALUES ($1, $2, $3, $4)
          RETURNING id, code
        `,
        [account.code, account.name, account.type, category.key]
      );
      categoryAccounts[account.type] = result.rows[0];
    }
    map.set(category.key, categoryAccounts);
  }
  return map;
}

async function seedUsers(client, branchMap) {
  const map = new Map();
  for (const user of demoUsers) {
    const branch = branchMap.get(user.branchCode);
    const passwordHash = hashPassword(user.password);
    const result = await client.query(
      `
        INSERT INTO users (email, password_hash, name, role, home_branch_id)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, email, name, role, home_branch_id
      `,
      [user.email, passwordHash, user.name, user.role, branch ? branch.id : null]
    );
    map.set(user.email, result.rows[0]);

    if (branch) {
      await client.query(
        `
          INSERT INTO user_branch_access (user_id, branch_id, access_type)
          VALUES ($1, $2, 'primary')
        `,
        [result.rows[0].id, branch.id]
      );
    }
  }
  return map;
}

async function seedExchangeRates(client) {
  for (let offset = 0; offset < 12; offset += 1) {
    const date = new Date(Date.UTC(2025, 3 + offset, 1));
    const period = monthLabel(date);
    await client.query(
      `
        INSERT INTO exchange_rates (period, currency, rate_to_cdf, effective_date)
        VALUES ($1, 'USD', $2, $3)
      `,
      [period, 2800 + offset * 11, date.toISOString().slice(0, 10)]
    );
  }
}

async function seedAssets(client, data, branchMap, userMap, glAccountMap) {
  const assetMap = new Map();
  const userNameMap = new Map(Array.from(userMap.values()).map((user) => [user.name, user.id]));

  for (const asset of data.assets) {
    const branchId = branchMap.get(asset.branchCode).id;
    const accounts = glAccountMap.get(asset.categoryKey);
    const createdByUserId = userNameMap.get(asset.createdBy) || userMap.get("finance@bankdrc.cd").id;
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
          last_verified_at,
          created_by_user_id,
          updated_by_user_id
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
          $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
          $21, $22, $23, $24
        )
        RETURNING id, asset_id, branch_id
      `,
      [
        asset.assetId,
        asset.tagCode,
        asset.name,
        asset.description,
        asset.categoryKey,
        branchId,
        asset.currency,
        asset.acquisitionCost,
        asset.residualValue,
        asset.capitalisationDate,
        asset.usefulLifeMonths,
        asset.depreciationMethod,
        asset.accumulatedDepreciation,
        0,
        asset.netBookValue,
        asset.status,
        accounts.asset.id,
        accounts.depreciation_expense.id,
        accounts.accumulated_depreciation.id,
        asset.purchaseOrderRef,
        asset.warrantyExpiryDate,
        asset.lastVerifiedAt,
        createdByUserId,
        createdByUserId
      ]
    );
    assetMap.set(asset.assetId, {
      id: result.rows[0].id,
      assetId: asset.assetId,
      branchId: result.rows[0].branch_id,
      branchCode: asset.branchCode,
      toBranchCode: asset.branchCode,
      categoryKey: asset.categoryKey
    });
  }

  return assetMap;
}

async function seedWorkflows(client, data, assetMap, branchMap, userMap) {
  const financeUser = userMap.get("finance@bankdrc.cd");

  for (const card of data.lifecycleCards) {
    const assetRef = assetMap.get(card.asset.assetId);
    const workflowType =
      card.column === "pendingDisposals"
        ? "DISPOSAL"
        : card.column === "pendingImpairments"
          ? "IMPAIRMENT"
          : "TRANSFER";
    const status =
      card.column === "pendingDisposals"
        ? "PENDING_DISPOSALS"
        : card.column === "pendingImpairments"
          ? "PENDING_IMPAIRMENTS"
          : card.column === "inTransit"
            ? "IN_TRANSIT"
            : "PENDING_TRANSFERS";
    const targetBranchCode = workflowType === "TRANSFER" ? (card.asset.branchCode === "BR-GOM" ? "BR-KIS" : "BR-GOM") : null;
    await client.query(
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
      `,
      [
        workflowType,
        status,
        assetRef.id,
        assetRef.branchId,
        targetBranchCode ? branchMap.get(targetBranchCode).id : null,
        financeUser.id,
        card.note,
        JSON.stringify({ title: card.title, owner: card.owner })
      ]
    );
  }
}

async function seedDepreciationRuns(client, data, userMap) {
  const financeUser = userMap.get("finance@bankdrc.cd");
  const itUser = userMap.get("it@bankdrc.cd");
  for (const run of data.depreciationRuns) {
    await client.query(
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
          approved_by_user_id,
          approved_at,
          posted_at,
          summary,
          gl_batch_reference
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
          $11, $12, $13, $14
        )
      `,
      [
        run.period,
        run.status,
        run.exchangeRateUsed,
        run.totalAssetsProcessed,
        run.totalAssetsSkipped,
        run.totalDepreciationUSD,
        run.totalDepreciationCDF,
        run.failureCount,
        run.status === "PENDING_APPROVAL" ? itUser.id : financeUser.id,
        run.approvedBy ? financeUser.id : null,
        run.approvedAt,
        run.postedAt,
        run.summary,
        run.glBatchReference
      ]
    );
  }
}

async function seedApprovalRequests(client, userMap) {
  const itUser = userMap.get("it@bankdrc.cd");
  const operationsUser = userMap.get("operations@bankdrc.cd");

  const latestRun = await client.query(
    `
      SELECT id, period
      FROM depreciation_runs
      WHERE status = 'PENDING_APPROVAL'
      ORDER BY period DESC
      LIMIT 1
    `
  );

  if (latestRun.rows[0]) {
    await client.query(
      `
        INSERT INTO approval_requests (
          entity_type,
          entity_id,
          action_type,
          title,
          detail,
          branch_id,
          requested_by_user_id,
          status,
          payload
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING', $8::jsonb)
      `,
      [
        "depreciation_run",
        latestRun.rows[0].id,
        "DEPRECIATION_POST",
        "Post monthly depreciation batch",
        `Posting approval requested for depreciation period ${latestRun.rows[0].period}.`,
        itUser.home_branch_id,
        itUser.id,
        JSON.stringify({ period: latestRun.rows[0].period, seeded: true })
      ]
    );
  }

  const workflow = await client.query(
    `
      SELECT id, asset_branch_id, workflow_type, status
      FROM workflow_requests
      WHERE status IN ('PENDING_TRANSFERS', 'PENDING_DISPOSALS', 'PENDING_IMPAIRMENTS')
      ORDER BY created_at ASC
      LIMIT 1
    `
  );

  if (workflow.rows[0]) {
    const actionType =
      workflow.rows[0].workflow_type === "TRANSFER"
        ? "TRANSFER_APPROVAL"
        : workflow.rows[0].workflow_type === "DISPOSAL"
          ? "DISPOSAL_APPROVAL"
          : "IMPAIRMENT_APPROVAL";

    await client.query(
      `
        INSERT INTO approval_requests (
          entity_type,
          entity_id,
          action_type,
          title,
          detail,
          branch_id,
          requested_by_user_id,
          status,
          payload
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING', $8::jsonb)
      `,
      [
        "workflow_request",
        workflow.rows[0].id,
        actionType,
        "Approve workflow movement",
        `Checker approval requested for workflow ${workflow.rows[0].workflow_type}.`,
        workflow.rows[0].asset_branch_id,
        operationsUser.id,
        JSON.stringify({ workflowType: workflow.rows[0].workflow_type, currentStatus: workflow.rows[0].status, seeded: true })
      ]
    );
  }
}

async function seedReconciliation(client, data, assetMap) {
  const latestPeriod = data.reconciliation.period;
  const grouped = new Map();

  for (const asset of data.assets) {
    const key = asset.glAssetAccount;
    if (!grouped.has(key)) {
      grouped.set(key, {
        glCode: key,
        label: `${asset.category} Assets`,
        famsUSD: 0,
        glUSD: 0,
        famsCDF: 0,
        glCDF: 0
      });
    }
    const bucket = grouped.get(key);
    if (asset.currency === "USD") {
      bucket.famsUSD += asset.netBookValue;
    } else {
      bucket.famsCDF += asset.netBookValue;
    }
  }

  const balanceRows = Array.from(grouped.values()).slice(0, 4).map((row, index) => ({
    ...row,
    glUSD: row.famsUSD - (index === 1 ? 1240 : 0),
    glCDF: row.famsCDF - (index === 2 ? 4200000 : 0)
  }));

  for (const row of balanceRows) {
    if (row.famsUSD > 0) {
      await client.query(
        `
          INSERT INTO gl_balances (period, gl_code, label, currency, balance)
          VALUES ($1, $2, $3, 'USD', $4)
        `,
        [latestPeriod, row.glCode, row.label, row.glUSD]
      );
    }
    if (row.famsCDF > 0) {
      await client.query(
        `
          INSERT INTO gl_balances (period, gl_code, label, currency, balance)
          VALUES ($1, $2, $3, 'CDF', $4)
        `,
        [latestPeriod, row.glCode, row.label, row.glCDF]
      );
    }
  }

  const run = await client.query(
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
        discrepancy_count
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      RETURNING id
    `,
    [
      data.reconciliation.period,
      data.reconciliation.status,
      data.reconciliation.famsBalanceUSD,
      data.reconciliation.famsBalanceCDF,
      data.reconciliation.glBalanceUSD,
      data.reconciliation.glBalanceCDF,
      data.reconciliation.varianceUSD,
      data.reconciliation.varianceCDF,
      data.reconciliation.discrepancyCount
    ]
  );

  for (const row of balanceRows) {
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
      [
        run.rows[0].id,
        row.glCode,
        row.label,
        row.famsUSD,
        row.glUSD,
        row.famsUSD - row.glUSD,
        row.famsCDF,
        row.glCDF,
        row.famsCDF - row.glCDF,
        row.famsUSD === row.glUSD && row.famsCDF === row.glCDF ? "MATCHED" : "EXCEPTION"
      ]
    );
  }
}

async function seedAuditLogs(client, data, branchMap, userMap) {
  for (const log of data.auditLogs) {
    const actor = Array.from(userMap.values()).find((item) => item.name === log.user) || null;
    const branch = Array.from(branchMap.values()).find((item) => item.name === log.branchName) || null;
    await client.query(
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
        VALUES ($1, $2, $3, $4, $5, 'asset', $6, $7, $8::jsonb)
      `,
      [actor ? actor.id : null, log.user, log.role, branch ? branch.id : null, log.action, log.entity, log.detail, JSON.stringify({ seeded: true })]
    );
  }
}

async function seedParallelRun(client, userMap, data) {
  const latestRun = data.depreciationRuns[data.depreciationRuns.length - 1];
  await client.query(
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
    `,
    [
      latestRun.period,
      "Legacy Excel Register",
      JSON.stringify({ totalDepreciationUSD: latestRun.totalDepreciationUSD + 950, totalDepreciationCDF: latestRun.totalDepreciationCDF + 1750000, totalAssetsProcessed: latestRun.totalAssetsProcessed }),
      JSON.stringify({ totalDepreciationUSD: latestRun.totalDepreciationUSD, totalDepreciationCDF: latestRun.totalDepreciationCDF, totalAssetsProcessed: latestRun.totalAssetsProcessed }),
      JSON.stringify({ varianceUSD: 950, varianceCDF: 1750000, varianceAssets: 0 }),
      "EXCEPTION",
      userMap.get("finance@bankdrc.cd").id
    ]
  );
}

async function seedJobQueue(client) {
  await client.query(
    `
      INSERT INTO job_queue (job_type, payload, status, run_after)
      VALUES
        ('monthly-depreciation', '{"period":"2026-04"}'::jsonb, 'PENDING', NOW()),
        ('daily-reconciliation', '{"period":"2026-04"}'::jsonb, 'PENDING', NOW())
    `
  );
}

async function seedAttachmentsAndImports(client, userMap, assetMap) {
  const financeUser = userMap.get("finance@bankdrc.cd");
  const opsUser = userMap.get("operations@bankdrc.cd");
  const sampleAssets = Array.from(assetMap.values()).slice(0, 3);

  for (const asset of sampleAssets) {
    await client.query(
      `
        INSERT INTO asset_attachments (
          asset_id,
          attachment_type,
          file_name,
          reference_url,
          note,
          uploaded_by_user_id
        )
        VALUES
          ($1, 'INVOICE', $2, $3, $4, $5),
          ($1, 'WARRANTY', $6, $7, $8, $5)
      `,
      [
        asset.id,
        `Invoice-${asset.assetId}.pdf`,
        `https://docs.bankdrc.cd/fams/invoices/${asset.assetId}.pdf`,
        "Seeded invoice support pack.",
        financeUser.id,
        `Warranty-${asset.assetId}.pdf`,
        `https://docs.bankdrc.cd/fams/warranty/${asset.assetId}.pdf`,
        "Seeded warranty support pack."
      ]
    );
  }

  const batch = await client.query(
    `
      INSERT INTO import_batches (source_type, status, summary, created_by_user_id, imported_at)
      VALUES ('CSV', 'IMPORTED', '{"totalRows":3,"validRows":3,"errorRows":0,"importedRows":3}'::jsonb, $1, NOW())
      RETURNING id
    `,
    [opsUser.id]
  );

  const importRows = [
    { rowNumber: 2, assetId: "PESA-FA-900001", tagCode: "TAG-900001" },
    { rowNumber: 3, assetId: "PESA-FA-900002", tagCode: "TAG-900002" },
    { rowNumber: 4, assetId: "PESA-FA-900003", tagCode: "TAG-900003" }
  ];

  for (const row of importRows) {
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
        VALUES ($1, $2, $3, $4, 'IMPORTED', 'Imported successfully.', $5::jsonb)
      `,
      [batch.rows[0].id, row.rowNumber, row.assetId, row.tagCode, JSON.stringify({ assetId: row.assetId, tagCode: row.tagCode })]
    );
  }
}

async function main() {
  const env = getEnv();
  if (!env.databaseUrl) {
    throw new Error("DATABASE_URL is required to seed the database.");
  }

  const data = buildBaseData();

  await withTransaction(env.databaseUrl, async (client) => {
    await truncateAll(client);
    const branchMap = await seedBranches(client);
    await seedCategoryPolicies(client);
    const glAccountMap = await seedGlAccounts(client);
    const userMap = await seedUsers(client, branchMap);
    await seedExchangeRates(client);
    const assetMap = await seedAssets(client, data, branchMap, userMap, glAccountMap);
    await seedWorkflows(client, data, assetMap, branchMap, userMap);
    await seedDepreciationRuns(client, data, userMap);
    await seedApprovalRequests(client, userMap);
    await seedReconciliation(client, data, assetMap);
    await seedAuditLogs(client, data, branchMap, userMap);
    await seedParallelRun(client, userMap, data);
    await seedJobQueue(client);
    await seedAttachmentsAndImports(client, userMap, assetMap);
  });

  console.log("Seed complete.");
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });
