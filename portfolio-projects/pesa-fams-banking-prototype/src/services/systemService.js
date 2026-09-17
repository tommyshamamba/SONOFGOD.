const jwt = require("jsonwebtoken");

const { withTransaction } = require("../config/db");
const { buildSchedule } = require("../data/generator");
const { buildReportPack, buildReportCsv, buildReportExcelXml, buildPrintableHtml } = require("../domain/reporting");
const { csvRowsToObjects } = require("../domain/importing");
const {
  approvalStatusTone,
  approvalEntityLabel,
  approvalActionLabel,
  canApproveOwnRequest,
  workflowApprovalAction,
  workflowNextStatus,
  approvalStatusLabel
} = require("../domain/approval");
const { calculateDepreciationLine } = require("../domain/depreciation");
const { permissionsMatrix, requirePermission, scopeForAssets } = require("../domain/permissions");
const { reportCatalog, statusPalette, categoryProfiles } = require("../data/referenceData");
const { hashPassword, verifyPassword } = require("../security/password");
const approvalRepository = require("../repositories/approvalRepository");
const userRepository = require("../repositories/userRepository");
const assetRepository = require("../repositories/assetRepository");
const workflowRepository = require("../repositories/workflowRepository");
const financeRepository = require("../repositories/financeRepository");
const auditRepository = require("../repositories/auditRepository");
const importRepository = require("../repositories/importRepository");
const jobRepository = require("../repositories/jobRepository");
const parallelRunRepository = require("../repositories/parallelRunRepository");

const attachmentTypeCatalog = [
  "INVOICE",
  "PURCHASE_ORDER",
  "WARRANTY",
  "TRANSFER_FORM",
  "DISPOSAL_MEMO",
  "IMPAIRMENT_EVIDENCE",
  "PHOTO",
  "OTHER"
];

const roleCatalog = [
  { value: "finance_admin", label: "Finance Administrator" },
  { value: "operations", label: "Operations User" },
  { value: "admin_user", label: "Admin User" },
  { value: "auditor", label: "Auditor" },
  { value: "it_admin", label: "IT Administrator" }
];

class AppError extends Error {
  constructor(statusCode, message) {
    super(message);
    this.statusCode = statusCode;
  }
}

function branchScopeRows(user, branchRows) {
  if (scopeForAssets(user) === "all") return [];
  return branchRows.map((row) => row.branch_id);
}

function toClientUser(user, branches) {
  return {
    id: user.id,
    email: user.email,
    name: user.name,
    role: user.role,
    roleLabel: user.role.replaceAll("_", " ").replace(/\b\w/g, (char) => char.toUpperCase()),
    title: user.role.replaceAll("_", " ").replace(/\b\w/g, (char) => char.toUpperCase()),
    branchCode: user.branch_code,
    branchName: user.branch_name || "All Branches",
    permissions: permissionsMatrix[user.role] || [],
    branches: branches.map((branch) => ({ id: branch.branch_id, code: branch.code, name: branch.name, city: branch.city, province: branch.province, accessType: branch.access_type }))
  };
}

function toClientAsset(asset) {
  return {
    id: asset.id,
    assetId: asset.asset_id,
    tagCode: asset.tag_code,
    name: asset.name,
    description: asset.description,
    categoryKey: asset.category_key,
    category: asset.category,
    branchId: asset.branch_id,
    branchCode: asset.branch_code,
    branchName: asset.branch_name,
    city: asset.city,
    province: asset.province,
    currency: asset.currency,
    acquisitionCost: Number(asset.acquisition_cost),
    residualValue: Number(asset.residual_value),
    capitalisationDate: asset.capitalisation_date,
    usefulLifeMonths: asset.useful_life_months,
    depreciationMethod: asset.depreciation_method,
    accumulatedDepreciation: Number(asset.accumulated_depreciation),
    impairmentLoss: Number(asset.impairment_loss),
    netBookValue: Number(asset.net_book_value),
    status: asset.status,
    statusLabel: statusPalette[asset.status]?.label || asset.status,
    roleTone: statusPalette[asset.status]?.tone || "muted",
    glAssetAccount: asset.gl_asset_account,
    glDepreciationExpenseAccount: asset.gl_depreciation_expense_account,
    glAccumulatedDepreciationAccount: asset.gl_accumulated_depreciation_account,
    purchaseOrderRef: asset.purchase_order_ref,
    warrantyExpiryDate: asset.warranty_expiry_date,
    lastVerifiedAt: asset.last_verified_at
  };
}

function assetForPreview(asset) {
  return {
    assetId: asset.asset_id,
    tagCode: asset.tag_code,
    name: asset.name,
    branchName: asset.branch_name,
    city: asset.city,
    province: asset.province,
    categoryKey: asset.category_key,
    category: asset.category,
    currency: asset.currency,
    netBookValue: Number(asset.net_book_value),
    status: asset.status
  };
}

function toClientDepreciationRun(run) {
  return {
    id: run.id,
    period: run.period,
    status: run.status,
    statusLabel: approvalStatusLabel(run.status),
    totalAssetsProcessed: run.total_assets_processed,
    totalAssetsSkipped: run.total_assets_skipped,
    totalDepreciationUSD: Number(run.total_depreciation_usd),
    totalDepreciationCDF: Number(run.total_depreciation_cdf),
    exchangeRateUsed: Number(run.exchange_rate_used),
    failureCount: run.failure_count,
    summary: run.summary,
    glBatchReference: run.gl_batch_reference,
    runByUserId: run.run_by_user_id || null,
    runByName: run.run_by_name || null,
    approvedByName: run.approved_by_name || null,
    approvedAt: run.approved_at || null,
    postedAt: run.posted_at || null
  };
}

function toClientApprovalRequest(request) {
  return {
    id: request.id,
    entityType: request.entity_type,
    entityLabel: approvalEntityLabel[request.entity_type] || request.entity_type,
    entityId: request.entity_id,
    actionType: request.action_type,
    actionLabel: approvalActionLabel[request.action_type] || request.action_type,
    title: request.title,
    detail: request.detail,
    branchId: request.branch_id,
    branchCode: request.branch_code || null,
    branchName: request.branch_name || "Head Office",
    status: request.status,
    statusLabel: approvalStatusLabel(request.status),
    statusTone: approvalStatusTone[request.status] || "muted",
    requestedByUserId: request.requested_by_user_id || null,
    requestedByName: request.requested_by_name || "System",
    requestedByRole: request.requested_by_role || "system",
    approvedByName: request.approved_by_name || null,
    rejectedByName: request.rejected_by_name || null,
    decisionNotes: request.decision_notes || "",
    requestedAt: request.requested_at,
    approvedAt: request.approved_at || null,
    rejectedAt: request.rejected_at || null,
    payload: request.payload || {}
  };
}

function toClientDepreciationException(line) {
  return {
    id: line.id,
    assetId: line.asset_public_id,
    tagCode: line.tag_code,
    assetName: line.asset_name,
    branchCode: line.branch_code,
    branchName: line.branch_name,
    openingNBV: Number(line.opening_nbv),
    depreciationCharge: Number(line.depreciation_charge),
    closingNBV: Number(line.closing_nbv),
    currency: line.currency,
    postingStatus: line.posting_status,
    postingReference: line.posting_reference || null,
    retryCount: line.retry_count || 0,
    failureReason: line.failure_reason || "",
    postedAt: line.posted_at || null
  };
}

function roundAmount(value) {
  return Math.round(Number(value || 0) * 100) / 100;
}

function roleLabel(role) {
  return roleCatalog.find((item) => item.value === role)?.label || role;
}

function toClientAttachment(attachment) {
  return {
    id: attachment.id,
    attachmentType: attachment.attachment_type,
    fileName: attachment.file_name,
    referenceUrl: attachment.reference_url || "",
    note: attachment.note || "",
    uploadedByName: attachment.uploaded_by_name || "System",
    createdAt: attachment.created_at
  };
}

function toClientImportBatch(batch) {
  return {
    id: batch.id,
    sourceType: batch.source_type,
    status: batch.status,
    summary: batch.summary || {},
    createdByName: batch.created_by_name || null,
    createdAt: batch.created_at,
    importedAt: batch.imported_at || null
  };
}

function toClientImportRow(row) {
  return {
    id: row.id,
    rowNumber: row.row_number,
    assetId: row.asset_public_id || "",
    tagCode: row.tag_code || "",
    status: row.status,
    message: row.message || "",
    payload: row.payload || {}
  };
}

function toClientAdminUser(user) {
  const branchAccess = Array.isArray(user.branch_access) ? user.branch_access : [];
  return {
    id: user.id,
    email: user.email,
    name: user.name,
    role: user.role,
    roleLabel: roleLabel(user.role),
    isActive: user.is_active,
    homeBranchId: user.home_branch_id || null,
    branchCode: user.branch_code || null,
    branchName: user.branch_name || "Unassigned",
    phoneNumber: user.phone_number || "",
    lastLoginAt: user.last_login_at || null,
    passwordChangedAt: user.password_changed_at || null,
    branchAccess: branchAccess.map((branch) => ({
      branchId: branch.branchId,
      code: branch.code,
      name: branch.name,
      accessType: branch.accessType
    }))
  };
}

function toClientVerificationQueueItem(row) {
  return {
    id: row.id,
    assetId: row.asset_id,
    tagCode: row.tag_code,
    name: row.name,
    status: row.status,
    statusLabel: statusPalette[row.status]?.label || row.status,
    branchCode: row.branch_code,
    branchName: row.branch_name,
    city: row.city,
    lastVerifiedAt: row.last_verified_at || null
  };
}

function hasAnyPermission(user, permissions) {
  return permissions.some((permission) => requirePermission(user, permission));
}

function ensureScopedBranchAccess(user, branchRows, branchId, message = "You do not have access to the selected branch.") {
  if (scopeForAssets(user) === "all") return;
  if (!branchRows.some((row) => row.branch_id === branchId)) {
    throw new AppError(403, message);
  }
}

function pickFirstValue(payload, keys) {
  for (const key of keys) {
    const value = payload[key];
    if (value != null && String(value).trim() !== "") return String(value).trim();
  }
  return "";
}

function isIsoDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || ""));
}

function toPositiveNumber(value, label) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new AppError(400, `${label} must be a valid non-negative number.`);
  }
  return roundAmount(parsed);
}

async function resolveAssetMutationInput(env, user, branchRows, payload, existingAsset = null) {
  const categoryKey = String(payload.categoryKey || existingAsset?.category_key || "").trim().toUpperCase();
  const category = categoryProfiles.find((item) => item.key === categoryKey);
  if (!category) throw new AppError(400, "Unknown asset category.");

  const branchCode = String(payload.branchCode || existingAsset?.branch_code || "").trim();
  const branch = await userRepository.findBranchByCode(env.databaseUrl, branchCode);
  if (!branch || !branch.is_active) throw new AppError(400, "Selected branch is not active.");
  ensureScopedBranchAccess(user, branchRows, branch.id);

  const currency = String(payload.currency || existingAsset?.currency || "").trim().toUpperCase();
  if (!["USD", "CDF"].includes(currency)) throw new AppError(400, "Currency must be either USD or CDF.");

  const assetId = String(payload.assetId || existingAsset?.asset_id || "").trim();
  const tagCode = String(payload.tagCode || existingAsset?.tag_code || "").trim().toUpperCase();
  if (!assetId) throw new AppError(400, "Asset ID is required.");
  if (!tagCode) throw new AppError(400, "Tag code is required.");
  if (!String(payload.name || existingAsset?.name || "").trim()) throw new AppError(400, "Asset name is required.");

  const acquisitionCost = toPositiveNumber(payload.acquisitionCost ?? existingAsset?.acquisition_cost, "Acquisition cost");
  const residualValue = payload.residualValue === "" || payload.residualValue == null
    ? roundAmount(acquisitionCost * Number(category.residualRate || 0))
    : toPositiveNumber(payload.residualValue, "Residual value");
  const capitalisationDate = String(payload.capitalisationDate || existingAsset?.capitalisation_date || "").trim();
  if (!isIsoDate(capitalisationDate)) throw new AppError(400, "Capitalisation date must use YYYY-MM-DD format.");

  const usefulLifeMonths = Number(payload.usefulLifeMonths || existingAsset?.useful_life_months || category.usefulLifeMonths);
  if (!Number.isInteger(usefulLifeMonths) || usefulLifeMonths < 0) {
    throw new AppError(400, "Useful life must be a valid number of months.");
  }

  const depreciationMethod = String(payload.depreciationMethod || existingAsset?.depreciation_method || category.method).trim().toUpperCase();
  if (!["SLM", "WDV", "NONE"].includes(depreciationMethod)) {
    throw new AppError(400, "Depreciation method must be SLM, WDV, or NONE.");
  }

  const accumulatedDepreciation = existingAsset ? Number(existingAsset.accumulated_depreciation || 0) : 0;
  const impairmentLoss = existingAsset ? Number(existingAsset.impairment_loss || 0) : 0;
  const netBookValue = payload.netBookValue === "" || payload.netBookValue == null
    ? roundAmount(Math.max(acquisitionCost - accumulatedDepreciation - impairmentLoss, 0))
    : toPositiveNumber(payload.netBookValue, "Net book value");

  const status = String(payload.status || existingAsset?.status || "ACTIVE").trim().toUpperCase();
  if (!statusPalette[status]) throw new AppError(400, "Selected asset status is invalid.");

  const defaultAccounts = await assetRepository.getDefaultAccountsForCategory(env.databaseUrl, categoryKey);
  return {
    assetId,
    tagCode,
    name: String(payload.name || existingAsset?.name || "").trim(),
    description: String(payload.description || existingAsset?.description || "").trim(),
    categoryKey,
    category,
    branchId: branch.id,
    branchCode: branch.code,
    currency,
    acquisitionCost,
    residualValue,
    capitalisationDate,
    usefulLifeMonths,
    depreciationMethod,
    netBookValue,
    status,
    purchaseOrderRef: String(payload.purchaseOrderRef || existingAsset?.purchase_order_ref || "").trim(),
    warrantyExpiryDate: String(payload.warrantyExpiryDate || existingAsset?.warranty_expiry_date || "").trim() || null,
    glAssetAccountId: defaultAccounts?.gl_asset_account_id || null,
    glDepreciationExpenseAccountId: defaultAccounts?.gl_depreciation_expense_account_id || null,
    glAccumulatedDepreciationAccountId: defaultAccounts?.gl_accumulated_depreciation_account_id || null
  };
}

function buildBatchReference(period, suffix, count) {
  return `${suffix}-${period.replace("-", "")}-${count}`;
}

function scopeLabelForUser(user) {
  return scopeForAssets(user) === "all" ? "All branches" : `${user.branch_name || user.branchName || "Assigned branch"} only`;
}

function buildWorkflowColumns(rows, pendingApprovals = new Map()) {
  const initial = { pendingTransfers: [], inTransit: [], pendingDisposals: [], pendingImpairments: [] };

  for (const row of rows) {
    const pendingApproval = pendingApprovals.get(row.id) || null;
    const card = {
      id: row.id,
      title:
        row.workflow_type === "TRANSFER"
          ? row.status === "IN_TRANSIT"
            ? "Awaiting Destination Confirmation"
            : "Transfer Awaiting Source Approval"
          : row.workflow_type === "DISPOSAL"
            ? "Disposal Request Pending Finance"
            : "Impairment Indicator Raised",
      actionLabel:
        row.workflow_type === "TRANSFER"
          ? row.status === "IN_TRANSIT"
            ? "Confirm Receipt"
            : "Approve Source"
          : row.workflow_type === "DISPOSAL"
            ? "Approve Disposal"
            : "Record Impairment",
      nextColumn: row.status === "IN_TRANSIT" ? "completed" : row.workflow_type === "TRANSFER" ? "inTransit" : "completed",
      note: row.notes,
      approvalState: pendingApproval ? "PENDING" : "READY",
      approvalRequestId: pendingApproval?.id || null,
      approvalLabel: pendingApproval ? "Submitted for checker approval" : "",
      asset: {
        id: row.asset_row_id,
        assetId: row.asset_id,
        tagCode: row.tag_code,
        name: row.asset_name,
        branchName: row.branch_name,
        branchCode: row.branch_code,
        status: row.asset_status
      }
    };

    if (row.status === "PENDING_TRANSFERS") initial.pendingTransfers.push(card);
    if (row.status === "IN_TRANSIT") initial.inTransit.push(card);
    if (row.status === "PENDING_DISPOSALS") initial.pendingDisposals.push(card);
    if (row.status === "PENDING_IMPAIRMENTS") initial.pendingImpairments.push(card);
  }

  return initial;
}

async function logAudit(databaseUrl, user, action, entityType, entityId, detail, metadata = {}, branchId = null) {
  await withTransaction(databaseUrl, async (client) => {
    await auditRepository.insertAuditLog(client, {
      actorUserId: user?.id || null,
      actorName: user?.name || "System",
      actorRole: user?.role || "system",
      branchId,
      action,
      entityType,
      entityId,
      detail,
      metadata
    });
  });
}

async function performWorkflowAdvance(client, workflow, user, metadata = {}) {
  const nextStatus = workflowNextStatus(workflow);
  const updatedWorkflow = await workflowRepository.advanceWorkflow(client, workflow.id, nextStatus, user.id);

  if (workflow.workflow_type === "TRANSFER" && nextStatus === "COMPLETED") {
    await workflowRepository.updateAssetBranch(client, workflow.asset_id, workflow.to_branch_id, "TRANSFERRED");
  } else if (workflow.workflow_type === "DISPOSAL" && nextStatus === "COMPLETED") {
    await workflowRepository.updateAssetStatus(client, workflow.asset_id, "DISPOSED");
  } else if (workflow.workflow_type === "IMPAIRMENT" && nextStatus === "COMPLETED") {
    await workflowRepository.updateAssetStatus(client, workflow.asset_id, "IMPAIRED");
  }

  await auditRepository.insertAuditLog(client, {
    actorUserId: user.id,
    actorName: user.name,
    actorRole: user.role,
    branchId: workflow.asset_branch_id,
    action: "WORKFLOW_ADVANCE",
    entityType: "workflow_request",
    entityId: workflow.id,
    detail: `Workflow ${workflow.workflow_type} moved to ${updatedWorkflow.status}.`,
    metadata: { workflowType: workflow.workflow_type, status: updatedWorkflow.status, ...metadata }
  });

  return updatedWorkflow;
}

async function getCurrentUserContext(env, userId) {
  const user = await userRepository.findUserById(env.databaseUrl, userId);
  if (!user || !user.is_active) throw new AppError(401, "User session is no longer valid.");
  const branches = await userRepository.listUserBranchAccess(env.databaseUrl, user.id);
  return { user, branchRows: branches };
}

async function login(env, credentials) {
  const user = await userRepository.findUserByEmail(env.databaseUrl, credentials.email);
  if (!user || !user.is_active) throw new AppError(401, "Invalid email or password.");
  const passwordMatches = verifyPassword(credentials.password, user.password_hash);
  if (!passwordMatches) throw new AppError(401, "Invalid email or password.");

  await userRepository.updateLastLogin(env.databaseUrl, user.id);
  const branches = await userRepository.listUserBranchAccess(env.databaseUrl, user.id);
  const token = jwt.sign({ sub: user.id, role: user.role }, env.jwtSecret, { expiresIn: env.jwtExpiresIn });
  await logAudit(env.databaseUrl, user, "LOGIN", "user", user.email, `User ${user.name} signed into the system.`, {}, user.home_branch_id);

  return { token, user: toClientUser(user, branches) };
}

async function meta(env, currentUser) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  const branches = requirePermission(user, "meta.read") && scopeForAssets(user) === "all"
    ? await userRepository.listBranches(env.databaseUrl)
    : branchRows.map((branch) => ({ id: branch.branch_id, code: branch.code, name: branch.name, city: branch.city, province: branch.province }));

  return {
    user: toClientUser(user, branchRows),
    branches,
    categories: categoryProfiles.map((category) => ({ key: category.key, label: category.label, method: category.method, usefulLifeMonths: category.usefulLifeMonths })),
    statuses: Object.entries(statusPalette).map(([status, definition]) => ({ status, label: definition.label })),
    roles: roleCatalog,
    attachmentTypes: attachmentTypeCatalog
  };
}

async function dashboard(env, currentUser) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  const branchIds = branchScopeRows(user, branchRows);
  const [metrics, alerts, breakdown, recentActivity, branchSummary, verificationQueue, depreciationRuns, importBatches] = await Promise.all([
    assetRepository.getDashboardMetrics(env.databaseUrl, branchIds),
    assetRepository.getDashboardAlerts(env.databaseUrl, branchIds),
    assetRepository.listStatusBreakdown(env.databaseUrl, branchIds),
    auditRepository.listRecentAuditLogs(env.databaseUrl, branchIds),
    assetRepository.listBranchSummary(env.databaseUrl, branchIds),
    assetRepository.listVerificationQueue(env.databaseUrl, branchIds, 8),
    financeRepository.listDepreciationRuns(env.databaseUrl),
    importRepository.listImportBatches(env.databaseUrl, 4)
  ]);

  return {
    kpis: {
      totalAssets: metrics.total_assets,
      totalLocations: metrics.total_locations,
      totalUSDNBV: Math.round(Number(metrics.total_usd_nbv)),
      totalCDFNBV: Math.round(Number(metrics.total_cdf_nbv)),
      pendingActions: metrics.pending_actions
    },
    alerts,
    statusBreakdown: breakdown.map((item) => ({ status: item.status, label: statusPalette[item.status]?.label || item.status, count: item.count })),
    depreciationTrend: depreciationRuns.slice().reverse().map((run) => ({ period: run.period, totalUSD: Number(run.total_depreciation_usd), totalCDF: Number(run.total_depreciation_cdf) })),
    recentActivity: recentActivity.map((item) => ({ id: item.id, title: item.action.replaceAll("_", " "), detail: item.detail, user: item.user, timestamp: item.timestamp })),
    branchSummary,
    verificationQueue: verificationQueue.map(toClientVerificationQueueItem),
    importBatches: importBatches.map(toClientImportBatch),
    currentRun: depreciationRuns[0] ? toClientDepreciationRun(depreciationRuns[0]) : null
  };
}

async function listAssets(env, currentUser, filters) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  const result = await assetRepository.listAssets(env.databaseUrl, filters, branchScopeRows(user, branchRows));
  return {
    ...result,
    items: result.items.map(toClientAsset),
    scope: scopeForAssets(user) === "all" ? "all_branches" : "branch_only"
  };
}

async function getAsset(env, currentUser, publicId) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  const asset = await assetRepository.getAssetByPublicId(env.databaseUrl, publicId, branchScopeRows(user, branchRows));
  if (!asset) throw new AppError(404, "Asset not found in your current scope.");
  const attachments = await assetRepository.listAssetAttachments(env.databaseUrl, asset.id);

  return {
    asset: toClientAsset(asset),
    attachments: attachments.map(toClientAttachment),
    depreciationSchedule: buildSchedule({
      acquisitionCost: Number(asset.acquisition_cost),
      residualValue: Number(asset.residual_value),
      depreciationMethod: asset.depreciation_method,
      usefulLifeMonths: asset.useful_life_months,
      capitalisationDate: asset.capitalisation_date
    }, 18),
    auditHistory: (await auditRepository.listAuditLogs(env.databaseUrl, { user: "", action: "" }, [])).filter((log) => log.entity_id === asset.asset_id).slice(0, 12)
  };
}

async function createAsset(env, currentUser, payload) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  if (!hasAnyPermission(user, ["assets.create", "assets.create.branch"])) {
    throw new AppError(403, "You do not have permission to create assets.");
  }

  const input = await resolveAssetMutationInput(env, user, branchRows, payload);
  const duplicates = await assetRepository.findAssetsByAssetIdsOrTagCodes(env.databaseUrl, [input.assetId], [input.tagCode]);
  if (duplicates.some((item) => item.asset_id === input.assetId)) {
    throw new AppError(409, "Asset ID already exists.");
  }
  if (duplicates.some((item) => String(item.tag_code).toLowerCase() === input.tagCode.toLowerCase())) {
    throw new AppError(409, "Tag code already exists.");
  }

  const created = await withTransaction(env.databaseUrl, async (client) => {
    const asset = await assetRepository.createAsset(client, {
      ...input,
      createdByUserId: user.id,
      updatedByUserId: user.id
    });

    await auditRepository.insertAuditLog(client, {
      actorUserId: user.id,
      actorName: user.name,
      actorRole: user.role,
      branchId: asset.branch_id,
      action: "ASSET_CREATE",
      entityType: "asset",
      entityId: asset.asset_id,
      detail: `Created asset ${asset.asset_id} for ${asset.branch_name}.`,
      metadata: { categoryKey: asset.category_key, currency: asset.currency }
    });

    return asset;
  });

  return {
    message: "Asset created successfully.",
    asset: toClientAsset(created)
  };
}

async function updateAsset(env, currentUser, publicId, payload) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  if (!hasAnyPermission(user, ["assets.update", "assets.update.branch"])) {
    throw new AppError(403, "You do not have permission to update assets.");
  }

  const existingAsset = await assetRepository.getAssetByPublicId(env.databaseUrl, publicId, []);
  if (!existingAsset) throw new AppError(404, "Asset not found.");
  ensureScopedBranchAccess(user, branchRows, existingAsset.branch_id, "You can only update assets in your assigned branch.");

  const input = await resolveAssetMutationInput(env, user, branchRows, payload, existingAsset);
  const duplicates = await assetRepository.findAssetsByAssetIdsOrTagCodes(env.databaseUrl, [input.assetId], [input.tagCode]);
  const conflicting = duplicates.find((item) => item.id !== existingAsset.id);
  if (conflicting) throw new AppError(409, "Another asset already uses this asset ID or tag code.");

  const updated = await withTransaction(env.databaseUrl, async (client) => {
    const asset = await assetRepository.updateAsset(client, existingAsset.id, {
      ...input,
      updatedByUserId: user.id
    });

    await auditRepository.insertAuditLog(client, {
      actorUserId: user.id,
      actorName: user.name,
      actorRole: user.role,
      branchId: asset.branch_id,
      action: "ASSET_UPDATE",
      entityType: "asset",
      entityId: asset.asset_id,
      detail: `Updated asset ${asset.asset_id}.`,
      metadata: { status: asset.status, branchCode: asset.branch_code }
    });

    return asset;
  });

  return {
    message: "Asset updated successfully.",
    asset: toClientAsset(updated)
  };
}

async function addAssetAttachment(env, currentUser, publicId, payload) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  if (!hasAnyPermission(user, ["assets.attach", "assets.attach.branch", "assets.update", "assets.update.branch"])) {
    throw new AppError(403, "You do not have permission to add asset attachments.");
  }

  const asset = await assetRepository.getAssetByPublicId(env.databaseUrl, publicId, []);
  if (!asset) throw new AppError(404, "Asset not found.");
  ensureScopedBranchAccess(user, branchRows, asset.branch_id, "You can only add attachments for assets in your assigned branch.");

  const attachmentType = String(payload.attachmentType || "").trim().toUpperCase();
  if (!attachmentTypeCatalog.includes(attachmentType)) {
    throw new AppError(400, "Attachment type is invalid.");
  }

  const fileName = String(payload.fileName || "").trim();
  if (!fileName) throw new AppError(400, "Attachment label is required.");

  const created = await withTransaction(env.databaseUrl, async (client) => {
    const attachment = await assetRepository.insertAssetAttachment(client, {
      assetId: asset.id,
      attachmentType,
      fileName,
      referenceUrl: String(payload.referenceUrl || "").trim(),
      note: String(payload.note || "").trim(),
      uploadedByUserId: user.id
    });

    await auditRepository.insertAuditLog(client, {
      actorUserId: user.id,
      actorName: user.name,
      actorRole: user.role,
      branchId: asset.branch_id,
      action: "ATTACHMENT_ADD",
      entityType: "asset",
      entityId: asset.asset_id,
      detail: `Added ${attachmentType.toLowerCase()} attachment to ${asset.asset_id}.`,
      metadata: { fileName }
    });

    return attachment;
  });

  return {
    message: "Attachment recorded successfully.",
    attachment: toClientAttachment({ ...created, uploaded_by_name: user.name })
  };
}

async function createAssetWorkflow(env, currentUser, publicId, payload) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  if (!hasAnyPermission(user, ["workflows.advance", "workflows.advance.branch", "assets.update", "assets.update.branch"])) {
    throw new AppError(403, "You do not have permission to raise lifecycle requests.");
  }

  const asset = await assetRepository.getAssetByPublicId(env.databaseUrl, publicId, []);
  if (!asset) throw new AppError(404, "Asset not found.");
  ensureScopedBranchAccess(user, branchRows, asset.branch_id, "You can only raise requests for assets in your assigned branch.");

  const workflowType = String(payload.workflowType || "").trim().toUpperCase();
  if (!["TRANSFER", "DISPOSAL", "IMPAIRMENT"].includes(workflowType)) {
    throw new AppError(400, "Workflow type is invalid.");
  }

  let targetBranch = null;
  if (workflowType === "TRANSFER") {
    const targetCode = String(payload.toBranchCode || "").trim();
    if (!targetCode) throw new AppError(400, "Destination branch is required for transfers.");
    targetBranch = await userRepository.findBranchByCode(env.databaseUrl, targetCode);
    if (!targetBranch || !targetBranch.is_active) throw new AppError(400, "Destination branch is not active.");
    if (targetBranch.id === asset.branch_id) throw new AppError(400, "Destination branch must differ from the current branch.");
  }

  const status =
    workflowType === "TRANSFER"
      ? "PENDING_TRANSFERS"
      : workflowType === "DISPOSAL"
        ? "PENDING_DISPOSALS"
        : "PENDING_IMPAIRMENTS";

  const workflow = await withTransaction(env.databaseUrl, async (client) => {
    const created = await workflowRepository.createWorkflowRequest(client, {
      workflowType,
      status,
      assetId: asset.id,
      assetBranchId: asset.branch_id,
      toBranchId: targetBranch?.id || null,
      requestedByUserId: user.id,
      notes: String(payload.notes || "").trim(),
      payload: {
        requestedFromAsset: asset.asset_id,
        fromBranchCode: asset.branch_code,
        toBranchCode: targetBranch?.code || null
      }
    });

    await auditRepository.insertAuditLog(client, {
      actorUserId: user.id,
      actorName: user.name,
      actorRole: user.role,
      branchId: asset.branch_id,
      action: "WORKFLOW_CREATE",
      entityType: "workflow_request",
      entityId: created.id,
      detail: `Raised ${workflowType.toLowerCase()} request for ${asset.asset_id}.`,
      metadata: { workflowType, targetBranchCode: targetBranch?.code || null }
    });

    return created;
  });

  return {
    message: `${workflowType.toLowerCase()} request created successfully.`,
    workflow
  };
}

async function previewAssetImport(env, currentUser, csvText) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  if (!hasAnyPermission(user, ["assets.import", "assets.import.branch", "assets.create", "assets.create.branch"])) {
    throw new AppError(403, "You do not have permission to import assets.");
  }

  const parsed = csvRowsToObjects(csvText);
  if (!parsed.headers.length || !parsed.items.length) {
    throw new AppError(400, "The import file is empty. Paste CSV content exported from Excel.");
  }

  const branchMap = new Map((await userRepository.listBranches(env.databaseUrl)).map((branch) => [branch.code, branch]));
  const categoryMap = new Map(categoryProfiles.map((category) => [category.key, category]));
  const requestedAssetIds = parsed.items.map((row) => pickFirstValue(row.payload, ["asset_id", "assetid", "asset"]));
  const requestedTagCodes = parsed.items.map((row) => pickFirstValue(row.payload, ["tag_code", "tag", "tagid"]));
  const existingRows = await assetRepository.findAssetsByAssetIdsOrTagCodes(env.databaseUrl, requestedAssetIds, requestedTagCodes);
  const existingAssetIds = new Set(existingRows.map((row) => row.asset_id));
  const existingTagCodes = new Set(existingRows.map((row) => String(row.tag_code).toLowerCase()));
  const seenAssetIds = new Set();
  const seenTagCodes = new Set();

  const validationRows = [];
  for (const item of parsed.items) {
    const assetId = pickFirstValue(item.payload, ["asset_id", "assetid", "asset"]);
    const tagCode = pickFirstValue(item.payload, ["tag_code", "tag", "tagid"]).toUpperCase();
    const name = pickFirstValue(item.payload, ["name", "asset_name"]);
    const categoryKey = pickFirstValue(item.payload, ["category_key", "category"]).toUpperCase();
    const branchCode = pickFirstValue(item.payload, ["branch_code", "branch"]);
    const currency = pickFirstValue(item.payload, ["currency"]);
    const acquisitionCost = pickFirstValue(item.payload, ["acquisition_cost", "cost"]);
    const capitalisationDate = pickFirstValue(item.payload, ["capitalisation_date", "capitalization_date", "capitalisation"]);

    const errors = [];
    if (!assetId) errors.push("Asset ID is required.");
    if (!tagCode) errors.push("Tag code is required.");
    if (!name) errors.push("Asset name is required.");
    if (!categoryMap.has(categoryKey)) errors.push("Category is invalid.");
    if (!branchMap.has(branchCode)) {
      errors.push("Branch code is invalid.");
    } else {
      try {
        ensureScopedBranchAccess(user, branchRows, branchMap.get(branchCode).id, "You can only import rows for your assigned branch.");
      } catch (error) {
        errors.push(error.message);
      }
    }
    if (!["USD", "CDF"].includes(currency.toUpperCase())) errors.push("Currency must be USD or CDF.");
    if (!isIsoDate(capitalisationDate)) errors.push("Capitalisation date must use YYYY-MM-DD.");
    if (!acquisitionCost || Number.isNaN(Number(acquisitionCost)) || Number(acquisitionCost) < 0) {
      errors.push("Acquisition cost must be a valid non-negative number.");
    }
    const status = (pickFirstValue(item.payload, ["status"]) || "ACTIVE").toUpperCase();
    if (!statusPalette[status]) errors.push("Status is invalid.");
    const depreciationMethod = (pickFirstValue(item.payload, ["depreciation_method", "method"]) || categoryMap.get(categoryKey)?.method || "SLM").toUpperCase();
    if (!["SLM", "WDV", "NONE"].includes(depreciationMethod)) errors.push("Depreciation method is invalid.");
    if (assetId && existingAssetIds.has(assetId)) errors.push("Asset ID already exists.");
    if (tagCode && existingTagCodes.has(tagCode.toLowerCase())) errors.push("Tag code already exists.");
    if (assetId && seenAssetIds.has(assetId)) errors.push("Asset ID is duplicated within the file.");
    if (tagCode && seenTagCodes.has(tagCode.toLowerCase())) errors.push("Tag code is duplicated within the file.");

    if (assetId) seenAssetIds.add(assetId);
    if (tagCode) seenTagCodes.add(tagCode.toLowerCase());

    const normalized = errors.length ? item.payload : {
      assetId,
      tagCode,
      name,
      description: pickFirstValue(item.payload, ["description"]),
      categoryKey,
      branchCode,
      currency: currency.toUpperCase(),
      acquisitionCost: roundAmount(Number(acquisitionCost)),
      residualValue: pickFirstValue(item.payload, ["residual_value", "residual"])
        ? roundAmount(Number(pickFirstValue(item.payload, ["residual_value", "residual"])))
        : roundAmount(Number(acquisitionCost) * Number(categoryMap.get(categoryKey).residualRate || 0)),
      capitalisationDate,
      usefulLifeMonths: Number(pickFirstValue(item.payload, ["useful_life_months", "useful_life"]) || categoryMap.get(categoryKey).usefulLifeMonths),
      depreciationMethod,
      status,
      purchaseOrderRef: pickFirstValue(item.payload, ["purchase_order_ref", "purchase_order"]),
      warrantyExpiryDate: pickFirstValue(item.payload, ["warranty_expiry_date", "warranty_expiry"]) || null
    };

    validationRows.push({
      rowNumber: item.rowNumber,
      assetPublicId: assetId,
      tagCode,
      status: errors.length ? "ERROR" : "VALID",
      message: errors.join(" "),
      payload: normalized
    });
  }

  const summary = {
    totalRows: validationRows.length,
    validRows: validationRows.filter((row) => row.status === "VALID").length,
    errorRows: validationRows.filter((row) => row.status === "ERROR").length
  };

  const batch = await withTransaction(env.databaseUrl, async (client) => {
    const created = await importRepository.createImportBatch(client, {
      sourceType: "CSV",
      status: summary.validRows > 0 ? "VALIDATED" : "FAILED",
      summary,
      createdByUserId: user.id
    });
    await importRepository.insertImportBatchRows(client, created.id, validationRows);
    await auditRepository.insertAuditLog(client, {
      actorUserId: user.id,
      actorName: user.name,
      actorRole: user.role,
      branchId: user.home_branch_id || null,
      action: "IMPORT_PREVIEW",
      entityType: "import_batch",
      entityId: created.id,
      detail: `Validated ${summary.totalRows} import rows.`,
      metadata: summary
    });
    return created;
  });

  return {
    batch: toClientImportBatch({ ...batch, created_by_name: user.name }),
    rows: validationRows.map((row) => ({
      rowNumber: row.rowNumber,
      assetId: row.assetPublicId,
      tagCode: row.tagCode,
      status: row.status,
      message: row.message,
      payload: row.payload
    }))
  };
}

async function commitAssetImport(env, currentUser, batchId) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  if (!hasAnyPermission(user, ["assets.import", "assets.import.branch", "assets.create", "assets.create.branch"])) {
    throw new AppError(403, "You do not have permission to import assets.");
  }

  const batch = await importRepository.findImportBatchById(env.databaseUrl, batchId);
  if (!batch) throw new AppError(404, "Import batch not found.");
  if (batch.status !== "VALIDATED") throw new AppError(409, "Only validated import batches can be committed.");

  const rows = await importRepository.listImportBatchRows(env.databaseUrl, batchId);
  const validRows = rows.filter((row) => row.status === "VALID");
  if (!validRows.length) throw new AppError(409, "There are no valid rows left to import.");

  const importedAssets = [];
  const updatedBatch = await withTransaction(env.databaseUrl, async (client) => {
    for (const row of validRows) {
      const payload = row.payload || {};
      const branch = await userRepository.findBranchByCode(env.databaseUrl, payload.branchCode);
      ensureScopedBranchAccess(user, branchRows, branch.id, "You can only import rows for your assigned branch.");
      const category = categoryProfiles.find((item) => item.key === payload.categoryKey);
      const accounts = await assetRepository.getDefaultAccountsForCategory(env.databaseUrl, payload.categoryKey);
      const created = await assetRepository.createAsset(client, {
        assetId: payload.assetId,
        tagCode: payload.tagCode,
        name: payload.name,
        description: payload.description,
        categoryKey: payload.categoryKey,
        branchId: branch.id,
        currency: payload.currency,
        acquisitionCost: payload.acquisitionCost,
        residualValue: payload.residualValue,
        capitalisationDate: payload.capitalisationDate,
        usefulLifeMonths: payload.usefulLifeMonths || category.usefulLifeMonths,
        depreciationMethod: payload.depreciationMethod || category.method,
        netBookValue: payload.acquisitionCost,
        status: payload.status || "ACTIVE",
        purchaseOrderRef: payload.purchaseOrderRef,
        warrantyExpiryDate: payload.warrantyExpiryDate,
        glAssetAccountId: accounts?.gl_asset_account_id || null,
        glDepreciationExpenseAccountId: accounts?.gl_depreciation_expense_account_id || null,
        glAccumulatedDepreciationAccountId: accounts?.gl_accumulated_depreciation_account_id || null,
        createdByUserId: user.id,
        updatedByUserId: user.id
      });

      importedAssets.push(created);
      await importRepository.updateImportBatchRowStatus(client, row.id, "IMPORTED", "Imported successfully.");
      await auditRepository.insertAuditLog(client, {
        actorUserId: user.id,
        actorName: user.name,
        actorRole: user.role,
        branchId: created.branch_id,
        action: "ASSET_IMPORT",
        entityType: "asset",
        entityId: created.asset_id,
        detail: `Imported asset ${created.asset_id} from batch ${batchId}.`,
        metadata: { importBatchId: batchId, rowNumber: row.row_number }
      });
    }

    return importRepository.updateImportBatchStatus(client, batchId, "IMPORTED", {
      ...(batch.summary || {}),
      importedRows: importedAssets.length
    });
  });

  return {
    message: `${importedAssets.length} assets imported successfully.`,
    batch: toClientImportBatch({ ...updatedBatch, created_by_name: user.name }),
    importedAssets: importedAssets.map(toClientAsset)
  };
}

async function listImports(env, currentUser) {
  const { user } = await getCurrentUserContext(env, currentUser.id);
  if (!hasAnyPermission(user, ["assets.import", "assets.import.branch", "assets.create", "assets.create.branch"])) {
    throw new AppError(403, "You do not have permission to view import batches.");
  }

  const batches = await importRepository.listImportBatches(env.databaseUrl, 12);
  return {
    items: batches.map(toClientImportBatch)
  };
}

async function listLifecycle(env, currentUser) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  const branchIds = branchScopeRows(user, branchRows);
  const [rows, approvals] = await Promise.all([
    workflowRepository.listWorkflowRequests(env.databaseUrl, branchIds),
    approvalRepository.listApprovalRequests(env.databaseUrl, { status: "PENDING", entityType: "workflow_request" }, branchIds)
  ]);

  const approvalMap = new Map(approvals.map((request) => [request.entity_id, request]));
  return { columns: buildWorkflowColumns(rows, approvalMap) };
}

async function advanceWorkflow(env, currentUser, workflowId) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  const branchIds = branchScopeRows(user, branchRows);
  const workflow = await workflowRepository.findWorkflowById(env.databaseUrl, workflowId);
  if (!workflow) throw new AppError(404, "Workflow item not found.");
  if (branchIds.length > 0 && !branchIds.includes(workflow.asset_branch_id)) throw new AppError(403, "You can only action workflows in your assigned branch.");

  const actionType = workflowApprovalAction(workflow);
  const pendingApproval = await approvalRepository.findPendingApprovalForEntity(env.databaseUrl, "workflow_request", workflow.id, actionType);
  if (pendingApproval) throw new AppError(409, "This workflow already has a pending approval request.");

  if (requirePermission(user, "workflows.advance") && !requirePermission(user, "workflows.advance.branch")) {
    return withTransaction(env.databaseUrl, async (client) => {
      const updatedWorkflow = await performWorkflowAdvance(client, workflow, user, { mode: "direct_checker_action" });
      return { message: `Workflow moved to ${updatedWorkflow.status}.`, workflow: updatedWorkflow };
    });
  }

  return withTransaction(env.databaseUrl, async (client) => {
    const request = await approvalRepository.createApprovalRequest(client, {
      entityType: "workflow_request",
      entityId: workflow.id,
      actionType,
      title: approvalActionLabel[actionType] || "Workflow approval",
      detail: `Branch request raised for ${workflow.workflow_type.toLowerCase()} workflow ${workflow.id}.`,
      branchId: workflow.asset_branch_id,
      requestedByUserId: user.id,
      payload: { workflowType: workflow.workflow_type, currentStatus: workflow.status, nextStatus: workflowNextStatus(workflow) }
    });
    await auditRepository.insertAuditLog(client, {
      actorUserId: user.id,
      actorName: user.name,
      actorRole: user.role,
      branchId: workflow.asset_branch_id,
      action: "WORKFLOW_SUBMIT",
      entityType: "workflow_request",
      entityId: workflow.id,
      detail: `Workflow ${workflow.workflow_type} submitted for checker approval.`,
      metadata: { approvalRequestId: request.id, workflowType: workflow.workflow_type, currentStatus: workflow.status, nextStatus: workflowNextStatus(workflow) }
    });

    return {
      message: "Workflow submitted for checker approval.",
      approvalRequest: toClientApprovalRequest({
        ...request,
        branch_code: branchRows.find((row) => row.branch_id === workflow.asset_branch_id)?.code || null,
        branch_name: branchRows.find((row) => row.branch_id === workflow.asset_branch_id)?.name || null,
        requested_by_name: user.name,
        requested_by_role: user.role
      })
    };
  });
}

async function getDepreciation(env) {
  const runs = await financeRepository.listDepreciationRuns(env.databaseUrl);
  const currentRun = runs[0] ? toClientDepreciationRun(runs[0]) : null;
  const exceptionRows = currentRun ? await financeRepository.listDepreciationLinesWithAssetDetail(env.databaseUrl, currentRun.id) : [];
  const pendingApproval = currentRun ? await approvalRepository.findPendingApprovalForEntity(env.databaseUrl, "depreciation_run", currentRun.id, "DEPRECIATION_POST") : null;
  return {
    currentRun,
    history: runs.map(toClientDepreciationRun),
    approvalRequest: pendingApproval ? toClientApprovalRequest(pendingApproval) : null,
    postingExceptions: exceptionRows
      .filter((line) => line.posting_status === "FAILED")
      .map(toClientDepreciationException),
    postedLines: exceptionRows
      .filter((line) => line.posting_status === "POSTED")
      .slice(0, 8)
      .map(toClientDepreciationException)
  };
}

async function runDepreciation(env, currentUser, requestedPeriod) {
  const { user } = await getCurrentUserContext(env, currentUser.id);
  const period = requestedPeriod || new Date().toISOString().slice(0, 7);
  const exchangeRate = await financeRepository.getLatestExchangeRate(env.databaseUrl, "USD");
  const assets = await financeRepository.listDepreciableAssets(env.databaseUrl);

  const lines = assets.map((asset, index) => {
    const result = calculateDepreciationLine(asset, new Date(`${period}-28T00:00:00.000Z`));
    const forcedFailure = result.depreciationCharge > 0 && index < 2;
    return {
      assetId: asset.id,
      openingNBV: result.openingNBV,
      depreciationCharge: result.depreciationCharge,
      closingNBV: result.closingNBV,
      currency: asset.currency,
      postingStatus: forcedFailure ? "FAILED" : result.skipped ? "SKIPPED" : "PENDING",
      failureReason: forcedFailure ? "Finacle posting validation placeholder for parallel-run review." : result.skipReason
    };
  });

  const totalAssetsProcessed = lines.filter((line) => line.postingStatus !== "SKIPPED").length;
  const totalAssetsSkipped = lines.filter((line) => line.postingStatus === "SKIPPED").length;
  const totalDepreciationUSD = lines.filter((line) => line.currency === "USD").reduce((sum, line) => sum + line.depreciationCharge, 0);
  const totalDepreciationCDF = lines.filter((line) => line.currency === "CDF").reduce((sum, line) => sum + line.depreciationCharge, 0);
  const failureCount = lines.filter((line) => line.postingStatus === "FAILED").length;

  const run = await withTransaction(env.databaseUrl, async (client) => {
    const depreciationRun = await financeRepository.insertDepreciationRun(client, {
      period,
      status: "PENDING_APPROVAL",
      exchangeRateUsed: exchangeRate ? Number(exchangeRate.rate_to_cdf) : null,
      totalAssetsProcessed,
      totalAssetsSkipped,
      totalDepreciationUSD,
      totalDepreciationCDF,
      failureCount,
      runByUserId: user.id,
      summary: "Depreciation recalculated and submitted for checker approval."
    });
    await financeRepository.replaceDepreciationLines(client, depreciationRun.id, lines);
    await approvalRepository.cancelPendingApprovals(client, "depreciation_run", depreciationRun.id, "DEPRECIATION_POST");
    const request = await approvalRepository.createApprovalRequest(client, {
      entityType: "depreciation_run",
      entityId: depreciationRun.id,
      actionType: "DEPRECIATION_POST",
      title: "Post monthly depreciation batch",
      detail: `Posting approval requested for depreciation period ${period}.`,
      branchId: user.home_branch_id || null,
      requestedByUserId: user.id,
      payload: { period, totalAssetsProcessed, totalAssetsSkipped, failureCount }
    });
    await auditRepository.insertAuditLog(client, {
      actorUserId: user.id,
      actorName: user.name,
      actorRole: user.role,
      branchId: user.home_branch_id || null,
      action: "DEPRECIATION_SUBMIT",
      entityType: "depreciation_run",
      entityId: depreciationRun.id,
      detail: `Prepared depreciation run for ${period} and sent it for checker approval.`,
      metadata: { period, totalAssetsProcessed, totalAssetsSkipped, failureCount, approvalRequestId: request.id }
    });
    return { depreciationRun, request };
  });

  return {
    message: "Depreciation run submitted for checker approval.",
    currentRun: toClientDepreciationRun(run.depreciationRun),
    approvalRequest: toClientApprovalRequest({
      ...run.request,
      requested_by_name: user.name,
      requested_by_role: user.role,
      branch_name: user.branch_name || "Head Office"
    })
  };
}

async function approveDepreciation(env, currentUser, runId, notes = "") {
  const { user } = await getCurrentUserContext(env, currentUser.id);
  const runs = await financeRepository.listDepreciationRuns(env.databaseUrl);
  const targetRun = runId ? await financeRepository.findDepreciationRunById(env.databaseUrl, runId) : runs.find((run) => run.status === "PENDING_APPROVAL");
  if (!targetRun) throw new AppError(404, "No depreciation run is available to approve.");
  if (targetRun.status !== "PENDING_APPROVAL") throw new AppError(409, "This depreciation run is not awaiting approval.");
  if (!canApproveOwnRequest(targetRun.run_by_user_id, user.id)) {
    throw new AppError(409, "Maker-checker control blocked this action. A different approver must post the batch.");
  }

  const approvalRequest = await approvalRepository.findPendingApprovalForEntity(env.databaseUrl, "depreciation_run", targetRun.id, "DEPRECIATION_POST");
  if (!approvalRequest) throw new AppError(409, "This depreciation run does not have an active approval request.");

  const lines = await financeRepository.listDepreciationLinesWithAssetDetail(env.databaseUrl, targetRun.id);
  const batchReference = buildBatchReference(targetRun.period, "BATCH", lines.length);
  const pendingAssetIds = lines.filter((line) => line.posting_status === "PENDING").map((line) => line.asset_id);

  const approvedRun = await withTransaction(env.databaseUrl, async (client) => {
    await financeRepository.applyDepreciationToAssets(
      client,
      lines.filter((line) => line.posting_status === "PENDING").map((line) => ({
        assetId: line.asset_id,
        depreciationCharge: Number(line.depreciation_charge),
        closingNBV: Number(line.closing_nbv),
        postingStatus: line.posting_status
      }))
    );
    await financeRepository.markDepreciationLinesPosted(client, targetRun.id, pendingAssetIds, batchReference);
    const updated = await financeRepository.approveDepreciationRun(client, targetRun.id, user.id, batchReference, "Posted to Finacle. Failed lines remain queued for retry.");
    await approvalRepository.approveApprovalRequest(client, approvalRequest.id, user.id, notes || "Posted after checker approval.");
    await auditRepository.insertAuditLog(client, {
      actorUserId: user.id,
      actorName: user.name,
      actorRole: user.role,
      branchId: user.home_branch_id || null,
      action: "DEPRECIATION_APPROVE",
      entityType: "depreciation_run",
      entityId: targetRun.id,
      detail: `Approved depreciation run ${targetRun.period} and posted batch ${batchReference}.`,
      metadata: { batchReference, approvalRequestId: approvalRequest.id }
    });
    return updated;
  });

  return {
    message: "Depreciation batch approved and posted.",
    currentRun: toClientDepreciationRun(approvedRun)
  };
}

async function retryDepreciationFailures(env, currentUser, runId) {
  const { user } = await getCurrentUserContext(env, currentUser.id);
  const runs = await financeRepository.listDepreciationRuns(env.databaseUrl);
  const targetRun = runId ? await financeRepository.findDepreciationRunById(env.databaseUrl, runId) : runs[0];
  if (!targetRun) throw new AppError(404, "No depreciation run is available.");
  if (targetRun.status !== "POSTED") throw new AppError(409, "Only posted depreciation runs can be retried.");

  const lines = await financeRepository.listDepreciationLinesWithAssetDetail(env.databaseUrl, targetRun.id);
  const failedLines = lines.filter((line) => line.posting_status === "FAILED");
  if (!failedLines.length) throw new AppError(409, "There are no failed posting lines left to retry.");

  const retryReference = buildBatchReference(targetRun.period, "RETRY", failedLines.length);
  const retriedRun = await withTransaction(env.databaseUrl, async (client) => {
    await financeRepository.applyDepreciationToAssets(
      client,
      failedLines.map((line) => ({
        assetId: line.asset_id,
        depreciationCharge: Number(line.depreciation_charge),
        closingNBV: Number(line.closing_nbv),
        postingStatus: "POSTED"
      }))
    );
    await financeRepository.retryFailedDepreciationLines(client, targetRun.id, failedLines.map((line) => line.asset_id), retryReference);
    const updated = await financeRepository.updateDepreciationRunAfterRetry(client, targetRun.id, 0, `All failed posting lines cleared after retry batch ${retryReference}.`);
    await auditRepository.insertAuditLog(client, {
      actorUserId: user.id,
      actorName: user.name,
      actorRole: user.role,
      branchId: user.home_branch_id || null,
      action: "DEPRECIATION_RETRY",
      entityType: "depreciation_run",
      entityId: targetRun.id,
      detail: `Retried ${failedLines.length} failed posting lines with batch ${retryReference}.`,
      metadata: { retryReference, failedLineCount: failedLines.length }
    });
    return updated;
  });

  return {
    message: `${failedLines.length} failed posting lines retried successfully.`,
    currentRun: toClientDepreciationRun(retriedRun)
  };
}

async function listApprovals(env, currentUser, filters = {}) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  const branchIds = requirePermission(user, "approvals.read") ? [] : branchRows.map((row) => row.branch_id);
  const requests = await approvalRepository.listApprovalRequests(env.databaseUrl, {
    status: filters.status || "",
    entityType: filters.entityType || ""
  }, branchIds);

  return {
    pendingCount: requests.filter((request) => request.status === "PENDING").length,
    items: requests.map(toClientApprovalRequest)
  };
}

async function approveRequest(env, currentUser, approvalId, notes = "") {
  const { user } = await getCurrentUserContext(env, currentUser.id);
  const request = await approvalRepository.findApprovalRequestById(env.databaseUrl, approvalId);
  if (!request) throw new AppError(404, "Approval request not found.");
  if (request.status !== "PENDING") throw new AppError(409, "This approval request has already been decided.");
  if (!canApproveOwnRequest(request.requested_by_user_id, user.id)) {
    throw new AppError(409, "Maker-checker control blocked this action. A different approver must make the decision.");
  }

  if (request.entity_type === "depreciation_run") {
    return approveDepreciation(env, currentUser, request.entity_id, notes);
  }

  if (request.entity_type === "workflow_request") {
    const workflow = await workflowRepository.findWorkflowById(env.databaseUrl, request.entity_id);
    if (!workflow) throw new AppError(404, "Linked workflow no longer exists.");

    return withTransaction(env.databaseUrl, async (client) => {
      const updatedWorkflow = await performWorkflowAdvance(client, workflow, user, { approvalRequestId: request.id });
      await approvalRepository.approveApprovalRequest(client, request.id, user.id, notes || "Approved by checker.");
      return { message: `Workflow moved to ${updatedWorkflow.status}.`, workflow: updatedWorkflow };
    });
  }

  throw new AppError(400, "Unsupported approval request type.");
}

async function rejectRequest(env, currentUser, approvalId, notes = "") {
  const { user } = await getCurrentUserContext(env, currentUser.id);
  const request = await approvalRepository.findApprovalRequestById(env.databaseUrl, approvalId);
  if (!request) throw new AppError(404, "Approval request not found.");
  if (request.status !== "PENDING") throw new AppError(409, "This approval request has already been decided.");
  if (!canApproveOwnRequest(request.requested_by_user_id, user.id)) {
    throw new AppError(409, "Maker-checker control blocked this action. A different approver must make the decision.");
  }

  return withTransaction(env.databaseUrl, async (client) => {
    const rejected = await approvalRepository.rejectApprovalRequest(client, approvalId, user.id, notes || "Rejected by checker.");

    if (request.entity_type === "depreciation_run") {
      await financeRepository.updateDepreciationRunStatus(client, request.entity_id, "REJECTED", `Posting request rejected. ${notes || "Please review and resubmit."}`, user.id);
    }

    await auditRepository.insertAuditLog(client, {
      actorUserId: user.id,
      actorName: user.name,
      actorRole: user.role,
      branchId: request.branch_id || user.home_branch_id || null,
      action: "APPROVAL_REJECT",
      entityType: request.entity_type,
      entityId: request.entity_id,
      detail: `${request.title} rejected.`,
      metadata: { approvalRequestId: request.id, notes }
    });

    return {
      message: "Approval request rejected.",
      approvalRequest: toClientApprovalRequest({
        ...request,
        ...rejected,
        requested_by_name: request.requested_by_name,
        requested_by_role: request.requested_by_role,
        branch_name: request.branch_name,
        branch_code: request.branch_code,
        rejected_by_name: user.name
      })
    };
  });
}

async function getReconciliation(env) {
  const latest = await financeRepository.getLatestReconciliation(env.databaseUrl);
  if (!latest) return null;
  return {
    period: latest.run.period,
    status: latest.run.status,
    famsBalanceUSD: Number(latest.run.fams_balance_usd),
    famsBalanceCDF: Number(latest.run.fams_balance_cdf),
    glBalanceUSD: Number(latest.run.gl_balance_usd),
    glBalanceCDF: Number(latest.run.gl_balance_cdf),
    varianceUSD: Number(latest.run.variance_usd),
    varianceCDF: Number(latest.run.variance_cdf),
    discrepancyCount: latest.run.discrepancy_count,
    lastRunAt: latest.run.created_at,
    accounts: latest.lines.map((line) => ({ glCode: line.gl_code, label: line.label, varianceUSD: Number(line.variance_usd), varianceCDF: Number(line.variance_cdf), status: line.status }))
  };
}

async function runReconciliation(env, currentUser, period) {
  const { user } = await getCurrentUserContext(env, currentUser.id);
  const effectivePeriod = period || new Date().toISOString().slice(0, 7);
  const [famsRows, glRows] = await Promise.all([
    financeRepository.getAssetBalancesByGlCode(env.databaseUrl),
    financeRepository.getGlBalancesForPeriod(env.databaseUrl, effectivePeriod)
  ]);
  const glMap = new Map(glRows.map((row) => [row.gl_code, row]));
  const lines = famsRows.slice(0, 10).map((row) => {
    const glRow = glMap.get(row.gl_code) || { gl_usd: 0, gl_cdf: 0 };
    return {
      glCode: row.gl_code,
      label: row.label,
      famsUSD: Number(row.fams_usd),
      glUSD: Number(glRow.gl_usd),
      varianceUSD: Number(row.fams_usd) - Number(glRow.gl_usd),
      famsCDF: Number(row.fams_cdf),
      glCDF: Number(glRow.gl_cdf),
      varianceCDF: Number(row.fams_cdf) - Number(glRow.gl_cdf),
      status: Number(row.fams_usd) === Number(glRow.gl_usd) && Number(row.fams_cdf) === Number(glRow.gl_cdf) ? "MATCHED" : "EXCEPTION"
    };
  });
  const totals = lines.reduce((acc, line) => ({ famsUSD: acc.famsUSD + line.famsUSD, glUSD: acc.glUSD + line.glUSD, famsCDF: acc.famsCDF + line.famsCDF, glCDF: acc.glCDF + line.glCDF }), { famsUSD: 0, glUSD: 0, famsCDF: 0, glCDF: 0 });

  const run = await withTransaction(env.databaseUrl, async (client) => {
    const reconciliationRun = await financeRepository.createReconciliationRun(client, {
      period: effectivePeriod,
      status: lines.some((line) => line.status === "EXCEPTION") ? "EXCEPTION" : "MATCHED",
      famsBalanceUSD: totals.famsUSD,
      famsBalanceCDF: totals.famsCDF,
      glBalanceUSD: totals.glUSD,
      glBalanceCDF: totals.glCDF,
      varianceUSD: totals.famsUSD - totals.glUSD,
      varianceCDF: totals.famsCDF - totals.glCDF,
      discrepancyCount: lines.filter((line) => line.status === "EXCEPTION").length,
      runByUserId: user.id
    });
    await financeRepository.insertReconciliationLines(client, reconciliationRun.id, lines);
    await auditRepository.insertAuditLog(client, {
      actorUserId: user.id,
      actorName: user.name,
      actorRole: user.role,
      branchId: user.home_branch_id || null,
      action: "GL_RECONCILE",
      entityType: "reconciliation_run",
      entityId: reconciliationRun.id,
      detail: `Ran GL reconciliation for ${effectivePeriod}.`,
      metadata: { discrepancyCount: lines.filter((line) => line.status === "EXCEPTION").length }
    });
    return reconciliationRun;
  });

  return {
    message: "Reconciliation refreshed.",
    reconciliation: {
      period: run.period,
      status: run.status,
      famsBalanceUSD: Number(run.fams_balance_usd),
      famsBalanceCDF: Number(run.fams_balance_cdf),
      glBalanceUSD: Number(run.gl_balance_usd),
      glBalanceCDF: Number(run.gl_balance_cdf),
      varianceUSD: Number(run.variance_usd),
      varianceCDF: Number(run.variance_cdf),
      discrepancyCount: run.discrepancy_count,
      lastRunAt: run.created_at,
      accounts: lines.map((line) => ({ glCode: line.glCode, label: line.label, varianceUSD: line.varianceUSD, varianceCDF: line.varianceCDF, status: line.status }))
    }
  };
}

async function listReports() {
  return {
    reports: reportCatalog.map((report) => ({
      ...report,
      exportFormats: ["preview", "csv", "excel", "print"]
    }))
  };
}

async function buildReportArtifacts(env, currentUser, reportId) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  const report = reportCatalog.find((item) => item.id === reportId);
  if (!report) throw new AppError(404, "Report definition not found.");

  const branchIds = branchScopeRows(user, branchRows);
  const [assets, reconciliation, depreciation] = await Promise.all([
    assetRepository.listAssets(env.databaseUrl, { page: 1, pageSize: 6000, search: "", status: "", branchCode: "", categoryKey: "" }, branchIds),
    reportId === "gl-reconciliation" ? financeRepository.getLatestReconciliation(env.databaseUrl) : Promise.resolve(null),
    (reportId === "ohada" || reportId === "ias16") ? financeRepository.listDepreciationRuns(env.databaseUrl) : Promise.resolve([])
  ]);

  const latestDepreciation = Array.isArray(depreciation) ? depreciation[0] : null;
  const pack = buildReportPack(report, assets.items.map(toClientAsset), {
    generatedAt: new Date().toISOString(),
    generatedBy: user.name,
    scopeLabel: scopeLabelForUser(user),
    exchangeRateToCdf: latestDepreciation?.exchange_rate_used || 2850,
    reconciliation: reconciliation
      ? {
          period: reconciliation.run.period,
          status: reconciliation.run.status,
          varianceUSD: Number(reconciliation.run.variance_usd),
          varianceCDF: Number(reconciliation.run.variance_cdf),
          accounts: reconciliation.lines.map((line) => ({
            glCode: line.gl_code,
            label: line.label,
            varianceUSD: Number(line.variance_usd),
            varianceCDF: Number(line.variance_cdf),
            status: line.status
          }))
        }
      : null
  });

  return { user, report, pack };
}

async function generateReport(env, currentUser, reportId) {
  const { user, report, pack } = await buildReportArtifacts(env, currentUser, reportId);
  await logAudit(env.databaseUrl, user, "REPORT_EXPORT", "report", report.id, `Generated ${report.title}.`, {}, user.home_branch_id);
  return {
    report,
    summary: pack.summary,
    preview: pack.preview,
    exports: {
      csv: `/api/reports/${report.id}/export.csv`,
      excel: `/api/reports/${report.id}/export.excel.xml`,
      print: `/api/reports/${report.id}/export.print.html`
    }
  };
}

async function exportReportCsv(env, currentUser, reportId) {
  const { pack } = await buildReportArtifacts(env, currentUser, reportId);
  return { filename: `${reportId}-${new Date().toISOString().slice(0, 10)}.csv`, csv: buildReportCsv(pack) };
}

async function exportReportExcel(env, currentUser, reportId) {
  const { pack } = await buildReportArtifacts(env, currentUser, reportId);
  return { filename: `${reportId}-${new Date().toISOString().slice(0, 10)}.xml`, xml: buildReportExcelXml(pack) };
}

async function exportReportPrintHtml(env, currentUser, reportId) {
  const { pack } = await buildReportArtifacts(env, currentUser, reportId);
  return { filename: `${reportId}-${new Date().toISOString().slice(0, 10)}.html`, html: buildPrintableHtml(pack) };
}

async function listAudit(env, currentUser, filters) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  const branchIds = scopeForAssets(user) === "all" ? [] : branchRows.map((row) => row.branch_id);
  const items = await auditRepository.listAuditLogs(env.databaseUrl, filters, branchIds);
  return { items };
}

async function listUsers(env, currentUser) {
  const { user } = await getCurrentUserContext(env, currentUser.id);
  if (!hasAnyPermission(user, ["admin.users.read", "admin.users.manage"])) {
    throw new AppError(403, "You do not have permission to view users.");
  }

  const users = await userRepository.listUsers(env.databaseUrl);
  return {
    items: users.map(toClientAdminUser)
  };
}

async function saveUser(env, currentUser, payload, userId = null) {
  const { user } = await getCurrentUserContext(env, currentUser.id);
  if (!requirePermission(user, "admin.users.manage")) {
    throw new AppError(403, "You do not have permission to manage users.");
  }

  const homeBranch = payload.homeBranchCode ? await userRepository.findBranchByCode(env.databaseUrl, payload.homeBranchCode) : null;
  if (payload.homeBranchCode && !homeBranch) throw new AppError(400, "Home branch code is invalid.");

  const branchCodes = Array.isArray(payload.branchCodes) ? payload.branchCodes : [];
  const branches = await Promise.all(branchCodes.map((code) => userRepository.findBranchByCode(env.databaseUrl, code)));
  if (branches.some((branch) => !branch)) throw new AppError(400, "One or more assigned branch codes are invalid.");
  const branchIds = branches.filter(Boolean).map((branch) => branch.id);

  let savedUserId = userId;
  await withTransaction(env.databaseUrl, async (client) => {
    if (userId) {
      const existing = await userRepository.findUserById(env.databaseUrl, userId);
      if (!existing) throw new AppError(404, "User not found.");
      await userRepository.updateUser(client, userId, {
        name: String(payload.name || "").trim(),
        role: payload.role,
        homeBranchId: homeBranch?.id || null,
        isActive: payload.isActive !== false,
        phoneNumber: String(payload.phoneNumber || "").trim()
      });
    } else {
      const email = String(payload.email || "").trim().toLowerCase();
      if (!email) throw new AppError(400, "Email is required.");
      if (!payload.password || String(payload.password).length < 8) {
        throw new AppError(400, "New users must have a password with at least 8 characters.");
      }
      const existing = await userRepository.findUserByEmail(env.databaseUrl, email);
      if (existing) throw new AppError(409, "A user with that email already exists.");
      const created = await userRepository.createUser(client, {
        email,
        passwordHash: hashPassword(payload.password),
        name: String(payload.name || "").trim(),
        role: payload.role,
        homeBranchId: homeBranch?.id || null,
        isActive: payload.isActive !== false,
        phoneNumber: String(payload.phoneNumber || "").trim()
      });
      savedUserId = created.id;
    }

    await userRepository.replaceUserBranchAccess(client, savedUserId, branchIds, homeBranch?.id || null);
    await auditRepository.insertAuditLog(client, {
      actorUserId: user.id,
      actorName: user.name,
      actorRole: user.role,
      branchId: homeBranch?.id || user.home_branch_id || null,
      action: userId ? "USER_UPDATE" : "USER_CREATE",
      entityType: "user",
      entityId: payload.email || userId,
      detail: userId ? `Updated user ${payload.email || userId}.` : `Created user ${payload.email}.`,
      metadata: { role: payload.role, branchCodes }
    });
  });

  const refreshedUsers = await userRepository.listUsers(env.databaseUrl);
  const saved = refreshedUsers.find((item) => item.id === savedUserId);
  return {
    message: userId ? "User updated successfully." : "User created successfully.",
    user: saved ? toClientAdminUser(saved) : null
  };
}

async function changePassword(env, currentUser, payload) {
  const { user } = await getCurrentUserContext(env, currentUser.id);
  if (!requirePermission(user, "account.password.change")) {
    throw new AppError(403, "You do not have permission to change your password.");
  }
  if (!verifyPassword(payload.currentPassword, user.password_hash)) {
    throw new AppError(409, "Current password is incorrect.");
  }
  if (String(payload.newPassword || "").length < 8) {
    throw new AppError(400, "New password must have at least 8 characters.");
  }
  if (payload.currentPassword === payload.newPassword) {
    throw new AppError(400, "New password must be different from the current password.");
  }

  await withTransaction(env.databaseUrl, async (client) => {
    await userRepository.updatePassword(client, user.id, hashPassword(payload.newPassword));
    await auditRepository.insertAuditLog(client, {
      actorUserId: user.id,
      actorName: user.name,
      actorRole: user.role,
      branchId: user.home_branch_id || null,
      action: "PASSWORD_CHANGE",
      entityType: "user",
      entityId: user.email,
      detail: `Password changed for ${user.email}.`,
      metadata: {}
    });
  });

  return {
    message: "Password changed successfully."
  };
}

async function listVerificationQueue(env, currentUser) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  const rows = await assetRepository.listVerificationQueue(env.databaseUrl, branchScopeRows(user, branchRows), 20);
  return {
    items: rows.map(toClientVerificationQueueItem)
  };
}

async function lookupVerificationAsset(env, currentUser, tagCode) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  const asset = await assetRepository.findAssetByTagCode(env.databaseUrl, tagCode, branchScopeRows(user, branchRows));
  if (!asset) throw new AppError(404, "Tag not found in your current scope.");
  return { asset: toClientAsset(asset) };
}

async function submitVerification(env, currentUser, payload) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  const asset = await assetRepository.findAssetByTagCode(env.databaseUrl, payload.tagCode, branchScopeRows(user, branchRows));
  if (!asset) throw new AppError(404, "Tag not found for verification.");

  const updatedAsset = await withTransaction(env.databaseUrl, async (client) => {
    const row = await assetRepository.recordVerification(client, { assetId: asset.id, branchId: asset.branch_id, verifiedByUserId: user.id, outcome: payload.outcome, notes: payload.notes });
    await auditRepository.insertAuditLog(client, {
      actorUserId: user.id,
      actorName: user.name,
      actorRole: user.role,
      branchId: asset.branch_id,
      action: "PHYSICAL_VERIFY",
      entityType: "asset",
      entityId: asset.asset_id,
      detail: `Verification outcome ${payload.outcome} recorded.`,
      metadata: { tagCode: payload.tagCode, notes: payload.notes || "" }
    });
    return row;
  });

  return {
    message: "Verification submitted.",
    asset: toClientAsset({
      ...asset,
      ...updatedAsset,
      category: asset.category,
      branch_code: asset.branch_code,
      branch_name: asset.branch_name,
      city: asset.city,
      province: asset.province,
      gl_asset_account: asset.gl_asset_account,
      gl_depreciation_expense_account: asset.gl_depreciation_expense_account,
      gl_accumulated_depreciation_account: asset.gl_accumulated_depreciation_account
    })
  };
}

async function syncOfflineVerifications(env, currentUser, items = []) {
  const { user, branchRows } = await getCurrentUserContext(env, currentUser.id);
  if (!Array.isArray(items) || !items.length) {
    throw new AppError(400, "At least one offline verification item is required.");
  }

  const results = [];
  for (const item of items.slice(0, 25)) {
    const asset = await assetRepository.findAssetByTagCode(env.databaseUrl, item.tagCode, branchScopeRows(user, branchRows));
    if (!asset) {
      results.push({
        tagCode: item.tagCode,
        status: "ERROR",
        message: "Tag not found in your current scope."
      });
      continue;
    }

    await withTransaction(env.databaseUrl, async (client) => {
      await assetRepository.recordVerification(client, {
        assetId: asset.id,
        branchId: asset.branch_id,
        verifiedByUserId: user.id,
        outcome: item.outcome,
        notes: item.notes,
        isOfflineSynced: true
      });
      await auditRepository.insertAuditLog(client, {
        actorUserId: user.id,
        actorName: user.name,
        actorRole: user.role,
        branchId: asset.branch_id,
        action: "PHYSICAL_VERIFY_SYNC",
        entityType: "asset",
        entityId: asset.asset_id,
        detail: `Offline verification synced with outcome ${item.outcome}.`,
        metadata: { tagCode: item.tagCode, deviceCapturedAt: item.capturedAt || null }
      });
    });

    results.push({
      tagCode: item.tagCode,
      status: "SYNCED",
      message: "Offline verification synced successfully."
    });
  }

  return {
    message: `${results.filter((item) => item.status === "SYNCED").length} offline verifications synced.`,
    results
  };
}

async function createParallelRun(env, currentUser, payload) {
  const { user } = await getCurrentUserContext(env, currentUser.id);
  const runs = await financeRepository.listDepreciationRuns(env.databaseUrl);
  const latestRun = runs[0];
  if (!latestRun) throw new AppError(409, "No system depreciation run exists yet for comparison.");

  const sourceSnapshot = { totalDepreciationUSD: payload.legacyDepreciationUSD, totalDepreciationCDF: payload.legacyDepreciationCDF, totalAssetsProcessed: payload.legacyAssetCount };
  const systemSnapshot = { totalDepreciationUSD: Number(latestRun.total_depreciation_usd), totalDepreciationCDF: Number(latestRun.total_depreciation_cdf), totalAssetsProcessed: latestRun.total_assets_processed };
  const varianceSummary = {
    varianceUSD: sourceSnapshot.totalDepreciationUSD - systemSnapshot.totalDepreciationUSD,
    varianceCDF: sourceSnapshot.totalDepreciationCDF - systemSnapshot.totalDepreciationCDF,
    varianceAssets: sourceSnapshot.totalAssetsProcessed - systemSnapshot.totalAssetsProcessed
  };

  return withTransaction(env.databaseUrl, async (client) => {
    const created = await parallelRunRepository.createParallelRun(client, {
      period: payload.period || latestRun.period,
      sourceName: payload.sourceName || "Legacy Excel Register",
      sourceSnapshot,
      systemSnapshot,
      varianceSummary,
      status: varianceSummary.varianceUSD === 0 && varianceSummary.varianceCDF === 0 && varianceSummary.varianceAssets === 0 ? "MATCHED" : "EXCEPTION",
      createdByUserId: user.id
    });
    await auditRepository.insertAuditLog(client, {
      actorUserId: user.id,
      actorName: user.name,
      actorRole: user.role,
      branchId: user.home_branch_id || null,
      action: "PARALLEL_RUN_COMPARE",
      entityType: "parallel_run",
      entityId: created.id,
      detail: `Recorded parallel-run comparison for ${payload.period || latestRun.period}.`,
      metadata: varianceSummary
    });
    return created;
  });
}

async function getLatestParallelRun(env) {
  return parallelRunRepository.getLatestParallelRun(env.databaseUrl);
}

async function enqueueJob(env, currentUser, payload) {
  const { user } = await getCurrentUserContext(env, currentUser.id);
  return withTransaction(env.databaseUrl, async (client) => {
    const job = await jobRepository.enqueueJob(client, payload);
    await auditRepository.insertAuditLog(client, {
      actorUserId: user.id,
      actorName: user.name,
      actorRole: user.role,
      branchId: user.home_branch_id || null,
      action: "JOB_ENQUEUE",
      entityType: "job",
      entityId: job.id,
      detail: `Scheduled job ${payload.jobType}.`,
      metadata: payload
    });
    return job;
  });
}

module.exports = {
  AppError,
  login,
  meta,
  dashboard,
  listAssets,
  getAsset,
  createAsset,
  updateAsset,
  addAssetAttachment,
  createAssetWorkflow,
  previewAssetImport,
  commitAssetImport,
  listImports,
  listLifecycle,
  advanceWorkflow,
  getDepreciation,
  runDepreciation,
  approveDepreciation,
  retryDepreciationFailures,
  listApprovals,
  approveRequest,
  rejectRequest,
  getReconciliation,
  runReconciliation,
  listReports,
  generateReport,
  exportReportCsv,
  exportReportExcel,
  exportReportPrintHtml,
  listAudit,
  listUsers,
  saveUser,
  changePassword,
  lookupVerificationAsset,
  listVerificationQueue,
  submitVerification,
  syncOfflineVerifications,
  createParallelRun,
  getLatestParallelRun,
  enqueueJob,
  getCurrentUserContext
};
