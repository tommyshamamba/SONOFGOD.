const state = {
  token: null,
  user: null,
  meta: null,
  dashboard: null,
  assets: { items: [], page: 1, totalPages: 1, total: 0, pageSize: 12, scope: "all_branches" },
  assetDetail: null,
  assetEditorId: null,
  lifecycle: null,
  depreciation: null,
  approvals: { pendingCount: 0, items: [] },
  reconciliation: null,
  reports: [],
  audit: [],
  imports: { items: [], lastPreview: null },
  adminUsers: [],
  adminEditorId: null,
  verificationAsset: null,
  verificationQueue: [],
  currentSection: "dashboard"
};

const sectionTitles = {
  dashboard: "Executive Dashboard",
  assets: "Asset Registry",
  lifecycle: "Asset Lifecycle Workflows",
  depreciation: "Monthly Depreciation Engine",
  approvals: "Maker Checker Approvals",
  reconciliation: "GL Reconciliation",
  reports: "Regulatory Reporting",
  audit: "Audit Trail",
  verification: "Physical Verification",
  admin: "Admin And Security"
};

const offlineQueueKey = "pesa-offline-verification-queue";
const fallbackRoles = [
  { value: "finance_admin", label: "Finance Administrator" },
  { value: "operations", label: "Operations User" },
  { value: "admin_user", label: "Admin User" },
  { value: "auditor", label: "Auditor" },
  { value: "it_admin", label: "IT Administrator" }
];
const fallbackAttachmentTypes = ["INVOICE", "PURCHASE_ORDER", "WARRANTY", "TRANSFER_FORM", "DISPOSAL_MEMO", "IMPAIRMENT_EVIDENCE", "PHOTO", "OTHER"];
let verificationStream = null;
let verificationTimer = null;

async function api(path, options = {}) {
  const headers = { "Content-Type": "application/json", ...(options.headers || {}) };
  if (state.token) headers.Authorization = `Bearer ${state.token}`;
  const response = await fetch(path, { ...options, headers });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.error || "Request failed.");
  return payload;
}

async function safeApi(path, fallback) {
  try {
    return await api(path);
  } catch (_error) {
    return fallback;
  }
}

async function downloadAuthenticated(path, filename) {
  const headers = {};
  if (state.token) headers.Authorization = `Bearer ${state.token}`;
  const response = await fetch(path, { headers });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || "Download failed.");
  }
  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

async function openPrintableReport(path) {
  const headers = {};
  if (state.token) headers.Authorization = `Bearer ${state.token}`;
  const response = await fetch(path, { headers });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || "Printable report could not be opened.");
  }
  const html = await response.text();
  const win = window.open("", "_blank", "noopener,noreferrer");
  if (!win) throw new Error("Pop-up blocked. Allow pop-ups to open the print-ready report.");
  win.document.open();
  win.document.write(html);
  win.document.close();
}

function showToast(message, tone = "info") {
  const toast = document.createElement("div");
  toast.className = `toast ${tone}`;
  toast.textContent = message;
  document.getElementById("toastStack").appendChild(toast);
  window.setTimeout(() => toast.remove(), 3200);
}

const formatDate = (value) => value ? new Date(value).toLocaleString("en-GB", { dateStyle: "medium", timeStyle: "short" }) : "-";
const formatDateOnly = (value) => value ? new Date(value).toLocaleDateString("en-GB", { dateStyle: "medium" }) : "-";
const formatAmount = (amount, currency) => currency === "CDF" ? `CDF ${new Intl.NumberFormat("fr-CD", { maximumFractionDigits: 0 }).format(amount)}` : `$${new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(amount)}`;
const statusPill = (label, tone) => `<span class="status-pill tone-${tone || "muted"}">${label}</span>`;
const currentPermissions = () => state.user?.permissions || [];
const hasPermission = (permission) => currentPermissions().includes(permission);

function getOfflineQueue() {
  try {
    return JSON.parse(window.localStorage.getItem(offlineQueueKey) || "[]");
  } catch (_error) {
    return [];
  }
}

function saveOfflineQueue(queue) {
  window.localStorage.setItem(offlineQueueKey, JSON.stringify(queue));
}

function setSelectOptions(id, items, valueKey, labelKey, placeholder = null) {
  const select = document.getElementById(id);
  if (!select) return;
  const options = [];
  if (placeholder) options.push(`<option value="">${placeholder}</option>`);
  options.push(...items.map((item) => `<option value="${item[valueKey]}">${item[labelKey]}</option>`));
  select.innerHTML = options.join("");
}

function getSelectedValues(selectId) {
  return Array.from(document.getElementById(selectId).selectedOptions).map((option) => option.value);
}

function updateTopbar() {
  document.getElementById("pageTitle").textContent = sectionTitles[state.currentSection];
  document.getElementById("branchPill").textContent = state.user.branchName;
  document.getElementById("rolePill").textContent = state.user.roleLabel;
}

function activateSection(section) {
  state.currentSection = section;
  document.querySelectorAll(".panel-section").forEach((panel) => panel.classList.toggle("active", panel.id === `section-${section}`));
  document.querySelectorAll(".nav-button").forEach((button) => button.classList.toggle("active", button.dataset.section === section));
  updateTopbar();
}

function renderDemoCredentials(data) {
  document.getElementById("demoCredentials").innerHTML = data.demoUsers.map((user) => `<article class="credential-card"><strong>${user.role}</strong><div>${user.email} / ${user.password}</div><div>${user.branchName}</div></article>`).join("");
}

function renderUserPanel() {
  document.getElementById("userPanel").innerHTML = `<strong>${state.user.name}</strong><p>${state.user.title}</p><p>${state.user.email}</p>`;
}

function populateFilters() {
  const roles = state.meta.roles || fallbackRoles;
  const attachmentTypes = state.meta.attachmentTypes || fallbackAttachmentTypes;
  setSelectOptions("assetBranchFilter", state.meta.branches, "code", "name", "All branches");
  setSelectOptions("assetCategoryFilter", state.meta.categories, "key", "label", "All categories");
  setSelectOptions("assetStatusFilter", state.meta.statuses, "status", "label", "All statuses");
  setSelectOptions("assetFormCategory", state.meta.categories, "key", "label");
  setSelectOptions("assetFormBranch", state.meta.branches, "code", "name");
  setSelectOptions("assetFormStatus", state.meta.statuses, "status", "label");
  setSelectOptions("assetWorkflowBranch", state.meta.branches, "code", "name", "Select destination");
  setSelectOptions("assetAttachmentType", attachmentTypes.map((item) => ({ value: item, label: item.replaceAll("_", " ") })), "value", "label");
  setSelectOptions("adminUserRole", roles, "value", "label");
  setSelectOptions("adminUserHomeBranch", state.meta.branches, "code", "name", "Optional home branch");
  document.getElementById("adminUserBranchAccess").innerHTML = state.meta.branches.map((branch) => `<option value="${branch.code}">${branch.name}</option>`).join("");
}

function populateAssetForm(asset = null) {
  state.assetEditorId = asset?.assetId || null;
  document.getElementById("assetFormAssetId").value = asset?.assetId || "";
  document.getElementById("assetFormTagCode").value = asset?.tagCode || "";
  document.getElementById("assetFormName").value = asset?.name || "";
  document.getElementById("assetFormCategory").value = asset?.categoryKey || state.meta.categories[0]?.key || "";
  document.getElementById("assetFormBranch").value = asset?.branchCode || state.meta.branches[0]?.code || "";
  document.getElementById("assetFormCurrency").value = asset?.currency || "USD";
  document.getElementById("assetFormCost").value = asset?.acquisitionCost || "";
  document.getElementById("assetFormResidual").value = asset?.residualValue || "";
  document.getElementById("assetFormCapitalisationDate").value = asset?.capitalisationDate || "";
  document.getElementById("assetFormUsefulLife").value = asset?.usefulLifeMonths || "";
  document.getElementById("assetFormMethod").value = asset?.depreciationMethod || "SLM";
  document.getElementById("assetFormStatus").value = asset?.status || "ACTIVE";
  document.getElementById("assetFormDescription").value = asset?.description || "";
  document.getElementById("assetFormPurchaseOrderRef").value = asset?.purchaseOrderRef || "";
  document.getElementById("assetFormWarrantyExpiryDate").value = asset?.warrantyExpiryDate || "";
  document.getElementById("assetFormMode").textContent = state.assetEditorId ? `Edit mode for ${asset.assetId}` : "Create mode";
  document.getElementById("assetFormSubmitButton").textContent = state.assetEditorId ? "Update Asset" : "Save Asset";
}

function populateAdminUserForm(user = null) {
  state.adminEditorId = user?.id || null;
  document.getElementById("adminUserEmail").value = user?.email || "";
  document.getElementById("adminUserEmail").disabled = Boolean(user);
  document.getElementById("adminUserName").value = user?.name || "";
  const roles = state.meta.roles || fallbackRoles;
  document.getElementById("adminUserRole").value = user?.role || roles[0]?.value || "operations";
  document.getElementById("adminUserHomeBranch").value = user?.branchCode || "";
  document.getElementById("adminUserPhoneNumber").value = user?.phoneNumber || "";
  document.getElementById("adminUserPassword").value = "";
  document.getElementById("adminUserActive").value = String(user?.isActive ?? true);
  const selected = new Set((user?.branchAccess || []).map((branch) => branch.code));
  Array.from(document.getElementById("adminUserBranchAccess").options).forEach((option) => {
    option.selected = selected.has(option.value);
  });
  document.getElementById("adminUserFormMode").textContent = state.adminEditorId ? `Edit mode for ${user.email}` : "Create mode";
}

function setDisabledWithin(containerId, disabled) {
  document.querySelectorAll(`#${containerId} input, #${containerId} select, #${containerId} textarea, #${containerId} button`).forEach((element) => {
    element.disabled = disabled;
  });
}

function applyRoleVisibility() {
  const adminVisible = hasPermission("admin.users.read") || hasPermission("admin.users.manage");
  document.querySelector('.nav-button[data-section="admin"]').classList.toggle("hidden", !adminVisible);
  if (!adminVisible && state.currentSection === "admin") activateSection("dashboard");

  const canManageAssets = hasPermission("assets.create") || hasPermission("assets.create.branch") || hasPermission("assets.update") || hasPermission("assets.update.branch");
  setDisabledWithin("assetForm", !canManageAssets);
  document.getElementById("assetFormResetButton").disabled = !canManageAssets;

  const canImport = hasPermission("assets.import") || hasPermission("assets.import.branch") || hasPermission("assets.create") || hasPermission("assets.create.branch");
  document.getElementById("importCsvInput").disabled = !canImport;
  document.getElementById("importPreviewButton").disabled = !canImport;
  document.getElementById("importCommitButton").disabled = !canImport;
}

function renderDashboard() {
  if (!state.dashboard) return;
  const { kpis, alerts } = state.dashboard;
  const cards = [
    { title: "Total Assets", value: new Intl.NumberFormat().format(kpis.totalAssets), meta: "Assets currently visible in your scope" },
    { title: "Total NBV (USD)", value: formatAmount(kpis.totalUSDNBV, "USD"), meta: "Core ledger-aligned view" },
    { title: "Total NBV (CDF)", value: formatAmount(kpis.totalCDFNBV, "CDF"), meta: "Translated for local reporting" },
    { title: "Pending Actions", value: new Intl.NumberFormat().format(kpis.pendingActions), meta: "Transfers, tagging, disposal, impairment" }
  ];
  document.getElementById("kpiGrid").innerHTML = cards.map((card) => `<article class="kpi-card"><div class="eyebrow">${card.title}</div><div class="kpi-value">${card.value}</div><div class="kpi-meta">${card.meta}</div></article>`).join("");

  const alertCards = [
    { label: "Overdue approvals", value: alerts?.overdueApprovals || 0, tone: "warning" },
    { label: "Failed posting lines", value: alerts?.failedPostingLines || 0, tone: "danger" },
    { label: "Fully depreciated assets", value: alerts?.fullyDepreciatedAssets || 0, tone: "info" },
    { label: "Unverified assets", value: alerts?.unverifiedAssets || 0, tone: "amber" }
  ];
  document.getElementById("dashboardAlerts").innerHTML = alertCards.map((item) => `<div class="summary-stat"><span>${item.label}</span><strong>${new Intl.NumberFormat().format(item.value)}</strong>${statusPill(item.value > 0 ? "Action needed" : "Under control", item.value > 0 ? item.tone : "success")}</div>`).join("");

  const trend = state.dashboard.depreciationTrend.length ? state.dashboard.depreciationTrend : [{ period: "N/A", totalUSD: 1, totalCDF: 1 }];
  const maxUSD = Math.max(...trend.map((point) => point.totalUSD || 1), 1);
  const maxCDF = Math.max(...trend.map((point) => point.totalCDF || 1), 1);
  document.getElementById("depreciationTrend").innerHTML = trend.map((point) => `<div class="bar-column"><div class="bar-stack"><span class="bar-usd" style="height:${Math.max(12, Math.round((point.totalUSD || 1) / maxUSD * 110))}px"></span><span class="bar-cdf" style="height:${Math.max(12, Math.round((point.totalCDF || 1) / maxCDF * 110))}px"></span></div><div class="bar-label">${point.period === "N/A" ? "-" : point.period.slice(5)}</div></div>`).join("");

  const colors = ["#10285d", "#c9a84c", "#2563eb", "#dc2626", "#7c3aed", "#d97706", "#94a3b8"];
  const total = Math.max(state.dashboard.statusBreakdown.reduce((sum, item) => sum + item.count, 0), 1);
  let cursor = 0;
  const slices = state.dashboard.statusBreakdown.map((item, index) => {
    const next = cursor + item.count / total * 360;
    const slice = `${colors[index]} ${cursor.toFixed(1)}deg ${next.toFixed(1)}deg`;
    cursor = next;
    return slice;
  });
  document.getElementById("statusRing").style.background = `conic-gradient(${slices.join(", ")})`;
  document.getElementById("statusLegend").innerHTML = state.dashboard.statusBreakdown.map((item, index) => `<div class="legend-row"><span class="legend-label"><span class="legend-dot" style="background:${colors[index]}"></span>${item.label}</span><strong>${new Intl.NumberFormat().format(item.count)}</strong></div>`).join("");
  document.getElementById("activityFeed").innerHTML = state.dashboard.recentActivity.map((item) => `<article class="activity-item"><div class="activity-title">${item.title}</div><div>${item.detail}</div><div class="activity-meta">${item.user} | ${formatDate(item.timestamp)}</div></article>`).join("");
  document.getElementById("branchTable").innerHTML = state.dashboard.branchSummary.map((branch) => `<tr><td>${branch.branchName}</td><td>${new Intl.NumberFormat().format(branch.assetCount)}</td><td>${branch.pendingActions}</td><td>${branch.verificationCoverage}%</td></tr>`).join("");
  document.getElementById("dashboardVerificationQueue").innerHTML = (state.dashboard.verificationQueue || []).length
    ? state.dashboard.verificationQueue.map((item) => `<tr><td>${item.assetId}<div class="kanban-note">${item.name}</div></td><td>${item.branchName}</td><td>${formatDateOnly(item.lastVerifiedAt)}</td><td>${statusPill(item.statusLabel, item.status === "ACTIVE" ? "success" : "warning")}</td></tr>`).join("")
    : `<tr><td colspan="4">No overdue verification items are visible in your scope.</td></tr>`;
  document.getElementById("dashboardImportTable").innerHTML = (state.dashboard.importBatches || []).length
    ? state.dashboard.importBatches.map((batch) => `<tr><td>${batch.id.slice(0, 8)}</td><td>${statusPill(batch.status, batch.status === "IMPORTED" ? "success" : batch.status === "FAILED" ? "danger" : "warning")}</td><td>${batch.createdByName || "-"}</td><td>${batch.summary.totalRows || 0} rows / ${batch.summary.validRows || 0} valid</td></tr>`).join("")
    : `<tr><td colspan="4">No import batches captured yet.</td></tr>`;
}

function renderAssets() {
  document.getElementById("assetScopeNote").textContent = state.assets.scope === "branch_only" ? "Scoped to your branch role" : "All branches visible";
  document.getElementById("assetTable").innerHTML = state.assets.items.map((asset) => `<tr class="asset-row" data-asset-id="${asset.assetId}"><td>${asset.assetId}</td><td>${asset.tagCode}</td><td>${asset.name}</td><td>${asset.category}</td><td>${asset.branchName}</td><td>${asset.currency}</td><td>${formatAmount(asset.acquisitionCost, asset.currency)}</td><td>${formatAmount(asset.netBookValue, asset.currency)}</td><td>${statusPill(asset.statusLabel, asset.roleTone)}</td></tr>`).join("");
  document.getElementById("assetPaginationLabel").textContent = `Page ${state.assets.page} of ${state.assets.totalPages} | ${new Intl.NumberFormat().format(state.assets.total)} assets`;
}

function drawPseudoQr(value) {
  const chars = value.split("");
  document.getElementById("pseudoQr").innerHTML = Array.from({ length: 441 }, (_, index) => {
    const code = chars[index % chars.length].charCodeAt(0);
    const active = (code + index * 7 + index % 3) % 5 < 2;
    return `<span class="qr-dot ${active ? "active" : ""}"></span>`;
  }).join("");
}

function renderAssetAttachments(detail) {
  document.getElementById("assetAttachmentTable").innerHTML = (detail.attachments || []).length
    ? detail.attachments.map((item) => `<tr><td>${item.attachmentType.replaceAll("_", " ")}</td><td>${item.referenceUrl ? `<a href="${item.referenceUrl}" target="_blank" rel="noreferrer">${item.fileName}</a>` : item.fileName}${item.note ? `<div class="kanban-note">${item.note}</div>` : ""}</td><td>${item.uploadedByName}</td></tr>`).join("")
    : `<tr><td colspan="3">No supporting evidence has been added yet.</td></tr>`;
}

function openAssetModal(detail) {
  state.assetDetail = detail;
  const asset = detail.asset;
  document.getElementById("assetModalTitle").textContent = asset.name;
  document.getElementById("qrTag").textContent = asset.tagCode;
  document.getElementById("qrName").textContent = asset.name;
  document.getElementById("qrBranch").textContent = `${asset.branchName} | ${asset.city}`;
  document.getElementById("qrFooter").textContent = asset.assetId;
  drawPseudoQr(asset.tagCode);
  document.getElementById("assetOverview").innerHTML = [["Category", asset.category], ["Branch", asset.branchName], ["Currency", asset.currency], ["Cost", formatAmount(asset.acquisitionCost, asset.currency)], ["NBV", formatAmount(asset.netBookValue, asset.currency)], ["Status", asset.statusLabel], ["Useful Life", `${asset.usefulLifeMonths} months`], ["Method", asset.depreciationMethod], ["Capitalisation", asset.capitalisationDate], ["GL Asset Account", asset.glAssetAccount]].map(([label, value]) => `<div class="detail-item"><span>${label}</span><strong>${value || "-"}</strong></div>`).join("");
  document.getElementById("assetSchedule").innerHTML = detail.depreciationSchedule.map((line) => `<tr><td>${line.period}</td><td>${formatAmount(line.openingNBV, asset.currency)}</td><td>${formatAmount(line.depreciationCharge, asset.currency)}</td><td>${formatAmount(line.closingNBV, asset.currency)}</td></tr>`).join("");
  renderAssetAttachments(detail);
  document.getElementById("assetWorkflowBranch").value = "";
  document.getElementById("assetWorkflowNotes").value = "";
  document.getElementById("assetModal").classList.remove("hidden");
}

function renderLifecycle() {
  const columns = [
    { key: "pendingTransfers", title: "Pending Transfers" },
    { key: "inTransit", title: "In Transit" },
    { key: "pendingDisposals", title: "Pending Disposals" },
    { key: "pendingImpairments", title: "Pending Impairments" }
  ];
  document.getElementById("kanbanBoard").innerHTML = columns.map((column) => {
    const cards = state.lifecycle.columns[column.key] || [];
    return `<article class="kanban-column"><h3>${column.title}</h3><div class="kanban-note">${cards.length} items</div>${cards.map((card) => `<div class="kanban-card"><strong>${card.title}</strong><div>${card.asset.name}</div><div class="kanban-note">${card.asset.assetId} | ${card.asset.branchName}</div><div class="kanban-note">${card.note || ""}</div>${card.approvalState === "PENDING" ? `<div class="approval-card-note">${statusPill(card.approvalLabel || "Pending approval", "warning")}<span class="inline-note">Checker action is still pending for this workflow.</span></div>` : card.nextColumn !== "completed" ? `<button class="secondary-button lifecycle-action" data-workflow-id="${card.id}">${card.actionLabel}</button>` : statusPill("Completed in current flow", "success")}</div>`).join("")}</article>`;
  }).join("");
}

function renderDepreciation() {
  const run = state.depreciation.currentRun;
  if (!run) {
    document.getElementById("depreciationPanel").innerHTML = "<div class=\"inline-note\">No depreciation run is available yet.</div>";
    document.getElementById("depreciationHistory").innerHTML = "";
    document.getElementById("depreciationExceptions").innerHTML = `<tr><td colspan="6">No failed depreciation lines are currently waiting for retry.</td></tr>`;
    return;
  }
  const approval = state.depreciation.approvalRequest;
  const approvalHtml = approval ? `<div>${statusPill(approval.statusLabel, approval.statusTone)}</div><div class="inline-note">Submitted by ${approval.requestedByName} on ${formatDate(approval.requestedAt)}.</div>` : `<div class="inline-note">No active approval request for this run.</div>`;
  document.getElementById("depreciationPanel").innerHTML = `<div>${statusPill(run.statusLabel || run.status, run.status === "POSTED" ? "success" : run.status === "PENDING_APPROVAL" ? "warning" : run.status === "REJECTED" ? "danger" : "info")}</div><h3>${run.period} monthly run</h3><p>${run.summary}</p>${approvalHtml}<div class="summary-grid"><div class="summary-stat"><span>Assets</span><strong>${new Intl.NumberFormat().format(run.totalAssetsProcessed)}</strong></div><div class="summary-stat"><span>Total USD</span><strong>${formatAmount(run.totalDepreciationUSD, "USD")}</strong></div><div class="summary-stat"><span>Total CDF</span><strong>${formatAmount(run.totalDepreciationCDF, "CDF")}</strong></div><div class="summary-stat"><span>Rate</span><strong>${new Intl.NumberFormat().format(run.exchangeRateUsed)}</strong></div></div><div class="kanban-note">Failures currently held for review: ${run.failureCount}</div>`;
  document.getElementById("depreciationHistory").innerHTML = state.depreciation.history.map((item) => `<tr><td>${item.period}</td><td>${statusPill(item.statusLabel || item.status, item.status === "POSTED" ? "success" : item.status === "PENDING_APPROVAL" ? "warning" : item.status === "REJECTED" ? "danger" : "info")}</td><td>${new Intl.NumberFormat().format(item.totalAssetsProcessed)}</td><td>${formatAmount(item.totalDepreciationUSD, "USD")}</td><td>${formatAmount(item.totalDepreciationCDF, "CDF")}</td><td>${item.failureCount}</td></tr>`).join("");
  document.getElementById("depreciationExceptions").innerHTML = (state.depreciation.postingExceptions || []).length ? state.depreciation.postingExceptions.map((item) => `<tr><td>${item.assetId}<div class="kanban-note">${item.assetName}</div></td><td>${item.branchName}</td><td>${formatAmount(item.depreciationCharge, item.currency)}</td><td>${statusPill(item.postingStatus, item.postingStatus === "FAILED" ? "danger" : "success")}</td><td>${item.retryCount}</td><td>${item.failureReason || "-"}</td></tr>`).join("") : `<tr><td colspan="6">No failed depreciation lines are currently waiting for retry.</td></tr>`;
  document.getElementById("runDepreciationButton").disabled = !hasPermission("depreciation.run");
  document.getElementById("approveDepreciationButton").disabled = !hasPermission("depreciation.approve") || !["PENDING_APPROVAL", "DRAFT"].includes(run.status);
  document.getElementById("retryFailuresButton").disabled = !hasPermission("depreciation.retry_failures") || !(state.depreciation.postingExceptions || []).length;
}

function renderReconciliation() {
  document.getElementById("reconciliationSummary").innerHTML = [["Status", state.reconciliation.status], ["Variance USD", formatAmount(state.reconciliation.varianceUSD, "USD")], ["Variance CDF", formatAmount(state.reconciliation.varianceCDF, "CDF")], ["Last Run", formatDate(state.reconciliation.lastRunAt)]].map(([label, value]) => `<div class="summary-stat"><span>${label}</span><strong>${value}</strong></div>`).join("");
  document.getElementById("reconciliationTable").innerHTML = state.reconciliation.accounts.map((account) => `<tr><td>${account.glCode}</td><td>${account.label}</td><td>${formatAmount(account.varianceUSD, "USD")}</td><td>${formatAmount(account.varianceCDF, "CDF")}</td><td>${statusPill(account.status, account.status === "MATCHED" ? "success" : "warning")}</td></tr>`).join("");
}

function renderReports() {
  document.getElementById("reportGrid").innerHTML = state.reports.map((report) => `<article class="report-card"><div class="eyebrow">${report.owner}</div><h3>${report.title}</h3><p>${report.description}</p><div class="action-row"><button class="secondary-button report-generate" data-report-id="${report.id}">Preview</button><button class="ghost-button report-download" data-report-id="${report.id}" data-format="csv">CSV</button><button class="ghost-button report-download" data-report-id="${report.id}" data-format="excel">Excel</button><button class="ghost-button report-download" data-report-id="${report.id}" data-format="print">Print Pack</button></div></article>`).join("");
}

function renderReportPreview(payload) {
  document.getElementById("reportPreviewCard").classList.remove("hidden");
  document.getElementById("reportPreviewTitle").textContent = payload.report.title;
  document.getElementById("reportSummaryGrid").innerHTML = (payload.summary || []).map((item) => `<div class="summary-stat"><span>${item.label}</span><strong>${item.value}</strong></div>`).join("");
  document.getElementById("reportPreviewHead").innerHTML = `<tr>${payload.preview.columns.map((column) => `<th>${column}</th>`).join("")}</tr>`;
  document.getElementById("reportPreviewBody").innerHTML = payload.preview.rows.map((row) => `<tr>${row.map((cell) => `<td>${cell}</td>`).join("")}</tr>`).join("");
  document.getElementById("reportActionRow").innerHTML = `<button class="ghost-button report-download" data-report-id="${payload.report.id}" data-format="csv">Download CSV</button><button class="ghost-button report-download" data-report-id="${payload.report.id}" data-format="excel">Open in Excel</button><button class="secondary-button report-download" data-report-id="${payload.report.id}" data-format="print">Open Print Pack</button>`;
}

function renderAudit() {
  const actions = [...new Set(state.audit.map((item) => item.action))];
  document.getElementById("auditActionFilter").innerHTML = [`<option value="">All actions</option>`].concat(actions.map((action) => `<option value="${action}">${action}</option>`)).join("");
  document.getElementById("auditTable").innerHTML = state.audit.map((log) => `<tr><td>${formatDate(log.timestamp)}</td><td>${log.user}</td><td>${log.action}</td><td>${log.entity_id || log.entity || "-"}</td><td>${log.detail}</td></tr>`).join("");
}

function renderApprovals() {
  document.getElementById("approvalSummary").textContent = `${state.approvals.pendingCount} pending`;
  const canDecide = hasPermission("approvals.decide");
  document.getElementById("approvalTable").innerHTML = state.approvals.items.length ? state.approvals.items.map((item) => `<tr><td>${item.entityLabel}<div class="kanban-note">${item.title}</div></td><td>${item.actionLabel}</td><td>${item.requestedByName}<div class="kanban-note">${item.requestedByRole}</div></td><td>${item.branchName}</td><td>${statusPill(item.statusLabel, item.statusTone)}</td><td>${formatDate(item.requestedAt)}</td><td>${item.status === "PENDING" && canDecide ? `<div class="decision-stack"><button class="secondary-button approval-approve" data-approval-id="${item.id}">Approve</button><button class="ghost-button approval-reject" data-approval-id="${item.id}">Reject</button></div>` : `<span class="inline-note">${item.decisionNotes || item.statusLabel}</span>`}</td></tr>`).join("") : `<tr><td colspan="7">No approval requests are currently visible in your scope.</td></tr>`;
}

function renderImportPreview() {
  const preview = state.imports.lastPreview;
  if (!preview) {
    document.getElementById("importPreviewTable").innerHTML = `<tr><td colspan="5">No preview run yet.</td></tr>`;
    document.getElementById("importSummaryNote").textContent = "Run a preview before importing.";
    return;
  }
  document.getElementById("importSummaryNote").textContent = `${preview.batch.summary.validRows || 0} valid rows and ${preview.batch.summary.errorRows || 0} error rows in batch ${preview.batch.id.slice(0, 8)}.`;
  document.getElementById("importPreviewTable").innerHTML = preview.rows.map((row) => `<tr><td>${row.rowNumber}</td><td>${row.assetId || "-"}</td><td>${row.tagCode || "-"}</td><td>${statusPill(row.status, row.status === "VALID" ? "success" : row.status === "IMPORTED" ? "info" : "danger")}</td><td>${row.message || "Ready to import."}</td></tr>`).join("");
}

function renderImportHistory() {
  document.getElementById("importHistoryTable").innerHTML = state.imports.items.length ? state.imports.items.map((batch) => `<tr><td>${batch.id.slice(0, 8)}<div class="kanban-note">${formatDate(batch.createdAt)}</div></td><td>${statusPill(batch.status, batch.status === "IMPORTED" ? "success" : batch.status === "FAILED" ? "danger" : "warning")}</td><td>${batch.createdByName || "-"}</td><td>${batch.summary.totalRows || 0} rows / ${batch.summary.validRows || 0} valid / ${batch.summary.importedRows || 0} imported</td></tr>`).join("") : `<tr><td colspan="4">No import batches available yet.</td></tr>`;
}

function renderVerificationResult() {
  const container = document.getElementById("verificationResult");
  if (!state.verificationAsset) {
    container.classList.add("hidden");
    document.getElementById("verificationActions").classList.add("hidden");
    document.getElementById("verificationNotesField").classList.add("hidden");
    return;
  }
  const asset = state.verificationAsset;
  container.classList.remove("hidden");
  document.getElementById("verificationActions").classList.remove("hidden");
  document.getElementById("verificationNotesField").classList.remove("hidden");
  container.innerHTML = `<div class="verification-asset"><strong>${asset.name}</strong><div>${asset.tagCode}</div><div>${asset.branchName}</div><div>Last verified: ${formatDateOnly(asset.lastVerifiedAt)}</div>${statusPill(asset.statusLabel, asset.roleTone)}</div>`;
}

function renderVerificationQueue() {
  document.getElementById("verificationQueueTable").innerHTML = state.verificationQueue.length ? state.verificationQueue.map((item) => `<tr><td>${item.assetId}<div class="kanban-note">${item.tagCode}</div></td><td>${item.branchName}</td><td>${formatDateOnly(item.lastVerifiedAt)}</td><td>${statusPill(item.statusLabel, item.status === "ACTIVE" ? "success" : "warning")}</td></tr>`).join("") : `<tr><td colspan="4">Verification queue is empty in your scope.</td></tr>`;
}

function renderOfflineQueue() {
  const queue = getOfflineQueue();
  document.getElementById("offlineQueueTable").innerHTML = queue.length ? queue.map((item) => `<tr><td>${item.tagCode}</td><td>${item.outcome}</td><td>${formatDate(item.capturedAt)}</td><td>${item.notes || "-"}</td></tr>`).join("") : `<tr><td colspan="4">Offline queue is empty.</td></tr>`;
}

function renderAdminUsers() {
  document.getElementById("adminUserTable").innerHTML = state.adminUsers.length ? state.adminUsers.map((user) => `<tr class="admin-user-row" data-user-id="${user.id}"><td>${user.name}<div class="kanban-note">${user.email}</div></td><td>${user.roleLabel}</td><td>${user.branchName}</td><td>${user.branchAccess.map((branch) => branch.code).join(", ") || "-"}</td><td>${formatDate(user.lastLoginAt)}</td><td>${statusPill(user.isActive ? "Active" : "Inactive", user.isActive ? "success" : "danger")}</td></tr>`).join("") : `<tr><td colspan="6">No users available or you do not have admin permission.</td></tr>`;
}

async function loadAssets(page = state.assets.page) {
  const params = new URLSearchParams({ page: String(page), pageSize: String(state.assets.pageSize), search: document.getElementById("assetSearch").value, status: document.getElementById("assetStatusFilter").value, branch: document.getElementById("assetBranchFilter").value, category: document.getElementById("assetCategoryFilter").value });
  state.assets = await api(`/api/assets?${params.toString()}`);
  renderAssets();
}

async function refreshAudit() {
  const params = new URLSearchParams({ user: document.getElementById("auditUserFilter").value, action: document.getElementById("auditActionFilter").value });
  state.audit = (await api(`/api/audit?${params.toString()}`)).items;
  renderAudit();
}

async function refreshApprovals() {
  state.approvals = await safeApi("/api/approvals", { pendingCount: 0, items: [] });
  renderApprovals();
}

async function refreshImports() {
  state.imports.items = (await safeApi("/api/imports", { items: [] })).items || [];
  renderImportHistory();
}

async function refreshVerificationQueue() {
  state.verificationQueue = (await safeApi("/api/verification/queue", { items: [] })).items || [];
  renderVerificationQueue();
}

async function refreshAdminUsers() {
  state.adminUsers = (await safeApi("/api/admin/users", { items: [] })).items || [];
  renderAdminUsers();
}

async function refreshDashboard() {
  state.dashboard = await api("/api/dashboard");
  renderDashboard();
}

async function loadAllData() {
  const [meta, dashboard, assets, lifecycle, depreciation, approvals, reconciliation, reports, audit, imports, verificationQueue, adminUsers] = await Promise.all([
    api("/api/meta"),
    api("/api/dashboard"),
    api("/api/assets?page=1&pageSize=12"),
    api("/api/lifecycle"),
    api("/api/depreciation"),
    safeApi("/api/approvals", { pendingCount: 0, items: [] }),
    api("/api/reconciliation"),
    api("/api/reports"),
    api("/api/audit"),
    safeApi("/api/imports", { items: [] }),
    safeApi("/api/verification/queue", { items: [] }),
    safeApi("/api/admin/users", { items: [] })
  ]);
  state.meta = meta;
  state.user = meta.user;
  state.dashboard = dashboard;
  state.assets = assets;
  state.lifecycle = lifecycle;
  state.depreciation = depreciation;
  state.approvals = approvals;
  state.reconciliation = reconciliation;
  state.reports = reports.reports;
  state.audit = audit.items;
  state.imports.items = imports.items || [];
  state.verificationQueue = verificationQueue.items || [];
  state.adminUsers = adminUsers.items || [];
  renderUserPanel();
  populateFilters();
  populateAssetForm();
  populateAdminUserForm();
  renderDashboard();
  renderAssets();
  renderLifecycle();
  renderDepreciation();
  renderApprovals();
  renderReconciliation();
  renderReports();
  renderAudit();
  renderImportPreview();
  renderImportHistory();
  renderVerificationResult();
  renderVerificationQueue();
  renderOfflineQueue();
  renderAdminUsers();
  applyRoleVisibility();
  updateTopbar();
}

async function handleLogin(event) {
  event.preventDefault();
  const error = document.getElementById("loginError");
  try {
    error.classList.add("hidden");
    const payload = await api("/api/auth/login", { method: "POST", body: JSON.stringify({ email: document.getElementById("emailInput").value, password: document.getElementById("passwordInput").value }) });
    state.token = payload.token;
    document.getElementById("loginView").classList.add("hidden");
    document.getElementById("appView").classList.remove("hidden");
    await loadAllData();
    showToast(`Signed in as ${payload.user.roleLabel}`, "success");
  } catch (err) {
    error.textContent = err.message;
    error.classList.remove("hidden");
  }
}

function readAssetFormPayload() {
  const usefulLifeValue = document.getElementById("assetFormUsefulLife").value;
  return {
    assetId: document.getElementById("assetFormAssetId").value.trim(),
    tagCode: document.getElementById("assetFormTagCode").value.trim(),
    name: document.getElementById("assetFormName").value.trim(),
    description: document.getElementById("assetFormDescription").value.trim(),
    categoryKey: document.getElementById("assetFormCategory").value,
    branchCode: document.getElementById("assetFormBranch").value,
    currency: document.getElementById("assetFormCurrency").value,
    acquisitionCost: Number(document.getElementById("assetFormCost").value || 0),
    residualValue: document.getElementById("assetFormResidual").value === "" ? "" : Number(document.getElementById("assetFormResidual").value),
    capitalisationDate: document.getElementById("assetFormCapitalisationDate").value,
    usefulLifeMonths: usefulLifeValue === "" ? undefined : Number(usefulLifeValue),
    depreciationMethod: document.getElementById("assetFormMethod").value,
    status: document.getElementById("assetFormStatus").value,
    purchaseOrderRef: document.getElementById("assetFormPurchaseOrderRef").value.trim(),
    warrantyExpiryDate: document.getElementById("assetFormWarrantyExpiryDate").value
  };
}

async function submitAssetForm(event) {
  event.preventDefault();
  const endpoint = state.assetEditorId ? `/api/assets/${encodeURIComponent(state.assetEditorId)}` : "/api/assets";
  const method = state.assetEditorId ? "PATCH" : "POST";
  const response = await api(endpoint, { method, body: JSON.stringify(readAssetFormPayload()) });
  populateAssetForm(response.asset);
  await loadAssets(1);
  await refreshDashboard();
  showToast(response.message, "success");
}

async function previewImport() {
  state.imports.lastPreview = await api("/api/assets/import-preview", { method: "POST", body: JSON.stringify({ csvText: document.getElementById("importCsvInput").value }) });
  renderImportPreview();
  await refreshImports();
  await refreshDashboard();
  showToast("Import preview generated.", "success");
}

async function commitImport() {
  const batchId = state.imports.lastPreview?.batch?.id;
  if (!batchId) throw new Error("Run an import preview first.");
  const response = await api("/api/assets/import-commit", { method: "POST", body: JSON.stringify({ batchId }) });
  state.imports.lastPreview = null;
  renderImportPreview();
  await refreshImports();
  await loadAssets(1);
  await refreshDashboard();
  showToast(response.message, "success");
}

async function loadAssetDetail(assetId) {
  const detail = await api(`/api/assets/${encodeURIComponent(assetId)}`);
  openAssetModal(detail);
  populateAssetForm(detail.asset);
  activateSection("assets");
}

async function submitAssetAttachment(event) {
  event.preventDefault();
  if (!state.assetDetail) throw new Error("Open an asset first.");
  const response = await api(`/api/assets/${encodeURIComponent(state.assetDetail.asset.assetId)}/attachments`, {
    method: "POST",
    body: JSON.stringify({
      attachmentType: document.getElementById("assetAttachmentType").value,
      fileName: document.getElementById("assetAttachmentFileName").value.trim(),
      referenceUrl: document.getElementById("assetAttachmentReferenceUrl").value.trim(),
      note: document.getElementById("assetAttachmentNote").value.trim()
    })
  });
  document.getElementById("assetAttachmentFileName").value = "";
  document.getElementById("assetAttachmentReferenceUrl").value = "";
  document.getElementById("assetAttachmentNote").value = "";
  state.assetDetail.attachments = [response.attachment, ...(state.assetDetail.attachments || [])];
  renderAssetAttachments(state.assetDetail);
  showToast(response.message, "success");
}

async function submitAssetWorkflow(event) {
  event.preventDefault();
  if (!state.assetDetail) throw new Error("Open an asset first.");
  const response = await api(`/api/assets/${encodeURIComponent(state.assetDetail.asset.assetId)}/workflows`, {
    method: "POST",
    body: JSON.stringify({
      workflowType: document.getElementById("assetWorkflowType").value,
      toBranchCode: document.getElementById("assetWorkflowBranch").value,
      notes: document.getElementById("assetWorkflowNotes").value.trim()
    })
  });
  document.getElementById("assetWorkflowNotes").value = "";
  state.lifecycle = await api("/api/lifecycle");
  await refreshApprovals();
  renderLifecycle();
  showToast(response.message, "success");
}

async function lookupVerificationAsset() {
  const tagCode = document.getElementById("verificationTagInput").value.trim();
  if (!tagCode) throw new Error("Enter a tag code to continue.");
  state.verificationAsset = (await api(`/api/verification/lookup/${encodeURIComponent(tagCode)}`)).asset;
  renderVerificationResult();
}

function queueOfflineVerification(payload) {
  const queue = getOfflineQueue();
  queue.unshift({ ...payload, capturedAt: new Date().toISOString() });
  saveOfflineQueue(queue.slice(0, 50));
  renderOfflineQueue();
}

async function submitVerification(outcome) {
  if (!state.verificationAsset) return;
  const payload = { tagCode: state.verificationAsset.tagCode, outcome, notes: document.getElementById("verificationNotes").value };
  try {
    const response = await api("/api/verification/submit", { method: "POST", body: JSON.stringify(payload) });
    state.verificationAsset = response.asset;
    renderVerificationResult();
    await refreshDashboard();
    await refreshVerificationQueue();
    showToast(response.message, "success");
  } catch (error) {
    queueOfflineVerification(payload);
    showToast(`Saved offline instead: ${error.message}`, "info");
  }
}

async function syncOfflineQueue() {
  const queue = getOfflineQueue();
  if (!queue.length) return showToast("Offline queue is already empty.", "info");
  const response = await api("/api/verification/sync", { method: "POST", body: JSON.stringify({ items: queue }) });
  const failedTags = new Set(response.results.filter((item) => item.status !== "SYNCED").map((item) => item.tagCode));
  saveOfflineQueue(queue.filter((item) => failedTags.has(item.tagCode)));
  renderOfflineQueue();
  await refreshDashboard();
  await refreshVerificationQueue();
  showToast(response.message, "success");
}

async function startScanner() {
  if (!navigator.mediaDevices?.getUserMedia) return showToast("Camera access is not supported in this browser.", "error");
  try {
    verificationStream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: "environment" } });
    const video = document.getElementById("verificationVideo");
    video.srcObject = verificationStream;
    video.classList.remove("hidden");
    document.getElementById("scannerPlaceholderText").textContent = "Point the camera at a QR or barcode tag.";
    if (!("BarcodeDetector" in window)) return showToast("BarcodeDetector is not available here. Use manual tag entry if auto-scan does not start.", "info");
    const detector = new window.BarcodeDetector({ formats: ["qr_code", "code_128", "ean_13"] });
    verificationTimer = window.setInterval(async () => {
      if (!verificationStream) return;
      try {
        const codes = await detector.detect(video);
        if (codes[0]?.rawValue) {
          document.getElementById("verificationTagInput").value = codes[0].rawValue;
          await lookupVerificationAsset();
          stopScanner();
          showToast("Tag detected from camera.", "success");
        }
      } catch (_error) {
        // Ignore intermittent detector errors while camera is warming up.
      }
    }, 1000);
  } catch (error) {
    showToast(error.message || "Camera could not be started.", "error");
  }
}

function stopScanner() {
  if (verificationTimer) {
    window.clearInterval(verificationTimer);
    verificationTimer = null;
  }
  if (verificationStream) {
    verificationStream.getTracks().forEach((track) => track.stop());
    verificationStream = null;
  }
  const video = document.getElementById("verificationVideo");
  video.classList.add("hidden");
  video.srcObject = null;
  document.getElementById("scannerPlaceholderText").textContent = "Camera scanner placeholder";
}

function debounce(element, callback) {
  window.clearTimeout(element._debounce);
  element._debounce = window.setTimeout(callback, 300);
}

async function refreshFinanceViews() {
  state.depreciation = await api("/api/depreciation");
  state.dashboard = await api("/api/dashboard");
  state.lifecycle = await api("/api/lifecycle");
  await refreshApprovals();
  renderDepreciation();
  renderDashboard();
  renderLifecycle();
}

function readUserPayload() {
  return {
    email: document.getElementById("adminUserEmail").value.trim(),
    password: document.getElementById("adminUserPassword").value,
    name: document.getElementById("adminUserName").value.trim(),
    role: document.getElementById("adminUserRole").value,
    homeBranchCode: document.getElementById("adminUserHomeBranch").value,
    branchCodes: getSelectedValues("adminUserBranchAccess"),
    isActive: document.getElementById("adminUserActive").value === "true",
    phoneNumber: document.getElementById("adminUserPhoneNumber").value.trim()
  };
}

async function submitUserForm(event) {
  event.preventDefault();
  const endpoint = state.adminEditorId ? `/api/admin/users/${state.adminEditorId}` : "/api/admin/users";
  const method = state.adminEditorId ? "PATCH" : "POST";
  const response = await api(endpoint, { method, body: JSON.stringify(readUserPayload()) });
  populateAdminUserForm(response.user);
  await refreshAdminUsers();
  showToast(response.message, "success");
}

async function submitPasswordForm(event) {
  event.preventDefault();
  const response = await api("/api/account/change-password", { method: "POST", body: JSON.stringify({ currentPassword: document.getElementById("currentPasswordInput").value, newPassword: document.getElementById("newPasswordInput").value }) });
  document.getElementById("currentPasswordInput").value = "";
  document.getElementById("newPasswordInput").value = "";
  showToast(response.message, "success");
}

function wireEvents() {
  document.getElementById("loginForm").addEventListener("submit", handleLogin);
  document.getElementById("navStack").addEventListener("click", (event) => {
    const button = event.target.closest(".nav-button");
    if (button) activateSection(button.dataset.section);
  });
  document.getElementById("assetForm").addEventListener("submit", async (event) => {
    try { await submitAssetForm(event); } catch (error) { showToast(error.message, "error"); }
  });
  document.getElementById("assetFormResetButton").addEventListener("click", () => populateAssetForm());
  document.getElementById("assetTable").addEventListener("click", async (event) => {
    const row = event.target.closest(".asset-row");
    if (!row) return;
    try { await loadAssetDetail(row.dataset.assetId); } catch (error) { showToast(error.message, "error"); }
  });
  document.getElementById("closeAssetModal").addEventListener("click", () => document.getElementById("assetModal").classList.add("hidden"));
  document.getElementById("assetAttachmentForm").addEventListener("submit", async (event) => {
    try { await submitAssetAttachment(event); } catch (error) { showToast(error.message, "error"); }
  });
  document.getElementById("assetWorkflowForm").addEventListener("submit", async (event) => {
    try { await submitAssetWorkflow(event); } catch (error) { showToast(error.message, "error"); }
  });
  document.getElementById("assetPrev").addEventListener("click", () => state.assets.page > 1 && loadAssets(state.assets.page - 1));
  document.getElementById("assetNext").addEventListener("click", () => state.assets.page < state.assets.totalPages && loadAssets(state.assets.page + 1));
  ["assetStatusFilter", "assetBranchFilter", "assetCategoryFilter"].forEach((id) => document.getElementById(id).addEventListener("change", () => loadAssets(1)));
  document.getElementById("assetSearch").addEventListener("keyup", () => debounce(document.getElementById("assetSearch"), () => loadAssets(1)));
  document.getElementById("importPreviewButton").addEventListener("click", async () => {
    try { await previewImport(); } catch (error) { showToast(error.message, "error"); }
  });
  document.getElementById("importCommitButton").addEventListener("click", async () => {
    try { await commitImport(); } catch (error) { showToast(error.message, "error"); }
  });
  document.getElementById("kanbanBoard").addEventListener("click", async (event) => {
    const button = event.target.closest(".lifecycle-action");
    if (!button) return;
    try {
      const payload = await api(`/api/lifecycle/${button.dataset.workflowId}/advance`, { method: "POST" });
      state.lifecycle = await api("/api/lifecycle");
      await refreshApprovals();
      renderLifecycle();
      showToast(payload.message, "success");
    } catch (error) {
      showToast(error.message, "error");
    }
  });
  document.getElementById("runDepreciationButton").addEventListener("click", async () => {
    try { const payload = await api("/api/depreciation/run", { method: "POST" }); await refreshFinanceViews(); showToast(payload.message, "success"); } catch (error) { showToast(error.message, "error"); }
  });
  document.getElementById("approveDepreciationButton").addEventListener("click", async () => {
    try { const payload = await api("/api/depreciation/approve", { method: "POST" }); await refreshFinanceViews(); showToast(payload.message, "success"); } catch (error) { showToast(error.message, "error"); }
  });
  document.getElementById("retryFailuresButton").addEventListener("click", async () => {
    try { const payload = await api("/api/depreciation/retry-failures", { method: "POST" }); await refreshFinanceViews(); showToast(payload.message, "success"); } catch (error) { showToast(error.message, "error"); }
  });
  document.getElementById("approvalTable").addEventListener("click", async (event) => {
    const approveButton = event.target.closest(".approval-approve");
    const rejectButton = event.target.closest(".approval-reject");
    if (!approveButton && !rejectButton) return;
    try {
      if (approveButton) {
        const payload = await api(`/api/approvals/${approveButton.dataset.approvalId}/approve`, { method: "POST", body: JSON.stringify({ notes: "" }) });
        await loadAllData();
        showToast(payload.message || "Approval completed.", "success");
      }
      if (rejectButton) {
        const notes = window.prompt("Optional rejection note", "Need more supporting evidence.") || "";
        const payload = await api(`/api/approvals/${rejectButton.dataset.approvalId}/reject`, { method: "POST", body: JSON.stringify({ notes }) });
        await loadAllData();
        showToast(payload.message || "Approval rejected.", "info");
      }
    } catch (error) {
      showToast(error.message, "error");
    }
  });
  document.getElementById("runReconciliationButton").addEventListener("click", async () => {
    try { const payload = await api("/api/reconciliation/run", { method: "POST" }); state.reconciliation = payload.reconciliation; renderReconciliation(); showToast(payload.message, "success"); } catch (error) { showToast(error.message, "error"); }
  });
  document.getElementById("reportGrid").addEventListener("click", async (event) => {
    const button = event.target.closest(".report-generate");
    const downloadButton = event.target.closest(".report-download");
    try {
      if (button) {
        const payload = await api(`/api/reports/${button.dataset.reportId}/generate`, { method: "POST" });
        renderReportPreview(payload);
        showToast(`${payload.report.title} generated.`, "success");
        return;
      }
      if (!downloadButton) return;
      const basePath = `/api/reports/${downloadButton.dataset.reportId}`;
      if (downloadButton.dataset.format === "csv") await downloadAuthenticated(`${basePath}/export.csv`, `${downloadButton.dataset.reportId}.csv`);
      else if (downloadButton.dataset.format === "excel") await downloadAuthenticated(`${basePath}/export.excel.xml`, `${downloadButton.dataset.reportId}.xml`);
      else await openPrintableReport(`${basePath}/export.print.html`);
      showToast("Report export prepared.", "success");
    } catch (error) {
      showToast(error.message, "error");
    }
  });
  document.getElementById("reportActionRow").addEventListener("click", async (event) => {
    const button = event.target.closest(".report-download");
    if (!button) return;
    try {
      const basePath = `/api/reports/${button.dataset.reportId}`;
      if (button.dataset.format === "csv") await downloadAuthenticated(`${basePath}/export.csv`, `${button.dataset.reportId}.csv`);
      else if (button.dataset.format === "excel") await downloadAuthenticated(`${basePath}/export.excel.xml`, `${button.dataset.reportId}.xml`);
      else await openPrintableReport(`${basePath}/export.print.html`);
      showToast("Report export prepared.", "success");
    } catch (error) {
      showToast(error.message, "error");
    }
  });
  document.getElementById("auditUserFilter").addEventListener("keyup", () => debounce(document.getElementById("auditUserFilter"), refreshAudit));
  document.getElementById("auditActionFilter").addEventListener("change", refreshAudit);
  document.getElementById("verificationLookupButton").addEventListener("click", async () => {
    try { await lookupVerificationAsset(); } catch (error) { showToast(error.message, "error"); }
  });
  document.getElementById("verificationActions").addEventListener("click", async (event) => {
    const button = event.target.closest(".status-action");
    if (!button) return;
    await submitVerification(button.dataset.verification);
  });
  document.getElementById("startScannerButton").addEventListener("click", startScanner);
  document.getElementById("stopScannerButton").addEventListener("click", stopScanner);
  document.getElementById("syncOfflineQueueButton").addEventListener("click", async () => {
    try { await syncOfflineQueue(); } catch (error) { showToast(error.message, "error"); }
  });
  document.getElementById("adminUserFormResetButton").addEventListener("click", () => populateAdminUserForm());
  document.getElementById("adminUserForm").addEventListener("submit", async (event) => {
    try { await submitUserForm(event); } catch (error) { showToast(error.message, "error"); }
  });
  document.getElementById("adminUserTable").addEventListener("click", (event) => {
    const row = event.target.closest(".admin-user-row");
    if (!row) return;
    const user = state.adminUsers.find((item) => item.id === row.dataset.userId);
    if (user) populateAdminUserForm(user);
  });
  document.getElementById("passwordForm").addEventListener("submit", async (event) => {
    try { await submitPasswordForm(event); } catch (error) { showToast(error.message, "error"); }
  });
}

async function bootstrap() {
  renderDemoCredentials(await api("/api/bootstrap"));
  wireEvents();
}

window.addEventListener("beforeunload", stopScanner);
bootstrap();
