const state = {
  token: null,
  user: null,
  meta: null,
  dashboard: null,
  assets: { items: [], page: 1, totalPages: 1, total: 0, pageSize: 12, scope: "all_branches" },
  lifecycle: null,
  depreciation: null,
  approvals: { pendingCount: 0, items: [] },
  reconciliation: null,
  reports: [],
  audit: [],
  currentSection: "dashboard",
  verificationAsset: null
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
  verification: "Physical Verification"
};

const clientPermissions = {
  finance_admin: ["depreciation.run", "depreciation.approve", "depreciation.retry_failures", "approvals.read", "approvals.decide"],
  operations: [],
  admin_user: ["approvals.read.branch"],
  auditor: ["approvals.read"],
  it_admin: ["depreciation.run", "depreciation.retry_failures", "approvals.read"]
};

async function api(path, options = {}) {
  const headers = { "Content-Type": "application/json", ...(options.headers || {}) };
  if (state.token) headers.Authorization = `Bearer ${state.token}`;
  const response = await fetch(path, { ...options, headers });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.error || "Prototype request failed.");
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
  const printWindow = window.open("", "_blank", "noopener,noreferrer");
  if (!printWindow) throw new Error("Pop-up blocked. Allow pop-ups to open the print-ready report.");
  printWindow.document.open();
  printWindow.document.write(html);
  printWindow.document.close();
}

function showToast(message, tone = "info") {
  const toast = document.createElement("div");
  toast.className = `toast ${tone}`;
  toast.textContent = message;
  document.getElementById("toastStack").appendChild(toast);
  window.setTimeout(() => toast.remove(), 3200);
}

const formatDate = (value) => new Date(value).toLocaleString("en-GB", { dateStyle: "medium", timeStyle: "short" });
const formatAmount = (amount, currency) => currency === "CDF" ? `CDF ${new Intl.NumberFormat("fr-CD", { maximumFractionDigits: 0 }).format(amount)}` : `$${new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(amount)}`;
const statusPill = (label, tone) => `<span class="status-pill tone-${tone || "muted"}">${label}</span>`;
const currentPermissions = () => state.user?.permissions || clientPermissions[state.user?.role] || [];
const hasPermission = (permission) => currentPermissions().includes(permission);

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

function renderDashboard() {
  const { kpis } = state.dashboard;
  const cards = [
    { title: "Total Assets", value: new Intl.NumberFormat().format(kpis.totalAssets), meta: "Assets currently visible in your scope" },
    { title: "Total NBV (USD)", value: formatAmount(kpis.totalUSDNBV, "USD"), meta: "Core ledger-aligned view" },
    { title: "Total NBV (CDF)", value: formatAmount(kpis.totalCDFNBV, "CDF"), meta: "Translated for local reporting" },
    { title: "Pending Actions", value: new Intl.NumberFormat().format(kpis.pendingActions), meta: "Transfers, tagging, disposal, impairment" }
  ];
  document.getElementById("kpiGrid").innerHTML = cards.map((card) => `<article class="kpi-card"><div class="eyebrow">${card.title}</div><div class="kpi-value">${card.value}</div><div class="kpi-meta">${card.meta}</div></article>`).join("");

  const maxUSD = Math.max(...state.dashboard.depreciationTrend.map((point) => point.totalUSD));
  const maxCDF = Math.max(...state.dashboard.depreciationTrend.map((point) => point.totalCDF));
  document.getElementById("depreciationTrend").innerHTML = state.dashboard.depreciationTrend.map((point) => `<div class="bar-column"><div class="bar-stack"><span class="bar-usd" style="height:${Math.max(12, Math.round(point.totalUSD / maxUSD * 110))}px"></span><span class="bar-cdf" style="height:${Math.max(12, Math.round(point.totalCDF / maxCDF * 110))}px"></span></div><div class="bar-label">${point.period.slice(5)}</div></div>`).join("");

  const colors = ["#10285d", "#c9a84c", "#2563eb", "#dc2626", "#7c3aed", "#d97706", "#94a3b8"];
  const total = state.dashboard.statusBreakdown.reduce((sum, item) => sum + item.count, 0);
  let cursor = 0;
  const slices = state.dashboard.statusBreakdown.map((item, index) => {
    const next = cursor + item.count / total * 360;
    const slice = `${colors[index]} ${cursor.toFixed(1)}deg ${next.toFixed(1)}deg`;
    cursor = next;
    return slice;
  });
  document.getElementById("statusRing").style.background = `conic-gradient(${slices.join(", ")})`;
  document.getElementById("statusLegend").innerHTML = state.dashboard.statusBreakdown.map((item, index) => `<div class="legend-row"><span class="legend-label"><span class="legend-dot" style="background:${colors[index]}"></span>${item.label}</span><strong>${new Intl.NumberFormat().format(item.count)}</strong></div>`).join("");
  document.getElementById("activityFeed").innerHTML = state.dashboard.recentActivity.map((item) => `<article class="activity-item"><div class="activity-title">${item.title}</div><div>${item.detail}</div><div class="activity-meta">${item.user} · ${formatDate(item.timestamp)}</div></article>`).join("");
  document.getElementById("branchTable").innerHTML = state.dashboard.branchSummary.map((branch) => `<tr><td>${branch.branchName}</td><td>${new Intl.NumberFormat().format(branch.assetCount)}</td><td>${branch.pendingActions}</td><td>${branch.verificationCoverage}%</td></tr>`).join("");
}

function populateFilters() {
  document.getElementById("assetBranchFilter").innerHTML = [`<option value="">All branches</option>`].concat(state.meta.branches.map((branch) => `<option value="${branch.code}">${branch.name}</option>`)).join("");
  document.getElementById("assetCategoryFilter").innerHTML = [`<option value="">All categories</option>`].concat(state.meta.categories.map((category) => `<option value="${category.key}">${category.label}</option>`)).join("");
  document.getElementById("assetStatusFilter").innerHTML = [`<option value="">All statuses</option>`].concat(state.meta.statuses.map((status) => `<option value="${status.status}">${status.label}</option>`)).join("");
}

function renderAssets() {
  document.getElementById("assetScopeNote").textContent = state.assets.scope === "branch_only" ? "Scoped to your branch role" : "All branches visible";
  document.getElementById("assetTable").innerHTML = state.assets.items.map((asset) => `<tr class="asset-row" data-asset-id="${asset.id}"><td>${asset.assetId}</td><td>${asset.tagCode}</td><td>${asset.name}</td><td>${asset.category}</td><td>${asset.branchName}</td><td>${asset.currency}</td><td>${formatAmount(asset.acquisitionCost, asset.currency)}</td><td>${formatAmount(asset.netBookValue, asset.currency)}</td><td>${statusPill(asset.statusLabel, asset.roleTone)}</td></tr>`).join("");
  document.getElementById("assetPaginationLabel").textContent = `Page ${state.assets.page} of ${state.assets.totalPages} · ${new Intl.NumberFormat().format(state.assets.total)} assets`;
}

async function loadAssets(page = state.assets.page) {
  const params = new URLSearchParams({
    page: String(page),
    pageSize: String(state.assets.pageSize),
    search: document.getElementById("assetSearch").value,
    status: document.getElementById("assetStatusFilter").value,
    branch: document.getElementById("assetBranchFilter").value,
    category: document.getElementById("assetCategoryFilter").value
  });
  state.assets = await api(`/api/assets?${params.toString()}`);
  renderAssets();
}

function drawPseudoQr(value) {
  const chars = value.split("");
  document.getElementById("pseudoQr").innerHTML = Array.from({ length: 441 }, (_, index) => {
    const code = chars[index % chars.length].charCodeAt(0);
    const active = (code + index * 7 + index % 3) % 5 < 2;
    return `<span class="qr-dot ${active ? "active" : ""}"></span>`;
  }).join("");
}

function openAssetModal(detail) {
  const asset = detail.asset;
  document.getElementById("assetModalTitle").textContent = asset.name;
  document.getElementById("qrTag").textContent = asset.tagCode;
  document.getElementById("qrName").textContent = asset.name;
  document.getElementById("qrBranch").textContent = `${asset.branchName} · ${asset.city}`;
  document.getElementById("qrFooter").textContent = asset.assetId;
  drawPseudoQr(asset.tagCode);
  document.getElementById("assetOverview").innerHTML = [["Category", asset.category], ["Branch", asset.branchName], ["Currency", asset.currency], ["Cost", formatAmount(asset.acquisitionCost, asset.currency)], ["NBV", formatAmount(asset.netBookValue, asset.currency)], ["Status", asset.statusLabel], ["Useful Life", `${asset.usefulLifeMonths} months`], ["Method", asset.depreciationMethod], ["Capitalisation", asset.capitalisationDate], ["GL Asset Account", asset.glAssetAccount]].map(([label, value]) => `<div class="detail-item"><span>${label}</span><strong>${value}</strong></div>`).join("");
  document.getElementById("assetSchedule").innerHTML = detail.depreciationSchedule.map((line) => `<tr><td>${line.period}</td><td>${formatAmount(line.openingNBV, asset.currency)}</td><td>${formatAmount(line.depreciationCharge, asset.currency)}</td><td>${formatAmount(line.closingNBV, asset.currency)}</td></tr>`).join("");
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
    return `<article class="kanban-column"><h3>${column.title}</h3><div class="kanban-note">${cards.length} items</div>${cards.map((card) => `<div class="kanban-card"><strong>${card.title}</strong><div>${card.asset.name}</div><div class="kanban-note">${card.asset.assetId} · ${card.asset.branchName}</div><div class="kanban-note">${card.note}</div>${card.approvalState === "PENDING" ? `<div class="approval-card-note">${statusPill(card.approvalLabel || "Pending approval", "warning")}<span class="inline-note">Checker action is still pending for this workflow.</span></div>` : card.nextColumn !== "completed" ? `<button class="secondary-button lifecycle-action" data-workflow-id="${card.id}">${card.actionLabel}</button>` : statusPill("Completed in prototype flow", "success")}</div>`).join("")}</article>`;
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
  const approvalHtml = approval
    ? `<div>${statusPill(approval.statusLabel, approval.statusTone)}</div><div class="inline-note">Submitted by ${approval.requestedByName} on ${formatDate(approval.requestedAt)}.</div>`
    : `<div class="inline-note">No active approval request for this run.</div>`;

  document.getElementById("depreciationPanel").innerHTML = `<div>${statusPill(run.statusLabel || run.status, run.status === "POSTED" ? "success" : run.status === "PENDING_APPROVAL" ? "warning" : run.status === "REJECTED" ? "danger" : "info")}</div><h3>${run.period} monthly run</h3><p>${run.summary}</p>${approvalHtml}<div class="summary-grid"><div class="summary-stat"><span>Assets</span><strong>${new Intl.NumberFormat().format(run.totalAssetsProcessed)}</strong></div><div class="summary-stat"><span>Total USD</span><strong>${formatAmount(run.totalDepreciationUSD, "USD")}</strong></div><div class="summary-stat"><span>Total CDF</span><strong>${formatAmount(run.totalDepreciationCDF, "CDF")}</strong></div><div class="summary-stat"><span>Rate</span><strong>${new Intl.NumberFormat().format(run.exchangeRateUsed)}</strong></div></div><div class="kanban-note">Failures currently held for review: ${run.failureCount}</div>`;
  document.getElementById("depreciationHistory").innerHTML = state.depreciation.history.map((item) => `<tr><td>${item.period}</td><td>${statusPill(item.statusLabel || item.status, item.status === "POSTED" ? "success" : item.status === "PENDING_APPROVAL" ? "warning" : item.status === "REJECTED" ? "danger" : "info")}</td><td>${new Intl.NumberFormat().format(item.totalAssetsProcessed)}</td><td>${formatAmount(item.totalDepreciationUSD, "USD")}</td><td>${formatAmount(item.totalDepreciationCDF, "CDF")}</td><td>${item.failureCount}</td></tr>`).join("");
  document.getElementById("depreciationExceptions").innerHTML = (state.depreciation.postingExceptions || []).length
    ? state.depreciation.postingExceptions.map((item) => `<tr><td>${item.assetId}<div class="kanban-note">${item.assetName}</div></td><td>${item.branchName}</td><td>${formatAmount(item.depreciationCharge, item.currency)}</td><td>${statusPill(item.postingStatus, item.postingStatus === "FAILED" ? "danger" : "success")}</td><td>${item.retryCount}</td><td>${item.failureReason || "-"}</td></tr>`).join("")
    : `<tr><td colspan="6">No failed depreciation lines are currently waiting for retry.</td></tr>`;
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
  document.getElementById("auditTable").innerHTML = state.audit.map((log) => `<tr><td>${formatDate(log.timestamp)}</td><td>${log.user}</td><td>${log.action}</td><td>${log.entity}</td><td>${log.detail}</td></tr>`).join("");
}

function renderApprovals() {
  document.getElementById("approvalSummary").textContent = `${state.approvals.pendingCount} pending`;
  const canDecide = hasPermission("approvals.decide");
  document.getElementById("approvalTable").innerHTML = state.approvals.items.length
    ? state.approvals.items.map((item) => `<tr><td>${item.entityLabel}<div class="kanban-note">${item.title}</div></td><td>${item.actionLabel}</td><td>${item.requestedByName}<div class="kanban-note">${item.requestedByRole}</div></td><td>${item.branchName}</td><td>${statusPill(item.statusLabel, item.statusTone)}</td><td>${formatDate(item.requestedAt)}</td><td>${item.status === "PENDING" && canDecide ? `<div class="decision-stack"><button class="secondary-button approval-approve" data-approval-id="${item.id}">Approve</button><button class="ghost-button approval-reject" data-approval-id="${item.id}">Reject</button></div>` : `<span class="inline-note">${item.decisionNotes || item.statusLabel}</span>`}</td></tr>`).join("")
    : `<tr><td colspan="7">No approval requests are currently visible in your scope.</td></tr>`;
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
  container.innerHTML = `<div class="verification-asset"><strong>${asset.name}</strong><div>${asset.tagCode}</div><div>${asset.branchName}</div><div>Last verified: ${asset.lastVerifiedAt}</div>${statusPill(asset.statusLabel, asset.roleTone)}</div>`;
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

async function loadAllData() {
  const [meta, dashboard, assets, lifecycle, depreciation, approvals, reconciliation, reports, audit] = await Promise.all([
    api("/api/meta"),
    api("/api/dashboard"),
    api("/api/assets?page=1&pageSize=12"),
    api("/api/lifecycle"),
    api("/api/depreciation"),
    safeApi("/api/approvals", { pendingCount: 0, items: [] }),
    api("/api/reconciliation"),
    api("/api/reports"),
    api("/api/audit")
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

  renderUserPanel();
  populateFilters();
  renderDashboard();
  renderAssets();
  renderLifecycle();
  renderDepreciation();
  renderApprovals();
  renderReconciliation();
  renderReports();
  renderAudit();
  renderVerificationResult();
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

async function lookupVerificationAsset() {
  const tagCode = document.getElementById("verificationTagInput").value.trim();
  if (!tagCode) return showToast("Enter a tag code to continue.", "error");
  state.verificationAsset = (await api(`/api/verification/lookup/${encodeURIComponent(tagCode)}`)).asset;
  renderVerificationResult();
}

async function submitVerification(outcome) {
  if (!state.verificationAsset) return;
  const payload = await api("/api/verification/submit", { method: "POST", body: JSON.stringify({ tagCode: state.verificationAsset.tagCode, outcome, notes: document.getElementById("verificationNotes").value }) });
  state.verificationAsset = payload.asset;
  renderVerificationResult();
  state.dashboard = await api("/api/dashboard");
  renderDashboard();
  showToast(payload.message, "success");
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

function wireEvents() {
  document.getElementById("loginForm").addEventListener("submit", handleLogin);
  document.getElementById("navStack").addEventListener("click", (event) => {
    const button = event.target.closest(".nav-button");
    if (button) activateSection(button.dataset.section);
  });
  document.getElementById("assetTable").addEventListener("click", async (event) => {
    const row = event.target.closest(".asset-row");
    if (row) openAssetModal(await api(`/api/assets/${row.dataset.assetId}`));
  });
  document.getElementById("closeAssetModal").addEventListener("click", () => document.getElementById("assetModal").classList.add("hidden"));
  document.getElementById("assetPrev").addEventListener("click", () => state.assets.page > 1 && loadAssets(state.assets.page - 1));
  document.getElementById("assetNext").addEventListener("click", () => state.assets.page < state.assets.totalPages && loadAssets(state.assets.page + 1));
  ["assetStatusFilter", "assetBranchFilter", "assetCategoryFilter"].forEach((id) => document.getElementById(id).addEventListener("change", () => loadAssets(1)));
  document.getElementById("assetSearch").addEventListener("keyup", () => debounce(document.getElementById("assetSearch"), () => loadAssets(1)));

  document.getElementById("kanbanBoard").addEventListener("click", async (event) => {
    const button = event.target.closest(".lifecycle-action");
    if (!button) return;
    const payload = await api(`/api/lifecycle/${button.dataset.workflowId}/advance`, { method: "POST" });
    state.lifecycle = await api("/api/lifecycle");
    await refreshApprovals();
    renderLifecycle();
    showToast(payload.message, "success");
  });

  document.getElementById("runDepreciationButton").addEventListener("click", async () => {
    const payload = await api("/api/depreciation/run", { method: "POST" });
    await refreshFinanceViews();
    showToast(payload.message, "success");
  });

  document.getElementById("approveDepreciationButton").addEventListener("click", async () => {
    const payload = await api("/api/depreciation/approve", { method: "POST" });
    await refreshFinanceViews();
    showToast(payload.message, "success");
  });

  document.getElementById("retryFailuresButton").addEventListener("click", async () => {
    const payload = await api("/api/depreciation/retry-failures", { method: "POST" });
    await refreshFinanceViews();
    showToast(payload.message, "success");
  });

  document.getElementById("approvalTable").addEventListener("click", async (event) => {
    const approveButton = event.target.closest(".approval-approve");
    const rejectButton = event.target.closest(".approval-reject");
    if (!approveButton && !rejectButton) return;

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
  });

  document.getElementById("runReconciliationButton").addEventListener("click", async () => {
    const payload = await api("/api/reconciliation/run", { method: "POST" });
    state.reconciliation = payload.reconciliation;
    renderReconciliation();
    showToast(payload.message, "success");
  });

  document.getElementById("reportGrid").addEventListener("click", async (event) => {
    const button = event.target.closest(".report-generate");
    const downloadButton = event.target.closest(".report-download");
    if (button) {
      const payload = await api(`/api/reports/${button.dataset.reportId}/generate`, { method: "POST" });
      renderReportPreview(payload);
      showToast(`${payload.report.title} generated.`, "success");
      return;
    }
    if (!downloadButton) return;
    try {
      const basePath = `/api/reports/${downloadButton.dataset.reportId}`;
      if (downloadButton.dataset.format === "csv") {
        await downloadAuthenticated(`${basePath}/export.csv`, `${downloadButton.dataset.reportId}.csv`);
      } else if (downloadButton.dataset.format === "excel") {
        await downloadAuthenticated(`${basePath}/export.excel.xml`, `${downloadButton.dataset.reportId}.xml`);
      } else {
        await openPrintableReport(`${basePath}/export.print.html`);
      }
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
      if (button.dataset.format === "csv") {
        await downloadAuthenticated(`${basePath}/export.csv`, `${button.dataset.reportId}.csv`);
      } else if (button.dataset.format === "excel") {
        await downloadAuthenticated(`${basePath}/export.excel.xml`, `${button.dataset.reportId}.xml`);
      } else {
        await openPrintableReport(`${basePath}/export.print.html`);
      }
      showToast("Report export prepared.", "success");
    } catch (error) {
      showToast(error.message, "error");
    }
  });

  document.getElementById("auditUserFilter").addEventListener("keyup", () => debounce(document.getElementById("auditUserFilter"), refreshAudit));
  document.getElementById("auditActionFilter").addEventListener("change", refreshAudit);
  document.getElementById("verificationLookupButton").addEventListener("click", async () => {
    try {
      await lookupVerificationAsset();
    } catch (err) {
      showToast(err.message, "error");
    }
  });
  document.getElementById("verificationActions").addEventListener("click", async (event) => {
    const button = event.target.closest(".status-action");
    if (!button) return;
    try {
      await submitVerification(button.dataset.verification);
    } catch (err) {
      showToast(err.message, "error");
    }
  });
}

async function bootstrap() {
  renderDemoCredentials(await api("/api/bootstrap"));
  wireEvents();
}

bootstrap();
