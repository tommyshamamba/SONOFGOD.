const path = require("node:path");
const crypto = require("node:crypto");
const express = require("express");
const cors = require("cors");
const helmet = require("helmet");

const { branches, categoryProfiles, demoUsers, reportCatalog, statusPalette } = require("./data/referenceData");
const { AS_OF_DATE, buildBaseData, buildSchedule } = require("./data/generator");
const { buildReportPack, buildReportCsv, buildReportExcelXml, buildPrintableHtml } = require("./domain/reporting");

function roleLabel(role) {
  return {
    finance_admin: "Finance Administrator",
    operations: "Operations User",
    admin_user: "Admin User",
    auditor: "Auditor / Reviewer",
    it_admin: "IT Administrator"
  }[role] || role;
}

function createApp() {
  const app = express();
  const state = buildBaseData();
  const sessions = new Map();

  app.use(helmet({ contentSecurityPolicy: false }));
  app.use(cors());
  app.use(express.json());
  app.use(express.static(path.join(__dirname, "..", "public")));

  const sanitizeUser = (user) => {
    const branch = branches.find((item) => item.code === user.branchCode);
    return { id: user.id, email: user.email, name: user.name, role: user.role, roleLabel: roleLabel(user.role), title: user.title, branchCode: user.branchCode, branchName: branch?.name || "All Branches" };
  };

  const addAudit = (user, action, entity, detail) => {
    const entry = { id: `runtime-${Date.now()}`, timestamp: new Date().toISOString(), user: user.name, role: user.role, action, entity, branchName: sanitizeUser(user).branchName, detail };
    state.auditLogs.unshift(entry);
    state.recentActivity.unshift({ id: entry.id, title: action.replaceAll("_", " "), detail, user: user.name, timestamp: entry.timestamp });
    state.recentActivity = state.recentActivity.slice(0, 20);
  };

  const requireAuth = (req, res, next) => {
    const authorization = req.headers.authorization || "";
    const token = authorization.startsWith("Bearer ") ? authorization.slice(7) : null;
    if (!token || !sessions.has(token)) return res.status(401).json({ error: "Authentication required." });
    req.user = sessions.get(token);
    return next();
  };

  const requireRole = (...roles) => (req, res, next) => {
    if (!roles.includes(req.user.role)) return res.status(403).json({ error: "You do not have permission to perform this action." });
    return next();
  };

  const scopedAssets = (user) => (user.role === "operations" ? state.assets.filter((asset) => asset.branchCode === user.branchCode) : state.assets);
  const assetView = (asset) => ({ ...asset, statusLabel: statusPalette[asset.status]?.label || asset.status, roleTone: statusPalette[asset.status]?.tone || "muted" });
  const reportPackFor = (user, report) => buildReportPack(report, scopedAssets(user), {
    generatedAt: new Date().toISOString(),
    generatedBy: user.name,
    scopeLabel: user.role === "operations" ? `${sanitizeUser(user).branchName} only` : "All branches",
    exchangeRateToCdf: state.depreciationRuns[state.depreciationRuns.length - 1]?.exchangeRateUsed || 2850,
    reconciliation: state.reconciliation
  });

  const dashboard = (user) => {
    const assets = scopedAssets(user);
    return {
      kpis: {
        totalAssets: assets.length,
        totalLocations: new Set(assets.map((asset) => asset.branchCode)).size,
        totalUSDNBV: Math.round(assets.filter((asset) => asset.currency === "USD").reduce((sum, asset) => sum + asset.netBookValue, 0)),
        totalCDFNBV: Math.round(assets.filter((asset) => asset.currency === "CDF").reduce((sum, asset) => sum + asset.netBookValue, 0)),
        pendingActions: assets.filter((asset) => ["PENDING", "TRANSFERRED", "HELD_FOR_SALE", "IMPAIRED"].includes(asset.status)).length
      },
      statusBreakdown: Object.keys(statusPalette).map((status) => ({ status, label: statusPalette[status].label, count: assets.filter((asset) => asset.status === status).length })),
      depreciationTrend: state.depreciationRuns.map((run) => ({ period: run.period, totalUSD: run.totalDepreciationUSD, totalCDF: run.totalDepreciationCDF })),
      recentActivity: state.recentActivity.slice(0, 10),
      branchSummary: user.role === "operations" ? state.branchSummary.filter((branch) => branch.branchCode === user.branchCode) : state.branchSummary.slice(0, 10),
      currentRun: state.depreciationRuns[state.depreciationRuns.length - 1]
    };
  };

  app.get("/api/health", (_req, res) => res.status(200).json({ status: "ok", service: "pesa-fams-prototype", asOfDate: AS_OF_DATE.toISOString(), generatedAt: state.generatedAt }));
  app.get("/api/bootstrap", (_req, res) => res.status(200).json({ demoUsers: demoUsers.map((user) => ({ email: user.email, password: user.password, role: roleLabel(user.role), branchName: branches.find((branch) => branch.code === user.branchCode)?.name || "All Branches" })), branchCount: branches.length, assetCount: state.assets.length }));

  app.post("/api/auth/login", (req, res) => {
    const { email, password } = req.body || {};
    const user = demoUsers.find((candidate) => candidate.email.toLowerCase() === String(email || "").toLowerCase().trim());
    if (!user || user.password !== password) return res.status(401).json({ error: "Invalid credentials for the prototype." });
    const token = crypto.randomUUID();
    sessions.set(token, user);
    addAudit(user, "LOGIN", user.email, `Interactive prototype login for ${user.name}`);
    return res.status(200).json({ token, user: sanitizeUser(user) });
  });

  app.get("/api/meta", requireAuth, (req, res) => res.status(200).json({
    user: sanitizeUser(req.user),
    branches: req.user.role === "operations" ? branches.filter((branch) => branch.code === req.user.branchCode) : branches,
    categories: categoryProfiles.map((category) => ({ key: category.key, label: category.label, method: category.method, usefulLifeMonths: category.usefulLifeMonths })),
    statuses: Object.entries(statusPalette).map(([status, definition]) => ({ status, label: definition.label }))
  }));

  app.get("/api/dashboard", requireAuth, (req, res) => res.status(200).json(dashboard(req.user)));

  app.get("/api/assets", requireAuth, (req, res) => {
    const query = {
      search: String(req.query.search || "").trim().toLowerCase(),
      status: String(req.query.status || ""),
      branch: String(req.query.branch || ""),
      category: String(req.query.category || ""),
      page: Math.max(Number(req.query.page) || 1, 1),
      pageSize: Math.min(Math.max(Number(req.query.pageSize) || 12, 1), 50)
    };
    let assets = scopedAssets(req.user);
    if (query.search) assets = assets.filter((asset) => [asset.assetId, asset.tagCode, asset.name, asset.branchName].join(" ").toLowerCase().includes(query.search));
    if (query.status) assets = assets.filter((asset) => asset.status === query.status);
    if (query.branch) assets = assets.filter((asset) => asset.branchCode === query.branch);
    if (query.category) assets = assets.filter((asset) => asset.categoryKey === query.category);

    const total = assets.length;
    const totalPages = Math.max(Math.ceil(total / query.pageSize), 1);
    const items = assets.slice((query.page - 1) * query.pageSize, query.page * query.pageSize).map(assetView);
    return res.status(200).json({ items, total, page: query.page, pageSize: query.pageSize, totalPages, scope: req.user.role === "operations" ? "branch_only" : "all_branches" });
  });

  app.get("/api/assets/:id", requireAuth, (req, res) => {
    const asset = scopedAssets(req.user).find((item) => item.id === req.params.id || item.assetId === req.params.id);
    if (!asset) return res.status(404).json({ error: "Asset not found in your current scope." });
    return res.status(200).json({ asset: assetView(asset), depreciationSchedule: buildSchedule(asset, 18), auditHistory: state.auditLogs.filter((log) => log.entity === asset.assetId).slice(0, 12) });
  });

  app.get("/api/lifecycle", requireAuth, (req, res) => {
    const visibleBranch = req.user.role === "operations" ? req.user.branchCode : null;
    const cards = visibleBranch ? state.lifecycleCards.filter((card) => card.asset.branchCode === visibleBranch) : state.lifecycleCards;
    return res.status(200).json({ columns: { pendingTransfers: cards.filter((card) => card.column === "pendingTransfers"), inTransit: cards.filter((card) => card.column === "inTransit"), pendingDisposals: cards.filter((card) => card.column === "pendingDisposals"), pendingImpairments: cards.filter((card) => card.column === "pendingImpairments") } });
  });

  app.post("/api/lifecycle/:id/advance", requireAuth, requireRole("finance_admin", "admin_user", "operations", "it_admin"), (req, res) => {
    const card = state.lifecycleCards.find((item) => item.id === req.params.id);
    if (!card) return res.status(404).json({ error: "Workflow item not found." });
    if (req.user.role === "operations" && card.asset.branchCode !== req.user.branchCode) return res.status(403).json({ error: "Operations users can only action workflows in their branch." });
    const previousColumn = card.column;
    card.column = card.nextColumn;
    addAudit(req.user, "WORKFLOW_ADVANCE", card.asset.assetId, `${card.title} moved from ${previousColumn} to ${card.nextColumn}`);
    return res.status(200).json({ message: `${card.title} advanced successfully.`, card });
  });

  app.get("/api/depreciation", requireAuth, (_req, res) => res.status(200).json({ currentRun: state.depreciationRuns[state.depreciationRuns.length - 1], history: state.depreciationRuns.slice().reverse() }));

  app.post("/api/depreciation/run", requireAuth, requireRole("finance_admin", "it_admin"), (req, res) => {
    const currentRun = state.depreciationRuns[state.depreciationRuns.length - 1];
    currentRun.status = "DRAFT";
    currentRun.runBy = req.user.name;
    currentRun.summary = "Depreciation recalculated with current exchange rate. Ready for approval.";
    currentRun.failureCount = 2;
    currentRun.glBatchReference = null;
    addAudit(req.user, "DEPRECIATION_RUN", currentRun.period, `Depreciation run prepared for ${currentRun.period}`);
    return res.status(200).json({ message: "Depreciation run prepared for approval.", currentRun });
  });

  app.post("/api/depreciation/approve", requireAuth, requireRole("finance_admin"), (req, res) => {
    const currentRun = state.depreciationRuns[state.depreciationRuns.length - 1];
    if (currentRun.status !== "DRAFT") return res.status(409).json({ error: "The current run is not awaiting approval." });
    currentRun.status = "POSTED";
    currentRun.approvedBy = req.user.name;
    currentRun.approvedAt = new Date().toISOString();
    currentRun.postedAt = new Date().toISOString();
    currentRun.glBatchReference = `BATCH-${currentRun.period.replace("-", "")}-4823`;
    currentRun.summary = "Posted to Finacle. Two entries held for manual retry review.";
    addAudit(req.user, "DEPRECIATION_APPROVE", currentRun.period, `Approved and posted depreciation batch ${currentRun.glBatchReference}`);
    return res.status(200).json({ message: "Depreciation batch approved and posted.", currentRun });
  });

  app.get("/api/reconciliation", requireAuth, (_req, res) => res.status(200).json(state.reconciliation));
  app.post("/api/reconciliation/run", requireAuth, requireRole("finance_admin", "auditor", "it_admin"), (req, res) => {
    state.reconciliation.lastRunAt = new Date().toISOString();
    addAudit(req.user, "GL_RECONCILE", state.reconciliation.period, `Reconciliation run executed for ${state.reconciliation.period}`);
    return res.status(200).json({ message: "Reconciliation refreshed.", reconciliation: state.reconciliation });
  });

  app.get("/api/reports", requireAuth, (_req, res) => res.status(200).json({
    reports: reportCatalog.map((report) => ({
      ...report,
      exportFormats: ["preview", "csv", "excel", "print"]
    }))
  }));
  app.post("/api/reports/:id/generate", requireAuth, (req, res) => {
    const report = reportCatalog.find((item) => item.id === req.params.id);
    if (!report) return res.status(404).json({ error: "Report definition not found." });
    addAudit(req.user, "REPORT_EXPORT", report.title, `Generated ${report.title}`);
    const pack = reportPackFor(req.user, report);
    return res.status(200).json({
      report,
      summary: pack.summary,
      preview: pack.preview,
      exports: {
        csv: `/api/reports/${report.id}/export.csv`,
        excel: `/api/reports/${report.id}/export.excel.xml`,
        print: `/api/reports/${report.id}/export.print.html`
      }
    });
  });

  app.get("/api/reports/:id/export.csv", requireAuth, (req, res) => {
    const report = reportCatalog.find((item) => item.id === req.params.id);
    if (!report) return res.status(404).json({ error: "Report definition not found." });
    const pack = reportPackFor(req.user, report);
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${req.params.id}-${new Date().toISOString().slice(0, 10)}.csv"`);
    return res.status(200).send(buildReportCsv(pack));
  });

  app.get("/api/reports/:id/export.excel.xml", requireAuth, (req, res) => {
    const report = reportCatalog.find((item) => item.id === req.params.id);
    if (!report) return res.status(404).json({ error: "Report definition not found." });
    const pack = reportPackFor(req.user, report);
    res.setHeader("Content-Type", "application/vnd.ms-excel; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${req.params.id}-${new Date().toISOString().slice(0, 10)}.xml"`);
    return res.status(200).send(buildReportExcelXml(pack));
  });

  app.get("/api/reports/:id/export.print.html", requireAuth, (req, res) => {
    const report = reportCatalog.find((item) => item.id === req.params.id);
    if (!report) return res.status(404).json({ error: "Report definition not found." });
    const pack = reportPackFor(req.user, report);
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.setHeader("Content-Disposition", `inline; filename="${req.params.id}-${new Date().toISOString().slice(0, 10)}.html"`);
    return res.status(200).send(buildPrintableHtml(pack));
  });

  app.get("/api/audit", requireAuth, (req, res) => {
    const action = String(req.query.action || "");
    const user = String(req.query.user || "").toLowerCase();
    let logs = state.auditLogs;
    if (action) logs = logs.filter((log) => log.action === action);
    if (user) logs = logs.filter((log) => log.user.toLowerCase().includes(user));
    if (req.user.role === "operations") logs = logs.filter((log) => log.branchName === sanitizeUser(req.user).branchName);
    return res.status(200).json({ items: logs.slice(0, 80) });
  });

  app.get("/api/verification/lookup/:tagCode", requireAuth, (req, res) => {
    const asset = scopedAssets(req.user).find((item) => item.tagCode.toLowerCase() === req.params.tagCode.toLowerCase());
    if (!asset) return res.status(404).json({ error: "Tag not found in current prototype scope." });
    return res.status(200).json({ asset: assetView(asset) });
  });

  app.post("/api/verification/submit", requireAuth, requireRole("operations", "admin_user", "finance_admin"), (req, res) => {
    const { tagCode, outcome, notes } = req.body || {};
    const asset = scopedAssets(req.user).find((item) => item.tagCode === tagCode);
    if (!asset) return res.status(404).json({ error: "Tag not found for verification." });
    asset.lastVerifiedAt = new Date().toISOString().slice(0, 10);
    if (outcome === "NOT_FOUND") asset.status = "IMPAIRED";
    addAudit(req.user, "PHYSICAL_VERIFY", asset.assetId, `Verification outcome ${outcome || "CONFIRMED"} recorded. ${notes || ""}`.trim());
    return res.status(200).json({ message: "Verification submitted.", asset: assetView(asset) });
  });

  app.use((req, res, next) => {
    if (req.path.startsWith("/api/")) return next();
    return res.sendFile(path.join(__dirname, "..", "public", "index.html"));
  });

  app.locals.resetState = () => {
    const fresh = buildBaseData();
    Object.keys(state).forEach((key) => delete state[key]);
    Object.assign(state, fresh);
    sessions.clear();
  };

  return app;
}

module.exports = { createApp };
