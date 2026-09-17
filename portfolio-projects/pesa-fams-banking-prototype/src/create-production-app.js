const path = require("node:path");
const jwt = require("jsonwebtoken");
const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const { z } = require("zod");

const db = require("./config/db");
const systemService = require("./services/systemService");
const { requirePermission } = require("./domain/permissions");

function createProductionApp(env) {
  const app = express();

  app.use(helmet({ contentSecurityPolicy: false }));
  app.use(cors());
  app.use(express.json());
  app.use(express.static(path.join(__dirname, "..", "public")));

  const sendError = (res, error) => {
    const statusCode = error.statusCode || 500;
    res.status(statusCode).json({ error: error.message || "Unexpected server error." });
  };

  const withHandler = (handler) => async (req, res, next) => {
    try {
      await handler(req, res, next);
    } catch (error) {
      sendError(res, error);
    }
  };

  const validate = (schema, source = "body") => (req) => schema.parse(req[source]);

  const requireAuth = withHandler(async (req, res, next) => {
    try {
      const authorization = req.headers.authorization || "";
      const token = authorization.startsWith("Bearer ") ? authorization.slice(7) : null;
      if (!token) {
        return res.status(401).json({ error: "Authentication required." });
      }
      const payload = jwt.verify(token, env.jwtSecret);
      req.currentUser = { id: payload.sub, role: payload.role };
      return next();
    } catch (_error) {
      return res.status(401).json({ error: "Invalid or expired session." });
    }
  });

  const authorize = (...permissions) => async (req, res, next) => {
    if (!req.currentUser) {
      return res.status(401).json({ error: "Authentication required." });
    }
    if (!permissions.some((permission) => requirePermission(req.currentUser, permission))) {
      return res.status(403).json({ error: "You do not have permission to perform this action." });
    }
    return next();
  };

  const loginSchema = z.object({
    email: z.string().email(),
    password: z.string().min(8)
  });

  const assetQuerySchema = z.object({
    search: z.string().optional().default(""),
    status: z.string().optional().default(""),
    branch: z.string().optional().default(""),
    category: z.string().optional().default(""),
    page: z.coerce.number().optional().default(1),
    pageSize: z.coerce.number().optional().default(12)
  });

  const assetMutationSchema = z.object({
    assetId: z.string().min(1),
    tagCode: z.string().min(1),
    name: z.string().min(1),
    description: z.string().optional().default(""),
    categoryKey: z.string().min(1),
    branchCode: z.string().min(1),
    currency: z.enum(["USD", "CDF"]),
    acquisitionCost: z.coerce.number().nonnegative(),
    residualValue: z.union([z.coerce.number().nonnegative(), z.literal(""), z.null()]).optional(),
    capitalisationDate: z.string().min(10),
    usefulLifeMonths: z.coerce.number().int().nonnegative().optional(),
    depreciationMethod: z.enum(["SLM", "WDV", "NONE"]).optional(),
    netBookValue: z.union([z.coerce.number().nonnegative(), z.literal(""), z.null()]).optional(),
    status: z.string().optional().default("ACTIVE"),
    purchaseOrderRef: z.string().optional().default(""),
    warrantyExpiryDate: z.string().optional().default("")
  });

  const attachmentSchema = z.object({
    attachmentType: z.enum(["INVOICE", "PURCHASE_ORDER", "WARRANTY", "TRANSFER_FORM", "DISPOSAL_MEMO", "IMPAIRMENT_EVIDENCE", "PHOTO", "OTHER"]),
    fileName: z.string().min(1),
    referenceUrl: z.string().optional().default(""),
    note: z.string().optional().default("")
  });

  const assetWorkflowSchema = z.object({
    workflowType: z.enum(["TRANSFER", "DISPOSAL", "IMPAIRMENT"]),
    toBranchCode: z.string().optional().default(""),
    notes: z.string().optional().default("")
  });

  const importPreviewSchema = z.object({
    csvText: z.string().min(1)
  });

  const importCommitSchema = z.object({
    batchId: z.string().min(1)
  });

  const verificationSchema = z.object({
    tagCode: z.string().min(1),
    outcome: z.enum(["CONFIRMED", "NOT_FOUND", "CONDITION_ISSUE"]),
    notes: z.string().optional().default("")
  });

  const verificationSyncSchema = z.object({
    items: z.array(z.object({
      tagCode: z.string().min(1),
      outcome: z.enum(["CONFIRMED", "NOT_FOUND", "CONDITION_ISSUE"]),
      notes: z.string().optional().default(""),
      capturedAt: z.string().optional().default("")
    })).min(1)
  });

  const parallelRunSchema = z.object({
    period: z.string().optional(),
    sourceName: z.string().optional(),
    legacyDepreciationUSD: z.coerce.number(),
    legacyDepreciationCDF: z.coerce.number(),
    legacyAssetCount: z.coerce.number().int()
  });

  const enqueueJobSchema = z.object({
    jobType: z.enum(["monthly-depreciation", "daily-reconciliation", "parallel-run-compare"]),
    payload: z.record(z.any()).optional().default({}),
    runAfter: z.string().optional(),
    maxAttempts: z.coerce.number().int().optional().default(3)
  });

  const approvalQuerySchema = z.object({
    status: z.string().optional().default(""),
    entityType: z.string().optional().default("")
  });

  const approvalDecisionSchema = z.object({
    notes: z.string().optional().default("")
  });

  const userSchema = z.object({
    email: z.string().email().optional(),
    password: z.string().min(8).optional(),
    name: z.string().min(1),
    role: z.enum(["finance_admin", "operations", "admin_user", "auditor", "it_admin"]),
    homeBranchCode: z.string().optional().default(""),
    branchCodes: z.array(z.string()).optional().default([]),
    isActive: z.boolean().optional().default(true),
    phoneNumber: z.string().optional().default("")
  });

  const changePasswordSchema = z.object({
    currentPassword: z.string().min(8),
    newPassword: z.string().min(8)
  });

  app.get("/api/health", withHandler(async (_req, res) => {
    try {
      await db.pingDatabase(env.databaseUrl);
      res.status(200).json({
        status: "ok",
        service: "pesa-fams-backend",
        mode: "database",
        database: { status: "up" }
      });
    } catch (_error) {
      res.status(503).json({
        status: "degraded",
        service: "pesa-fams-backend",
        mode: "database",
        database: {
          status: "down",
          message: "Database connection check failed."
        }
      });
    }
  }));

  app.get("/api/bootstrap", (_req, res) => {
    if (!env.demoCredentialsEnabled) {
      return res.status(200).json({ demoUsers: [], branchCount: 0, assetCount: 0 });
    }
    return res.status(200).json({
      demoUsers: [
        { email: "finance@bankdrc.cd", password: "Finance123!", role: "Finance Administrator", branchName: "Head Office (Kinshasa HQ)" },
        { email: "operations@bankdrc.cd", password: "Ops123!", role: "Operations User", branchName: "Gombe Branch" },
        { email: "admin@bankdrc.cd", password: "Admin123!", role: "Admin User", branchName: "Lubumbashi Branch" },
        { email: "auditor@bankdrc.cd", password: "Audit123!", role: "Auditor / Reviewer", branchName: "Head Office (Kinshasa HQ)" },
        { email: "it@bankdrc.cd", password: "ITAdmin123!", role: "IT Administrator", branchName: "Head Office (Kinshasa HQ)" }
      ]
    });
  });

  app.post("/api/auth/login", withHandler(async (req, res) => {
    const input = validate(loginSchema)(req);
    res.status(200).json(await systemService.login(env, input));
  }));

  app.get("/api/meta", requireAuth, withHandler(async (req, res) => {
    res.status(200).json(await systemService.meta(env, req.currentUser));
  }));

  app.get("/api/dashboard", requireAuth, withHandler(async (req, res) => {
    res.status(200).json(await systemService.dashboard(env, req.currentUser));
  }));

  app.get("/api/assets", requireAuth, withHandler(async (req, res) => {
    const query = validate(assetQuerySchema, "query")(req);
    res.status(200).json(await systemService.listAssets(env, req.currentUser, {
      search: query.search,
      status: query.status,
      branchCode: query.branch,
      categoryKey: query.category,
      page: query.page,
      pageSize: query.pageSize
    }));
  }));

  app.get("/api/assets/:id", requireAuth, withHandler(async (req, res) => {
    res.status(200).json(await systemService.getAsset(env, req.currentUser, req.params.id));
  }));

  app.post("/api/assets", requireAuth, authorize("assets.create", "assets.create.branch"), withHandler(async (req, res) => {
    const input = validate(assetMutationSchema)(req);
    res.status(200).json(await systemService.createAsset(env, req.currentUser, input));
  }));

  app.patch("/api/assets/:id", requireAuth, authorize("assets.update", "assets.update.branch"), withHandler(async (req, res) => {
    const input = validate(assetMutationSchema)(req);
    res.status(200).json(await systemService.updateAsset(env, req.currentUser, req.params.id, input));
  }));

  app.post("/api/assets/:id/attachments", requireAuth, authorize("assets.attach", "assets.attach.branch", "assets.update", "assets.update.branch"), withHandler(async (req, res) => {
    const input = validate(attachmentSchema)(req);
    res.status(200).json(await systemService.addAssetAttachment(env, req.currentUser, req.params.id, input));
  }));

  app.post("/api/assets/:id/workflows", requireAuth, authorize("workflows.advance", "workflows.advance.branch", "assets.update", "assets.update.branch"), withHandler(async (req, res) => {
    const input = validate(assetWorkflowSchema)(req);
    res.status(200).json(await systemService.createAssetWorkflow(env, req.currentUser, req.params.id, input));
  }));

  app.post("/api/assets/import-preview", requireAuth, authorize("assets.import", "assets.import.branch", "assets.create", "assets.create.branch"), withHandler(async (req, res) => {
    const input = validate(importPreviewSchema)(req);
    res.status(200).json(await systemService.previewAssetImport(env, req.currentUser, input.csvText));
  }));

  app.post("/api/assets/import-commit", requireAuth, authorize("assets.import", "assets.import.branch", "assets.create", "assets.create.branch"), withHandler(async (req, res) => {
    const input = validate(importCommitSchema)(req);
    res.status(200).json(await systemService.commitAssetImport(env, req.currentUser, input.batchId));
  }));

  app.get("/api/imports", requireAuth, authorize("assets.import", "assets.import.branch", "assets.create", "assets.create.branch"), withHandler(async (req, res) => {
    res.status(200).json(await systemService.listImports(env, req.currentUser));
  }));

  app.get("/api/lifecycle", requireAuth, withHandler(async (req, res) => {
    res.status(200).json(await systemService.listLifecycle(env, req.currentUser));
  }));

  app.post("/api/lifecycle/:id/advance", requireAuth, authorize("workflows.advance", "workflows.advance.branch"), withHandler(async (req, res) => {
    res.status(200).json(await systemService.advanceWorkflow(env, req.currentUser, req.params.id));
  }));

  app.get("/api/depreciation", requireAuth, withHandler(async (_req, res) => {
    res.status(200).json(await systemService.getDepreciation(env));
  }));

  app.post("/api/depreciation/run", requireAuth, authorize("depreciation.run"), withHandler(async (req, res) => {
    res.status(200).json(await systemService.runDepreciation(env, req.currentUser, req.body?.period));
  }));

  app.post("/api/depreciation/approve", requireAuth, authorize("depreciation.approve"), withHandler(async (req, res) => {
    res.status(200).json(await systemService.approveDepreciation(env, req.currentUser, req.body?.runId, req.body?.notes || ""));
  }));

  app.post("/api/depreciation/retry-failures", requireAuth, authorize("depreciation.retry_failures"), withHandler(async (req, res) => {
    res.status(200).json(await systemService.retryDepreciationFailures(env, req.currentUser, req.body?.runId));
  }));

  app.get("/api/reconciliation", requireAuth, withHandler(async (_req, res) => {
    res.status(200).json(await systemService.getReconciliation(env));
  }));

  app.post("/api/reconciliation/run", requireAuth, authorize("reconciliation.run"), withHandler(async (req, res) => {
    res.status(200).json(await systemService.runReconciliation(env, req.currentUser, req.body?.period));
  }));

  app.get("/api/reports", requireAuth, authorize("reports.read"), withHandler(async (_req, res) => {
    res.status(200).json(await systemService.listReports());
  }));

  app.post("/api/reports/:id/generate", requireAuth, authorize("reports.export", "reports.export.branch"), withHandler(async (req, res) => {
    res.status(200).json(await systemService.generateReport(env, req.currentUser, req.params.id));
  }));

  app.get("/api/reports/:id/export.csv", requireAuth, authorize("reports.export", "reports.export.branch"), withHandler(async (req, res) => {
    const exported = await systemService.exportReportCsv(env, req.currentUser, req.params.id);
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${exported.filename}"`);
    res.status(200).send(exported.csv);
  }));

  app.get("/api/reports/:id/export.excel.xml", requireAuth, authorize("reports.export", "reports.export.branch"), withHandler(async (req, res) => {
    const exported = await systemService.exportReportExcel(env, req.currentUser, req.params.id);
    res.setHeader("Content-Type", "application/vnd.ms-excel; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${exported.filename}"`);
    res.status(200).send(exported.xml);
  }));

  app.get("/api/reports/:id/export.print.html", requireAuth, authorize("reports.export", "reports.export.branch"), withHandler(async (req, res) => {
    const exported = await systemService.exportReportPrintHtml(env, req.currentUser, req.params.id);
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.setHeader("Content-Disposition", `inline; filename="${exported.filename}"`);
    res.status(200).send(exported.html);
  }));

  app.get("/api/audit", requireAuth, authorize("audit.read"), withHandler(async (req, res) => {
    res.status(200).json(await systemService.listAudit(env, req.currentUser, {
      user: String(req.query.user || ""),
      action: String(req.query.action || "")
    }));
  }));

  app.get("/api/admin/users", requireAuth, authorize("admin.users.read", "admin.users.manage"), withHandler(async (req, res) => {
    res.status(200).json(await systemService.listUsers(env, req.currentUser));
  }));

  app.post("/api/admin/users", requireAuth, authorize("admin.users.manage"), withHandler(async (req, res) => {
    const input = validate(userSchema)(req);
    res.status(200).json(await systemService.saveUser(env, req.currentUser, input));
  }));

  app.patch("/api/admin/users/:id", requireAuth, authorize("admin.users.manage"), withHandler(async (req, res) => {
    const input = validate(userSchema)(req);
    res.status(200).json(await systemService.saveUser(env, req.currentUser, input, req.params.id));
  }));

  app.post("/api/account/change-password", requireAuth, withHandler(async (req, res) => {
    const input = validate(changePasswordSchema)(req);
    res.status(200).json(await systemService.changePassword(env, req.currentUser, input));
  }));

  app.get("/api/approvals", requireAuth, authorize("approvals.read", "approvals.read.branch"), withHandler(async (req, res) => {
    const query = validate(approvalQuerySchema, "query")(req);
    res.status(200).json(await systemService.listApprovals(env, req.currentUser, query));
  }));

  app.post("/api/approvals/:id/approve", requireAuth, authorize("approvals.decide"), withHandler(async (req, res) => {
    const input = validate(approvalDecisionSchema)(req);
    res.status(200).json(await systemService.approveRequest(env, req.currentUser, req.params.id, input.notes));
  }));

  app.post("/api/approvals/:id/reject", requireAuth, authorize("approvals.decide"), withHandler(async (req, res) => {
    const input = validate(approvalDecisionSchema)(req);
    res.status(200).json(await systemService.rejectRequest(env, req.currentUser, req.params.id, input.notes));
  }));

  app.get("/api/verification/lookup/:tagCode", requireAuth, withHandler(async (req, res) => {
    res.status(200).json(await systemService.lookupVerificationAsset(env, req.currentUser, req.params.tagCode));
  }));

  app.get("/api/verification/queue", requireAuth, withHandler(async (req, res) => {
    res.status(200).json(await systemService.listVerificationQueue(env, req.currentUser));
  }));

  app.post("/api/verification/submit", requireAuth, authorize("assets.verify"), withHandler(async (req, res) => {
    const input = validate(verificationSchema)(req);
    res.status(200).json(await systemService.submitVerification(env, req.currentUser, input));
  }));

  app.post("/api/verification/sync", requireAuth, authorize("assets.verify"), withHandler(async (req, res) => {
    const input = validate(verificationSyncSchema)(req);
    res.status(200).json(await systemService.syncOfflineVerifications(env, req.currentUser, input.items));
  }));

  app.get("/api/parallel-runs/latest", requireAuth, authorize("parallel_runs.read"), withHandler(async (_req, res) => {
    res.status(200).json(await systemService.getLatestParallelRun(env));
  }));

  app.post("/api/parallel-runs/compare", requireAuth, authorize("parallel_runs.create"), withHandler(async (req, res) => {
    const input = validate(parallelRunSchema)(req);
    res.status(200).json(await systemService.createParallelRun(env, req.currentUser, input));
  }));

  app.post("/api/jobs/enqueue", requireAuth, authorize("jobs.enqueue"), withHandler(async (req, res) => {
    const input = validate(enqueueJobSchema)(req);
    res.status(200).json(await systemService.enqueueJob(env, req.currentUser, input));
  }));

  app.use((req, res, next) => {
    if (req.path.startsWith("/api/")) {
      return next();
    }
    return res.sendFile(path.join(__dirname, "..", "public", "index.html"));
  });

  return app;
}

module.exports = {
  createProductionApp
};
