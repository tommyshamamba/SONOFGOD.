const permissionsMatrix = {
  finance_admin: [
    "meta.read",
    "dashboard.read",
    "assets.read",
    "assets.create",
    "assets.update",
    "assets.import",
    "assets.attach",
    "assets.verify",
    "workflows.read",
    "workflows.advance",
    "depreciation.read",
    "depreciation.run",
    "depreciation.approve",
    "depreciation.retry_failures",
    "reconciliation.read",
    "reconciliation.run",
    "reports.read",
    "reports.export",
    "audit.read",
    "approvals.read",
    "approvals.decide",
    "parallel_runs.read",
    "parallel_runs.create",
    "admin.users.read",
    "admin.users.manage",
    "account.password.change",
    "jobs.read",
    "jobs.enqueue",
    "demo.reset"
  ],
  operations: [
    "meta.read",
    "dashboard.read",
    "assets.read.branch",
    "assets.verify",
    "workflows.read.branch",
    "workflows.advance.branch",
    "reports.read",
    "reports.export.branch",
    "account.password.change"
  ],
  admin_user: [
    "meta.read",
    "dashboard.read",
    "assets.read.branch",
    "assets.create.branch",
    "assets.update.branch",
    "assets.import.branch",
    "assets.attach.branch",
    "assets.verify",
    "workflows.read.branch",
    "workflows.advance.branch",
    "reports.read",
    "reports.export.branch",
    "approvals.read.branch",
    "account.password.change"
  ],
  auditor: [
    "meta.read",
    "dashboard.read",
    "assets.read",
    "depreciation.read",
    "reconciliation.read",
    "reports.read",
    "reports.export",
    "audit.read",
    "approvals.read",
    "parallel_runs.read",
    "account.password.change"
  ],
  it_admin: [
    "meta.read",
    "dashboard.read",
    "assets.read",
    "assets.create",
    "assets.update",
    "assets.import",
    "assets.attach",
    "workflows.read",
    "depreciation.read",
    "depreciation.run",
    "depreciation.retry_failures",
    "reconciliation.read",
    "reconciliation.run",
    "reports.read",
    "audit.read",
    "approvals.read",
    "parallel_runs.read",
    "admin.users.read",
    "admin.users.manage",
    "account.password.change",
    "jobs.read",
    "jobs.enqueue",
    "jobs.process",
    "demo.reset"
  ]
};

function hasPermission(role, permission) {
  return (permissionsMatrix[role] || []).includes(permission);
}

function requirePermission(user, permission) {
  return hasPermission(user.role, permission);
}

function scopeForAssets(user) {
  if (hasPermission(user.role, "assets.read")) return "all";
  if (hasPermission(user.role, "assets.read.branch")) return "branch";
  return "none";
}

module.exports = {
  permissionsMatrix,
  hasPermission,
  requirePermission,
  scopeForAssets
};
