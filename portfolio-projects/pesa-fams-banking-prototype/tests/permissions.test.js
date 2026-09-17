const test = require("node:test");
const assert = require("node:assert/strict");
const { hasPermission, scopeForAssets } = require("../src/domain/permissions");

test("finance admin has approval permissions", () => {
  assert.equal(hasPermission("finance_admin", "depreciation.approve"), true);
  assert.equal(hasPermission("finance_admin", "reconciliation.run"), true);
});

test("auditor is read-only for assets scope", () => {
  assert.equal(hasPermission("auditor", "depreciation.approve"), false);
  assert.equal(scopeForAssets({ role: "auditor" }), "all");
});

test("operations users are branch-scoped", () => {
  assert.equal(hasPermission("operations", "assets.read.branch"), true);
  assert.equal(scopeForAssets({ role: "operations" }), "branch");
});

test("it admin has user management and demo reset permissions", () => {
  assert.equal(hasPermission("it_admin", "admin.users.manage"), true);
  assert.equal(hasPermission("it_admin", "demo.reset"), true);
});
