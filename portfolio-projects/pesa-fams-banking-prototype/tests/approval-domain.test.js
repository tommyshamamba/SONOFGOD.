const test = require("node:test");
const assert = require("node:assert/strict");

const {
  canApproveOwnRequest,
  workflowApprovalAction,
  workflowNextStatus,
  approvalStatusLabel
} = require("../src/domain/approval");

test("maker checker prevents the same user from approving their own request", () => {
  assert.equal(canApproveOwnRequest("user-a", "user-a"), false);
  assert.equal(canApproveOwnRequest("user-a", "user-b"), true);
});

test("workflow approval action is derived from workflow type and stage", () => {
  assert.equal(workflowApprovalAction({ workflow_type: "TRANSFER", status: "PENDING_TRANSFERS" }), "TRANSFER_APPROVAL");
  assert.equal(workflowApprovalAction({ workflow_type: "TRANSFER", status: "IN_TRANSIT" }), "TRANSFER_RECEIPT");
  assert.equal(workflowApprovalAction({ workflow_type: "DISPOSAL", status: "PENDING_DISPOSALS" }), "DISPOSAL_APPROVAL");
});

test("workflow next status follows the transfer handoff rules", () => {
  assert.equal(workflowNextStatus({ workflow_type: "TRANSFER", status: "PENDING_TRANSFERS" }), "IN_TRANSIT");
  assert.equal(workflowNextStatus({ workflow_type: "TRANSFER", status: "IN_TRANSIT" }), "COMPLETED");
  assert.equal(workflowNextStatus({ workflow_type: "DISPOSAL", status: "PENDING_DISPOSALS" }), "COMPLETED");
});

test("approval status labels are human readable", () => {
  assert.equal(approvalStatusLabel("PENDING_APPROVAL"), "Pending Approval");
  assert.equal(approvalStatusLabel("REJECTED"), "Rejected");
});
