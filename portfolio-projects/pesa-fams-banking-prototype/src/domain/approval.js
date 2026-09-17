const approvalStatusTone = {
  PENDING: "warning",
  APPROVED: "success",
  REJECTED: "danger",
  CANCELLED: "muted"
};

const approvalEntityLabel = {
  depreciation_run: "Depreciation Run",
  workflow_request: "Workflow Request"
};

const approvalActionLabel = {
  DEPRECIATION_POST: "Post depreciation batch",
  TRANSFER_APPROVAL: "Approve transfer movement",
  TRANSFER_RECEIPT: "Confirm destination receipt",
  DISPOSAL_APPROVAL: "Approve disposal",
  IMPAIRMENT_APPROVAL: "Approve impairment"
};

function canApproveOwnRequest(requestedByUserId, checkerUserId) {
  if (!requestedByUserId || !checkerUserId) return true;
  return requestedByUserId !== checkerUserId;
}

function workflowApprovalAction(workflow) {
  if (workflow.workflow_type === "TRANSFER" && workflow.status === "IN_TRANSIT") {
    return "TRANSFER_RECEIPT";
  }
  if (workflow.workflow_type === "TRANSFER") {
    return "TRANSFER_APPROVAL";
  }
  if (workflow.workflow_type === "DISPOSAL") {
    return "DISPOSAL_APPROVAL";
  }
  return "IMPAIRMENT_APPROVAL";
}

function workflowNextStatus(workflow) {
  if (workflow.workflow_type === "TRANSFER" && workflow.status === "PENDING_TRANSFERS") {
    return "IN_TRANSIT";
  }
  return "COMPLETED";
}

function approvalStatusLabel(status) {
  return status
    .toLowerCase()
    .replaceAll("_", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

module.exports = {
  approvalStatusTone,
  approvalEntityLabel,
  approvalActionLabel,
  canApproveOwnRequest,
  workflowApprovalAction,
  workflowNextStatus,
  approvalStatusLabel
};
