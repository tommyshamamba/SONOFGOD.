CREATE TABLE approval_requests (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type TEXT NOT NULL CHECK (entity_type IN ('depreciation_run', 'workflow_request')),
  entity_id UUID NOT NULL,
  action_type TEXT NOT NULL,
  title TEXT NOT NULL,
  detail TEXT NOT NULL,
  branch_id UUID REFERENCES branches(id),
  requested_by_user_id UUID REFERENCES users(id),
  approved_by_user_id UUID REFERENCES users(id),
  rejected_by_user_id UUID REFERENCES users(id),
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED', 'CANCELLED')),
  decision_notes TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  approved_at TIMESTAMPTZ,
  rejected_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX idx_approval_requests_pending_unique
  ON approval_requests(entity_type, entity_id, action_type)
  WHERE status = 'PENDING';

CREATE INDEX idx_approval_requests_status_requested_at
  ON approval_requests(status, requested_at DESC);

ALTER TABLE depreciation_runs
  DROP CONSTRAINT IF EXISTS depreciation_runs_status_check;

ALTER TABLE depreciation_runs
  ADD CONSTRAINT depreciation_runs_status_check
  CHECK (status IN ('DRAFT', 'PENDING_APPROVAL', 'REJECTED', 'POSTED'));

ALTER TABLE depreciation_lines
  ADD COLUMN IF NOT EXISTS posting_reference TEXT,
  ADD COLUMN IF NOT EXISTS posted_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
