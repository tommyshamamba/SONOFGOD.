CREATE EXTENSION IF NOT EXISTS "pgcrypto";

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'user_role') THEN
    CREATE TYPE user_role AS ENUM ('finance_admin', 'operations', 'admin_user', 'auditor', 'it_admin');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'currency_code') THEN
    CREATE TYPE currency_code AS ENUM ('CDF', 'USD');
  END IF;
END $$;

CREATE TABLE branches (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  code TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  city TEXT NOT NULL,
  province TEXT NOT NULL,
  is_head_office BOOLEAN NOT NULL DEFAULT FALSE,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE asset_category_policies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  category_key TEXT NOT NULL UNIQUE,
  label TEXT NOT NULL,
  default_depreciation_method TEXT NOT NULL CHECK (default_depreciation_method IN ('SLM', 'WDV', 'NONE')),
  default_useful_life_months INTEGER NOT NULL,
  residual_rate NUMERIC(10, 4) NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE gl_accounts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  code TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  account_type TEXT NOT NULL CHECK (account_type IN ('asset', 'depreciation_expense', 'accumulated_depreciation', 'gain_loss')),
  category_key TEXT,
  currency currency_code,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_gl_accounts_code ON gl_accounts(code);

CREATE TABLE gl_balances (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  period TEXT NOT NULL,
  gl_code TEXT NOT NULL,
  label TEXT NOT NULL,
  currency currency_code NOT NULL,
  balance NUMERIC(18, 2) NOT NULL,
  source TEXT NOT NULL DEFAULT 'FINACLE',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (period, gl_code, currency)
);

CREATE INDEX idx_gl_balances_period ON gl_balances(period);

CREATE TABLE users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  name TEXT NOT NULL,
  role user_role NOT NULL,
  home_branch_id UUID REFERENCES branches(id),
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  last_login_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE user_branch_access (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  branch_id UUID NOT NULL REFERENCES branches(id) ON DELETE CASCADE,
  access_type TEXT NOT NULL DEFAULT 'primary',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (user_id, branch_id)
);

CREATE TABLE assets (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  asset_id TEXT NOT NULL UNIQUE,
  tag_code TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  description TEXT,
  category_key TEXT NOT NULL REFERENCES asset_category_policies(category_key),
  branch_id UUID NOT NULL REFERENCES branches(id),
  currency currency_code NOT NULL,
  acquisition_cost NUMERIC(18, 2) NOT NULL,
  residual_value NUMERIC(18, 2) NOT NULL DEFAULT 0,
  capitalisation_date DATE NOT NULL,
  useful_life_months INTEGER NOT NULL,
  depreciation_method TEXT NOT NULL CHECK (depreciation_method IN ('SLM', 'WDV', 'NONE')),
  accumulated_depreciation NUMERIC(18, 2) NOT NULL DEFAULT 0,
  impairment_loss NUMERIC(18, 2) NOT NULL DEFAULT 0,
  net_book_value NUMERIC(18, 2) NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'ACTIVE', 'TRANSFERRED', 'IMPAIRED', 'REVALUED', 'HELD_FOR_SALE', 'DISPOSED')),
  gl_asset_account_id UUID REFERENCES gl_accounts(id),
  gl_depreciation_expense_account_id UUID REFERENCES gl_accounts(id),
  gl_accumulated_depreciation_account_id UUID REFERENCES gl_accounts(id),
  purchase_order_ref TEXT,
  warranty_expiry_date DATE,
  last_verified_at DATE,
  created_by_user_id UUID REFERENCES users(id),
  updated_by_user_id UUID REFERENCES users(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_assets_branch_status ON assets(branch_id, status);
CREATE INDEX idx_assets_category ON assets(category_key);
CREATE INDEX idx_assets_asset_id ON assets(asset_id);
CREATE INDEX idx_assets_tag_code ON assets(tag_code);

CREATE TABLE workflow_requests (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_type TEXT NOT NULL CHECK (workflow_type IN ('TRANSFER', 'DISPOSAL', 'IMPAIRMENT')),
  status TEXT NOT NULL CHECK (status IN ('PENDING_TRANSFERS', 'IN_TRANSIT', 'PENDING_DISPOSALS', 'PENDING_IMPAIRMENTS', 'COMPLETED')),
  asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
  asset_branch_id UUID NOT NULL REFERENCES branches(id),
  to_branch_id UUID REFERENCES branches(id),
  requested_by_user_id UUID REFERENCES users(id),
  decided_by_user_id UUID REFERENCES users(id),
  notes TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  decided_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_workflow_requests_status ON workflow_requests(status);
CREATE INDEX idx_workflow_requests_asset_branch ON workflow_requests(asset_branch_id);

CREATE TABLE exchange_rates (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  period TEXT NOT NULL,
  currency currency_code NOT NULL,
  rate_to_cdf NUMERIC(18, 6) NOT NULL,
  effective_date DATE NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (period, currency)
);

CREATE TABLE depreciation_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  period TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL CHECK (status IN ('DRAFT', 'POSTED')),
  exchange_rate_used NUMERIC(18, 6),
  total_assets_processed INTEGER NOT NULL DEFAULT 0,
  total_assets_skipped INTEGER NOT NULL DEFAULT 0,
  total_depreciation_usd NUMERIC(18, 2) NOT NULL DEFAULT 0,
  total_depreciation_cdf NUMERIC(18, 2) NOT NULL DEFAULT 0,
  failure_count INTEGER NOT NULL DEFAULT 0,
  run_by_user_id UUID REFERENCES users(id),
  approved_by_user_id UUID REFERENCES users(id),
  approved_at TIMESTAMPTZ,
  posted_at TIMESTAMPTZ,
  summary TEXT,
  gl_batch_reference TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE depreciation_lines (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  depreciation_run_id UUID NOT NULL REFERENCES depreciation_runs(id) ON DELETE CASCADE,
  asset_id UUID NOT NULL REFERENCES assets(id),
  opening_nbv NUMERIC(18, 2) NOT NULL,
  depreciation_charge NUMERIC(18, 2) NOT NULL,
  closing_nbv NUMERIC(18, 2) NOT NULL,
  currency currency_code NOT NULL,
  posting_status TEXT NOT NULL CHECK (posting_status IN ('PENDING', 'POSTED', 'FAILED', 'SKIPPED')),
  failure_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (depreciation_run_id, asset_id)
);

CREATE INDEX idx_depreciation_lines_run ON depreciation_lines(depreciation_run_id);

CREATE TABLE reconciliation_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  period TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('MATCHED', 'EXCEPTION')),
  fams_balance_usd NUMERIC(18, 2) NOT NULL,
  fams_balance_cdf NUMERIC(18, 2) NOT NULL,
  gl_balance_usd NUMERIC(18, 2) NOT NULL,
  gl_balance_cdf NUMERIC(18, 2) NOT NULL,
  variance_usd NUMERIC(18, 2) NOT NULL,
  variance_cdf NUMERIC(18, 2) NOT NULL,
  discrepancy_count INTEGER NOT NULL DEFAULT 0,
  run_by_user_id UUID REFERENCES users(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE reconciliation_lines (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  reconciliation_run_id UUID NOT NULL REFERENCES reconciliation_runs(id) ON DELETE CASCADE,
  gl_code TEXT NOT NULL,
  label TEXT NOT NULL,
  fams_usd NUMERIC(18, 2) NOT NULL DEFAULT 0,
  gl_usd NUMERIC(18, 2) NOT NULL DEFAULT 0,
  variance_usd NUMERIC(18, 2) NOT NULL DEFAULT 0,
  fams_cdf NUMERIC(18, 2) NOT NULL DEFAULT 0,
  gl_cdf NUMERIC(18, 2) NOT NULL DEFAULT 0,
  variance_cdf NUMERIC(18, 2) NOT NULL DEFAULT 0,
  status TEXT NOT NULL CHECK (status IN ('MATCHED', 'EXCEPTION'))
);

CREATE TABLE verification_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
  branch_id UUID NOT NULL REFERENCES branches(id),
  verified_by_user_id UUID REFERENCES users(id),
  outcome TEXT NOT NULL CHECK (outcome IN ('CONFIRMED', 'NOT_FOUND', 'CONDITION_ISSUE')),
  notes TEXT,
  verified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  is_offline_synced BOOLEAN NOT NULL DEFAULT TRUE,
  synced_at TIMESTAMPTZ
);

CREATE TABLE parallel_run_sessions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  period TEXT NOT NULL,
  source_name TEXT NOT NULL,
  source_snapshot JSONB NOT NULL,
  system_snapshot JSONB NOT NULL,
  variance_summary JSONB NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('UNDER_REVIEW', 'MATCHED', 'EXCEPTION')),
  created_by_user_id UUID REFERENCES users(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE job_queue (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_type TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')),
  run_after TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  locked_at TIMESTAMPTZ,
  locked_by TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  last_error TEXT,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_job_queue_status_run_after ON job_queue(status, run_after);

CREATE TABLE audit_logs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  actor_user_id UUID REFERENCES users(id),
  actor_name TEXT,
  actor_role TEXT,
  branch_id UUID REFERENCES branches(id),
  action TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  detail TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_logs_entity ON audit_logs(entity_type, entity_id);
CREATE INDEX idx_audit_logs_created_at ON audit_logs(created_at DESC);
