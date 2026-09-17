ALTER TABLE users
  ADD COLUMN IF NOT EXISTS password_changed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS phone_number TEXT;

CREATE TABLE IF NOT EXISTS asset_attachments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
  attachment_type TEXT NOT NULL CHECK (
    attachment_type IN (
      'INVOICE',
      'PURCHASE_ORDER',
      'WARRANTY',
      'TRANSFER_FORM',
      'DISPOSAL_MEMO',
      'IMPAIRMENT_EVIDENCE',
      'PHOTO',
      'OTHER'
    )
  ),
  file_name TEXT NOT NULL,
  reference_url TEXT,
  note TEXT,
  uploaded_by_user_id UUID REFERENCES users(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_asset_attachments_asset_id
  ON asset_attachments(asset_id, created_at DESC);

CREATE TABLE IF NOT EXISTS import_batches (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_type TEXT NOT NULL CHECK (source_type IN ('CSV')),
  status TEXT NOT NULL CHECK (status IN ('VALIDATED', 'IMPORTED', 'FAILED')),
  summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_by_user_id UUID REFERENCES users(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  imported_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS import_batch_rows (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  import_batch_id UUID NOT NULL REFERENCES import_batches(id) ON DELETE CASCADE,
  row_number INTEGER NOT NULL,
  asset_public_id TEXT,
  tag_code TEXT,
  status TEXT NOT NULL CHECK (status IN ('VALID', 'IMPORTED', 'ERROR', 'SKIPPED')),
  message TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_import_batch_rows_batch_id
  ON import_batch_rows(import_batch_id, row_number ASC);
