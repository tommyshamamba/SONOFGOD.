## Production Architecture

### Runtime modes

- `prototype`: in-memory mock mode used when `DATABASE_URL` is not set.
- `database`: PostgreSQL-backed mode used when `APP_MODE=database` and `DATABASE_URL` is configured.

### Core modules

- `src/create-production-app.js`
  Exposes the real HTTP API with JWT auth, zod validation, and permission guards.
- `src/services/systemService.js`
  Holds business workflows and transaction orchestration.
- `src/repositories/*`
  Encapsulates SQL access for users, assets, finance, workflows, audit, jobs, and parallel-run sessions.
- `src/domain/*`
  Permission matrix and depreciation engine logic.
- `db/migrations/001_schema.sql`
  Relational schema for assets, GL data, audit, jobs, verification, workflows, and parallel runs.

### Main persisted entities

- `branches`
- `users`
- `user_branch_access`
- `asset_category_policies`
- `gl_accounts`
- `gl_balances`
- `assets`
- `workflow_requests`
- `exchange_rates`
- `depreciation_runs`
- `depreciation_lines`
- `reconciliation_runs`
- `reconciliation_lines`
- `verification_events`
- `parallel_run_sessions`
- `job_queue`
- `audit_logs`

### Operational flow

1. Users authenticate with JWT against the `users` table.
2. Branch-scoped roles are filtered through `user_branch_access`.
3. Writes run inside database transactions.
4. Every material action writes to `audit_logs`.
5. Background jobs are stored in `job_queue` and executed by `scripts/worker.js`.
6. Parallel-run comparisons persist side-by-side legacy vs system snapshots for cutover control.

### Next production steps after this foundation

1. Add refresh-token/session revocation handling.
2. Add real Finacle integration adapters.
3. Add attachment/document storage for workflow evidence.
4. Add PDF reporting and BI connectors.
5. Add observability, metrics, and operational alerting.
