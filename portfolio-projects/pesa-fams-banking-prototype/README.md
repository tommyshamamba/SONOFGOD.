# PESA FAMS

For the tested local setup, see the [local demo guide](../../docs/LOCAL_DEMOS.md) and [latest verification results](../../docs/LOCAL_DEMO_VERIFICATION.md).

A fixed-asset management demonstration with synthetic bank/branch records and two runtime modes. Use Node 24.19.0 and, for database mode, PostgreSQL:

- `prototype` mode for fast demo work without PostgreSQL
- `database` mode for the real PostgreSQL-backed MVP foundation

## Quick start

### Prototype mode

```powershell
npm ci
$env:APP_MODE = "prototype"
$env:HOST = "127.0.0.1"
node src/server.js
```

Open `http://localhost:3100`.

The documented `HOST=127.0.0.1` setting serves this computer only. For an intentional demonstration on a trusted LAN, set `HOST=0.0.0.0` before starting and use the host computer's LAN address. Keep synthetic credentials and data confined to the demonstration.

### Database mode

If you previously selected prototype mode in PowerShell, run `$env:APP_MODE = "database"` before starting the database-backed server. Use a disposable local database for setup and seed commands.

1. Copy `.env.example` to `.env`
2. Run a preflight check:

```powershell
node scripts/db-preflight.js
```

3. Start PostgreSQL, run migrations, and seed data:

```powershell
node scripts/db-up.js
node scripts/migrate.js
node scripts/seed.js
```

4. Start the server:

```powershell
node src/server.js
```

The address is determined by `HOST` and `PORT`. Use `HOST=127.0.0.1` for a local database demonstration. LAN access requires an intentional non-loopback bind and appropriate network controls.

5. Run smoke checks:

```powershell
node scripts/db-smoke.js
```

6. Optional job runner:

```powershell
node scripts/worker.js
```

If Docker is not installed, the preflight script will tell you early and the app will stay in a clear failed state instead of silently dropping back to prototype mode.

## Notes on LAN access

- `localhost` only works on the same machine that is running the app.
- Another PC can connect only when the server is intentionally bound to a LAN interface; the quickstart's loopback address is local-only.
- If another PC still cannot open the app, Windows Firewall is usually the next thing to check for Node.js or port `3100`.

## Demo users

- `finance@bankdrc.cd` / `Finance123!`
- `operations@bankdrc.cd` / `Ops123!`
- `admin@bankdrc.cd` / `Admin123!`
- `auditor@bankdrc.cd` / `Audit123!`
- `it@bankdrc.cd` / `ITAdmin123!`

## Implemented database-backed paths

These features have source implementations. Live PostgreSQL integration and deployment require separate validation. Seeded asset and branch counts below describe generated demo data.

- PostgreSQL schema and migration runner
- Seed pipeline for 5,600 assets, 37 branches, users, GL balances, workflows, audit logs, and depreciation history
- JWT authentication against real user records
- Branch-scoped RBAC with permissions matrix
- Transaction-safe workflow advancement
- Depreciation run creation and approval with persisted lines
- Maker-checker approval queue for depreciation posting and branch workflow actions
- Failed posting retry workflow for depreciation exceptions
- Reconciliation runs persisted to the database
- Parallel-run comparison storage
- Background job queue and worker foundation
- CSV report export endpoint
- Excel-friendly XML workbook export for reports
- Print-ready HTML report packs for browser-to-PDF output
- Asset onboarding create/edit flow backed by PostgreSQL
- CSV-from-Excel asset import preview, validation, commit logs, and import history
- Attachment and supporting-evidence tracking on each asset
- User administration with branch access assignment and password change flow
- Camera-assisted verification screen with offline queue sync
- Dashboard alert cards for approvals, failed postings, unverified assets, and fully depreciated assets

## Demo reset

**These commands reset the configured database. Use only a disposable demo database whose records you intend to replace.** To restore its generated showcase data:

```powershell
node scripts/seed.js
```

or:

```powershell
npm.cmd run db:reset-demo
```

## Verification

```powershell
node --test --test-isolation=none
```

## Notes on maker-checker flow

- Run depreciation as `it@bankdrc.cd` or another maker account.
- Approve and post it as `finance@bankdrc.cd`.
- If a user prepares a batch, that same user cannot approve it.
- After applying migration `002_maker_checker_and_posting.sql`, reseed with `node scripts/seed.js` if you want demo approval items to appear immediately.
- After applying migration `003_onboarding_admin_and_imports.sql`, reseed with `node scripts/seed.js` if you want sample attachments and import history to appear immediately.

## Notes on report exports

- Each report now supports preview, CSV, Excel-friendly XML, and print-ready HTML.
- The print-ready HTML export is intended for browser print-to-PDF workflows.
- If you are already running the server, restart it before testing the new report endpoints and UI buttons.


## PostgreSQL integration verification

Use a disposable database named `pesa_test_integration` (never a customer database).
Set `DATABASE_URL` to its PostgreSQL connection URL and `APP_MODE=database`.
Run `npm run db:migrate` and `npm run db:seed`, then set `TEST_DATABASE_URL`
to the same URL and run `npm test`. The integration suite requires a
`pesa_test_` database name and uses synthetic seed credentials. Without
`TEST_DATABASE_URL`, those integration cases are skipped. GitHub Actions creates
and seeds an isolated PostgreSQL service automatically. The seed command is for
disposable demo/test databases only.

The suite contains seven PostgreSQL integration cases, including asset writes, branch isolation, concurrent maker/checker approval, rollback and disabled-user sessions. Other tests cover job-payload validation, pagination, malformed requests, authentication, report exports and financial calculations. Consult the [current evidence report](../../docs/VERIFICATION.md) for observed counts and the tested commit. No test result establishes regulatory compliance, live bank integration or complete financial certification.
