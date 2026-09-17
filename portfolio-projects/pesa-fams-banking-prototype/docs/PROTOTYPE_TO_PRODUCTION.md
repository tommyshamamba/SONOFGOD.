## Prototype to Production Roadmap

### What this prototype covers
- Executive dashboard with 5,600 generated assets across 37 locations.
- Searchable asset registry with branch scoping for operations users.
- Click-through asset details with a printable label layout and depreciation schedule.
- Depreciation run and approval flow with role restrictions.
- GL reconciliation view with exception lines.
- Lifecycle workflow board for transfers, disposals, and impairment actions.
- Regulatory report previews.
- Audit trail and physical verification flow.

### What turns this into the full system
1. Replace generated data with PostgreSQL tables and seed/migration scripts.
2. Move authentication from demo credentials to real user records and password hashing.
3. Split the prototype endpoints into production modules:
   - `auth`
   - `assets`
   - `depreciation`
   - `gl-reconciliation`
   - `reports`
   - `audit`
   - `verification`
4. Introduce persistent workflow tables for transfers, disposals, impairment, and approvals.
5. Add CSV import/export, PDF/Excel generation, and real QR/barcode output.
6. Integrate with Finacle through the bank-approved API or file interface.
7. Add job scheduling for month-end depreciation and daily reconciliation.
8. Add database-backed audit retention and immutable controls.

### Recommended delivery phases
1. `Prototype`:
   - Use this build for demos and feedback.
   - Confirm user roles, branch list, depreciation policy, and report layout expectations.
2. `Core system`:
   - PostgreSQL schema, migrations, auth, asset CRUD, depreciation engine, audit log.
3. `Bank integration`:
   - Finacle posting, GL reconciliation import, exchange-rate management, operational alerts.
4. `Go-live readiness`:
   - Data migration, UAT, branch verification rollout, production hosting, monitoring.

### Bank decisions still needed
- Exact depreciation rules currently used in the Excel register.
- Final GL account codes and posting rules in Finacle.
- Finacle integration method available in Bank DRC's licensed environment.
- Hosting preference and security constraints.
- Ownership of historical data cleansing before migration.
