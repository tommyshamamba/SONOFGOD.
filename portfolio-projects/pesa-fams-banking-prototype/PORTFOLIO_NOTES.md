# Banking prototype source snapshot

PESA FAMS is a fixed-asset management prototype with a demo runtime and a PostgreSQL-backed implementation path. The source includes demo data generation, asset workflows, depreciation, approvals and reports. This is not a claim of a live bank deployment or production readiness.

The local .env file, tunnel logs and unrelated nested Interview Nailer project are excluded. Demo credentials in the source and tests are examples for local prototype use only. Configure unique credentials before any deployment. See docs/PROTOTYPE_TO_PRODUCTION.md for the original implementation notes.

Validation: all 22 existing Node tests passed during the source import using the locally available dependency installation. These cover demo workflows, permissions, depreciation, reporting, CSV parsing and mocked database health. A live PostgreSQL deployment was not tested.
