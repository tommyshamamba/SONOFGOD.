# Production-readiness checklist

Scope: applications, infrastructure examples and isolated legacy tests in this repository. The [current verification record](VERIFICATION.md) identifies the checked commit, commands, hosted results and remaining failures. The September reports are historical snapshots.

Automated tests establish the behaviors they exercise. They do not establish production readiness, commercial use, financial certification or profitable trading. Complete the remaining items for the specific application and environment being released.

## Implemented verification

- [x] Project catalog, contribution instructions, source checks and pinned dependency installations.
- [x] Production builds for the four frontends.
- [x] Trace ONNX execution tests, upload validation and frontend client/cart tests; pretrained-model execution is a separate optional check.
- [x] PESA FAMS migrations, synthetic seed data and real PostgreSQL tests covering authentication, writes, permissions, rollback and concurrent approvals.
- [x] Interview Nailer tests in file and PostgreSQL modes, including account isolation and mocked provider responses.
- [x] Blockchain and Kubernetes API tests; local Voice event persistence and duplicate-delivery tests.
- [x] Hosted jobs for container checks, Terraform validation and dependency audits.
- [x] Isolated Python regressions and Solidity execution in an in-memory VM; no external transactions.

Browser results and the overall workflow outcome are recorded in [current verification](VERIFICATION.md). A passing subset of jobs must not be described as a complete passing run.

## Apply separately to each application being released

### Reproducibility and scope

- [ ] Record application, owner, release commit, runtime versions and deployment environment.
- [ ] Identify which features are live, mocked, disabled or unsupported.
- [ ] Verify dependency installation from a fresh checkout and retain lockfiles.
- [ ] Review dependency/security findings and document remediation or justified exceptions.

### Correctness and security

- [ ] Core workflows, error paths and concurrent operations have behavioral tests.
- [ ] Authentication, expired/invalid tokens, role boundaries and cross-user access are tested.
- [ ] Production rejects development secrets, demo credentials and invalid configuration.
- [ ] Input limits, rate limits, provider timeouts and safe error responses are verified.
- [ ] Secrets and private documents are excluded from Git, logs and CI artifacts.
- [ ] Browser journeys, accessibility basics and supported screen sizes are checked.

### Persistence and integrations

- [ ] Migrations succeed on a fresh database and the intended upgrade path.
- [ ] Transactions, duplicate requests and partial failures preserve data integrity.
- [ ] Backups are restored and verified in a separate environment.
- [ ] Provider integrations are tested with synthetic content and documented test accounts.
- [ ] Retention, deletion and access controls cover uploaded files and derived data.

### Deployment and operations

- [ ] Infrastructure is validated and a sandbox deployment is verified where applicable.
- [ ] HTTPS, network restrictions, least-privilege credentials and secret rotation are configured.
- [ ] Readiness checks reflect critical dependencies rather than only process uptime.
- [ ] Capacity and latency are measured using a reproducible, representative workload.
- [ ] Monitoring, actionable alerts and incident ownership are exercised.
- [ ] Rollback and recovery steps are tested and their timings recorded.

## Project-specific release gates

| Project | Evidence still needed |
|---|---|
| Trace | Representative segmentation evaluation, request limits under load, model provenance/license review; orders/storage/payments if offered |
| Interview Nailer | Authorized live-provider smoke test with synthetic input; production database operations, retention and deployment configuration |
| PESA FAMS | Broader financial acceptance scenarios, branch/maker-checker review, backup/restore rehearsal and operational controls |
| Blockchain API | Real RPC-provider integration, deployment-specific quotas and durable production storage; current provider-failure tests use controlled fixtures |
| Kubernetes Demo | Target-cluster rollout, service connectivity, readiness, rollback and resource limits |
| Terraform Modules | Reviewed plan, authorized sandbox provisioning, connectivity and cleanup; syntax/provider validation is already automated |
| Voice AI | Provider webhook verification, real test call, audio flow and human fallback; local duplicate handling is already tested |
| Trading/Solidity | Independent contract review, actual protocol/address compatibility, full bot dependencies and deployment-specific storage-layout review; local regressions do not establish profit or safety with funds |

## Evidence record template

For each release gate, record:

```text
Application and commit:
Check and expected outcome:
Environment and dependency versions:
Command or test procedure:
Observed outcome and evidence link:
Date and reviewer:
Remaining limitation or accepted exception:
```

Keep secrets and personal data out of evidence. Mark a gate complete only when the linked result supports it. For execution order, see the [PR roadmap](PR_ROADMAP.md).
