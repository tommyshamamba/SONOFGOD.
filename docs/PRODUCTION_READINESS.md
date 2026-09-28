# Production-readiness checklist

Scope: projects in this repository. Baseline evidence is local commit `3987438`.
Checked items describe completed local verification only. Unchecked items require evidence, not necessarily new code. This checklist is not a certification.

## Verified baseline

- [x] Project catalog, contribution instructions and source checker exist.
- [x] All four frontend production builds passed locally.
- [x] Trace executes ONNX inference; eight local tests passed including pretrained U2NetP execution.
- [x] Banking migrations and seed ran against disposable PostgreSQL; all 26 tests passed.
- [x] Root GitHub Actions jobs exist for these checks.
- [ ] A successful hosted Actions run is linked for the exact release commit.

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
| Interview Nailer | Existing Anthropic mode tested, invalid configuration rejected, two-user isolation in both storage modes, upload cleanup |
| PESA FAMS | Financial write and concurrency coverage, maker/checker and branch tests on PostgreSQL, restore rehearsal |
| Blockchain API | Provider failure/retry behavior, API authorization, response correctness and key handling |
| Kubernetes Demo | Target-cluster rollout, service connectivity, readiness, rollback and resource limits |
| Terraform Modules | Module validation, reviewed plan and authorized sandbox provisioning/cleanup |
| Voice AI | Verified webhooks, duplicate handling, test call, audio flow and human fallback |
| Trading/Solidity | Offline/local-chain tests and independent review before real funds; outside application release scope |

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
