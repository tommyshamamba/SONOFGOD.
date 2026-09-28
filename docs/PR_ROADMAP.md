# Engineering PR roadmap

Baseline: local commit `3987438`, reviewed September 28, 2026.
Scope: this repository's project collection, not every repository in a GitHub organization.
These are proposed PRs, not opened GitHub pull requests or production certification.

## Completed baseline

| Area | Evidence | Remaining limit |
|---|---|---|
| Trace ONNX | CPU inference, startup warmup and eight passing local tests, including U2NetP smoke test | Segmentation quality and deployment capacity unmeasured |
| Banking PostgreSQL | Three migrations, seeded disposable PostgreSQL 18.4, 26 passing tests including four integration cases | Not every write workflow is tested |
| Frontends | All four production builds passed locally | Browser behavior and hosted deployments unverified |
| CI | Root workflow covers source checks, banking/PostgreSQL, Trace and four frontend builds | Successful hosted run not verified |
| Source checks | 14 JSON manifests, seven Python and 54 JavaScript files passed | Syntax does not establish behavior or security |

See [verification details](REVIEW_REPORT.md). Do not recreate the already implemented ONNX adapter or initial database tests.

## PR 1 — Verify CI and make installations reproducible

**Priority:** first. **Depends on:** publishing the baseline commit.

- Run the existing workflow on GitHub; fix actual Linux runner failures and record the run URL and commit SHA.
- Commit a banking dependency lockfile and use `npm ci` after verifying it from a clean installation.
- Retain test summaries and build artifacts with explicit retention periods; keep credentials and uploaded documents out of artifacts.
- Add behavioral jobs for currently uncovered backends as their suites are implemented.

**Acceptance:** every configured job passes on the same commit from fresh dependencies. Document that the downloaded pretrained model check is optional and skipped in normal CI; the tiny executable ONNX graph is covered there.

## PR 2 — Harden and test Interview Nailer's existing provider mode

**Priority:** next application change. **Depends on:** PR 1 for hosted verification.

Existing code: `backend/config/ai.js` already calls Anthropic for regular and streaming responses. `config/env.js` defaults to mock mode and silently maps unknown modes to mock. This needs validation, not an entirely new provider integration.

- Validate AI/storage mode values and reject invalid configuration. Require a provider key in provider mode and a non-default signing secret in production.
- Make the provider model configurable; verify supported models against provider documentation during implementation.
- Inject the provider client so ordinary tests need no network or paid API calls.
- Test valid responses, malformed output, timeout, rate limiting, upstream failure and interrupted streaming. Validate structured output before storing it.
- Test registration/login, invalid and expired tokens, and user A attempting to access user B's resumes, interviews and answers in both file and PostgreSQL modes.
- Test failed-upload cleanup, file-size/type limits and concurrent file-store updates; migrate production persistence if file storage cannot safely meet requirements.

**Acceptance:** deterministic tests pass without credentials; production configuration fails safely; two-user isolation tests pass. Separately record an opt-in live provider smoke test using synthetic content and a bounded token budget when an account is configured. Until then, label live behavior unverified.

## PR 3 — Extend banking transaction and authorization coverage

**Priority:** before any real financial data. **Depends on:** PR 1.

- Add PostgreSQL tests for create/update assets, depreciation, transfers, approvals and audit records using isolated seeded fixtures.
- Exercise branch restrictions, maker/checker rules and repeated or simultaneous approvals through the API.
- Inject failures midway through multi-table operations and assert rollback leaves no partial financial records.
- Verify rounding, residual value boundaries, reconciliation totals and duplicate-request behavior.
- Rehearse database backup and restore into a second disposable database; compare restored records and migration versions.

**Acceptance:** rejected actions leave data unchanged; concurrent approval cannot apply twice; audit records identify the actor; restored data passes integrity checks. Record schema and test versions.

## PR 4 — Test browser journeys and review dependencies

**Priority:** before public demonstrations. **Depends on:** PR 1; use controlled test services.

- Add browser tests for Trace upload/download and Interview Nailer login, resume upload and interview flow.
- For Blockchain and Kubernetes frontends, test loading, successful responses and unavailable backend states with deterministic fixtures.
- Check keyboard access, form errors, mobile layouts and cross-origin configuration.
- Review dependency advisories and unsupported tooling; upgrade in small changes and rerun builds and behavior tests. Builds alone do not resolve dependency risks.

**Acceptance:** documented browser journeys pass in CI with no real user data or paid transactions. Preserve useful failure screenshots without secrets.

## PR 5 — Validate infrastructure in stages

**Priority:** before deployment. **Depends on:** application tests and an identified target environment.

- Run Terraform formatting, initialization without a backend, and validation for each independent module/root; validate Kubernetes manifests against the target API version.
- Render Compose configuration and test container health/readiness locally where supported.
- In a dedicated sandbox account, review an explicit plan, resource names, estimated cost and cleanup steps before provisioning.
- After authorized sandbox provisioning, verify networking, least-privilege access, readiness and rollback. Destroy only resources created by that test and verify cleanup.

**Acceptance:** retain validation output, reviewed plan and sandbox smoke-test evidence. Static validation must not be described as a successful cloud deployment. Account/region/budget and deployment authorization are prerequisites for provisioning.

## PR 6 — Complete Voice AI's provider-backed call flow

**Priority:** after choosing providers and configuring test accounts.

- Define call/webhook contracts and test signature verification, replay protection and duplicate delivery.
- Implement audio input/output, timeouts, retry limits, human handoff and failure handling.
- Use simulated events first; then perform an authorized end-to-end call with test numbers.
- Define recording consent, retention, deletion and redacted operational logs appropriate to the deployment.

**Acceptance:** one documented test call completes the intended flow; invalid webhooks fail; duplicate events do not repeat actions; provider outage reaches a safe fallback. Real calls and account credentials are external prerequisites.

## PR 7 — Isolate and test trading and Solidity examples

**Priority:** before making any safety or financial claims; independent of application release.

- Inventory entry points, dependencies, supported networks and key handling without running live operations.
- Add offline unit/property tests, mocked market data and local-chain contract tests, including authorization and failure paths.
- Introduce an explicit dry-run default and prevent accidental live transactions in tests.
- Arrange an independent security review before using real funds; automated tests are not a smart-contract audit.

**Acceptance:** tests execute without production keys, live orders or mainnet transactions. Publish precisely what was reviewed and the unresolved findings.

## PR 8 — Release one application with operational evidence

**Priority:** after relevant PRs above. Choose one application rather than treating the collection as one deployable product.

- Complete its [readiness checklist](PRODUCTION_READINESS.md), assign an owner and record deployment/rollback instructions.
- Measure representative load and failure recovery; define operational targets from those measurements.
- Verify monitoring, backup restoration, secret rotation and incident handling in staging.

**Acceptance:** the named application/version has reproducible evidence for its declared scope. Never extend that result to unrelated projects or claim guaranteed reliability.
