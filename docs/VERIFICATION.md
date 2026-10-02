# Verification evidence

This report records observed checks for the portfolio. It distinguishes reproducible local and hosted tests from cloud deployment or live-provider validation.

## Verified result — 2 October 2026 UTC

**All 30 hosted jobs passed** in [run 36959456953](https://github.com/tommyshamamba/SONOFGOD./actions/runs/36959456953), testing source commit [`cb07140ce49aba790a70ca0be5314709be6a7706`](https://github.com/tommyshamamba/SONOFGOD./commit/cb07140ce49aba790a70ca0be5314709be6a7706). The [workflow page](https://github.com/tommyshamamba/SONOFGOD./actions/workflows/projects.yml) shows subsequent runs. Documentation and screenshots may be published after this tested source commit.

All six browser journeys passed with no uncaught browser exceptions. See [the application walkthrough](DEMO_GALLERY.md) for screenshots and engineering decisions.

| Area | Evidence and scope |
| --- | --- |
| Frontends | Production builds for Trace, Interview, Blockchain and Kubernetes; Trace client tests and Interview API URL tests. |
| Trace API | Eight Python tests, including real downloaded U2NetP inference, invalid uploads, model failure handling and required-model readiness. |
| PESA FAMS | 36 passing domain/API/integration cases, including seven against a disposable PostgreSQL database. Browser checks exercise separate maker/checker accounts, authenticated printing, account switching and auditor restrictions. |
| Interview | API suites against file storage and real PostgreSQL, plus a complete browser session through persisted coaching. AI output uses deterministic mock mode. |
| Blockchain | Ten API/storage cases including API-key ownership and revocation, dependency failures, exclusive locking and forced-crash recovery. Queries in browser/Compose use demo mode. |
| Kubernetes | Two API tests; browser reads live local process metadata. |
| Voice simulation | Python unit tests and browser event/replay flow against local SQLite. No telephone calls or messages are sent. |
| Containers | Blockchain and Kubernetes Compose images build, become healthy, serve the frontend and proxy an API request. |
| Terraform | Four directories initialize without a backend and validate. Nine mock-provider cases cover VPC routing and ALB security-group/listener configuration; no AWS resources are provisioned. |
| Dependency checks | Ten npm lockfile audits and a hash-locked Python dependency audit. Audit results describe published advisories at run time. |
| Legacy experiments | 23 local EVM tests and 16 Python tests. Uses the repository proxy, synthetic contract fixtures, extracted bot functions and mocked network behavior; enforces the EIP-170 bytecode limit. |

The frontend container images now include HTTP health checks, so Compose waits for the web server to accept requests before testing it. The banking prototype exposes supported permissions, rejects self-approval and duplicate posting, and labels its GL output as simulated.

## Reproduce

Use Node **24.19.0** and Python **3.12**, matching [the workflow](../.github/workflows/projects.yml). Start with [the local demo guide](LOCAL_DEMOS.md). A fresh clone can install/build the applications with `npm ci` and `npm run demo:setup`; `npm run test:browser:managed` starts isolated synthetic stores and exercises six browser journeys after Playwright Chromium is installed.

Project READMEs provide focused test commands. PostgreSQL tests need disposable databases; CI supplies PostgreSQL 18. Terraform checks use 1.9.8. Browser screenshots are uploaded as `browser-evidence` with 14-day retention. Test screenshots use synthetic accounts and data.

Windows sandbox restrictions prevented local child-process execution for some builds and the forced-crash test, and blocked temporary-directory access for six Python cases. Hosted Linux runs exercise those paths; these restrictions were not converted into passing test results or skipped assertions.

## What these checks do not establish

- Cloud infrastructure apply, Kubernetes rollout, autoscaling, recovery drills or production load capacity.
- Segmentation accuracy on a representative dataset or a million-images-per-day throughput claim.
- Live Anthropic, RPC, SMS/voice, payments, banking/core-ledger or other provider behavior.
- Contract audit, live bot compatibility, profitable trading, mainnet-fork execution or safety of a deployed proxy upgrade.
- Production security certification, regulatory compliance or a guarantee that no undiscovered defects remain.

The three [architecture proposals](plans/README.md) remain planned systems. Their capacity targets are design goals, not achieved measurements. The [production checklist](PRODUCTION_READINESS.md) identifies release work beyond this portfolio verification.
