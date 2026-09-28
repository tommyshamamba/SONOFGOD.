# Local demo fix verification

September 28, 2026. Scope: local working demonstrations, as selected by the user. This extends the earlier baseline report.

## Code defects fixed

- Trace website now calls the image API, displays real original/processed images, offers PNG download, reports errors/fallback honestly and persists a functional demo cart. CORS exposes the processor header.
- Interview Nailer enforces résumé ownership at session creation and completion, including legacy associations. Configuration is validated, file writes are serialized/atomic, provider responses have contracts, and authentication/AI routes have limits. Uploads no longer leave raw files behind. Streaming clients handle split frames.
- Banking inserts the required approval status, fixes unsupported PostgreSQL UUID aggregation, validates input errors, rechecks active users/roles, and atomically claims approvals/retries to prevent duplicate financial effects. Posted or active depreciation periods cannot be overwritten by resubmission.
- Blockchain persists accounts and hashed API keys; dashboard actions use full creation-time keys and exact revocation IDs. RPC failures/timeouts are bounded, Redis outages return 503, broadcasting defaults off, and offline data is explicitly labeled. The local backend serves the built dashboard.
- Kubernetes demo uses the same-origin API, reports connection errors, serves its built frontend locally, validates configuration and masks secrets. Compose DNS alias and container binding were corrected.
- Voice now has a local SQLite-backed missed-call simulation with draft responses and replay protection.
- Terraform duplicate provider configuration, malformed identity-policy interpolation, listener block structure and a wrong VPC output were fixed.

## Results

| Check | Observed result |
|---|---|
| Trace API | 8 passed, including downloaded U2NetP; one upstream deprecation warning |
| Trace frontend client/cart | 11 passed, including actual multipart request to running ONNX API |
| Interview backend, file storage | 8 passed; complete mock interview journey and isolation/concurrency cases |
| Interview backend, PostgreSQL | Same 8 tests passed against a separate real PostgreSQL database |
| Banking | 30 passed, including 7 real database integration cases |
| Blockchain backend | 8 passed: persistence, ownership, limits, provider errors, demo mode and configuration |
| Kubernetes backend | 2 passed |
| Voice simulation | 4 passed, including concurrent duplicate delivery and HTTP origin rejection |
| React production builds | Interview, Blockchain and Kubernetes passed after changes |
| Trace production build | Next.js production build passed after the final frontend changes |
| Terraform | VPC, ALB and Kubernetes root all passed `validate`; no plan/apply |
| Source validation | 17 manifests, 9 Python files and 60 JavaScript files passed; 21 YAML documents parsed |
| Local HTTP availability | All seven demo/API ports returned HTTP 200 during verification |

## Limits and follow-up

- Browser smoke checks were added but could not execute: the in-app tool failed to initialize; standalone Chrome failed with Windows IPC access-denied errors. API journey tests and builds passed; visual/browser validation remains pending.
- Current verification servers use isolated scratch stores for Interview and Blockchain; the guide uses their default project data paths.
- Root CI covers the new suites and infrastructure validation. A successful hosted run still requires publishing/authentication and verification on GitHub.
- Local demos do not implement real print checkout, live banking settlement, real voice/SMS delivery or cloud hosting. Interview's live Anthropic mode was tested through injected responses, not paid provider calls.
- Banking backup/restore remains unverified; the portable PostgreSQL runtime on this host lacks pg_dump/pg_restore. Full financial correctness and operational certification are outside these demo results.
- Dependency advisories and deployment load/security review remain release gates. Trading/Solidity files were not executed or changed.

Do not infer commercial deployment, accuracy benchmarks or production certification from these local results.
