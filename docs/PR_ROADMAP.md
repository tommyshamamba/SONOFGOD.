# Engineering roadmap

[Portfolio](../README.md) · [Current verification](VERIFICATION.md) · [Release checklist](PRODUCTION_READINESS.md)

## Implemented foundation

The collection includes authenticated application workflows, PostgreSQL integration tests, real CPU ONNX inference, a managed local demo launcher, browser journeys, dependency audits, container smoke checks and Terraform validation. The isolated legacy suite exercises contract callbacks and Python transaction coordination without a live network.

Consult the verification report for which checks passed on the published commit. The presence of a test or deployment file alone is not a successful run.

## Next release work

| Priority | Work | Completion evidence |
| --- | --- | --- |
| 1 | Operate one selected application in a dedicated staging environment | Named release, HTTPS, environment configuration, successful browser journey and tested rollback. |
| 2 | Rehearse database recovery | Backup restored into a second disposable database; migrations, row counts and application reads agree. |
| 3 | Evaluate image processing | Representative images, output-quality rubric, model checksum, hardware, raw latency samples and failure cases. |
| 4 | Verify live AI integration when a test account is available | Bounded synthetic requests, supported model, validated responses, provider-error handling and recorded cost. |
| 5 | Complete provider-backed voice workflows | Signature verification, consent/retention rules, duplicate handling and one authorized test call. |
| 6 | Review cloud operations | Target cluster/account, reviewed infrastructure plan, budget, deployment smoke test and cleanup evidence. |

Live provider accounts, cloud deployments, payment processing and real transactions are outside ordinary local/CI verification. Provisioning and usage must follow a defined environment and budget.

## Design proposals

The [CV, RAG and commerce proposals](plans/README.md) define future implementations and acceptance criteria. Their targets are not measured achievements. Complete one useful end-to-end workflow before expanding each stack.

## Legacy experiments

The [isolated regression suite](../legacy-tests/README.md) covers selected contract and transaction-coordination logic. Real lender/router deployments, market assumptions, every strategy adapter, storage upgrades and operational trading require independent review. Local fixture profits are synthetic and establish no financial result.
