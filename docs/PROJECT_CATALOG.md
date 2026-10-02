# Project catalog

[Portfolio home](../README.md) · [Documentation index](README.md) · [Local demo guide](LOCAL_DEMOS.md) · [Current verification](VERIFICATION.md)

## Implemented projects

Each entry is a folder in `SONOFGOD.`. Source and setup links below point to the maintained collection rather than similarly named standalone repositories.

| Project | Source and setup | Demonstrable behavior | Boundary |
| --- | --- | --- | --- |
| Trace & Store | [Image API and storefront](../trace-stores/) | ONNX background removal, real upload/preview/download and a persistent browser cart. | CPU inference; no checkout, order backend or throughput benchmark. |
| PESA FAMS | [Banking prototype](../portfolio-projects/pesa-fams-banking-prototype/) | Synthetic fixed-asset workflows, approvals, depreciation and reporting; API tests exercise a real PostgreSQL database. | No live core-banking connection; backup recovery and production operation are unverified. |
| Interview Nailer | [Interview application](../portfolio-projects/interview-nailer/) | Accounts, résumé upload, mock interviews, scoring, coaching and saved history; file and PostgreSQL storage tests. | Local AI mode uses mock responses; live provider behavior remains separately unverified. |
| Blockchain API Service | [API and dashboard](../portfolio-projects/blockchain-api-service/) | Registration, persistent hashed API keys, revocation and simulated query responses in demo mode. | File storage is single-process; live RPC behavior is a separate integration path. |
| Kubernetes Demo | [Application and infrastructure](../portfolio-projects/kubernetes-demo/) | Frontend/backend application, configuration display and deployment examples. | Local application checks do not verify a cluster rollout. |
| Terraform Modules | [VPC and load-balancer modules](../portfolio-projects/terraform-modules/) | Reusable infrastructure source and examples with automated Terraform validation. | No cloud provisioning or deployed environment is claimed. |
| Voice AI Missed-Call | [Simulation and integration guides](../portfolio-projects/voice-ai-missed-call/) | Missed-call events, saved drafts and duplicate protection. | Local simulation sends no real call, SMS or provider message. |

## Proposed systems

These are architecture documents, not runnable applications. They preserve the intended project direction while keeping implementation status explicit.

| Working name | Design document | Relationship to existing code |
| --- | --- | --- |
| `cv-platform-1m-day` | [Queued CV platform](plans/cv-platform-1m-day.md) | Could extend Trace with durable jobs, scale tests and ETL; one million images/day is a target. |
| `rag-agent-platform` | [Retrieval and agent platform](plans/rag-agent-platform.md) | New implementation; no document embedding or vector retrieval service is present. |
| `stickermule-clone` | [Custom-product commerce backend](plans/stickermule-clone.md) | Could serve the existing storefront; Go, GraphQL and payment services are not implemented. |

The [proposal index](plans/README.md) defines milestones and the evidence needed before describing these as delivered projects.

## Suggested review route

1. **Trace & Store:** inspect the API contract, model handling, upload validation and frontend integration.
2. **PESA FAMS:** inspect SQL, authorization, approvals and database test coverage.
3. **Interview Nailer:** inspect account boundaries, storage modes and structured AI responses.
4. **Supporting projects:** explore API keys, deployment configuration and idempotent event handling.
5. **Architecture proposals:** review design tradeoffs and delivery milestones.

## Verification and next work

Use the [current verification record](VERIFICATION.md) for commands, results and exact-commit evidence. [GitHub Actions](https://github.com/tommyshamamba/SONOFGOD./actions/workflows/projects.yml) shows the status of each hosted run. The [September local report](LOCAL_DEMO_VERIFICATION.md) and [earlier source review](REVIEW_REPORT.md) are retained as historical evidence.

Implementation priorities remain in the [PR roadmap](PR_ROADMAP.md). Deployment, broader integration coverage and reproducible performance measurements remain separate milestones.

## Legacy experiments

The root Python bots and Solidity contracts are separate trading experiments. Their [isolated regression suite](../legacy-tests/README.md) documents the compiler, local VM tests, mocked Python checks and remaining limitations. This is narrower evidence than running a complete bot or interacting with a real protocol. No live transactions or profitable-trading results are claimed.
