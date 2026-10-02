# Tommy Shamamba · Software engineering portfolio

[![Project checks](https://github.com/tommyshamamba/SONOFGOD./actions/workflows/projects.yml/badge.svg?branch=main)](https://github.com/tommyshamamba/SONOFGOD./actions/workflows/projects.yml)

Applications, APIs and infrastructure projects covering image processing, interview preparation, database workflows and cloud tooling.

**Start with the featured projects below.** Each links to its source, setup instructions and implementation details. This repository, `SONOFGOD.`, keeps the projects together in one collection.

[Project catalog](docs/PROJECT_CATALOG.md) · [Run the demos](docs/LOCAL_DEMOS.md) · [Verification results](docs/VERIFICATION.md) · [Documentation](docs/README.md)

## Featured projects

| Project | What you can explore | Core technologies |
| --- | --- | --- |
| **[Trace & Store](trace-stores/)** | Upload artwork, run ONNX background removal, preview and download a transparent PNG, and manage a browser-persisted demo cart. | Python, FastAPI, ONNX Runtime, Next.js, React, TypeScript |
| **[PESA FAMS](portfolio-projects/pesa-fams-banking-prototype/)** | Fixed-asset management with approval workflows, depreciation, reporting and a PostgreSQL implementation. | Node.js, Express, PostgreSQL, SQL |
| **[Interview Nailer](portfolio-projects/interview-nailer/)** | Accounts, résumé upload, interview sessions, scoring, coaching and saved history. Local demonstrations use mock AI. | React, Node.js, Express, PostgreSQL |

These are development projects with local demonstration workflows. The [verification report](docs/VERIFICATION.md) records completed checks and remaining limits; production deployment and performance benchmarks are separate work.

## APIs, infrastructure and automation

| Project | Focus | Available implementation |
| --- | --- | --- |
| [Blockchain API Service](portfolio-projects/blockchain-api-service/) | API authentication and data access | Dashboard, persistent hashed API keys, revocation and an offline query mode; provider configuration supports further integration. |
| [Kubernetes Demo](portfolio-projects/kubernetes-demo/) | Containerized application delivery | Frontend and backend, Docker Compose, Kubernetes manifests and Terraform configuration. |
| [Terraform Modules](portfolio-projects/terraform-modules/) | Reusable infrastructure | VPC and application load-balancer modules with examples. |
| [Voice AI Missed-Call](portfolio-projects/voice-ai-missed-call/) | Event processing and automation | Local missed-call simulation, saved response drafts and duplicate-event handling, plus provider integration guides. |

## Run a project

Use Node **24.19.0** (see `.nvmrc`) and Python **3.12**. From the repository root:

```sh
npm ci
npm run demo:setup
npm run demos
```

Setup installs application dependencies, builds the frontends, creates a Python virtual environment and downloads checksum-verified model weights. The launcher starts six local demonstrations using synthetic data. Ctrl+C stops its processes. See the [local demo guide](docs/LOCAL_DEMOS.md) for addresses, individual launches and browser verification.

For image processing, begin with the [Trace & Store quickstart](trace-stores/#run-locally). It covers the model download, required-model configuration, API and storefront. For database workflows, use the [PESA FAMS guide](portfolio-projects/pesa-fams-banking-prototype/README.md).

## Architecture proposals

The following documents develop the next project ideas. **They are design proposals, with no implementation or benchmark results yet.** Their component layouts and milestones describe planned work.

| Proposal | Engineering focus |
| --- | --- |
| [CV Platform · 1M images/day target](docs/plans/cv-platform-1m-day.md) | Queued image processing, worker scaling, analytical storage and batch ETL. |
| [RAG Agent Platform](docs/plans/rag-agent-platform.md) | Document retrieval, grounded responses, controlled tools and quality/cost evaluation. |
| [Custom-product Commerce Backend](docs/plans/stickermule-clone.md) | Go and GraphQL services, PostgreSQL orders, payment webhooks and tenant isolation. |

See the [proposal index](docs/plans/README.md) for scope and implementation priorities. The existing [Trace & Store](trace-stores/) is the starting point for image processing and storefront experiments.

## Repository map

```text
SONOFGOD./
├── trace-stores/          # Image-processing API and storefront
├── portfolio-projects/   # Six application/infrastructure projects
├── docs/                 # Catalog, demo guides and verification reports
│   └── plans/            # Three proposed systems and delivery milestones
├── scripts/              # Source checks and browser smoke checks
├── .github/workflows/    # Automated project checks
└── CONTRIBUTING.md       # Development and contribution workflow
```

The root Python and Solidity files are separate trading experiments. Their [isolated regression suite](legacy-tests/README.md) uses mocked RPC behavior and an in-memory EVM. They are outside the application demo launcher and have not been approved for live operation.

## Engineering notes

- **Verification:** [Current results](docs/VERIFICATION.md), [source review](docs/REVIEW_REPORT.md) and [GitHub Actions](https://github.com/tommyshamamba/SONOFGOD./actions/workflows/projects.yml).
- **Next work:** [Prioritized implementation roadmap](docs/PR_ROADMAP.md) and [deployment-readiness checklist](docs/PRODUCTION_READINESS.md).
- **Contributions:** [Development workflow](CONTRIBUTING.md). Keep credentials, uploaded documents and runtime records out of Git.

Original owned code is available under the [MIT license](LICENSE), with [third-party exclusions](THIRD_PARTY_NOTICES.md). See the [security policy](SECURITY.md) for reporting.

Documentation distinguishes implemented behavior, historical test results and proposed work. Scale targets become performance claims only after reproducible measurements are available.
