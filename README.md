# Tommy Shamamba · Software engineering portfolio

Applications, APIs and infrastructure projects covering image processing, interview preparation, database workflows and cloud tooling.

**Start with the featured projects below.** Each links to its source, setup instructions and implementation details. This repository, `SONOFGOD.`, keeps the projects together in one collection.

[Project catalog](docs/PROJECT_CATALOG.md) · [Run the demos](docs/LOCAL_DEMOS.md) · [Verification results](docs/LOCAL_DEMO_VERIFICATION.md) · [Documentation](docs/README.md)

## Featured projects

| Project | What you can explore | Core technologies |
| --- | --- | --- |
| **[Trace & Store](trace-stores/)** | Upload artwork, run ONNX background removal, preview and download a transparent PNG, and manage a browser-persisted demo cart. | Python, FastAPI, ONNX Runtime, Next.js, React, TypeScript |
| **[PESA FAMS](portfolio-projects/pesa-fams-banking-prototype/)** | Fixed-asset management with approval workflows, depreciation, reporting and a PostgreSQL implementation. | Node.js, Express, PostgreSQL, SQL |
| **[Interview Nailer](portfolio-projects/interview-nailer/)** | Accounts, résumé upload, interview sessions, scoring, coaching and saved history. Local demonstrations use mock AI. | React, Node.js, Express, PostgreSQL |

These are development projects with local demonstration workflows. The [dated verification report](docs/LOCAL_DEMO_VERIFICATION.md) records completed checks and remaining limits; production deployment and performance benchmarks are separate work.

## APIs, infrastructure and automation

| Project | Focus | Available implementation |
| --- | --- | --- |
| [Blockchain API Service](portfolio-projects/blockchain-api-service/) | API authentication and data access | Dashboard, persistent hashed API keys, revocation and an offline query mode; provider configuration supports further integration. |
| [Kubernetes Demo](portfolio-projects/kubernetes-demo/) | Containerized application delivery | Frontend and backend, Docker Compose, Kubernetes manifests and Terraform configuration. |
| [Terraform Modules](portfolio-projects/terraform-modules/) | Reusable infrastructure | VPC and application load-balancer modules with examples. |
| [Voice AI Missed-Call](portfolio-projects/voice-ai-missed-call/) | Event processing and automation | Local missed-call simulation, saved response drafts and duplicate-event handling, plus provider integration guides. |

## Run a project

Follow the [local demo guide](docs/LOCAL_DEMOS.md) to choose a project and configure its services. Dependencies are installed separately in each application; this collection has no single command that starts every project.

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

The root Python and Solidity files are separate trading experiments. They are outside the application demo workflow; inspect them independently before use.

## Engineering notes

- **Verification:** [Local demo results](docs/LOCAL_DEMO_VERIFICATION.md), [source review](docs/REVIEW_REPORT.md) and [GitHub Actions](https://github.com/tommyshamamba/SONOFGOD./actions/workflows/projects.yml).
- **Next work:** [Prioritized implementation roadmap](docs/PR_ROADMAP.md) and [deployment-readiness checklist](docs/PRODUCTION_READINESS.md).
- **Contributions:** [Development workflow](CONTRIBUTING.md). Keep credentials, uploaded documents and runtime records out of Git.

Documentation distinguishes implemented behavior, historical test results and proposed work. Scale targets become performance claims only after reproducible measurements are available.
