# SONOFGOD project collection

This repository contains multiple projects in folders. They will not appear as separate repositories on the GitHub profile. Open a project below to see its source and setup instructions.

[Project catalog and remaining work](docs/PROJECT_CATALOG.md) | [Development workflow](CONTRIBUTING.md) | [Automated checks](.github/workflows/projects.yml)

[Prioritized PR roadmap](docs/PR_ROADMAP.md) | [Production-readiness checklist](docs/PRODUCTION_READINESS.md)

**Start here:** [Run the local demos](docs/LOCAL_DEMOS.md) · [Latest fix and test results](docs/LOCAL_DEMO_VERIFICATION.md)

## Portfolio projects

| Project | Source and documentation |
|---|---|
| PESA FAMS banking prototype | [Fixed-asset management prototype, demo workflows, tests and PostgreSQL implementation](portfolio-projects/pesa-fams-banking-prototype/) |
| Interview Nailer | [Interview preparation frontend and backend](portfolio-projects/interview-nailer/) |
| Blockchain API Service | [API, frontend and infrastructure source](portfolio-projects/blockchain-api-service/) |
| Kubernetes Demo | [Application, container and infrastructure examples](portfolio-projects/kubernetes-demo/) |
| Terraform Modules | [VPC and load-balancer modules](portfolio-projects/terraform-modules/) |
| Voice AI Missed-Call | [Starter scripts, prompts and integration guides](portfolio-projects/voice-ai-missed-call/) |
| Trace and Stores | [Existing project and setup instructions](trace-stores/) |

See the [portfolio collection notes](portfolio-projects/README.md) for import details. These are development projects and prototypes; inclusion here does not certify production readiness, commercial deployment or performance results. All four frontend production builds passed locally. Banking now has real PostgreSQL integration tests, and Trace implements ONNX inference. See the [verification report](docs/REVIEW_REPORT.md) for results and limits.

## Other existing source

The repository also retains the Python and Solidity files at its root. Consult and review those files independently before running them. The portfolio import did not validate or change their behavior.

## Running projects

Each project has its own setup and dependencies. Follow its README and configure local credentials where required. Do not deploy with demo credentials. Private environment files, runtime records and uploaded application documents are not part of the portfolio import.
