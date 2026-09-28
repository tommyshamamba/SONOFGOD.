# Project catalog

All paths below are relative to the repository root. These projects are folders within SONOFGOD., not separate GitHub repositories.

| Project | Location | Implementation and limits |
|---|---|---|
| PESA FAMS | [Banking prototype](../portfolio-projects/pesa-fams-banking-prototype/) | Node/Express fixed-asset demo and PostgreSQL implementation path; generated demo records, approvals, depreciation and reporting. Live database deployment remains unverified. |
| Interview Nailer | [Interview application](../portfolio-projects/interview-nailer/) | React frontend and Express backend. File storage and mock AI support a local demo; mock responses are not live model inference. |
| Blockchain API Service | [Blockchain application](../portfolio-projects/blockchain-api-service/) | Node API, React frontend and infrastructure examples. Provider credentials and services are needed; not production-certified. |
| Kubernetes Demo | [Container demo](../portfolio-projects/kubernetes-demo/) | Frontend/backend example, Docker Compose, Kubernetes manifests and Terraform. Cluster/cloud deployment not verified in this review. |
| Terraform Modules | [Infrastructure modules](../portfolio-projects/terraform-modules/) | VPC and load-balancer source modules. No apply or paid cloud provisioning performed. |
| Voice AI Missed-Call | [Automation starter](../portfolio-projects/voice-ai-missed-call/) | Integration guides, prompts and scripts. Requires telephony and automation providers; not a completed autonomous agent deployment. |
| Trace and Stores | [Image API and storefront](../trace-stores/) | FastAPI service and Next.js storefront. Read the project documentation for model availability and fallback behavior. |

## Suggested review order

Start with PESA FAMS to examine tested application workflows, then Interview Nailer and Trace and Stores for UI/API organization. Review infrastructure and integration examples afterward. This ordering reflects available evidence, not a claim of commercial experience or seniority.

## Remaining work

- Exercise each frontend build in a clean environment and retain results.
- Test database-backed paths against a disposable PostgreSQL instance.
- Review authentication, authorization, rate limits and dependency advisories before internet deployment.
- Add deployment instructions only for environments actually tested.
- Record measured performance only with a reproducible workload and environment.
- Keep the root trading-related Python and Solidity files separate from portfolio application execution; they were not executed or audited here.
