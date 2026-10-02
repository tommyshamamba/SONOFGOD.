# Architecture proposals

These documents turn three project ideas into scoped implementation plans. Each proposal describes intended behavior, design choices, and evidence needed before making performance or production claims.

**Status: design proposals; not implemented. No validated benchmarks are available for these proposals.** They are documentation within SONOFGOD, not separate deployed applications or GitHub repositories.

| Proposal | First useful release | Next evidence to produce |
|---|---|---|
| [Computer vision processing platform](cv-platform-1m-day.md) | An asynchronous image-processing pipeline with durable job state and retries | A reproducible workload, correctness checks, and measured throughput |
| [RAG agent platform](rag-agent-platform.md) | Document ingestion and answers grounded in permission-filtered sources | A versioned evaluation set with retrieval, answer-quality, latency, and cost results |
| [Custom print commerce](stickermule-clone.md) | Product catalog, persistent cart, and verified test-mode checkout | End-to-end order tests, tenant isolation checks, and webhook replay results |

## Start with existing code

[Trace & Store](../../trace-stores/README.md) already contains a FastAPI image-processing service and a Next.js storefront. Its [storefront documentation](../../trace-stores/apps/storefront/README.md) distinguishes the browser cart and artwork previews from unimplemented payments and order fulfillment.

The vision and commerce proposals can build on that work. Their queues, analytics, commerce backend, and scale targets are future work. The RAG platform is a separate proposal, with a possible later integration to the Trace API.

## How a proposal becomes a project

1. Implement one complete workflow with documented dependencies and configuration.
2. Add meaningful correctness and failure-path checks against the actual implementation.
3. Record reproducible results, including the commit, environment, workload, and limitations.
4. Add runnable setup instructions only when their files and commands exist.
5. Update the [project catalog](../PROJECT_CATALOG.md) with the implementation location and verified status.

Performance targets, optional integrations, and production controls stay labeled as planned until supported by evidence. None of these proposals asserts vendor affiliation or an achieved compliance certification.

[Back to the project catalog](../PROJECT_CATALOG.md)
