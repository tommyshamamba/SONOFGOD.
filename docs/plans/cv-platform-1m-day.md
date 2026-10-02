# Computer vision processing platform

**Status: design proposal; not implemented. No validated benchmarks.**

Working identifier: `cv-platform-1m-day`. Processing one million images per day is a future capacity target, not an achieved result. This document does not describe a running service at that scale.

[All proposals](README.md) · [Project catalog](../PROJECT_CATALOG.md) · [Existing Trace & Store implementation](../../trace-stores/README.md)

## Purpose and first release

Build a Python service that accepts an image, schedules processing, and returns a durable result without holding an HTTP request open for inference. Start with background removal using the existing Trace API's ONNX model path. Detection, classification, and alternative models should follow only after the job lifecycle is reliable.

The existing [Trace API](../../trace-stores/services/trace-api/) provides synchronous CPU inference. Durable queues, GPU workers, analytics pipelines, and autoscaling described below are proposed additions.

The first release would support one image operation, authenticated tenants, private object storage, job polling, bounded retries, and downloadable results. Real-time video, model training, and Kubernetes operations are outside that release.

## Proposed architecture

```text
Client -> FastAPI -> private image storage
                 -> PostgreSQL job + transactional outbox
Outbox dispatcher -> durable queue -> inference worker
Inference worker -> result storage + PostgreSQL job status
Client -> job status -> authorized result download
```

| Component | Initial choice and responsibility |
|---|---|
| API | Python and FastAPI for validation, tenant authorization, job submission, and status retrieval |
| Object storage | Azure Blob Storage for private input and output objects with defined retention |
| Queue | Azure Service Bus with retry limits and a dead-letter queue; message payloads reference objects rather than containing images |
| Job database | PostgreSQL for ownership, idempotency keys, operation/model version, attempt history, and terminal state |
| Worker | Dockerized Python process reusing ONNX inference first; benchmark a GPU path before adding GPU capacity |
| Analytics | SQL summaries from job records initially; OpenTelemetry spans and service metrics for diagnostics |

Use at-least-once delivery with idempotent processing: a duplicate message must not create a second logical result. A job and its outbox record commit together so an API success cannot silently lose the work. Workers acquire a bounded lease, acknowledge only after persisting the result, and recover expired leases after a crash. Poison jobs move to the dead-letter queue for explicit review and controlled replay.

Treat image decoding as untrusted input: enforce byte and decoded-pixel limits, validate the decoded format, limit processing time, and authorize every status or object access. Logs should reference job IDs without storing image contents or signed URLs.

## Later integrations, with a reason to add each

- **PyTorch and OpenCV:** add a detection or classification pipeline when a documented use case requires it; keep preprocessing and model versions reproducible. TensorFlow is an alternative model runtime, not a parallel requirement.
- **Airflow, Azure Databricks, and Spark:** schedule and execute batch ETL when SQL summaries no longer meet volume or transformation needs. Backfills must be repeatable without double-counting jobs.
- **ClickHouse:** introduce an analytics store when measured query requirements justify a separate database.
- **Kafka:** consider streaming only for consumers that need event replay or independent subscriptions; keep the work queue separate from that decision.
- **Redis and Kubernetes:** add caching or orchestration after profiling demonstrates a benefit. AWS S3/SQS is an alternative deployment profile, not another mandatory cloud dependency.

## Proposed implementation layout

The following paths are illustrative and do not exist as this platform's implementation:

```text
api/           # Submission, authorization, and job-status endpoints
workers/       # Queue consumer, inference adapters, and lease recovery
data/          # Database migrations, outbox, and SQL summaries
etl/           # Optional scheduled analytics and backfills
infra/         # Container definitions and one chosen cloud profile
benchmarks/    # Workload manifests, load driver, and raw measurements
```

## Milestones and acceptance criteria

| Milestone | Evidence required to complete it |
|---|---|
| 1. One end-to-end job | A valid upload reaches a terminal state and an authorized client downloads its result; malformed images and cross-tenant requests are rejected |
| 2. Reliable delivery | Duplicate submission, duplicate queue delivery, worker termination, and broker interruption tests preserve one logical result; exhausted jobs are visible and replayable |
| 3. Observable processing | A job can be traced from submission through inference; queue age, failures, retries, worker utilization, and processing time are recorded |
| 4. Capacity experiment | A versioned workload runs with published hardware, configuration, raw results, and output-quality checks; observed limits are documented |
| 5. Optional ETL | Repeated backfills produce the same totals and reconcile with authoritative job records before analytics are used for reporting |

## Measurement plan

Define the future target as **1,000,000 successfully completed images in 24 hours** for a published image-size distribution, model, quality threshold, concurrency, and compute budget. That implies about 11.6 successful completions per second averaged over the day; burst demand and recovery headroom require separate tests.

Measure submission latency separately from queue wait, inference duration, and total completion time. Report p50/p95/p99, successful and failed jobs, retry rate, backlog growth, memory, CPU/GPU utilization, and cost per successfully processed image. Include cold starts, corrupt inputs, worker loss, and a sustained run rather than extrapolating from a short peak.

Compare any optimization against the same baseline workload and output-quality criteria. Store the commit, model checksum, environment, pricing assumptions, and raw samples with the report. Do not publish a throughput, availability, or latency improvement claim until that evidence exists.
