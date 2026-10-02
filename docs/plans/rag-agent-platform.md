# RAG agent platform

**Status: design proposal; not implemented. No validated benchmarks.**

A proposed document assistant that retrieves authorized source material, produces cited answers, and can later invoke narrowly scoped tools. Accuracy, latency, and cost improvements remain unmeasured.

[All proposals](README.md) · [Project catalog](../PROJECT_CATALOG.md)

## Purpose and first release

Start with a complete question-answering workflow: upload supported text or text-based PDF documents, process them asynchronously, ask a question, and inspect the passages used in the answer. When the documents do not support an answer, the assistant should say so.

The first release would support authenticated workspaces, document versioning and deletion, permission-filtered retrieval, citations, and a versioned evaluation harness. Autonomous purchasing, unrestricted SQL, arbitrary web browsing, and mobile clients are outside this release. Scanned PDFs would require a separately evaluated OCR stage.

## Proposed architecture

```text
Upload -> private object storage -> ingestion worker
Ingestion worker -> parse -> chunk -> embed -> PostgreSQL + pgvector
Question -> authorize -> retrieve permitted chunks -> generate cited answer
Answer + retrieved passages + usage -> evaluation and diagnostic records
```

| Component | Initial choice and responsibility |
|---|---|
| API | Python and FastAPI for document lifecycle, questions, and workspace permissions |
| Ingestion | Durable PostgreSQL job records with worker leases, bounded retries, and visible failure states |
| Retrieval | PostgreSQL and pgvector for chunks, embeddings, document versions, and tenant metadata in one datastore |
| Generation | OpenAI API behind an explicit provider interface; record the configured model and prompt version |
| Client | React and TypeScript for ingestion status, chat, and source inspection |
| Evaluation | Python harness with a versioned question set and recorded quality, latency, and usage results |

Use stable document and chunk IDs with source-page metadata where available. Ingestion should be idempotent, and a replacement version should become searchable only after it is fully processed. Deletion must remove source objects, searchable chunks, and affected cache entries under a documented retention policy.

Apply access restrictions during retrieval, then verify every returned source before using it in a response. An embedding match does not establish authorization. Record source IDs and operational metadata without placing sensitive document contents or credentials in general application logs.

## Grounding and tools

Treat retrieved text as evidence, never as instructions that can change permissions or trigger actions. Use only retrieved source IDs for citations, validate that each citation resolves to an allowed document version, and evaluate whether its passage supports the associated claim. Citation validity alone does not establish factual correctness.

Begin with retrieval as the only tool. A later integration could call the existing [Trace API](../../trace-stores/README.md) to process a user-provided image, but that agent integration does not exist today. Any future tool should use a fixed input schema, server-side authorization, time and resource limits, and an audit record. Actions that change orders or external systems should require an explicit user confirmation and an idempotency key.

Show source references and tool outcomes in the UI. Do not expose private model reasoning or sensitive backend traces.

## Later integrations, with a reason to add each

- **LangChain:** consider orchestration when multiple tested tools make the direct workflow difficult to maintain; keep tool contracts independent of the framework.
- **Redis:** introduce exact-match or semantic caching only after measuring its value. Keys must include tenant, permissions, document version, model, and prompt version; invalidate on access or document changes.
- **Pinecone or Qdrant:** evaluate as alternatives to pgvector when measured retrieval or operational requirements warrant a migration.
- **Supabase:** consider as a managed PostgreSQL/authentication option after assessing deployment needs; it is not required for the initial architecture.
- **Reranking, hybrid retrieval, and Expo:** prioritize from observed retrieval failures or client needs, with separate evaluations for each change.

## Proposed implementation layout

The following paths are illustrative and do not exist as this platform's implementation:

```text
api/          # Documents, questions, authorization, and source endpoints
ingestion/    # Parsing, chunking, embedding, and version publication
retrieval/    # Permission-aware search and citation resolution
tools/        # Optional approved tool contracts and execution controls
frontend/     # Upload status, chat, and source viewer
evals/        # Versioned fixtures, evaluation runner, and result reports
```

## Milestones and acceptance criteria

| Milestone | Evidence required to complete it |
|---|---|
| 1. Ingest and retrieve | A document produces searchable chunks with resolvable sources; retries do not duplicate chunks, and replacements/deletions update retrieval correctly |
| 2. Answer with evidence | A fixed evaluation set records retrieval recall, citation support, answer correctness, and appropriate abstention on unanswerable questions |
| 3. Enforce boundaries | Tests cover cross-tenant retrieval, permission changes, malicious document instructions, citation spoofing, and sensitive-log handling |
| 4. Measure the baseline | A report records model/configuration, dataset version, repeated-run variation, latency, failures, and cost per answered question |
| 5. Add one useful tool | Contract tests demonstrate authorized execution, invalid-input rejection, timeout behavior, and confirmation for any external side effect |

## Evaluation and cost plan

Create a representative set with answerable, ambiguous, unanswerable, and adversarial questions. Keep tuning examples separate from the final evaluation set. Include questions whose correct answer changes after a document update or permission revocation.

Report retrieval recall at a specified number of results, citation support, answer correctness, and abstention behavior separately. Combine automated checks with a documented human-review rubric; do not treat a model-based judge as proof that hallucinations have been eliminated.

Measure ingestion time and embedding cost separately from question latency and generation cost. Record p50/p95 latency, input/output tokens, retrieval failures, provider errors, and the pricing date used for estimates. Compare caching or retrieval changes on the same held-out questions while also checking freshness and authorization. No savings or production-scale claims are established by this proposal.
