# Custom print commerce

**Status: design proposal; not implemented. No validated benchmarks.**

Working identifier: `stickermule-clone`. The proposed product is an independent custom-print commerce demonstration. It is not affiliated with or endorsed by Sticker Mule, and it does not reproduce that company's proprietary implementation or brand assets.

[All proposals](README.md) · [Project catalog](../PROJECT_CATALOG.md) · [Existing storefront](../../trace-stores/apps/storefront/README.md)

## Purpose and first release

Extend an artwork-preview workflow into a small, dependable commerce backend: browse a product catalog, persist a cart, complete Stripe test-mode checkout, and view an order whose payment status is verified by the server.

The existing [Trace & Store](../../trace-stores/README.md) contains image processing, product previews, and a browser-local demo cart. It does not create orders, take payments, or fulfill products. The backend and integrations below are future work.

Start with a Go modular monolith, one storefront client, and explicit tenant boundaries. Email marketing, SMS campaigns, live support, native mobile clients, manufacturing, and shipping-provider integrations are outside the first release. Product previews remain illustrative until a separate print-proof workflow is implemented.

## Proposed architecture

```text
Next.js storefront -> Go GraphQL API -> PostgreSQL
                                  -> Stripe test-mode Checkout
Stripe webhook -> verified event inbox -> order state transition
Committed order event -> outbox worker -> transactional notification
Artwork upload -> existing Trace API -> preview
```

| Component | Initial choice and responsibility |
|---|---|
| API | Go and GraphQL for products, carts, checkout initiation, and authorized order reads |
| Client | Extend the existing Next.js/TypeScript storefront before introducing a second client |
| Database | PostgreSQL for tenants, products, server-owned prices, carts, orders, and event records |
| Payments | Stripe test mode with server-side totals, signed webhook verification, and replay-safe handlers |
| Artwork | Existing Trace API for previews; private object storage and authorized downloads for any retained uploads |
| Deployment | Docker first; evaluate GCP Cloud Run and managed PostgreSQL after the local workflow passes verification |

Keep catalog, checkout, orders, and notifications as modules with explicit boundaries. Use database constraints and transactions for consistency before introducing distributed services. Every resolver must derive tenant identity from authenticated server context and enforce object ownership; client-supplied IDs are not authorization.

## Checkout and order integrity

1. Recalculate quantities and prices on the server from an active catalog. Persist an immutable order-price snapshot, currency, and a pending-payment order before creating checkout.
2. Create a payment-provider session with an idempotency key. Associate it with the internal order and tenant; define retry and reconciliation behavior for a lost response.
3. Verify webhook signatures against the raw request body. Persist a unique provider-event ID, validate the payment amount and currency, and apply only permitted state transitions in a transaction.
4. Show confirmed payment only after the backend verifies a paid state. A browser redirect alone must never mark an order paid.
5. Publish a notification through a transactional outbox after the order update commits. Duplicate and out-of-order events must not duplicate orders, notifications, or fulfillment requests.

Document expiration, canceled checkout, failed payment, and refund states before exposing those operations. GraphQL requests need bounded pagination, query complexity limits, rate limits, and controlled error messages. Logs should contain internal identifiers without payment secrets or customer artwork.

## Later integrations, with a reason to add each

- **Redis:** add caching or background coordination only when profiling justifies it. PostgreSQL remains authoritative for money and orders.
- **Notifications and support:** add transactional email first. Marketing preferences, SMS consent, and support conversations need their own scoped data model and access controls.
- **Expo:** consider a mobile client after the API contract and storefront workflow are stable; shared TypeScript types do not imply one UI works unchanged everywhere.
- **Kubernetes and service mesh:** consider only when service count and operational requirements exceed the simpler deployment. No mesh or cross-cloud queue is required for checkout.
- **Analytics and observability:** add OpenTelemetry traces and order metrics early; a dedicated analytics datastore can follow measured reporting needs.

## Proposed implementation layout

The following paths are illustrative and do not exist as this commerce backend's implementation:

```text
cmd/api/           # Go application entry point
internal/catalog/  # Products and price validation
internal/checkout/ # Cart validation and payment-session creation
internal/orders/   # Order state and ownership rules
internal/payments/ # Webhook inbox, verification, and reconciliation
internal/notify/   # Transactional outbox consumer
db/migrations/     # Schema, constraints, and indexes
infra/             # Docker and optional GCP deployment configuration
```

## Milestones and acceptance criteria

| Milestone | Evidence required to complete it |
|---|---|
| 1. Persistent catalog and cart | Data survives restart; quantity/price validation and cross-tenant access tests pass against PostgreSQL |
| 2. Test-mode checkout | A browser workflow reaches a paid order through a verified provider event; altered client prices, invalid signatures, and payment mismatches are rejected |
| 3. Reliable order processing | Duplicate, concurrent, and out-of-order webhooks preserve valid state; simulated crashes and lost responses are reconciled without duplicate purchases |
| 4. Storefront integration | Cart persistence, checkout cancellation, pending payment, payment failure, and order history are demonstrated with clear UI states |
| 5. Deployment candidate | Configuration, secrets, migration, backup/restore, monitoring, and rollback procedures are exercised in a documented test environment |

## Verification and measurement plan

Use PostgreSQL integration tests for transactions and tenant isolation, payment-provider test fixtures for event handling, and an end-to-end browser test for checkout. Keep external calls out of ordinary unit tests; separately verify the actual test-mode payment integration.

Measure API latency by operation, database query counts, webhook processing lag, error rate, reconciliation backlog, and notification delivery outcomes under a published workload. Record dataset size, concurrency, environment, commit, and raw results. Treat payment-provider latency separately from application processing.

This proposal makes no claim about customers served, revenue, uptime, latency savings, security certification, or production readiness. Those claims require an implemented system and appropriate evidence.
