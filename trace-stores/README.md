# Trace & Store

A portfolio-ready monorepo that pairs a FastAPI image-processing service with a polished Next.js custom-product storefront.

## What is included

- **Trace API** — validates uploaded images and returns a PNG using a near-white-pixel alpha mask. ONNX inference is not implemented. The existing health flag detects a model file's presence, not successful model loading or inference; do not use that flag as evidence of model readiness.
- **Storefront** — a responsive creator flow for artwork uploads, product discovery, and print-product previews.
- **Local environment** — Docker Compose launches both services together.
- **Quality gates** — API health test and GitHub Actions workflow.

## Run locally

### Docker

```bash
docker compose up --build
```

Open `http://localhost:3000` for the storefront and `http://localhost:8000/docs` for the API documentation.

### Without Docker

```bash
cd services/trace-api
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

In another shell:

```bash
cd apps/storefront
npm install
npm run dev
```

## API contract

`POST /v1/remove-background` accepts an `image` form field containing PNG, JPEG, or WebP up to 10 MB and returns `image/png`.

> The checked-in implementation deliberately does not redistribute a model file. Mount a licensed U²-Net-compatible ONNX model at `/models/u2net.onnx` to signal model readiness, then replace the documented fallback mask with the model inference adapter for production.

## Production next steps

1. Add a signed-object upload flow backed by S3-compatible storage.
2. Put image jobs behind Redis and a worker queue.
3. Persist products and orders in PostgreSQL through Prisma.
4. Implement Stripe Checkout using server-side price IDs and webhook verification.
5. Add ONNX model inference, request tracing, rate limiting, and image retention policies.
