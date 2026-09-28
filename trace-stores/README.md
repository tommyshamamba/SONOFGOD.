# Trace & Store

A portfolio-ready monorepo that pairs a FastAPI image-processing service with a polished Next.js custom-product storefront.

## What is included

- **Trace API** — executes U2-Net/U2-NetP ONNX inference on CPU, validates and warms up the model before readiness, and preserves image transparency. Optional development fallback is explicitly reported.
- **Storefront** — a responsive creator flow for artwork uploads, product discovery, and print-product previews.
- **Local environment** — Docker Compose launches both services together.
- **Quality gates** — API health test and GitHub Actions workflow.

## Run locally

### Docker

```bash
python scripts/download_model.py
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

Model weights are excluded from Git. The download script retrieves U2NetP from the rembg release and verifies the upstream checksum. Review upstream model licensing before deployment. Compose mounts `models/` read-only.

Set `REQUIRE_MODEL=true` when inference is required; missing, corrupt or incompatible models then prevent startup. Without Docker, set `MODEL_PATH` to the absolute weights path. `/health` and `/ready` report active mode. Inference failures return 503; existing transparency is preserved. Uploads are limited to 10 MB and 16 megapixels.

## Verification

From `services/trace-api`, run `pip install -r requirements-test.txt` and `python -m pytest tests`. Set `TEST_PRETRAINED_MODEL` to downloaded weights to include the optional pretrained smoke test. This verifies execution, not segmentation quality across a benchmark dataset.

From `apps/storefront`, run `npm ci` and `npm run build`. Restricted Windows environments can set `BUILD_WITH_THREADS=1` to avoid child-process restrictions.


## Production next steps

1. Add a signed-object upload flow backed by S3-compatible storage.
2. Put image jobs behind Redis and a worker queue.
3. Persist products and orders in PostgreSQL through Prisma.
4. Implement Stripe Checkout using server-side price IDs and webhook verification.
5. Add request tracing, rate limiting, and image retention policies.
