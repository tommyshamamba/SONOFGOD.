# Run the local demos

These commands run synthetic demonstrations on your computer. Node 22.15+ and Python 3.12 are required. Run each command from its named folder, in a separate terminal. Install dependencies once with `npm ci`. Stop a server with Ctrl+C.

| Demo | Address | What works |
|---|---|---|
| Trace storefront | http://localhost:3000 | Actual API upload, processed PNG preview/download, browser-persisted demo cart |
| Trace API | http://localhost:8000/docs | U2NetP ONNX inference with required-model validation |
| Interview Nailer | http://localhost:5000 | Registration, résumé upload, mock interview, scoring, coaching and saved history |
| PESA FAMS | http://localhost:3100 | Local fixed-asset workflows; optional real PostgreSQL mode |
| Kubernetes application | http://127.0.0.1:3200 | Application/configuration display backed by the local API |
| Blockchain dashboard | http://localhost:3300 | Registration, persistent API keys, revocation and explicitly simulated blockchain queries |
| Voice simulation | http://127.0.0.1:8090 | Missed-call events, persistent response drafts, duplicate protection and optional browser speech preview |

## Trace

From `trace-stores`, download the model:

```powershell
python scripts/download_model.py
```

From `trace-stores/services/trace-api`:

```powershell
python -m pip install -r requirements.txt
$env:MODEL_PATH = (Resolve-Path ../../models/u2netp.onnx).Path
$env:REQUIRE_MODEL = 'true'
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

From `trace-stores/apps/storefront`:

```powershell
npm ci
npm run dev
```

Choose a PNG/JPEG/WebP, inspect the processor label and download the returned PNG. Add products and refresh to verify the cart. The cart does not submit orders or collect payments. A production build is `npm run build` followed by `npm start`; restricted Windows hosts can set `$env:BUILD_WITH_THREADS='1'` before building.

## Interview Nailer

From `portfolio-projects/interview-nailer/frontend`, run `npm ci` then `npm run build`.
From its `backend` folder:

```powershell
npm ci
$env:AI_MODE = 'mock'
$env:STORAGE_MODE = 'file'
$env:PORT = '5000'
npm start
```

Open the application and register a synthetic account with a password of at least 12 characters. Upload a TXT/PDF résumé containing 100–50000 characters, then start an interview and save answers. Mock outputs are fixed demonstration logic, not a live AI assessment.

File mode supports one application process. Data is stored in `backend/data/store.json`; keep that file private. PostgreSQL mode is separately supported and tested. Provider mode exists but live Anthropic calls require your own key and remain unverified here. Do not paste keys into chats or commit them. The backend buffers and validates structured answers before emitting its response stream.

## PESA FAMS banking

From `portfolio-projects/pesa-fams-banking-prototype`:

```powershell
npm ci
$env:APP_MODE = 'prototype'
$env:HOST = '127.0.0.1'
npm start
```

The local login page supplies demo accounts. Prototype state is a demonstration; use database mode for persistence. Follow the project's PostgreSQL setup instructions with a new disposable database. Never run `db:seed` on data you need to retain: it resets the demo tables. Financial posting is local prototype behavior; no Finacle connection or real bank transaction is performed.

## Blockchain dashboard

From `portfolio-projects/blockchain-api-service/frontend`, run `npm ci` then `npm run build`.
From its `backend` folder:

```powershell
npm ci
$env:DEMO_MODE = 'true'
$env:PORT = '3300'
npm start
```

Register a synthetic account, create a key, copy it once and test address `0x1111111111111111111111111111111111111111`. Demo mode performs no external RPC calls or transactions. Revoke the key and confirm it stops working. Accounts and hashed keys persist in `backend/data/store.json`; raw keys are only returned when created. The local signing key changes on restart unless JWT_SECRET is configured, so sign in again after restarting.

Storage is single-process and takes an exclusive lock. Stop normally with Ctrl+C. If a crash leaves `store.json.lock`, confirm the process ID inside it is no longer running before removing only that lock file. Keep the data file. Multiple replicas require shared database storage and are not supported by this file store.

## Kubernetes application without a cluster

From `portfolio-projects/kubernetes-demo/frontend`, run `npm ci` and `npm run build`. From `backend`, run `npm ci` and `npm start`. The backend serves the built website at port 3200. This verifies the application locally, not a Kubernetes cluster rollout. Compose and Kubernetes use `HOST=0.0.0.0` inside their containers.

## Voice simulation

From `portfolio-projects/voice-ai-missed-call`:

```powershell
python demo/server.py
```

Simulate a missed call, inspect its draft and replay the same event. Nothing is sent to a real telephone or provider. Saved events are in `demo/data/demo.sqlite`. Browser audio is optional and depends on installed speech voices.

## Verification

- Run `npm test` in each backend and the Trace storefront.
- For banking PostgreSQL cases, set `TEST_DATABASE_URL` to a migrated/seeded `pesa_test_` database.
- For Interview PostgreSQL cases, set `TEST_STORAGE_MODE=postgres`, `STORAGE_MODE=postgres`, `DATABASE_SSL=false` and `DATABASE_URL` to a disposable `interview_test_` database, run `npm run db:init`, then `npm test`.
- Run `python -m unittest discover -s demo -p 'test_*.py'` in the Voice project.
- Browser smoke script: from the repository root run `npm ci`, `npx playwright install chromium`, start Trace/Interview/Kubernetes/Voice as above, then `npm run test:browser`. It creates synthetic records only. Browser execution was blocked by Windows process permissions in this environment and is not recorded as passed.
- Terraform: inside each module/root run `terraform init -backend=false`, then `terraform validate`. These commands do not provision cloud resources. No `apply` is needed for local demos.

See [the current verification report](LOCAL_DEMO_VERIFICATION.md) for exact results and outstanding limitations.
