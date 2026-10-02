# Interview Nailer

Interview preparation with accounts, résumé upload, practice sessions, answer scoring and saved coaching reports. The local demonstration uses deterministic mock AI; live Anthropic responses require separate configuration.

[Portfolio](../../README.md) · [Local demos](../../docs/LOCAL_DEMOS.md) · [Deployment](DEPLOY.md) · [Verification](../../docs/VERIFICATION.md)

## Architecture

React and Vite provide the client. Express handles authentication, uploads and structured AI responses. Storage can use a serialized local JSON store for a single process or PostgreSQL for shared persistence. Ownership checks apply to résumés and interview sessions.

## Run locally

Use Node 24.19.0 (the repository `.nvmrc`). From this directory:

```sh
npm run install:backend
npm run install:frontend
npm run build:frontend
npm --prefix backend start
```

Open http://localhost:5000. With no provider/database settings, development uses `AI_MODE=mock` and `STORAGE_MODE=file`. Use a synthetic account and résumé. The backend serves the built frontend on the same origin.

For frontend development, keep the backend running and start a second terminal in this directory:

```sh
npm run dev:frontend
```

The Vite client runs at http://localhost:3001 and proxies `/api` to port 5000. Copy each component's `.env.example` only when changing its configuration. A separate frontend host uses `REACT_APP_API_BASE_URL=https://your-backend.example/api`; rebuild after changing it and configure backend `CLIENT_URL` to the exact frontend origin.

## Tests

```sh
npm --prefix backend test
npm --prefix frontend test
npm run build:frontend
```

Backend tests cover configuration, response contracts, accounts, uploads, cross-user ownership, full mock interview sessions, concurrent writes and rate limiting. Frontend tests cover API URL resolution for same-origin, split-host and local-network use. The repository browser suite completes every interview question and reloads the coaching report to verify persistence.

For PostgreSQL tests, configure `STORAGE_MODE=postgres`, `TEST_STORAGE_MODE=postgres` and `DATABASE_URL` for a disposable database whose name begins `interview_test_`; set `DATABASE_SSL=false` only for a local database. Run `npm --prefix backend run db:init` followed by the backend tests. GitHub Actions supplies its own disposable PostgreSQL service.

## Configuration and limits

- File storage supports one application process. Keep its data file and uploaded content private; hosted persistence uses PostgreSQL.
- Invalid AI/storage modes fail startup. Production requires a strong signing secret and PostgreSQL. Production mock mode requires the explicit `ALLOW_MOCK_AI=true` setting and remains visibly labeled.
- Live mode uses `AI_MODE=anthropic`, `ANTHROPIC_API_KEY` and `ANTHROPIC_MODEL`. Configure secrets outside Git. Mock tests do not verify a live provider account.
- The [Render Blueprint](render.yaml) is a deployment example; provisioning, TLS, backups and live provider calls require deployment-specific verification.

See [deployment instructions](DEPLOY.md) and the [current evidence report](../../docs/VERIFICATION.md) for tested scope and remaining work.
