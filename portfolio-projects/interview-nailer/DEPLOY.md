# Interview Nailer deployment

Start with the [local demo guide](../../docs/LOCAL_DEMOS.md). The application serves its built frontend and API from one Node.js process; a separate frontend host is optional.

## Local demo

Use Node.js 24.19 or later in the 24.x line. From this project directory:

```sh
npm run build:render
npm --prefix backend start
```

With no provider credentials or database URL, development mode uses labeled mock AI and a local JSON store. Open http://localhost:5000. The local store supports one process; use PostgreSQL for hosted deployments. Keep real applicant data out of shared demonstrations.

## Render Blueprint

The supplied `render.yaml` is a configuration example; it has not been deployed from this workspace.

1. In Render, select this GitHub repository and the Blueprint path `portfolio-projects/interview-nailer/render.yaml`.
2. Keep the service root directory `portfolio-projects/interview-nailer`. If you extract this project into its own repository, change `rootDir` to `.`.
3. Review the proposed web service and PostgreSQL plans, limits and charges in Render before creating them. Free database availability and retention are provider restrictions, not application guarantees.
4. The Blueprint builds both components, generates a signing secret and injects the database URL. It explicitly sets `AI_MODE=mock` and `ALLOW_MOCK_AI=true`, so the hosted interface remains a labeled demonstration.
5. The start command applies the idempotent schema before starting the service. Render's free web tier does not support `preDeployCommand`; a paid, multi-instance deployment should run migrations once in a dedicated deployment step.
6. The web service and database use the same region and private network. `DATABASE_SSL=false` applies only to that internal connection. For an external database URL, use certificate-verified TLS and configure trust for the database provider; do not carry this internal-network setting over.
7. `RENDER_EXTERNAL_URL` supplies the HTTPS origin automatically. `TRUST_PROXY_HOPS=1` tells Express about Render's reverse proxy. Reassess this value if adding another proxy.

After deployment, verify `/health` and `/api/status`, then register a synthetic account, complete a practice interview, restart the service and verify that the interview remains available. Provider deployment success and these browser checks are still required.

## Real AI mode

Set `AI_MODE=anthropic`, `ANTHROPIC_API_KEY` and the account-supported `ANTHROPIC_MODEL` in the hosting provider's secret/environment controls. Set `ALLOW_MOCK_AI=false`. Do not commit a key. Run a bounded sample interview and confirm provider costs, response contracts and error handling before inviting users. Live provider responses have not been tested without an account.

## Separate frontend host

Build the frontend with `REACT_APP_API_BASE_URL` set to the backend HTTPS API URL, including `/api` (for example, `https://interview-api.example.com/api`). Set backend `CLIENT_URL` to the exact frontend HTTPS origin. This value is embedded at build time, so rebuild after changing it. Configure the static host to send application routes to `index.html`.

## Configuration references

- [Render monorepo roots](https://render.com/docs/monorepo-support)
- [Render Blueprint specification](https://render.com/docs/blueprint-spec)
- [Render deployment steps](https://render.com/docs/deploys)
- [Render PostgreSQL connections and TLS](https://render.com/docs/postgresql-creating-connecting)

Local build and API tests validate application behavior. They do not verify Render provisioning, backups, production traffic, email delivery or a live AI account.
