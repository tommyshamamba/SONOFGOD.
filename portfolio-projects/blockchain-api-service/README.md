# Blockchain API Service

A Node.js API and React dashboard for accounts, API keys, rate limits and blockchain queries. The default local demonstration returns labeled, deterministic blockchain data. Transaction broadcasting is disabled.

See the [local demo guide](../../docs/LOCAL_DEMOS.md) and [verification results](../../docs/LOCAL_DEMO_VERIFICATION.md).

## Run locally

Use Node.js 24.19 or later in the 24.x line. From this directory:

```sh
npm --prefix backend ci
npm --prefix frontend ci
npm --prefix frontend run build
```

In PowerShell:

```powershell
$env:DEMO_MODE = 'true'
npm --prefix backend start
```

Open http://localhost:3300. Register a synthetic account, create a key and copy the full secret once; use that key to query the demo. The backend serves the built dashboard and API on the same origin. Development commands and all six application links are in the local guide.

## Docker Compose

```sh
docker compose up --build -d --wait
```

- Dashboard: http://localhost:8081
- Direct API: http://localhost:3301
- API readiness: http://localhost:3301/ready

The Compose project binds published ports to this computer, uses Redis for rate limiting, and stores JSON data in the `blockchain-data` named volume. The frontend proxies API requests to the backend. It waits for backend readiness; the backend waits for Redis readiness. Run `docker compose down` to stop; keep the named volume to preserve accounts and keys.

Both Compose files in this portfolio use distinct host ports. Docker Desktop's Linux engine must be running. Container builds and a live Kubernetes rollout still require verification in a working container environment.

## Persistence and crash recovery

Users, password hashes, hashed API keys, revocations and usage counters live in `DATA_FILE` (default `backend/data/store.json`). Writes flush a temporary file and atomically replace the JSON snapshot. No raw API key is stored.

The adjacent `.lock.sqlite` file supplies an OS-backed exclusive lock. A second writer fails immediately; a terminated writer releases the lock through the operating system. The mutex file stays on disk permanently. **Do not delete `.lock.sqlite` to unlock a store:** an active process may still hold it, and deletion can create independent locks. Restarting is sufficient after a crash.

Use one backend replica and a local, durable filesystem. Network filesystems, multi-host shared volumes and horizontal backend scaling are unsupported. Move account/key storage to a transactional shared database before scaling. Keep the entire data volume in backups, and test restores independently.

### Upgrading an older checkout

Older code created a PID file at `DATA_FILE.lock`. The new version refuses that legacy lock instead of guessing whether an older process is still writing. Stop every old service/container that uses the store, back up the JSON file, and verify the recorded PID/process is gone. Only then remove the legacy `.lock` file. Keep the JSON snapshot and the new `.lock.sqlite` file. Existing container volumes must be writable by the new backend's UID/GID `1000:1000`; adjust ownership while stopped if an older root-owned container created the volume.

## Kubernetes local example

Start a local Minikube cluster, then build and load explicitly tagged images:

```sh
docker build -t blockchain-api-backend:local ./backend
docker build -t blockchain-api-frontend:local ./frontend
minikube image load blockchain-api-backend:local
minikube image load blockchain-api-frontend:local
kubectl apply -f k8s/
kubectl rollout status deployment/blockchain-api-backend --timeout=180s
kubectl rollout status deployment/blockchain-api-frontend --timeout=180s
minikube service blockchain-api-frontend
```

The manifests use `imagePullPolicy: IfNotPresent`, one backend replica, `Recreate` updates and a persistent volume claim. The HPA is capped at one backend replica because file storage is local. Sample secrets are demonstration values. For registry deployments, use your own registry image tags/digests and secret management. The example is not a verified production deployment.

## API

Authentication: `POST /api/auth/register`, `POST /api/auth/login`.

API key management uses `Authorization: Bearer <token>`: `POST /api/keys`, `GET /api/keys`, `DELETE /api/keys/:keyId`. Keys are scoped to their owner; only the creation response contains the full secret.

Blockchain endpoints use `X-API-Key: <full secret>`:

- `GET /api/v1/:chain/balance/:address`
- `GET /api/v1/:chain/nonce/:address`
- `GET /api/v1/:chain/block/:blockNumber`
- `GET /api/v1/:chain/transaction/:txHash`
- `GET /api/v1/:chain/gas-price`
- `POST /api/v1/:chain/estimate-gas`
- `POST /api/v1/:chain/broadcast` (disabled by default)
- `GET /api/v1/usage`

`GET /api/v1/chains` lists configured chains without exposing RPC URLs. `/health` reports the mode; `/ready` checks dependencies. Provider failures return bounded errors, and a Redis outage fails closed with HTTP 503.

## Live configuration and limits

`DEMO_MODE=true` makes no blockchain RPC calls. For live mode configure the four RPC URLs, Redis, durable `DATA_FILE`, exact allowed `CORS_ORIGIN` values and a strong random `JWT_SECRET`. Production rejects demonstration mode and incomplete settings. Broadcasting additionally requires its explicit opt-in. Live provider behavior, real transactions, performance, backups and cloud provisioning are not verified by the local test suite.

The [Terraform directory](terraform/README.md) contains a separately validated infrastructure example. It does not connect the application to every provisioned resource. Billing, paid tiers, transaction submission guarantees and commercial service-level commitments are not implemented.

## Tests

```sh
npm --prefix backend test
```

The suite checks authentication, key ownership, persistence, rate limiter/provider failures, crash locking and configuration. The forced-process-termination case requires permission to launch child processes; Windows application sandboxes may block it. GitHub Actions runs the complete suite on Linux.

## License

Proprietary — all rights reserved. No additional open-source license is granted here.
