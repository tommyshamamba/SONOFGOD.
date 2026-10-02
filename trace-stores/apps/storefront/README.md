# Trace storefront

**Upload artwork, preview a transparent PNG, and explore custom-print products.**

This Next.js 14 / React 18 frontend connects to the [Trace image API](../../README.md). It displays the actual returned image, identifies ONNX or development-fallback processing, offers PNG download, and maintains a local demo cart.

[Project overview and API setup](../../README.md) · [UI source](app/page.tsx) · [Upload and cart logic](lib/creator.mjs) · [Tests](tests/creator.test.mjs)

## Local launch

Use Node.js 24 with npm for the local setup and documented test commands. The test script uses `--test-isolation=none`; that flag was renamed after Node 22, as documented in the [Node CLI reference](https://nodejs.org/download/release/v24.19.0/docs/api/cli.html#--test-isolationmode). The existing repository workflow still selects Node 22 and needs a runtime or test-command update. Start the API using the [Bash or PowerShell setup](../../README.md#1-start-the-api), then open another terminal at the repository root:

```sh
cd trace-stores/apps/storefront
npm ci
npm run dev
```

Open [the storefront](http://localhost:3000). The API defaults to `http://localhost:8000`.

## Configuration

To use another API origin, copy `.env.example` to `.env.local` in this directory.

```bash
# Bash
cp .env.example .env.local
```

```powershell
# PowerShell
Copy-Item .env.example .env.local
```

Set the value in `.env.local`:

```dotenv
NEXT_PUBLIC_TRACE_API_URL=http://localhost:8000
```

This address must be reachable from the browser. Configure the API's `CORS_ORIGINS` to include the storefront origin. An HTTPS frontend needs an HTTPS API to avoid mixed-content blocking. Restart the development server or rebuild the production frontend after changing this public environment variable; it must never contain a secret.

## Artwork and cart behavior

1. Select or drop one PNG, JPEG, or WebP image, up to 10 MiB. The API also validates image content and its 16-megapixel limit.
2. Compare the original and processed images, inspect the processor label, and download the PNG.
3. Preview the artwork on products and add quantities to the cart. The cart supports quantity changes, removal, clearing, and restoration after refresh.

Replacing an image cancels the previous request and clears its result. Uploads have a two-minute client timeout, a cancel control, and visible errors. Preview object URLs are released when replaced or unmounted.

The cart saves product quantities in `localStorage`; it does not save uploaded artwork. Prices are estimates in USD, previews are illustrative, and checkout, orders, payments, and fulfillment are not connected.

## Verification

From this directory, after `npm ci`:

```sh
npm test
npm run build
```

`npm test` runs Node's test runner over the upload client and cart logic. It checks request content, failure handling, cancellation, output validation, safe download filenames, cart sanitization, quantities, and totals. The live upload test is skipped unless `TRACE_TEST_API_URL` is configured.

With the API running, include the live multipart upload test:

```bash
# Bash
TRACE_TEST_API_URL=http://localhost:8000 npm test
```

```powershell
# PowerShell
$env:TRACE_TEST_API_URL = "http://localhost:8000"
npm test
```

To inspect the production build locally, stop the development server on port 3000 and run:

```sh
npm start
```

The API must remain running. In restricted Windows environments that cannot create Next.js build worker processes, the existing configuration supports:

```powershell
$env:BUILD_WITH_THREADS = "1"
npm run build
```

For a browser check, upload and replace an image, verify PNG download and the processor label, cancel an upload, and confirm that a service error does not show a successful result. Then add multiple products, change quantities, remove an item, refresh to check persistence, and clear the cart. Node tests exercise client logic; these manual checks cover the rendered workflow.

## Source map

| File | Responsibility |
|---|---|
| [`app/page.tsx`](app/page.tsx) | Creator workflow, image previews, request lifecycle, and cart dialog. |
| [`app/styles.css`](app/styles.css) | Storefront layout and visual styling. |
| [`lib/creator.mjs`](lib/creator.mjs) | Upload validation/client, product definitions, filenames, and cart calculations. |
| [`tests/creator.test.mjs`](tests/creator.test.mjs) | Client/cart tests and optional live API request. |
| [`.env.example`](.env.example) | Browser API URL and optional build setting. |

See the [project scope and next steps](../../README.md#scope-and-next-steps) for the backend and commerce work needed before a hosted product release.
