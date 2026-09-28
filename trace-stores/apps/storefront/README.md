# Trace storefront

Next.js artwork workflow and local demo cart. Images are uploaded to the Trace API; the returned PNG is displayed, previewed on products, and downloadable. The UI reports whether the API used ONNX or the development fallback. Failed uploads never show a completed result.

## Local launch

Start the API first using the instructions in [the Trace README](../../README.md). In this directory:

```sh
npm ci
npm run dev
```

Open http://localhost:3000. The API defaults to http://localhost:8000. To change it, copy `.env.example` to `.env.local` and set `NEXT_PUBLIC_TRACE_API_URL` to an address reachable from your browser. Set the API's `CORS_ORIGINS` to the storefront origin. For a deployed HTTPS website, use an HTTPS API address. Restart development or rebuild after changing the public environment variable.

## Verification

```sh
npm test
npm run build
npm start
```

Node 22 or newer is required for the test runner. With a local API already running, set `TRACE_TEST_API_URL=http://localhost:8000` before `npm test` to include the live multipart upload test. On restricted Windows environments, set `BUILD_WITH_THREADS=1` before `npm run build`.

Manual browser check:

1. Upload a PNG, JPEG, or WebP. Compare original and processed images, check the processor label, and download the PNG.
2. Upload a replacement image; verify that the previous preview disappears. Retry after a service error, and cancel an in-flight upload.
3. Add multiple products. Open the cart, change quantities, remove an item, refresh, and verify the cart is restored. Clear the cart.

## Scope

The cart stores product quantities in this browser. It does not store artwork, create orders, take payments, or fulfill products. Prices are estimates and previews are illustrative, not print proofs. The UI validates file type and size; the API additionally validates image content and its 16-megapixel limit. Object URLs are revoked on replacement or unmount and pending uploads are canceled when replaced.
