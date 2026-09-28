import test from "node:test";
import assert from "node:assert/strict";
import { MAX_UPLOAD_BYTES, cartTotals, changeQuantity, downloadName, removeBackground, sanitizeCart, validateArtwork } from "../lib/creator.mjs";

const png = Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAIAAAAlC+aJAAABG0lEQVR4nO2aaxICIQyDQ8Z7O548nkAFWliy9fvr2CSkPmaHJgnOEOYQ5hDmEOYQ5hDmEOYQ5hDmPBbNbe316SXpmSmk1L8SX3wvStKyAgxZT4xBXOoesfcmNBCUj1fBQ9xjdhoPcT89k+e4n5vMo9xPzLf/JeZpxz+qwgPdD2lVWqG28fj7FSs14B2gbd+fTt0yDXgHaBftT496jQZOhjCHMIcwhzCHMIcwhzCHqBBAqc+TR9FX9RoN3CGALtoi/dIt08AdAmj7FqlDsVID2FuC+rSGG9iTQd0qxVZoTwkamT/ZwLoMGpw8v0IrMmh8ZugzkJtBU9NyrhoEH90pcBA530IRB4rV+L+tcrP7QvshzCHMIczh1QaivAHh8mljhbovPQAAAABJRU5ErkJggg==", "base64");
const file = () => new File([png], "artwork.png", { type: "image/png" });

test("upload sends the actual image to the API and returns its PNG and processor", async () => {
  const artwork = file();
  const result = await removeBackground(artwork, { apiUrl: "http://localhost:8000/", fetchImpl: async (url, options) => {
    assert.equal(url, "http://localhost:8000/v1/remove-background");
    assert.equal(options.method, "POST");
    const uploaded = options.body.get("image");
    assert.equal(uploaded.name, artwork.name);
    assert.deepEqual(Buffer.from(await uploaded.arrayBuffer()), png);
    return new Response(png, { headers: { "Content-Type": "image/png", "X-Trace-Processor": "u2netp.onnx" } });
  } });
  assert.equal(result.processor, "u2netp.onnx");
  assert.deepEqual(Buffer.from(await result.blob.arrayBuffer()), png);
});

test("fallback identity is preserved instead of claiming AI inference", async () => {
  const result = await removeBackground(file(), { apiUrl: "", fetchImpl: async () => new Response(png, { headers: { "content-type": "image/png", "X-Trace-Processor": "development alpha-mask fallback" } }) });
  assert.match(result.processor, /fallback/);
});

test("invalid types, empty files and oversized files are rejected before upload", async () => {
  assert.throws(() => validateArtwork({ type: "text/html", size: 10 }), /PNG, JPEG, or WebP/);
  assert.throws(() => validateArtwork({ type: "image/png", size: 0 }), /10 MB/);
  let requests = 0;
  await assert.rejects(removeBackground({ type: "image/png", size: MAX_UPLOAD_BYTES + 1 }, { apiUrl: "", fetchImpl: async () => { requests++; } }), /10 MB/);
  assert.equal(requests, 0);
});

test("API validation errors are surfaced without displaying success", async () => {
  await assert.rejects(removeBackground(file(), { apiUrl: "", fetchImpl: async () => new Response(JSON.stringify({ detail: "Image exceeds 16 megapixels." }), { status: 413, headers: { "content-type": "application/json" } }) }), /16 megapixels/);
});

test("non-JSON failures and network failures have actionable messages", async () => {
  await assert.rejects(removeBackground(file(), { apiUrl: "", fetchImpl: async () => new Response("Bad gateway", { status: 502 }) }), /502/);
  await assert.rejects(removeBackground(file(), { apiUrl: "", fetchImpl: async () => { throw new TypeError("fetch failed"); } }), /Check that the Trace API is running/);
});

test("unexpected successful responses and empty images are rejected", async () => {
  await assert.rejects(removeBackground(file(), { apiUrl: "", fetchImpl: async () => new Response("{}", { headers: { "content-type": "application/json" } }) }), /unexpected format/);
  await assert.rejects(removeBackground(file(), { apiUrl: "", fetchImpl: async () => new Response("", { headers: { "content-type": "image/png" } }) }), /empty image/);
});

test("cancellation is forwarded to the upload request", async () => {
  const controller = new AbortController();
  const canceled = new DOMException("Canceled", "AbortError");
  controller.abort();
  await assert.rejects(removeBackground(file(), { apiUrl: "", signal: controller.signal, fetchImpl: async (_, options) => {
    assert.equal(options.signal, controller.signal);
    throw canceled;
  } }), { name: "AbortError" });
});

test("download filenames are PNGs without unsafe path characters", () => {
  assert.equal(downloadName("my logo.JPEG"), "my_logo-transparent.png");
  assert.equal(downloadName("../file.webp"), "___file-transparent.png");
});

test("cart restores only valid products and bounded integer quantities", () => {
  assert.deepEqual(sanitizeCart({ "die-cut": 2, clear: 1000, labels: -2, unknown: 5 }), { "die-cut": 2, clear: 99 });
  assert.deepEqual(sanitizeCart({ clear: "3", labels: 1.5 }), {});
  assert.deepEqual(sanitizeCart(null), {});
  assert.deepEqual(sanitizeCart([]), {});
});

test("cart additions, quantity changes, removal and totals agree", () => {
  let cart = changeQuantity({}, "die-cut", 1);
  cart = changeQuantity(cart, "die-cut", 1);
  cart = changeQuantity(cart, "clear", 1);
  assert.deepEqual(cartTotals(cart), { count: 3, price: 62 });
  cart = changeQuantity(cart, "clear", -1);
  assert.deepEqual(cart, { "die-cut": 2 });
  assert.deepEqual(changeQuantity(cart, "unknown", 10), cart);
  assert.deepEqual(changeQuantity({ "die-cut": 99 }, "die-cut", 1), { "die-cut": 99 });
  assert.deepEqual(cartTotals({ "die-cut": -2, unknown: 999 }), { count: 0, price: 0 });
});

test("configured live API returns a real PNG through the browser upload client", { skip: !process.env.TRACE_TEST_API_URL }, async () => {
  const result = await removeBackground(file(), { apiUrl: process.env.TRACE_TEST_API_URL });
  const data = Buffer.from(await result.blob.arrayBuffer());
  assert.deepEqual(data.subarray(0, 8), Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]));
  assert.ok(result.processor, "API must identify its processor");
});
