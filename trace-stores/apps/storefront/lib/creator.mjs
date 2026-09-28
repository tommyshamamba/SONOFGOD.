export const MAX_UPLOAD_BYTES = 10 * 1024 * 1024;
export const CART_KEY = "trace-store-cart-v1";
export const products = [
  { id: "die-cut", name: "Die Cut Stickers", price: 19, mark: "✦" },
  { id: "clear", name: "Clear Stickers", price: 24, mark: "◌" },
  { id: "labels", name: "Custom Labels", price: 28, mark: "▣" },
];

/** @param {{type: string, size: number}} file */
export function validateArtwork(file) {
  if (!["image/png", "image/jpeg", "image/webp"].includes(file.type)) {
    throw new Error("Choose a PNG, JPEG, or WebP image.");
  }
  if (!file.size || file.size > MAX_UPLOAD_BYTES) {
    throw new Error("Choose an image between 1 byte and 10 MB.");
  }
}

/**
 * @param {File} file
 * @param {{apiUrl: string, signal?: AbortSignal, fetchImpl?: typeof fetch}} options
 */
export async function removeBackground(file, { apiUrl, signal, fetchImpl = fetch }) {
  validateArtwork(file);
  const body = new FormData();
  body.append("image", file);
  let response;
  try {
    response = await fetchImpl(`${apiUrl.replace(/\/+$/, "")}/v1/remove-background`, {
      method: "POST", body, signal,
    });
  } catch (error) {
    if (signal?.aborted) throw error;
    throw new Error("Cannot reach the image service. Check that the Trace API is running, then retry.");
  }
  if (!response.ok) {
    const details = await response.json().catch(() => null);
    throw new Error(typeof details?.detail === "string"
      ? details.detail
      : `The image service could not process this file (${response.status}). Please retry.`);
  }
  if (!response.headers.get("content-type")?.toLowerCase().startsWith("image/png")) {
    throw new Error("The image service returned an unexpected format. Please retry.");
  }
  const blob = await response.blob();
  if (!blob.size) throw new Error("The image service returned an empty image. Please retry.");
  return { blob, processor: response.headers.get("X-Trace-Processor") };
}

/** @param {string} name */
export function downloadName(name) {
  return `${name.replace(/\.[^.]+$/, "").replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 100) || "artwork"}-transparent.png`;
}

/** @param {unknown} value @returns {Record<string, number>} */
export function sanitizeCart(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  const result = {};
  for (const product of products) {
    const quantity = /** @type {Record<string, unknown>} */ (value)[product.id];
    if (typeof quantity === "number" && Number.isInteger(quantity) && quantity > 0) {
      result[product.id] = Math.min(quantity, 99);
    }
  }
  return result;
}

/** @param {Record<string, number>} cart @param {string} id @param {number} delta */
export function changeQuantity(cart, id, delta) {
  const result = sanitizeCart(cart);
  if (!products.some(product => product.id === id) || !Number.isInteger(delta)) return result;
  const quantity = Math.max(0, Math.min(99, (result[id] || 0) + delta));
  if (quantity) result[id] = quantity;
  else delete result[id];
  return result;
}

/** @param {Record<string, number>} cart */
export function cartTotals(cart) {
  const quantities = sanitizeCart(cart);
  return products.reduce((total, product) => ({
    count: total.count + (quantities[product.id] || 0),
    price: total.price + (quantities[product.id] || 0) * product.price,
  }), { count: 0, price: 0 });
}
