"use client";
import { ChangeEvent, DragEvent, useEffect, useRef, useState } from "react";
import { CART_KEY, cartTotals, changeQuantity, downloadName, products, removeBackground, sanitizeCart, validateArtwork } from "../lib/creator.mjs";

const apiUrl = process.env.NEXT_PUBLIC_TRACE_API_URL || "http://localhost:8000";
const money = (amount: number) => new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(amount);

export default function Home() {
  const [file, setFile] = useState<File | null>(null);
  const [originalUrl, setOriginalUrl] = useState("");
  const [result, setResult] = useState<{ blob: Blob; processor: string | null } | null>(null);
  const [resultUrl, setResultUrl] = useState("");
  const [processing, setProcessing] = useState(false);
  const [error, setError] = useState("");
  const [dragging, setDragging] = useState(false);
  const [cart, setCart] = useState<Record<string, number>>({});
  const [cartLoaded, setCartLoaded] = useState(false);
  const [cartNotice, setCartNotice] = useState("");
  const dialog = useRef<HTMLDialogElement>(null);
  const request = useRef<AbortController | null>(null);
  const totals = cartTotals(cart);

  useEffect(() => {
    try { setCart(sanitizeCart(JSON.parse(localStorage.getItem(CART_KEY) || "{}"))); }
    catch { setCart({}); }
    setCartLoaded(true);
  }, []);
  useEffect(() => {
    if (!cartLoaded) return;
    try { localStorage.setItem(CART_KEY, JSON.stringify(cart)); }
    catch { setCartNotice("Browser storage is unavailable. Your cart will last for this visit."); }
  }, [cart, cartLoaded]);
  useEffect(() => {
    if (!file) { setOriginalUrl(""); return; }
    const url = URL.createObjectURL(file);
    setOriginalUrl(url);
    return () => URL.revokeObjectURL(url);
  }, [file]);
  useEffect(() => {
    if (!result) { setResultUrl(""); return; }
    const url = URL.createObjectURL(result.blob);
    setResultUrl(url);
    return () => URL.revokeObjectURL(url);
  }, [result]);
  useEffect(() => () => request.current?.abort(), []);

  async function processFile(selected: File) {
    request.current?.abort();
    const controller = new AbortController();
    request.current = controller;
    setError(""); setResult(null); setFile(null); setProcessing(false);
    try { validateArtwork(selected); }
    catch (problem) { setError((problem as Error).message); return; }
    setFile(selected); setProcessing(true);
    let timedOut = false;
    const timeout = window.setTimeout(() => { timedOut = true; controller.abort(); }, 120_000);
    try {
      const response = await removeBackground(selected, { apiUrl, signal: controller.signal });
      if (request.current === controller && !controller.signal.aborted) setResult(response);
    } catch (problem) {
      if (request.current !== controller) return;
      setError(timedOut ? "Processing took too long. Try a smaller image or retry." : controller.signal.aborted ? "Processing canceled. You can retry or choose another image." : (problem as Error).message);
    } finally {
      window.clearTimeout(timeout);
      if (request.current === controller) setProcessing(false);
    }
  }
  function chooseFile(event: ChangeEvent<HTMLInputElement>) {
    const selected = event.target.files?.[0];
    event.target.value = "";
    if (selected) void processFile(selected);
  }
  function dropFile(event: DragEvent<HTMLLabelElement>) {
    event.preventDefault(); setDragging(false);
    if (event.dataTransfer.files.length !== 1) { setError("Drop one image at a time."); return; }
    void processFile(event.dataTransfer.files[0]);
  }
  function addToCart(id: string, name: string) {
    setCart(current => changeQuantity(current, id, 1));
    setCartNotice(`${name} added to your cart.`);
  }

  return <main>
    <nav><a className="brand" href="#top">trace<span>&</span>store</a><div className="links"><a href="#products">Products</a><a href="#how">How it works</a><button className="cart" onClick={() => dialog.current?.showModal()} aria-haspopup="dialog">Cart <b>{totals.count}</b></button></div></nav>
    <section className="hero" id="top"><p className="eyebrow">ARTWORK & CUSTOM PRINTING DEMO</p><h1>From rough idea<br/>to <i>ready-to-create.</i></h1><p className="lede">Remove backgrounds, explore your artwork on custom products, and build your own collection.</p><a className="button" href="#create">Start creating <span>→</span></a><div className="hero-art" aria-hidden="true"><div className="burst">MAKE<br/>IT<br/>STICK.</div><div className="sticker one">WILD<br/><small>IDEAS</small></div><div className="sticker two">✿</div><div className="sticker three">HELLO!</div></div></section>
    <section className="creator" id="create"><div><p className="eyebrow">THE CREATOR</p><h2>Your artwork,<br/><i>cleaned up.</i></h2><p>Upload an image to remove its background, compare the result, and download a transparent PNG.</p><p className="note">Your image is sent to the configured Trace image service for processing. Review edges and resolution before printing.</p></div><div className="upload-area"><label className={`drop${result ? " complete" : ""}${dragging ? " dragging" : ""}`} onDragOver={event => { event.preventDefault(); setDragging(true); }} onDragLeave={() => setDragging(false)} onDrop={dropFile}><input type="file" aria-label="Upload artwork" accept="image/png,image/jpeg,image/webp" onChange={chooseFile}/><span className="upload-icon" aria-hidden="true">↑</span><strong>{processing ? "Removing background…" : result ? "Your image is ready to preview" : "Drop artwork here or browse files"}</strong><small>PNG, JPG, or WebP · maximum 10 MB / 16 megapixels</small><small>Choose another image at any time.</small></label><div className="processing-status" role="status" aria-live="polite">{processing && <p>Processing {file?.name}… <button className="text-button" onClick={() => request.current?.abort()}>Cancel</button></p>}</div>{error && <div className="error" role="alert"><p>{error}</p>{file && !processing && <button className="light-button" onClick={() => void processFile(file)}>Retry image</button>}</div>}</div>
      {file && originalUrl && <div className="preview-section"><div className="previews"><figure><div className="image-surface"><img src={originalUrl} alt={`Original artwork: ${file.name}`}/></div><figcaption>Original</figcaption></figure>{result && resultUrl && <figure><div className="image-surface"><img src={resultUrl} alt="Processed artwork on a transparency grid"/></div><figcaption>Processed PNG</figcaption></figure>}</div>{result && resultUrl && <div className="result-actions"><div><p className="processor">{result.processor?.toLowerCase().includes("fallback") ? "Development fallback: removes near-white backgrounds. The AI model is not active." : result.processor ? `Processed with ${result.processor}.` : "Processed by the Trace API. Processor details were not provided."}</p><p className="note">Transparency is shown as a checkerboard. Product previews are illustrative and are not print proofs.</p></div><a className="button download" href={resultUrl} download={downloadName(file.name)}>Download PNG <span aria-hidden="true">↓</span></a></div>}</div>}
    </section>
    <section className="products" id="products"><p className="eyebrow">PICK YOUR CANVAS</p><h2>Made for your<br/><i>big ideas.</i></h2><p className="product-note">Demo products and estimated starting prices. Checkout and fulfillment are not connected.</p><p className="cart-status" role="status" aria-live="polite">{cartNotice}</p><div className="grid">{products.map(product => <article key={product.id}><div className="product-art">{resultUrl ? <img src={resultUrl} alt={`${product.name} artwork preview`}/> : <span aria-hidden="true">{product.mark}</span>}</div><h3>{product.name}</h3><p>From {money(product.price)}</p><button onClick={() => addToCart(product.id, product.name)} disabled={(cart[product.id] || 0) >= 99}>Add to cart <span aria-hidden="true">→</span></button></article>)}</div></section>
    <section className="how" id="how"><p className="eyebrow">SIMPLE BY DESIGN</p><div><h2>Create in three<br/><i>good steps.</i></h2><ol><li><b>01</b><span><strong>Upload your artwork</strong><em>Start with a PNG, JPEG, or WebP image.</em></span></li><li><b>02</b><span><strong>Remove the background</strong><em>Review the result and download your PNG.</em></span></li><li><b>03</b><span><strong>Explore a product</strong><em>Preview your artwork and save a demo cart.</em></span></li></ol></div></section>
    <footer><span>© 2026 Trace & Store</span><span>A custom-printing portfolio demo.</span></footer>
    <dialog ref={dialog} className="cart-dialog" aria-labelledby="cart-title"><div className="cart-heading"><h2 id="cart-title">Your cart</h2><button className="light-button" onClick={() => dialog.current?.close()} aria-label="Close cart">Close ×</button></div><p className="cart-disclaimer">Demo cart only. Prices are estimates in USD. No order is placed and no payment is collected.</p>{!totals.count ? <p>Your cart is empty. Add a product to get started.</p> : <><ul className="cart-items">{products.filter(product => cart[product.id]).map(product => <li key={product.id}><div><strong>{product.name}</strong><p>{money(product.price)} starting price × {cart[product.id]}</p></div><div className="quantity-controls"><button aria-label={`Decrease ${product.name} quantity`} onClick={() => setCart(current => changeQuantity(current, product.id, -1))}>−</button><span aria-label={`Quantity for ${product.name}`}>{cart[product.id]}</span><button disabled={cart[product.id] >= 99} aria-label={`Increase ${product.name} quantity`} onClick={() => setCart(current => changeQuantity(current, product.id, 1))}>+</button><button className="remove-item" onClick={() => setCart(current => { const updated = { ...current }; delete updated[product.id]; return updated; })}>Remove <span className="sr-only">{product.name}</span></button></div></li>)}</ul><p className="cart-total">Estimated subtotal <strong>{money(totals.price)}</strong></p><p className="cart-disclaimer">Excludes tax, shipping and final print specifications. Your cart is stored in this browser; uploaded artwork is not saved with it.</p><button className="light-button" onClick={() => { setCart({}); setCartNotice("Cart cleared."); }}>Clear cart</button></>}</dialog>
  </main>;
}
