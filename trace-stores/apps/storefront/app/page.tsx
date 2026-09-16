"use client";
import { ChangeEvent, useState } from "react";

const products = [
  ["Die Cut Stickers", "From $19", "✦"],
  ["Clear Stickers", "From $24", "◌"],
  ["Custom Labels", "From $28", "▣"],
];

export default function Home() {
  const [fileName, setFileName] = useState("Drop artwork here or browse files");
  const [processing, setProcessing] = useState(false);
  const [complete, setComplete] = useState(false);
  const chooseFile = (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;
    setFileName(file.name); setProcessing(true); setComplete(false);
    window.setTimeout(() => { setProcessing(false); setComplete(true); }, 900);
  };
  return <main>
    <nav><a className="brand" href="#top">trace<span>&</span>store</a><div className="links"><a href="#products">Products</a><a href="#how">How it works</a><button className="cart">Cart <b>0</b></button></div></nav>
    <section className="hero" id="top"><p className="eyebrow">AI-POWERED CUSTOM PRINTING</p><h1>From rough idea<br/>to <i>ready-to-print.</i></h1><p className="lede">Remove backgrounds, refine your artwork, and turn it into beautiful custom products—all in one place.</p><a className="button" href="#create">Start creating <span>→</span></a><div className="hero-art"><div className="burst">MAKE<br/>IT<br/>STICK.</div><div className="sticker one">WILD<br/><small>IDEAS</small></div><div className="sticker two">✿</div><div className="sticker three">HELLO!</div></div></section>
    <section className="creator" id="create"><div><p className="eyebrow">THE CREATOR</p><h2>Your artwork,<br/><i>cleaned up.</i></h2><p>Upload an image and our background-removal API prepares a transparent PNG for your product preview.</p><p className="note">Demo mode uses a local preview. Connect the Trace API for live processing.</p></div><label className={complete ? "drop complete" : "drop"}><input type="file" accept="image/png,image/jpeg,image/webp" onChange={chooseFile}/><span className="upload-icon">↑</span><strong>{processing ? "Removing background…" : complete ? "Background removed — ready to use" : fileName}</strong><small>PNG, JPG, or WebP · maximum 10 MB</small>{complete && <span className="done">✓</span>}</label></section>
    <section className="products" id="products"><p className="eyebrow">PICK YOUR CANVAS</p><h2>Made for your<br/><i>big ideas.</i></h2><div className="grid">{products.map(([name, price, mark]) => <article key={name}><div className="product-art">{mark}</div><h3>{name}</h3><p>{price}</p><button>Add to cart <span>→</span></button></article>)}</div></section>
    <section className="how" id="how"><p className="eyebrow">SIMPLE BY DESIGN</p><div><h2>Create in three<br/><i>good steps.</i></h2><ol><li><b>01</b><span><strong>Upload your artwork</strong><em>Start with any image, sketch, or logo.</em></span></li><li><b>02</b><span><strong>Remove the background</strong><em>Use AI to create a clean, print-ready asset.</em></span></li><li><b>03</b><span><strong>Choose a product</strong><em>Preview it, check out, and make it real.</em></span></li></ol></div></section>
    <footer><span>© 2026 Trace & Store</span><span>Built to turn ideas into objects.</span></footer>
  </main>;
}
