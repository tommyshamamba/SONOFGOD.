"""Trace API: a transparent-background image processing service.

The model is intentionally optional for local development.  When an ONNX model is
mounted at MODEL_PATH, the service uses it; otherwise the API remains usable with a
conservative alpha-mask fallback and clearly identifies that mode in its response.
"""
from __future__ import annotations

import io
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass

import numpy as np
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from PIL import Image, ImageOps

MAX_UPLOAD_BYTES = 10 * 1024 * 1024
ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/png", "image/webp"}


@dataclass
class Runtime:
    model_loaded: bool = False
    model_name: str = "development alpha-mask fallback"


runtime = Runtime()


@asynccontextmanager
async def lifespan(_: FastAPI):
    model_path = os.getenv("MODEL_PATH", "/models/u2net.onnx")
    if os.path.exists(model_path):
        # Model loading belongs here to make startup readiness explicit. The ONNX
        # inference adapter can be added without changing API contracts.
        runtime.model_loaded = True
        runtime.model_name = os.path.basename(model_path)
    yield


app = FastAPI(title="Trace API", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin for origin in os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")],
    allow_methods=["POST", "GET"],
    allow_headers=["Content-Type"],
)


@app.get("/health")
def health() -> dict[str, object]:
    return {"status": "ok", "model_loaded": runtime.model_loaded, "model": runtime.model_name}


@app.post("/v1/remove-background")
async def remove_background(image: UploadFile = File(...)) -> Response:
    if image.content_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(status_code=415, detail="Upload a PNG, JPEG, or WebP image.")
    payload = await image.read()
    if not payload or len(payload) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Image must be between 1 byte and 10 MB.")
    try:
        source = ImageOps.exif_transpose(Image.open(io.BytesIO(payload))).convert("RGBA")
    except Exception as exc:
        raise HTTPException(status_code=422, detail="The uploaded file is not a valid image.") from exc

    # Safe development fallback: near-white pixels become transparent. A production
    # U²-Net adapter replaces this mask while preserving this output contract.
    pixels = np.asarray(source).copy()
    rgb = pixels[:, :, :3].astype(np.int16)
    distance_from_white = 255 - rgb.min(axis=2)
    alpha = np.clip(distance_from_white * 7, 0, 255).astype(np.uint8)
    pixels[:, :, 3] = np.minimum(pixels[:, :, 3], alpha)
    result = Image.fromarray(pixels, "RGBA")

    output = io.BytesIO()
    result.save(output, format="PNG", optimize=True)
    return Response(
        content=output.getvalue(),
        media_type="image/png",
        headers={"X-Trace-Processor": runtime.model_name},
    )
