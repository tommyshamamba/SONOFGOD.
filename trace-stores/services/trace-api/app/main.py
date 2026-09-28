"""Trace API: a transparent-background image processing service.

The model is intentionally optional for local development.  When an ONNX model is
mounted at MODEL_PATH, the service uses it; otherwise the API remains usable with a
conservative alpha-mask fallback and clearly identifies that mode in its response.
"""
from __future__ import annotations

import io
import os
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from threading import Lock

import numpy as np
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from PIL import Image, ImageOps
from starlette.concurrency import run_in_threadpool
from .inference import U2Net

MAX_UPLOAD_BYTES = 10 * 1024 * 1024
MAX_IMAGE_PIXELS = 16_000_000
ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/png", "image/webp"}


@dataclass
class Runtime:
    model_loaded: bool = False
    model_name: str = "development alpha-mask fallback"
    model: U2Net | None = None
    required: bool = False
    error: str | None = None


runtime = Runtime()
inference_lock = Lock()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    runtime.model = None
    runtime.model_loaded = False
    runtime.model_name = "development alpha-mask fallback"
    runtime.error = None
    runtime.required = os.getenv("REQUIRE_MODEL", "false").lower() == "true"
    model_path = os.getenv("MODEL_PATH", "/models/u2net.onnx")
    if os.path.exists(model_path):
        try:
            runtime.model = await run_in_threadpool(U2Net, model_path)
            runtime.model_loaded = True
            runtime.model_name = runtime.model.name
        except Exception:
            runtime.error = "Model failed validation; inspect server logs."
            logger.exception("ONNX model initialization failed")
    else:
        runtime.error = "Model file not found."
    if runtime.required and not runtime.model_loaded:
        raise RuntimeError(runtime.error)
    try:
        yield
    finally:
        runtime.model = None
        runtime.model_loaded = False


app = FastAPI(title="Trace API", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin for origin in os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")],
    allow_methods=["POST", "GET"],
    allow_headers=["Content-Type"],
)


@app.get("/health")
def health() -> dict[str, object]:
    return {"status": "ok", "model_loaded": runtime.model_loaded, "model": runtime.model_name,
            "mode": "onnx" if runtime.model_loaded else "fallback", "model_error": runtime.error}


@app.get("/ready")
def ready() -> dict[str, object]:
    if runtime.required and not runtime.model_loaded:
        raise HTTPException(status_code=503, detail="Model unavailable")
    return health()


@app.post("/v1/remove-background")
async def remove_background(image: UploadFile = File(...)) -> Response:
    if image.content_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(status_code=415, detail="Upload a PNG, JPEG, or WebP image.")
    payload = await image.read(MAX_UPLOAD_BYTES + 1)
    if not payload or len(payload) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Image must be between 1 byte and 10 MB.")
    try:
        opened = Image.open(io.BytesIO(payload))
        if opened.width * opened.height > MAX_IMAGE_PIXELS:
            raise HTTPException(status_code=413, detail="Image exceeds 16 megapixels.")
        source = ImageOps.exif_transpose(opened).convert("RGBA")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=422, detail="The uploaded file is not a valid image.") from exc

    try:
        result = await run_in_threadpool(process_image, source)
    except Exception as exc:
        logger.exception("Image inference failed")
        raise HTTPException(status_code=503, detail="Image processing failed; retry later.") from exc

    output = io.BytesIO()
    result.save(output, format="PNG", optimize=True)
    return Response(content=output.getvalue(), media_type="image/png",
                    headers={"X-Trace-Processor": runtime.model_name})


def process_image(source: Image.Image) -> Image.Image:
    pixels = np.asarray(source).copy()
    if runtime.model is not None:
        with inference_lock:
            alpha = np.asarray(runtime.model.mask(source))
    else:
        if runtime.required:
            raise RuntimeError("Required model is unavailable")
        rgb = pixels[:, :, :3].astype(np.int16)
        alpha = np.clip((255 - rgb.min(axis=2)) * 7, 0, 255).astype(np.uint8)
    pixels[:, :, 3] = np.minimum(pixels[:, :, 3], alpha)
    return Image.fromarray(pixels, "RGBA")
