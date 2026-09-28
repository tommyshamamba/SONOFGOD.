"""CPU inference adapter for U2-Net/U2-NetP NCHW float32 ONNX exports."""
from pathlib import Path

import numpy as np
import onnxruntime as ort
from PIL import Image


class U2Net:
    def __init__(self, path: str):
        options = ort.SessionOptions()
        options.intra_op_num_threads = 1
        self.session = ort.InferenceSession(path, sess_options=options, providers=["CPUExecutionProvider"])
        inputs = self.session.get_inputs()
        if len(inputs) != 1 or inputs[0].type != "tensor(float)":
            raise ValueError("Expected a single float32 U2-Net input")
        shape = inputs[0].shape
        if len(shape) != 4 or shape[0] not in (1, None, "batch_size") or shape[1] != 3:
            raise ValueError("Expected a U2-Net input with shape [1, 3, H, W]")
        self.height = shape[2] if isinstance(shape[2], int) else 320
        self.width = shape[3] if isinstance(shape[3], int) else 320
        if not (1 <= self.height <= 2048 and 1 <= self.width <= 2048):
            raise ValueError("Unsupported model input dimensions")
        self.input_name = inputs[0].name
        self.name = Path(path).name
        # Warmup checks the executable graph and output contract before readiness.
        self.mask(Image.new("RGB", (self.width, self.height)))

    def mask(self, source: Image.Image) -> Image.Image:
        resized = source.convert("RGB").resize((self.width, self.height), Image.Resampling.LANCZOS)
        pixels = np.asarray(resized, dtype=np.float32)
        pixels /= max(float(pixels.max()), 1e-6)
        pixels = (pixels - np.array([0.485, 0.456, 0.406], dtype=np.float32)) / np.array([0.229, 0.224, 0.225], dtype=np.float32)
        tensor = np.ascontiguousarray(pixels.transpose(2, 0, 1)[None], dtype=np.float32)
        prediction = np.asarray(self.session.run(None, {self.input_name: tensor})[0])
        if prediction.ndim != 4 or prediction.shape[:2] != (1, 1) or not np.isfinite(prediction).all():
            raise ValueError("Expected finite U2-Net output [1, 1, H, W]")
        mask = prediction[0, 0]
        low, high = float(mask.min()), float(mask.max())
        mask = (mask - low) / (high - low) if high > low else np.clip(mask, 0, 1)
        return Image.fromarray((np.clip(mask, 0, 1) * 255).astype(np.uint8)).resize(source.size, Image.Resampling.LANCZOS)
