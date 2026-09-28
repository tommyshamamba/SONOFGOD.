import io
import os

import numpy as np
import onnx
import pytest
from fastapi.testclient import TestClient
from onnx import TensorProto, helper
from PIL import Image

from app.main import app


@pytest.mark.skipif(not os.getenv("TEST_PRETRAINED_MODEL"), reason="Optional downloaded model")
def test_pretrained_model_end_to_end(monkeypatch):
    monkeypatch.setenv("MODEL_PATH", os.environ["TEST_PRETRAINED_MODEL"])
    monkeypatch.setenv("REQUIRE_MODEL", "true")
    with TestClient(app) as client:
        assert client.get("/ready").json()["mode"] == "onnx"
        response = upload(client, Image.new("RGB", (80, 60), "blue"))
        assert response.status_code == 200
        result = Image.open(io.BytesIO(response.content))
        assert result.mode == "RGBA"
        assert result.size == (80, 60)


@pytest.fixture
def model(tmp_path, monkeypatch):
    # A real, tiny executable ONNX graph with the U2-Net tensor contract.
    # Tests the adapter, not pretrained segmentation quality.
    graph = helper.make_graph([
        helper.make_node("ReduceMean", ["image"], ["mean"], axes=[1], keepdims=1),
        helper.make_node("Sigmoid", ["mean"], ["mask"]),
    ], "test-mask", [helper.make_tensor_value_info("image", TensorProto.FLOAT, [1, 3, 32, 32])],
       [helper.make_tensor_value_info("mask", TensorProto.FLOAT, [1, 1, 32, 32])])
    path = tmp_path / "test-mask.onnx"
    value = helper.make_model(graph, opset_imports=[helper.make_opsetid("", 13)])
    value.ir_version = 9
    onnx.save(value, path)
    monkeypatch.setenv("MODEL_PATH", str(path))
    monkeypatch.setenv("REQUIRE_MODEL", "true")
    return path


def upload(client, image):
    stream = io.BytesIO()
    image.save(stream, format="PNG")
    return client.post("/v1/remove-background", files={"image": ("sample.png", stream.getvalue(), "image/png")})


def test_real_onnx_inference_preserves_size_and_transparency(model):
    pixels = np.zeros((20, 40, 4), dtype=np.uint8)
    pixels[:, :20, :3] = 255
    pixels[:, :, 3] = 180
    pixels[0, 0, 3] = 0
    with TestClient(app) as client:
        health = client.get("/ready").json()
        assert health["model_loaded"] is True
        assert health["mode"] == "onnx"
        response = upload(client, Image.fromarray(pixels))
        assert response.status_code == 200
        assert response.headers["x-trace-processor"] == model.name
        result = Image.open(io.BytesIO(response.content))
        assert result.size == (40, 20)
        alpha = np.asarray(result)[:, :, 3]
        assert alpha[0, 0] == 0
        assert alpha.max() <= 180
        assert alpha[10, 5] > alpha[10, 35]  # Opposite of the near-white fallback.


def test_constant_black_input_does_not_produce_nan(model):
    with TestClient(app) as client:
        assert upload(client, Image.new("RGB", (12, 12), "black")).status_code == 200


def test_missing_required_model_fails_startup(tmp_path, monkeypatch):
    monkeypatch.setenv("MODEL_PATH", str(tmp_path / "missing.onnx"))
    monkeypatch.setenv("REQUIRE_MODEL", "true")
    with pytest.raises(RuntimeError, match="not found"):
        with TestClient(app):
            pass


def test_corrupt_optional_model_reports_fallback(tmp_path, monkeypatch):
    path = tmp_path / "corrupt.onnx"
    path.write_bytes(b"not an ONNX model")
    monkeypatch.setenv("MODEL_PATH", str(path))
    monkeypatch.setenv("REQUIRE_MODEL", "false")
    with TestClient(app) as client:
        health = client.get("/health").json()
        assert health["model_loaded"] is False
        assert health["mode"] == "fallback"
        assert health["model_error"]


def test_invalid_upload_and_content_type(model):
    with TestClient(app) as client:
        response = client.post("/v1/remove-background", files={"image": ("x.png", b"broken", "image/png")})
        assert response.status_code == 422
        response = client.post("/v1/remove-background", files={"image": ("x.txt", b"text", "text/plain")})
        assert response.status_code == 415


def test_inference_failure_is_not_silently_fallback(model, monkeypatch):
    from app.main import runtime
    with TestClient(app) as client:
        def fail(_):
            raise RuntimeError("test failure")
        monkeypatch.setattr(runtime.model, "mask", fail)
        assert upload(client, Image.new("RGB", (12, 12))).status_code == 503
