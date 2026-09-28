"""Download the upstream U2NetP model and verify its published checksum."""
from pathlib import Path
import hashlib
import urllib.request

URL = "https://github.com/danielgatis/rembg/releases/download/v0.0.0/u2netp.onnx"
# Published by rembg/sessions/u2netp.py; integrity check, not a signature.
MD5 = "8e83ca70e441ab06c318d82300c84806"


def main():
    destination = Path(__file__).resolve().parents[1] / "models" / "u2netp.onnx"
    destination.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(URL, timeout=120) as response:
        data = response.read(10 * 1024 * 1024 + 1)
    if hashlib.md5(data, usedforsecurity=False).hexdigest() != MD5:
        raise RuntimeError("Model checksum mismatch; destination left unchanged")
    destination.write_bytes(data)
    print(f"Verified model: {destination}")


if __name__ == "__main__":
    main()
