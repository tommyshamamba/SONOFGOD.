"""Dependency-free source checks. Run from any directory with Python 3.12+.

Validates manifests and Python syntax; checks server/browser JavaScript with Node.
JSX/TypeScript need their project build and are deliberately not checked here.
Does not start services, execute bots or contact external systems.
"""
from pathlib import Path
import argparse
import json
import os
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
SKIP = {"node_modules", ".git", ".next", ".terraform", ".test-data", "build", "dist", ".venv", "__pycache__"}


def source_files(base):
    for directory, folders, files in os.walk(base):
        folders[:] = sorted(name for name in folders if name not in SKIP and not name.startswith(("pytest-cache-files-", ".pytest_cache")))
        for name in sorted(files):
            yield Path(directory) / name


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node", default=shutil.which("node"), help="Node executable")
    args = parser.parse_args()
    if not args.node:
        parser.error("Node is required. Install Node or provide --node PATH.")
    errors = []
    counts = {"json": 0, "python": 0, "javascript": 0}
    for base in [ROOT / "portfolio-projects", ROOT / "trace-stores", ROOT / "scripts", ROOT / "legacy-tests"]:
        for path in source_files(base):
            rel = path.relative_to(ROOT)
            if not path.is_file() or any(part in SKIP for part in rel.parts):
                continue
            try:
                if path.name in {"package.json", "package-lock.json"}:
                    json.loads(path.read_text(encoding="utf-8-sig"))
                    counts["json"] += 1
                elif path.suffix == ".py":
                    compile(path.read_bytes(), str(rel), "exec")
                    counts["python"] += 1
                elif path.suffix in {".js", ".mjs", ".cjs"} and not (path.suffix == ".js" and "frontend" in rel.parts and "src" in rel.parts):
                    result = subprocess.run([args.node, "--check", str(path)], capture_output=True, text=True, timeout=30)
                    if result.returncode:
                        raise ValueError(result.stderr.strip())
                    counts["javascript"] += 1
            except (ValueError, SyntaxError, OSError, subprocess.TimeoutExpired) as exc:
                errors.append(f"{rel}: {exc}")
    print(json.dumps({"checked": counts, "errors": errors}, indent=2))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
