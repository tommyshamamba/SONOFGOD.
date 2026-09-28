"""Local-only missed-call simulator. No calls, SMS, CRM or provider requests."""
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import argparse
import json
import re
import sqlite3
from contextlib import contextmanager

ROOT = Path(__file__).resolve().parent
MISSED = {"no-answer", "busy", "failed"}


class Store:
    def __init__(self, path):
        self.path = str(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute("CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, status TEXT NOT NULL, contact TEXT NOT NULL, outcome TEXT NOT NULL, message TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)")

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        try:
            with db:
                yield db
        finally:
            db.close()

    def simulate(self, payload):
        if not isinstance(payload, dict):
            raise ValueError("Expected a JSON object.")
        event_id = payload.get("id", "")
        status = payload.get("status", "")
        contact = payload.get("contact", "Demo caller")
        if not isinstance(event_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", event_id):
            raise ValueError("Use an event ID containing 1–80 letters, numbers, dashes or underscores.")
        if not isinstance(status, str) or status not in MISSED | {"completed"}:
            raise ValueError("Unsupported call status.")
        if not isinstance(contact, str) or not 1 <= len(contact.strip()) <= 80:
            raise ValueError("Use a short demo contact name.")
        outcome = "drafted" if status in MISSED else "ignored"
        message = "Sorry we missed your call. Tell us what you need and a team member will help arrange a callback." if outcome == "drafted" else ""
        with self.connect() as db:
            inserted = db.execute("INSERT OR IGNORE INTO events (id,status,contact,outcome,message) VALUES (?,?,?,?,?)", (event_id, status, contact.strip(), outcome, message)).rowcount
            db.row_factory = sqlite3.Row
            event = dict(db.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone())
        if not inserted and (event["status"] != status or event["contact"] != contact.strip()):
            raise ValueError("This event ID already belongs to a different event.")
        return {"mode": "local-simulation", "duplicate": not bool(inserted), "event": event}

    def events(self):
        with self.connect() as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute("SELECT * FROM events ORDER BY rowid DESC LIMIT 100")]


def create_server(port, store):
    class Handler(BaseHTTPRequestHandler):
        def reply(self, status, payload, content_type="application/json"):
            body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/":
                self.reply(200, (ROOT / "index.html").read_bytes(), "text/html; charset=utf-8")
            elif self.path == "/api/events":
                self.reply(200, {"mode": "local-simulation", "events": store.events()})
            elif self.path == "/health":
                self.reply(200, {"status": "ok", "mode": "local-simulation"})
            else:
                self.reply(404, {"error": "Not found"})

        def do_POST(self):
            if self.path != "/api/simulate":
                return self.reply(404, {"error": "Not found"})
            expected_origins = {f"http://127.0.0.1:{self.server.server_port}", f"http://localhost:{self.server.server_port}"}
            if self.headers.get("Origin") and self.headers["Origin"] not in expected_origins:
                return self.reply(403, {"error": "Use the local demo page."})
            if self.headers.get_content_type() != "application/json":
                return self.reply(415, {"error": "Expected application/json."})
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 4096:
                    return self.reply(413, {"error": "Request must be 1–4096 bytes."})
                result = store.simulate(json.loads(self.rfile.read(size)))
                self.reply(200, result)
            except (ValueError, UnicodeDecodeError) as error:
                self.reply(400, {"error": str(error)})
            except sqlite3.Error:
                self.reply(503, {"error": "Local storage unavailable. Retry later."})

        def log_message(self, *_):
            pass

    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--database", type=Path, default=ROOT / "data" / "demo.sqlite")
    args = parser.parse_args()
    server = create_server(args.port, Store(args.database))
    print(f"Local simulation: http://127.0.0.1:{server.server_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
