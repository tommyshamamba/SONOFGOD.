import json
from pathlib import Path
import shutil
import uuid
import threading
import unittest
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from server import Store, create_server


class DemoTests(unittest.TestCase):
    def setUp(self):
        self.temp = Path(__file__).resolve().parent / "data" / ("test-" + uuid.uuid4().hex)
        self.temp.mkdir(parents=True)
        self.path = self.temp / "test.sqlite"
        self.store = Store(self.path)

    def tearDown(self):
        assert self.temp.resolve().parent == Path(__file__).resolve().parent / "data"
        shutil.rmtree(self.temp)

    def test_missed_call_persists_and_completed_call_is_ignored(self):
        self.assertEqual(self.store.simulate({"id": "a", "status": "busy"})["event"]["outcome"], "drafted")
        self.assertEqual(self.store.simulate({"id": "b", "status": "completed"})["event"]["message"], "")
        self.assertEqual(len(Store(self.path).events()), 2)

    def test_concurrent_replays_create_one_draft(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: self.store.simulate({"id": "same", "status": "no-answer"}), range(12)))
        self.assertEqual(sum(not result["duplicate"] for result in results), 1)
        self.assertEqual(len(self.store.events()), 1)

    def test_validation_and_conflicting_replay(self):
        for invalid in [[], {}, {"id": "x", "status": "unknown"}]:
            with self.assertRaises(ValueError): self.store.simulate(invalid)
        self.store.simulate({"id": "x", "status": "busy"})
        with self.assertRaises(ValueError): self.store.simulate({"id": "x", "status": "completed"})

    def test_http_demo_and_cross_origin_rejection(self):
        server = create_server(0, self.store)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{server.server_port}"
        try:
            with urllib.request.urlopen(base + "/") as response:
                self.assertIn(b"Local simulation", response.read())
            request = urllib.request.Request(base + "/api/simulate", data=json.dumps({"id": "http", "status": "failed"}).encode(), headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request) as response:
                self.assertEqual(json.load(response)["event"]["outcome"], "drafted")
            request.add_header("Origin", "https://untrusted.example")
            with self.assertRaises(urllib.error.HTTPError) as error: urllib.request.urlopen(request)
            self.assertEqual(error.exception.code, 403)
        finally:
            server.shutdown(); server.server_close(); thread.join()


if __name__ == "__main__":
    unittest.main()
