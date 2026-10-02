"""Exercise extracted websocket helpers without importing or starting the bot."""
import ast
import asyncio
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any, AsyncIterator, Dict
import unittest

SOURCE = Path(__file__).resolve().parent.parent / "JEHOVAH2.0.py"


def helpers():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    names = {"_ws_subscribe", "_ws_stream", "_ws_extract", "_iter_ws_subscription", "_ws_unsubscribe",
             "_detect_ws_stream_label", "ws_self_test"}
    nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    context = dict(asyncio=asyncio, json=json, Any=Any, AsyncIterator=AsyncIterator, Dict=Dict,
                   _WSSubscription=SimpleNamespace, logger=logging.getLogger("websocket-test"))
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), "exec"), context)
    return context


class WebsocketTests(unittest.IsolatedAsyncioTestCase):
    async def test_raw_and_decoded_events_are_normalized_and_filtered(self):
        api = helpers()
        async def events():
            for message in [None, b"not-json", {"params": []}, {"subscription": "other", "result": "ignore"},
                            {"method": "eth_subscription", "params": {"subscription": "ours", "result": "nested"}},
                            {"subscription": "ours", "result": "decoded"},
                            b'{"params":{"subscription":"ours","result":"bytes"}}']:
                yield message
        aw3 = SimpleNamespace(socket=SimpleNamespace(process_subscriptions=events))
        iterator = api["_iter_ws_subscription"](aw3, SimpleNamespace(id="ours"))
        self.assertEqual([await anext(iterator) for _ in range(3)], ["nested", "decoded", "bytes"])
        with self.assertRaises(ConnectionError):
            await anext(iterator)

    async def test_silent_stream_times_out_and_is_closed(self):
        api = helpers()
        closed = []
        async def events():
            try:
                await asyncio.Event().wait()
                yield None
            finally:
                closed.append(True)
        aw3 = SimpleNamespace(ws=SimpleNamespace(listen_to_websocket=events))
        iterator = api["_iter_ws_subscription"](aw3, SimpleNamespace(id="ours"), timeout=0.02)
        with self.assertRaises(asyncio.TimeoutError):
            await anext(iterator)
        self.assertEqual(closed, [True])

    async def test_selftest_receives_header_and_unsubscribes(self):
        api = helpers()
        unsubscribed = []
        async def subscribe(method, params):
            self.assertEqual((method, params), ("newHeads", []))
            return "header-id"
        async def unsubscribe(sid):
            unsubscribed.append(sid)
        async def events():
            yield {"subscription": "header-id", "result": {"number": "0x12"}}
        socket = SimpleNamespace(process_subscriptions=events)
        aw3 = SimpleNamespace(socket=socket, provider=SimpleNamespace(socket=socket),
                              eth=SimpleNamespace(subscribe=subscribe, unsubscribe=unsubscribe))
        with self.assertLogs("websocket-test", level="INFO") as logs:
            await api["ws_self_test"](aw3, timeout_seconds=0.1)
        self.assertTrue(any("self-test OK" in message for message in logs.output))
        self.assertEqual(unsubscribed, ["header-id"])

    def test_helpers_have_one_definition_before_the_main_entry_point(self):
        tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
        iterators = [node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "_iter_ws_subscription"]
        self.assertEqual(len(iterators), 1)
        self.assertIsInstance(tree.body[-1], ast.If)
        self.assertIn("__name__", ast.unparse(tree.body[-1].test))


if __name__ == "__main__":
    unittest.main()
