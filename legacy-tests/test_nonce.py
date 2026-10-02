"""Offline regression tests. Never import either networked bot module."""
import ast
import asyncio
import importlib.util
import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Tuple, Optional
import unittest

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("legacy_nonce", ROOT / "legacy_nonce.py")
nonce_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nonce_module)
NonceMgr = nonce_module.NonceMgr


def isolated_function(filename, function, context):
    tree = ast.parse((ROOT / filename).read_text(encoding="utf-8"))
    node = next(n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == function)
    module = ast.Module(body=[node], type_ignores=[])
    exec(compile(module, filename, "exec"), context)
    return context[function]


class FakeEth:
    def __init__(self):
        self.pending = 7
        self.sent = []
        self.queries = []
        self.account = SimpleNamespace(sign_transaction=self.sign)

    def get_transaction_count(self, addr, tag):
        self.queries.append(tag)
        return self.pending

    def sign(self, tx, key):
        return SimpleNamespace(raw_transaction=tx["nonce"].to_bytes(4, "big"))

    def send_raw_transaction(self, raw):
        self.sent.append(int.from_bytes(raw, "big"))
        return raw


class NonceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.eth = FakeEth()
        self.mgr = NonceMgr(SimpleNamespace(eth=self.eth), "local-test-account")

    async def prepare(self, nonce):
        return nonce

    async def broadcast(self, nonce):
        await asyncio.sleep(0)
        self.eth.sent.append(nonce)
        return f"hash-{nonce}"

    async def test_discarded_estimates_do_not_consume_nonces(self):
        self.assertEqual(await asyncio.gather(*(self.mgr.preview() for _ in range(100))), [7] * 100)
        self.assertEqual(await self.mgr.submit(self.prepare, self.broadcast), "hash-7")
        self.assertEqual(self.eth.sent, [7])
        self.assertEqual(set(self.eth.queries), {"pending"})

    async def test_concurrent_submissions_are_contiguous_and_unique(self):
        await asyncio.gather(*(self.mgr.submit(self.prepare, self.broadcast) for _ in range(20)))
        self.assertEqual(self.eth.sent, list(range(7, 27)))

    async def test_signing_failure_leaves_nonce_available(self):
        async def fail(_):
            raise ValueError("cannot sign")
        with self.assertRaises(ValueError):
            await self.mgr.submit(fail, self.broadcast)
        self.assertFalse(self.mgr.uncertain)
        self.assertEqual(await self.mgr.submit(self.prepare, self.broadcast), "hash-7")

    async def test_ambiguous_broadcast_stops_nonce_reuse_and_further_sends(self):
        async def uncertain(nonce):
            self.eth.sent.append(nonce)
            raise TimeoutError("accepted, response lost")
        with self.assertRaises(TimeoutError):
            await self.mgr.submit(self.prepare, uncertain)
        with self.assertRaises(nonce_module.NonceUncertain):
            await self.mgr.submit(self.prepare, self.broadcast)
        self.assertEqual(self.eth.sent, [7])

    async def test_cancellation_during_broadcast_is_uncertain(self):
        async def cancelled(_):
            raise asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await self.mgr.submit(self.prepare, cancelled)
        self.assertTrue(self.mgr.uncertain)

    async def test_missing_hash_blocks_further_submissions(self):
        async def empty(_):
            return None
        with self.assertRaises(nonce_module.NonceUncertain):
            await self.mgr.submit(self.prepare, empty)
        with self.assertRaises(nonce_module.NonceUncertain):
            await self.mgr.submit(self.prepare, self.broadcast)
        self.assertFalse(self.eth.sent)

    async def test_external_pending_nonce_can_advance_but_not_regress(self):
        await self.mgr.submit(self.prepare, self.broadcast)
        self.eth.pending = 20
        await self.mgr.submit(self.prepare, self.broadcast)
        self.eth.pending = 7
        await self.mgr.submit(self.prepare, self.broadcast)
        self.assertEqual(self.eth.sent, [7, 20, 21])

    async def test_pending_lookup_failure_does_not_fallback_to_latest(self):
        def fail(addr, tag):
            self.assertEqual(tag, "pending")
            raise ConnectionError("offline")
        self.eth.get_transaction_count = fail
        with self.assertRaises(ConnectionError):
            await self.mgr.submit(self.prepare, self.broadcast)
        self.assertFalse(self.eth.sent)

    async def test_both_bots_assign_nonce_at_submission_and_support_current_signed_tx_field(self):
        for filename in ("SONOFGOD.py", "JEHOVAH2.0.py"):
            with self.subTest(bot=filename):
                eth = FakeEth()
                w3 = SimpleNamespace(eth=eth)
                context = dict(Dict=Dict, Any=Any, Tuple=Tuple, Optional=Optional, asyncio=asyncio,
                               ACCOUNT=True, PRIVATE_KEY="synthetic-test-only", USE_FLASHBOTS=False, ALLOW_LIVE_TRANSACTIONS=True,
                               NONCE=NonceMgr(w3, "local-test-account"), w3=w3, logger=logging.getLogger("test"))
                send = isolated_function(filename, "send_tx_async", context)
                tx = {"nonce": 999, "gas": 21000}
                results = await asyncio.gather(*(send(tx) for _ in range(4)))
                self.assertTrue(all(ok for ok, _ in results))
                self.assertEqual(eth.sent, [7, 8, 9, 10])
                self.assertEqual(tx["nonce"], 999)

    async def test_both_bots_block_signing_and_submission_without_explicit_opt_in(self):
        for filename in ("SONOFGOD.py", "JEHOVAH2.0.py"):
            with self.subTest(bot=filename):
                context = dict(Dict=Dict, Any=Any, Tuple=Tuple, Optional=Optional,
                               ALLOW_LIVE_TRANSACTIONS=False, logger=logging.getLogger("test"))
                send = isolated_function(filename, "send_tx_async", context)
                # No account, signer or RPC is provided: the gate must exit before
                # resolving any of them, even if a transaction was already built.
                self.assertEqual(await send({"nonce": 1}), (False, None))

    async def test_both_bots_encode_private_relay_bytes_and_support_legacy_signed_field(self):
        for filename in ("SONOFGOD.py", "JEHOVAH2.0.py"):
            with self.subTest(bot=filename):
                eth = FakeEth()
                eth.block_number = 100
                eth.account.sign_transaction = lambda tx, key: SimpleNamespace(rawTransaction=b"\x12\x34")
                sent = []
                def post(url, json, timeout):
                    sent.append(json)
                    return SimpleNamespace(json=lambda: {"result": "0x" + "ab" * 32})
                w3 = SimpleNamespace(eth=eth)
                context = dict(Dict=Dict, Any=Any, Tuple=Tuple, Optional=Optional, asyncio=asyncio,
                               ACCOUNT=True, PRIVATE_KEY="synthetic-test-only", USE_FLASHBOTS=True, ALLOW_LIVE_TRANSACTIONS=True,
                               FLASHBOTS_RPC="https://synthetic.invalid", requests=SimpleNamespace(post=post),
                               NONCE=NonceMgr(w3, "local-test-account"), w3=w3, logger=logging.getLogger("test"))
                send = isolated_function(filename, "send_tx_async", context)
                self.assertEqual(await send({"gas": 21000}), (True, "0x" + "ab" * 32))
                self.assertEqual(sent[0]["params"], [{"tx": "0x1234", "maxBlockNumber": "0x69"}])
                self.assertEqual(eth.sent, [])

    async def test_both_bots_wrap_dodo_quote_loans_with_actual_pool_base(self):
        for filename in ("SONOFGOD.py", "JEHOVAH2.0.py"):
            with self.subTest(bot=filename):
                class Functions:
                    def _BASE_TOKEN_(self):
                        return SimpleNamespace(call=lambda: "base")
                    def _QUOTE_TOKEN_(self):
                        return SimpleNamespace(call=lambda: "quote")
                encoded = []
                def encode(types, values):
                    encoded.append((types, values))
                    return b"wrapped"
                context = dict(_cs=str, w3=SimpleNamespace(eth=SimpleNamespace(contract=lambda **_: SimpleNamespace(functions=Functions()))),
                               os=SimpleNamespace(getenv=lambda *_: "pool"), erc20_balance=lambda *_: 1000, abi_encode=encode)
                wrap = isolated_function(filename, "encode_flashloan_params", context)
                self.assertEqual(wrap("DODO", "quote", 1000, b"inner"), b"wrapped")
                self.assertEqual(encoded, [(["address", "address", "bytes"], ["pool", "base", b"inner"])])
                self.assertEqual(wrap("AAVE", "quote", 1000, b"inner"), b"inner")
                with self.assertRaises(ValueError):
                    wrap("DODO", "quote", 1001, b"inner")


if __name__ == "__main__":
    unittest.main()
