"""Offline tests for the pure graph and gas modules, plus the engine's gas guard."""
import ast
import asyncio
import importlib.util
import logging
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict
import unittest

ROOT = Path(__file__).resolve().parent.parent / "onchain_engine"


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


graph = load("graph")
gas = load("gas")
D = Decimal


def g(edges):
    out = {}
    for u, v, r in edges:
        out.setdefault(u, {})[v] = D(str(r))
    return out


PROFITABLE = g([("A", "B", 2.0), ("B", "C", 0.6), ("C", "A", 0.9),   # 1.08 cycle
                ("A", "C", 1.0), ("C", "B", 1.5), ("B", "A", 0.45)])   # 0.675 cycle
FAIR = g([("A", "B", 2.0), ("B", "A", 0.5), ("B", "C", 3.0), ("C", "B", 1 / 3), ("A", "C", 6.0), ("C", "A", 1 / 6)])


class GraphTests(unittest.TestCase):
    def test_bellman_ford_returns_closed_profitable_cycle(self):
        cycles = graph.bellman_ford_arbitrage(PROFITABLE)
        self.assertTrue(cycles, "a 1.08x cycle must be found")
        for c in cycles:
            self.assertEqual(c[0], c[-1])
            self.assertGreater(graph.cycle_rate(PROFITABLE, c), 1.0)
        self.assertEqual({frozenset(c) for c in cycles}, {frozenset("ABC")})

    def test_bellman_ford_no_false_positive_on_consistent_prices(self):
        self.assertEqual(graph.bellman_ford_arbitrage(FAIR), [])

    def test_bellman_ford_handles_dangling_nodes(self):
        gr = dict(PROFITABLE)
        gr["A"] = dict(gr["A"], Z=D(1))  # Z has no outgoing edges and no predecessor chain issues
        self.assertTrue(graph.bellman_ford_arbitrage(gr))

    def test_enumerators_agree_and_deduplicate(self):
        full = graph.enumerate_cycles(PROFITABLE, 4)
        keys = [tuple(sorted(c[:-1])) for c in full]
        self.assertEqual(len(full), 2)  # A->B->C->A and A->C->B->A, each once
        self.assertEqual(len(set(map(tuple, full))), len(full))
        bounded = graph.bounded_simple_cycles(PROFITABLE, 4)
        self.assertTrue(set(map(tuple, bounded)) <= set(map(tuple, full)) or len(bounded) <= len(full))
        self.assertEqual(set(keys), {("A", "B", "C")})

    def test_blacklist_respected(self):
        self.assertEqual(graph.enumerate_cycles(PROFITABLE, 4, {"B"}), [])
        self.assertEqual(graph.bounded_simple_cycles(PROFITABLE, 4, {"B"}), [])


class GasTests(unittest.TestCase):
    def test_cost_math(self):
        # 1,000,000 gas at 0.01 gwei = 0.00001 ETH; at $3000 and margin 1.0 = $0.03
        self.assertEqual(gas.gas_cost_usd(1_000_000, 10_000_000, D(3000), 1.0), D("0.03"))

    def test_missing_inputs_are_errors_not_zero(self):
        for args in [(1, 0, D(3000)), (1, 10, None), (1, 10, D(0)), (0, 10, D(3000))]:
            with self.subTest(args=args), self.assertRaises(gas.GasCostUnavailable):
                gas.gas_cost_usd(*args)


def isolated(function, context):
    path = ROOT / "engine.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    node = next(n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == function)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), context)
    return context[function]


class EngineGasGuardTests(unittest.IsolatedAsyncioTestCase):
    def context(self, oracle, estimate_ok=True):
        calls = []
        def estimate_gas(tx):
            calls.append("estimate")
            if not estimate_ok:
                raise ValueError("revert")
            return 500_000
        async def predict(fast=False):
            return 10_000_000
        ctx = dict(Dict=Dict, Any=Any, Decimal=Decimal, asyncio=asyncio, logger=logging.getLogger("t"), TX_GAS_LIMIT=2_400_000,
                   w3=SimpleNamespace(eth=SimpleNamespace(estimate_gas=estimate_gas)), predict_gas_price_wei=predict,
                   get_oracle_price=lambda sym: oracle, _gas_cost_usd=gas.gas_cost_usd)
        return ctx, calls

    async def test_missing_eth_price_raises_instead_of_free_gas(self):
        ctx, _ = self.context(oracle=None)
        cost = isolated("usd_cost_for_tx", ctx)
        with self.assertRaises(gas.GasCostUnavailable):
            await cost({"to": "0x1", "data": "0x"})

    async def test_failed_estimate_uses_upper_bound(self):
        ctx, calls = self.context(oracle=D(2000), estimate_ok=False)
        cost = isolated("usd_cost_for_tx", ctx)
        got = await cost({"to": "0x1"}, margin=1.0)
        self.assertEqual(got, D(2_400_000) * D(10_000_000) / D(10**18) * D(2000))
        self.assertEqual(calls, ["estimate"])

    def test_engine_has_no_blocking_gas_calls_in_async_paths(self):
        src = (ROOT / "engine.py").read_text(encoding="utf-8")
        self.assertNotIn("_usd_cost_for_tx", src)
        self.assertNotIn("w3.eth.estimate_gas(", src)  # only via asyncio.to_thread


if __name__ == "__main__":
    unittest.main()
