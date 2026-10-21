# On-chain Execution Engine: security and design case study

This is a research prototype of a flash-loan executor contract and an async Python engine for an EVM chain (Arbitrum). **It has never been deployed or run against a live network, and it has made no money.** Live sending stays off unless `ALLOW_LIVE_TRANSACTIONS=true` *and* a key is configured. No key is in this repository.

This folder is kept as a case study for two reasons: a contract design that's worth reading, and an honest list of why the strategy around it wouldn't work. The reusable infrastructure idea in it (upstream failover and supervision) was rebuilt properly in Go as [rpc-failover-proxy](https://github.com/tommyshamamba/rpc-failover-proxy).

## Layout
| Path | What it is |
|---|---|
| `FlashExecutor.sol` | UUPS-upgradeable, pausable, owner-gated executor. It takes flash loans from Aave V3, Balancer V2, Uniswap V3 and DODO |
| `ERC1967Proxy.sol` | Standard proxy, used by the tests |
| `onchain_engine/graph.py` | Price-graph cycle search (pure functions) |
| `onchain_engine/gas.py` | Gas-cost arithmetic that refuses to treat an unknown price as free |
| `onchain_engine/nonce.py` | Serialized nonce manager that halts when a broadcast outcome is unknown |
| `onchain_engine/ws_compat.py` | WebSocket subscription helpers for web3.py v6 and v7 |
| `onchain_engine/config.py` | Environment configuration |
| `onchain_engine/engine.py` | Runtime: RPC failover, quoting, strategy loop, supervisor. Connects to RPC on import |
| `tests/` | 26 offline Python tests and 23 contract tests in an in-memory EVM |

Run it with `python -m onchain_engine`. Importing the package itself does not import `engine`.

## The part worth studying: callback binding in the contract
A flash-loan receiver is a public function that lenders call back. The classic bug is letting *anyone* call it with crafted data. Here:
- `requestFlashLoan` (owner only, `nonReentrant`, `whenNotPaused`) stores a one-time commitment before borrowing: lender, provider, asset, amount, the keccak hash of the callback data, and the pre-loan balance. See `FlashExecutor.sol` lines 174-222.
- Every lender callback goes through `_beginCallback` (lines 225-231). It requires the phase to be `AWAITING_CALLBACK` and checks `msg.sender`, provider, asset, amount and the data hash against the commitment before doing anything. Aave's `initiator` is also checked.
- The request keeps the reentrancy guard held for the whole loan. The callback uses the phase state machine instead of re-entering the guard. After the loan the contract checks that its idle reserves weren't consumed, and it resets Aave's approval to 0.
- Upgrades are proposed, timelocked for 1-14 days and checked for `proxiableUUID`. A guardian can pause and cancel upgrades, but cannot execute anything.

## Known limitations
These are listed so nobody mistakes this for a working trading system.
1. **Cross-exchange cycles can't be executed.** `_arb` runs the whole route on a single exchange (`FlashExecutor.sol` lines 312-319 and 358). The Python search finds cycles across Uniswap V3, Sushi and Camelot, and most of those can't be expressed in one request.
2. **The on-chain profit floor is 1 wei.** Gas isn't covered on-chain, so the off-chain estimate decides everything.
3. **The pricing model is naive.** Each edge is quoted once at a fixed size of about $5k, minus a flat slippage haircut. Real pools move with trade size, so "profitable" cycles mostly aren't. The graph is rebuilt from up to N² quoter calls, which is stale against 250 ms blocks.
4. **Competition makes it uneconomic.** On Arbitrum, ordering is first-come-first-served plus the Timeboost express lane, which a few professional firms dominate. Cyclic arbitrage is already saturated by on-chain probe bots. A Python bot on public RPC loses that race. Sources: arXiv 2509.22143, arXiv 2603.20175, arXiv 2506.01462, and Solmaz et al., AFT 2025.
5. **`engine.py` is still about 3,000 lines of script-style code.** It has a flat config namespace and many broad `except` blocks. Only the self-contained parts were extracted into modules.
6. Lenders, tokens and routers in the tests are local fixtures, not real protocols.

## Fixed in the October 2026 refactor
- **Removed duplication:** `arbitrage_engine.py` was about 99% a copy of `stream_engine.py` (3,162 of 3,193 lines). It's gone, and the single engine is now the `onchain_engine` package.
- **Gas cost could silently be $0.** Before, a missing ETH/USD price or gas price made gas cost $0, which made every trade look profitable. `gas.gas_cost_usd` now raises `GasCostUnavailable` and callers skip the trade. A failed `estimate_gas` falls back to `TX_GAS_LIMIT`, which is an upper bound.
- **Blocking calls:** RPC calls on async paths (`estimate_gas`, `gas_price`, `fee_history`, `eth_call` simulation) used to block the event loop. They now run via `asyncio.to_thread`. The sync feature code reads only the cached gas price.
- **Bellman-Ford never contributed.** It returned open paths, and the caller's `c[0] == c[-1]` filter dropped every one. It now returns closed, deduplicated cycles whose rate product is checked to be above 1, and it can't walk a `-1` predecessor.
- **Cycle search docs:** the depth-capped "Johnson" search is now named `bounded_simple_cycles` and documented as non-exhaustive. Both enumerators deduplicate rotations and take the blacklist as a parameter instead of reading a global.

## Tests
These run offline: no RPC connection, no keys, nothing deployed.
```sh
python -m unittest discover -s onchain-execution-engine/tests -p "test_*.py" -v   # 26 tests
cd onchain-execution-engine/tests && npm ci --ignore-scripts && npm test && npm run compile   # 23 EVM tests
```
The suite also checks that the ABI embedded in `engine.py` matches the compiled contract, that the storage layout stays upgrade-safe, that sending is blocked without opt-in, and that a missing price raises instead of reporting free gas. CI runs it in the `execution-engine-tests` job.
