# On-chain Execution Engine (research prototype)

An asynchronous Python service and Solidity executor contract for studying event-driven
transaction pipelines on an EVM chain (Arbitrum). This is research code. It has not been
run as a production system, and no live results are claimed.

## What the code does

| Component | File | Responsibility |
| --- | --- | --- |
| Block-stream engine | `stream_engine.py` | WebSocket subscriptions across web3.py v6/v7, endpoint latency probing and failover, a supervisor that restarts failed tasks |
| Graph engine | `arbitrage_engine.py` | Polls DEX quoters/subgraphs into a token price graph, finds candidate cycles (Johnson simple cycles, Bellman-Ford negative cycles), simulates them, and ranks routes with a decaying multi-armed bandit |
| Nonce manager | `nonce_manager.py` | Serialises nonce assignment at submission time so concurrent sends never reuse a nonce |
| Executor contract | `FlashExecutor.sol` | UUPS-upgradeable, pausable, owner-gated contract (OpenZeppelin 5) that decodes and executes request payloads |
| Proxy | `ERC1967Proxy.sol` | Standard ERC-1967 proxy used by the tests |

Safety defaults:
- Transaction signing and submission are blocked unless `ALLOW_LIVE_TRANSACTIONS=true`.
- Without `PRIVATE_KEY` the engine runs read-only.
- All configuration comes from environment variables.

## Architecture

```
WebSocket/HTTP RPC ──► stream_engine (subscriptions, failover, supervisor)
                          │ new block
                          ▼
             arbitrage_engine: quote cache ─► price graph ─► cycle search ─► simulation
                          │ candidate
                          ▼
             nonce_manager ─► signed tx (opt-in only) ─► FlashExecutor (behind ERC-1967 proxy)
```

## Tests

Offline only: no RPC connection, no wallet keys, nothing deployed.

```sh
# Python: nonce coordination, send gating, WebSocket compatibility (16 tests)
python -m unittest discover -s onchain-execution-engine/tests -p "test_*.py" -v

# Solidity: compile with solc 0.8.37 and run in an in-memory EthereumJS VM (23 tests)
cd onchain-execution-engine/tests && npm ci --ignore-scripts && npm test && npm run compile
```

The tests also check that the ABI embedded in the Python engines matches the compiled
contract, and that storage layout stays upgrade-safe. CI runs both suites in the
`execution-engine-tests` job. See [tests/README.md](tests/README.md) for compiler settings and limits.

## Limitations

- Lender, token and router behaviour comes from local fixtures, not real protocols.
- The two engine files are large single modules. Splitting them into packages is the next refactor.
