"""On-chain execution engine: research prototype (never deployed).

Modules
-------
ws_compat  WebSocket subscription helpers (web3.py v6/v7)
config     environment configuration
graph      price-graph cycle search (pure)
gas        gas-cost arithmetic that refuses to price unknown inputs as free
nonce      serialized nonce manager that halts on ambiguous broadcasts
engine     runtime: RPC failover, quoting, strategy, supervisor (imports web3, connects on import)

Importing the package does not import `engine`.
"""
