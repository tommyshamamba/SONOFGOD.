# Legacy executor: isolated regression suite

This suite compiles `../Power2.sol` and runs it behind an ERC-1967 proxy in a fresh
in-memory EthereumJS VM. Lenders, tokens and the swap router are local fixtures.
It opens no RPC connection, reads no wallet keys, and deploys nothing to a network.
The synthetic signing keys in the tests have no external use.

## Run

Use Node 24 and Python 3.11 or newer. From this directory:

```sh
npm ci --ignore-scripts
npm test
npm run compile
```

From the repository root:

```sh
python -m unittest discover -s legacy-tests -p "test_*.py" -v
```

The Solidity build pins compiler 0.8.37 and OpenZeppelin 5.0.2, uses the Shanghai
EVM target, optimizer (200 runs), and IR compilation. The build fails if the
runtime exceeds the EIP-170 bytecode limit. These settings are required; the
ordinary compilation pipeline produces an oversized contract. Dependencies and
integrities are fixed in `package-lock.json`. The compiler's `tmp` dependency is
overridden to the patched 0.2.7 release; its public API remains compatible. ABI
compatibility is checked against both bots' embedded request/management ABIs;
no hand-edited ABI artifact is needed.

Verified locally on 30 September 2026 with Node 24.19.0: **21 EVM tests and
10 Python tests passed**. Power's compiled runtime is **23,554 bytes**, below
the 24,576-byte EIP-170 limit. The registry-backed full dependency audit reported
zero known vulnerabilities for this test toolchain at that time.

## Changes exercised

- An owner request records lender, provider, asset, amount and callback-data hash.
  Only one matching callback may execute; unsolicited, nested, altered and
  repeated callbacks fail. The outer request keeps its reentrancy guard active.
- Aave verifies its initiator and receives an exact repayment allowance. The
  pool pulls repayment after the callback, then the executor clears approval.
  Balancer and Uniswap receive direct repayment.
- DODO V2's DVM, DPP and DSP callback names are supported, along with the existing
  alias. Base and quote borrowing repay the actual borrowed token. Both Python
  bots now wrap DODO requests with their configured pool and pool base token.
- Payload selectors retain Solidity's left-aligned `bytes4` representation.
- Successful arbitrage preserves preexisting borrowed-asset balances; an old
  reserve cannot subsidize an unprofitable trade. `withdrawProfit(address)` is
  implemented for the owner's idle residual-token withdrawal.
- Estimation reads a nonce without allocating one. Submission signs under a
  shared lock and advances the nonce only after RPC acceptance. Ambiguous sends
  (including cancellation) block subsequent submissions rather than reusing an
  uncertain nonce. Both bot send functions are tested by extracting their AST;
  the bot modules themselves are never imported or started.

## Boundaries

These are targeted local correctness tests, **not a contract audit or proof of
profitable trading**. The profitable swap output is deliberately manufactured
by the fixture. Network addresses, market liquidity, price/MEV behavior, real
lender and router deployments, all DEX adapters, liquidation strategies, bridge
integrations and production bot operation still require independent validation.
No mainnet fork or live transaction was used.

The nonce helper coordinates one process with exclusive account ownership. Its
state is not durable across restarts and private-relay acceptance does not prove
mining. After an ambiguous send or expired private transaction, reconcile receipts
and pending transactions before restarting or resuming with that account. Do not
run both bots against the same account concurrently.

The new active-loan struct consumes five slots from the original 45-slot storage
gap; the test asserts its size and remaining gap. This does not authorize an
upgrade of any deployed proxy. A real upgrade requires comparing the deployed
implementation's complete storage layout and configuration.

## Protocol references

- [Aave flash-loan repayment flow](https://aave.com/docs/aave-v3/guides/flash-loans)
- [DODO V2 callback names](https://docs.dodoex.io/en/developer/contracts/dodo-v1-v2/guides/flash-loan)
- [Solidity compiler configuration](https://docs.soliditylang.org/en/v0.8.22/using-the-compiler.html)
- [EthereumJS VM local execution](https://github.com/ethereumjs/ethereumjs-monorepo/tree/master/packages/vm)
