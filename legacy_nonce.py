"""Nonce coordination without imports, keys or connections from the trading bots.

One manager must exclusively own an account. Estimation only previews. Submission
is serialized through acceptance; an ambiguous broadcast failure blocks further
submissions until the transaction is reconciled by the operator.
"""
import asyncio


class NonceUncertain(RuntimeError):
    pass


class NonceMgr:
    def __init__(self, w3, addr):
        self.w3 = w3
        self.addr = addr
        self.nonce = None  # Next nonce after a confirmed RPC acceptance.
        self.lock = asyncio.Lock()
        self.uncertain = False

    async def _chain_pending(self):
        # Falling back to latest can reuse a nonce already in the mempool.
        return int(await asyncio.to_thread(self.w3.eth.get_transaction_count, self.addr, "pending"))

    async def preview(self):
        pending = await self._chain_pending()
        return max(pending, self.nonce if self.nonce is not None else pending)

    async def submit(self, prepare, broadcast):
        async with self.lock:
            if self.uncertain:
                raise NonceUncertain("Previous broadcast outcome is unknown; reconcile before sending again")
            nonce = await self.preview()
            # A signing/preparation failure has not broadcast anything.
            prepared = await prepare(nonce)
            # Cancellation, timeout or RPC failure may occur AFTER acceptance.
            self.uncertain = True
            result = await broadcast(prepared)
            if not result:
                raise NonceUncertain("Broadcast did not return a transaction hash")
            self.nonce = nonce + 1
            self.uncertain = False
            return result
