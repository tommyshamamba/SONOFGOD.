"""WebSocket subscription helpers that work across web3.py v6 and v7.

Kept free of RPC URLs, keys and global state so they can be tested offline.
"""
import asyncio
import json
import logging

logger = logging.getLogger("onchain_engine.ws")

# ============================ WSS compat (web3.py v6/v7) ============================
from dataclasses import dataclass
from typing import Any, Dict, AsyncIterator

@dataclass(frozen=True)
class _WSSubscription:
    id: str
    method: str
    _aw3: Any

    # Allow: `async for ev in sub:` to work transparently
    def __aiter__(self) -> AsyncIterator[Any]:  # type: ignore[override]
        return _iter_ws_subscription(self._aw3, self)

async def _ws_subscribe(aw3, method: str, params=None) -> _WSSubscription:
    sid = await aw3.eth.subscribe(method, [] if params is None else params)
    return _WSSubscription(id=sid, method=method, _aw3=aw3)

def _ws_stream(aw3) -> AsyncIterator[Dict[str, Any]]:
    """Return an async iterator over provider subscription messages across web3.py versions.

    Tries, in order:
      - web3.py v7: aw3.socket.process_subscriptions()
      - web3.py v6: aw3.ws.listen_to_websocket()
      - Provider-bound variants (aw3.provider.socket / ws / websocket / _ws)
      - A direct async-iterable websocket object (as last resort)
    """
    roots = [aw3]
    prov = getattr(aw3, "provider", None)
    if prov:
        roots.append(prov)
    objs = []
    for root in roots:
        for attr in ("socket", "ws", "websocket", "_ws"):
            obj = getattr(root, attr, None)
            if obj is not None:
                objs.append(obj)

    # v7: .process_subscriptions()
    for obj in objs:
        cb = getattr(obj, "process_subscriptions", None)
        if callable(cb):
            return cb()

    # v6: .listen_to_websocket()
    for obj in objs:
        cb = getattr(obj, "listen_to_websocket", None)
        if callable(cb):
            return cb()

    # If any candidate is directly async-iterable, use it
    for obj in objs:
        try:
            ait = obj.__aiter__  # type: ignore[attr-defined]
        except Exception:
            ait = None
        if callable(ait):
            return obj  # itself can be `async for`'d

    raise RuntimeError("No websocket subscription stream found on AsyncWeb3 instance.")



def _detect_ws_stream_label(aw3) -> str:
    """Return which underlying websocket interface will be used (for logging/self-test)."""
    provider = getattr(aw3, 'provider', None) or getattr(aw3, 'ws', None)
    if provider is None:
        return "none"
    # web3>=6 AsyncHTTPProvider has .ws attr during ws, older adapters may expose .socket
    if hasattr(provider, 'ws') and hasattr(getattr(provider, 'ws', None), 'receive'):
        return "provider.ws.receive"
    if hasattr(provider, 'ws') and hasattr(getattr(provider, 'ws', None), '__aiter__'):
        return "provider.ws.__aiter__"
    if hasattr(provider, 'socket') and hasattr(getattr(provider, 'socket', None), 'recv'):
        return "provider.socket.recv"
    return "unknown"

async def ws_self_test(aw3, timeout_seconds: float = 5.0) -> str:
    """Subscribe to newHeads briefly to verify events arrive; returns stream label."""
    label = _detect_ws_stream_label(aw3)
    try:
        sub = await _ws_subscribe(aw3, "newHeads")
        # try to read one event using our iterator with a hard timeout
        try:
            async def _one():
                async for _ in _iter_ws_subscription(aw3, sub, timeout=timeout_seconds):
                    return True
            ok = await asyncio.wait_for(_one(), timeout=timeout_seconds + 1.0)
        finally:
            try:
                await _ws_unsubscribe(aw3, sub)
            except Exception:
                pass
        if ok:
            logger.info(f"WSS self-test OK via {label}")
        else:
            logger.warning(f"WSS self-test: no events within {timeout_seconds:.1f}s via {label}")
    except Exception as e:
        logger.warning(f"WSS self-test failed via {label}: {e}")
    return label


def _ws_extract(message):
    """Normalize raw JSON-RPC and web3.py's decoded subscription messages."""
    from collections.abc import Mapping
    if isinstance(message, (bytes, bytearray, str)):
        try:
            message = json.loads(message)
        except (ValueError, UnicodeDecodeError):
            return None, None
    if not isinstance(message, Mapping):
        return None, None
    payload = message.get("params", message)
    if not isinstance(payload, Mapping):
        return None, None
    return payload.get("subscription"), payload.get("result")


async def _iter_ws_subscription(aw3, sub: _WSSubscription, timeout: float = None) -> AsyncIterator[Any]:
    """Read one subscription; the caller owns unsubscribe and reconnect behavior."""
    if timeout is not None and timeout <= 0:
        raise ValueError("Subscription timeout must be positive")
    iterator = _ws_stream(aw3).__aiter__()
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout if timeout is not None else None
    try:
        while True:
            try:
                if deadline is None:
                    message = await iterator.__anext__()
                else:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        raise asyncio.TimeoutError("subscription idle timeout")
                    message = await asyncio.wait_for(iterator.__anext__(), timeout=remaining)
            except StopAsyncIteration:
                raise ConnectionError("WebSocket subscription stream ended") from None
            sid, result = _ws_extract(message)
            if sid == sub.id:
                if timeout is not None:
                    deadline = loop.time() + timeout
                yield result
    finally:
        close = getattr(iterator, "aclose", None)
        if close is not None:
            await close()

async def _ws_unsubscribe(aw3, sub) -> None:
    sid = getattr(sub, "id", sub)
    try:
        await aw3.eth.unsubscribe(sid)
    except Exception:
        pass
# ============================ end WSS compat ============================
