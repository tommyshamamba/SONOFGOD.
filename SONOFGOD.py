# Arbitrum MEV Bot: Arbitrage + Liquidations + Backruns + Profit Recovery + Bridging
# (Merged from THELORD.py and ALPHA.py)
# - Advanced AI-driven arbitrage engine (Bandit + ML)
# - Fully implemented Aave v3 Liquidation Hunter
# - Complete Profit Recovery: Withdraw, classify (ERC20, ERC4626, aToken, LP), redeem, consolidate, and bridge
# - Self-healing supervisor for all tasks (arbitrage, liquidations, etc.)
# - Resilient WSS connection with auto-failover
# - Superior color-coded logging for developer experience

import os, json, time, sqlite3, asyncio, aiohttp, random, requests, signal, math, logging, hashlib, statistics, re, inspect
from typing import Dict, List, Tuple, Optional, Any, Set, Callable, Awaitable

# ============================ Logging Setup (from ALPHA.py) ============================
class ColorFormatter(logging.Formatter):
    LEVELS = {
        logging.DEBUG: ("🧩", "\x1b[37m"),    # Grey
        logging.INFO:  ("🟢", "\x1b[32m"),    # Green
        logging.WARNING: ("🟡", "\x1b[33m"),  # Yellow
        logging.ERROR: ("🔴", "\x1b[31m"),    # Red
        logging.CRITICAL: ("🛑", "\x1b[41m"), # White on Red
    }
    RESET = "\x1b[0m"

    def format(self, record: logging.LogRecord) -> str:
        emoji, color = self.LEVELS.get(record.levelno, ("", ""))
        ctx_str = ""
        # The 'extra' kwarg to logging functions adds to the record's __dict__
        if "ctx" in record.__dict__:
            ctx = record.__dict__["ctx"]
            ctx_str = f" | ctx={json.dumps(ctx, sort_keys=True, default=str)}" if isinstance(ctx, dict) else ""
        
        msg = super().format(record)
        return f"{color}{emoji} {msg}{ctx_str}{self.RESET}"

def _install_logging():
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    handler = logging.StreamHandler()
    formatter = ColorFormatter(
        "%(asctime)s.%(msecs)03d %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)
    root.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))
    # Silence noisy libraries
    logging.getLogger("web3").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

_install_logging()
logger = logging.getLogger("arb-bot")


if os.name == "nt":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        logger.debug("Using WindowsSelectorEventLoopPolicy")
    except Exception as e:
        logger.warning("Failed to set WindowsSelectorEventLoopPolicy: %s", e)

from decimal import Decimal, ROUND_FLOOR, getcontext
from urllib.parse import urlparse

from dotenv import load_dotenv
from aiohttp import web

from web3 import Web3
WSProvider = None
AsyncWeb3 = None
try:
    from web3.providers.websocket import WebsocketProviderV2 as WSProvider
    from web3 import AsyncWeb3
except Exception:
    try:
        from web3.providers.websocket import WebsocketProvider as WSProvider
        from web3 import AsyncWeb3
    except Exception:
        WSProvider = None
        AsyncWeb3 = None

try:
    from web3.middleware import geth_poa_middleware, construct_sign_and_send_raw_middleware
except Exception:
    try:
        from web3.middleware.geth_poa import geth_poa_middleware
    except Exception:
        geth_poa_middleware = None
    construct_sign_and_send_raw_middleware = None

from eth_account import Account
from eth_utils import function_signature_to_4byte_selector
from eth_abi import encode as abi_encode

# ============================ Global init ============================
load_dotenv(override=True)
getcontext().prec = 40

random.seed(os.urandom(8))
STOP_EVENT = asyncio.Event()
BLOCK_UPDATE_EVENT = asyncio.Event()
WSS_READY_EVENT = asyncio.Event()  # set when WS healthy, cleared on drop

# ============================ ENV helpers ============================
def _first_env(*names: str) -> str:
    for n in names:
        v = os.getenv(n, "").strip()
        if v:
            return v
    return ""

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "")
    if v == "":
        return default
    return v.lower() in {"1", "true", "yes", "y", "on"}

def _env_int(name: str, default: int = 0) -> int:
    v = os.getenv(name, "")
    try:
        return int(v) if v != "" else default
    except Exception:
        return default

def _env_float(name: str, default: float = 0.0) -> float:
    v = os.getenv(name, "")
    try:
        return float(v) if v != "" else default
    except Exception:
        return default

def _env_addr(*keys: str, default: str = "") -> str:
    for k in keys:
        v = os.getenv(k, "").strip()
        if v:
            return v
    return default

# ============================ RPCs ============================
ALCHEMY_HTTP    = os.getenv("ALCHEMY_HTTP", "").strip()
ALCHEMY_WSS     = os.getenv("ALCHEMY_WSS", "").strip()
INFURA_HTTP     = os.getenv("INFURA_HTTP", "").strip()
INFURA_WSS      = os.getenv("INFURA_WSS", "").strip()
CHAINSTACK_HTTP = os.getenv("CHAINSTACK_HTTP", "").strip()
CHAINSTACK_WSS  = os.getenv("CHAINSTACK_WSS", "").strip()
LAVA_HTTP       = os.getenv("LAVA_HTTP", "").strip()
LAVA_WSS        = os.getenv("LAVA_WSS", "").strip()
WSS_URL_OVERRIDE = os.getenv("WSS_URL_OVERRIDE", "").strip()
WSS_SUBSCRIBE_ORDER = [s.strip().lower() for s in os.getenv("WSS_SUBSCRIBE_ORDER", "pending,newHeads").split(",") if s.strip()]
WSS_ACCEPT_ON_PROBE = _env_bool("WSS_ACCEPT_ON_PROBE", True)
HTTP_URL = _first_env("ALCHEMY_HTTP", "INFURA_HTTP", "CHAINSTACK_HTTP", "LAVA_HTTP")

# ============================ Keys & contract ============================
PRIVATE_KEY   = os.getenv("PRIVATE_KEY", "").strip()
CHAIN_ID      = _env_int("CHAIN_ID", 42161)
ARB_EXECUTOR_ADDRESS_ENV = os.getenv("ARB_EXECUTOR_ADDRESS", "0x0000000000000000000000000000000000000000")
ARB_EXECUTOR_OWNER_ENV   = os.getenv("ARB_EXECUTOR_OWNER",   "0x0000000000000000000000000000000000000000")
EXECUTOR_EVENTS_ABI_PATH = os.getenv("EXECUTOR_EVENTS_ABI_PATH", "abi/executor_events.json")
SKIP_OWNER_CHECK = _env_bool("SKIP_OWNER_CHECK", False)

# Phase-2 toggles
USE_JOHNSON_CYCLES = _env_bool("USE_JOHNSON_CYCLES", True)
BANDIT_DECAY_GAMMA = float(os.getenv("BANDIT_DECAY_GAMMA", "0.995"))
BANDIT_DECAY_INTERVAL_SEC = _env_int("BANDIT_DECAY_INTERVAL_SEC", 3600)

def _is_zero_address(addr: str) -> bool:
    s = (addr or "").lower()
    if not s.startswith("0x"):
        s = "0x" + s
    return s == "0x" + "0"*40

try:
    ARB_EXECUTOR_ADDRESS = Web3.to_checksum_address(ARB_EXECUTOR_ADDRESS_ENV)
except Exception:
    ARB_EXECUTOR_ADDRESS = "0x0000000000000000000000000000000000000000"
try:
    ARB_EXECUTOR_OWNER = Web3.to_checksum_address(ARB_EXECUTOR_OWNER_ENV)
except Exception:
    ARB_EXECUTOR_OWNER = "0x0000000000000000000000000000000000000000"

# ============================ Telegram / health ============================
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_HEARTBEAT_MIN = _env_int("TELEGRAM_HEARTBEAT_MIN", 0)
HEALTH_HOST = os.getenv("HEALTH_HOST", "0.0.0.0")
HEALTH_PORT = _env_int("HEALTH_PORT", 8788)

# ============================ Gas / execution ============================
DYNAMIC_GAS   = _env_bool("DYNAMIC_GAS", True)
GAS_URGENCY   = os.getenv("GAS_URGENCY", "balanced")
MAX_FEE_PER_GAS_GWEI  = _env_float("MAX_FEE_PER_GAS_GWEI", 0.0)
MAX_PRIORITY_FEE_GWEI = _env_float("MAX_PRIORITY_FEE_GWEI", 0.25)
TX_GAS_LIMIT          = _env_int("TX_GAS_LIMIT", 2_400_000)
USE_FLASHBOTS = _env_bool("USE_FLASHBOTS", False) and CHAIN_ID == 1
FLASHBOTS_RPC = os.getenv("FLASHBOTS_RELAY", os.getenv("FLASHBOTS_RPC", "https://rpc.flashbots.net")).strip()

# ============================ Strategy / limits ============================
MIN_PROFIT_USD    = Decimal(os.getenv("MIN_PROFIT_USD", "25"))
ORACLE_DIVERGENCE = Decimal(os.getenv("ORACLE_DIVERGENCE", "0.02"))
ANOMALY_THRESHOLD = Decimal(os.getenv("ANOMALY_THRESHOLD", "0.30"))
MAX_HOPS          = _env_int("MAX_HOPS", 4)
SLIPPAGE          = Decimal(os.getenv("SLIPPAGE", "0.003"))
SCAN_INTERVAL     = _env_int("SCAN_INTERVAL", 5)
USE_UNIV3_ONLY    = _env_bool("USE_UNIV3_ONLY", True)
MAX_TOKENS        = _env_int("MAX_TOKENS", 60)
BAL_PER_HOP       = _env_int("BAL_PER_HOP", 10)
HYBRID_AI_DISCOVERY = _env_bool("HYBRID_AI_DISCOVERY", True)
ML_BACKEND        = os.getenv("ML_BACKEND", "auto").strip().lower()

SLIPPAGE_MAX               = Decimal(os.getenv("SLIPPAGE_MAX", "0.010"))
SLIPPAGE_HOP_BUMP          = Decimal(os.getenv("SLIPPAGE_HOP_BUMP", "0.0005"))
SLIPPAGE_SIZE_STEP_USD     = Decimal(os.getenv("SLIPPAGE_SIZE_STEP_USD", "100000"))
SLIPPAGE_SIZE_STEP_BUMP    = Decimal(os.getenv("SLIPPAGE_SIZE_STEP_BUMP", "0.0005"))
SLIPPAGE_SIZE_MAX_STEPS    = _env_int("SLIPPAGE_SIZE_MAX_STEPS", 10)

# Flashloan provider caps
CAP_AAVE_PCT      = Decimal(os.getenv("CAP_AAVE_PCT", "0.80"))
CAP_BALANCER_PCT  = Decimal(os.getenv("CAP_BALANCER_PCT", "0.80"))
CAP_DODO_PCT      = Decimal(os.getenv("CAP_DODO_PCT", "0.80"))

# Liquids/Backruns
ENABLE_LIQUIDATIONS = _env_bool("ENABLE_LIQUIDATIONS", True)
LIQ_POOL_ADDR = Web3.to_checksum_address(os.getenv("LIQ_POOL", "0x794a61358D6845594F94dc1DB02A252b5b4814aD")) # AAVE V3 Arbitrum
LIQ_CLOSE_FACTOR_PCT = Decimal(os.getenv("LIQ_CLOSE_FACTOR_PCT", "0.5")) # Liquidate up to 50% of debt
LIQ_MIN_PROFIT_USD = Decimal(os.getenv("LIQ_MIN_PROFIT_USD", "25"))

ENABLE_BACKRUNS = _env_bool("ENABLE_BACKRUNS", True)
BACKRUN_TARGETS = [a.strip() for a in os.getenv("BACKRUN_TARGETS", "").split(",") if a.strip()]
WITHDRAW_INTERVAL_SEC = _env_int("WITHDRAW_INTERVAL_SEC", 3600)

# Profit recovery / consolidation
AUTO_CONSOLIDATE_PROFITS = _env_bool("AUTO_CONSOLIDATE_PROFITS", True)
CONSOLIDATE_PREFERRED_BASE = os.getenv("CONSOLIDATE_PREFERRED_BASE", "").strip().upper() or os.getenv("TARGET_TOKEN", "USDC").strip().upper()
CONSOLIDATE_MIN_USD = Decimal(os.getenv("CONSOLIDATE_MIN_USD", "5"))
CONSOLIDATE_SLIPPAGE = Decimal(os.getenv("CONSOLIDATE_SLIPPAGE", "0.005"))
PROFIT_RECOVERY_INTERVAL_SEC = _env_int("PROFIT_RECOVERY_INTERVAL_SEC", 900)
TOKEN_DISCOVERY_INTERVAL = _env_int("TOKEN_DISCOVERY_INTERVAL", 600)
CONSOLIDATE_UNWRAP_WETH = _env_bool("CONSOLIDATE_UNWRAP_WETH", False)

# Bridging (Across)
BRIDGE_ENABLED = _env_bool("BRIDGE_ENABLED", False)
BRIDGE_PROTOCOL = os.getenv("BRIDGE_PROTOCOL", "AUTO").strip().upper()
BRIDGE_TARGET_CHAIN_ID = _env_int("BRIDGE_TARGET_CHAIN_ID", CHAIN_ID)
BRIDGE_MIN_USD = Decimal(os.getenv("BRIDGE_MIN_USD", "50"))
BRIDGE_MAX_PCT_BALANCE = float(os.getenv("BRIDGE_MAX_PCT_BALANCE", "0.95"))
BRIDGE_PREFERRED_TOKEN = os.getenv("BRIDGE_PREFERRED_TOKEN", "USDC").strip().upper()
ACROSS_SPOKEPOOL_ADDRESS_ENV = os.getenv("ACROSS_SPOKEPOOL_ADDRESS", "").strip()
ACROSS_SPOKEPOOL_ABI_PATH = os.getenv("ACROSS_SPOKEPOOL_ABI_PATH", "abi/across_spokepool.json").strip()
ACROSS_RELAYER_FEE_BPS = _env_int("ACROSS_RELAYER_FEE_BPS", 30)
ACROSS_QUOTE_VALID_SECS = _env_int("ACROSS_QUOTE_VALID_SECS", 900)

# Recovery log scan tuning
RECOVERY_TRANSFER_LOOKBACK_BLOCKS = _env_int("RECOVERY_TRANSFER_LOOKBACK_BLOCKS", 200_000)
RECOVERY_LOG_CHUNK = _env_int("RECOVERY_LOG_CHUNK", 25_000)

# ============================ Accounts & Web3 ============================
ACCOUNT: Optional[Account] = None
ACCOUNT_ADDRESS: Optional[str] = None
if PRIVATE_KEY:
    try:
        ACCOUNT = Account.from_key(PRIVATE_KEY)
        ACCOUNT_ADDRESS = ACCOUNT.address
    except Exception as e:
        logger.error("Invalid PRIVATE_KEY: %s. Tx sending disabled.", e)
else:
    logger.warning("No PRIVATE_KEY provided. Running in read-only mode (no tx sends).")

if not HTTP_URL:
    raise RuntimeError("No HTTP RPC configured. Set ALCHEMY_HTTP or another provider URL.")
w3 = Web3(Web3.HTTPProvider(HTTP_URL, request_kwargs={"timeout": 15}))
if not w3.is_connected():
    raise RuntimeError("HTTP provider not connected")
try:
    if geth_poa_middleware:
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
except Exception:
    pass
try:
    if ACCOUNT and construct_sign_and_send_raw_middleware:
        w3.middleware_onion.add(construct_sign_and_send_raw_middleware(ACCOUNT))
        w3.eth.default_account = ACCOUNT.address
except Exception:
    pass
try:
    from gas_manager import attach_dynamic_fee_middleware  # optional external
    if DYNAMIC_GAS:
        attach_dynamic_fee_middleware(w3)
        logger.info("Dynamic gas enabled (urgency=%s, tip_floor=%s gwei)", GAS_URGENCY, MAX_PRIORITY_FEE_GWEI)
except Exception as e:
    if _env_bool("QUIET_WEB3", True) is False:
        logger.warning("Dynamic gas middleware not attached: %s", e)

# ============================ HTTP Session Management ============================
HTTP_SESSION: Optional[aiohttp.ClientSession] = None

async def _ensure_http_session() -> aiohttp.ClientSession:
    global HTTP_SESSION
    if HTTP_SESSION is None or HTTP_SESSION.closed:
        timeout = aiohttp.ClientTimeout(total=_env_float("HTTP_TIMEOUT", 15.0))
        HTTP_SESSION = aiohttp.ClientSession(timeout=timeout)
        logger.debug("Created new aiohttp.ClientSession")
    return HTTP_SESSION

async def close_http_session():
    global HTTP_SESSION
    if HTTP_SESSION and not HTTP_SESSION.closed:
        await HTTP_SESSION.close()
        logger.debug("Closed aiohttp.ClientSession")
    HTTP_SESSION = None

# ============================ Async WSS ============================
WSS_ACTIVE_URL: Optional[str] = None
aw3: Optional["AsyncWeb3"] = None
WSS_SUBPROTOCOL = os.getenv("WS_JSONRPC_SUBPROTOCOL", "").strip() or ""
WS_ORIGIN = os.getenv("WS_ORIGIN", "").strip() or None

def _wss_kwargs() -> Dict[str, Any]:
    kw: Dict[str, Any] = {}
    try:
        if WSS_SUBPROTOCOL and WSS_SUBPROTOCOL.lower() not in ("none", "auto"):
            kw["subprotocols"] = [WSS_SUBPROTOCOL]
    except Exception:
        pass
    headers: Dict[str, str] = {}
    if WS_ORIGIN:
        headers["Origin"] = WS_ORIGIN
    if headers:
        kw["extra_headers"] = headers
    open_timeout = _env_float("WSS_OPEN_TIMEOUT", 10.0)
    if open_timeout > 0:
        kw["open_timeout"] = open_timeout
    close_timeout = _env_float("WSS_CLOSE_TIMEOUT", 5.0)
    if close_timeout > 0:
        kw["close_timeout"] = close_timeout
    ping_interval = _env_float("WS_PING_INTERVAL", 20.0)
    if ping_interval >= 0:
        kw["ping_interval"] = ping_interval or None
    ping_timeout = _env_float("WS_PING_TIMEOUT", 10.0)
    if ping_timeout >= 0:
        kw["ping_timeout"] = ping_timeout or None
    max_size = _env_int("WS_MAX_SIZE", 10_485_760) # 10MB default
    if max_size > 0:
        kw["max_size"] = max_size
    return kw

def _mask_url(u: str) -> str:
    if not u: return "N/A"
    try:
        p = urlparse(u)
        masked_path = re.sub(r"/([A-Za-z0-9\-_]{16,})", "/...", p.path)
        return f"{p.scheme}://{p.netloc}{masked_path or ''}"
    except Exception:
        return u


def _sorted_kwargs_keys(pk: Any) -> List[str]:
    if isinstance(pk, dict):
        try:
            return sorted(str(k) for k in pk.keys())
        except Exception:
            return []
    return []


def _label_kwargs(pk: Any) -> str:
    keys = _sorted_kwargs_keys(pk)
    return ",".join(keys) if keys else "<default>"

async def _probe_wss_latency(
    url: str, attempt_kwargs: List[Dict[str, Any]]
) -> Tuple[bool, float, Optional[str]]:
    if WSProvider is None or AsyncWeb3 is None:
        return False, float("inf"), "AsyncWeb3 unavailable"
    best = float("inf"); ok = False
    last_error: Optional[str] = None
    attempt_errors: List[str] = []
    for pk in attempt_kwargs:
        t0 = time.time()
        provider = None
        try:
            provider = WSProvider(url, **pk)
            _aw3 = AsyncWeb3(provider)
            is_conn = await _aw3.is_connected()
            if not is_conn and WSS_ACCEPT_ON_PROBE:
                _ = await _aw3.eth.block_number
                is_conn = True
            if is_conn:
                ok = True
                dt = time.time() - t0
                if dt < best: best = dt
            else:
                details: List[str] = ["is_connected() returned False"]
                ws_obj = getattr(provider, "ws", None) or getattr(provider, "_ws", None)
                close_code = getattr(ws_obj, "close_code", None)
                close_reason = getattr(ws_obj, "close_reason", None)
                if close_code is not None:
                    details.append(f"close_code={close_code}")
                if close_reason:
                    details.append(f"close_reason={close_reason}")
                detail_msg = ", ".join(details)
                last_error = detail_msg
                key_label = _label_kwargs(pk)
                attempt_errors.append(f"{key_label}: {detail_msg}")
                logger.warning(
                    "WSS probe inactive for %s with kwargs=%s: %s",
                    _mask_url(url), key_label, last_error,
                )
        except Exception as e:
            ok = False
            detail_msg = f"{type(e).__name__}: {e}".strip()
            last_error = detail_msg
            key_label = _label_kwargs(pk)
            attempt_errors.append(f"{key_label}: {repr(e)}")
            logger.warning(
                "WSS probe error for %s with kwargs=%s: %s",
                _mask_url(url), key_label, detail_msg or repr(e),
            )
        finally:
            if provider:
                try:
                    await provider.disconnect()
                except Exception:
                    pass
    if attempt_errors:
        last_error = "; ".join(attempt_errors)
    return ok, best, last_error

async def _init_wss() -> Optional["AsyncWeb3"]:
    global aw3, WSS_ACTIVE_URL
    if WSProvider is None or AsyncWeb3 is None:
        logger.warning("WSProvider/AsyncWeb3 unavailable; WSS disabled.")
        return None
    env_keys = ("WSS_URL_OVERRIDE", "ALCHEMY_WSS", "INFURA_WSS", "CHAINSTACK_WSS", "LAVA_WSS", "PUBLIC_WSS")
    seen, candidates = set(), []
    for k in env_keys:
        v = (os.getenv(k) or "").strip()
        if v and v not in seen:
            candidates.append(v[:-3] if v.endswith("/ws") else v)
            seen.add(v)
    if not candidates:
        logger.info("No WSS candidates; WSS disabled.")
        return None

    masked = [_mask_url(u) for u in candidates]
    logger.info("Probing WSS candidates: %s", ", ".join(masked))

    ws_kw = {"open_timeout": 12, "ping_interval": 20, "max_size": 10 * 1024 * 1024}
    ws_kw.update(_wss_kwargs())
    ws_timeout = _env_float("WS_TIMEOUT_SEC", 15.0)

    attempt_kwargs: List[Dict[str, Any]] = []
    base_kwargs: Dict[str, Any] = {}
    if ws_timeout > 0:
        try:
            sig_params = set(inspect.signature(WSProvider.__init__).parameters)
        except Exception:
            sig_params = set()
        if "websocket_timeout" in sig_params:
            base_kwargs["websocket_timeout"] = ws_timeout
        elif "timeout" in sig_params:
            base_kwargs["timeout"] = ws_timeout
        elif "request_timeout" in sig_params:
            base_kwargs["request_timeout"] = ws_timeout
    if ws_kw:
        kw = dict(base_kwargs); kw["websocket_kwargs"] = ws_kw; attempt_kwargs.append(kw)
    if base_kwargs: attempt_kwargs.append(dict(base_kwargs))
    attempt_kwargs.append({})

    scores: List[Tuple[float, str]] = []
    for url in candidates:
        ok, dt, err = await _probe_wss_latency(url, attempt_kwargs)
        if ok:
            scores.append((dt, url))
            logger.info("WSS probe OK: %s (latency=%.1fms)", _mask_url(url), dt * 1000.0)
        else:
            if err:
                logger.warning("WSS probe FAIL: %s | last_error=%s", _mask_url(url), err)
            else:
                logger.warning("WSS probe FAIL: %s", _mask_url(url))

    if not scores:
        logger.error("All WSS probes failed; backruns/mempool disabled.")
        return None

    scores.sort()
    best_url = scores[0][1]

    for pk in attempt_kwargs:
        try:
            provider = WSProvider(best_url, **pk)
            _aw3 = AsyncWeb3(provider)
            if await _aw3.is_connected():
                aw3 = _aw3
                WSS_ACTIVE_URL = best_url
                masked = _mask_url(best_url)
                METRICS["wss"] = f"ON({masked})"
                METRICS["wss_endpoint"] = masked
                METRICS["wss_ready_at"] = int(time.time())
                logger.info("WSS connected to best endpoint: %s", masked)
                return _aw3
        except Exception as e:
            kwargs_keys = _sorted_kwargs_keys(pk)
            logger.error(
                "WSS final connection attempt failed for %s with kwargs=%s: %r",
                _mask_url(best_url), kwargs_keys, e,
            )
    
    logger.error("Could not establish WSS connection with selected endpoint %s", _mask_url(best_url))
    return None

async def _auto_wss_initializer():
    global aw3, WSS_ACTIVE_URL

    async def connect_once():
        attempt = 0
        while not STOP_EVENT.is_set():
            attempt += 1
            try:
                w = await _init_wss()
                if w:
                    logger.info("WebSocket successfully connected.")
                    return w
                raise ConnectionError("WSS returned None, will retry.")
            except Exception as e:
                delay = min(2 ** attempt, 20) + random.uniform(0,1) # Add jitter
                logger.warning(
                    "WSS disconnected — retrying in %s s (attempt %s / ∞)…",
                    delay, attempt, extra={"ctx": {"reason": str(e)}}
                )
                await asyncio.sleep(delay)
        return None

    async def watchdog():
        global aw3, WSS_ACTIVE_URL
        while not STOP_EVENT.is_set():
            healthy = False
            if aw3 and aw3.provider:
                try:
                    healthy = await aw3.is_connected()
                except Exception:
                    healthy = False

            if not healthy:
                if WSS_READY_EVENT.is_set():
                    WSS_READY_EVENT.clear()
                METRICS["wss"] = "OFF"
                METRICS["wss_endpoint"] = None
                METRICS["wss_ready_at"] = None
                logger.warning("WebSocket appears disconnected. Attempting to reconnect...")
                
                if aw3 and aw3.provider:
                    try: await aw3.provider.disconnect()
                    except Exception: pass
                
                new_w3 = await connect_once()
                if new_w3:
                    aw3 = new_w3
                    WSS_READY_EVENT.set()
                    METRICS["wss"] = f"ON({_mask_url(WSS_ACTIVE_URL)})" if WSS_ACTIVE_URL else "ON"
                    logger.info("WebSocket reconnected: %s", _mask_url(WSS_ACTIVE_URL or ""))
            else:
                if not WSS_READY_EVENT.is_set():
                    WSS_READY_EVENT.set()
            
            await asyncio.sleep(15)

    aw3 = await connect_once()
    if aw3:
        WSS_READY_EVENT.set()
    else:
        WSS_READY_EVENT.clear()
        logger.critical("Initial WSS connection failed after multiple retries. The bot may not function correctly.")

    asyncio.create_task(watchdog())
    return aw3

# ============================ Metrics & state ============================
START_TIME = time.time()
METRICS: Dict[str, Any] = {
    "trades_total": 0, "trades_success": 0,
    "liqs_total": 0, "liqs_success": 0,
    "backruns_seen": 0,
    "last_strategy": None, "last_dex": None, "last_cycle": None,
    "last_gross_usd": None, "last_net_usd": None, "last_tx": None,
    "last_ts": None, "wss": "OFF",
    "wss_endpoint": None,
    "wss_ready_at": None,
    "last_block": None,
    "mempool_mode": "OFF",
    "mempool_endpoint": None,
    "mempool_ready_at": None,
}
DEBUG_DECISIONS = _env_bool("DEBUG_DECISIONS", True)
BLACKLIST: set = set()
GAS_EMA_ALPHA = _env_float("GAS_EMA_ALPHA", 0.8)
_gas_ema: Optional[float] = None
_GAS_PRICE_CACHE: Tuple[int, float] = (0, 0.0)

def get_cached_gas_price() -> int:
    global _GAS_PRICE_CACHE
    now = time.time()
    if now - _GAS_PRICE_CACHE[1] > 2.0:
        try:
            _GAS_PRICE_CACHE = (int(w3.eth.gas_price), now)
        except Exception:
            pass
    return _GAS_PRICE_CACHE[0]

def predict_gas_price_wei(fast: bool = False) -> int:
    global _gas_ema
    if fast:
        try:
            return int(get_cached_gas_price() or w3.eth.gas_price)
        except Exception:
            return int(_gas_ema or 0) if _gas_ema else 0
    try:
        gp = int(get_cached_gas_price() or 0)
    except Exception:
        gp = int(_gas_ema or 0)
    if gp <= 0:
        return int(_gas_ema or 0) if _gas_ema else 0
    if _gas_ema is None:
        _gas_ema = float(gp)
    else:
        _gas_ema = GAS_EMA_ALPHA * float(gp) + (1.0 - GAS_EMA_ALPHA) * (_gas_ema or 0.0)
    return int(_gas_ema)

def _fee_history_suggested() -> Tuple[Optional[int], Optional[int]]:
    try:
        fh = w3.eth.fee_history(10, "latest", [10, 50, 90])
        base_fees = list(map(int, fh.get("baseFeePerGas") or []))
        rewards = fh.get("reward") or []
        if not base_fees or not rewards:
            return None, None
        base = int(statistics.median(base_fees))
        prios = [int(statistics.median([int(x) for x in block])) for block in rewards if block]
        tip = int(statistics.median(prios)) if prios else None
        if tip is None:
            return None, None
        max_fee = int(base + 2*tip)
        return max_fee, tip
    except Exception:
        return None, None

def _usd_cost_for_tx(tx: Dict[str, Any], margin: float = 1.3) -> Decimal:
    try:
        gas_est = w3.eth.estimate_gas({k:v for k,v in tx.items() if k in ("from","to","data","value")})
    except Exception:
        gas_est = TX_GAS_LIMIT
    gp = Decimal(predict_gas_price_wei(fast=True) or w3.eth.gas_price)
    eth = Decimal(gas_est) * gp / Decimal(1e18)
    return (eth * (get_oracle_price("WETH") or Decimal("0"))) * Decimal(margin)

def estimate_gas_cost(gas_limit: int, fast: bool = False) -> Decimal:
    try:
        gp = Decimal(predict_gas_price_wei(fast=fast) or w3.eth.gas_price)
    except Exception:
        gp = Decimal(0)
    eth_cost = (Decimal(gas_limit) * gp) / Decimal(1e18)
    px = get_oracle_price("WETH") or Decimal("0")
    return eth_cost * px

# ============================ SQLite ============================
DB_FILE = "arb_trades.db"
DB_LOCK = asyncio.Lock()
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cur  = conn.cursor()

async def _db_exec(sql, params=()):
    async with DB_LOCK:
        await asyncio.to_thread(cur.execute, sql, params)
        await asyncio.to_thread(conn.commit)

async def _db_fetchall(sql, params=()):
    async with DB_LOCK:
        return await asyncio.to_thread(lambda: cur.execute(sql, params).fetchall())

async def _db_fetchone(sql, params=()):
    async with DB_LOCK:
        return await asyncio.to_thread(lambda: cur.execute(sql, params).fetchone())

cur.execute("""
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    strategy TEXT,
    cycle TEXT,
    dex TEXT,
    gross_profit REAL,
    net_profit REAL,
    gas_cost REAL,
    flashloan_size INTEGER,
    used_flashbots INTEGER,
    anomaly INTEGER
)""")
cur.execute("""
CREATE TABLE IF NOT EXISTS bandit (
    key TEXT PRIMARY KEY,
    alpha REAL,
    beta  REAL
)""")
cur.execute("""
CREATE TABLE IF NOT EXISTS bayes_grid (
    key TEXT PRIMARY KEY,
    state TEXT
)""")
cur.execute("""
CREATE TABLE IF NOT EXISTS ml_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER,
    weights TEXT,
    feature_names TEXT
)""")
cur.execute("""
CREATE TABLE IF NOT EXISTS ml_state_versions (
    version INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT,
    weights TEXT,
    feature_names TEXT,
    fingerprint TEXT
)""")
cur.execute("""
CREATE TABLE IF NOT EXISTS token_ml_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    weights TEXT,
    feature_names TEXT
)""")
cur.execute("""
CREATE TABLE IF NOT EXISTS recovery_ops (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT,
    chain_id INTEGER,
    action TEXT,
    token_in TEXT,
    token_out TEXT,
    amount_in TEXT,
    amount_out TEXT,
    tx_hash TEXT,
    status TEXT,
    notes TEXT
)""")
conn.commit()

def now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

async def log_recovery_op(action: str, token_in: str, token_out: str, amount_in: int, amount_out: int,
                    tx_hash: Optional[str], status: str = "submitted", notes: str = ""):
    await _db_exec(
        "INSERT INTO recovery_ops (ts, chain_id, action, token_in, token_out, amount_in, amount_out, tx_hash, status, notes) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (now_ts(), CHAIN_ID, action, token_in, token_out, str(amount_in), str(amount_out), tx_hash or "", status, notes[:500])
    )

async def log_trade(strategy, cycle, dex, gross, net, gas_cost, size, fb_used, anomaly=0):
    await _db_exec(
        "INSERT INTO trades (timestamp,strategy,cycle,dex,gross_profit,net_profit,gas_cost,flashloan_size,used_flashbots,anomaly) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (now_ts(), strategy, json.dumps(cycle), dex, float(gross), float(net), float(gas_cost), int(size), int(int(USE_FLASHBOTS)), int(anomaly))
    )
    METRICS["trades_total"] += 1
    METRICS["last_strategy"] = strategy
    METRICS["last_dex"] = dex
    METRICS["last_cycle"] = cycle
    METRICS["last_gross_usd"] = float(gross)
    METRICS["last_net_usd"] = float(net)
    METRICS["last_ts"] = now_ts()

def bandit_key(route: List[str]) -> str:
    return "->".join(route)

async def bandit_get(key: str) -> Tuple[float, float]:
    row = await _db_fetchone("SELECT alpha,beta FROM bandit WHERE key=?", (key,))
    if row:
        return float(row[0]), float(row[1])
    await _db_exec("INSERT INTO bandit(key,alpha,beta) VALUES (?,?,?)", (key, 1.0, 1.0))
    return 1.0, 1.0

async def bandit_update(key: str, success: bool, reward: float = 0.0):
    a, b = await bandit_get(key)
    # Weight success by profit sign, or simply increment on success
    inc = 1.0 if reward >= 0 else 0.0
    a = a + inc if success else a
    b = b if success else b + 1.0
    await _db_exec("UPDATE bandit SET alpha=?,beta=? WHERE key=?", (a, b, key))

async def bandit_decay_all(gamma: float):
    rows = await _db_fetchall("SELECT key,alpha,beta FROM bandit")
    for k, a, b in rows:
        a2 = max(1.0, float(a) * gamma)
        b2 = max(1.0, float(b) * gamma)
        await _db_exec("UPDATE bandit SET alpha=?,beta=? WHERE key=?", (a2, b2, k))

# ============================ ABIs / Contracts ============================
UNIV3_QUOTER_ENV    = os.getenv("UNISWAP_V3_QUOTER", "").strip()
UNIV3_QUOTER        = Web3.to_checksum_address(UNIV3_QUOTER_ENV) if UNIV3_QUOTER_ENV else Web3.to_checksum_address("0x61fFE014bA17989E743c5F6cB21bF9697530B21e")

SUSHI_ROUTER_ADDR   = _env_addr("SUSHI_V2_ROUTER", "SUSHI_V3_ROUTER", default="0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506")
CAMELOT_ROUTER_ADDR = _env_addr("CAMELOT_V2_ROUTER", "CAMELOT_V3_ROUTER", default="0xC873fEcbD354f5A56E00E710B90Ef4201db2448d")
SUSHI_ROUTER        = Web3.to_checksum_address(SUSHI_ROUTER_ADDR)
CAMELOT_ROUTER      = Web3.to_checksum_address(CAMELOT_ROUTER_ADDR)

AAVE_DATA_PROVIDER_ADDR_ENV = os.getenv("AAVE_DATA_PROVIDER", "0x69FA688f1Dc47d4B5d8029D5a35FB7a54831065e").strip()

UNIV3_QUOTER_ABI = json.loads('[{"inputs":[{"internalType":"bytes","name":"path","type":"bytes"},{"internalType":"uint256","name":"amountIn","type":"uint256"}],"name":"quoteExactInput","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint160[]","name":"sqrtPriceX96AfterList","type":"uint160[]"},{"internalType":"uint32[]","name":"initializedTicksCrossedList","type":"uint32[]"},{"internalType":"uint256","name":"gasEstimate","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}]')
AGGREGATOR_ABI = json.loads('[{"inputs":[],"name":"latestRoundData","outputs":[{"internalType":"uint80","name":"roundId","type":"uint80"},{"internalType":"int256","name":"answer","type":"int256"},{"internalType":"uint256","name":"startedAt","type":"uint256"},{"internalType":"uint256","name":"updatedAt","type":"uint256"},{"internalType":"uint80","name":"answeredInRound","type":"uint80"}],"stateMutability":"view","type":"function"}]')
ERC20_ABI = json.loads('[{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"owner","type":"address"},{"name":"spender","type":"address"}],"name":"allowance","outputs":[{"type":"uint256"}],"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],"name":"approve","outputs":[{"type":"bool"}],"stateMutability":"nonpayable","type":"function"}]')
ERC20_SYMBOL_ABI = json.loads('[{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"}]')
ERC20_SYMBOL32_ABI = json.loads('[{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"}]')
V2_ROUTER_ABI = json.loads('[{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"}],"name":"getAmountsOut","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForTokensSupportingFeeOnTransferTokens","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountAMin","type":"uint256"},{"internalType":"uint256","name":"amountBMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"removeLiquidity","outputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"amountB","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}]')
EXECUTOR_ABI = json.loads('[{"inputs":[{"internalType":"uint8","name":"p","type":"uint8"},{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"bytes","name":"params","type":"bytes"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"requestFlashLoan","outputs":[],"stateMutability":"nonpayable","type":"function"}]')
EXECUTOR_MGMT_ABI = json.loads('[{"inputs":[{"internalType":"address","name":"token","type":"address"}],"name":"withdrawProfit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]')
# --- Power2 v14 pure payload helpers (on-chain encoder)
ARB_LIQ_HELPERS_ABI = json.loads(r"""
[
  {
    "inputs": [
      {
        "components": [
          { "internalType": "uint256",   "name":"version",       "type":"uint256" },
          { "internalType": "address[]", "name":"path",          "type":"address[]" },
          { "internalType": "uint256",   "name":"minOutput",     "type":"uint256" },
          { "internalType": "uint8",     "name":"dex",           "type":"uint8" },
          { "internalType": "uint256",   "name":"deadline",      "type":"uint256" },
          { "internalType": "uint24",    "name":"feeTier",       "type":"uint24" },
          { "internalType": "bytes",     "name":"uniV3Path",     "type":"bytes" },
          { "components": [
              { "components": [
                  { "internalType":"bytes32", "name":"poolId",        "type":"bytes32" },
                  { "internalType":"uint256", "name":"assetInIndex",  "type":"uint256" },
                  { "internalType":"uint256", "name":"assetOutIndex", "type":"uint256" },
                  { "internalType":"uint256", "name":"amount",        "type":"uint256" },
                  { "internalType":"bytes",   "name":"userData",      "type":"bytes" }
                ], "internalType":"struct IBalancerVault.BatchSwapStep[]", "name":"steps", "type":"tuple[]" },
              { "internalType":"address[]", "name":"assets", "type":"address[]" },
              { "internalType":"int256[]",  "name":"limits", "type":"int256[]" }
            ],
            "internalType":"struct Power.BalancerSwapParams",
            "name":"balancerParams",
            "type":"tuple"
          },
          { "components": [
              { "internalType":"address", "name":"pool", "type":"address" },
              { "internalType":"int128",  "name":"i",    "type":"int128" },
              { "internalType":"int128",  "name":"j",    "type":"int128" }
            ],
            "internalType":"struct Power.CurveSwapParams",
            "name":"curveParams",
            "type":"tuple"
          }
        ],
        "internalType":"struct Power.ArbitrageOpportunity",
        "name":"opp",
        "type":"tuple"
      }
    ],
    "name":"buildArbPayload",
    "outputs":[{ "internalType":"bytes", "name":"", "type":"bytes" }],
    "stateMutability":"pure",
    "type":"function"
  },
  {
    "inputs": [
      {
        "components": [
          { "internalType":"address","name":"collateralAsset","type":"address" },
          { "internalType":"address","name":"debtAsset","type":"address" },
          { "internalType":"address","name":"user","type":"address" },
          { "internalType":"uint256","name":"debtToCover","type":"uint256" },
          { "internalType":"address[]","name":"swapPath","type":"address[]" },
          { "internalType":"uint8","name":"dex","type":"uint8" },
          { "internalType":"uint256","name":"minProfit","type":"uint256" },
          { "internalType":"uint256","name":"deadline","type":"uint256" },
          { "internalType":"bytes","name":"uniV3Path","type":"bytes" }
        ],
        "internalType":"struct Power.LiquidationOpportunity",
        "name":"opp",
        "type":"tuple"
      }
    ],
    "name":"buildLiqPayload",
    "outputs":[{ "internalType":"bytes", "name":"", "type":"bytes" }],
    "stateMutability":"pure",
    "type":"function"
  }
]
""")

AAVE_POOL_EXT_ABI = json.loads('[{"inputs":[],"name":"getReservesList","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"address","name":"to","type":"address"}],"name":"withdraw","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"collateralAsset","type":"address"},{"internalType":"address","name":"debtAsset","type":"address"},{"internalType":"address","name":"user","type":"address"},{"internalType":"uint256","name":"debtToCover","type":"uint256"},{"internalType":"bool","name":"receiveAToken","type":"bool"}],"name":"liquidationCall","outputs":[],"stateMutability":"nonpayable","type":"function"}]')
AAVE_DATA_PROVIDER_ABI = json.loads('[{"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getReserveConfigurationData","outputs":[{"internalType":"uint256","name":"decimals","type":"uint256"},{"internalType":"uint256","name":"ltv","type":"uint256"},{"internalType":"uint256","name":"liquidationThreshold","type":"uint256"},{"internalType":"uint256","name":"liquidationBonus","type":"uint256"},{"internalType":"uint256","name":"reserveFactor","type":"uint256"},{"internalType":"bool","name":"usageAsCollateralEnabled","type":"bool"},{"internalType":"bool","name":"borrowingEnabled","type":"bool"},{"internalType":"bool","name":"stableBorrowRateEnabled","type":"bool"},{"internalType":"bool","name":"isActive","type":"bool"},{"internalType":"bool","name":"isFrozen","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getReserveTokensAddresses","outputs":[{"internalType":"address","name":"aTokenAddress","type":"address"},{"internalType":"address","name":"stableDebtTokenAddress","type":"address"},{"internalType":"address","name":"variableDebtTokenAddress","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getUserAccountData","outputs":[{"internalType":"uint256","name":"totalCollateralBase","type":"uint256"},{"internalType":"uint256","name":"totalDebtBase","type":"uint256"},{"internalType":"uint256","name":"availableBorrowsBase","type":"uint256"},{"internalType":"uint256","name":"currentLiquidationThreshold","type":"uint256"},{"internalType":"uint256","name":"ltv","type":"uint256"},{"internalType":"uint256","name":"healthFactor","type":"uint256"}],"stateMutability":"view","type":"function"}]')

# Contracts
aave_pool = w3.eth.contract(address=LIQ_POOL_ADDR, abi=AAVE_POOL_EXT_ABI)
aave_data_provider = None
if AAVE_DATA_PROVIDER_ADDR_ENV:
    try:
        aave_data_provider = w3.eth.contract(address=Web3.to_checksum_address(AAVE_DATA_PROVIDER_ADDR_ENV), abi=AAVE_DATA_PROVIDER_ABI)
    except Exception as e:
        logger.error("Could not initialize Aave Data Provider: %s", e)

uniswap_quoter = w3.eth.contract(address=UNIV3_QUOTER, abi=UNIV3_QUOTER_ABI)
sushi_router   = w3.eth.contract(address=SUSHI_ROUTER,   abi=V2_ROUTER_ABI)
camelot_router = w3.eth.contract(address=CAMELOT_ROUTER, abi=V2_ROUTER_ABI)
exec_c    = w3.eth.contract(address=ARB_EXECUTOR_ADDRESS, abi=(EXECUTOR_ABI + ARB_LIQ_HELPERS_ABI))
exec_mgmt = w3.eth.contract(address=ARB_EXECUTOR_ADDRESS, abi=EXECUTOR_MGMT_ABI)

# Across SpokePool binding
def _load_abi_file(path: str) -> Optional[List[Dict[str, Any]]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("ABI load failed for %s: %s", path, e)
        return None

ACROSS_SPOKEPOOL_ADDRESS: Optional[str] = None
across_spokepool = None
if ACROSS_SPOKEPOOL_ADDRESS_ENV:
    try:
        ACROSS_SPOKEPOOL_ADDRESS = Web3.to_checksum_address(ACROSS_SPOKEPOOL_ADDRESS_ENV)
        abi = _load_abi_file(ACROSS_SPOKEPOOL_ABI_PATH)
        if abi:
            across_spokepool = w3.eth.contract(address=ACROSS_SPOKEPOOL_ADDRESS, abi=abi)
        else:
            logger.warning("Across ABI not loaded; bridging will be disabled.")
    except Exception as e:
        logger.warning("Across SpokePool init failed: %s", e)

# ============================ Oracles & token baseline ============================
CHAINLINK_FEEDS = {
  "USDC": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3",
  "USDT": "0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7",
  "DAI" : "0xc5a5C42992dECbae36851359345FE25997F5C42d",
"WBTC": "0x6ce185860a4963106506C203335A2910413708e9",
  "WETH": "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"
}
# Optional: override feeds via env JSON to hotfix addresses without redeploys.
try:
    _feeds_override = json.loads(os.getenv("CHAINLINK_FEEDS_JSON", "") or "{}")
    for _sym, _addr in _feeds_override.items():
        if isinstance(_addr, str) and _addr.startswith("0x"):
            CHAINLINK_FEEDS[_sym.upper()] = _addr
except Exception:
    pass

oracles = {sym: w3.eth.contract(address=Web3.to_checksum_address(addr), abi=AGGREGATOR_ABI) for sym, addr in CHAINLINK_FEEDS.items()}
ORACLE_CACHE: Dict[str, Tuple[Decimal, float]] = {}
VOL_HIST: Dict[str, List[Decimal]] = {}

def get_oracle_price(symbol: str) -> Decimal:
    symbol = symbol.upper()
    now = time.time()
    cached = ORACLE_CACHE.get(symbol)
    if cached:
        px, ts = cached
        if (now - ts) < 10.0:
            return px
    
    if symbol == "USDC.E": symbol = "USDC"
    
    if symbol not in oracles:
        if "USD" in symbol: return Decimal("1.0")
        return cached[0] if cached else Decimal("0")

    try:
        _, answer, _, _, _ = oracles[symbol].functions.latestRoundData().call()
        px = Decimal(answer) / Decimal(1e8)
        ORACLE_CACHE[symbol] = (px, now)
        VOL_HIST.setdefault(symbol, []).append(px)
        if len(VOL_HIST[symbol]) > 200:
            VOL_HIST[symbol] = VOL_HIST[symbol][-200:]
        return px
    except Exception:
        return cached[0] if cached else Decimal("0")

# ============================ Telegram ============================
async def send_telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    session = await _ensure_http_session()
    try:
        await session.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"})
    except Exception as e:
        logger.warning("Telegram send failed: %s", e)


def pretty_timedelta(secs: int) -> str:
    m, s = divmod(int(max(0, secs)), 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    parts = []
    if d: parts.append(f"{d}d")
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    if s or not parts: parts.append(f"{s}s")
    return " ".join(parts)

async def telegram_heartbeat():
    hb = TELEGRAM_HEARTBEAT_MIN
    if hb <= 0:
        return
    await asyncio.sleep(random.uniform(2.0, 5.0))
    last = 0.0
    while not STOP_EVENT.is_set():
        now = time.time()
        if now - last < max(60, hb * 60):
            await asyncio.sleep(5)
            continue
        last = now
        uptime = pretty_timedelta(int(now - START_TIME))
        trades = METRICS.get("trades_total", 0)
        succ = METRICS.get("trades_success", 0)
        liqs = METRICS.get("liqs_success", 0)
        msg = (
            f"🫀 *Heartbeat*\n"
            f"WSS: {METRICS.get('wss','OFF')}\n"
            f"Uptime: {uptime}\n"
            f"Trades: {succ}/{trades} | Liqs: {liqs}\n"
            f"Last net: ${float(METRICS.get('last_net_usd') or 0):.2f}\n"
            f"Block: {METRICS.get('last_block')}"
        )
        await send_telegram(msg)

# ============================ Discovery & quoting ============================
TOKENS: Dict[str, Dict[str, Any]] = {}
BASELINE_TOKENS = [
    ("USDC", "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8", 6),
    ("USDT", "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", 6),
    ("DAI",  "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1", 18),
    ("WETH", "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", 18),
    ("WBTC", "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f", 8),
    ("ARB",  "0x912CE59144191C1204E64559FE8253a0e49E6548", 18),
]
BASELINE_ADDRS = {Web3.to_checksum_address(a) for _, a, _ in BASELINE_TOKENS}
BASELINE_TOKEN_INFO = {Web3.to_checksum_address(a): {"symbol": s, "decimals": d} for s, a, d in BASELINE_TOKENS}

def _is_baseline(addr: str) -> bool:
    try:
        return Web3.to_checksum_address(addr) in BASELINE_ADDRS
    except Exception:
        return False

def baseline_addr_for_symbol(sym: str) -> Optional[str]:
    sym = (sym or "").upper()
    for addr, info in BASELINE_TOKEN_INFO.items():
        if (info.get("symbol", "").upper()) == sym:
            return addr
    return None

UNIV3_ADJ: Dict[str, Set[str]] = {}
SUSHI_ADJ: Dict[str, Set[str]] = {}
_adjacency_warned = False

LIQ_SCORE: Dict[str, float] = {}
_LIQ_TS: float = 0.0

async def subgraph_query(url: str, query: str) -> Dict[str, Any]:
    session = await _ensure_http_session()
    backoff = 0.5
    for attempt in range(4):
        try:
            async with session.post(url, json={"query": query}) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    logger.warning("Subgraph query to %s failed with status %d", url, resp.status)
        except asyncio.TimeoutError:
            logger.warning("Subgraph query to %s timed out", url)
        except aiohttp.ClientError as e:
            logger.warning("Subgraph query to %s failed: %s", url, e)
        
        if attempt < 3:
             await asyncio.sleep(backoff)
             backoff *= 1.7
    return {}


SUBG_UNIV3 = "https://api.thegraph.com/subgraphs/name/ianlapham/arbitrum-minimal"
SUBG_SUSHI = "https://api.thegraph.com/subgraphs/name/sushiswap/exchange-arbitrum"
SUBG_BAL   = "https://api.thegraph.com/subgraphs/name/balancer-labs/balancer-v2-arbitrum"
SUBG_AAVE  = "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-arbitrum"

async def discover_univ3_adjacency(limit_pools: int = 800) -> None:
    try:
        q = f"""{{ pools(first:{limit_pools}, orderBy: totalValueLockedUSD, orderDirection: desc) {{
            token0 {{ id }} token1 {{ id }} feeTier totalValueLockedUSD
        }} }}"""
        data = await subgraph_query(SUBG_UNIV3, q)
        pools = (data.get("data", {}) or {}).get("pools", [])
        for p in pools:
            try:
                t0 = Web3.to_checksum_address(((p or {}).get("token0") or {}).get("id"))
                t1 = Web3.to_checksum_address(((p or {}).get("token1") or {}).get("id"))
            except Exception:
                continue
            UNIV3_ADJ.setdefault(t0, set()).add(t1)
            UNIV3_ADJ.setdefault(t1, set()).add(t0)
            try:
                tvl = float(p.get("totalValueLockedUSD") or 0.0)
                if tvl > 0:
                    LIQ_SCORE[t0] = LIQ_SCORE.get(t0, 0.0) + tvl
                    LIQ_SCORE[t1] = LIQ_SCORE.get(t1, 0.0) + tvl
            except Exception:
                pass
    except Exception:
        pass

async def discover_sushi_adjacency(limit_pairs: int = 800) -> None:
    try:
        q = f"""{{ pairs(first:{limit_pairs}, orderBy: reserveUSD, orderDirection: desc) {{
            token0 {{ id }} token1 {{ id }} reserveUSD
        }} }}"""
        data = await subgraph_query(SUBG_SUSHI, q)
        pairs = (data.get("data", {}) or {}).get("pairs", [])
        for p in pairs:
            try:
                t0 = Web3.to_checksum_address(((p or {}).get("token0") or {}).get("id"))
                t1 = Web3.to_checksum_address(((p or {}).get("token1") or {}).get("id"))
            except Exception:
                continue
            SUSHI_ADJ.setdefault(t0, set()).add(t1)
            SUSHI_ADJ.setdefault(t1, set()).add(t0)
            try:
                usd = float(p.get("reserveUSD") or 0.0)
                if usd > 0:
                    LIQ_SCORE[t0] = LIQ_SCORE.get(t0, 0.0) + usd
                    LIQ_SCORE[t1] = LIQ_SCORE.get(t1, 0.0) + usd
            except Exception:
                pass
    except Exception:
        pass

async def discover_balancer_tokens(limit_pools: int = 100) -> Dict[str, str]:
    tokens: Dict[str, str] = {}
    q = f"""{{ pools(first:{limit_pools}, orderBy: totalLiquidity, orderDirection: desc) {{ tokensList totalLiquidity }} }}"""
    try:
        data = await subgraph_query(SUBG_BAL, q)
        for p in data.get("data", {}).get("pools", []):
            tvl = float(p.get("totalLiquidity") or 0.0)
            for a in p.get("tokensList", []):
                addr = Web3.to_checksum_address(a)
                tokens[addr] = f"TKN{addr[-4:]}"
                if tvl > 0:
                    LIQ_SCORE[addr] = LIQ_SCORE.get(addr, 0.0) + tvl * 0.1
    except Exception:
        pass
    return tokens

async def compute_liquidity_scores():
    global _LIQ_TS
    try:
        LIQ_SCORE.clear()
        await discover_univ3_adjacency(limit_pools=800)
        await discover_sushi_adjacency(limit_pairs=800)
        await discover_balancer_tokens(limit_pools=200)
        _LIQ_TS = time.time()
        logger.info("Liquidity scoring updated for %s tokens", len(LIQ_SCORE))
    except Exception as e:
        logger.debug("compute_liquidity_scores error: %s", e)

# ============================ Quoting helpers ============================
QUOTE_SEMAPHORE = asyncio.Semaphore(4)
QUOTE_CACHE: Dict[Tuple[int, str, str, int], Tuple[Optional[str], Optional[int], Dict[str, int]]] = {}
_cached_block_number = 0
_block_cache_time = 0
_cached_chain_id: Optional[int] = None
_chain_id_cache_time = 0.0

def _on_new_block(bn: int):
    global _cached_block_number, _block_cache_time
    try:
        if bn > 0 and bn != _cached_block_number:
            _cached_block_number = bn
            _block_cache_time = time.time()
            try:
                METRICS["last_block"] = int(bn)
            except Exception:
                pass
            try:
                floor = _cached_block_number - 2
                for k in [k for k in list(QUOTE_CACHE.keys()) if k[0] < floor]:
                    QUOTE_CACHE.pop(k, None)
            except Exception:
                QUOTE_CACHE.clear()
            BLOCK_UPDATE_EVENT.set()
            BLOCK_UPDATE_EVENT.clear()
    except Exception:
        pass

async def get_cached_block_number() -> int:
    global _cached_block_number, _block_cache_time
    if _cached_block_number > 0:
        return _cached_block_number
    try:
        _cached_block_number = w3.eth.block_number
        _block_cache_time = time.time()
    except Exception:
        pass
    return _cached_block_number

async def get_cached_chain_id() -> int:
    global _cached_chain_id, _chain_id_cache_time
    now = time.time()
    if _cached_chain_id is None or now - _chain_id_cache_time > 86400:
        try:
            _cached_chain_id = int(w3.eth.chain_id)
            _chain_id_cache_time = now
        except Exception:
            if _cached_chain_id is None:
                _cached_chain_id = CHAIN_ID
    return _cached_chain_id if _cached_chain_id is not None else CHAIN_ID

async def purge_quote_cache(keep_last: int = 2):
    try:
        floor = await get_cached_block_number() - keep_last
    except Exception:
        floor = 0
    for k in [k for k in list(QUOTE_CACHE.keys()) if k[0] < floor]:
        QUOTE_CACHE.pop(k, None)

def _normalize_quoter_return(ret) -> Optional[int]:
    if ret is None:
        return None
    if isinstance(ret, int):
        return ret
    if isinstance(ret, (list, tuple)) and len(ret) >= 1:
        return int(ret[0])
    return None

async def quote_univ3_exact_in(tokens: List[str], fees: List[int], amount_in: int) -> Optional[int]:
    path = encode_univ3_path(tokens, fees)
    async with QUOTE_SEMAPHORE:
        try:
            def _call():
                try:
                    raw = uniswap_quoter.functions.quoteExactInput(path, int(amount_in)).call()
                    out = _normalize_quoter_return(raw)
                    return int(out) if (out is not None and out > 0) else None
                except Exception:
                    return None
            return await asyncio.to_thread(_call)
        except Exception:
            return None

async def v2_quote_path(router, path: List[str], amount_in: int) -> Optional[int]:
    def _call():
        try:
            amounts = router.functions.getAmountsOut(int(amount_in), [Web3.to_checksum_address(p) for p in path]).call()
            return int(amounts[-1]) if amounts and len(amounts) >= 2 else None
        except Exception:
            return None
    async with QUOTE_SEMAPHORE:
        return await asyncio.to_thread(_call)

async def quote_sushi_pair(token_in: str, token_out: str, amount_in: int) -> Optional[int]:
    return await v2_quote_path(sushi_router, [token_in, token_out], amount_in)

async def quote_camelot_pair(token_in: str, token_out: str, amount_in: int) -> Optional[int]:
    return await v2_quote_path(camelot_router, [token_in, token_out], amount_in)

async def fetch_best_quote(token_in: str, token_out: str, amount_in: int) -> Tuple[Optional[str], Optional[int], Dict[str, int]]:
    blk = await get_cached_block_number()
    key = (blk, token_in, token_out, amount_in)
    if key in QUOTE_CACHE:
        return QUOTE_CACHE[key]
    
    fees_try = [100, 500, 3000, 10000]
    uni_best: Optional[int] = None

    def _adj_ok(adj: Dict[str, Set[str]], u: str, v: str) -> bool:
        s = adj.get(u, set())
        if v in s: return True
        s2 = adj.get(v, set())
        if u in s2: return True
        return _is_baseline(u) or _is_baseline(v)

    # Concurrently fetch quotes from all DEXes and all UniV3 fee tiers
    tasks = []
    if not UNIV3_ADJ or _adj_ok(UNIV3_ADJ, token_in, token_out):
        for f in fees_try:
            tasks.append(asyncio.create_task(quote_univ3_exact_in([token_in, token_out], [f], amount_in)))
    
    if not USE_UNIV3_ONLY:
        if not SUSHI_ADJ or _adj_ok(SUSHI_ADJ, token_in, token_out):
            tasks.append(asyncio.create_task(quote_sushi_pair(token_in, token_out, amount_in)))
        tasks.append(asyncio.create_task(quote_camelot_pair(token_in, token_out, amount_in)))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)

    candidates: Dict[str, int] = {}
    uni_quotes = [q for q in results[:len(fees_try)] if isinstance(q, int) and q > 0]
    if uni_quotes:
        candidates["UNISWAP_V3"] = max(uni_quotes)
    
    if not USE_UNIV3_ONLY:
        sushi_res_idx = len(fees_try)
        if sushi_res_idx < len(results) and isinstance(results[sushi_res_idx], int) and results[sushi_res_idx] > 0:
            candidates["SUSHI"] = results[sushi_res_idx]
        
        camelot_res_idx = sushi_res_idx + 1
        if camelot_res_idx < len(results) and isinstance(results[camelot_res_idx], int) and results[camelot_res_idx] > 0:
            candidates["CAMELOT"] = results[camelot_res_idx]
            
    if not candidates:
        QUOTE_CACHE[key] = (None, None, {})
        return QUOTE_CACHE[key]
    
    best_dex = max(candidates, key=candidates.get)
    QUOTE_CACHE[key] = (best_dex, candidates[best_dex], candidates)
    return QUOTE_CACHE[key]

def sample_size_for(token: str, target_usd: Decimal = Decimal("5000")) -> int:
    info = TOKENS.get(token, BASELINE_TOKEN_INFO.get(token, {"symbol":"WETH","decimals":18}))
    sym, dec = info["symbol"], int(info["decimals"])
    px = get_oracle_price(sym) or get_oracle_price("WETH") or Decimal("1")
    units = (target_usd / px) * Decimal(10**dec)
    # clamp to avoid absurd quotes on low-decimals
    return int(max(10**(dec-6), min(units, Decimal(10**dec) * Decimal(10))))  # 10 tokens cap

async def build_graph(_: int) -> Dict[str, Dict[str, Decimal]]:
    graph = {}
    addrs_full = [a for a in TOKENS.keys() if a not in BLACKLIST]
    sorted_tokens = sorted(addrs_full, key=lambda a: LIQ_SCORE.get(a, 0.0), reverse=True) if LIQ_SCORE else list(addrs_full)
    addrs = sorted_tokens[:max(5, min(MAX_TOKENS, len(sorted_tokens)))]
    tasks, pairs = [], []
    for u in addrs:
        amt_in = sample_size_for(u)
        for v in addrs:
            if u == v: continue
            
            # Adjacency filter - re-add from original logic
            any_adj = bool(UNIV3_ADJ) or bool(SUSHI_ADJ)
            if any_adj:
                ok = False
                if UNIV3_ADJ and (v in UNIV3_ADJ.get(u, set()) or u in UNIV3_ADJ.get(v, set())):
                    ok = True
                if not ok and SUSHI_ADJ and (v in SUSHI_ADJ.get(u, set()) or u in SUSHI_ADJ.get(v, set())):
                    ok = True
                if not ok and (_is_baseline(u) or _is_baseline(v)):
                    ok = True
                if not ok:
                    continue

            tasks.append(asyncio.create_task(fetch_best_quote(u, v, amt_in)))
            pairs.append((u, v, amt_in))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for (u, v, amt_in), r in zip(pairs, results):
        if isinstance(r, Exception) or not r: continue
        _, best_out, _ = r
        if best_out:
            dec_u = int(TOKENS.get(u, BASELINE_TOKEN_INFO.get(u, {"decimals":18}))["decimals"])
            dec_v = int(TOKENS.get(v, BASELINE_TOKEN_INFO.get(v, {"decimals":18}))["decimals"])
            rate = (Decimal(best_out)/Decimal(10**dec_v)) / (Decimal(amt_in)/Decimal(10**dec_u))
            graph.setdefault(u, {})[v] = rate * (Decimal("1.0") - SLIPPAGE)
    return graph

# ============================ Graph search & scoring ============================
def enumerate_cycles(graph: Dict[str, Dict[str, Decimal]], max_hops: int) -> List[List[str]]:
    cycles: List[List[str]] = []
    nodes = list(graph.keys())
    def dfs(start, node, path, depth):
        if depth > max_hops:
            return
        for nxt in graph.get(node, {}):
            if nxt == start and len(path) >= 3:
                if all((x not in BLACKLIST) for x in path + [nxt]):
                    cycles.append(path + [nxt])
            elif nxt not in path and nxt not in BLACKLIST:
                dfs(start, nxt, path + [nxt], depth + 1)
    for n in nodes:
        if n in BLACKLIST:
            continue
        dfs(n, n, [n], 0)
    return cycles

def _johnson_simple_cycles(graph: Dict[str, Dict[str, Decimal]], max_hops: int) -> List[List[str]]:
    index = {v: i for i, v in enumerate(graph.keys())}
    vertices = list(graph.keys())
    blocked: Set[str] = set()
    B: Dict[str, Set[str]] = {v: set() for v in vertices}
    stack: List[str] = []
    cycles: List[List[str]] = []

    def unblock(u: str):
        if u in blocked:
            blocked.remove(u)
            for w in list(B[u]):
                B[u].remove(w)
                unblock(w)

    def circuit(v: str, s: str, depth: int = 0) -> bool:
        f = False
        blocked.add(v)
        stack.append(v)
        for w in graph.get(v, {}):
            if depth + 1 > max_hops:
                continue
            if w == s and len(stack) >= 2:
                cycles.append(stack[:] + [s])
                f = True
            elif w not in blocked:
                if circuit(w, s, depth + 1):
                    f = True
        if f:
            unblock(v)
        else:
            for w in graph.get(v, {}):
                B[w].add(v)
        stack.pop()
        return f

    for s in vertices:
        circuit(s, s, 0)
        blocked = set()
        B = {v: set() for v in vertices}
    uniq = []
    seen = set()
    for c in cycles:
        if len(c) < 3:
            continue
        core = c[:-1]
        m = min(range(len(core)), key=lambda i: core[i])
        rot = tuple(core[m:] + core[:m] + [core[m]])
        if rot not in seen:
            seen.add(rot)
            uniq.append(c)
    return uniq

def bellman_ford_arbitrage(graph: Dict[str, Dict[str, Decimal]]) -> List[List[str]]:
    nodes = list(graph.keys()); idx = {n: i for i, n in enumerate(nodes)}
    edges: List[Tuple[int, int, float]] = []
    for u in nodes:
        for v, r in graph.get(u, {}).items():
            if r > 0:
                w = -math.log(float(r))
                edges.append((idx[u], idx[v], w))
    dist = [0.0] * len(nodes)
    prev = [-1] * len(nodes)
    cyc_paths: List[List[str]] = []
    for _ in range(len(nodes) - 1):
        updated = False
        for u, v, w in edges:
            if dist[u] + w < dist[v]:
                dist[v] = dist[u] + w
                prev[v] = u
                updated = True
        if not updated:
            break
    for u, v, w in edges:
        if dist[u] + w < dist[v]:
            x = v
            for _ in range(len(nodes)):
                x = prev[x]
            cycle = []
            cur = x
            while True:
                cycle.append(nodes[cur])
                cur = prev[cur]
                if cur == x or cur == -1:
                    break
            cyc_paths.append(list(reversed(cycle)))
    return cyc_paths

def dijkstra_best_path(graph: Dict[str, Dict[str, Decimal]], src: str) -> Dict[str, Tuple[float, List[str]]]:
    import heapq
    dist: Dict[str, float] = {src: 0.0}
    path: Dict[str, List[str]] = {src: [src]}
    pq = [(0.0, src)]
    while pq:
        d, u = heapq.heappop(pq)
        if d > dist.get(u, 1e18):
            continue
        for v, r in graph.get(u, {}).items():
            if r <= 0:
                continue
            w = -math.log(float(r))
            nd = d + w
            if nd < dist.get(v, 1e18):
                dist[v] = nd
                path[v] = path[u] + [v]
                heapq.heappush(pq, (nd, v))
    return {k: (math.exp(-d), path[k]) for k, d in dist.items()}

def refine_cycle_with_dijkstra(graph, cycle, max_hops, min_improve: float = 0.003, max_extra_hops_per_edge: int = 2) -> List[str]:
    if not cycle or len(cycle) < 3:
        return cycle
    start = cycle[0]
    refined: List[str] = [start]
    current_hops = 0
    for i in range(len(cycle) - 1):
        u, v = cycle[i], cycle[i+1]
        direct = graph.get(u, {}).get(v)
        if direct is None or direct <= 0:
            if refined[-1] != u:
                refined.append(u)
            refined.append(v)
            current_hops += 1
            continue
        best_from_u = dijkstra_best_path(graph, u)
        best_uv = best_from_u.get(v)
        if not best_uv:
            if refined[-1] != u:
                refined.append(u)
            refined.append(v)
            current_hops += 1
            continue
        rate, path = best_uv
        extra_hops = max(0, len(path) - 2)
        if extra_hops > max_extra_hops_per_edge:
            path = [u, v]
            extra_hops = 0
        if rate <= float(direct) * (1.0 + min_improve) or (current_hops + 1 + extra_hops) > max_hops:
            path = [u, v]
            extra_hops = 0
        if refined[-1] != u:
            refined.append(u)
        for node in path[1:]:
            refined.append(node)
        current_hops += 1 + extra_hops
        if current_hops > max_hops:
            if refined[-1] != start:
                refined[-1] = start
            break
    if refined[-1] != start:
        refined.append(start)
    if len(refined) - 1 > max_hops:
        return cycle
    return refined

def profit_usd(token: str, token_delta: Decimal) -> Decimal:
    dec = TOKENS.get(token, BASELINE_TOKEN_INFO.get(token, {"decimals": 18}))["decimals"]
    sym = TOKENS.get(token, BASELINE_TOKEN_INFO.get(token, {"symbol": "WETH"}))["symbol"]
    px  = get_oracle_price(sym) if sym in CHAINLINK_FEEDS else get_oracle_price("WETH")
    return (token_delta / Decimal(10**dec)) * (px or Decimal("0"))

def usd_notional(token: str, amount: int) -> Decimal:
    try:
        dec = TOKENS.get(token, BASELINE_TOKEN_INFO.get(token, {"decimals": 18}))["decimals"]
        sym = TOKENS.get(token, BASELINE_TOKEN_INFO.get(token, {"symbol": "WETH"}))["symbol"]
    except KeyError:
        logger.warning("Token %s not found in local registry for usd_notional", token)
        return Decimal(0)
    px  = get_oracle_price(sym)
    return (Decimal(amount) / Decimal(10**dec)) * (px or Decimal("0"))

def simulate_cycle_profit_tokens(graph: Dict[str, Dict[str, Decimal]], cycle: List[str], amount_in: int) -> Optional[Decimal]:
    amt = Decimal(amount_in)
    for i in range(len(cycle) - 1):
        u, v = cycle[i], cycle[i+1]
        rate = graph.get(u, {}).get(v)
        if not rate:
            return None
        amt *= rate
    return amt - Decimal(amount_in)

def oracle_divergence_ok(cycle: List[str], graph: Dict[str, Dict[str, Decimal]]) -> bool:
    prod = Decimal("1.0")
    for i in range(len(cycle) - 1):
        u, v = cycle[i], cycle[i+1]
        r = graph.get(u, {}).get(v)
        if not r:
            return False
        prod *= (r / (Decimal("1.0") - SLIPPAGE))
    return abs(prod - Decimal("1.0")) <= ORACLE_DIVERGENCE

def dynamic_slippage_for_trade(base_token: str, size_tokens: int, hops: int) -> Decimal:
    size_usd = usd_notional(base_token, size_tokens)
    hop_bump = max(0, hops - 1) * SLIPPAGE_HOP_BUMP
    steps = int((size_usd / SLIPPAGE_SIZE_STEP_USD).to_integral_value(rounding=ROUND_FLOOR))
    steps = min(steps, SLIPPAGE_SIZE_MAX_STEPS)
    size_bump = steps * SLIPPAGE_SIZE_STEP_BUMP
    total = SLIPPAGE + hop_bump + size_bump
    if total > SLIPPAGE_MAX:
        total = SLIPPAGE_MAX
    if total < SLIPPAGE:
        total = SLIPPAGE
    return total

# ============================ Allowance & balances ============================
def erc20_balance(token: str, owner: str) -> int:
    try:
        c = w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)
        return int(c.functions.balanceOf(Web3.to_checksum_address(owner)).call())
    except Exception:
        return 0

def erc20_allowance(token: str, owner: str, spender: str) -> int:
    try:
        c = w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)
        return int(c.functions.allowance(Web3.to_checksum_address(owner), Web3.to_checksum_address(spender)).call())
    except Exception:
        return 0

# ============================ Nonce / TX opts ============================
class NonceMgr:
    def __init__(self, w3, addr):
        self.w3 = w3
        self.addr = addr
        self.nonce: Optional[int] = None
        self.lock = asyncio.Lock()
    async def _chain_pending(self) -> int:
        def _get():
            try:
                return self.w3.eth.get_transaction_count(self.addr, "pending")
            except Exception:
                return self.w3.eth.get_transaction_count(self.addr)
        return await asyncio.to_thread(_get)
    async def next(self) -> int:
        async with self.lock:
            if self.nonce is None:
                self.nonce = await self._chain_pending()
            else:
                current_chain_nonce = await self._chain_pending()
                if current_chain_nonce > self.nonce:
                    logger.warning("Nonce manager detected external transaction; resetting nonce to %d", current_chain_nonce)
                    self.nonce = current_chain_nonce
                else:
                    self.nonce += 1
            return self.nonce
    async def bump_on_failure(self):
        async with self.lock:
            self.nonce = None

NONCE = NonceMgr(w3, ACCOUNT_ADDRESS or "0x0000000000000000000000000000000000000000")

def _chain_uses_legacy_gas(cid: int) -> bool:
    return cid in (42161, 421614)

async def tx_opts_base_async(fast_gas: bool = False) -> Dict[str, Any]:
    if not ACCOUNT or not ACCOUNT_ADDRESS:
        raise RuntimeError("PRIVATE_KEY not configured. Cannot build tx opts.")
    chain_id = await get_cached_chain_id()
    d: Dict[str, Any] = {
        "from": ACCOUNT_ADDRESS,
        "nonce": await NONCE.next(),
        "gas": TX_GAS_LIMIT,
        "chainId": chain_id,
    }
    if _chain_uses_legacy_gas(chain_id):
        d["gasPrice"] = int(get_cached_gas_price() or 0) if fast_gas else int(predict_gas_price_wei(fast=False) or 0)
    else:
        max_fee, tip = _fee_history_suggested()
        if max_fee is None or tip is None:
            max_fee = (int(get_cached_gas_price() or 0) if (fast_gas or MAX_FEE_PER_GAS_GWEI == 0)
                       else w3.to_wei(float(MAX_FEE_PER_GAS_GWEI), "gwei"))
            tip = w3.to_wei(float(MAX_PRIORITY_FEE_GWEI), "gwei")
        d["maxFeePerGas"] = int(max_fee)
        d["maxPriorityFeePerGas"] = int(tip)
    return d

# ============================ Simulate / send / monitor ============================
def simulate_tx(tx: Dict[str, Any]) -> bool:
    call_dict = {k: v for k, v in tx.items() if k in ("from", "to", "data", "value")}
    try:
        w3.eth.call(call_dict); return True
    except Exception as e:
        logger.debug("Simulate eth_call failed: %s. Trying estimate_gas.", e)
        try:
            gas_est = w3.eth.estimate_gas(call_dict)
            logger.debug("Simulate estimate_gas OK, est: %d", gas_est)
            return gas_est <= tx.get("gas", TX_GAS_LIMIT)
        except Exception as e2:
            logger.warning("Simulate failed on both call and estimate_gas: %s", e2)
            return False

async def send_tx_async(tx: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    if not ACCOUNT:
        logger.error("No PRIVATE_KEY configured — refusing to send transaction.")
        return False, None
    try:
        def _sign():
            return w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
        signed = await asyncio.to_thread(_sign)
        raw = signed.rawTransaction
        if USE_FLASHBOTS:
            payload = {
                "jsonrpc": "2.0", "id": 1, "method": "eth_sendPrivateTransaction",
                "params": [{"tx": raw.hex(), "maxBlockNumber": hex(w3.eth.block_number + 5)}]
            }
            try:
                r = await asyncio.to_thread(lambda: requests.post(FLASHBOTS_RPC, json=payload, timeout=8).json())
            except Exception as e:
                logger.warning("Flashbots RPC error: %s", e)
                return False, None
            if "error" in r or not r.get("result"):
                logger.warning("Flashbots error: %s — aborting to avoid public exposure", r.get("error"))
                return False, None
            return True, r.get("result")
        def _send():
            return w3.eth.send_raw_transaction(raw).hex()
        h = await asyncio.to_thread(_send)
        return True, h
    except Exception as e:
        logger.error("TX send error: %s", e)
        if "nonce too low" in str(e).lower() or "replacement transaction underpriced" in str(e).lower():
            await NONCE.bump_on_failure()
            logger.info("Nonce-related error detected, manager state was reset.")
        return False, None

async def wait_receipt(txh: str, timeout: int = 180) -> Optional[dict]:
    try:
        start = time.time()
        while time.time() - start < timeout:
            try:
                rcpt = await asyncio.to_thread(lambda: w3.eth.get_transaction_receipt(txh))
                if rcpt:
                    return rcpt
            except Exception:
                pass
            await asyncio.sleep(2)
    except Exception:
        pass
    return None

async def monitor_tx(txh: Optional[str], kind: str, meta: Dict[str, Any]):
    if not txh:
        return
    try:
        await send_telegram(f"🔍 Tx submitted: `{txh}` — monitoring…")
    except Exception:
        pass
    rcpt = await wait_receipt(txh, timeout=180)

    if rcpt:
        status = int(rcpt.get("status", 0) or 0)
        block_no = int(rcpt.get("blockNumber", 0) or 0)
        try:
            token = str(meta.get("token") or "N/A")
            amount = meta.get("amount") or 0
            netu = float(meta.get("net_usd") or 0.0)
            path = meta.get("path") or ""
            dexes = meta.get("dexes") or ""
            msg = (
                ("✅" if status == 1 else "❌") + f" *{kind.capitalize()} Executed*\n"
                f"Token: {token}\n"
                f"Amount: {amount}\n"
                f"Profit: ${netu:.2f}\n"
                f"tx: `{txh}`\n"
                f"block: {block_no}"
            )
            extra = []
            if path: extra.append(f"Path: {path}")
            if dexes: extra.append(f"DEXs: {dexes}")
            if extra: msg += "\n" + "\n".join(extra)
            await send_telegram(msg)
        except Exception:
            pass
        
        if status == 1:
            if kind.lower() == "arb": METRICS["trades_success"] += 1
            if kind.lower() == "liquidation": METRICS["liqs_success"] += 1
    else:
        await send_telegram(f"⏳ {kind.capitalize()} tx not mined in time: `{txh}`")

# ============================ Payload encoding (Power2 v14) ============================
ARB_SELECTOR_HEX = "0x260751a8"  # for offline fallback
LIQ_SELECTOR_HEX = "0x64917a78"

DexTag = { "UNISWAP_V3":0, "SUSHI":1, "CAMELOT":2, "BALANCER":3, "CURVE":4 }
FlashloanProvider = { "AAVE":0, "BALANCER":1, "UNISWAP":2, "DODO":3 }

def _cs(a: str) -> str:
    return Web3.to_checksum_address(a)

def encode_univ3_path(tokens: List[str], fees: List[int]) -> bytes:
    assert len(tokens) >= 2 and len(fees) == len(tokens) - 1
    b = b""
    for i in range(len(tokens) - 1):
        b += bytes.fromhex(_cs(tokens[i])[2:].rjust(40, "0"))
        b += int(fees[i]).to_bytes(3, "big")
    b += bytes.fromhex(_cs(tokens[-1])[2:].rjust(40, "0"))
    return b

def _arb_struct_v14(path: List[str], min_output: int, dex_name: str, fees: List[int], deadline: int) -> dict:
    fee_tier = int(fees[0]) if fees else 3000
    uni_v3_path = encode_univ3_path(path, fees) if dex_name == "UNISWAP_V3" else b""
    return {
        "version": 14,
        "path":    [_cs(p) for p in path],
        "minOutput": int(min_output),
        "dex":       int(DexTag[dex_name]),
        "deadline":  int(deadline),
        "feeTier":   int(fee_tier),
        "uniV3Path": uni_v3_path,
        "balancerParams": {
            "steps":  [],           # IBalancerVault.BatchSwapStep[]
            "assets": [],           # address[]
            "limits": []            # int256[]
        },
        "curveParams": {
            "pool": "0x0000000000000000000000000000000000000000",
            "i": 0,
            "j": 0
        }
    }

def encode_arbitrage_payload(path: List[str], min_output: int, dex_name: str,
                             fees: List[int], deadline: int) -> bytes:
    """
    Preferred: call Power2.buildArbPayload() (pure) to get abi.encodePacked(ARB_SELECTOR, abi.encode(opp)).
    Fallback: local abi-encode with the exact tuple signature used by Power2 v14.
    """
    opp = _arb_struct_v14(path, min_output, dex_name, fees, deadline)

    # Try the on-chain pure helper first (zero gas via eth_call)
    try:
        return exec_c.functions.buildArbPayload(opp).call()
    except Exception:
        pass  # fall back to local ABI pack below

    # Local ABI fallback
    tuple_type = "(uint256,address[],uint256,uint8,uint256,uint24,bytes,((bytes32,uint256,uint256,uint256,bytes)[],address[],int256[]),(address,int128,int128))"
    packed = abi_encode([tuple_type], [(
        int(opp["version"]),
        opp["path"],
        int(opp["minOutput"]),
        int(opp["dex"]),
        int(opp["deadline"]),
        int(opp["feeTier"]),
        opp["uniV3Path"],
        ([], [], []),  # BalancerSwapParams
        (_cs(opp["curveParams"]["pool"]), 0, 0)  # CurveSwapParams
    )])
    return bytes.fromhex(ARB_SELECTOR_HEX[2:]) + packed

def encode_liquidation_payload(collateral: str, debt: str, user: str, debt_to_cover: int,
                               swap_path: List[str], dex_name: str, min_profit: int,
                               deadline: int, uni_v3_path: Optional[bytes] = None) -> bytes:
    opp = {
        "collateralAsset": _cs(collateral),
        "debtAsset":       _cs(debt),
        "user":            _cs(user),
        "debtToCover":     int(debt_to_cover),
        "swapPath":        [_cs(p) for p in (swap_path or [])],
        "dex":             int(DexTag[dex_name]),
        "minProfit":       int(min_profit),
        "deadline":        int(deadline),
        "uniV3Path":       bytes(uni_v3_path or b"")
    }
    try:
        return exec_c.functions.buildLiqPayload(opp).call()
    except Exception:
        pass
    tuple_type = "(address,address,address,uint256,address[],uint8,uint256,uint256,bytes)"
    packed = abi_encode([tuple_type], [(
        opp["collateralAsset"], opp["debtAsset"], opp["user"], opp["debtToCover"],
        opp["swapPath"], opp["dex"], opp["minProfit"], opp["deadline"], opp["uniV3Path"]
    )])
    return bytes.fromhex(LIQ_SELECTOR_HEX[2:]) + packed

async def choose_univ3_fees_for_path(tokens: List[str], amount_in: int) -> Tuple[Optional[List[int]], int]:
    if not tokens or len(tokens) < 2:
        return None, 0
    fees: List[int] = []
    last_out = int(amount_in)
    for i in range(len(tokens) - 1):
        u, v = tokens[i], tokens[i+1]
        picked_fee, best_out = None, -1
        for f in [100, 500, 3000, 10000]:
            q = await quote_univ3_exact_in([u, v], [f], last_out)
            if q is not None and q > best_out:
                best_out, picked_fee = q, f
        if picked_fee is None or best_out <= 0:
            return None, 0
        fees.append(picked_fee)
        last_out = best_out
    return fees, last_out

# ============================ Redemption & swaps ============================
UINT_MAX = (1 << 256) - 1
UNI_V2_PAIR_ABI = json.loads('[{"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getReserves","outputs":[{"internalType":"uint112","name":"_reserve0","type":"uint112"},{"internalType":"uint112","name":"_reserve1","type":"uint112"},{"internalType":"uint32","name":"_blockTimestampLast","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]')
ERC4626_ABI_MIN = json.loads('[{"inputs":[],"name":"asset","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"shares","type":"uint256"},{"internalType":"address","name":"receiver","type":"address"},{"internalType":"address","name":"owner","type":"address"}],"name":"redeem","outputs":[{"internalType":"uint256","name":"assets","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}]')
AAVE_ATOKEN_ABI_MIN = json.loads('[{"inputs":[],"name":"UNDERLYING_ASSET_ADDRESS","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]')

class TokenKind:
    BASE = "BASE"
    ERC20 = "ERC20"
    ERC4626 = "ERC4626"
    AAVE_ATOKEN = "AAVE_ATOKEN"
    UNI_V2_LP = "UNI_V2_LP"
    UNKNOWN = "UNKNOWN"

def classify_token(token: str) -> Tuple[str, Dict[str, Any]]:
    addr = Web3.to_checksum_address(token)
    if _is_baseline(addr):
        return TokenKind.BASE, {}
    try:
        v = w3.eth.contract(address=addr, abi=ERC4626_ABI_MIN)
        underlying = v.functions.asset().call()
        if Web3.is_address(underlying):
            return TokenKind.ERC4626, {"underlying": Web3.to_checksum_address(underlying)}
    except Exception:
        pass
    try:
        c = w3.eth.contract(address=addr, abi=AAVE_ATOKEN_ABI_MIN)
        underlying = c.functions.UNDERLYING_ASSET_ADDRESS().call()
        if Web3.is_address(underlying):
            return TokenKind.AAVE_ATOKEN, {"underlying": Web3.to_checksum_address(underlying)}
    except Exception:
        pass
    try:
        p = w3.eth.contract(address=addr, abi=UNI_V2_PAIR_ABI)
        t0 = p.functions.token0().call()
        t1 = p.functions.token1().call()
        if Web3.is_address(t0) and Web3.is_address(t1):
            return TokenKind.UNI_V2_LP, {"token0": Web3.to_checksum_address(t0), "token1": Web3.to_checksum_address(t1)}
    except Exception:
        pass
    return TokenKind.ERC20, {}

async def build_v2_swap_tx(router, amount_in: int, min_out: int, path: List[str], to: str, deadline: int) -> Tuple[Optional[Dict[str, Any]], str]:
    opts = await tx_opts_base_async(fast_gas=False)
    try:
        tx1 = router.functions.swapExactTokensForTokensSupportingFeeOnTransferTokens(
            int(amount_in), int(min_out), [Web3.to_checksum_address(p) for p in path],
            Web3.to_checksum_address(to), int(deadline)
        ).build_transaction(opts)
        if simulate_tx(tx1):
            return tx1, "v2.swapSupporting"
    except Exception:
        pass
    try:
        tx2 = router.functions.swapExactTokensForTokens(
            int(amount_in), int(min_out), [Web3.to_checksum_address(p) for p in path],
            Web3.to_checksum_address(to), int(deadline)
        ).build_transaction(opts)
        if simulate_tx(tx2):
            return tx2, "v2.swap"
    except Exception:
        pass
    return None, "none"

WETH_MIN_ABI = json.loads('[{"inputs":[{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"withdraw","outputs":[],"stateMutability":"nonpayable","type":"function"}]')

async def unwrap_weth_if_configured(target_sym: str) -> None:
    if not CONSOLIDATE_UNWRAP_WETH:
        return
    if (target_sym or "").upper() != "WETH":
        return
    try:
        weth_addr = baseline_addr_for_symbol("WETH")
        if not weth_addr:
            return
        bal = erc20_balance(weth_addr, ACCOUNT_ADDRESS)
        if bal <= 0:
            return
        weth_c = w3.eth.contract(address=Web3.to_checksum_address(weth_addr), abi=WETH_MIN_ABI)
        tx = weth_c.functions.withdraw(int(bal)).build_transaction(await tx_opts_base_async(fast_gas=False))
        if not simulate_tx(tx):
            return
        ok, txh = await send_tx_async(tx)
        await log_recovery_op("weth.withdraw", weth_addr, "ETH", bal, 0, txh, "submitted")
        if ok and txh:
            rcpt = await wait_receipt(txh, 180)
            await monitor_tx(txh, "unwrap", {"token": weth_addr, "amount": int(bal), "gross_usd": float(usd_notional(weth_addr, bal)), "net_usd": 0})
    except Exception as e:
        logger.debug("unwrap_weth_if_configured error: %s", e)

async def redeem_erc4626(token: str, amount: int) -> Tuple[bool, Optional[str], Optional[str], Optional[int]]:
    try:
        v = w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC4626_ABI_MIN)
        underlying = v.functions.asset().call()
        tx = v.functions.redeem(int(amount), Web3.to_checksum_address(ACCOUNT_ADDRESS), Web3.to_checksum_address(ACCOUNT_ADDRESS)).build_transaction(await tx_opts_base_async(fast_gas=False))
        if not simulate_tx(tx):
            logger.debug("ERC4626 redeem simulate failed token=%s", token)
            return False, None, None, None
        ok, txh = await send_tx_async(tx)
        await log_recovery_op("erc4626.redeem", token, underlying, amount, 0, txh, "submitted")
        if ok and txh:
            rcpt = await wait_receipt(txh, 180)
            await monitor_tx(txh, "redeem", {"token": token, "amount": amount, "gross_usd": 0, "net_usd": 0})
            if not rcpt or int(rcpt.get("status",0)) != 1:
                return False, txh, None, None
            return True, txh, Web3.to_checksum_address(underlying), None
        return False, None, None, None
    except Exception as e:
        logger.debug("redeem_erc4626 error: %s", e)
        return False, None, None, None

async def redeem_aave_atoken(a_token: str, amount: int) -> Tuple[bool, Optional[str], Optional[str], Optional[int]]:
    try:
        c = w3.eth.contract(address=Web3.to_checksum_address(a_token), abi=AAVE_ATOKEN_ABI_MIN)
        underlying = c.functions.UNDERLYING_ASSET_ADDRESS().call()
        if not underlying: return False, None, None, None

        tx = aave_pool.functions.withdraw(Web3.to_checksum_address(underlying), int(amount), Web3.to_checksum_address(ACCOUNT_ADDRESS)).build_transaction(await tx_opts_base_async(fast_gas=False))
        if not simulate_tx(tx):
            logger.debug("Aave withdraw simulate failed aToken=%s", a_token)
            return False, None, None, None
        ok, txh = await send_tx_async(tx)
        await log_recovery_op("aave.withdraw", a_token, underlying, amount, 0, txh, "submitted")
        if ok and txh:
            rcpt = await wait_receipt(txh, 180)
            await monitor_tx(txh, "redeem", {"token": a_token, "amount": amount, "gross_usd": 0, "net_usd": 0})
            if not rcpt or int(rcpt.get("status",0)) != 1:
                return False, txh, None, None
            return True, txh, Web3.to_checksum_address(underlying), None
        return False, None, None, None
    except Exception as e:
        logger.debug("redeem_aave_atoken error: %s", e)
        return False, None, None, None

async def remove_liquidity_v2(lp_token: str, router_pref: str = "SUSHI") -> Tuple[bool, Optional[str], Optional[str], Optional[str], Optional[int], Optional[int]]:
    try:
        pair = w3.eth.contract(address=Web3.to_checksum_address(lp_token), abi=UNI_V2_PAIR_ABI)
        t0 = Web3.to_checksum_address(pair.functions.token0().call())
        t1 = Web3.to_checksum_address(pair.functions.token1().call())
        reserves = pair.functions.getReserves().call()
        reserve0, reserve1 = int(reserves[0]), int(reserves[1])
        total_supply = int(pair.functions.totalSupply().call())
        liq = erc20_balance(lp_token, ACCOUNT_ADDRESS)
        if liq <= 0 or total_supply == 0:
            return False, None, None, None, None, None
        amt0_exp = (liq * reserve0) // total_supply
        amt1_exp = (liq * reserve1) // total_supply
        amt0_min = int(Decimal(amt0_exp) * (Decimal("1") - CONSOLIDATE_SLIPPAGE))
        amt1_min = int(Decimal(amt1_exp) * (Decimal("1") - CONSOLIDATE_SLIPPAGE))
        router = sushi_router if router_pref.upper() == "SUSHI" else camelot_router
        router_addr = router.address
        ap_tx = await _approve_if_needed(lp_token, router_addr, liq)
        if ap_tx:
            await wait_receipt(ap_tx, timeout=180)
        tx = router.functions.removeLiquidity(
            t0, t1, int(liq), int(amt0_min), int(amt1_min),
            Web3.to_checksum_address(ACCOUNT_ADDRESS), int(chain_deadline(300))
        ).build_transaction(await tx_opts_base_async(fast_gas=False))
        if not simulate_tx(tx):
            logger.debug("removeLiquidity simulate failed")
            return False, None, None, None, None, None
        ok, txh = await send_tx_async(tx)
        await log_recovery_op("v2.removeLiquidity", lp_token, f"{t0}|{t1}", liq, 0, txh, "submitted")
        if ok and txh:
            rcpt = await wait_receipt(txh, 180)
            await monitor_tx(txh, "liquidity-exit", {"token": lp_token, "amount": liq, "gross_usd": 0, "net_usd": 0})
            if not rcpt or int(rcpt.get("status",0)) != 1:
                 return False, txh, None, None, None, None
            return True, txh, t0, t1, amt0_min, amt1_min
        return False, None, None, None, None, None
    except Exception as e:
        logger.debug("remove_liquidity_v2 error: %s", e)
        return False, None, None, None, None, None

async def find_best_v2_path_and_router(token_in: str, token_out: str, amount_in: int) -> Tuple[Optional[str], Optional[List[str]], Optional[int]]:
    candidates_paths: List[List[str]] = []
    in_cs = Web3.to_checksum_address(token_in)
    out_cs = Web3.to_checksum_address(token_out)
    if in_cs == out_cs:
        return None, None, None
    candidates_paths.append([in_cs, out_cs])
    weth = baseline_addr_for_symbol("WETH")
    usdc = baseline_addr_for_symbol("USDC")
    if weth and weth not in {in_cs, out_cs}:
        candidates_paths.append([in_cs, weth, out_cs])
    if usdc and usdc not in {in_cs, out_cs}:
        candidates_paths.append([in_cs, usdc, out_cs])

    best_router = None
    best_path: Optional[List[str]] = None
    best_out = -1
    for path in candidates_paths:
        out_s = await v2_quote_path(sushi_router, path, amount_in)
        if out_s is not None and out_s > best_out:
            best_out = out_s; best_router = "SUSHI"; best_path = path
        out_c = await v2_quote_path(camelot_router, path, amount_in)
        if out_c is not None and out_c > best_out:
            best_out = out_c; best_router = "CAMELOT"; best_path = path
    if best_out <= 0:
        return None, None, None
    return best_router, best_path, best_out

async def _approve_if_needed(token: str, spender: str, amount: int) -> Optional[str]:
    try:
        token_cs = Web3.to_checksum_address(token)
        spender_cs = Web3.to_checksum_address(spender)
        if not ACCOUNT_ADDRESS: return None
        cur_allow = erc20_allowance(token_cs, ACCOUNT_ADDRESS, spender_cs)
        if cur_allow >= amount:
            return None
        c = w3.eth.contract(address=token_cs, abi=ERC20_ABI)
        # some tokens require allowance=0 before raising
        if cur_allow > 0:
            tx0 = c.functions.approve(spender_cs, 0).build_transaction(await tx_opts_base_async(fast_gas=False))
            if not simulate_tx(tx0): return None
            ok0, h0 = await send_tx_async(tx0)
            if not ok0 or not h0: return None
            await wait_receipt(h0, 180)
        tx1 = c.functions.approve(spender_cs, int(UINT_MAX)).build_transaction(await tx_opts_base_async(fast_gas=False))
        if not simulate_tx(tx1): return None
        ok1, h1 = await send_tx_async(tx1)
        return h1 if ok1 else None
    except Exception as e:
        logger.debug("approve error %s->%s: %s", token, spender, e)
        return None

# ============================ Wallet / Executor token discovery via logs (RESTORED) ============================
TRANSFER_TOPIC = Web3.keccak(text="Transfer(address,address,uint256)").hex()

async def _get_logs_for_to(addr_hex: str, from_block: int, to_block: int):
    out = []
    addr_topic = "0x" + "0"*24 + addr_hex.lower()[2:]
    cur = from_block
    latest = to_block
    while cur <= latest:
        high = min(cur + RECOVERY_LOG_CHUNK - 1, latest)
        params = {"fromBlock": hex(cur), "toBlock": hex(high), "topics": [TRANSFER_TOPIC, None, addr_topic]}
        try:
            logs = await asyncio.to_thread(lambda: w3.eth.get_logs(params))
            out.extend(logs or [])
        except Exception as e:
            logger.debug(f"_get_logs_for_to chunk {cur}-{high} error: {e}")
        cur = high + 1
    return out

async def discover_wallet_tokens_from_transfers(wallet: str, lookback_blocks: int = 20000) -> Set[str]:
    try:
        latest = w3.eth.block_number
        from_block = max(0, latest - lookback_blocks)
        logs = await _get_logs_for_to(Web3.to_checksum_address(wallet), from_block, latest)
        addrs = {Web3.to_checksum_address(log["address"]) for log in logs if "address" in log}
        return addrs
    except Exception as e:
        logger.debug(f"discover_wallet_tokens_from_transfers error: {e}")
        return set()

async def discover_executor_tokens_from_transfers(executor: str, lookback_blocks: int = None) -> Set[str]:
    try:
        lookback = lookback_blocks if lookback_blocks is not None else RECOVERY_TRANSFER_LOOKBACK_BLOCKS
        latest = w3.eth.block_number
        from_block = max(0, latest - lookback)
        logs = await _get_logs_for_to(Web3.to_checksum_address(executor), from_block, latest)
        addrs = {Web3.to_checksum_address(log["address"]) for log in logs if "address" in log}
        return addrs
    except Exception as e:
        logger.debug(f"discover_executor_tokens_from_transfers error: {e}")
        return set()

async def tokens_from_db_history(limit: int = 2000) -> Set[str]:
    out: Set[str] = set()
    rows = await _db_fetchall("SELECT token_in, token_out FROM recovery_ops ORDER BY id DESC LIMIT ?", (limit,))
    for a,b in rows:
        for x in (a,b):
            if isinstance(x, str) and x.startswith("0x") and len(x) == 42:
                try: out.add(Web3.to_checksum_address(x))
                except Exception: pass
    rows = await _db_fetchall("SELECT cycle FROM trades ORDER BY id DESC LIMIT 500")
    for (cycle_json,) in rows:
        try:
            cycle = json.loads(cycle_json) if isinstance(cycle_json, str) else None
        except Exception:
            cycle = None
        if isinstance(cycle, list):
            for x in cycle:
                if isinstance(x, str) and x.startswith("0x") and len(x) == 42:
                    try: out.add(Web3.to_checksum_address(x))
                    except Exception: pass
    return out

# ============================ Provider caps (Aave/Balancer/DODO) ============================
async def provider_cap(provider: str, token_in: str, token_out: str) -> int:
    provider = provider.upper()
    if provider == "AAVE":
        # Approximate cap from Pool's on-chain balance of the underlying.
        # (For tighter precision, add a minimal ABI for getReserveData.availableLiquidity.)
        try:
            pool_addr = Web3.to_checksum_address(LIQ_POOL_ADDR)
            avail = erc20_balance(Web3.to_checksum_address(token_in), pool_addr)
            return int(Decimal(avail) * CAP_AAVE_PCT)
        except Exception:
            return 0
    if provider == "BALANCER":
        try:
            q = f'''{{ pools(first: 3, orderBy: totalLiquidity, orderDirection: desc, where: {{ tokensList_contains: ["{token_in.lower()}","{token_out.lower()}"] }}) {{ id }} }}'''
            data = await subgraph_query(SUBG_BAL, q)
            pools = data.get("data", {}).get("pools", [])
            if not pools:
                return 0
            pool_id = pools[0]["id"]
            vault_addr = Web3.to_checksum_address(os.getenv("BALANCER_VAULT", "0xBA12222222228d8Ba445958a75a0704d566BF2C8"))
            balancer_tokens = w3.eth.contract(address=vault_addr, abi=[{
              "inputs":[{"internalType":"bytes32","name":"poolId","type":"bytes32"}],
              "name":"getPoolTokens",
              "outputs":[{"internalType":"address[]","name":"tokens","type":"address[]"},
                         {"internalType":"uint256[]","name":"balances","type":"uint256[]"},
                         {"internalType":"uint256","name":"lastChangeBlock","type":"uint256"}],
              "stateMutability":"view","type":"function"}])
            tokens, balances, _ = balancer_tokens.functions.getPoolTokens(bytes.fromhex(pool_id[2:])).call()
            t = [Web3.to_checksum_address(x) for x in tokens]
            token_in_cs = Web3.to_checksum_address(token_in)
            if token_in_cs in t:
                idx = t.index(token_in_cs)
                return int(Decimal(int(balances[idx])) * CAP_BALANCER_PCT)
        except Exception as e:
            logger.debug("Balancer cap error: %s", e)
            return 0
        return 0
    if provider == "DODO":
        pools = [p.strip() for p in os.getenv("DODO_POOL", "").split(",") if p.strip()]
        best_bal = 0
        for p in pools:
            try:
                bal = erc20_balance(Web3.to_checksum_address(token_in), Web3.to_checksum_address(p))
                best_bal = max(best_bal, bal)
            except Exception:
                continue
        return int(Decimal(best_bal) * CAP_DODO_PCT)
    return 0

# ============================ ML stub ============================
class BaseML:
    async def predict_proba(self, feats: Dict[str, float]) -> float: return 0.5
    async def update(self, feats: Dict[str, float], label: float, importance: float = 1.0): ...
    async def save(self): ...
    async def load(self): ...

class OnlineLogReg(BaseML):
    def __init__(self, lr: float = 0.05, l2: float = 1e-6):
        self.lr = lr
        self.l2 = l2
        self.weights: Dict[str, float] = {}
        self.feature_names: List[str] = []
        self.version: int = 0
    def _ensure_names(self, names: List[str]):
        exist = set(self.feature_names)
        for n in names:
            if n not in exist:
                self.feature_names.append(n); exist.add(n)
                if n not in self.weights: self.weights[n] = 0.0
    async def predict_proba(self, feats: Dict[str, float]) -> float:
        self._ensure_names(list(feats.keys()))
        z = 0.0
        for n in self.feature_names:
            z += self.weights.get(n, 0.0) * float(feats.get(n, 0.0))
        try:
            p = 1.0 / (1.0 + math.exp(-z)) if z >= 0 else math.exp(z) / (1.0 + math.exp(z))
        except OverflowError:
            p = 0.999 if z > 0 else 0.001
        return max(0.001, min(0.999, p))
    async def update(self, feats: Dict[str, float], label: float, importance: float = 1.0):
        p = await self.predict_proba(feats); err = (label - p) * importance
        self._ensure_names(list(feats.keys()))
        for n in self.feature_names:
            x = float(feats.get(n, 0.0))
            g = -err * (-x) + self.l2 * self.weights.get(n, 0.0)
            self.weights[n] = self.weights.get(n, 0.0) - self.lr * g
    def _fingerprint(self) -> str:
        blob = json.dumps({"w": self.weights, "f": self.feature_names}, sort_keys=True)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()
    async def save(self):
        fp = self._fingerprint()
        await _db_exec("INSERT INTO ml_state_versions (created_at, weights, feature_names, fingerprint) VALUES (?,?,?,?)",
                    (now_ts(), json.dumps(self.weights), json.dumps(self.feature_names), fp))
        row = await _db_fetchone("SELECT MAX(version) FROM ml_state_versions")
        ver = int(row[0] or 1)
        self.version = ver
        await _db_exec("INSERT OR REPLACE INTO ml_state (id, version, weights, feature_names) VALUES (1, ?, ?, ?)",
                    (ver, json.dumps(self.weights), json.dumps(self.feature_names)))
    async def load(self):
        row = await _db_fetchone("SELECT version, weights, feature_names FROM ml_state WHERE id=1")
        if row:
            try:
                self.version = int(row[0] or 0)
                self.weights = json.loads(row[1]) or {}
                self.feature_names = json.loads(row[2]) or []
                return
            except Exception: pass
        row2 = await _db_fetchone("SELECT version, weights, feature_names FROM ml_state_versions ORDER BY version DESC LIMIT 1")
        if row2:
            try:
                self.version = int(row2[0] or 0)
                self.weights = json.loads(row2[1]) or {}
                self.feature_names = json.loads(row2[2]) or []
                return
            except Exception: pass
        self.version = 0; self.weights = {}; self.feature_names = []

async def _make_ml() -> BaseML:
    m = OnlineLogReg(); await m.load(); return m

ML_MAIN: Optional[BaseML] = None

def _safe_std(xs: List[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    m = sum(xs) / n
    var = sum((x - m)**2 for x in xs) / (n - 1)
    return max(0.0, var)**0.5

def features_for_cycle(graph: Dict[str, Dict[str, Decimal]], cycle: List[str], size: int, fast_gas: bool = False) -> Dict[str, float]:
    feats: Dict[str, float] = {}
    hops = len(cycle) - 1
    feats["hops"] = float(hops)
    one_minus = float((Decimal("1.0") - SLIPPAGE))
    prod = 1.0
    rates: List[float] = []
    for i in range(len(cycle) - 1):
        u, v = cycle[i], cycle[i+1]
        r = graph.get(u, {}).get(v)
        if not r: continue
        denom = one_minus if one_minus > 0 else 1.0
        rv = float(r / Decimal(denom))
        rates.append(rv); prod *= rv
    feats["path_spread"] = float(prod - 1.0)
    avg_rate = (sum(rates) / len(rates)) if rates else 1.0
    feats["avg_rate_log"] = math.log(max(1e-9, avg_rate))
    feats["rate_std"] = _safe_std(rates) if rates else 0.0
    feats["base_vol"] = 0.0
    feats["quote_vol"] = 0.0
    feats["oracle_div"] = abs(feats["path_spread"])
    size_usd = float(usd_notional(cycle[0], size))
    feats["size_usd_log"] = math.log(max(1.0, size_usd))
    gp = float(predict_gas_price_wei(fast=fast_gas) or w3.eth.gas_price)
    gas_floor = max(1e-9, gp if gp > 0 else float(_gas_ema or 1.0))
    feats["gas_ema_log"] = math.log(gas_floor)
    feats["uniq_tokens"] = float(len(set(cycle)))
    return feats

async def blended_route_score(route: List[str], graph: Dict[str, Dict[str, Decimal]], size: int, fast_gas: bool = False) -> float:
    b_key = bandit_key(route)
    b_alpha, b_beta = await bandit_get(b_key)
    import random as _r
    b_draw = _r.betavariate(max(1e-3, b_alpha), max(1e-3, b_beta))
    feats = features_for_cycle(graph, route, size, fast_gas=fast_gas)
    p = await ML_MAIN.predict_proba(feats) if ML_MAIN else 0.5
    gtokens = simulate_cycle_profit_tokens(graph, route, size) or Decimal(0)
    gross_usd = float(profit_usd(route[0], gtokens))
    score = 0.5 * p + 0.4 * b_draw + 0.1 * (gross_usd > 0)
    score += 0.000001 * max(0.0, gross_usd)
    return score

# ============================ Profit Recovery (withdraw + consolidate + optional bridge) ============================
async def _consolidation_swap(token: str, target_addr: str, bal: int, router, path: List[str], min_out: int) -> Optional[str]:
    ap_tx = await _approve_if_needed(token, router.address, bal)
    if ap_tx:
        await wait_receipt(ap_tx, timeout=180)
    tx, tag = await build_v2_swap_tx(router, bal, min_out, path, ACCOUNT_ADDRESS, chain_deadline(300))
    if not tx:
        logger.debug("swap build failed %s->%s (both variants)", token, target_addr)
        return None
    ok, txh = await send_tx_async(tx)
    await log_recovery_op(tag, token, target_addr, bal, int(min_out), txh, "submitted")
    if ok:
        await monitor_tx(txh, "swap", {"token": token, "amount": int(bal), "gross_usd": float(usd_notional(token, bal)), "net_usd": 0})
        return txh
    return None

async def withdraw_profits():
    if not ACCOUNT or not ACCOUNT_ADDRESS:
        logger.info("withdraw_profits skipped (no PRIVATE_KEY)")
        return
    if _is_zero_address(ARB_EXECUTOR_ADDRESS):
        return

    extras = set()
    for a in [a.strip() for a in os.getenv("EXTRA_WITHDRAW_TOKENS", "").split(",") if a.strip()]:
        try:
            extras.add(Web3.to_checksum_address(a))
        except Exception:
            continue

    targets: Set[str] = set(BASELINE_ADDRS) | set(TOKENS.keys()) | extras | await tokens_from_db_history()
    try:
        exec_seen = await discover_executor_tokens_from_transfers(ARB_EXECUTOR_ADDRESS, lookback_blocks=RECOVERY_TRANSFER_LOOKBACK_BLOCKS)
        targets |= exec_seen
    except Exception as e:
        logger.debug("executor token scan failed: %s", e)

    for token in list(targets):
        try:
            token_cs = Web3.to_checksum_address(token)
        except Exception:
            continue
        try:
            exec_bal = erc20_balance(token_cs, ARB_EXECUTOR_ADDRESS)
            if exec_bal <= 0:
                if DEBUG_DECISIONS:
                    logger.info("[WITHDRAW] skip zero exec bal token=%s", token_cs)
                continue
            tx = exec_mgmt.functions.withdrawProfit(token_cs).build_transaction(await tx_opts_base_async(fast_gas=False))
        except Exception:
            continue
        if not simulate_tx(tx):
            if DEBUG_DECISIONS:
                logger.info("[WITHDRAW] simulate failed token=%s bal=%s", token_cs, exec_bal)
            continue
        ok, txh = await send_tx_async(tx)
        await log_recovery_op("exec.withdrawProfit", token_cs, token_cs, exec_bal, 0, txh, "submitted")
        if ok:
            await monitor_tx(txh, "withdraw", {"token": token_cs, "amount": int(exec_bal), "gross_usd": float(usd_notional(token_cs, exec_bal)), "net_usd": 0})

async def recover_profits_once():
    if ACCOUNT is None or not ACCOUNT_ADDRESS:
        return

    try:
        await withdraw_profits()
    except Exception as e:
        logger.debug("withdraw_profits error (continuing): %s", e)

    target_sym = CONSOLIDATE_PREFERRED_BASE
    target_addr = baseline_addr_for_symbol(target_sym)
    if not target_addr:
        logger.warning("Preferred base %s not recognized; skipping consolidation", target_sym)
        return

    tokens: Set[str] = set()
    tokens.update(BASELINE_ADDRS)
    tokens.update(TOKENS.keys())
    tokens.update(await tokens_from_db_history())
    try:
        more_w = await discover_wallet_tokens_from_transfers(ACCOUNT_ADDRESS, lookback_blocks=RECOVERY_TRANSFER_LOOKBACK_BLOCKS)
        tokens.update(more_w)
    except Exception:
        pass
    try:
        more_x = await discover_executor_tokens_from_transfers(ARB_EXECUTOR_ADDRESS, lookback_blocks=RECOVERY_TRANSFER_LOOKBACK_BLOCKS)
        tokens.update(more_x)
    except Exception:
        pass

    for token in list(tokens):
        try:
            token = Web3.to_checksum_address(token)
        except Exception:
            continue

        bal = erc20_balance(token, ACCOUNT_ADDRESS)
        if bal <= 0:
            continue

        try:
            usd_val = float(usd_notional(token, bal))
        except Exception:
            usd_val = 0.0
        if usd_val < float(CONSOLIDATE_MIN_USD):
            continue

        if token == target_addr:
            continue

        kind, meta = classify_token(token)
        for _ in range(3): # Max 3 redemption steps
            if kind == TokenKind.BASE or token == target_addr:
                break
            if kind == TokenKind.ERC4626:
                ok, _, underlying, _ = await redeem_erc4626(token, bal)
                if not ok or not underlying:
                    break
                token = Web3.to_checksum_address(underlying)
                bal = erc20_balance(token, ACCOUNT_ADDRESS)
                kind, meta = classify_token(token)
                continue
            if kind == TokenKind.AAVE_ATOKEN:
                ok, _, underlying, _ = await redeem_aave_atoken(token, bal)
                if not ok or not underlying:
                    break
                token = Web3.to_checksum_address(underlying)
                bal = erc20_balance(token, ACCOUNT_ADDRESS)
                kind, meta = classify_token(token)
                continue
            if kind == TokenKind.UNI_V2_LP:
                ok, _, _, _, _, _ = await remove_liquidity_v2(token, router_pref="SUSHI")
                if not ok: break
                # After removing liquidity, we have multiple tokens, so we end this chain.
                # The main loop will pick them up on the next iteration.
                break 
            break

        bal = erc20_balance(token, ACCOUNT_ADDRESS)
        if bal <= 0 or token == target_addr:
            continue

        router_name, path, out_quote = await find_best_v2_path_and_router(token, target_addr, bal)
        if not router_name or not path or not out_quote or out_quote <= 0:
            logger.debug("No viable swap path for %s -> %s, skipping", token, target_addr)
            continue
        min_out = int(Decimal(out_quote) * (Decimal("1.0") - CONSOLIDATE_SLIPPAGE))
        router = sushi_router if router_name == "SUSHI" else camelot_router
        _ = await _consolidation_swap(token, target_addr, bal, router, path, min_out)

    if not BRIDGE_ENABLED or BRIDGE_TARGET_CHAIN_ID == CHAIN_ID:
        await unwrap_weth_if_configured(target_sym)

# ============================ Arbitrage strategy ============================
def chain_deadline(secs: int = 120) -> int:
    try:
        return int(w3.eth.get_block("latest")["timestamp"]) + int(secs)
    except Exception:
        return int(time.time()) + int(secs)

async def choose_best_provider_and_size(
    cycle: List[str], graph: Dict[str, Dict[str, Decimal]], fast_gas: bool = False,
) -> Tuple[Optional[str], Optional[int], Decimal, Decimal, float]:
    token_in, token_out = cycle[0], cycle[1]
    providers = ["AAVE", "BALANCER", "DODO"]
    best: Tuple[Optional[str], Optional[int], Decimal, Decimal, float] = (None, None, Decimal("-inf"), Decimal(0), float(SLIPPAGE))

    async def feasible(sz: int) -> bool:
        if sz <= 0:
            return False
        q = await quote_univ3_exact_in(cycle, [3000] * (len(cycle) - 1), sz)
        return bool(q and q > 0)

    for p in providers:
        cap = await provider_cap(p, token_in, token_out)
        if cap <= 0:
            continue
        size_low, size_high = 1, int(cap)
        for _ in range(6):
            mid = (size_low + size_high) // 2
            if mid <= 0:
                break
            if await feasible(mid):
                size_low = mid
            else:
                size_high = max(0, mid - 1)
        size = max(1, size_low)
        gtokens = simulate_cycle_profit_tokens(graph, cycle, size)
        if gtokens is None:
            continue
        
        # Build a temporary transaction to estimate gas cost more accurately
        fees_temp, out_est_temp = await choose_univ3_fees_for_path(cycle, size)
        if not fees_temp or out_est_temp <= 0: continue
        min_out_temp = int(out_est_temp * (1.0 - float(SLIPPAGE)))
        deadline_temp = chain_deadline(180)
        payload_temp = encode_arbitrage_payload(cycle, min_out_temp, "UNISWAP_V3", fees_temp, deadline_temp)
        try:
            tx_temp = exec_c.functions.requestFlashLoan(
                int(FlashloanProvider.get(p, 0)),
                Web3.to_checksum_address(cycle[0]), int(size), payload_temp, int(deadline_temp)
            ).build_transaction(await tx_opts_base_async(fast_gas=fast_gas))
            
            gas_cost_usd = _usd_cost_for_tx(tx_temp)
            gross_usd = profit_usd(cycle[0], gtokens)
            net_usd = gross_usd - gas_cost_usd

            if net_usd > best[2]:
                best = (p, size, net_usd, gross_usd, float(SLIPPAGE))
        except Exception:
            continue

    return best

async def arbitrage_strategy(trigger: str = "poll") -> bool:
    if not WSS_READY_EVENT.is_set():
        return False
    if _is_zero_address(ARB_EXECUTOR_ADDRESS):
        logger.debug("Executor address is zero. Skipping strategy tick.")
        return False
    addrs = list(TOKENS.keys())
    if len(addrs) < 3:
        return False

    fast_gas = trigger.lower() in {"backrun", "pending"}
    graph = await build_graph(0) # amount_in_base is now unused
    if STOP_EVENT.is_set():
        return False
    if DEBUG_DECISIONS:
        try:
            edge_count = sum(len(vs) for vs in graph.values())
            logger.info("[ARB] trigger=%s nodes=%s edges=%s", trigger, len(graph), edge_count)
        except Exception:
            pass

    if USE_JOHNSON_CYCLES:
        cycles = _johnson_simple_cycles(graph, MAX_HOPS)
    else:
        cycles = enumerate_cycles(graph, MAX_HOPS)
    bf_cycles = bellman_ford_arbitrage(graph)
    for c in bf_cycles:
        if c and c[0] == c[-1] and c not in cycles:
            cycles.append(c)

    if not cycles:
        return False

    anchor = next(iter(TOKENS.keys()), baseline_addr_for_symbol("WETH"))
    score_size = sample_size_for(anchor)
    scores = await asyncio.gather(*[
        blended_route_score(c, graph, score_size, fast_gas=fast_gas) for c in cycles
    ])
    cycles = [c for _, c in sorted(zip(scores, cycles), key=lambda t: t[0], reverse=True)]

    if DEBUG_DECISIONS:
        logger.info("[ARB] cycles found (scored)=%s", len(cycles))

    for cycle in cycles[:200]:
        if STOP_EVENT.is_set():
            break
        if not oracle_divergence_ok(cycle, graph):
            await bandit_update(bandit_key(cycle), success=False)
            continue
        
        refined = refine_cycle_with_dijkstra(graph, cycle, MAX_HOPS)
        
        provider, size, net_usd, gross_usd, slip = await choose_best_provider_and_size(refined, graph, fast_gas=fast_gas)
        
        if not provider or not size:
            if DEBUG_DECISIONS:
                logger.info("[ARB] skip: no provider/size path=%s", "->".join(refined))
            continue
        if net_usd < MIN_PROFIT_USD:
            if DEBUG_DECISIONS:
                logger.info("[ARB] skip: net $%.2f < min $%.2f path=%s prov=%s size=%s",
                            float(net_usd), float(MIN_PROFIT_USD), "->".join(refined), provider, size)
            continue
        
        # Final parameters for execution
        fees, out_est = await choose_univ3_fees_for_path(refined, size)
        if not fees or out_est <= 0:
            continue
        
        dyn_slip = float(dynamic_slippage_for_trade(refined[0], size, len(refined) - 1))
        min_out = int(out_est * (1.0 - dyn_slip))
        deadline = chain_deadline(180)
        payload = encode_arbitrage_payload(refined, min_out, "UNISWAP_V3", fees, deadline)
        
        try:
            tx = exec_c.functions.requestFlashLoan(
                int(FlashloanProvider.get(provider, 0)),
                Web3.to_checksum_address(refined[0]),
                int(size),
                payload,
                int(deadline)
            ).build_transaction(await tx_opts_base_async(fast_gas=fast_gas))
            
            gas_cost_final = _usd_cost_for_tx(tx)
            if (gross_usd - gas_cost_final) < MIN_PROFIT_USD:
                logger.info("[ARB] Final check failed: profit too low after precise gas. est_net=$%.2f final_net=$%.2f", net_usd, gross_usd - gas_cost_final)
                continue

            if not simulate_tx(tx):
                if DEBUG_DECISIONS:
                    logger.info("[ARB] skip: simulate failed path=%s size=%s prov=%s", "->".join(refined), size, provider)
                continue
            ok, txh = await send_tx_async(tx)
            if not ok:
                continue
            
            final_net_usd = gross_usd - gas_cost_final
            await log_trade("arb", refined, "UNISWAP_V3", float(gross_usd), float(final_net_usd), float(gas_cost_final), int(size), int(USE_FLASHBOTS))
            await monitor_tx(txh, "arb", {"token": refined[0], "amount": size, "gross_usd": float(gross_usd), "net_usd": float(final_net_usd), "path": "->".join(refined), "dexes": "UNISWAP_V3"})
            await bandit_update(bandit_key(refined), success=True, reward=float(final_net_usd))
            return True
        except Exception as e:
            logger.debug("flashloan build failed: %s", e)
            continue
            
    return False

# ============================ Token discovery ============================
async def _read_token_decimals(addr: str) -> int:
    def _call() -> int:
        try:
            c = w3.eth.contract(address=Web3.to_checksum_address(addr), abi=ERC20_ABI)
            return int(c.functions.decimals().call())
        except Exception:
            return 18
    return await asyncio.to_thread(_call)

async def _read_token_symbol(addr: str) -> str:
    def _call() -> str:
        try:
            c = w3.eth.contract(address=Web3.to_checksum_address(addr), abi=ERC20_SYMBOL_ABI)
            s = c.functions.symbol().call()
            if isinstance(s, str) and s:
                return s.upper()[:16]
        except Exception:
            pass
        try:
            c2 = w3.eth.contract(address=Web3.to_checksum_address(addr), abi=ERC20_SYMBOL32_ABI)
            b = c2.functions.symbol().call()
            if isinstance(b, (bytes, bytearray)):
                s = b.decode("utf-8", errors="ignore").rstrip("\x00")
                if s:
                    return s.upper()[:16]
        except Exception:
            pass
        return f"TKN{addr[-4:]}".upper()
    return await asyncio.to_thread(_call)

async def discover_tokens(limit: int = MAX_TOKENS):
    global TOKENS
    addr_set: Set[str] = set(BASELINE_ADDRS)

    if not USE_UNIV3_ONLY:
        for u, vs in SUSHI_ADJ.items():
            addr_set.add(Web3.to_checksum_address(u))
            for v in vs: addr_set.add(Web3.to_checksum_address(v))
    for u, vs in UNIV3_ADJ.items():
        addr_set.add(Web3.to_checksum_address(u))
        for v in vs: addr_set.add(Web3.to_checksum_address(v))


    try:
        reserves = aave_pool.functions.getReservesList().call()
        for a in reserves:
            if Web3.is_address(a):
                addr_set.add(Web3.to_checksum_address(a))
    except Exception:
        pass

    addr_set |= await tokens_from_db_history()

    try:
        if ACCOUNT_ADDRESS:
            addr_set |= await discover_wallet_tokens_from_transfers(ACCOUNT_ADDRESS, lookback_blocks=RECOVERY_TRANSFER_LOOKBACK_BLOCKS)
    except Exception:
        pass
    try:
        if not _is_zero_address(ARB_EXECUTOR_ADDRESS):
            addr_set |= await discover_executor_tokens_from_transfers(ARB_EXECUTOR_ADDRESS, lookback_blocks=RECOVERY_TRANSFER_LOOKBACK_BLOCKS)
    except Exception:
        pass

    def liq_score(a: str) -> float:
        base_boost = 1e12 if a in BASELINE_ADDRS else 0.0
        return base_boost + LIQ_SCORE.get(a, 0.0)

    all_addrs = sorted({Web3.to_checksum_address(a) for a in addr_set}, key=liq_score, reverse=True)
    selected = all_addrs[:max(5, min(limit, len(all_addrs)))]

    new_tokens: Dict[str, Dict[str, Any]] = {}
    tasks = []
    for a in selected:
        if a in BASELINE_TOKEN_INFO:
            new_tokens[a] = {"symbol": BASELINE_TOKEN_INFO[a]["symbol"], "decimals": BASELINE_TOKEN_INFO[a]["decimals"]}
        else:
            tasks.append((a, asyncio.create_task(_read_token_decimals(a)), asyncio.create_task(_read_token_symbol(a))))
    for a, tdec, tsym in tasks:
        try:
            dec = await tdec
        except Exception:
            dec = 18
        try:
            sym = await tsym
        except Exception:
            sym = f"TKN{a[-4:]}".upper()
        new_tokens[a] = {"symbol": sym, "decimals": int(dec)}

    TOKENS = new_tokens
    logger.info("Discovered %s tokens (limit=%s)", len(TOKENS), limit)

# ============================ Mempool backrun watcher ============================
_seen_pending: set = set()

async def _fetch_tx(txh: str) -> Optional[dict]:
    try:
        return await asyncio.to_thread(lambda: w3.eth.get_transaction(txh))
    except Exception:
        return None

async def process_pending_tx_hash(txh: str):
    if not WSS_READY_EVENT.is_set():
        return
    if txh in _seen_pending:
        return
    _seen_pending.add(txh)
    tx = await _fetch_tx(txh)
    if not tx:
        return
    try:
        to_addr = tx.get("to") or getattr(tx, "to", None)
        if not to_addr:
            return
        to_addr = Web3.to_checksum_address(to_addr)
    except Exception:
        return
    targets = {Web3.to_checksum_address(a) for a in BACKRUN_TARGETS if a}
    if targets and to_addr not in targets:
        return
    try:
        METRICS["backruns_seen"] = int(METRICS.get("backruns_seen", 0)) + 1
    except Exception:
        pass
    try:
        await arbitrage_strategy(trigger="backrun")
    except Exception as e:
        logger.debug("backrun scan error for %s: %s", txh, e)

async def mempool_loop():
    if not ENABLE_BACKRUNS:
        logger.info("Backruns disabled; mempool loop not started.")
        return

    METRICS["mempool_mode"] = "waiting"
    METRICS["mempool_endpoint"] = None
    METRICS["mempool_ready_at"] = None
    last_announced_endpoint: Optional[str] = None
    while not STOP_EVENT.is_set():
        await WSS_READY_EVENT.wait()

        endpoint_label = _mask_url(WSS_ACTIVE_URL) if WSS_ACTIVE_URL else "HTTP fallback"
        if endpoint_label != last_announced_endpoint:
            logger.info("Mempool loop entering realtime mode via %s", endpoint_label)
            last_announced_endpoint = endpoint_label
            METRICS["mempool_mode"] = "realtime" if WSS_ACTIVE_URL else "http-fallback"
            METRICS["mempool_endpoint"] = endpoint_label
            METRICS["mempool_ready_at"] = int(time.time())

        sub = None
        if aw3 is not None and WSS_ACTIVE_URL:
            order = WSS_SUBSCRIBE_ORDER or ["pending", "newheads"]
            for mode in order:
                if STOP_EVENT.is_set() or not WSS_READY_EVENT.is_set(): break
                try:
                    if mode == "pending":
                        sub = await aw3.eth.subscribe("newPendingTransactions")
                        logger.info("WSS subscribed to newPendingTransactions on %s", _mask_url(WSS_ACTIVE_URL))
                        async for ev in sub:
                            if STOP_EVENT.is_set() or not WSS_READY_EVENT.is_set(): break
                            txh = ev.hex() if isinstance(ev, (bytes, bytearray)) else str(ev)
                            if txh.startswith("0x"):
                                asyncio.create_task(process_pending_tx_hash(txh))
                    elif mode == "newheads":
                        sub = await aw3.eth.subscribe("newHeads")
                        logger.info("WSS subscribed to newHeads on %s", _mask_url(WSS_ACTIVE_URL))
                        async for hdr in sub:
                            if STOP_EVENT.is_set() or not WSS_READY_EVENT.is_set(): break
                            bn = hdr.get("number")
                            if bn is not None: _on_new_block(int(bn, 16) if isinstance(bn, str) else int(bn))
                except Exception as e:
                    logger.warning("WSS %s subscription error: %s. Watchdog will handle.", mode, e)
                    await asyncio.sleep(5)
                finally:
                    if sub:
                        try: await aw3.eth.unsubscribe(sub.id)
                        except Exception: pass
        else:
            logger.warning("Mempool polling via HTTP is inefficient and not recommended. Running in degraded mode.")
            METRICS["mempool_mode"] = "http-fallback"
            METRICS["mempool_endpoint"] = endpoint_label
            await asyncio.sleep(30)

        if not WSS_READY_EVENT.is_set():
            last_announced_endpoint = None
            METRICS["mempool_mode"] = "paused"
            METRICS["mempool_endpoint"] = None
            METRICS["mempool_ready_at"] = None

# ============================ Header loop ============================
async def block_header_loop():
    last = -1
    while not STOP_EVENT.is_set():
        await WSS_READY_EVENT.wait()
        
        sub = None
        if aw3 is not None and WSS_ACTIVE_URL and "newheads" not in WSS_SUBSCRIBE_ORDER:
            try:
                sub = await aw3.eth.subscribe("newHeads")
                logger.info("WSS block header listener on %s", _mask_url(WSS_ACTIVE_URL))
                async for hdr in sub:
                    if STOP_EVENT.is_set() or not WSS_READY_EVENT.is_set(): break
                    bn = hdr.get("number")
                    if bn is not None: _on_new_block(int(bn, 16) if isinstance(bn, str) else int(bn))
            except Exception as e:
                logger.warning("WSS header subscription failed: %s; watchdog will handle.", e)
                await asyncio.sleep(5)
            finally:
                if sub:
                    try: await aw3.eth.unsubscribe(sub.id)
                    except Exception: pass
        else:
            try:
                bn = await asyncio.to_thread(w3.eth.block_number)
                if isinstance(bn, int) and bn != last:
                    _on_new_block(bn)
                    last = bn
            except Exception as e:
                logger.debug("HTTP block polling error: %s", e)
            
            try:
                await asyncio.wait_for(BLOCK_UPDATE_EVENT.wait(), timeout=max(1.0, SCAN_INTERVAL / 2.0))
            except asyncio.TimeoutError:
                pass

# ============================ Loops, Health, Summary, Preflight ============================
async def token_discovery_loop():
    try:
        await compute_liquidity_scores()
        if not UNIV3_ADJ: await discover_univ3_adjacency(limit_pools=800)
        if not SUSHI_ADJ and not USE_UNIV3_ONLY: await discover_sushi_adjacency(limit_pairs=800)
        await discover_tokens(limit=MAX_TOKENS)
    except Exception as e:
        logger.error("Initial token discovery failed: %s", e, exc_info=True)

    interval = max(120, TOKEN_DISCOVERY_INTERVAL)
    while not STOP_EVENT.is_set():
        await asyncio.sleep(interval)
        try:
            await compute_liquidity_scores()
            await discover_tokens(limit=MAX_TOKENS)
        except Exception as e:
            logger.error("Periodic token discovery failed: %s", e)

async def bandit_decay_loop():
    if BANDIT_DECAY_INTERVAL_SEC <= 0 or not (0.0 < BANDIT_DECAY_GAMMA <= 1.0):
        return
    while not STOP_EVENT.is_set():
        await asyncio.sleep(BANDIT_DECAY_INTERVAL_SEC)
        try:
            await bandit_decay_all(BANDIT_DECAY_GAMMA)
            logger.info("Bandit decay applied with gamma=%s", BANDIT_DECAY_GAMMA)
        except Exception as e:
            logger.debug("bandit_decay_loop error: %s", e)

async def health_handler(request):
    try:
        return web.json_response({
            "ok": True,
            "chainId": await get_cached_chain_id(),
            "wss": METRICS.get("wss","OFF"),
            "metrics": METRICS,
            "ts": int(time.time()),
        })
    except Exception:
        return web.json_response({"ok": False}, status=500)

async def start_health_server():
    app = web.Application()
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HEALTH_HOST, HEALTH_PORT)
    try:
        await site.start()
        logger.info("Health server on http://%s:%s/health", HEALTH_HOST, HEALTH_PORT)
    except Exception as e:
        logger.error("Failed to start health server: %s", e)


def _startup_summary():
    try:
        summary = (
            f"🚀 Startup\n"
            f"  chain={CHAIN_ID} wss_urls={bool(WSS_URL_OVERRIDE or ALCHEMY_WSS or INFURA_WSS)} "
            f"backruns={'ON' if ENABLE_BACKRUNS else 'OFF'} flashbots={'ON' if USE_FLASHBOTS else 'OFF'}\n"
            f"  exec={ARB_EXECUTOR_ADDRESS} owner_env={ARB_EXECUTOR_OWNER}\n"
            f"  acct={'ON (' + ACCOUNT_ADDRESS[:6] + '...)' if ACCOUNT_ADDRESS else 'OFF'}\n"
            f"  consolidate base={CONSOLIDATE_PREFERRED_BASE} min=${CONSOLIDATE_MIN_USD} slip={CONSOLIDATE_SLIPPAGE}"
        )
        logger.info(summary)
    except Exception as e:
        logger.warning("Could not generate startup summary: %s", e)

async def _rpc_probe(label: str, fn: Callable, timeout: float = 8.0):
    try:
        val = await asyncio.wait_for(asyncio.to_thread(fn), timeout=timeout)
        logger.info("Preflight OK: %s -> %s", label, val)
        return True, val, None
    except asyncio.TimeoutError:
        logger.warning("Preflight TIMEOUT: %s after %.1fs", label, timeout)
        return False, None, f"timeout({timeout}s)"
    except Exception as e:
        logger.warning("Preflight ERROR: %s -> %s", label, e)
        return False, None, str(e)

async def preflight():
    global ML_MAIN
    logger.info("Preflight: Starting...")
    ML_MAIN = await _make_ml()
    if _env_bool("PREFLIGHT_DISABLE_ALL", False):
        logger.warning("Preflight: Skipping all checks due to PREFLIGHT_DISABLE_ALL=1")
        return

    ok_chain, chain_id, _ = await _rpc_probe("eth_chainId", lambda: w3.eth.chain_id)
    if ok_chain and int(chain_id) != CHAIN_ID:
        logger.warning("Preflight: Chain ID mismatch! ENV=%s, Node=%s", CHAIN_ID, chain_id)
    
    await _rpc_probe("eth_blockNumber", lambda: w3.eth.block_number)

    if not SKIP_OWNER_CHECK and not _is_zero_address(ARB_EXECUTOR_ADDRESS):
        ok_code, code, _ = await _rpc_probe("executor.code_size", lambda: w3.eth.get_code(ARB_EXECUTOR_ADDRESS))
        if ok_code and len(code or b'') > 2:
            ok_owner, owner_raw, _ = await _rpc_probe("executor.owner()", lambda: exec_mgmt.functions.owner().call())
            if ok_owner:
                onchain_owner = Web3.to_checksum_address(owner_raw)
                if ACCOUNT_ADDRESS and onchain_owner != Web3.to_checksum_address(ACCOUNT_ADDRESS):
                    logger.warning("Preflight: ACCOUNT_ADDRESS %s is NOT executor owner %s", ACCOUNT_ADDRESS, onchain_owner)
                elif ACCOUNT_ADDRESS:
                    logger.info("Preflight: Account address matches executor owner.")
        else:
            logger.error("Preflight: No code found at executor address %s on chain %s. Check config.", ARB_EXECUTOR_ADDRESS, CHAIN_ID)

    if BRIDGE_ENABLED and BRIDGE_TARGET_CHAIN_ID == CHAIN_ID:
        logger.warning("Bridge is enabled, but target chain is the same as current chain. It will not do anything.")

    logger.info("Preflight: Completed.")

async def supervisor(task_factories: Dict[str, Callable[[], Awaitable]], check_interval: int = 20):
    running: Dict[str, asyncio.Task] = {}
    while not STOP_EVENT.is_set():
        for name, factory in task_factories.items():
            t = running.get(name)
            if t is None or t.done():
                if t and t.exception():
                    logger.error("Task '%s' crashed: %s", name, t.exception(), exc_info=True)
                elif t:
                    logger.warning("Task '%s' completed unexpectedly. Restarting.", name)
                
                try:
                    running[name] = asyncio.create_task(factory())
                    logger.info("Started/restarted task: '%s'", name)
                except Exception as e:
                    logger.error("Failed to start task '%s': %s", name, e)
        await asyncio.sleep(check_interval)
   
    logger.info("Supervisor shutting down...")
    for name, t in running.items():
        if not t.done():
            t.cancel()
    await asyncio.gather(*running.values(), return_exceptions=True)
    logger.info("Supervisor shut down gracefully.")


async def scan_loop():
    while not STOP_EVENT.is_set():
        await WSS_READY_EVENT.wait()
        try:
            await arbitrage_strategy(trigger="poll")
        except Exception as e:
            logger.error("scan_loop error: %s", e, exc_info=True)
        
        try:
            await asyncio.wait_for(BLOCK_UPDATE_EVENT.wait(), timeout=max(1.0, SCAN_INTERVAL))
        except asyncio.TimeoutError:
            pass

# ============================ Liquidation Strategy (from ALPHA.py) ============================
# helper: precise user debt (on-chain) + close factor
def _user_total_debt(user: str, debt_asset: str) -> int:
    try:
        # v3 data provider returns (aToken, stableDebt, variableDebt)
        _, sDebt_addr, vDebt_addr = aave_data_provider.functions.getReserveTokensAddresses(
            Web3.to_checksum_address(debt_asset)
        ).call()
        sd = erc20_balance(sDebt_addr, user)
        vd = erc20_balance(vDebt_addr, user)
        return int(sd + vd)
    except Exception:
        return 0

async def _liq_debt_to_cover(user: str, debt_asset: str, debt_dec: int) -> int:
    total = _user_total_debt(user, debt_asset)
    if total <= 0: return 0
    max_cover = int(Decimal(total) * LIQ_CLOSE_FACTOR_PCT)
    # cap to a reasonable USD clip to limit risk
    token_info = TOKENS.get(debt_asset, BASELINE_TOKEN_INFO.get(debt_asset, {"symbol":"WETH"}))
    px = get_oracle_price(token_info["symbol"])
    clip = int((Decimal("2000")/max(px, Decimal("0.0001"))) * Decimal(10**debt_dec))
    return max(0, min(max_cover, clip))

async def run_liquidation_cycle():
    if not aave_data_provider or not ACCOUNT_ADDRESS:
        logger.debug("Liquidation cycle skipped: Aave data provider or account address not set.")
        return

    logger.info("Starting liquidation scan...", extra={"ctx": {"strategy": "liquidation"}})
    
    query = """
    {
      users(
        first: 50,
        orderBy: createdAt,
        orderDirection: desc,
        where: {borrowedReservesCount_gt: 0, healthFactor_lt: "1000000000000000000"}
      ) {
        id
        reserves {
          reserve {
            symbol
            underlyingAsset
            decimals
            liquidationBonus
          }
          usageAsCollateralEnabledOnUser
        }
      }
    }
    """
    data = await subgraph_query(SUBG_AAVE, query)
    subgraph_users = data.get("data", {}).get("users", [])
    if not subgraph_users:
        logger.info("No active liquidatable borrowers found in subgraph scan.", extra={"ctx": {"strategy": "liquidation"}})
        return

    for user_data in subgraph_users:
        user_address = Web3.to_checksum_address(user_data["id"])
        
        try:
            collaterals = [r for r in user_data["reserves"] if r["usageAsCollateralEnabledOnUser"]]
            debts = [r for r in user_data["reserves"] if not r["usageAsCollateralEnabledOnUser"]]

            for debt_reserve in debts:
                for collateral_reserve in collaterals:
                    debt_asset = Web3.to_checksum_address(debt_reserve["reserve"]["underlyingAsset"])
                    collateral_asset = Web3.to_checksum_address(collateral_reserve["reserve"]["underlyingAsset"])
                    
                    if debt_asset == collateral_asset: continue
                    
                    debt_dec = int(debt_reserve["reserve"]["decimals"])
                    coll_dec = int(collateral_reserve["reserve"]["decimals"])
                    debt_to_cover = await _liq_debt_to_cover(user_address, debt_asset, debt_dec)
                    if debt_to_cover == 0: 
                        continue

                    # expected collateral to seize (rough but correct direction):
                    debt_px = get_oracle_price(debt_reserve["reserve"]["symbol"]) or Decimal("0")
                    coll_px = get_oracle_price(collateral_reserve["reserve"]["symbol"]) or Decimal("0")
                    if debt_px == 0 or coll_px == 0: 
                        continue

                    bonus_bps = int(collateral_reserve["reserve"]["liquidationBonus"])
                    bonus = Decimal(bonus_bps) / Decimal(10000)  # usually 1.05–1.1
                    coll_to_seize = (Decimal(debt_to_cover) / Decimal(10**debt_dec)) * (debt_px / coll_px) * bonus
                    coll_to_seize_tokens = int(coll_to_seize * Decimal(10**coll_dec))

                    # choose swap path (collateral -> debt) for repayment loop
                    fees, _ = await choose_univ3_fees_for_path([collateral_asset, debt_asset], coll_to_seize_tokens)
                    if not fees:
                        continue
                    uni_path = encode_univ3_path([collateral_asset, debt_asset], [fees[0]])

                    min_profit_usd = LIQ_MIN_PROFIT_USD
                    min_profit_tokens = int((min_profit_usd / debt_px) * Decimal(10**debt_dec)) if debt_px > 0 else 0

                    deadline = chain_deadline(180)

                    payload = encode_liquidation_payload(
                        collateral=collateral_asset,
                        debt=debt_asset,
                        user=user_address,
                        debt_to_cover=debt_to_cover,
                        swap_path=[collateral_asset, debt_asset],   # executor uses uniV3Path primarily
                        dex_name="UNISWAP_V3",
                        min_profit=min_profit_tokens,
                        deadline=deadline,
                        uni_v3_path=uni_path
                    )

                    try:
                        tx = exec_c.functions.requestFlashLoan(
                            int(FlashloanProvider["AAVE"]),
                            Web3.to_checksum_address(debt_asset),
                            int(debt_to_cover),
                            payload,
                            int(deadline)
                        ).build_transaction(await tx_opts_base_async(fast_gas=True))
                        
                        gas_cost_usd = _usd_cost_for_tx(tx)
                        if min_profit_usd < gas_cost_usd:
                            logger.info("Liquidation profit $%.2f less than gas cost $%.2f, skipping.", min_profit_usd, gas_cost_usd)
                            continue

                        if simulate_tx(tx):
                            ok, txh = await send_tx_async(tx)
                            if ok and txh:
                                METRICS["liqs_total"] += 1
                                net_profit = min_profit_usd - gas_cost_usd
                                await monitor_tx(txh, "liquidation", {
                                    "token": debt_asset, "amount": debt_to_cover,
                                    "gross_usd": float(min_profit_usd), "net_usd": float(net_profit)
                                })
                                return # Found and executed one, break for this cycle
                    except Exception as e:
                        logger.error("Liquidation flashloan path failed: %s", e)

        except Exception as e:
            logger.error("Error processing user %s for liquidation: %s", user_address, e, exc_info=True)


async def start_liquidations():
    if not ENABLE_LIQUIDATIONS:
        logger.info("Liquidations are disabled by config.")
        return
    while not STOP_EVENT.is_set():
        await WSS_READY_EVENT.wait()
        try:
            await run_liquidation_cycle()
        except Exception as e:
            logger.error("liquidations error: %s", e, exc_info=True)
        await asyncio.sleep(60)


async def profit_recovery_loop():
    if not AUTO_CONSOLIDATE_PROFITS:
        return
    
    # Wait 10 seconds on startup before the first run
    await asyncio.sleep(10)
    
    interval = max(60, PROFIT_RECOVERY_INTERVAL_SEC)
    while not STOP_EVENT.is_set():
        try:
            logger.info("Starting profit recovery and consolidation cycle...")
            await recover_profits_once()
            logger.info("Profit recovery cycle finished. Waiting for next interval.")
        except Exception as e:
            logger.error("profit_recovery_loop error: %s", e, exc_info=True)
        await asyncio.sleep(interval)

# ============================ Main bootstrap ============================
async def main():
    supervisor_task = None
    bg_tasks = []
    try:
        await preflight()
        _startup_summary()
        await start_health_server()

        # Run WSS initializer in the background, don't block startup
        wss_init_task = asyncio.create_task(_auto_wss_initializer())
        bg_tasks.append(wss_init_task)
        
        bg_tasks.extend([
            asyncio.create_task(token_discovery_loop()),
            asyncio.create_task(bandit_decay_loop()),
            asyncio.create_task(block_header_loop()),
            asyncio.create_task(mempool_loop()),
            asyncio.create_task(telegram_heartbeat()),
        ])

        task_factories: Dict[str, Callable[[], Awaitable]] = {
            "arbitrage_scan": scan_loop,
            "profit_recovery": profit_recovery_loop,
        }
        if ENABLE_LIQUIDATIONS:
            task_factories["liquidations"] = start_liquidations

        supervisor_task = asyncio.create_task(supervisor(task_factories))

        loop = asyncio.get_running_loop()
        for sig_name in ("SIGINT", "SIGTERM"):
            if hasattr(signal, sig_name):
                try:
                    loop.add_signal_handler(getattr(signal, sig_name), STOP_EVENT.set)
                except NotImplementedError:
                    logger.warning("Signal handlers not supported on this platform.")
        
        logger.info("Bot is now running. Press Ctrl+C to stop.")
        await STOP_EVENT.wait()

    except Exception as e:
        logger.critical("FATAL: Unhandled exception in main(): %s", e, exc_info=True)
    finally:
        logger.info("Shutdown sequence initiated...")
        STOP_EVENT.set()

        all_tasks = bg_tasks
        if supervisor_task:
            all_tasks.append(supervisor_task)

        for t in all_tasks:
            if not t.done():
                t.cancel()
        
        await asyncio.gather(*all_tasks, return_exceptions=True)

        await close_http_session()
        if conn:
            conn.close()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt caught, shutting down.")
