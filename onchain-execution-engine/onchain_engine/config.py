"""Environment-driven configuration. Importing this module reads env vars only:
it opens no connections and loads no keys into memory beyond the env string."""
import os
from decimal import Decimal
from typing import List

from web3 import Web3

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
WSS_SUBSCRIBE_ORDER = [s.strip() for s in os.getenv("WSS_SUBSCRIBE_ORDER", "pending,newHeads").split(",") if s.strip()]
WSS_ACCEPT_ON_PROBE = _env_bool("WSS_ACCEPT_ON_PROBE", True)
HTTP_URL = _first_env("ALCHEMY_HTTP", "INFURA_HTTP", "CHAINSTACK_HTTP", "LAVA_HTTP")

# ============================ Keys & contract ============================
PRIVATE_KEY   = os.getenv("PRIVATE_KEY", "").strip()
ALLOW_LIVE_TRANSACTIONS = _env_bool("ALLOW_LIVE_TRANSACTIONS", False)
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
