"""Pure gas-cost arithmetic, separated so the money-safety rule is unit-tested.

The original script returned a USD cost of 0 whenever the ETH price feed or
gas price was missing, which made every candidate trade look profitable.
Here a missing input is an error and the caller must skip the trade.
"""
from decimal import Decimal
from typing import Optional


class GasCostUnavailable(RuntimeError):
    """Gas cost cannot be priced; callers must not treat it as free."""


def gas_cost_usd(gas_units: int, gas_price_wei: int, eth_usd: Optional[Decimal], margin: float = 1.3) -> Decimal:
    if gas_units is None or int(gas_units) <= 0:
        raise GasCostUnavailable("gas estimate missing")
    if gas_price_wei is None or int(gas_price_wei) <= 0:
        raise GasCostUnavailable("gas price unavailable")
    if eth_usd is None or Decimal(eth_usd) <= 0:
        raise GasCostUnavailable("ETH/USD price unavailable")
    if margin < 1:
        raise ValueError("margin must be >= 1")
    eth = Decimal(int(gas_units)) * Decimal(int(gas_price_wei)) / Decimal(10**18)
    return eth * Decimal(eth_usd) * Decimal(str(margin))
