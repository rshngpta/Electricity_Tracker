# backend/lib/smart_elec_core/estimator.py
from typing import Dict, Iterable
from decimal import Decimal, ROUND_HALF_UP

class BillingEstimator:
    def __init__(self, tariff_rate_per_kwh: float = 0.20):
        """
        tariff_rate_per_kwh: flat rate in currency units per kWh (e.g., EUR/kWh)
        """
        self.rate = float(tariff_rate_per_kwh)

    def estimate_cost(self, usage_by_period: Dict[str, float]) -> float:
        """
        usage_by_period: dict like {'2025-11-01': 3.4, ...}
        returns total cost rounded to 2 decimals
        """
        total_kwh = sum(float(v) for v in usage_by_period.values())
        cost = total_kwh * self.rate
        # round to 2 decimal places (banker's rounding avoided; use ROUND_HALF_UP)
        rounded = float(Decimal(cost).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
        return rounded
