from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Tuple
from .models import MeterReading

class EnergyAnalyzer:
    def __init__(self, readings: List[MeterReading]):
        # Ensure readings are sorted by timestamp
        self.readings = sorted(readings, key=lambda r: (r.device_id, r.timestamp))

    def daily_usage(self) -> Dict[str, float]:
        """
        Returns a dict keyed by 'YYYY-MM-DD' -> total_kwh.

        Assumes readings.kwh are interval values (not cumulative meter readings).
        Sums provided kwh values per day.
        """
        daily = defaultdict(float)
        for r in self.readings:
            key_date = r.timestamp.strftime("%Y-%m-%d")
            daily[key_date] += r.kwh
        return dict(daily)

    def monthly_usage(self) -> Dict[str, float]:
        """
        Aggregates the daily_usage into monthly totals (YYYY-MM).
        """
        daily = self.daily_usage()
        monthly = defaultdict(float)
        for day_str, kwh in daily.items():
            month = day_str[:7]  # YYYY-MM
            monthly[month] += kwh
        return dict(monthly)

    def detect_spikes(self, threshold_pct: float = 50.0) -> List[Tuple[str, float, float]]:
        """
        Detects spikes where day N increased by more than threshold_pct compared to previous day.
        Returns list of tuples: (date_str, prev_total, curr_total)
        """
        daily = self.daily_usage()
        items = sorted(daily.items())
        spikes = []
        for i in range(1, len(items)):
            prev_date, prev_val = items[i-1]
            curr_date, curr_val = items[i]
            if prev_val == 0:
                continue
            change_pct = (curr_val - prev_val) / prev_val * 100
            if change_pct > threshold_pct:
                spikes.append((curr_date, round(prev_val, 4), round(curr_val, 4)))
        return spikes
