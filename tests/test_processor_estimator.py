# tests/test_processor_estimator.py
from backend.lib.smart_elec_core.models import MeterReading
from backend.lib.smart_elec_core.processor import EnergyAnalyzer
from backend.lib.smart_elec_core.estimator import BillingEstimator
from datetime import datetime, timezone

def make_readings():
    # Create sample readings across two days
    return [
        MeterReading("d1", datetime(2025,11,1,0,0,tzinfo=timezone.utc), 1.0),
        MeterReading("d1", datetime(2025,11,1,1,0,tzinfo=timezone.utc), 1.5),
        MeterReading("d1", datetime(2025,11,2,0,0,tzinfo=timezone.utc), 5.0),  # spike
        MeterReading("d1", datetime(2025,11,2,1,0,tzinfo=timezone.utc), 2.0),
    ]

def test_daily_and_spike_detection():
    readings = make_readings()
    analyzer = EnergyAnalyzer(readings)
    daily = analyzer.daily_usage()
    assert daily["2025-11-01"] == 2.5
    assert daily["2025-11-02"] == 7.0
    spikes = analyzer.detect_spikes(threshold_pct=50.0)
    # prev=2.5, curr=7.0 -> change = 180% -> should be flagged
    assert len(spikes) == 1
    assert spikes[0][0] == "2025-11-02"

def test_estimator():
    estimator = BillingEstimator(tariff_rate_per_kwh=0.25)
    usage = {"2025-11-01": 2.5, "2025-11-02": 7.0}
    cost = estimator.estimate_cost(usage)
    # total kwh = 9.5 * 0.25 = 2.375 -> rounds to 2.38
    assert cost == 2.38
