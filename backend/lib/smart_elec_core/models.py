# backend/lib/smart_elec_core/models.py
from dataclasses import dataclass
from datetime import datetime

@dataclass
class MeterReading:
    device_id: str
    timestamp: datetime
    kwh: float
