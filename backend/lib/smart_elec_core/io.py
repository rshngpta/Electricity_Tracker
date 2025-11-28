# backend/lib/smart_elec_core/io.py
import csv
from datetime import datetime
from typing import List
from .models import MeterReading
from io import StringIO

def parse_csv_string(csv_text: str) -> List[MeterReading]:
    """
    Parse CSV text with header: device_id,timestamp,kwh
    Timestamp should be ISO8601, e.g. 2025-11-01T00:00:00Z
    """
    f = StringIO(csv_text.strip())
    reader = csv.DictReader(f)
    readings = []
    for row in reader:
        # Basic validation
        if not row.get('device_id') or not row.get('timestamp') or not row.get('kwh'):
            raise ValueError(f"Missing field in row: {row}")
        # Convert timestamp with Z to +00:00 for fromisoformat
        ts_text = row['timestamp'].replace("Z", "+00:00")
        timestamp = datetime.fromisoformat(ts_text)
        kwh = float(row['kwh'])
        if kwh < 0:
            raise ValueError("kwh must be >= 0")
        readings.append(MeterReading(device_id=row['device_id'], timestamp=timestamp, kwh=kwh))
    return readings
