# backend/run_local.py
from backend.lib.smart_elec_core.io import parse_csv_string
import sys
from pathlib import Path

def main(csv_path):
    text = Path(csv_path).read_text()
    readings = parse_csv_string(text)
    print(f"Parsed {len(readings)} readings:")
    for r in readings:
        print(f" - {r.device_id} @ {r.timestamp.isoformat()} : {r.kwh} kWh")

if __name__ == "__main__":
    csv = sys.argv[1] if len(sys.argv) > 1 else "tests/sample.csv"
    main(csv)
