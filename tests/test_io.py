# tests/test_io.py
from backend.lib.smart_elec_core.io import parse_csv_string
import pathlib

def test_parse_sample_csv():
    p = pathlib.Path(__file__).parent / "sample.csv"
    text = p.read_text()
    readings = parse_csv_string(text)
    assert len(readings) == 3
    assert readings[0].device_id == "device-001"
    assert readings[0].kwh == 0.34
