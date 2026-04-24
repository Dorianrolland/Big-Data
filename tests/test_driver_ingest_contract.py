"""Driver ingest contract checks."""
from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path


_INGEST_DIR = Path(__file__).resolve().parent.parent / "driver_ingest"
if str(_INGEST_DIR) not in sys.path:
    sys.path.insert(0, str(_INGEST_DIR))
os.environ.setdefault("DRIVER_INGEST_ALLOW_INSECURE_DEV", "true")
_SPEC = importlib.util.spec_from_file_location("driver_ingest_main_contract", _INGEST_DIR / "main.py")
assert _SPEC and _SPEC.loader
driver_ingest = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = driver_ingest
_SPEC.loader.exec_module(driver_ingest)


def test_position_contract_accepts_operational_statuses():
    for status in ("pickup_assigned", "pickup_en_route", "repositioning"):
        position = driver_ingest.PositionIn(
            courier_id="drv_demo_001",
            lat=40.7580,
            lon=-73.9855,
            status=status,
        )
        assert position.status == status
