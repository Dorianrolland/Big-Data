from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import sys

_CTX_DIR = Path(__file__).resolve().parent.parent / "context_poller"
if str(_CTX_DIR) not in sys.path:
    sys.path.insert(0, str(_CTX_DIR))

import main as ctx  # noqa: E402


def test_supply_index_scales_with_member_count():
    assert ctx._supply_index_from_member_count(0) == 0.2
    assert ctx._supply_index_from_member_count(1) == 0.25
    assert ctx._supply_index_from_member_count(4) == 1.0
    assert ctx._supply_index_from_member_count(20) == 5.0


def test_traffic_factor_has_rush_hour_floor():
    state = ctx.ContextState()
    state.weather_precip_mm = 0.0
    state.nyc311_incidents = []
    rush_ts = datetime(2026, 4, 16, 8, 30, tzinfo=ZoneInfo("America/New_York")).timestamp()

    traffic = ctx._compute_traffic_factor(state, 40.758, -73.985, now_ts=rush_ts)
    assert traffic > 1.0

