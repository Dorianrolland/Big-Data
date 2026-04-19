from __future__ import annotations

from datetime import datetime
import importlib.util
from pathlib import Path
from zoneinfo import ZoneInfo
import sys

_CTX_DIR = Path(__file__).resolve().parent.parent / "context_poller"
if str(_CTX_DIR) not in sys.path:
    sys.path.insert(0, str(_CTX_DIR))
_CTX_MAIN = _CTX_DIR / "main.py"
_CTX_SPEC = importlib.util.spec_from_file_location("context_poller_main", _CTX_MAIN)
assert _CTX_SPEC and _CTX_SPEC.loader
ctx = importlib.util.module_from_spec(_CTX_SPEC)
sys.modules.setdefault("context_poller_main", ctx)
_CTX_SPEC.loader.exec_module(ctx)  # type: ignore[attr-defined]  # noqa: E402


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


def test_traffic_factor_fallback_without_dot_signals_matches_legacy_base():
    state = ctx.ContextState()
    state.weather_precip_mm = 1.8
    state.nyc311_incidents = [(40.7580, -73.9855)]
    state.dot_closure_records = []
    state.dot_speed_samples = []
    now_ts = datetime(2026, 4, 16, 10, 15, tzinfo=ZoneInfo("America/New_York")).timestamp()

    actual = ctx._compute_traffic_factor(state, 40.7580, -73.9855, now_ts=now_ts)
    expected = round(
        min(
            ctx.TRAFFIC_FACTOR_CAP,
            1.0
            + min(0.45, 1.0 / 12.0)
            + min(0.2, 1.8 / 6.0)
            + ctx._rush_hour_factor(now_ts),
        ),
        3,
    )
    assert actual == expected


def test_closure_pressure_higher_for_nearby_zone():
    now_ts = datetime(2026, 4, 19, 9, 0, tzinfo=ZoneInfo("UTC")).timestamp()
    closures = [
        {
            "lat": 40.7580,
            "lon": -73.9855,
            "start_ts": now_ts - 600.0,
            "end_ts": now_ts + 4 * 3600.0,
            "severity": 1.0,
        }
    ]
    near = ctx._compute_closure_pressure(closures, lat=40.7581, lon=-73.9854, now_ts=now_ts)
    far = ctx._compute_closure_pressure(closures, lat=40.6500, lon=-74.1000, now_ts=now_ts)
    assert near > 0.0
    assert near > far


def test_speed_pressure_increases_when_speed_is_low():
    slow = [{"lat": 40.7580, "lon": -73.9855, "speed_kmh": 9.0, "severity": ctx._speed_severity(9.0)}]
    fast = [{"lat": 40.7580, "lon": -73.9855, "speed_kmh": 45.0, "severity": ctx._speed_severity(45.0)}]

    slow_pressure = ctx._compute_speed_pressure(slow, lat=40.7580, lon=-73.9855)
    fast_pressure = ctx._compute_speed_pressure(fast, lat=40.7580, lon=-73.9855)
    assert slow_pressure > fast_pressure
    assert fast_pressure == 0.0


def test_traffic_factor_fusion_penalizes_impacted_zone():
    state = ctx.ContextState()
    state.weather_precip_mm = 0.0
    state.nyc311_incidents = []
    now_ts = datetime(2026, 4, 19, 13, 0, tzinfo=ZoneInfo("UTC")).timestamp()
    state.dot_closure_records = [
        {
            "lat": 40.7580,
            "lon": -73.9855,
            "start_ts": now_ts - 900.0,
            "end_ts": now_ts + 2 * 3600.0,
            "severity": 1.1,
        }
    ]
    state.dot_speed_samples = [
        {"lat": 40.7580, "lon": -73.9855, "speed_kmh": 8.0, "severity": ctx._speed_severity(8.0)}
    ]

    impacted = ctx._compute_traffic_factor(state, 40.7580, -73.9855, now_ts=now_ts)
    baseline = ctx._compute_traffic_factor(state, 40.6400, -74.1000, now_ts=now_ts)
    assert impacted > baseline


def test_extract_closure_records_from_weekly_advisory_html():
    raw_html = """
        <html><body>
        <h2>Weekly Traffic Advisory for Saturday April 18, 2026, to Friday April 24, 2026</h2>
        <h3>Manhattan</h3>
        <p>Northbound West Street between West 30th and West 34th Street will be closed nightly.</p>
        <h3>Queens</h3>
        <p>Single lane closure on Queensboro Bridge in effect overnight.</p>
        </body></html>
    """
    now_ts = datetime(2026, 4, 19, 9, 0, tzinfo=ZoneInfo("UTC")).timestamp()
    rows = ctx._extract_closure_records_from_weekly_advisory(raw_html, now_ts=now_ts)
    assert len(rows) >= 2
    assert all(float(row["end_ts"]) > float(row["start_ts"]) for row in rows)


def test_extract_closure_records_handles_cross_borough_heading():
    raw_html = """
        <html><body>
        <h2>Weekly Traffic Advisory for Saturday April 18, 2026, to Friday April 24, 2026</h2>
        <h3>Bronx/Manhattan</h3>
        <p>Broadway Bridge over the Harlem River has a single lane closure.</p>
        </body></html>
    """
    now_ts = datetime(2026, 4, 19, 9, 0, tzinfo=ZoneInfo("UTC")).timestamp()
    rows = ctx._extract_closure_records_from_weekly_advisory(raw_html, now_ts=now_ts)
    assert len(rows) == 1
    assert rows[0]["severity"] > 0.0


def test_extract_speed_samples_from_text_rows():
    raw = "40.7580,-73.9855,12.0\n-73.99,40.75,21.0\ninvalid,row"
    rows = ctx._extract_speed_samples_from_text(raw)
    assert len(rows) == 2
    assert rows[0]["severity"] > 0.0


def test_extract_speed_samples_from_text_with_extra_columns():
    raw = "segment_01,2026-04-19T09:00:00Z,-73.9900,40.7500,18.0,ok"
    rows = ctx._extract_speed_samples_from_text(raw)
    assert len(rows) == 1
    assert rows[0]["speed_kmh"] > 20.0


def test_normalize_speed_to_kmh_respects_unit_hint():
    assert round(ctx._normalize_speed_to_kmh(20.0, speed_key="avg_speed_mph"), 3) == round(20.0 * 1.60934, 3)
    assert ctx._normalize_speed_to_kmh(20.0, speed_key="speed_kmh") == 20.0


def test_event_pressure_is_higher_for_nearby_zone():
    now_ts = datetime(2026, 4, 18, 10, 0, tzinfo=ZoneInfo("UTC")).timestamp()
    events = [
        {
            "lat": 40.7580,
            "lon": -73.9855,
            "start_ts": now_ts - 1200.0,
            "end_ts": now_ts + 1800.0,
            "intensity": 1.2,
        }
    ]
    near = ctx._compute_event_pressure(events, lat=40.7581, lon=-73.9856, now_ts=now_ts)
    far = ctx._compute_event_pressure(events, lat=40.6480, lon=-74.0200, now_ts=now_ts)

    assert near > 0.0
    assert near > far


def test_event_time_weight_increases_when_start_gets_closer():
    start_ts = datetime(2026, 4, 18, 12, 0, tzinfo=ZoneInfo("UTC")).timestamp()
    end_ts = start_ts + 5400.0
    now_far = start_ts - 5 * 3600.0
    now_near = start_ts - 30 * 60.0

    far_weight = ctx._event_time_weight(
        now_ts=now_far,
        start_ts=start_ts,
        end_ts=end_ts,
        lookahead_hours=6.0,
        post_window_minutes=60.0,
    )
    near_weight = ctx._event_time_weight(
        now_ts=now_near,
        start_ts=start_ts,
        end_ts=end_ts,
        lookahead_hours=6.0,
        post_window_minutes=60.0,
    )

    assert near_weight > far_weight
    assert 0.0 <= far_weight <= 1.0


def test_event_pressure_drops_to_zero_after_post_window():
    now_ts = datetime(2026, 4, 18, 10, 0, tzinfo=ZoneInfo("UTC")).timestamp()
    events = [
        {
            "lat": 40.7580,
            "lon": -73.9855,
            "start_ts": now_ts - 7200.0,
            "end_ts": now_ts - 3700.0,
            "intensity": 1.0,
        }
    ]
    pressure = ctx._compute_event_pressure(
        events,
        lat=40.7580,
        lon=-73.9855,
        now_ts=now_ts,
        post_window_minutes=60.0,
    )
    assert pressure == 0.0


def test_event_pressure_is_zero_without_events_data():
    now_ts = datetime(2026, 4, 18, 10, 0, tzinfo=ZoneInfo("UTC")).timestamp()
    assert ctx._compute_event_pressure([], lat=40.7580, lon=-73.9855, now_ts=now_ts) == 0.0


def test_event_pressure_is_capped_under_dense_signals():
    now_ts = datetime(2026, 4, 18, 10, 0, tzinfo=ZoneInfo("UTC")).timestamp()
    events = [
        {
            "lat": 40.7580,
            "lon": -73.9855,
            "start_ts": now_ts - 300.0,
            "end_ts": now_ts + 3600.0,
            "intensity": 1.4,
        }
        for _ in range(16)
    ]
    pressure = ctx._compute_event_pressure(events, lat=40.7580, lon=-73.9855, now_ts=now_ts)
    assert pressure == ctx.EVENT_PRESSURE_CAP


def test_event_pressure_ignores_invalid_event_rows():
    now_ts = datetime(2026, 4, 18, 10, 0, tzinfo=ZoneInfo("UTC")).timestamp()
    events = [
        {"lat": None, "lon": -73.98, "start_ts": now_ts - 200.0, "end_ts": now_ts + 900.0, "intensity": 1.0},
        {"lat": 40.75, "lon": None, "start_ts": now_ts - 200.0, "end_ts": now_ts + 900.0, "intensity": 1.0},
        {"lat": 40.75, "lon": -73.98, "start_ts": None, "end_ts": now_ts + 900.0, "intensity": 1.0},
    ]
    pressure = ctx._compute_event_pressure(events, lat=40.7580, lon=-73.9855, now_ts=now_ts)
    assert pressure == 0.0


def test_zone_demand_applies_event_pressure_with_cap():
    state = ctx.ContextState()
    state.gbfs_stations = []
    demand = ctx._compute_zone_demand(state, 40.7580, -73.9855, event_pressure=1.8)

    assert demand == round(min(ctx.DEMAND_INDEX_CAP, 1.0 + 1.8), 3)


def test_zone_demand_applies_temporal_pressure_with_cap():
    state = ctx.ContextState()
    state.gbfs_stations = []
    demand = ctx._compute_zone_demand(
        state,
        40.7580,
        -73.9855,
        event_pressure=0.3,
        temporal_pressure=0.14,
    )
    assert demand == round(min(ctx.DEMAND_INDEX_CAP, 1.0 + 0.3 + 0.14), 3)


def test_weather_intensity_score_increases_with_harsher_weather():
    mild = ctx._weather_intensity_score(precip_mm=0.0, wind_kmh=5.0)
    harsh = ctx._weather_intensity_score(precip_mm=5.0, wind_kmh=45.0)
    assert 0.0 <= mild <= 1.0
    assert 0.0 <= harsh <= 1.0
    assert harsh > mild


def test_temporal_context_marks_peak_and_holiday():
    # 2026-07-03 is the observed US federal holiday for Independence Day.
    ts = datetime(2026, 7, 3, 8, 30, tzinfo=ZoneInfo("America/New_York")).timestamp()
    temporal = ctx._temporal_context(ts)
    assert temporal["is_peak_hour"] is True
    assert temporal["is_holiday"] is True
    assert float(temporal["temporal_pressure"]) > 0.0


def test_is_holiday_local_handles_year_boundary_observed_day():
    # Jan 1, 2022 was Saturday => observed Friday Dec 31, 2021.
    dt_local = datetime(2021, 12, 31, 12, 0, tzinfo=ZoneInfo("America/New_York"))
    assert ctx._is_holiday_local(dt_local) is True


def test_freshness_snapshot_marks_missing_sources_stale():
    state = ctx.ContextState()
    snap = ctx._freshness_snapshot(state, now_ts=datetime(2026, 4, 19, 10, 0, tzinfo=ZoneInfo("UTC")).timestamp())
    assert snap["fallback_applied"] is True
    assert int(snap["stale_count"]) >= 1
    assert bool(snap["stale"]["weather"]) is True


def test_freshness_snapshot_marks_recent_sources_fresh():
    now_ts = datetime(2026, 4, 19, 10, 0, tzinfo=ZoneInfo("UTC")).timestamp()
    state = ctx.ContextState()
    state.gbfs_last_updated = now_ts - 10.0
    state.weather_last_updated = now_ts - 30.0
    state.nyc311_last_updated = now_ts - 20.0
    state.events_last_updated = now_ts - 45.0
    state.dot_closure_last_updated = now_ts - 60.0
    state.dot_speeds_last_updated = now_ts - 25.0
    snap = ctx._freshness_snapshot(state, now_ts=now_ts)
    assert snap["fallback_applied"] is False
    assert int(snap["stale_count"]) == 0
    assert bool(snap["stale"]["weather"]) is False
    assert bool(snap["stale"]["events"]) is False


def test_build_source_value_includes_temporal_and_freshness():
    source = ctx._build_source_value(
        "gbfs+open-meteo",
        {
            "event_pressure": 0.25,
            "temporal_is_peak": True,
            "freshness_policy": "stale_neutral_v1",
            "freshness_stale_sources": 2,
        },
    )
    assert source.startswith("gbfs+open-meteo;")
    assert "event_pressure=0.25" in source
    assert "temporal_is_peak=1" in source
    assert "freshness_policy=stale_neutral_v1" in source
    assert "freshness_stale_sources=2" in source


def test_build_event_record_uses_borough_centroid_and_parses_times():
    row = {
        "event_id": "ev_1",
        "event_name": "Queens Festival",
        "event_type": "Festival",
        "event_borough": "Queens",
        "event_location": "Some Park",
        "start_date_time": "2026-04-18T13:00:00.000",
        "end_date_time": "2026-04-18T16:00:00.000",
    }
    evt = ctx._build_event_record(row)
    assert evt is not None
    assert evt["lat"] == ctx.BOROUGH_CENTROIDS["queens"][0]
    assert evt["lon"] == ctx.BOROUGH_CENTROIDS["queens"][1]
    assert float(evt["end_ts"]) > float(evt["start_ts"])


def test_build_event_record_rejects_unknown_borough():
    row = {
        "event_id": "ev_unknown",
        "event_name": "Unknown",
        "event_type": "Festival",
        "event_borough": "Atlantis",
        "start_date_time": "2026-04-18T13:00:00.000",
        "end_date_time": "2026-04-18T16:00:00.000",
    }
    assert ctx._build_event_record(row) is None
