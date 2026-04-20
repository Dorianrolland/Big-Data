from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from ml.train_copilot_model import build_training_frame


def _write_events_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    escaped = str(path).replace("\\", "/").replace("'", "''")
    conn = duckdb.connect(":memory:")
    conn.execute(
        f"""
        COPY (
            SELECT * FROM (
                VALUES
                    (
                        'order.offer.v1', 'off_1', 'drv_1', 'zone_a',
                        TIMESTAMPTZ '2024-01-10 10:00:00+00',
                        20.0, 4.5, 18.0, 1.2, 1.0, 1.0, NULL, NULL, NULL
                    ),
                    (
                        'order.offer.v1', 'off_2', 'drv_1', 'zone_a',
                        TIMESTAMPTZ '2024-01-10 10:05:00+00',
                        13.0, 3.5, 16.0, 1.0, 1.0, 1.0, NULL, NULL, NULL
                    ),
                    (
                        'order.event.v1', 'off_1', NULL, NULL,
                        TIMESTAMPTZ '2024-01-10 10:02:00+00',
                        NULL, NULL, NULL, NULL, NULL, NULL, 'accepted', NULL, NULL
                    ),
                    (
                        'order.event.v1', 'off_1', NULL, NULL,
                        TIMESTAMPTZ '2024-01-10 10:20:00+00',
                        NULL, NULL, NULL, NULL, NULL, NULL, 'dropped_off', 24.0, NULL
                    ),
                    (
                        'order.event.v1', 'off_2', NULL, NULL,
                        TIMESTAMPTZ '2024-01-10 10:06:00+00',
                        NULL, NULL, NULL, NULL, NULL, NULL, 'rejected', NULL, NULL
                    ),
                    (
                        'context.signal.v1', NULL, NULL, 'zone_a',
                        TIMESTAMPTZ '2024-01-10 09:58:00+00',
                        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0.9
                    ),
                    (
                        'context.signal.v1', NULL, NULL, 'zone_a',
                        TIMESTAMPTZ '2024-01-10 10:03:00+00',
                        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0.8
                    )
            ) AS t(
                event_type,
                offer_id,
                courier_id,
                zone_id,
                ts,
                estimated_fare_eur,
                estimated_distance_km,
                estimated_duration_min,
                demand_index,
                weather_factor,
                traffic_factor,
                status,
                actual_fare_eur,
                supply_index
            )
        ) TO '{escaped}' (FORMAT PARQUET)
        """
    )
    conn.close()


def test_build_training_frame_groups_outcomes_and_context_lookup(tmp_path: Path) -> None:
    events_root = tmp_path / "events"
    _write_events_parquet(events_root / "sample.parquet")

    df = build_training_frame(
        events_root=events_root,
        revenue_threshold_eur_h=20.0,
        context_window_minutes=60,
        positive_quantile=0.6,
        train_window=(
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 2, 1, tzinfo=timezone.utc),
        ),
    )

    assert set(df["offer_id"]) == {"off_1", "off_2"}
    by_offer = df.set_index("offer_id")

    assert int(by_offer.loc["off_1", "accepted_flag"]) == 1
    assert int(by_offer.loc["off_1", "dropped_off_flag"]) == 1
    assert int(by_offer.loc["off_1", "rejected_flag"]) == 0
    assert by_offer.loc["off_1", "label_source"] == "observed_realized"

    assert int(by_offer.loc["off_2", "rejected_flag"]) == 1
    assert by_offer.loc["off_2", "label_source"] == "observed_rejected"

    assert by_offer.loc["off_1", "supply_index"] == pytest.approx(0.9)
    assert by_offer.loc["off_2", "supply_index"] == pytest.approx(0.8)
    assert by_offer.loc["off_1", "context_fallback_applied"] == pytest.approx(0.0)
    assert by_offer.loc["off_2", "context_fallback_applied"] == pytest.approx(0.0)


def test_build_training_frame_honors_train_window(tmp_path: Path) -> None:
    events_root = tmp_path / "events"
    _write_events_parquet(events_root / "sample.parquet")

    df = build_training_frame(
        events_root=events_root,
        revenue_threshold_eur_h=20.0,
        context_window_minutes=30,
        positive_quantile=0.6,
        train_window=(
            datetime(2024, 3, 1, tzinfo=timezone.utc),
            datetime(2024, 4, 1, tzinfo=timezone.utc),
        ),
    )

    assert df.empty
