from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import datetime

EARTH_RADIUS_KM = 6371.0


@dataclass(frozen=True)
class MotionSegment:
    start_s: float
    end_s: float
    progress_start: float
    progress_end: float
    paused: bool = False


@dataclass(frozen=True)
class MotionSchedule:
    segments: tuple[MotionSegment, ...]
    total_duration_s: float


def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


def _bearing_delta_deg(first: float, second: float) -> float:
    return abs(((second - first + 180.0) % 360.0) - 180.0)


def _seed_fraction(seed: str, salt: str) -> float:
    token = f"{seed}|{salt}"
    digest = hashlib.sha1(token.encode("utf-8", errors="ignore")).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


def _congestion_factor(reference_ts: datetime, status: str) -> float:
    factor = 1.0
    hour = int(reference_ts.hour)
    weekday = int(reference_ts.weekday())
    is_weekend = weekday >= 5

    if 7 <= hour < 10 or 16 <= hour < 19:
        factor = 1.35
    elif 11 <= hour < 14:
        factor = 1.12
    elif 20 <= hour < 23:
        factor = 0.95
    elif 0 <= hour < 6:
        factor = 0.72

    if is_weekend:
        factor *= 0.88
    if status == "delivering":
        factor *= 1.04
    return max(0.6, factor)


def _route_stop_markers(
    geometry: list[tuple[float, float]],
    cumulative_km: list[float],
    *,
    seed: str,
    min_spacing_km: float,
    turn_threshold_deg: float,
    max_markers: int,
) -> list[tuple[float, float]]:
    if len(geometry) < 3 or not cumulative_km:
        return []
    total_km = float(cumulative_km[-1] or 0.0)
    if total_km <= max(min_spacing_km * 1.2, 0.08):
        return []

    markers: list[tuple[float, float]] = []
    spacing = max(0.06, float(min_spacing_km))
    margin_km = min(total_km * 0.12, spacing)
    last_marker_km = -1e9

    for idx in range(1, len(geometry) - 1):
        km_at = float(cumulative_km[idx] or 0.0)
        if km_at <= margin_km or km_at >= (total_km - margin_km):
            continue
        if km_at - last_marker_km < (spacing * 0.7):
            continue
        prev_bearing = _bearing_deg(*geometry[idx - 1], *geometry[idx])
        next_bearing = _bearing_deg(*geometry[idx], *geometry[idx + 1])
        turn_delta = _bearing_delta_deg(prev_bearing, next_bearing)
        if turn_delta < turn_threshold_deg:
            continue
        weight = 0.85 + min(turn_delta, 120.0) / 120.0
        markers.append((km_at / total_km, weight))
        last_marker_km = km_at
        if len(markers) >= max_markers:
            return markers

    spacing_seed = 0.85 + (_seed_fraction(seed, "spacing") * 0.4)
    periodic_spacing_km = max(spacing, min(0.28, total_km / 5.0)) * spacing_seed
    periodic_progress_gap = max(0.03, periodic_spacing_km / max(total_km, 0.001))
    start_offset_km = min(total_km * 0.2, periodic_spacing_km * (0.9 + _seed_fraction(seed, "offset") * 0.5))
    cursor_km = max(margin_km, start_offset_km)

    while cursor_km < (total_km - margin_km) and len(markers) < max_markers:
        progress = cursor_km / total_km
        if all(abs(existing_progress - progress) >= periodic_progress_gap for existing_progress, _ in markers):
            jitter = 0.55 + (_seed_fraction(seed, f"periodic:{len(markers)}") * 0.35)
            markers.append((progress, jitter))
        cursor_km += periodic_spacing_km

    markers.sort(key=lambda item: item[0])
    return markers[:max_markers]


def build_motion_schedule(
    *,
    geometry: list[tuple[float, float]],
    cumulative_km: list[float],
    total_duration_s: float,
    seed: str,
    reference_ts: datetime,
    status: str,
    min_spacing_km: float = 0.16,
    turn_threshold_deg: float = 28.0,
    base_stop_seconds: float = 5.0,
    max_pause_ratio: float = 0.28,
    max_markers: int = 10,
) -> MotionSchedule:
    total_duration_s = max(0.0, float(total_duration_s))
    if total_duration_s <= 0:
        return MotionSchedule(segments=(MotionSegment(0.0, 0.0, 1.0, 1.0, False),), total_duration_s=0.0)
    if len(geometry) < 2 or not cumulative_km or float(cumulative_km[-1] or 0.0) <= 0.01:
        return MotionSchedule(
            segments=(MotionSegment(0.0, total_duration_s, 0.0, 1.0, False),),
            total_duration_s=total_duration_s,
        )

    markers = _route_stop_markers(
        geometry,
        cumulative_km,
        seed=seed,
        min_spacing_km=min_spacing_km,
        turn_threshold_deg=turn_threshold_deg,
        max_markers=max_markers,
    )
    if not markers or total_duration_s < 40.0:
        return MotionSchedule(
            segments=(MotionSegment(0.0, total_duration_s, 0.0, 1.0, False),),
            total_duration_s=total_duration_s,
        )

    congestion = _congestion_factor(reference_ts, status)
    seed_scale = 0.9 + (_seed_fraction(seed, "pause_scale") * 0.25)
    stop_seconds = max(2.0, float(base_stop_seconds)) * seed_scale
    raw_pause_s = sum(weight * stop_seconds for _, weight in markers) * congestion
    min_moving_ratio = 0.45 if status == "delivering" else 0.5
    total_pause_s = min(
        total_duration_s * max(0.0, float(max_pause_ratio)),
        raw_pause_s,
        total_duration_s * max(0.0, 1.0 - min_moving_ratio),
    )
    if total_pause_s < 1.0:
        return MotionSchedule(
            segments=(MotionSegment(0.0, total_duration_s, 0.0, 1.0, False),),
            total_duration_s=total_duration_s,
        )

    total_move_s = max(1.0, total_duration_s - total_pause_s)
    progress_markers = [0.0, *[progress for progress, _ in markers if 0.0 < progress < 1.0], 1.0]
    pause_weights = [weight for _, weight in markers]
    pause_weight_total = max(sum(pause_weights), 1e-6)

    segments: list[MotionSegment] = []
    cursor_s = 0.0
    for idx in range(len(progress_markers) - 1):
        progress_start = max(0.0, min(1.0, float(progress_markers[idx])))
        progress_end = max(progress_start, min(1.0, float(progress_markers[idx + 1])))
        progress_span = max(0.0, progress_end - progress_start)
        move_duration_s = total_move_s * progress_span
        if move_duration_s > 0.01:
            next_cursor = min(total_duration_s, cursor_s + move_duration_s)
            segments.append(
                MotionSegment(
                    start_s=cursor_s,
                    end_s=next_cursor,
                    progress_start=progress_start,
                    progress_end=progress_end,
                    paused=False,
                )
            )
            cursor_s = next_cursor

        if idx < len(pause_weights):
            pause_duration_s = total_pause_s * (pause_weights[idx] / pause_weight_total)
            if pause_duration_s > 0.25:
                next_cursor = min(total_duration_s, cursor_s + pause_duration_s)
                segments.append(
                    MotionSegment(
                        start_s=cursor_s,
                        end_s=next_cursor,
                        progress_start=progress_end,
                        progress_end=progress_end,
                        paused=True,
                    )
                )
                cursor_s = next_cursor

    if not segments:
        segments = [MotionSegment(0.0, total_duration_s, 0.0, 1.0, False)]
    elif cursor_s < total_duration_s:
        last = segments[-1]
        if last.paused:
            segments.append(MotionSegment(cursor_s, total_duration_s, last.progress_end, 1.0, False))
        else:
            segments[-1] = MotionSegment(
                start_s=last.start_s,
                end_s=total_duration_s,
                progress_start=last.progress_start,
                progress_end=1.0,
                paused=False,
            )
    else:
        last = segments[-1]
        segments[-1] = MotionSegment(
            start_s=last.start_s,
            end_s=total_duration_s,
            progress_start=last.progress_start,
            progress_end=last.progress_end,
            paused=last.paused,
        )

    return MotionSchedule(segments=tuple(segments), total_duration_s=total_duration_s)


def progress_at_elapsed(schedule: MotionSchedule, elapsed_s: float) -> tuple[float, bool]:
    if not schedule.segments:
        return 1.0, False
    if elapsed_s <= 0:
        first = schedule.segments[0]
        return first.progress_start, first.paused

    capped_elapsed = min(max(0.0, float(elapsed_s)), schedule.total_duration_s)
    for segment in schedule.segments:
        if capped_elapsed <= segment.end_s:
            if segment.paused or segment.end_s <= segment.start_s:
                return segment.progress_end, segment.paused
            ratio = (capped_elapsed - segment.start_s) / max(segment.end_s - segment.start_s, 1e-6)
            progress = segment.progress_start + ((segment.progress_end - segment.progress_start) * ratio)
            return max(0.0, min(1.0, progress)), False

    last = schedule.segments[-1]
    return max(0.0, min(1.0, last.progress_end)), last.paused
