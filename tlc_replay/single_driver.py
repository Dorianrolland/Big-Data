"""
Single-driver scenario for the TLC replay service.

What it does
------------
Build a deterministic pseudo-driver that moves continuously on the NYC
street network for the whole live window. The driver consumes the same
sorted TLC parquet as the fleet scenario but greedily picks a chain of
compatible trips: a trip is appended to the chain only if its pickup
time is close enough to the previous dropoff (accounting for a short
repositioning leg). Between two trips the driver is routed from the
previous dropoff to the next pickup; a lunch break can be inserted if
its window is reached while idle.

Key differences vs. the fleet scenario
--------------------------------------
* ONE driver, with a fixed id (`TLC_SINGLE_DRIVER_ID`), never disappears.
* No straight-line interpolation ever: every segment (reposition +
  delivering) is routed via the `RoutingClient` chain. If no provider
  is available the driver is held at its last routed point and marked
  stale explicitly instead of teleporting.
* One CourierPositionV1 emitted every tick, even when idle/paused, so
  the UI always sees a recent position.
* Night acceleration: when the virtual clock enters the configured
  window (default 02:00 -> 08:00) we advance the clock by
  `TLC_NIGHT_ACCEL_FACTOR` ticks per real tick, which keeps the demo
  watchable during low-traffic hours.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, time as dtime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from copilot_events_pb2 import CourierPositionV1, OrderEventV1, OrderOfferV1

from routing import (
    Route,
    RoutingClient,
    RoutingUnavailableError,
    cumulative_distances_km,
    interpolate_on_route,
)

if TYPE_CHECKING:  # avoid circular import at runtime
    from main import TLCReplay, Trip

log = logging.getLogger("tlc-replay.single")

# ── env ────────────────────────────────────────────────────────────────────────
SINGLE_DRIVER_ID = os.getenv("TLC_SINGLE_DRIVER_ID", "drv_demo_001").strip() or "drv_demo_001"
MAX_REPOSITION_MIN = float(os.getenv("TLC_SINGLE_MAX_REPOSITION_MIN", "20"))
MAX_IDLE_GAP_MIN = float(os.getenv("TLC_SINGLE_MAX_IDLE_GAP_MIN", "45"))
REPOSITION_AVG_SPEED_KMH = float(os.getenv("TLC_SINGLE_REPOSITION_KMH", "24"))

LUNCH_BREAK_ENABLED = os.getenv("TLC_LUNCH_BREAK_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
LUNCH_BREAK_WINDOW = os.getenv("TLC_LUNCH_BREAK_WINDOW", "12:00-14:00").strip()
LUNCH_BREAK_MIN = int(os.getenv("TLC_LUNCH_BREAK_MIN", "20"))
LUNCH_BREAK_MAX = int(os.getenv("TLC_LUNCH_BREAK_MAX", "45"))

NIGHT_ACCEL_START = os.getenv("TLC_NIGHT_ACCEL_START", "02:00").strip()
NIGHT_ACCEL_END = os.getenv("TLC_NIGHT_ACCEL_END", "08:00").strip()
NIGHT_ACCEL_FACTOR = max(1, int(os.getenv("TLC_NIGHT_ACCEL_FACTOR", "8")))

STATUS_KEY_SINGLE = "copilot:replay:tlc:single:status"
CURSOR_KEY_SINGLE = "copilot:replay:tlc:single:cursor"
SINGLE_SOURCE_PLATFORM = (
    os.getenv("TLC_SOURCE_PLATFORM", "tlc_hvfhv_historical") or "tlc_hvfhv_historical"
).strip()


def _parse_hhmm(value: str, fallback: dtime) -> dtime:
    try:
        hh, mm = value.split(":", 1)
        return dtime(hour=int(hh), minute=int(mm))
    except Exception:
        return fallback


def _parse_window(value: str) -> tuple[dtime, dtime]:
    try:
        start_s, end_s = value.split("-", 1)
    except ValueError:
        return dtime(12, 0), dtime(14, 0)
    return _parse_hhmm(start_s, dtime(12, 0)), _parse_hhmm(end_s, dtime(14, 0))


def _in_time_window(now: datetime, start: dtime, end: dtime) -> bool:
    t = now.time()
    if start <= end:
        return start <= t <= end
    return t >= start or t <= end


def _deterministic_offset_minutes(day_key: str, span_min: int, span_max: int) -> int:
    if span_max <= span_min:
        return span_min
    h = int(hashlib.sha1(day_key.encode()).hexdigest()[:8], 16)
    return span_min + (h % (span_max - span_min + 1))


# ── state ──────────────────────────────────────────────────────────────────────
@dataclass
class _DriverState:
    lat: float
    lon: float
    heading: float = 0.0
    status: str = "idle"
    last_route: Route | None = None
    last_route_cumkm: list[float] = field(default_factory=list)
    route_start: datetime | None = None
    route_end: datetime | None = None
    lunch_done_for_day: str = ""
    stale_reason: str | None = None


# ── main scenario class ────────────────────────────────────────────────────────
class SingleDriverScenario:
    """Run the single-driver loop on top of an existing `TLCReplay`.

    The scenario reuses the parent replay's producer / redis / duckdb
    connections so we don't duplicate transport setup.
    """

    def __init__(self, replay: "TLCReplay") -> None:
        self.replay = replay
        self.routing: RoutingClient = RoutingClient()
        lunch_start, lunch_end = _parse_window(LUNCH_BREAK_WINDOW)
        self.lunch_start = lunch_start
        self.lunch_end = lunch_end
        self.night_start = _parse_hhmm(NIGHT_ACCEL_START, dtime(2, 0))
        self.night_end = _parse_hhmm(NIGHT_ACCEL_END, dtime(8, 0))
        self.state: _DriverState | None = None
        self._cursor = 0
        self._stop = False
        self.stats_positions = 0
        self.stats_trips = 0
        self.stats_reposition = 0
        self.stats_lunch = 0
        self.stats_routing_errors = 0
        self.stats_route_requests = 0
        self.stats_route_success = 0
        self.stats_hold_ticks = 0
        self._active_trip: "Trip | None" = None
        self._active_trip_route: Route | None = None
        self._active_trip_cum: list[float] = []
        self._startup_health = {}
        self._startup_all_unhealthy = False
        self._pending_events: list[tuple[str, datetime]] = []
        # Cross-month continuity: when run() is called a second time on a
        # freshly-prepared month parquet we restore these instead of
        # re-anchoring on the first trip of that month (which would teleport
        # the driver back to January every 31 days).
        self._vt: datetime | None = None
        self._current_trip: "Trip | None" = None
        self._phase: str = "idle"

    # ─ lifecycle ─
    async def start(self) -> None:
        await self.routing.start()
        # Fail-informative probe: we don't abort the scenario on OSRM down
        # (the driver gracefully holds at last point and emits a stale
        # reason), but we DO want a loud log line at startup so ops can tell
        # the difference between "routing broken" and "nothing happening".
        self._startup_health = {}
        try:
            self._startup_health = await self.routing.healthcheck()
        except Exception as exc:
            log.warning("single-driver: healthcheck raised %s", exc)
        self._startup_all_unhealthy = bool(self._startup_health) and all(
            value.startswith("unhealthy") for value in self._startup_health.values()
        )
        log.info(
            "single-driver scenario ready driver_id=%s providers=%s health=%s",
            SINGLE_DRIVER_ID,
            ",".join(self.routing.active_providers) or "(none)",
            self._startup_health or "(skipped)",
        )
        if not self.routing.active_providers:
            log.warning(
                "single-driver: no routing provider configured - "
                "driver will be held at its last point",
            )
        if self._startup_all_unhealthy:
            log.warning(
                "single-driver: every routing provider failed the startup probe — "
                "driver will be held at its last point until a provider recovers",
            )

    async def close(self) -> None:
        await self.routing.close()

    def stop(self) -> None:
        self._stop = True

    # ─ helpers ─
    def _is_night(self, vt: datetime) -> bool:
        return _in_time_window(vt, self.night_start, self.night_end)

    def _is_lunch(self, vt: datetime) -> bool:
        return LUNCH_BREAK_ENABLED and _in_time_window(vt, self.lunch_start, self.lunch_end)

    def _is_routing_degraded(self) -> bool:
        if not self.routing.active_providers:
            return True
        if self._startup_all_unhealthy:
            return True
        if self.routing.last_error:
            return True
        return False

    async def _emit_position(self, vt: datetime, status: str, speed_kmh: float) -> None:
        assert self.state is not None
        pos = CourierPositionV1(
            event_id=f"pos_{SINGLE_DRIVER_ID}_{int(vt.timestamp())}",
            event_type="courier.position.v1",
            ts=vt.astimezone(timezone.utc).isoformat(),
            courier_id=SINGLE_DRIVER_ID,
            lat=round(self.state.lat, 6),
            lon=round(self.state.lon, 6),
            speed_kmh=round(max(0.0, speed_kmh), 1),
            heading_deg=round(self.state.heading, 1),
            status=status,
            accuracy_m=8.0,
            battery_pct=100.0,
            source_platform=SINGLE_SOURCE_PLATFORM,
        )
        await self.replay._send(_courier_topic(), SINGLE_DRIVER_ID, pos.SerializeToString())
        self.stats_positions += 1
        self.state.status = status

    async def _emit_offer(self, trip: "Trip") -> None:
        offer = OrderOfferV1(
            event_id=f"offer_{SINGLE_DRIVER_ID}_{trip.trip_key}",
            event_type="order.offer.v1",
            ts=trip.request_ts.astimezone(timezone.utc).isoformat(),
            offer_id=trip.offer_id,
            courier_id=SINGLE_DRIVER_ID,
            pickup_lat=trip.pickup_lat,
            pickup_lon=trip.pickup_lon,
            dropoff_lat=trip.dropoff_lat,
            dropoff_lon=trip.dropoff_lon,
            estimated_fare_eur=round(trip.fare_usd, 2),
            estimated_distance_km=trip.trip_km,
            estimated_duration_min=trip.trip_min,
            demand_index=1.0,
            weather_factor=1.0,
            traffic_factor=1.0,
            zone_id=f"nyc_{trip.pu_loc}",
            source_platform=SINGLE_SOURCE_PLATFORM,
        )
        await self.replay._send(_offers_topic(), SINGLE_DRIVER_ID, offer.SerializeToString())

    async def _emit_event(self, trip: "Trip", status: str, ts: datetime, actuals: bool) -> None:
        evt = OrderEventV1(
            event_id=f"evt_{status}_{SINGLE_DRIVER_ID}_{trip.trip_key}",
            event_type="order.event.v1",
            ts=ts.astimezone(timezone.utc).isoformat(),
            offer_id=trip.offer_id,
            order_id=trip.order_id,
            courier_id=SINGLE_DRIVER_ID,
            status=status,
            actual_fare_eur=round(trip.fare_usd, 2) if actuals else 0.0,
            actual_distance_km=trip.trip_km if actuals else 0.0,
            actual_duration_min=trip.trip_min if actuals else 0.0,
            zone_id=f"nyc_{trip.pu_loc}",
            source_platform=SINGLE_SOURCE_PLATFORM,
        )
        await self.replay._send(_events_topic(), SINGLE_DRIVER_ID, evt.SerializeToString())

    async def _status(self, vt: datetime) -> None:
        if self.replay.redis is None:
            return
        route_success_rate = (
            self.stats_route_success / self.stats_route_requests
            if self.stats_route_requests > 0
            else 0.0
        )
        hold_ratio = (
            self.stats_hold_ticks / self.stats_positions
            if self.stats_positions > 0
            else 0.0
        )
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "virtual_time": vt.astimezone(timezone.utc).isoformat(),
            "driver_id": SINGLE_DRIVER_ID,
            "positions": str(self.stats_positions),
            "trips": str(self.stats_trips),
            "repositions": str(self.stats_reposition),
            "lunch_breaks": str(self.stats_lunch),
            "routing_errors": str(self.stats_routing_errors),
            "route_requests": str(self.stats_route_requests),
            "route_successes": str(self.stats_route_success),
            "hold_ticks": str(self.stats_hold_ticks),
            "routing_success_rate": f"{route_success_rate:.4f}",
            "hold_ratio": f"{hold_ratio:.4f}",
            "routing_providers": ",".join(self.routing.active_providers),
            "routing_health_json": json.dumps(self._startup_health, separators=(",", ":"), sort_keys=True),
            "routing_degraded": "1" if self._is_routing_degraded() else "0",
            "routing_last_error": self.routing.last_error or "",
            "state": (self.state.status if self.state else "unknown"),
            "stale_reason": (self.state.stale_reason if self.state and self.state.stale_reason else ""),
        }
        await self.replay.redis.hset(STATUS_KEY_SINGLE, mapping=payload)
        await self.replay.redis.expire(STATUS_KEY_SINGLE, 86400)

    async def _save_cursor(self) -> None:
        if self.replay.redis is None:
            return
        await self.replay.redis.hset(CURSOR_KEY_SINGLE, self.replay.month, str(self._cursor))
        await self.replay.redis.expire(CURSOR_KEY_SINGLE, 30 * 86400)

    async def _load_cursor(self) -> int:
        if self.replay.redis is None:
            return 0
        val = await self.replay.redis.hget(CURSOR_KEY_SINGLE, self.replay.month)
        try:
            return int(val) if val else 0
        except (TypeError, ValueError):
            return 0

    # ─ trip sourcing ─
    def _iter_trips(self, cursor: int) -> Any:
        sql = f"SELECT * FROM read_parquet('{self.replay.sorted_path.as_posix()}') OFFSET {cursor}"
        return self.replay.duck.execute(sql)

    def _next_compatible_trip(self, it: Any, cols: list[str], after_ts: datetime) -> "Trip | None":
        """Return the first trip whose pickup fits `after_ts + MAX_IDLE_GAP_MIN`.

        Trips whose pickup is in the past (before after_ts) are skipped: the
        driver cannot teleport back in time. Trips too far in the future
        open an idle pause that is handled by the caller. We cap the scan
        at 5000 rows to avoid blocking on an empty tail.
        """
        max_scan = 5000
        while max_scan > 0:
            rows = it.fetchmany(1)
            if not rows:
                return None
            max_scan -= 1
            self._cursor += 1
            row = dict(zip(cols, rows[0]))
            trip = self.replay._row_to_trip(row)
            if trip is None:
                continue
            if trip.pickup_ts < after_ts:
                continue
            gap_min = (trip.pickup_ts - after_ts).total_seconds() / 60.0
            if gap_min > MAX_IDLE_GAP_MIN + MAX_REPOSITION_MIN:
                # too far -> treat as idle gap, pause until pickup minus reposition budget
                return trip
            return trip
        return None

    # ─ core loop ─
    async def run(self, total_rows: int) -> None:
        assert self.replay.redis is not None

        first_call = self.state is None
        if first_call:
            # cold start: load redis cursor so a container restart within the
            # same month can resume roughly where it was (best-effort; the
            # driver's physical state is lost, but the parquet offset isn't).
            self._cursor = await self._load_cursor()
        else:
            # warm continuation across months: new parquet, start at row 0 but
            # do NOT touch self.state / self._vt / self._current_trip.
            self._cursor = 0
        log.info(
            "single-driver replay month=%s cursor=%d/%d first_call=%s",
            self.replay.month, self._cursor, total_rows, first_call,
        )

        it = self._iter_trips(self._cursor)
        cols = [d[0] for d in it.description]

        if first_call:
            # anchor on first trip
            first: "Trip | None" = None
            while first is None:
                rows = it.fetchmany(1)
                if not rows:
                    log.info("no trips to drive in month %s", self.replay.month)
                    return
                self._cursor += 1
                first = self.replay._row_to_trip(dict(zip(cols, rows[0])))

            self.state = _DriverState(lat=first.pickup_lat, lon=first.pickup_lon)
            vt = first.request_ts
            log.info("virtual clock start (single) %s at pickup zone %d", vt.isoformat(), first.pu_loc)

            # place the first trip in flight immediately
            current_trip: "Trip | None" = first
            phase = "await_pickup"
            await self._emit_offer(first)
            await self._emit_event(first, "accepted", first.request_ts, actuals=False)
            self.stats_trips += 1
        else:
            assert self._vt is not None
            vt = self._vt
            current_trip = self._current_trip
            phase = self._phase
            trip_key = getattr(current_trip, "trip_key", None)
            log.info(
                "single-driver resuming month=%s vt=%s phase=%s trip=%s",
                self.replay.month, vt.isoformat(), phase, trip_key,
            )

        tick_interval = float(getattr(self.replay, "tick_interval_sec", 5.0))
        last_log = asyncio.get_event_loop().time()

        while not self._stop:
            assert self.state is not None

            # --- phase transitions at this virtual time ---
            if current_trip is None:
                peek = self._next_compatible_trip(it, cols, vt)
                if peek is None:
                    await self._emit_position(vt, "available", 0.0)
                    log.info("single-driver: trip stream exhausted, pausing idle")
                    break
                current_trip = peek
                await self._emit_offer(current_trip)
                await self._emit_event(current_trip, "accepted", max(vt, current_trip.request_ts), actuals=False)
                self.stats_trips += 1
                phase = "repositioning"
                await self._begin_reposition(current_trip, vt)

            if phase == "await_pickup":
                if vt >= current_trip.pickup_ts:
                    phase = "delivering"
                    await self._begin_delivering(current_trip)
                else:
                    # either reposition-to-pickup if we have a route, or just keep keepalive
                    if self.state.last_route is None:
                        await self._begin_reposition(current_trip, vt)
                        phase = "repositioning"

            if phase == "repositioning":
                await self._walk_route(vt, "repositioning", target_status="repositioning")
                if self.state.route_end is not None and vt >= self.state.route_end:
                    # arrived at pickup — wait for pickup_ts if still ahead
                    self.state.lat = current_trip.pickup_lat
                    self.state.lon = current_trip.pickup_lon
                    self.state.last_route = None
                    await self._emit_position(vt, "pickup_arrived", 0.0)
                    phase = "await_pickup"

            if phase == "delivering" and current_trip is not None:
                if vt >= current_trip.dropoff_ts:
                    self.state.lat = current_trip.dropoff_lat
                    self.state.lon = current_trip.dropoff_lon
                    await self._emit_position(current_trip.dropoff_ts, "available", 0.0)
                    await self._emit_event(current_trip, "dropped_off", current_trip.dropoff_ts, actuals=True)
                    self._active_trip = None
                    self._active_trip_route = None
                    self._active_trip_cum = []
                    current_trip = None
                    phase = "idle"
                else:
                    await self._walk_route(vt, "delivering", target_status="delivering")

            if phase == "idle":
                # Maybe lunch break
                if self._maybe_take_lunch(vt):
                    phase = "lunch"

            if phase == "lunch":
                await self._emit_position(vt, "idle", 0.0)
                end_ts = self.state.route_end
                if end_ts is not None and vt >= end_ts:
                    self.state.last_route = None
                    self.state.route_end = None
                    phase = "idle"

            if phase == "idle" and current_trip is None:
                await self._emit_position(vt, "available", 0.0)

            # --- status log ---
            now_wall = asyncio.get_event_loop().time()
            if now_wall - last_log >= 15.0:
                await self._save_cursor()
                await self._status(vt)
                log.info(
                    "single cursor=%d positions=%d trips=%d repos=%d lunch=%d errors=%d state=%s vt=%s",
                    self._cursor,
                    self.stats_positions,
                    self.stats_trips,
                    self.stats_reposition,
                    self.stats_lunch,
                    self.stats_routing_errors,
                    self.state.status,
                    vt.strftime("%Y-%m-%d %H:%M"),
                )
                last_log = now_wall

            # --- advance virtual clock ---
            factor = NIGHT_ACCEL_FACTOR if self._is_night(vt) else 1
            vt = vt + timedelta(seconds=tick_interval * factor)
            await asyncio.sleep(tick_interval / max(float(getattr(self.replay, "speed_factor", 1.0)), 0.01))

        # Persist loop-local state so a subsequent run() on the next month
        # (see main.py month loop) resumes instead of re-anchoring.
        self._vt = vt
        self._current_trip = current_trip
        self._phase = phase

        await self._save_cursor()
        await self._status(vt)
        log.info(
            "single-driver month=%s ended vt=%s phase=%s positions=%d trips=%d repos=%d errors=%d",
            self.replay.month,
            vt.strftime("%Y-%m-%d %H:%M"),
            phase,
            self.stats_positions,
            self.stats_trips,
            self.stats_reposition,
            self.stats_routing_errors,
        )

    # ─ phase helpers ─
    async def _begin_reposition(self, trip: "Trip", vt: datetime) -> None:
        assert self.state is not None
        origin = (self.state.lat, self.state.lon)
        dest = (trip.pickup_lat, trip.pickup_lon)
        self.stats_route_requests += 1
        try:
            route = await self.routing.route_segment(origin, dest)
        except RoutingUnavailableError as exc:
            self.stats_routing_errors += 1
            self.state.stale_reason = f"routing_unavailable:{exc}"
            log.warning("single-driver reposition routing failed: %s", exc)
            # Hold driver at its last point. We still emit keepalive.
            self.state.last_route = None
            self.state.route_end = trip.pickup_ts
            return

        self.state.stale_reason = None
        self.stats_route_success += 1
        self.stats_reposition += 1
        self.state.last_route = route
        self.state.last_route_cumkm = cumulative_distances_km(route.geometry)
        self.state.route_start = vt
        # we must arrive at pickup before pickup_ts; clamp to remaining time if needed
        eta_min = max(1.0, route.duration_min)
        arrival = vt + timedelta(minutes=eta_min)
        if arrival > trip.pickup_ts:
            arrival = trip.pickup_ts
        # drive faster if pickup window is short
        self.state.route_end = arrival

    async def _begin_delivering(self, trip: "Trip") -> None:
        assert self.state is not None
        origin = (trip.pickup_lat, trip.pickup_lon)
        dest = (trip.dropoff_lat, trip.dropoff_lon)
        self.stats_route_requests += 1
        try:
            route = await self.routing.route_segment(origin, dest)
        except RoutingUnavailableError as exc:
            self.stats_routing_errors += 1
            self.state.stale_reason = f"routing_unavailable:{exc}"
            log.warning("single-driver delivering routing failed: %s", exc)
            self._active_trip = trip
            self._active_trip_route = None
            self._active_trip_cum = []
            self.state.last_route = None
            self.state.route_start = trip.pickup_ts
            self.state.route_end = trip.dropoff_ts
            return
        self.state.stale_reason = None
        self.stats_route_success += 1
        self._active_trip = trip
        self._active_trip_route = route
        self._active_trip_cum = cumulative_distances_km(route.geometry)
        self.state.last_route = route
        self.state.last_route_cumkm = self._active_trip_cum
        self.state.route_start = trip.pickup_ts
        self.state.route_end = trip.dropoff_ts

    async def _walk_route(self, vt: datetime, leg: str, target_status: str) -> None:
        """Move the driver along the current route to position at `vt`."""
        assert self.state is not None
        route = self.state.last_route
        start = self.state.route_start
        end = self.state.route_end
        if route is None or start is None or end is None or end <= start:
            # degraded / held: emit keepalive at last point
            self.stats_hold_ticks += 1
            await self._emit_position(vt, target_status, 0.0)
            return
        elapsed = (vt - start).total_seconds()
        total = max(1.0, (end - start).total_seconds())
        progress = max(0.0, min(1.0, elapsed / total))
        (lat, lon), bearing = interpolate_on_route(route, self.state.last_route_cumkm, progress)
        self.state.lat = lat
        self.state.lon = lon
        self.state.heading = bearing
        speed_kmh = (route.distance_km / max(total / 3600.0, 1e-4))
        await self._emit_position(vt, target_status, speed_kmh)

    def _maybe_take_lunch(self, vt: datetime) -> bool:
        if not LUNCH_BREAK_ENABLED:
            return False
        assert self.state is not None
        day_key = vt.strftime("%Y-%m-%d")
        if self.state.lunch_done_for_day == day_key:
            return False
        if not self._is_lunch(vt):
            return False
        minutes = _deterministic_offset_minutes(
            f"{SINGLE_DRIVER_ID}:{day_key}",
            LUNCH_BREAK_MIN,
            LUNCH_BREAK_MAX,
        )
        self.state.last_route = None
        self.state.route_start = vt
        self.state.route_end = vt + timedelta(minutes=minutes)
        self.state.lunch_done_for_day = day_key
        self.stats_lunch += 1
        log.info("single-driver lunch break started at %s for %d min", vt.isoformat(), minutes)
        return True


# ── topic helpers (kept local so the module has no hard env import) ────────────
def _courier_topic() -> str:
    return os.getenv("KAFKA_TOPIC", "livreurs-gps")


def _offers_topic() -> str:
    return os.getenv("ORDER_OFFERS_TOPIC", "order-offers-v1")


def _events_topic() -> str:
    return os.getenv("ORDER_EVENTS_TOPIC", "order-events-v1")
