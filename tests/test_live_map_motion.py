"""Non-regression checks for smooth fleet marker motion on the live map."""
from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


def _live_map_source() -> str:
    path = ROOT / "api" / "static" / "index.html"
    return path.read_text(encoding="utf-8", errors="ignore")


def test_fleet_map_has_interpolation_state_and_loop():
    src = _live_map_source()
    assert "const fleetMotion = new Map();" in src
    assert "function upsertFleetMotion(lv, now)" in src
    assert "function fleetRenderTick()" in src
    assert "requestAnimationFrame(fleetRenderTick);" in src


def test_fleet_poll_uses_motion_upsert_not_direct_jump():
    src = _live_map_source()
    assert "upsertFleetMotion(lv, now);" in src
    assert "m.setLatLng(ll);" not in src
    assert "track_points=" in src


def test_fleet_map_uses_recent_track_timeline_when_available():
    src = _live_map_source()
    assert "function fleetRemainingPath(state, now, currentRendered)" in src
    assert "function buildFleetPath(state, lv, currentRendered, now)" in src
    assert "function pathLengthKm(path)" in src
    assert "state.path = path;" in src
    assert "lv.route_source === 'hold'" in src
    assert "fleetTrack') || '48'" in src


def test_focus_mode_has_interpolated_glide_not_fixed_step():
    src = _live_map_source()
    assert "function focusDurationMs(" in src
    assert "function buildFocusPath(state, pos, currentRendered, trail, nextServerTsMs)" in src
    assert "function currentFocusInterpolated(now)" in src
    assert "focusState.path = motion.path;" in src
    assert "const freezeMarker = focusState.stale || pos.route_source === 'hold' || pos.anomaly_state === 'anomalous';" in src
    assert "FOCUS_FREEZE_JUMP_KM" in src
    assert "focusState.targetUpdateWall = now + focusDurationMs(" in src
