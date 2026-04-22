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


def test_focus_mode_has_interpolated_glide_not_fixed_step():
    src = _live_map_source()
    assert "function focusDurationMs(" in src
    assert "function currentFocusInterpolated(now)" in src
    assert "focusState.targetUpdateWall = now + focusDurationMs(" in src
