"""COP-033 - Fleet demo readiness check (drivers + replay availability).

Usage:
  python scripts/fleet_demo_check.py
  python scripts/fleet_demo_check.py --min-drivers 50 --timeout 120
  python scripts/fleet_demo_check.py --kpis-only
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
_ENV_PATH = _ROOT / "env" / "fleet_demo.env"
_LOCAL_HTTP_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
_REQUIRED_DEMO_SERVICES = {"api", "hot-consumer", "tlc-replay", "redis", "redpanda"}


def _safe_print(text: str) -> None:
    print(text.encode("ascii", errors="replace").decode("ascii", errors="replace"))


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


_ENV = _read_env_file(_ENV_PATH)
FLEET_DEMO_PARAMS = {
    "n_drivers": int(_ENV.get("TLC_FLEET_DEMO_N_DRIVERS", "250") or "250"),
    "seed": int(_ENV.get("TLC_FLEET_DEMO_SEED", "42") or "42"),
    "zone_spread": int(_ENV.get("TLC_FLEET_DEMO_ZONE_SPREAD", "30") or "30"),
    "warmup_min_drivers": int(_ENV.get("FLEET_DEMO_WARMUP_MIN_DRIVERS", "50") or "50"),
    "warmup_timeout_s": int(_ENV.get("FLEET_DEMO_WARMUP_TIMEOUT_S", "120") or "120"),
    "replay_grace_s": int(_ENV.get("FLEET_DEMO_REPLAY_GRACE_S", "45") or "45"),
    "trip_sample_rate": float(_ENV.get("TLC_TRIP_SAMPLE_RATE", "0.15") or "0.15"),
    "speed_factor": float(_ENV.get("TLC_SPEED_FACTOR", "1.0") or "1.0"),
    "tick_interval_s": float(_ENV.get("TLC_TICK_INTERVAL_SEC", "5.0") or "5.0"),
    "gps_ttl_seconds": int(_ENV.get("GPS_TTL_SECONDS", "20") or "20"),
    "env_file": "env/fleet_demo.env",
    "scenario": _ENV.get("TLC_SCENARIO", "fleet") or "fleet",
}


def _get_json(url: str, timeout: int = 8, retries: int = 1) -> dict[str, Any]:
    attempts = max(1, int(retries) + 1)
    for idx in range(attempts):
        try:
            with _LOCAL_HTTP_OPENER.open(url, timeout=timeout) as response:
                payload = response.read().decode("utf-8")
            return json.loads(payload) if payload else {}
        except Exception:
            if idx >= attempts - 1:
                return {}
            time.sleep(min(1.5, 0.3 * float(idx + 1)))
    return {}


def _running_services() -> set[str]:
    cmd = ["docker", "compose"]
    if _ENV_PATH.exists():
        cmd.extend(["--env-file", str(_ENV_PATH)])
    cmd.extend(["ps", "--services", "--filter", "status=running"])
    try:
        result = subprocess.run(
            cmd,
            cwd=_ROOT,
            text=True,
            capture_output=True,
            check=False,
            timeout=10,
        )
    except Exception:
        return set()
    if result.returncode != 0:
        return set()
    lines = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
    return set(lines)


def _allow_replay_active_fallback() -> bool:
    raw = str(os.getenv("COPILOT_ALLOW_REPLAY_ACTIVE_FALLBACK", "0") or "0").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _parse_offer_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _discover_driver_with_offers(api_base: str, timeout_sec: int = 8) -> tuple[str | None, str | None]:
    nearby = _get_json(
        f"{api_base}/livreurs-proches?lat=40.7580&lon=-73.9855&rayon=12",
        timeout=max(3, int(timeout_sec)),
        retries=0,
    )
    candidates: list[str] = []
    for row in nearby.get("livreurs", []) if isinstance(nearby, dict) else []:
        if not isinstance(row, dict):
            continue
        driver_id = str(row.get("livreur_id", "")).strip()
        if driver_id and driver_id not in candidates:
            candidates.append(driver_id)

    fallback_ids = ["drv_demo_001", "L001"]
    env_driver = str(os.getenv("TLC_SINGLE_DRIVER_ID", "")).strip()
    if env_driver:
        fallback_ids.insert(0, env_driver)
    for fallback in fallback_ids:
        if fallback not in candidates:
            candidates.append(fallback)

    for driver_id in candidates:
        payload = _get_json(
            f"{api_base}/copilot/driver/{urllib.parse.quote(driver_id)}/offers?limit=1",
            timeout=max(3, int(timeout_sec)),
            retries=0,
        )
        offers = payload.get("offers", []) if isinstance(payload, dict) else []
        if isinstance(offers, list) and offers:
            first = offers[0] if isinstance(offers[0], dict) else {}
            return driver_id, str(first.get("ts") or "")
    return (candidates[0], None) if candidates else (None, None)


def _check_replay_available(api_base: str, timeout_s: int, driver_id_hint: str | None = None) -> tuple[bool, dict[str, Any]]:
    deadline = time.time() + timeout_s
    last_count = 0
    selected_driver = driver_id_hint
    selected_offer_ts: str | None = None

    while time.time() < deadline:
        remaining = max(1, int(deadline - time.time()))
        if not selected_driver:
            selected_driver, selected_offer_ts = _discover_driver_with_offers(
                api_base,
                timeout_sec=min(8, max(3, remaining)),
            )

        anchors: list[datetime] = []
        offer_anchor = _parse_offer_ts(selected_offer_ts)
        if offer_anchor is not None:
            anchors.append(offer_anchor)

        health_payload = _get_json(
            f"{api_base}/copilot/health",
            timeout=min(15, max(4, remaining)),
            retries=0,
        )
        virtual_ts_str = (
            ((health_payload.get("tlc_replay") or {}).get("virtual_time"))
            if isinstance(health_payload, dict)
            else None
        )
        virtual_anchor = _parse_offer_ts(str(virtual_ts_str or ""))
        if virtual_anchor is not None:
            anchors.append(virtual_anchor)
        if not anchors:
            anchors.append(datetime.now(timezone.utc))

        for anchor in anchors:
            replay_from = anchor - timedelta(minutes=2)
            replay_to = anchor + timedelta(minutes=2)
            probe_drivers: list[str | None] = []
            if selected_driver:
                probe_drivers.append(selected_driver)
            probe_drivers.append(None)
            for probe_driver in probe_drivers:
                params_obj = {
                    "from": replay_from.isoformat(),
                    "to": replay_to.isoformat(),
                    "limit": "20",
                }
                if probe_driver:
                    params_obj["driver_id"] = str(probe_driver)
                params = urllib.parse.urlencode(params_obj)
                replay_url = f"{api_base}/copilot/replay?{params}"
                query_timeout = min(12, max(3, int(deadline - time.time())))
                replay_payload = _get_json(replay_url, timeout=query_timeout, retries=0)
                count = int(replay_payload.get("count", 0) or 0) if isinstance(replay_payload, dict) else 0
                last_count = max(last_count, count)
                if count > 0:
                    return True, {
                        "driver_id": probe_driver,
                        "replay_count": count,
                        "replay_url": replay_url,
                    }

        time.sleep(min(4, max(1, int(deadline - time.time()))))

    health_payload = _get_json(f"{api_base}/copilot/health", timeout=15, retries=0)
    replay_payload = (health_payload.get("tlc_replay") or {}) if isinstance(health_payload, dict) else {}
    health_events = int(replay_payload.get("events", 0) or 0) if replay_payload else 0
    if health_events > 0:
        return True, {
            "driver_id": selected_driver,
            "replay_count": max(last_count, health_events),
            "replay_url": "health_fallback",
        }

    return False, {"driver_id": selected_driver, "replay_count": last_count}


def _active_couriers(api_base: str) -> int:
    def _as_int(value: Any) -> int:
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return 0

    stats = _get_json(f"{api_base}/stats", timeout=12, retries=1)
    active = _as_int(stats.get("active_couriers", 0) if isinstance(stats, dict) else 0)
    if isinstance(stats, dict):
        active = max(active, _as_int((stats.get("hot_path") or {}).get("livreurs_actifs", 0)))
    if active > 0:
        return active
    nearby = _get_json(
        f"{api_base}/livreurs-proches?lat=40.7580&lon=-73.9855&rayon=20&limit=250",
        timeout=10,
        retries=1,
    )
    rows = nearby.get("livreurs", []) if isinstance(nearby, dict) else []
    nearby_count = len(rows) if isinstance(rows, list) else 0
    if nearby_count > 0:
        return max(active, nearby_count)

    if not _allow_replay_active_fallback():
        return max(active, nearby_count)

    health = _get_json(f"{api_base}/copilot/health", timeout=15, retries=0)
    replay = (health.get("tlc_replay") or {}) if isinstance(health, dict) else {}
    active_trips = _as_int(replay.get("active_trips", 0))
    return max(active, nearby_count, active_trips)


def check_readiness(
    api_base: str,
    min_drivers: int,
    timeout_s: int,
    require_replay: bool,
    replay_driver: str | None = None,
    replay_grace_s: int = 45,
) -> tuple[bool, dict[str, Any]]:
    deadline = time.time() + timeout_s
    _safe_print(
        f"[fleet-check] Waiting for >= {min_drivers} active drivers "
        f"(timeout {timeout_s}s, require_replay={require_replay})..."
    )
    last_active = 0
    replay_meta: dict[str, Any] = {}

    while time.time() < deadline:
        observed_active = _active_couriers(api_base)
        last_active = max(last_active, int(observed_active))
        ready_drivers = last_active >= min_drivers
        if ready_drivers and not require_replay:
            _safe_print(f"[fleet-check] OK - active drivers: {observed_active} (peak={last_active})")
            return True, {"active_couriers": last_active, "replay": {"required": False}}

        if ready_drivers and require_replay:
            replay_wait_budget = max(10, int(replay_grace_s))
            replay_ok, replay_meta = _check_replay_available(
                api_base,
                timeout_s=replay_wait_budget,
                driver_id_hint=replay_driver,
            )
            if replay_ok:
                _safe_print(
                    f"[fleet-check] OK - active drivers: {observed_active} (peak={last_active}), "
                    f"replay_count: {int(replay_meta.get('replay_count', 0))}"
                )
                return True, {"active_couriers": last_active, "replay": replay_meta}

        remaining = max(0, int(deadline - time.time()))
        _safe_print(
            f"[fleet-check]   active={observed_active}/{min_drivers}, "
            f"peak={last_active}, remaining={remaining}s"
        )
        time.sleep(5)

    _safe_print(
        f"[fleet-check] TIMEOUT - peak_active={last_active}/{min_drivers}, "
        f"replay_count={int(replay_meta.get('replay_count', 0))}"
    )
    return False, {"active_couriers": last_active, "replay": replay_meta}


def print_fleet_kpis(api_base: str) -> None:
    _safe_print("\n-- Fleet KPIs ----------------------------------------------------------")
    copilot_health = {}
    for path in ["/health", "/stats", "/copilot/health"]:
        payload = _get_json(f"{api_base}{path}", timeout=8)
        if not payload:
            continue
        if path == "/copilot/health" and isinstance(payload, dict):
            copilot_health = payload
        _safe_print(f"\n{path}:")
        for key, value in list(payload.items())[:12]:
            _safe_print(f"  {key}: {value}")
    replay = (copilot_health.get("tlc_replay") or {}) if isinstance(copilot_health, dict) else {}
    if isinstance(replay, dict):
        trajectory_mode = str(replay.get("trajectory_mode", "") or "").strip().lower()
        osrm_success = int(replay.get("route_osrm_success", 0) or 0)
        osrm_public_success = int(replay.get("route_osrm_public_success", 0) or 0)
        linear_fallback = int(replay.get("route_linear_fallback", 0) or 0)
        if trajectory_mode == "osrm" and (osrm_success + osrm_public_success) == 0 and linear_fallback >= 50:
            _safe_print("\n[warn] replay route_mode=osrm but no routed segments succeeded yet.")
            _safe_print("       Most trips are currently using fallback geometry (check local OSRM profile/data).")
    _safe_print("------------------------------------------------------------------------")


def get_fleet_demo_config() -> dict:
    """Return fleet demo config for docs/tests."""
    return dict(FLEET_DEMO_PARAMS)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fleet demo readiness check")
    parser.add_argument("--api", default="http://localhost:8001")
    parser.add_argument("--min-drivers", type=int, default=FLEET_DEMO_PARAMS["warmup_min_drivers"])
    parser.add_argument("--timeout", type=int, default=FLEET_DEMO_PARAMS["warmup_timeout_s"])
    parser.add_argument(
        "--replay-grace",
        type=int,
        default=FLEET_DEMO_PARAMS["replay_grace_s"],
        help="Extra seconds to wait for replay data once min drivers is reached.",
    )
    parser.add_argument("--driver", default=None, help="Optional driver id hint for replay probe.")
    parser.add_argument("--skip-replay-check", action="store_true", help="Only verify active drivers.")
    parser.add_argument("--kpis-only", action="store_true")
    args = parser.parse_args()

    if args.kpis_only:
        print_fleet_kpis(args.api.rstrip("/"))
        return

    running = _running_services()
    missing = sorted(_REQUIRED_DEMO_SERVICES - running) if running else []
    if missing:
        _safe_print(
            "[fleet-check] warning: required services are not all running: "
            + ", ".join(missing)
        )
        _safe_print("[fleet-check] run: docker compose --env-file env/fleet_demo.env --profile routing up -d")

    ok, details = check_readiness(
        api_base=args.api.rstrip("/"),
        min_drivers=max(1, int(args.min_drivers)),
        timeout_s=max(10, int(args.timeout)),
        require_replay=not args.skip_replay_check,
        replay_driver=str(args.driver).strip() if args.driver else None,
        replay_grace_s=max(5, int(args.replay_grace)),
    )

    print_fleet_kpis(args.api.rstrip("/"))
    if ok:
        _safe_print("\n[fleet-check] Demo ready.")
        _safe_print(f"  Copilot PWA      -> {args.api.rstrip('/')}/copilot")
        _safe_print(f"  API docs         -> {args.api.rstrip('/')}/docs")
        replay_count = int((details.get("replay") or {}).get("replay_count", 0) or 0)
        _safe_print(f"  Replay available -> {replay_count > 0} (count={replay_count})")
        raise SystemExit(0)

    _safe_print("\n[fleet-check] Demo not ready yet.")
    raise SystemExit(1)


if __name__ == "__main__":
    main()
