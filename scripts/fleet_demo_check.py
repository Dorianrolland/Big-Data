"""COP-027 — Vérification readiness mode flotte démo.

Attend que le nombre minimum de chauffeurs actifs soit atteint
avant de signaler la démo comme prête.

Usage:
    python3 scripts/fleet_demo_check.py
    python3 scripts/fleet_demo_check.py --min-drivers 50 --timeout 120
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

try:
    import urllib.request
    import json as _json
except ImportError:
    pass

_ROOT = Path(__file__).resolve().parent.parent

FLEET_DEMO_PARAMS = {
    "n_drivers": 250,
    "seed": 42,
    "zone_spread": 30,
    "warmup_min_drivers": 50,
    "warmup_timeout_s": 120,
    "env_file": "env/fleet_demo.env",
    "scenario": "fleet_demo",
}


def _get_json(url: str, timeout: int = 5) -> dict:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return _json.loads(resp.read())
    except Exception:
        return {}


def check_readiness(api_base: str, min_drivers: int, timeout_s: int) -> bool:
    deadline = time.time() + timeout_s
    print(f"[fleet-check] Attente de {min_drivers} chauffeurs actifs (timeout {timeout_s}s)...")

    while time.time() < deadline:
        data = _get_json(f"{api_base}/stats")
        active = data.get("active_couriers", 0)
        if active >= min_drivers:
            print(f"[fleet-check] OK — {active} chauffeurs actifs")
            return True
        remaining = max(0, int(deadline - time.time()))
        print(f"[fleet-check]   {active}/{min_drivers} chauffeurs actifs — {remaining}s restantes")
        time.sleep(5)

    print(f"[fleet-check] TIMEOUT — moins de {min_drivers} chauffeurs après {timeout_s}s")
    return False


def print_fleet_kpis(api_base: str) -> None:
    print("\n── KPIs flotte ──────────────────────────────────────")
    for path in ["/stats", "/health"]:
        data = _get_json(f"{api_base}{path}")
        if data:
            print(f"\n{path}:")
            for k, v in list(data.items())[:10]:
                print(f"  {k}: {v}")
    print("─────────────────────────────────────────────────────")


def get_fleet_demo_config() -> dict:
    """Retourne la configuration du mode flotte pour documentation/tests."""
    return FLEET_DEMO_PARAMS


def main() -> None:
    parser = argparse.ArgumentParser(description="Fleet demo readiness check")
    parser.add_argument("--api", default="http://localhost:8001")
    parser.add_argument("--min-drivers", type=int, default=50)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--kpis-only", action="store_true")
    args = parser.parse_args()

    if args.kpis_only:
        print_fleet_kpis(args.api)
        return

    ok = check_readiness(args.api, args.min_drivers, args.timeout)
    if ok:
        print_fleet_kpis(args.api)
        print("\n[fleet-check] Demo prête !")
        print(f"  PWA Copilot   → {args.api}/copilot")
        print(f"  API docs      → {args.api}/docs")
        print(f"  Fleet wallboard → {args.api}/copilot/fleet  (COP-029)")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
