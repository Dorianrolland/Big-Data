"""Tests du mode flotte démo (COP-027)."""
from __future__ import annotations

from pathlib import Path
from importlib.util import spec_from_file_location, module_from_spec

_ROOT = Path(__file__).resolve().parent.parent


def _import_script(name, path):
    spec = spec_from_file_location(name, path)
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_fleet_demo_env_exists():
    env_path = _ROOT / "env" / "fleet_demo.env"
    assert env_path.exists(), "fleet_demo.env doit exister"


def test_fleet_demo_env_has_required_keys():
    env_path = _ROOT / "env" / "fleet_demo.env"
    content = env_path.read_text()
    required_keys = [
        "TLC_FLEET_DEMO_N_DRIVERS",
        "TLC_FLEET_DEMO_SEED",
        "FLEET_DEMO_WARMUP_MIN_DRIVERS",
        "FLEET_DEMO_WARMUP_TIMEOUT_S",
        "GPS_TTL_SECONDS",
        "BATCH_INTERVAL_SECONDS",
    ]
    for key in required_keys:
        assert key in content, f"Clé manquante dans fleet_demo.env: {key}"


def test_fleet_demo_config():
    fleet_check = _import_script(
        "fleet_demo_check", _ROOT / "scripts" / "fleet_demo_check.py"
    )
    config = fleet_check.get_fleet_demo_config()
    assert config["n_drivers"] >= 100, "Doit configurer au moins 100 chauffeurs pour la démo"
    assert config["seed"] == 42, "Seed doit être stable (42)"
    assert config["warmup_min_drivers"] > 0


def test_makefile_has_fleet_demo_targets():
    makefile = (_ROOT / "Makefile").read_text()
    assert "fleet-demo-up" in makefile
    assert "fleet-demo-down" in makefile
    assert "fleet-demo-check" in makefile


def test_fleet_demo_env_seed_reproducible():
    env_path = _ROOT / "env" / "fleet_demo.env"
    content = env_path.read_text()
    lines = {
        line.split("=")[0]: line.split("=")[1].strip()
        for line in content.splitlines()
        if "=" in line and not line.startswith("#")
    }
    assert lines.get("TLC_FLEET_DEMO_SEED") == "42", "Seed doit être 42 pour reproductibilité"
