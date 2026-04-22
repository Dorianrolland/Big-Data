"""Fleet demo configuration and readiness checks."""
from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent


def _import_script(name, path):
    spec = spec_from_file_location(name, path)
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_fleet_demo_env_exists():
    env_path = _ROOT / "env" / "fleet_demo.env"
    assert env_path.exists(), "fleet_demo.env must exist"


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
        assert key in content, f"missing key in fleet_demo.env: {key}"


def test_fleet_demo_config():
    fleet_check = _import_script(
        "fleet_demo_check", _ROOT / "scripts" / "fleet_demo_check.py"
    )
    config = fleet_check.get_fleet_demo_config()
    assert config["n_drivers"] >= 100
    assert config["seed"] == 42
    assert config["warmup_min_drivers"] > 0
    assert config["replay_grace_s"] >= 10


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
    assert lines.get("TLC_FLEET_DEMO_SEED") == "42"


def test_check_readiness_uses_replay_grace_budget(monkeypatch):
    fleet_check = _import_script(
        "fleet_demo_check", _ROOT / "scripts" / "fleet_demo_check.py"
    )
    monkeypatch.setattr(fleet_check, "_active_couriers", lambda _api: 80)
    captured = {"timeout_s": 0}

    def _fake_replay(_api, timeout_s, driver_id_hint=None):
        _ = _api, driver_id_hint
        captured["timeout_s"] = int(timeout_s)
        return True, {"replay_count": 5}

    monkeypatch.setattr(fleet_check, "_check_replay_available", _fake_replay)

    ok, details = fleet_check.check_readiness(
        api_base="http://localhost:8001",
        min_drivers=50,
        timeout_s=10,
        require_replay=True,
        replay_driver="drv_demo_001",
        replay_grace_s=33,
    )
    assert ok is True
    assert details["replay"]["replay_count"] == 5
    assert captured["timeout_s"] == 33


def test_active_couriers_does_not_fallback_to_tlc_replay_without_flag(monkeypatch):
    fleet_check = _import_script(
        "fleet_demo_check", _ROOT / "scripts" / "fleet_demo_check.py"
    )

    def _fake_get_json(url, timeout=8, retries=1):  # noqa: ANN001
        _ = timeout, retries
        if url.endswith("/stats"):
            return {}
        if "/livreurs-proches" in url:
            return {}
        if url.endswith("/copilot/health"):
            return {"tlc_replay": {"active_trips": "88"}}
        return {}

    monkeypatch.setattr(fleet_check, "_get_json", _fake_get_json)
    monkeypatch.setenv("COPILOT_ALLOW_REPLAY_ACTIVE_FALLBACK", "0")
    assert fleet_check._active_couriers("http://localhost:8001") == 0


def test_active_couriers_falls_back_to_tlc_replay_health_with_flag(monkeypatch):
    fleet_check = _import_script(
        "fleet_demo_check", _ROOT / "scripts" / "fleet_demo_check.py"
    )

    def _fake_get_json(url, timeout=8, retries=1):  # noqa: ANN001
        _ = timeout, retries
        if url.endswith("/stats"):
            return {}
        if "/livreurs-proches" in url:
            return {}
        if url.endswith("/copilot/health"):
            return {"tlc_replay": {"active_trips": "88"}}
        return {}

    monkeypatch.setattr(fleet_check, "_get_json", _fake_get_json)
    monkeypatch.setenv("COPILOT_ALLOW_REPLAY_ACTIVE_FALLBACK", "1")
    assert fleet_check._active_couriers("http://localhost:8001") == 88


def test_check_replay_available_falls_back_to_health_events(monkeypatch):
    fleet_check = _import_script(
        "fleet_demo_check", _ROOT / "scripts" / "fleet_demo_check.py"
    )
    monkeypatch.setattr(
        fleet_check,
        "_discover_driver_with_offers",
        lambda *_args, **_kwargs: (None, None),
    )
    monkeypatch.setattr(fleet_check.time, "sleep", lambda _s: None)

    def _fake_get_json(url, timeout=8, retries=0):  # noqa: ANN001
        _ = timeout, retries
        if url.endswith("/copilot/health"):
            return {"tlc_replay": {"events": "12", "active_trips": "80"}}
        if "/copilot/replay" in url:
            return {}
        return {}

    monkeypatch.setattr(fleet_check, "_get_json", _fake_get_json)

    ok, meta = fleet_check._check_replay_available(
        api_base="http://localhost:8001",
        timeout_s=1,
        driver_id_hint=None,
    )
    assert ok is True
    assert meta["replay_url"] == "health_fallback"
    assert meta["replay_count"] == 12
