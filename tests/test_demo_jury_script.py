"""Tests du script jury demo (COP-030)."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_DEMO_JURY = _ROOT / "scripts" / "demo-jury.ps1"
_DEMO_RUN = _ROOT / "scripts" / "demo_run.ps1"


def _read_utf8(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_demo_jury_script_exists():
    assert _DEMO_JURY.exists()


def test_demo_runbook_exists():
    assert (_ROOT / "docs" / "demo-runbook.md").exists()


def test_demo_runbook_has_kpi_table():
    content = (_ROOT / "docs" / "demo-runbook.md").read_text()
    assert "KPIs attendus" in content or "KPI" in content
    assert "hot_path" in content.lower() or "p99" in content.lower()


def test_demo_jury_ps1_has_required_sections():
    content = _read_utf8(_DEMO_JURY)
    assert "docker compose" in content
    assert "health" in content.lower()
    assert "backtest" in content.lower()
    assert "KPI" in content or "kpi" in content.lower()
    assert "fleet/overview" in content
    assert "accept_score" in content
    assert "route_linear_fallback" in content
    assert "route_hold_fallback" in content
    assert "routing_quality_ok" in content


def test_demo_run_ps1_uses_accept_score_contract():
    content = _read_utf8(_DEMO_RUN)
    assert "accept_score" in content
    assert ".score" not in content


def test_demo_jury_ps1_one_command():
    """Le script doit être exécutable sans arguments obligatoires."""
    content = _read_utf8(_DEMO_JURY)
    # Verify param block has all defaults
    assert 'ApiBase     = "http://localhost:8001"' in content
    assert 'WarmupSec      = 45' in content or 'WarmupSec' in content
    assert '[switch]$Fleet       = $true' in content
    assert '[int]$MinDrivers     = 140' in content


def test_backtest_generates_csv():
    """Verify backtest script generates CSV (smoke test)."""
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        result = subprocess.run(
            [
                sys.executable,
                str(_ROOT / "ml" / "backtest_copilot.py"),
                "--out",
                tmp,
                "--max-offers",
                "2000",
            ],
            capture_output=True, text=True, timeout=30, cwd=str(_ROOT)
        )
        assert result.returncode == 0, f"backtest failed: {result.stderr}"
        assert (Path(tmp) / "backtest_summary.csv").exists()
        assert (Path(tmp) / "backtest_summary.json").exists()


def test_demo_ps_scripts_ascii_safe():
    """PowerShell demo scripts should stay ASCII-safe for Windows CP encodings."""
    for path in (_DEMO_JURY, _DEMO_RUN):
        data = path.read_bytes()
        data.decode("ascii")


def test_demo_ps_scripts_parse_when_powershell_available():
    ps = shutil.which("powershell") or shutil.which("pwsh")
    if not ps:
        import pytest

        pytest.skip("PowerShell not available on this environment")

    for path in (_DEMO_JURY, _DEMO_RUN):
        cmd = f"[void][scriptblock]::Create((Get-Content '{path}' -Raw)); Write-Output 'ok'"
        result = subprocess.run(
            [ps, "-NoProfile", "-Command", cmd],
            capture_output=True,
            text=True,
            timeout=20,
            cwd=str(_ROOT),
            env=dict(os.environ),
        )
        assert result.returncode == 0, f"script parse failed for {path}: {result.stderr}"
        assert "ok" in result.stdout


def test_kpi_script_smoke():
    """Verify KPI query script runs (requires mart to be built first)."""
    mart_dir = _ROOT / "data" / "marts" / "copilot"
    if not mart_dir.exists():
        import pytest
        pytest.skip("mart not built — run make build-mart first")
    result = subprocess.run(
        [sys.executable, str(_ROOT / "scripts" / "query-copilot-kpis.py")],
        capture_output=True, text=True, timeout=30, cwd=str(_ROOT)
    )
    assert result.returncode == 0
    assert "KPIs" in result.stdout or "kpi" in result.stdout.lower()
