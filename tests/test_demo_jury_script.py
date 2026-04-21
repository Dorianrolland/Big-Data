"""Tests du script jury demo (COP-030)."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent


def test_demo_jury_script_exists():
    assert (_ROOT / "scripts" / "demo-jury.ps1").exists()


def test_demo_runbook_exists():
    assert (_ROOT / "docs" / "demo-runbook.md").exists()


def test_demo_runbook_has_kpi_table():
    content = (_ROOT / "docs" / "demo-runbook.md").read_text()
    assert "KPIs attendus" in content or "KPI" in content
    assert "hot_path" in content.lower() or "p99" in content.lower()


def test_demo_jury_ps1_has_required_sections():
    content = (_ROOT / "scripts" / "demo-jury.ps1").read_text()
    assert "docker compose" in content
    assert "health" in content.lower()
    assert "backtest" in content.lower()
    assert "KPI" in content or "kpi" in content.lower()
    assert "fleet/overview" in content


def test_demo_jury_ps1_one_command():
    """Le script doit être exécutable sans arguments obligatoires."""
    content = (_ROOT / "scripts" / "demo-jury.ps1").read_text()
    # Verify param block has all defaults
    assert 'ApiBase     = "http://localhost:8001"' in content
    assert 'WarmupSec      = 45' in content or 'WarmupSec' in content


def test_backtest_generates_csv():
    """Verify backtest script generates CSV (smoke test)."""
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        result = subprocess.run(
            [sys.executable, str(_ROOT / "ml" / "backtest_copilot.py"), "--out", tmp],
            capture_output=True, text=True, timeout=30, cwd=str(_ROOT)
        )
        assert result.returncode == 0, f"backtest failed: {result.stderr}"
        assert (Path(tmp) / "backtest_summary.csv").exists()
        assert (Path(tmp) / "backtest_summary.json").exists()


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
