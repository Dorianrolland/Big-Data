"""UI non-regression checks for dashboard text contrast."""
from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


def _dashboard_source() -> str:
    path = ROOT / "dashboard" / "app.py"
    return path.read_text(encoding="utf-8", errors="ignore")


def test_dashboard_cards_force_dark_text_on_light_backgrounds():
    src = _dashboard_source()
    assert ".bi-card, .cold-card, .sla-card, .map-card, .spotlight-container, .bento-card," in src
    assert "color: #0F172A !important;" in src


def test_dashboard_keeps_inline_color_overrides():
    src = _dashboard_source()
    assert ".bi-card *:not([style*=\"color:\"])" in src
