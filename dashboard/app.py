"""
FleetStream — Analytics Dashboard
Pixel Perfect "SaaS 2026" — HTML/CSS pur, zéro chrome Streamlit visible.
Anti-DuplicateWidgetID : widgets hors boucle, résultats en session_state.
"""
import os
import time
from datetime import datetime

import pandas as pd
import pydeck as pdk
import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://api:8000")
_refresh_raw = os.getenv("REFRESH_SECONDS", os.getenv("DASHBOARD_REFRESH_SECONDS", "8"))
REFRESH_SECONDS = max(5, int(_refresh_raw or "8"))
ANALYTICS_TTL_SECONDS = max(
    30,
    int(os.getenv("DASHBOARD_ANALYTICS_TTL_SECONDS", "90") or "90"),
)
ZONE_COVERAGE_TTL_SECONDS = max(
    ANALYTICS_TTL_SECONDS,
    int(os.getenv("DASHBOARD_ZONE_COVERAGE_TTL_SECONDS", str(ANALYTICS_TTL_SECONDS + 30)) or str(ANALYTICS_TTL_SECONDS + 30)),
)

STATUS_COLOR_RGB = {
    "delivering": [251, 146,  60, 220],
    "available":  [ 52, 211, 153, 220],
    "idle":       [148, 163, 184, 150],
}

st.set_page_config(
    page_title="FleetStream Analytics",
    page_icon="🛵",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ════════════════════════════════════════════════════════════════════════════
#  GLOBAL CSS — Pixel Perfect
# ════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:opsz,wght@14..32,300..900&display=swap');
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&display=swap');

/* ─── Reset ───────────────────────────────────────────────────────────────── */
*, *::before, *::after { box-sizing: border-box; }
html, body, [class*="css"] {
    font-family: 'Inter', system-ui, -apple-system, sans-serif !important;
}

/* ─── Fond dégradé ────────────────────────────────────────────────────────── */
.stApp {
    background: linear-gradient(135deg, #f0f4ff 0%, #e8ecf4 50%, #dde3f0 100%) !important;
    min-height: 100vh;
}

/* ─── Cacher TOUT le chrome Streamlit ────────────────────────────────────── */
#MainMenu, footer, header,
[data-testid="stToolbar"],
[data-testid="stDecoration"],
[data-testid="stStatusWidget"],
[data-testid="stDeployButton"],
[data-testid="stSidebarCollapsedControl"],
[data-testid="collapsedControl"],
.st-emotion-cache-h4xjwg,
.st-emotion-cache-1dp5vir,
button[kind="header"] { display: none !important; }
[data-testid="stSidebar"] { display: none !important; }

/* ─── Block container ─────────────────────────────────────────────────────── */
.block-container {
    padding: 5.75rem 2.5rem 3rem !important;
    max-width: 1500px !important;
}

/* ═══════════════════════════════════════════════════════════════════════════
   NAVBAR FIXE — Glassmorphism
═══════════════════════════════════════════════════════════════════════════ */
.fs-navbar {
    position: fixed;
    top: 0; left: 0; right: 0; z-index: 9999;
    height: 56px;
    background: rgba(255, 255, 255, 0.72);
    backdrop-filter: blur(16px) saturate(180%);
    -webkit-backdrop-filter: blur(16px) saturate(180%);
    border-bottom: 1px solid rgba(99, 102, 241, 0.1);
    display: flex; align-items: center;
    justify-content: space-between;
    padding: 0 2.5rem;
    box-shadow: 0 1px 28px rgba(0, 0, 0, 0.05);
}
.fs-logo { display: flex; align-items: center; gap: 10px; }
.fs-logo-badge {
    width: 32px; height: 32px;
    background: linear-gradient(135deg, #6366F1, #8B5CF6);
    border-radius: 9px;
    display: flex; align-items: center; justify-content: center;
    font-size: 16px;
    box-shadow: 0 2px 10px rgba(99, 102, 241, 0.4);
}
.fs-logo-name {
    font-size: 15px; font-weight: 800;
    color: #0F172A; letter-spacing: -0.3px;
}
.fs-logo-name em { color: #6366F1; font-style: normal; }
.fs-nav-right { display: flex; align-items: center; gap: 8px; }
.fs-pill {
    font-size: 11px; font-weight: 600; color: #64748B;
    background: rgba(100, 116, 139, 0.07);
    border: 1px solid rgba(100, 116, 139, 0.12);
    border-radius: 6px; padding: 3px 10px;
}
.fs-btn-live {
    display: inline-flex; align-items: center; gap: 5px;
    font-size: 12px; font-weight: 700;
    background: linear-gradient(135deg, #6366F1, #8B5CF6);
    color: white !important; text-decoration: none !important;
    border-radius: 9px; padding: 7px 15px;
    box-shadow: 0 2px 12px rgba(99, 102, 241, 0.35);
    transition: box-shadow 0.2s, transform 0.15s;
}
.fs-btn-live:hover {
    box-shadow: 0 4px 20px rgba(99, 102, 241, 0.5);
    transform: translateY(-1px); color: white !important;
}

/* ═══════════════════════════════════════════════════════════════════════════
   SECTION HEADER
═══════════════════════════════════════════════════════════════════════════ */
.fs-sh { margin: 2px 0 16px; }
.fs-sh .overline {
    font-size: 10.5px; font-weight: 700;
    letter-spacing: 1.4px; text-transform: uppercase;
    color: #94A3B8; margin-bottom: 4px;
}
.fs-sh .heading {
    font-size: 1.2rem; font-weight: 800;
    color: #0F172A; letter-spacing: -0.3px; margin-bottom: 3px;
}
.fs-sh .sub { font-size: 12.5px; color: #64748B; line-height: 1.5; }

/* ═══════════════════════════════════════════════════════════════════════════
   BENTO GRID — KPIs
═══════════════════════════════════════════════════════════════════════════ */
.bento-grid {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 14px; margin-bottom: 28px;
}
.bento-card {
    background: #FFFFFF; border-radius: 20px;
    padding: 18px 18px 16px; position: relative; overflow: hidden;
    border: 1px solid rgba(226, 232, 240, 0.7);
    box-shadow: 0 10px 15px -3px rgba(0,0,0,0.06), 0 4px 6px -4px rgba(0,0,0,0.04);
    transition: transform 0.2s ease, box-shadow 0.2s ease; cursor: default;
}
.bento-card:hover {
    transform: translateY(-3px);
    box-shadow: 0 20px 25px -5px rgba(0,0,0,0.09), 0 8px 10px -6px rgba(0,0,0,0.04);
}
.bento-card::before {
    content: '';
    position: absolute; left: 0; top: 14px; bottom: 14px;
    width: 3.5px; border-radius: 0 3px 3px 0;
}
.bc-indigo::before  { background: linear-gradient(180deg, #6366F1, #8B5CF6); }
.bc-emerald::before { background: linear-gradient(180deg, #10B981, #059669); }
.bc-amber::before   { background: linear-gradient(180deg, #F59E0B, #F97316); }
.bc-sky::before     { background: linear-gradient(180deg, #0EA5E9, #6366F1); }
.bc-slate::before   { background: linear-gradient(180deg, #94A3B8, #64748B); }
.bento-icon {
    width: 38px; height: 38px; border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    font-size: 19px; margin-bottom: 13px;
}
.bi-indigo  { background: rgba(99,102,241,0.1); }
.bi-emerald { background: rgba(16,185,129,0.1); }
.bi-amber   { background: rgba(245,158,11,0.1); }
.bi-sky     { background: rgba(14,165,233,0.1); }
.bi-slate   { background: rgba(148,163,184,0.1); }
.bento-lbl {
    font-size: 10px; font-weight: 700; letter-spacing: 0.9px;
    text-transform: uppercase; color: #94A3B8; margin-bottom: 3px;
}
.bento-val { font-size: 2.1rem; font-weight: 800; line-height: 1.08; letter-spacing: -1.5px; }
.bv-indigo  { color: #4F46E5; }
.bv-emerald { color: #059669; }
.bv-amber   { color: #D97706; }
.bv-sky     { color: #0284C7; }
.bv-slate   { color: #64748B; }
.bento-sub  { font-size: 11px; color: #94A3B8; margin-top: 5px; font-weight: 500; }
.bento-glyph {
    position: absolute; right: 14px; bottom: 10px;
    font-size: 2.8rem; opacity: 0.06; pointer-events: none; line-height: 1;
}

/* ═══════════════════════════════════════════════════════════════════════════
   SLA CARD
═══════════════════════════════════════════════════════════════════════════ */
.sla-card {
    background: #FFFFFF; border-radius: 20px;
    border: 1px solid rgba(226,232,240,0.7);
    box-shadow: 0 10px 15px -3px rgba(0,0,0,0.06);
    padding: 22px 26px; margin-bottom: 22px;
}
.sla-card-header {
    display: flex; align-items: center; gap: 10px; margin-bottom: 18px;
}
.sla-card-title { font-size: 14px; font-weight: 800; color: #0F172A; }
.sla-badge-ok {
    font-size: 10px; font-weight: 800; letter-spacing: 0.3px;
    background: #DCFCE7; color: #166534;
    border: 1px solid #86EFAC; border-radius: 20px; padding: 2px 9px;
}
.sla-badge-ko {
    font-size: 10px; font-weight: 800; letter-spacing: 0.3px;
    background: #FEF9C3; color: #854D0E;
    border: 1px solid #FDE047; border-radius: 20px; padding: 2px 9px;
}
.sla-item { margin-bottom: 12px; }
.sla-item-row {
    display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;
}
.sla-lbl  { font-size: 12px; font-weight: 500; color: #64748B; }
.sla-ms   { font-size: 12px; font-weight: 700; font-family: 'JetBrains Mono', monospace; }
.sla-ok   { color: #059669; }
.sla-warn { color: #D97706; }
.sla-bad  { color: #DC2626; }
.sla-track { height: 5px; background: #F1F5F9; border-radius: 999px; overflow: hidden; }
.sla-fill  { height: 100%; border-radius: 999px; transition: width .6s cubic-bezier(.4,0,.2,1); }

/* ═══════════════════════════════════════════════════════════════════════════
   SPOTLIGHT SEARCH
═══════════════════════════════════════════════════════════════════════════ */
.spotlight-container {
    background: rgba(255,255,255,0.88);
    backdrop-filter: blur(20px) saturate(160%);
    -webkit-backdrop-filter: blur(20px) saturate(160%);
    border: 1.5px solid rgba(99,102,241,0.2);
    border-radius: 18px;
    box-shadow:
        0 8px 32px rgba(99,102,241,0.1),
        0 2px 8px rgba(0,0,0,0.05),
        inset 0 1px 0 rgba(255,255,255,0.9);
    padding: 20px 22px 16px;
    margin-bottom: 18px;
}
.spotlight-header {
    font-size: 13px; font-weight: 700; color: #0F172A;
    display: flex; align-items: center; gap: 7px; margin-bottom: 14px;
}
.spotlight-container [data-testid="stTextInput"] input {
    background: #F8FAFC !important;
    border: 1.5px solid rgba(99,102,241,0.18) !important;
    border-radius: 12px !important;
    font-size: 15px !important; font-weight: 600 !important;
    padding: 10px 16px !important; color: #0F172A !important;
    letter-spacing: -0.2px !important; height: 44px !important;
    transition: all 0.2s !important;
}
.spotlight-container [data-testid="stTextInput"] input:focus {
    border-color: #6366F1 !important;
    box-shadow: 0 0 0 3px rgba(99,102,241,0.12) !important;
    background: #FFFFFF !important; outline: none !important;
}
.spotlight-container [data-testid="stSelectbox"] > div > div {
    background: #F8FAFC !important;
    border: 1.5px solid rgba(99,102,241,0.18) !important;
    border-radius: 12px !important;
    font-size: 14px !important; font-weight: 500 !important; min-height: 44px !important;
}
.spotlight-container [data-testid="stButton"] > button {
    background: linear-gradient(135deg, #6366F1 0%, #8B5CF6 55%, #7C3AED 100%) !important;
    color: #FFFFFF !important; border: none !important;
    border-radius: 12px !important; font-weight: 700 !important;
    font-size: 14px !important; height: 44px !important; letter-spacing: 0.15px !important;
    box-shadow: 0 4px 16px rgba(99,102,241,0.45),
                inset 0 1px 0 rgba(255,255,255,0.25) !important;
    transition: all 0.2s ease !important;
}
.spotlight-container [data-testid="stButton"] > button:hover {
    box-shadow: 0 6px 24px rgba(99,102,241,0.55),
                inset 0 1px 0 rgba(255,255,255,0.25) !important;
    transform: translateY(-1px) !important;
}
.spotlight-container [data-testid="stButton"] > button:active {
    transform: translateY(0) !important;
}

/* ═══════════════════════════════════════════════════════════════════════════
   KPIs TRAJECTOIRE
═══════════════════════════════════════════════════════════════════════════ */
.traj-kpis {
    display: grid; grid-template-columns: repeat(4, 1fr);
    gap: 12px; margin-bottom: 18px;
}
.traj-kpi {
    background: linear-gradient(140deg, #F5F7FF 0%, #EEF2FF 100%);
    border-radius: 14px; padding: 14px 16px;
    border: 1px solid rgba(99,102,241,0.1);
}
.tk-lbl  { font-size: 10px; font-weight: 700; color: #94A3B8; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 5px; }
.tk-val  { font-size: 1.55rem; font-weight: 800; color: #4F46E5; letter-spacing: -0.8px; line-height: 1.1; }
.tk-unit { font-size: 10.5px; color: #94A3B8; margin-top: 3px; font-weight: 500; }

/* ═══════════════════════════════════════════════════════════════════════════
   MAP CARD WRAPPER
═══════════════════════════════════════════════════════════════════════════ */
.map-card {
    background: #FFFFFF; border-radius: 20px;
    border: 1px solid rgba(226,232,240,0.7);
    box-shadow: 0 10px 15px -3px rgba(0,0,0,0.06);
    padding: 18px 18px 14px; margin-bottom: 14px;
}
.map-card-header {
    font-size: 12px; font-weight: 700; color: #475569;
    display: flex; align-items: center; gap: 6px;
    margin-bottom: 12px; padding: 0 2px;
}
.map-card iframe { border-radius: 12px !important; border: none !important; }

/* ═══════════════════════════════════════════════════════════════════════════
   LÉGENDE TRAJECTOIRE
═══════════════════════════════════════════════════════════════════════════ */
.traj-legend {
    display: flex; flex-wrap: wrap; gap: 14px;
    padding: 10px 14px; margin-top: 10px;
    background: #F8FAFC; border-radius: 10px;
}
.tl-item { display: flex; align-items: center; gap: 6px; font-size: 11.5px; color: #475569; font-weight: 500; }
.tl-dot  { width: 9px; height: 9px; border-radius: 50%; flex-shrink: 0; }
.tl-line {
    width: 24px; height: 4px; border-radius: 2px; flex-shrink: 0;
    background: linear-gradient(90deg, rgba(99,102,241,0.25), #6366F1);
}

/* ═══════════════════════════════════════════════════════════════════════════
   COLD PATH CARD
═══════════════════════════════════════════════════════════════════════════ */
.cold-card {
    background: #FFFFFF; border-radius: 20px;
    border: 1px solid rgba(226,232,240,0.7);
    box-shadow: 0 10px 15px -3px rgba(0,0,0,0.06);
    padding: 22px 26px; margin-bottom: 22px;
}
.cold-card-header { display: flex; align-items: center; gap: 10px; margin-bottom: 16px; }
.cold-card-title  { font-size: 14px; font-weight: 800; color: #0F172A; }
.cold-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
.cold-stat {
    background: #F8FAFC; border-radius: 12px; padding: 14px 16px;
    border: 1px solid rgba(226,232,240,0.9);
}
.cs-lbl { font-size: 10px; font-weight: 700; color: #94A3B8; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 5px; }
.cs-val { font-size: 1.4rem; font-weight: 800; color: #0F172A; letter-spacing: -0.5px; }
.cs-sub { font-size: 11px; color: #64748B; margin-top: 3px; }

/* ═══════════════════════════════════════════════════════════════════════════
   EMPTY STATE
═══════════════════════════════════════════════════════════════════════════ */
.empty-state {
    padding: 52px 24px; text-align: center;
    background: rgba(248,250,252,0.7);
    border-radius: 16px; border: 2px dashed #E2E8F0; margin: 8px 0;
}
.es-icon { font-size: 40px; margin-bottom: 12px; }
.es-text { font-size: 14px; color: #94A3B8; font-weight: 500; line-height: 1.6; }

/* ═══════════════════════════════════════════════════════════════════════════
   FOOTER / TIMESTAMP
═══════════════════════════════════════════════════════════════════════════ */
.fs-footer {
    text-align: center; font-size: 11px; color: #94A3B8;
    font-family: 'JetBrains Mono', monospace; padding: 10px 0 4px;
}
.live-dot {
    display: inline-block; width: 6px; height: 6px;
    border-radius: 50%; background: #10B981;
    margin-right: 5px; vertical-align: middle;
    animation: pulse-dot 1.8s ease-in-out infinite;
}
@keyframes pulse-dot {
    0%, 100% { opacity: 1; transform: scale(1); }
    50% { opacity: 0.4; transform: scale(0.7); }
}

/* ═══════════════════════════════════════════════════════════════════════════
   BUSINESS INTELLIGENCE
═══════════════════════════════════════════════════════════════════════════ */
.bi-card {
    background: #FFFFFF; border-radius: 20px;
    border: 1px solid rgba(226,232,240,0.7);
    box-shadow: 0 10px 15px -3px rgba(0,0,0,0.06);
    padding: 22px 26px; margin-bottom: 22px;
}
.bi-card-header {
    display: flex; align-items: center; gap: 10px; margin-bottom: 18px;
}
.bi-card-title { font-size: 14px; font-weight: 800; color: #0F172A; }

/* Health score ring */
.health-ring {
    display: flex; align-items: center; gap: 28px; margin-bottom: 18px;
}
.health-score-circle {
    width: 90px; height: 90px; border-radius: 50%;
    display: flex; flex-direction: column; align-items: center; justify-content: center;
    font-size: 2rem; font-weight: 900; line-height: 1;
    border: 5px solid; flex-shrink: 0;
}
.hs-excellent { border-color: #10B981; color: #10B981; background: rgba(16,185,129,0.08); }
.hs-bon       { border-color: #3B82F6; color: #3B82F6; background: rgba(59,130,246,0.08); }
.hs-attention { border-color: #F59E0B; color: #F59E0B; background: rgba(245,158,11,0.08); }
.hs-critique  { border-color: #EF4444; color: #EF4444; background: rgba(239,68,68,0.08); }
.health-score-sub { font-size: 10px; font-weight: 700; letter-spacing: 0.5px; text-transform: uppercase; margin-top: 2px; }

/* KPI row */
.bi-kpi-row { display: grid; grid-template-columns: repeat(3,1fr); gap: 12px; }
.bi-kpi {
    background: #F8FAFC; border-radius: 14px; padding: 14px 16px;
    border: 1px solid rgba(226,232,240,0.9);
}
.bi-kpi-lbl { font-size: 10px; font-weight: 700; color: #94A3B8; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 5px; }
.bi-kpi-val { font-size: 1.5rem; font-weight: 800; color: #0F172A; letter-spacing: -0.5px; }
.bi-kpi-sub { font-size: 11px; color: #64748B; margin-top: 3px; }

/* Driver score */
.grade-badge {
    display: inline-flex; align-items: center; justify-content: center;
    width: 52px; height: 52px; border-radius: 14px;
    font-size: 1.6rem; font-weight: 900; flex-shrink: 0;
}
.grade-A { background: #DCFCE7; color: #166534; }
.grade-B { background: #DBEAFE; color: #1E40AF; }
.grade-C { background: #FEF9C3; color: #854D0E; }
.grade-D { background: #FED7AA; color: #9A3412; }
.grade-E { background: #FEE2E2; color: #991B1B; }

.score-bar-track {
    height: 8px; background: #F1F5F9; border-radius: 999px;
    overflow: hidden; margin-top: 6px;
}
.score-bar-fill { height: 100%; border-radius: 999px; transition: width .8s cubic-bezier(.4,0,.2,1); }

.score-detail-grid { display: grid; grid-template-columns: repeat(2,1fr); gap: 10px; margin-top: 14px; }
.score-detail-item {
    background: #F8FAFC; border-radius: 12px; padding: 12px 14px;
    border: 1px solid rgba(226,232,240,0.9);
}
.sd-lbl { font-size: 10px; font-weight: 700; color: #94A3B8; text-transform: uppercase; letter-spacing: 0.7px; }
.sd-pts { font-size: 1.1rem; font-weight: 800; color: #0F172A; }
.sd-detail { font-size: 11px; color: #64748B; margin-top: 2px; }

/* Anomalies */
.anomaly-item {
    border-radius: 12px; padding: 14px 16px; margin-bottom: 10px;
    border-left: 4px solid;
}
.anomaly-critique { background: #FEF2F2; border-color: #EF4444; }
.anomaly-warning  { background: #FFFBEB; border-color: #F59E0B; }
.anomaly-header { display: flex; align-items: center; gap: 10px; margin-bottom: 6px; }
.anomaly-id { font-size: 13px; font-weight: 800; color: #0F172A; }
.anomaly-badge {
    font-size: 9px; font-weight: 800; letter-spacing: 0.3px;
    border-radius: 20px; padding: 2px 8px; text-transform: uppercase;
}
.ab-critique { background: #FEE2E2; color: #991B1B; }
.ab-warning  { background: #FEF3C7; color: #92400E; }
.anomaly-type { font-size: 11px; font-weight: 700; color: #475569; margin-bottom: 2px; }
.anomaly-detail { font-size: 11px; color: #64748B; }
.anomaly-risque { font-size: 10.5px; color: #94A3B8; margin-top: 3px; font-style: italic; }

/* Zone coverage */
.zone-grid-bi { display: grid; grid-template-columns: repeat(2,1fr); gap: 14px; margin-top: 4px; }
.zone-col-title {
    font-size: 11px; font-weight: 800; letter-spacing: 0.5px;
    text-transform: uppercase; margin-bottom: 10px; padding: 5px 10px;
    border-radius: 8px;
}
.zone-col-under { background: #FEF2F2; color: #991B1B; }
.zone-col-over  { background: #DCFCE7; color: #166534; }
.zone-item {
    background: #F8FAFC; border-radius: 10px; padding: 10px 12px;
    margin-bottom: 7px; border: 1px solid rgba(226,232,240,0.9);
    font-size: 11px;
}
.zi-coords { font-family: 'JetBrains Mono', monospace; font-size: 10px; color: #64748B; }
.zi-stat { font-weight: 700; color: #0F172A; margin-top: 3px; }
.zi-gap  { font-size: 10px; color: #94A3B8; margin-top: 2px; }

.reco-item {
    background: #F0FDF4; border: 1px solid #BBF7D0; border-radius: 10px;
    padding: 10px 14px; margin-bottom: 8px;
    font-size: 12px; color: #166534; line-height: 1.5;
}
.reco-item.warn { background: #FFFBEB; border-color: #FDE68A; color: #92400E; }
.reco-item.ok   { background: #F0FDF4; border-color: #BBF7D0; color: #166534; }

/* ═══════════════════════════════════════════════════════════════════════════
   PREDICT DEMAND — Dispatch prédictif
═══════════════════════════════════════════════════════════════════════════ */
.demand-signal {
    display: inline-flex; align-items: center; gap: 5px;
    font-size: 10px; font-weight: 800; letter-spacing: 0.3px;
    border-radius: 20px; padding: 3px 10px; text-transform: uppercase;
}
.ds-hausse { background: #FEE2E2; color: #991B1B; }
.ds-stable { background: #F1F5F9; color: #64748B; }
.ds-baisse { background: #DCFCE7; color: #166534; }
.demand-zone {
    background: #F8FAFC; border-radius: 12px; padding: 12px 14px;
    border: 1px solid rgba(226,232,240,0.9); margin-bottom: 8px;
}
.dz-coords { font-family: 'JetBrains Mono', monospace; font-size: 10px; color: #64748B; }
.dz-trend { font-size: 1.2rem; font-weight: 800; line-height: 1.2; }
.dz-action { font-size: 11px; color: #475569; margin-top: 4px; font-style: italic; }

/* ─── Divers Streamlit → invisible ───────────────────────────────────────── */
[data-testid="stDivider"] { display: none !important; }
.stDeckGlJsonChart { width: 100% !important; }
iframe { border: none !important; border-radius: 12px !important; }
[data-testid="stInfo"] {
    background: #EFF6FF !important; border: 1.5px solid #BFDBFE !important;
    border-radius: 12px !important; color: #1E3A8A !important; font-weight: 500 !important;
}

/* Force readable foreground on light cards (prevents white-on-white in dark theme inheritance). */
.bi-card, .cold-card, .sla-card, .map-card, .spotlight-container, .bento-card,
.zone-item, .demand-zone, .score-detail-item, .reco-item {
    color: #0F172A !important;
}
.bi-card *:not([style*="color:"]),
.cold-card *:not([style*="color:"]),
.sla-card *:not([style*="color:"]),
.spotlight-container *:not([style*="color:"]) {
    color: inherit;
}
</style>
""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ════════════════════════════════════════════════════════════════════════════
def fetch(path: str, params: dict | None = None) -> dict:
    try:
        return requests.get(f"{API_URL}{path}", params=params, timeout=5).json()
    except Exception:
        return {}


def cached_fetch(
    cache_key: str,
    path: str,
    params: dict | None = None,
    *,
    ttl_seconds: float = 15.0,
    force: bool = False,
) -> dict:
    """
    Soft cache per Streamlit session to avoid hammering heavy endpoints on every
    UI refresh tick. On transient failures, keep the last known payload.
    """
    now = time.time()
    cache = st.session_state.setdefault("_api_cache", {})
    entry = cache.get(cache_key)
    if (not force) and entry and (now - float(entry.get("ts", 0.0)) < ttl_seconds):
        return entry.get("data", {})

    data = fetch(path, params)
    if data:
        cache[cache_key] = {"ts": now, "data": data}
        return data
    if entry:
        return entry.get("data", {})
    return {}


def bento(color: str, icon: str, label: str, value: str, sub: str = "", glyph: str = "") -> str:
    sub_html  = f'<div class="bento-sub">{sub}</div>' if sub else ""
    glph_html = f'<div class="bento-glyph">{glyph}</div>' if glyph else ""
    return (
        f'<div class="bento-card bc-{color}">'
        f'<div class="bento-icon bi-{color}">{icon}</div>'
        f'<div class="bento-lbl">{label}</div>'
        f'<div class="bento-val bv-{color}">{value}</div>'
        f'{sub_html}{glph_html}'
        f'</div>'
    )


def sla_row(label: str, ms: float | None, target: float = 10.0) -> str:
    if ms is None:
        return ""
    pct = min(100, (ms / target) * 100)
    if ms < target * 0.5:
        color, cls = "#10B981", "sla-ok"
    elif ms < target:
        color, cls = "#F59E0B", "sla-warn"
    else:
        color, cls = "#EF4444", "sla-bad"
    return (
        f'<div class="sla-item">'
        f'<div class="sla-item-row">'
        f'<span class="sla-lbl">{label}</span>'
        f'<span class="sla-ms {cls}">{ms:.2f} ms</span>'
        f'</div>'
        f'<div class="sla-track">'
        f'<div class="sla-fill" style="width:{pct:.1f}%;background:{color}"></div>'
        f'</div></div>'
    )


def traj_kpi(label: str, value: str, unit: str = "") -> str:
    unit_html = f'<div class="tk-unit">{unit}</div>' if unit else ""
    return (
        f'<div class="traj-kpi">'
        f'<div class="tk-lbl">{label}</div>'
        f'<div class="tk-val">{value}</div>'
        f'{unit_html}</div>'
    )


def cold_stat(label: str, value: str, sub: str = "") -> str:
    sub_html = f'<div class="cs-sub">{sub}</div>' if sub else ""
    return (
        f'<div class="cold-stat">'
        f'<div class="cs-lbl">{label}</div>'
        f'<div class="cs-val">{value}</div>'
        f'{sub_html}</div>'
    )


# ════════════════════════════════════════════════════════════════════════════
#  NAVBAR — position: fixed via HTML
# ════════════════════════════════════════════════════════════════════════════
st.markdown("""
<div class="fs-navbar">
    <div class="fs-logo">
        <div class="fs-logo-badge">🛵</div>
        <span class="fs-logo-name">Fleet<em>Stream</em></span>
    </div>
    <div class="fs-nav-right">
        <span class="fs-pill">Architecture Lambda</span>
        <span class="fs-pill">Redis · Parquet · DuckDB</span>
        <a class="fs-btn-live" href="http://localhost:8001/map" target="_blank">
            🗺️ Carte live
        </a>
    </div>
</div>
""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
#  SECTION 1 — HOT PATH KPIs (en HAUT, le plus important)
#  Placeholders créés dans l'ORDRE D'AFFICHAGE
# ════════════════════════════════════════════════════════════════════════════
ph_bento = st.empty()

# ── CARTE LIVE FLEET — composant HTML autonome (deck.gl JS) ─────────────
# Rendu UNE SEULE FOIS, jamais remounté. Le JS interne fetch l'API
# toutes les 3s et met à jour les layers deck.gl en place → 0 clignotement.
import streamlit.components.v1 as components

_MAP_API = os.getenv("MAP_API_URL", "http://localhost:8001")
_FLEET_MAP_HTML = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<script src="https://unpkg.com/deck.gl@9.1.4/dist.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@500;700;800&display=swap" rel="stylesheet"/>
<style>
  * {{ margin:0; padding:0; }}
  body {{ background: transparent; font-family: 'Inter', sans-serif; }}
  #map {{ width:100%; height:430px; border-radius:12px; overflow:hidden; background:#e8ecf4; }}
  #hud {{
    position:absolute; top:12px; right:14px; z-index:10;
    display:flex; gap:10px; font-size:11px; font-weight:700;
  }}
  .hud-pill {{
    padding:4px 10px; border-radius:8px;
    background:rgba(15,23,42,0.7); backdrop-filter:blur(8px);
    color:#F8FAFC; display:flex; align-items:center; gap:5px;
  }}
  .hud-dot {{ width:8px; height:8px; border-radius:50%; }}
  #tooltip {{
    position:absolute; z-index:20; pointer-events:none;
    background:#fff; border:1.5px solid #E2E8F0; border-radius:12px;
    padding:10px 14px; box-shadow:0 8px 24px rgba(0,0,0,0.12);
    font-size:12px; display:none; min-width:160px;
  }}
</style>
</head>
<body>
<div style="position:relative">
  <div id="hud">
    <div class="hud-pill"><div class="hud-dot" style="background:#FB923C"></div><span id="n-del">0</span> delivering</div>
    <div class="hud-pill"><div class="hud-dot" style="background:#34D399"></div><span id="n-av">0</span> available</div>
    <div class="hud-pill"><div class="hud-dot" style="background:#94A3B8"></div><span id="n-idl">0</span> idle</div>
  </div>
  <div id="tooltip"></div>
  <div id="map"></div>
</div>
<script>
const API = "{_MAP_API}";
const STATUS_COLORS = {{
  delivering: [251,146,60,220],
  available:  [52,211,153,220],
  idle:       [148,163,184,150],
}};
const MAP_QUERY = '/livreurs-proches?lat=40.7580&lon=-73.9855&rayon=28&limit=500';
const MAP_POLL_BASE_MS = 2000;
const MAP_POLL_MAX_MS = 20000;
const MAP_REQUEST_TIMEOUT_MS = 4500;
const MAP_TRANSITION_MS = 2200;
let mapPollInFlight = false;
let mapPollFailures = 0;
let mapPollDelayMs = MAP_POLL_BASE_MS;
let mapPollTimer = null;
let mapHidden = document.hidden === true;

document.addEventListener('visibilitychange', () => {{
  mapHidden = document.hidden === true;
}});

function isAbortLike(err) {{
  if (!err) return false;
  const name = String(err.name || '').toLowerCase();
  const msg = String(err.message || '').toLowerCase();
  return name.includes('abort') || msg.includes('abort');
}}

function scheduleRefresh(delayMs) {{
  const nextDelay = Number.isFinite(delayMs) ? Math.max(900, delayMs) : MAP_POLL_BASE_MS;
  if (mapPollTimer) clearTimeout(mapPollTimer);
  mapPollTimer = setTimeout(refresh, nextDelay);
}}

function computeMapBackoffMs() {{
  if (mapPollFailures <= 0) return MAP_POLL_BASE_MS;
  return Math.min(MAP_POLL_MAX_MS, MAP_POLL_BASE_MS * Math.pow(2, Math.min(mapPollFailures, 4)));
}}

const BASEMAP = new deck.TileLayer({{
  id: 'basemap',
  data: [
    'https://a.basemaps.cartocdn.com/rastertiles/voyager/{{z}}/{{x}}/{{y}}.png',
    'https://b.basemaps.cartocdn.com/rastertiles/voyager/{{z}}/{{x}}/{{y}}.png',
    'https://c.basemaps.cartocdn.com/rastertiles/voyager/{{z}}/{{x}}/{{y}}.png',
    'https://d.basemaps.cartocdn.com/rastertiles/voyager/{{z}}/{{x}}/{{y}}.png',
  ],
  tileSize: 256,
  minZoom: 0,
  maxZoom: 19,
  renderSubLayers: props => {{
    const {{ bbox: {{ west, south, east, north }} }} = props.tile;
    return new deck.BitmapLayer(props, {{
      data: null,
      image: props.data,
      bounds: [west, south, east, north],
    }});
  }},
}});

const deckgl = new deck.DeckGL({{
  container: 'map',
  initialViewState: {{ latitude:40.7580, longitude:-73.9855, zoom:11, pitch:0 }},
  controller: true,
  layers: [BASEMAP],
  getTooltip: null,
  onHover: (info) => {{
    const tt = document.getElementById('tooltip');
    if (info.object) {{
      const d = info.object;
      tt.innerHTML =
        '<b style="color:#6366F1;font-size:14px">' + d.id + '</b><br/>' +
        '<span style="color:#475569">' + d.status + '</span><br/>' +
        '<span style="color:#64748B;font-size:11px">⚡ ' + d.speed.toFixed(1) + ' km/h · 🧭 ' + d.heading.toFixed(0) + '°</span><br/>' +
        '<span style="color:#64748B;font-size:11px">🔋 ' + d.battery.toFixed(0) + '%</span>';
      tt.style.display = 'block';
      tt.style.left = (info.x + 12) + 'px';
      tt.style.top  = (info.y + 12) + 'px';
    }} else {{
      tt.style.display = 'none';
    }}
  }}
}});

async function refresh() {{
  if (mapHidden) {{
    scheduleRefresh(Math.max(1000, MAP_POLL_BASE_MS * 2));
    return;
  }}
  if (mapPollInFlight) {{
    scheduleRefresh(mapPollDelayMs);
    return;
  }}
  mapPollInFlight = true;
  const startedAt = performance.now();
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), MAP_REQUEST_TIMEOUT_MS);
  try {{
    const r = await fetch(API + MAP_QUERY, {{
      cache: 'no-store',
      signal: controller.signal,
    }});
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const data = await r.json();
    const livreurs = (data.livreurs || []).map(lv => ({{
      position: [lv.lon, lv.lat],
      color: STATUS_COLORS[lv.status] || [148,163,184,150],
      id: lv.livreur_id,
      status: lv.status,
      speed: lv.speed_kmh || 0,
      heading: lv.heading_deg || 0,
      battery: lv.battery_pct || 0,
    }}));
    livreurs.sort((a, b) => String(a.id || '').localeCompare(String(b.id || '')));

    deckgl.setProps({{
      layers: [
        BASEMAP,
        new deck.ScatterplotLayer({{
          id: 'glow',
          data: livreurs,
          getPosition: d => d.position,
          getFillColor: d => [...d.color.slice(0,3), 35],
          getRadius: 300,
          radiusMinPixels: 10,
          radiusMaxPixels: 30,
          transitions: {{ getPosition: MAP_TRANSITION_MS }},
        }}),
        new deck.ScatterplotLayer({{
          id: 'fleet',
          data: livreurs,
          getPosition: d => d.position,
          getFillColor: d => d.color,
          getRadius: 120,
          radiusMinPixels: 5,
          radiusMaxPixels: 14,
          pickable: true,
          autoHighlight: true,
          highlightColor: [99,102,241,255],
          transitions: {{ getPosition: MAP_TRANSITION_MS }},
        }}),
      ]
    }});

    let nd=0, na=0, ni=0;
    livreurs.forEach(l => {{
      if (l.status==='delivering') nd++;
      else if (l.status==='available') na++;
      else ni++;
    }});
    document.getElementById('n-del').textContent = nd;
    document.getElementById('n-av').textContent  = na;
    document.getElementById('n-idl').textContent = ni;
    mapPollFailures = 0;
    mapPollDelayMs = MAP_POLL_BASE_MS;
  }} catch(e) {{
    if (!isAbortLike(e)) {{
      console.warn('Fleet refresh error:', e);
    }}
    mapPollFailures = Math.min(mapPollFailures + 1, 6);
    mapPollDelayMs = computeMapBackoffMs();
  }} finally {{
    clearTimeout(timeoutId);
    mapPollInFlight = false;
    const elapsedMs = performance.now() - startedAt;
    const jitterMs = Math.random() * 250;
    scheduleRefresh(Math.max(900, mapPollDelayMs - elapsedMs + jitterMs));
  }}
}}

scheduleRefresh(0);
</script>
</body>
</html>
"""

st.markdown("""
<div class="map-card">
    <div class="map-card-header">
        <span>🗺️</span>
        <span>Flotte en temps réel — New York City · Auto-refresh 2s</span>
    </div>
""", unsafe_allow_html=True)
components.html(_FLEET_MAP_HTML, height=460, scrolling=False)
st.markdown("</div>", unsafe_allow_html=True)

# ── SLA + Cold Path côte à côte ──────────────────────────────────────────
col_sla, col_cold = st.columns(2)
with col_sla:
    ph_sla = st.empty()
with col_cold:
    ph_cold = st.empty()

# ════════════════════════════════════════════════════════════════════════════
#  SECTION 2 — BUSINESS INTELLIGENCE
# ════════════════════════════════════════════════════════════════════════════
st.markdown("""
<div class="fs-sh" style="margin-top:32px">
    <div class="overline">Business Intelligence · DuckDB Analytics</div>
    <div class="heading">Performance & Sante de la Flotte</div>
    <div class="sub">
        Indicateurs operationnels temps reel, score de performance livreur,
        couverture territoriale, detection d'anomalies et dispatch predictif.
    </div>
</div>
""", unsafe_allow_html=True)

# ── Driver Score — widget de recherche ──────────────────────────────────────
st.markdown("""
<div class="spotlight-container">
    <div class="spotlight-header"><span>🏆</span> Score de performance — Analyse individuelle</div>
""", unsafe_allow_html=True)
col_ds_id, col_ds_h, col_ds_btn = st.columns([3, 2, 1])
with col_ds_id:
    ds_livreur_id = st.text_input(
        "DS ID", value="L042", placeholder="ex: L007, L042…",
        key="ds_livreur_id", label_visibility="collapsed",
    )
with col_ds_h:
    ds_heures = st.selectbox(
        "DS Fenêtre", [1, 2, 4, 8, 24],
        format_func=lambda h: f"Dernière{'s' if h > 1 else ''} {h}h",
        key="ds_heures", label_visibility="collapsed",
    )
with col_ds_btn:
    ds_analyse = st.button("🏆 Scorer", key="ds_btn", use_container_width=True)
st.markdown("</div>", unsafe_allow_html=True)

if ds_analyse:
    st.session_state.last_ds = fetch(
        f"/analytics/driver-score/{ds_livreur_id}",
        {"heures": ds_heures},
    )

ph_driver_score = st.empty()

# ── Placeholders BI auto-refresh ─────────────────────────────────────────────
ph_insights  = st.empty()

col_anom, col_demand = st.columns(2)
with col_anom:
    ph_anomalies = st.empty()
with col_demand:
    ph_predict = st.empty()

ph_fraud  = st.empty()
ph_zones  = st.empty()


# ════════════════════════════════════════════════════════════════════════════
#  SECTION 2b — COPILOT DRIVER COCKPIT
# ════════════════════════════════════════════════════════════════════════════
st.markdown("""
<div class="fs-sh" style="margin-top:32px">
    <div class="overline">Driver Revenue Copilot · ML + Open Data</div>
    <div class="heading">Cockpit Chauffeur</div>
    <div class="sub">
        Score d'acceptation des offres, zones de repositionnement,
        signaux Citi Bike (GBFS) et bornes de recharge (OpenChargeMap).
    </div>
</div>
""", unsafe_allow_html=True)

# ── Copilot — Score an offer ─────────────────────────────────────────────────
st.markdown("""
<div class="spotlight-container">
    <div class="spotlight-header"><span>🎯</span> Copilot — Scorer une offre</div>
""", unsafe_allow_html=True)
col_cp_id, col_cp_btn = st.columns([4, 1])
with col_cp_id:
    cp_driver_id = st.text_input(
        "CP Driver", value="L042", placeholder="ex: L007, L042...",
        key="cp_driver_id", label_visibility="collapsed",
    )
with col_cp_btn:
    cp_score_btn = st.button("🎯 Offres", key="cp_score_btn", use_container_width=True)
st.markdown("</div>", unsafe_allow_html=True)

if cp_score_btn:
    st.session_state.last_cp_offers = fetch(f"/copilot/driver/{cp_driver_id}/offers", {"limit": 10})
    st.session_state.last_cp_zones = fetch(f"/copilot/driver/{cp_driver_id}/next-best-zone", {"top_k": 5})
    st.session_state.last_cp_health = fetch("/copilot/health")

ph_copilot = st.empty()

# ── Copilot GBFS/IRVE context ─────────────────────────────────────────────────
col_gbfs, col_irve = st.columns(2)
with col_gbfs:
    ph_gbfs = st.empty()
with col_irve:
    ph_irve = st.empty()


# ════════════════════════════════════════════════════════════════════════════
#  SECTION 3 — COLD PATH — Analyse trajectoire
# ════════════════════════════════════════════════════════════════════════════
st.markdown("""
<div class="fs-sh" style="margin-top:32px">
    <div class="overline">Cold Path · DuckDB → Apache Parquet</div>
    <div class="heading">Analyse de trajectoire</div>
    <div class="sub">Historique complet depuis le Data Lake — distance Haversine, vitesses, statut dominant.</div>
</div>
""", unsafe_allow_html=True)

# Spotlight search container
st.markdown('<div class="spotlight-container">', unsafe_allow_html=True)
st.markdown(
    '<div class="spotlight-header"><span>🔍</span> Rechercher un livreur</div>',
    unsafe_allow_html=True,
)
col_id, col_h, col_btn = st.columns([3, 2, 1])
with col_id:
    livreur_id = st.text_input(
        "ID", value="L042", placeholder="ex: L007, L042…",
        key="hist_livreur_id", label_visibility="collapsed",
    )
with col_h:
    heures = st.selectbox(
        "Fenêtre", [1, 2, 4, 8, 24],
        format_func=lambda h: f"Dernière{'s' if h > 1 else ''} {h}h",
        key="hist_heures", label_visibility="collapsed",
    )
with col_btn:
    analyse = st.button("⚡ Analyser", key="hist_btn", use_container_width=True)
st.markdown("</div>", unsafe_allow_html=True)

ph_hist = st.empty()

if "last_hist"  not in st.session_state: st.session_state.last_hist  = {}
if "last_lv_id" not in st.session_state: st.session_state.last_lv_id = livreur_id

if analyse or livreur_id != st.session_state.last_lv_id:
    st.session_state.last_lv_id = livreur_id
    with st.spinner(f"Requête DuckDB — {livreur_id}, {heures}h…"):
        st.session_state.last_hist = fetch(
            f"/analytics/history/{livreur_id}", {"heures": heures}
        )

hist = st.session_state.last_hist

with ph_hist.container():
    if "resume" in hist:
        r    = hist["resume"]
        traj = hist.get("trajectory", [])

        # KPIs trajectoire — HTML pur
        st.markdown(
            '<div class="traj-kpis">'
            + traj_kpi("Points GPS",      f"{r.get('nb_points', 0):,}")
            + traj_kpi("Distance totale", f"{r.get('distance_totale_km', 0):.2f}", "km")
            + traj_kpi("Vitesse moyenne", f"{r.get('vitesse_moy_kmh', 0):.1f}", "km/h")
            + traj_kpi("Vitesse max",     f"{r.get('vitesse_max_kmh', 0):.1f}", "km/h")
            + "</div>",
            unsafe_allow_html=True,
        )

        if traj:
            df = pd.DataFrame([
                {
                    "lat":    p["lat"],
                    "lon":    p["lon"],
                    "speed":  p.get("speed_kmh") or 0.0,
                    "status": p.get("status", "idle"),
                    "color":  STATUS_COLOR_RGB.get(p.get("status", "idle"), [148, 163, 184, 150]),
                }
                for p in traj
            ])
            n = len(df)

            # Segments avec alpha progressif (temporel)
            step = max(1, n // 80)
            segs = []
            for i in range(0, n - step, step):
                alpha = int(60 + 180 * (i / max(n - step, 1)))
                pt_a = [df.iloc[i]["lon"], df.iloc[i]["lat"]]
                pt_b = [df.iloc[min(i + step, n - 1)]["lon"],
                        df.iloc[min(i + step, n - 1)]["lat"]]
                segs.append({"path": [pt_a, pt_b], "color": [99, 102, 241, alpha]})

            layer_outer = pdk.Layer(
                "PathLayer", data=segs,
                get_path="path", get_color=[120, 80, 255, 20],
                get_width=18, width_min_pixels=9,
                rounded=True, joint_rounded=True,
            )
            layer_inner = pdk.Layer(
                "PathLayer", data=segs,
                get_path="path", get_color=[99, 102, 241, 55],
                get_width=9, width_min_pixels=5,
                rounded=True, joint_rounded=True,
            )
            layer_path = pdk.Layer(
                "PathLayer", data=segs,
                get_path="path", get_color="color",
                get_width=3, width_min_pixels=2, width_max_pixels=7,
                rounded=True, joint_rounded=True,
            )

            layer_scatter = pdk.Layer(
                "ScatterplotLayer", data=df,
                get_position=["lon", "lat"],
                get_fill_color="color",
                get_radius=40, radius_min_pixels=2, radius_max_pixels=9,
                pickable=True, auto_highlight=True,
                highlight_color=[99, 102, 241, 255],
            )

            endpoints = pd.DataFrame([
                {"lon": df.iloc[0]["lon"],  "lat": df.iloc[0]["lat"],
                 "color": [16, 185, 129, 255], "r": 90},
                {"lon": df.iloc[-1]["lon"], "lat": df.iloc[-1]["lat"],
                 "color": [239, 68, 68, 255],  "r": 90},
            ])
            layer_ep = pdk.Layer(
                "ScatterplotLayer", data=endpoints,
                get_position=["lon", "lat"],
                get_fill_color="color", get_radius="r",
                radius_min_pixels=8, radius_max_pixels=16,
                stroked=True, get_line_color=[255, 255, 255, 220],
                line_width_min_pixels=2,
            )

            deck = pdk.Deck(
                layers=[layer_outer, layer_inner, layer_path, layer_scatter, layer_ep],
                initial_view_state=pdk.ViewState(
                    latitude=df["lat"].mean(),
                    longitude=df["lon"].mean(),
                    zoom=13, pitch=35,
                ),
                tooltip={
                    "html": (
                        "<div style='font-family:Inter,sans-serif;"
                        "padding:6px 4px;min-width:130px'>"
                        "<b style='color:#6366F1;font-size:13px'>{status}</b><br/>"
                        "<span style='color:#64748B;font-size:12px'>⚡ {speed:.1f} km/h</span>"
                        "</div>"
                    ),
                    "style": {
                        "background": "#FFFFFF",
                        "color": "#0F172A",
                        "fontSize": "12px",
                        "borderRadius": "10px",
                        "border": "1.5px solid #E2E8F0",
                        "padding": "8px 12px",
                        "boxShadow": "0 8px 24px rgba(0,0,0,0.1)",
                    },
                },
                map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
            )

            st.markdown("""
            <div class="map-card">
                <div class="map-card-header">
                    <span>📍</span>
                    <span>Trajectoire — CartoDB Positron · pydeck GL</span>
                </div>
            """, unsafe_allow_html=True)
            st.pydeck_chart(deck, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

            st.markdown("""
            <div class="traj-legend">
                <div class="tl-item"><div class="tl-line"></div> Trajectoire (néon Indigo)</div>
                <div class="tl-item"><div class="tl-dot" style="background:#FB923C"></div> En livraison</div>
                <div class="tl-item"><div class="tl-dot" style="background:#34D399"></div> Disponible · Départ</div>
                <div class="tl-item"><div class="tl-dot" style="background:#EF4444"></div> Arrivée</div>
                <div class="tl-item"><div class="tl-dot" style="background:#94A3B8"></div> Inactif</div>
            </div>
            """, unsafe_allow_html=True)

    elif "detail" in hist:
        st.info(f"ℹ️ {hist['detail']}")
    else:
        st.markdown("""
        <div class="empty-state">
            <div class="es-icon">📡</div>
            <div class="es-text">
                Saisir un identifiant livreur (ex. <strong>L042</strong>)<br/>
                et cliquer sur <strong>⚡ Analyser</strong>
            </div>
        </div>
        """, unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
#  FOOTER placeholder
# ════════════════════════════════════════════════════════════════════════════
ph_ts = st.empty()


# ════════════════════════════════════════════════════════════════════════════
#  BOUCLE — met à jour uniquement les placeholders
# ════════════════════════════════════════════════════════════════════════════
while True:
    stats = cached_fetch("stats", "/stats", ttl_seconds=max(2.0, float(REFRESH_SECONDS)))
    hp    = stats.get("hot_path", {})
    cp    = stats.get("cold_path", {})
    cts   = hp.get("statuts", {})

    # ══════════════════════════════════════════════════════════════════════
    #  BENTO GRID + LIVE MAP (Section 1 — en haut)
    # ══════════════════════════════════════════════════════════════════════
    with ph_bento.container():
        n_msgs = hp.get("messages_traites", 0)
        st.markdown("""
        <div class="fs-sh" style="margin-top:24px">
            <div class="overline">Hot Path · Redis GEOSEARCH</div>
            <div class="heading">Métriques temps réel</div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown(
            '<div class="bento-grid">'
            + bento("indigo",  "🛵", "Livreurs actifs",
                    str(hp.get("livreurs_actifs", 0)),
                    "Redis TTL 30s", "🛵")
            + bento("sky",     "📨", "Messages GPS",
                    f"{n_msgs:,}" if isinstance(n_msgs, int) else "—",
                    "Redpanda → Hot path", "📡")
            + bento("amber",   "📦", "En livraison",
                    str(cts.get("delivering", 0)),
                    "Statut delivering", "🚀")
            + bento("emerald", "✅", "Disponibles",
                    str(cts.get("available", 0)),
                    "Statut available", "✓")
            + bento("slate",   "💤", "Inactifs",
                    str(cts.get("idle", 0)),
                    "Statut idle", "⏸")
            + "</div>",
            unsafe_allow_html=True,
        )

    # ── SLA GEOSEARCH ─────────────────────────────────────────────────────
    perf  = cached_fetch("perf", "/health/performance", {"samples": 20}, ttl_seconds=90.0)
    bench = perf.get("geosearch_benchmark", {})
    rinfo = perf.get("redis_info", {})
    p99   = bench.get("p99_ms")
    sla_ok = p99 is not None and p99 < 10

    with ph_sla.container():
        badge = (
            '<span class="sla-badge-ok">✅ SLA OK — p99 &lt; 10 ms</span>'
            if sla_ok else
            '<span class="sla-badge-ko">⚠️ SLA KO</span>'
        )
        bars = (
            sla_row("P50", bench.get("p50_ms")) +
            sla_row("P95", bench.get("p95_ms")) +
            sla_row("P99", bench.get("p99_ms")) +
            sla_row("Max", bench.get("max_ms"), target=15)
        ) if bench else "<p style='color:#94A3B8;font-size:13px;padding:4px 0'>Chargement…</p>"

        redis_extra = ""
        if rinfo:
            redis_extra = (
                "<div style='display:flex;gap:20px;margin-top:14px;padding-top:14px;"
                "border-top:1px solid #F1F5F9;flex-wrap:wrap'>"
                + "".join(
                    f"<div style='font-size:12px;color:#64748B'>"
                    f"<span style='font-weight:700;color:#0F172A'>{v}</span>"
                    f"<span style='margin-left:4px'>{lbl}</span></div>"
                    for lbl, v in [
                        ("version",  rinfo.get("version", "—")),
                        ("mémoire",  rinfo.get("used_memory_human", "—")),
                        ("ops/sec",  rinfo.get("ops_per_sec", "—")),
                        ("clients",  rinfo.get("connected_clients", "—")),
                    ]
                )
                + "</div>"
            )

        st.markdown(
            f'<div class="sla-card">'
            f'<div class="sla-card-header">'
            f'<span class="sla-card-title">📊 SLA GEOSEARCH</span>'
            f'{badge}'
            f'<span style="font-size:11px;color:#94A3B8;margin-left:auto">Redis Stack 7.2</span>'
            f'</div>'
            f'{bars}{redis_extra}'
            f'</div>',
            unsafe_allow_html=True,
        )

    # ── Cold Path Card ────────────────────────────────────────────────────
    with ph_cold.container():
        nb_files = cp.get("fichiers_parquet", 0)
        taille   = cp.get("taille_totale_mb", 0)
        st.markdown(
            '<div class="cold-card">'
            '<div class="cold-card-header">'
            '<span style="font-size:18px">🗄️</span>'
            '<span class="cold-card-title">Batch Layer · Apache Parquet</span>'
            '</div>'
            '<div class="cold-grid">'
            + cold_stat("Fichiers Parquet", str(nb_files),       "Snappy compression")
            + cold_stat("Taille totale",    f"{taille:.1f} MB",  "Data Lake local")
            + cold_stat("Compression",      "Snappy",            "Ratio ~3×")
            + cold_stat("Partitionnement",  "Hive",              "year/month/day/hour")
            + "</div></div>",
            unsafe_allow_html=True,
        )

    # ══════════════════════════════════════════════════════════════════════
    #  SECTION BI — auto-refresh
    # ══════════════════════════════════════════════════════════════════════

    # ── Driver Score (résultat statique mis à jour sur clic) ─────────────
    ds = st.session_state.get("last_ds", {})
    with ph_driver_score.container():
        if ds and "score_global" in ds:
            grade = ds.get("grade", "?")
            score = ds.get("score_global", 0)
            interp = ds.get("interpretation", "")
            det = ds.get("details_score", {})
            met = ds.get("metriques", {})
            score_color = (
                "#10B981" if grade == "A" else
                "#3B82F6" if grade == "B" else
                "#F59E0B" if grade == "C" else
                "#EF4444"
            )
            prod = det.get("productivite", {}); sec = det.get("securite", {})
            fid  = det.get("fiabilite", {});   act = det.get("activite", {})
            st.markdown(
                f'<div class="bi-card">'
                f'<div class="bi-card-header">'
                f'<span style="font-size:18px">🏆</span>'
                f'<span class="bi-card-title">Score Livreur — {ds.get("livreur_id","?")} '
                f'· {ds.get("periode_heures","?")}h</span>'
                f'</div>'
                f'<div class="health-ring">'
                f'<div class="grade-badge grade-{grade}">{grade}</div>'
                f'<div style="flex:1">'
                f'<div style="font-size:2.2rem;font-weight:900;color:{score_color};line-height:1">{score}<span style="font-size:1rem;color:#94A3B8;font-weight:600">/100</span></div>'
                f'<div style="font-size:12px;color:#475569;margin-top:4px">{interp}</div>'
                f'<div class="score-bar-track" style="margin-top:10px">'
                f'<div class="score-bar-fill" style="width:{score}%;background:{score_color}"></div>'
                f'</div>'
                f'</div>'
                f'</div>'
                f'<div class="score-detail-grid">'
                f'<div class="score-detail-item"><div class="sd-lbl">🚀 Productivité</div>'
                f'<div class="sd-pts">{prod.get("points","?")} <span style="font-size:11px;color:#94A3B8">/ {prod.get("max",40)} pts</span></div>'
                f'<div class="sd-detail">{prod.get("detail","")}</div></div>'
                f'<div class="score-detail-item"><div class="sd-lbl">🛡️ Sécurité</div>'
                f'<div class="sd-pts">{sec.get("points","?")} <span style="font-size:11px;color:#94A3B8">/ {sec.get("max",30)} pts</span></div>'
                f'<div class="sd-detail">{sec.get("detail","")}</div></div>'
                f'<div class="score-detail-item"><div class="sd-lbl">🎯 Fiabilité</div>'
                f'<div class="sd-pts">{fid.get("points","?")} <span style="font-size:11px;color:#94A3B8">/ {fid.get("max",20)} pts</span></div>'
                f'<div class="sd-detail">{fid.get("detail","")}</div></div>'
                f'<div class="score-detail-item"><div class="sd-lbl">⚡ Activité</div>'
                f'<div class="sd-pts">{act.get("points","?")} <span style="font-size:11px;color:#94A3B8">/ {act.get("max",10)} pts</span></div>'
                f'<div class="sd-detail">{act.get("detail","")}</div></div>'
                f'</div>'
                f'<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:14px">'
                f'<div class="bi-kpi"><div class="bi-kpi-lbl">Distance</div>'
                f'<div class="bi-kpi-val">{met.get("distance_parcourue_km","—")} <span style="font-size:11px;color:#94A3B8">km</span></div></div>'
                f'<div class="bi-kpi"><div class="bi-kpi-lbl">Vitesse moy.</div>'
                f'<div class="bi-kpi-val">{met.get("vitesse_moyenne_kmh","—")} <span style="font-size:11px;color:#94A3B8">km/h</span></div></div>'
                f'<div class="bi-kpi"><div class="bi-kpi-lbl">Taux livraison</div>'
                f'<div class="bi-kpi-val">{met.get("taux_livraison_pct","—")} <span style="font-size:11px;color:#94A3B8">%</span></div></div>'
                f'</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
        elif "detail" in ds:
            st.info(f"ℹ️ {ds['detail']}")

    # ── Fleet Insights (auto-refresh) ─────────────────────────────────────
    insights = cached_fetch("fleet_insights", "/analytics/fleet-insights", ttl_seconds=float(ANALYTICS_TTL_SECONDS))
    with ph_insights.container():
        if insights and "sante_operationnelle" in insights:
            sante = insights.get("sante_operationnelle", {})
            score_s = sante.get("score", 0)
            niveau  = sante.get("niveau", "bon")
            hs_cls  = f"hs-{niveau}"
            fleet   = insights.get("flotte_temps_reel", {})
            prod_h  = insights.get("productivite_historique", {})
            alertes = insights.get("alertes", {})
            recos   = insights.get("recommandations", [])

            suspects_html = ""
            for s in alertes.get("detail_suspects", [])[:3]:
                suspects_html += (
                    f'<div style="font-size:11px;color:#991B1B;padding:4px 8px;'
                    f'background:#FEE2E2;border-radius:7px;margin-bottom:4px">'
                    f'⚠️ <b>{s.get("livreur_id")}</b> · {s.get("speed_kmh")} km/h · {s.get("status")}'
                    f'</div>'
                )

            reco_html = "".join(
                f'<div class="reco-item {"ok" if "nominale" in r else "warn"}">'
                f'{"✅" if "nominale" in r else "⚠️"} {r}</div>'
                for r in recos
            )

            st.markdown(
                f'<div class="bi-card">'
                f'<div class="bi-card-header">'
                f'<span style="font-size:18px">📊</span>'
                f'<span class="bi-card-title">Santé Opérationnelle · Temps Réel</span>'
                f'<span style="margin-left:auto;font-size:10px;color:#94A3B8">'
                f'Fenêtre {insights.get("periode_heures","?")}h</span>'
                f'</div>'
                f'<div class="health-ring">'
                f'<div class="health-score-circle {hs_cls}">'
                f'{score_s}<div class="health-score-sub">{niveau}</div>'
                f'</div>'
                f'<div style="flex:1">'
                f'<div class="bi-kpi-row">'
                f'<div class="bi-kpi"><div class="bi-kpi-lbl">Utilisation</div>'
                f'<div class="bi-kpi-val">{fleet.get("taux_utilisation_pct","—")}<span style="font-size:11px;color:#94A3B8"> %</span></div>'
                f'<div class="bi-kpi-sub">delivering / total</div></div>'
                f'<div class="bi-kpi"><div class="bi-kpi-lbl">Disponibles</div>'
                f'<div class="bi-kpi-val">{fleet.get("taux_disponibilite_pct","—")}<span style="font-size:11px;color:#94A3B8"> %</span></div>'
                f'<div class="bi-kpi-sub">prêts à livrer</div></div>'
                f'<div class="bi-kpi"><div class="bi-kpi-lbl">Vitesse moy.</div>'
                f'<div class="bi-kpi-val">{prod_h.get("vitesse_moyenne_kmh","—")}<span style="font-size:11px;color:#94A3B8"> km/h</span></div>'
                f'<div class="bi-kpi-sub">flotte complète</div></div>'
                f'</div></div></div>'
                + (f'<div style="margin-bottom:12px">'
                   f'<div style="font-size:11px;font-weight:700;color:#EF4444;margin-bottom:6px">'
                   f'🚨 {alertes.get("livreurs_immobiles_en_livraison",0)} livreur(s) suspect(s)</div>'
                   f'{suspects_html}</div>' if alertes.get("livreurs_immobiles_en_livraison", 0) > 0 else "")
                + reco_html
                + '</div>',
                unsafe_allow_html=True,
            )

    # ── Anomalies (auto-refresh) ──────────────────────────────────────────
    anomalies_data = cached_fetch(
        "analytics_anomalies",
        "/analytics/anomalies",
        {"fenetre_minutes": 10},
        ttl_seconds=float(ANALYTICS_TTL_SECONDS),
    )
    with ph_anomalies.container():
        if anomalies_data:
            resume = anomalies_data.get("resume", {})
            anom_list = anomalies_data.get("anomalies", [])
            nb_crit = resume.get("critiques", 0)
            nb_warn = resume.get("warnings", 0)

            anomalies_html = ""
            if anom_list:
                for a in anom_list[:6]:
                    niv = a.get("niveau", "warning")
                    cls = f"anomaly-{niv}"
                    badge_cls = f"ab-{niv}"
                    types_html = "".join(
                        f'<div class="anomaly-type">▸ {t.get("type","")}</div>'
                        f'<div class="anomaly-detail">{t.get("detail","")}</div>'
                        f'<div class="anomaly-risque">{t.get("risque","")}</div>'
                        for t in a.get("anomalies", [])
                    )
                    anomalies_html += (
                        f'<div class="anomaly-item {cls}">'
                        f'<div class="anomaly-header">'
                        f'<span class="anomaly-id">{a.get("livreur_id","?")}</span>'
                        f'<span class="anomaly-badge {badge_cls}">{niv.upper()}</span>'
                        f'<span style="margin-left:auto;font-size:11px;color:#94A3B8">'
                        f'z={a.get("z_score","?")} · {a.get("vitesse_max_kmh","?")} km/h max</span>'
                        f'</div>{types_html}</div>'
                    )
            else:
                anomalies_html = (
                    '<div class="reco-item ok">✅ Aucune anomalie comportementale '
                    f'détectée sur les {anomalies_data.get("fenetre_minutes",10)} dernières minutes '
                    f'({resume.get("livreurs_scannes",0)} livreurs analysés).</div>'
                )

            header_color = "#EF4444" if nb_crit > 0 else "#F59E0B" if nb_warn > 0 else "#10B981"
            st.markdown(
                f'<div class="bi-card">'
                f'<div class="bi-card-header">'
                f'<span style="font-size:18px">🔬</span>'
                f'<span class="bi-card-title">Anomalies · ML</span>'
                f'<span style="margin-left:auto;font-size:10px;color:{header_color};font-weight:700">'
                f'{len(anom_list)} anomalie(s)</span>'
                f'</div>'
                f'{anomalies_html}'
                f'<div style="font-size:10px;color:#94A3B8;margin-top:10px;font-style:italic">'
                f'Méthode : {anomalies_data.get("methodologie","")}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    # ── Predict Demand (auto-refresh) ─────────────────────────────────────
    demand_data = cached_fetch(
        "analytics_predict_demand",
        "/analytics/predict-demand",
        {"horizon_minutes": 30},
        ttl_seconds=float(ANALYTICS_TTL_SECONDS),
    )
    with ph_predict.container():
        if demand_data and "resume" in demand_data:
            d_resume = demand_data.get("resume", {})
            dispatch = demand_data.get("dispatch_prioritaire", [])
            cold_z   = demand_data.get("zones_a_decouvrir", [])

            dispatch_html = ""
            for z in dispatch[:4]:
                trend = z.get("tendance_pct", 0)
                dispatch_html += (
                    f'<div class="demand-zone">'
                    f'<div style="display:flex;justify-content:space-between;align-items:center">'
                    f'<div class="dz-coords">lat {z.get("lat","?")} · lon {z.get("lon","?")}</div>'
                    f'<span class="demand-signal ds-hausse">🔺 +{trend}%</span>'
                    f'</div>'
                    f'<div class="dz-action">→ {z.get("action","")}</div>'
                    f'</div>'
                )

            if not dispatch:
                dispatch_html = (
                    '<div class="reco-item ok">✅ Aucune zone en hausse significative '
                    '— couverture stable.</div>'
                )

            st.markdown(
                f'<div class="bi-card">'
                f'<div class="bi-card-header">'
                f'<span style="font-size:18px">📡</span>'
                f'<span class="bi-card-title">Dispatch Prédictif</span>'
                f'<span style="margin-left:auto;font-size:10px;color:#94A3B8">'
                f'Horizon 30 min</span>'
                f'</div>'
                f'<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:14px">'
                f'<div class="bi-kpi"><div class="bi-kpi-lbl">🔺 Zones en hausse</div>'
                f'<div class="bi-kpi-val" style="color:#EF4444">{d_resume.get("zones_en_hausse",0)}</div></div>'
                f'<div class="bi-kpi"><div class="bi-kpi-lbl">➡️ Stables</div>'
                f'<div class="bi-kpi-val" style="color:#64748B">{d_resume.get("zones_stables",0)}</div></div>'
                f'<div class="bi-kpi"><div class="bi-kpi-lbl">🔻 En baisse</div>'
                f'<div class="bi-kpi-val" style="color:#10B981">{d_resume.get("zones_en_baisse",0)}</div></div>'
                f'</div>'
                f'<div style="font-size:11px;font-weight:700;color:#0F172A;margin-bottom:8px">'
                f'Zones prioritaires — envoi de livreurs recommandé</div>'
                f'{dispatch_html}'
                f'<div style="font-size:10px;color:#94A3B8;margin-top:10px;font-style:italic">'
                f'Méthode : {demand_data.get("methode","")}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    # ── GPS Fraud Detection (auto-refresh) ───────────────────────────────
    fraud_data = cached_fetch(
        "analytics_gps_fraud",
        "/analytics/gps-fraud",
        {"fenetre_minutes": 15},
        ttl_seconds=float(ANALYTICS_TTL_SECONDS),
    )
    with ph_fraud.container():
        if fraud_data and "resume" in fraud_data:
            f_resume = fraud_data.get("resume", {})
            teleports = fraud_data.get("teleportations", [])
            frozen_list = fraud_data.get("positions_figees", [])
            total_frauds = f_resume.get("total_fraudes", 0)

            fraud_html = ""
            if teleports:
                for t in teleports[:4]:
                    niv = t.get("niveau", "warning")
                    cls = f"anomaly-{niv}"
                    badge_cls = f"ab-{niv}"
                    fraud_html += (
                        f'<div class="anomaly-item {cls}">'
                        f'<div class="anomaly-header">'
                        f'<span class="anomaly-id">{t.get("livreur_id","?")}</span>'
                        f'<span class="anomaly-badge {badge_cls}">TELEPORTATION</span>'
                        f'<span style="margin-left:auto;font-size:11px;color:#94A3B8">'
                        f'{t.get("saut_km",0):.1f} km · {t.get("vitesse_implicite_kmh",0):.0f} km/h implicite</span>'
                        f'</div>'
                        f'<div class="anomaly-detail">Saut de {t.get("saut_km",0):.2f} km '
                        f'— vitesse implicite {t.get("vitesse_implicite_kmh",0):.0f} km/h '
                        f'(seuil: {t.get("seuil_kmh",90)} km/h)</div>'
                        f'<div class="anomaly-risque">{t.get("risque","")}</div>'
                        f'</div>'
                    )

            if frozen_list:
                for f in frozen_list[:3]:
                    fraud_html += (
                        f'<div class="anomaly-item anomaly-warning">'
                        f'<div class="anomaly-header">'
                        f'<span class="anomaly-id">{f.get("livreur_id","?")}</span>'
                        f'<span class="anomaly-badge ab-warning">GPS FIGÉ</span>'
                        f'<span style="margin-left:auto;font-size:11px;color:#94A3B8">'
                        f'{f.get("nb_repetitions",0)}× même position</span>'
                        f'</div>'
                        f'<div class="anomaly-detail">Coordonnées identiques '
                        f'({f.get("lat","?")}, {f.get("lon","?")}) '
                        f'sur {f.get("nb_repetitions",0)} mesures consécutives</div>'
                        f'<div class="anomaly-risque">{f.get("risque","")}</div>'
                        f'</div>'
                    )

            if not fraud_html:
                fraud_html = (
                    '<div class="reco-item ok">✅ Aucune fraude GPS détectée '
                    f'— {f_resume.get("livreurs_scannes",0)} livreurs analysés.</div>'
                )

            fraud_color = "#EF4444" if total_frauds > 0 else "#10B981"
            st.markdown(
                f'<div class="bi-card">'
                f'<div class="bi-card-header">'
                f'<span style="font-size:18px">🛡️</span>'
                f'<span class="bi-card-title">Détection Fraude GPS</span>'
                f'<span style="margin-left:auto;font-size:10px;color:{fraud_color};font-weight:700">'
                f'{total_frauds} alerte(s)</span>'
                f'</div>'
                f'<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:14px">'
                f'<div class="bi-kpi"><div class="bi-kpi-lbl">🔴 Téléportations</div>'
                f'<div class="bi-kpi-val" style="color:#EF4444">{f_resume.get("teleportations",0)}</div></div>'
                f'<div class="bi-kpi"><div class="bi-kpi-lbl">⚠️ GPS figés</div>'
                f'<div class="bi-kpi-val" style="color:#F59E0B">{f_resume.get("positions_figees",0)}</div></div>'
                f'<div class="bi-kpi"><div class="bi-kpi-lbl">🔍 Livreurs scannés</div>'
                f'<div class="bi-kpi-val">{f_resume.get("livreurs_scannes",0)}</div></div>'
                f'</div>'
                f'{fraud_html}'
                f'<div style="font-size:10px;color:#94A3B8;margin-top:10px;font-style:italic">'
                f'Méthode : {fraud_data.get("methodologie","")}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    # ── Zone Coverage (auto-refresh) ──────────────────────────────────────
    zones_data = cached_fetch(
        "analytics_zone_coverage",
        "/analytics/zone-coverage",
        ttl_seconds=float(ZONE_COVERAGE_TTL_SECONDS),
    )
    with ph_zones.container():
        if zones_data and "alertes_dispatch" in zones_data:
            res = zones_data.get("resume", {})
            alertes_z = zones_data.get("alertes_dispatch", {})
            sous = alertes_z.get("zones_prioritaires", [])
            sur  = alertes_z.get("zones_saturees", [])

            def zone_item(z):
                return (
                    f'<div class="zone-item">'
                    f'<div class="zi-coords">lat {z.get("lat","?")} · lon {z.get("lon","?")}</div>'
                    f'<div class="zi-stat">{z.get("livreurs_actifs",0)} livreur(s) · '
                    f'{z.get("passages_historiques",0):,} passages hist.</div>'
                    f'<div class="zi-gap">Écart : {z.get("ecart_couverture","?")}</div>'
                    f'</div>'
                )

            sous_html = "".join(zone_item(z) for z in sous) or '<div style="font-size:12px;color:#94A3B8;padding:8px">Aucune</div>'
            sur_html  = "".join(zone_item(z) for z in sur)  or '<div style="font-size:12px;color:#94A3B8;padding:8px">Aucune</div>'

            st.markdown(
                f'<div class="bi-card">'
                f'<div class="bi-card-header">'
                f'<span style="font-size:18px">🗺️</span>'
                f'<span class="bi-card-title">Couverture Territoriale · Dispatch</span>'
                f'<span style="margin-left:auto;font-size:10px;color:#94A3B8">'
                f'{zones_data.get("nb_zones_analysees",0)} zones analysées</span>'
                f'</div>'
                f'<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:16px">'
                f'<div class="bi-kpi"><div class="bi-kpi-lbl">🔴 Sous-couvertes</div>'
                f'<div class="bi-kpi-val" style="color:#EF4444">{res.get("zones_sous_couvertes",0)}</div>'
                f'<div class="bi-kpi-sub">priorité dispatch</div></div>'
                f'<div class="bi-kpi"><div class="bi-kpi-lbl">✅ Équilibrées</div>'
                f'<div class="bi-kpi-val" style="color:#10B981">{res.get("zones_equilibrees",0)}</div>'
                f'<div class="bi-kpi-sub">couverture nominale</div></div>'
                f'<div class="bi-kpi"><div class="bi-kpi-lbl">🟢 Sur-couvertes</div>'
                f'<div class="bi-kpi-val" style="color:#3B82F6">{res.get("zones_sur_couvertes",0)}</div>'
                f'<div class="bi-kpi-sub">redéploiement possible</div></div>'
                f'</div>'
                f'<div class="zone-grid-bi">'
                f'<div>'
                f'<div class="zone-col-title zone-col-under">🔴 Zones à couvrir en priorité</div>'
                f'{sous_html}</div>'
                f'<div>'
                f'<div class="zone-col-title zone-col-over">🟢 Zones sur-dotées (redéployer)</div>'
                f'{sur_html}</div>'
                f'</div></div>',
                unsafe_allow_html=True,
            )

    # ── Copilot driver offers + zones ────────────────────────────────────
    cp_offers = st.session_state.get("last_cp_offers")
    cp_zones = st.session_state.get("last_cp_zones")
    cp_health = st.session_state.get("last_cp_health")

    with ph_copilot.container():
        if cp_offers and cp_offers.get("offers"):
            offers = cp_offers["offers"]
            offers_html = ""
            for o in offers[:8]:
                score = float(o.get("accept_score", 0))
                decision = o.get("decision", "?")
                eph = float(o.get("eur_per_hour_net", 0))
                color = "#10B981" if decision == "accept" else "#EF4444"
                bar_w = max(5, int(score * 100))
                reasons = ", ".join(o.get("explanation", []))
                offers_html += (
                    f'<div style="display:flex;align-items:center;gap:12px;padding:8px 0;'
                    f'border-bottom:1px solid #F1F5F9">'
                    f'<div style="min-width:70px;font-size:11px;font-weight:700;color:{color}">'
                    f'{decision.upper()}</div>'
                    f'<div style="flex:1">'
                    f'<div style="background:#F1F5F9;border-radius:6px;height:8px;overflow:hidden">'
                    f'<div style="width:{bar_w}%;height:100%;background:{color};'
                    f'border-radius:6px;transition:width 0.3s"></div></div></div>'
                    f'<div style="min-width:50px;text-align:right;font-size:13px;font-weight:800">'
                    f'{score:.0%}</div>'
                    f'<div style="min-width:80px;text-align:right;font-size:12px;color:#64748B">'
                    f'{eph:.1f} EUR/h</div>'
                    f'</div>'
                )
            model_used = offers[0].get("model_used", "heuristic") if offers else "?"

            zones_html = ""
            if cp_zones and cp_zones.get("recommendations"):
                for z in cp_zones["recommendations"][:5]:
                    opp = float(z.get("opportunity_score", 0))
                    zones_html += (
                        f'<div style="display:flex;justify-content:space-between;padding:6px 0;'
                        f'border-bottom:1px solid #F1F5F9;font-size:12px">'
                        f'<span style="font-weight:600">{z.get("zone_id","?")}</span>'
                        f'<span>demande {z.get("demand_index",0):.1f} / offre {z.get("supply_index",0):.1f}</span>'
                        f'<span style="font-weight:800;color:#6366F1">{opp:.2f}</span>'
                        f'</div>'
                    )

            connectors_html = ""
            if cp_health:
                for name, key in [("Weather", "weather_context"), ("Citi Bike", "gbfs_context"),
                                  ("OCM EV", "irve_context"), ("TLC Replay", "tlc_replay")]:
                    ctx = cp_health.get(key, {})
                    status = ctx.get("status", "off")
                    dot_color = "#10B981" if status == "ok" else "#F59E0B" if "degraded" in status else "#94A3B8"
                    connectors_html += (
                        f'<span style="display:inline-flex;align-items:center;gap:4px;padding:3px 8px;'
                        f'background:#F8FAFC;border-radius:6px;font-size:10px;font-weight:600">'
                        f'<span style="width:6px;height:6px;border-radius:50%;background:{dot_color}"></span>'
                        f'{name}</span> '
                    )

            no_zones = '<div style="font-size:12px;color:#94A3B8">Aucune donnee</div>'
            zones_display = zones_html if zones_html else no_zones

            st.markdown(
                f'<div class="bi-card">'
                f'<div class="bi-card-header">'
                f'<span style="font-size:18px">🎯</span>'
                f'<span class="bi-card-title">Copilot — {cp_offers.get("driver_id","?")}</span>'
                f'<span style="margin-left:auto;font-size:10px;color:#94A3B8">model: {model_used}</span>'
                f'</div>'
                f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:20px">'
                f'<div>'
                f'<div style="font-size:11px;font-weight:700;color:#0F172A;margin-bottom:8px">'
                f'Dernieres offres ({len(offers)})</div>'
                f'{offers_html}'
                f'</div>'
                f'<div>'
                f'<div style="font-size:11px;font-weight:700;color:#0F172A;margin-bottom:8px">'
                f'Zones recommandees (repositionnement)</div>'
                f'{zones_display}'
                f'</div></div>'
                f'<div style="margin-top:14px;display:flex;gap:6px;flex-wrap:wrap">'
                f'{connectors_html}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    # ── GBFS / IRVE context cards ────────────────────────────────────────
    gbfs_data = cached_fetch("copilot_gbfs_zones", "/copilot/gbfs/zones", {"top_k": 8}, ttl_seconds=30.0)
    with ph_gbfs.container():
        if gbfs_data and gbfs_data.get("zones"):
            zones_g = gbfs_data["zones"][:8]
            rows_html = ""
            for z in zones_g:
                boost = float(z.get("demand_boost", 0))
                boost_color = "#EF4444" if boost > 0.3 else "#F59E0B" if boost > 0.15 else "#10B981"
                rows_html += (
                    f'<div style="display:flex;justify-content:space-between;padding:5px 0;'
                    f'border-bottom:1px solid #F1F5F9;font-size:11px">'
                    f'<span>{z.get("zone_id","?")}</span>'
                    f'<span>{z.get("bikes_available",0)} velos</span>'
                    f'<span style="color:{boost_color};font-weight:700">+{boost:.0%}</span>'
                    f'</div>'
                )
            st.markdown(
                f'<div class="bi-card">'
                f'<div class="bi-card-header">'
                f'<span style="font-size:16px">🚲</span>'
                f'<span class="bi-card-title">Citi Bike — Signal demande</span>'
                f'<span style="margin-left:auto;font-size:10px;color:#94A3B8">'
                f'{gbfs_data.get("count",0)} zones</span>'
                f'</div>'
                f'{rows_html}'
                f'<div style="font-size:9px;color:#94A3B8;margin-top:8px">'
                f'Stations vides = forte demande transport = boost taxi</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    irve_health = cached_fetch("copilot_health", "/copilot/health", ttl_seconds=30.0)
    with ph_irve.container():
        irve_ctx = (irve_health or {}).get("irve_context", {})
        tlc_ctx = (irve_health or {}).get("tlc_replay", {})
        gbfs_ctx = (irve_health or {}).get("gbfs_context", {})

        def status_dot(ctx):
            s = ctx.get("status", "off")
            c = "#10B981" if s == "ok" else "#F59E0B" if "degraded" in s else "#94A3B8"
            return f'<span style="width:8px;height:8px;border-radius:50%;background:{c};display:inline-block"></span>'

        st.markdown(
            f'<div class="bi-card">'
            f'<div class="bi-card-header">'
            f'<span style="font-size:16px">🔌</span>'
            f'<span class="bi-card-title">Connecteurs Open Data</span>'
            f'</div>'
            f'<div style="display:flex;flex-direction:column;gap:10px">'
            f'<div style="display:flex;align-items:center;gap:8px;font-size:12px">'
            f'{status_dot(irve_ctx)} <strong>IRVE</strong> — '
            f'{irve_ctx.get("stations_loaded","0")} EV charging stations loaded</div>'
            f'<div style="display:flex;align-items:center;gap:8px;font-size:12px">'
            f'{status_dot(gbfs_ctx)} <strong>Citi Bike GBFS</strong> — '
            f'{gbfs_ctx.get("stations_polled","0")} stations, '
            f'{gbfs_ctx.get("zones_updated","0")} zones</div>'
            f'<div style="display:flex;align-items:center;gap:8px;font-size:12px">'
            f'{status_dot(tlc_ctx)} <strong>NYC TLC Replay</strong> — '
            f'month {tlc_ctx.get("month","?")} · {tlc_ctx.get("emitted","0")} events · {tlc_ctx.get("progress_pct","0")}%</div>'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    # ── Footer timestamp ──────────────────────────────────────────────────
    with ph_ts.container():
        st.markdown(
            f'<div class="fs-footer">'
            f'<span class="live-dot"></span>'
            f'Mis à jour le {datetime.now().strftime("%d/%m/%Y à %H:%M:%S")}'
            f' · Refresh toutes les {REFRESH_SECONDS} s'
            f'</div>',
            unsafe_allow_html=True,
        )

    time.sleep(REFRESH_SECONDS)
