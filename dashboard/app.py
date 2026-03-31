"""
FleetStream — Analytics Dashboard (Streamlit)
Premium SaaS Design — Inter, cards blanches, palette Indigo/Emeraude/Ambre.

Anti-DuplicateWidgetID : widgets déclarés UNE SEULE FOIS hors boucle,
résultats mémorisés dans st.session_state.
"""
import os
import time
from datetime import datetime

import pandas as pd
import pydeck as pdk
import requests
import streamlit as st

API_URL         = os.getenv("API_URL", "http://api:8000")
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "4"))

STATUS_COLOR_RGB = {
    "delivering": [251, 146,  60, 230],
    "available":  [ 52, 211, 153, 230],
    "idle":       [148, 163, 184, 160],
}

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="FleetStream Analytics",
    page_icon="🛵",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ════════════════════════════════════════════════════════════════════════════
#  DESIGN SYSTEM — CSS global
# ════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

/* ── Reset global ─────────────────────────────────────────────────────────── */
html, body, [class*="css"], .stApp {
    font-family: 'Inter', 'Segoe UI', system-ui, sans-serif !important;
    background: #F8FAFC !important;
}

/* ── Cacher éléments Streamlit natifs ─────────────────────────────────────── */
#MainMenu            { visibility: hidden !important; }
footer               { visibility: hidden !important; }
header               { visibility: hidden !important; }
[data-testid="stToolbar"]      { display: none !important; }
[data-testid="stDecoration"]   { display: none !important; }
[data-testid="stStatusWidget"] { display: none !important; }

/* ── Layout principal ────────────────────────────────────────────────────── */
.block-container {
    padding: 1.75rem 2.5rem 2rem !important;
    max-width: 1440px !important;
}

/* ── Composant card ──────────────────────────────────────────────────────── */
.card {
    background: #FFFFFF;
    border-radius: 16px;
    border: 1px solid #E2E8F0;
    box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.07), 0 2px 4px -2px rgb(0 0 0 / 0.04);
    padding: 1.5rem 1.75rem;
    margin-bottom: 1.25rem;
}
.card-title {
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 1px;
    text-transform: uppercase;
    color: #94A3B8;
    margin-bottom: 1.1rem;
}
.card-heading {
    font-size: 18px;
    font-weight: 700;
    color: #0F172A;
    margin-bottom: .25rem;
    display: flex;
    align-items: center;
    gap: 8px;
}
.card-sub {
    font-size: 13px;
    color: #64748B;
    margin-bottom: 1.25rem;
    line-height: 1.5;
}

/* ── Métriques KPI ───────────────────────────────────────────────────────── */
[data-testid="metric-container"] {
    background: #FAFAFA !important;
    border: 1.5px solid #E2E8F0 !important;
    border-radius: 14px !important;
    padding: 1rem 1.25rem .875rem !important;
    box-shadow: none !important;
    transition: box-shadow .15s ease !important;
}
[data-testid="metric-container"]:hover {
    box-shadow: 0 4px 12px rgb(0 0 0 / .08) !important;
}
[data-testid="metric-container"] label {
    font-size: 11px !important;
    font-weight: 600 !important;
    color: #94A3B8 !important;
    letter-spacing: .6px !important;
    text-transform: uppercase !important;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size: 2rem !important;
    font-weight: 800 !important;
    color: #0F172A !important;
    line-height: 1.15 !important;
    letter-spacing: -.5px !important;
}
[data-testid="stMetricDelta"] { font-size: 12px !important; }

/* ── Bouton principal ────────────────────────────────────────────────────── */
[data-testid="stButton"] > button {
    background: #6366F1 !important;
    color: #FFFFFF !important;
    border: none !important;
    border-radius: 10px !important;
    font-weight: 600 !important;
    font-size: 13px !important;
    padding: .55rem 1.4rem !important;
    letter-spacing: .1px !important;
    box-shadow: 0 1px 3px rgb(99 102 241 / .35) !important;
    transition: background .15s, box-shadow .15s !important;
}
[data-testid="stButton"] > button:hover {
    background: #4F46E5 !important;
    box-shadow: 0 4px 12px rgb(99 102 241 / .4) !important;
}

/* ── Inputs ──────────────────────────────────────────────────────────────── */
[data-testid="stTextInput"] input {
    border: 1.5px solid #E2E8F0 !important;
    border-radius: 10px !important;
    font-size: 14px !important;
    padding: .5rem .875rem !important;
    background: #FAFAFA !important;
    transition: border-color .15s, box-shadow .15s !important;
}
[data-testid="stTextInput"] input:focus {
    border-color: #6366F1 !important;
    box-shadow: 0 0 0 3px rgb(99 102 241 / .15) !important;
    background: #FFFFFF !important;
}
/* Selectbox */
[data-testid="stSelectbox"] > div > div {
    border: 1.5px solid #E2E8F0 !important;
    border-radius: 10px !important;
    font-size: 14px !important;
    background: #FAFAFA !important;
}

/* ── Alertes ─────────────────────────────────────────────────────────────── */
[data-testid="stSuccess"] {
    background: #F0FDF4 !important;
    border: 1.5px solid #86EFAC !important;
    border-radius: 12px !important;
    color: #14532D !important;
    font-weight: 500 !important;
}
[data-testid="stError"] {
    background: #FFF7ED !important;
    border: 1.5px solid #FCD34D !important;
    border-radius: 12px !important;
    color: #78350F !important;
    font-weight: 500 !important;
}
[data-testid="stInfo"] {
    background: #EFF6FF !important;
    border: 1.5px solid #BFDBFE !important;
    border-radius: 12px !important;
    color: #1E3A8A !important;
}

/* ── Divider ─────────────────────────────────────────────────────────────── */
[data-testid="stDivider"] hr {
    border-color: #E2E8F0 !important;
}

/* ── Expander ────────────────────────────────────────────────────────────── */
[data-testid="stExpander"] {
    border: 1.5px solid #E2E8F0 !important;
    border-radius: 12px !important;
    background: #FAFAFA !important;
    box-shadow: none !important;
}

/* ── Caption ─────────────────────────────────────────────────────────────── */
[data-testid="stCaptionContainer"] small {
    color: #94A3B8 !important;
    font-size: 11.5px !important;
}

/* ── pydeck iframe ───────────────────────────────────────────────────────── */
iframe { border-radius: 14px !important; border: 1.5px solid #E2E8F0 !important; }

/* ── SLA Progress Bar (HTML natif dans st.markdown) ─────────────────────── */
.sla-bar-track {
    width: 100%; height: 5px; background: #F1F5F9;
    border-radius: 999px; margin: 4px 0 12px;
    overflow: hidden;
}
.sla-bar-fill {
    height: 100%; border-radius: 999px;
    transition: width .5s cubic-bezier(.4,0,.2,1);
}
.sla-row-label {
    display: flex; justify-content: space-between;
    font-size: 12px; color: #64748B; font-weight: 500;
    font-family: 'Inter', sans-serif;
}
.sla-val { font-weight: 700; font-family: 'JetBrains Mono', monospace; font-size: 12px; }
.sla-ok   { color: #059669; }
.sla-warn { color: #D97706; }
.sla-bad  { color: #DC2626; }

/* ── Cold Path table ─────────────────────────────────────────────────────── */
.cold-row {
    display: flex; justify-content: space-between; align-items: center;
    padding: 7px 0; border-bottom: 1px solid #F1F5F9;
    font-size: 13px;
}
.cold-row:last-child { border-bottom: none; }
.cold-key { color: #64748B; }
.cold-val { font-weight: 700; color: #6366F1; font-size: 13px; }

/* ── Légende ─────────────────────────────────────────────────────────────── */
.legend-dot {
    display: inline-block; width: 10px; height: 10px;
    border-radius: 50%; margin-right: 5px; vertical-align: middle;
}
.legend-item { font-size: 12px; color: #475569; margin-right: 16px; }
</style>
""", unsafe_allow_html=True)


def fetch(path: str, params: dict | None = None) -> dict:
    try:
        return requests.get(f"{API_URL}{path}", params=params, timeout=5).json()
    except Exception:
        return {}


def sla_bar(label: str, ms: float | None, target: float = 10.0) -> str:
    if ms is None:
        return ""
    pct = min(100, (ms / target) * 100)
    if ms < target * 0.7:
        color, cls = "#10B981", "sla-ok"
    elif ms < target:
        color, cls = "#F59E0B", "sla-warn"
    else:
        color, cls = "#EF4444", "sla-bad"
    return f"""
    <div class="sla-row-label">
        <span>{label}</span>
        <span class="sla-val {cls}">{ms:.2f} ms</span>
    </div>
    <div class="sla-bar-track">
        <div class="sla-bar-fill" style="width:{pct:.1f}%;background:{color}"></div>
    </div>"""


# ════════════════════════════════════════════════════════════════════════════
#  HEADER
# ════════════════════════════════════════════════════════════════════════════
col_h, col_btn = st.columns([6, 1])
with col_h:
    st.markdown("""
    <div style="padding-bottom:.75rem">
        <h1 style="font-size:1.6rem;font-weight:800;color:#0F172A;
                   letter-spacing:-.5px;margin:0;line-height:1.2">
            🛵 FleetStream <span style="color:#6366F1">Analytics</span>
        </h1>
        <p style="font-size:13px;color:#64748B;margin:.35rem 0 0">
            Architecture Lambda · Redis Hot Path &lt;10ms · DuckDB Cold Path · Apache Parquet
        </p>
    </div>
    """, unsafe_allow_html=True)
with col_btn:
    st.markdown(
        "<div style='padding-top:1.1rem;text-align:right'>"
        "<a href='http://localhost:8001/map' target='_blank' "
        "style='background:#6366F1;color:#fff;padding:9px 18px;"
        "border-radius:10px;font-size:13px;font-weight:600;"
        "text-decoration:none;box-shadow:0 2px 8px rgb(99 102 241/.3)'>"
        "🗺️ Carte live</a></div>",
        unsafe_allow_html=True,
    )

st.divider()

# ════════════════════════════════════════════════════════════════════════════
#  SECTION HISTORIQUE — widgets déclarés UNE SEULE FOIS (hors boucle)
# ════════════════════════════════════════════════════════════════════════════
st.markdown("""
<div class="card">
    <div class="card-title">Cold Path · DuckDB → Parquet</div>
    <div class="card-heading">📍 Trajectoire historique</div>
    <div class="card-sub">
        Analyse complète depuis le Data Lake — distance Haversine, vitesses, statut dominant.
    </div>
</div>
""", unsafe_allow_html=True)

ci, ch, cb = st.columns([3, 2, 1])
with ci:
    livreur_id = st.text_input(
        "ID", value="L042", placeholder="ex: L007",
        key="hist_livreur_id", label_visibility="collapsed",
    )
with ch:
    heures = st.selectbox(
        "Fenêtre", [1, 2, 4, 8, 24],
        format_func=lambda h: f"Dernière{'s' if h > 1 else ''} {h}h",
        key="hist_heures", label_visibility="collapsed",
    )
with cb:
    analyse = st.button("🔍 Analyser", key="hist_btn", use_container_width=True)

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

        # ── KPIs trajectoire ──────────────────────────────────────────────────
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("📌 Points GPS",       f"{r.get('nb_points', 0):,}")
        k2.metric("📏 Distance",         f"{r.get('distance_totale_km', 0):.2f} km")
        k3.metric("⚡ Vitesse moyenne",  f"{r.get('vitesse_moy_kmh', 0):.1f} km/h")
        k4.metric("🏁 Statut dominant",  r.get("statut_dominant", "—").capitalize())

        if traj:
            df = pd.DataFrame([
                {
                    "lat": p["lat"], "lon": p["lon"],
                    "speed": p.get("speed_kmh") or 0.0,
                    "status": p.get("status", "idle"),
                    "color": STATUS_COLOR_RGB.get(p.get("status", "idle"), [148, 163, 184, 160]),
                }
                for p in traj
            ])
            n = len(df)

            # PathLayer — Indigo vibrant avec alpha croissant (début pâle → fin dense)
            step = max(1, n // 80)
            segments = []
            for i in range(0, n - step, step):
                alpha = int(60 + 180 * (i / max(n - step, 1)))
                segments.append({
                    "path": [
                        [df.iloc[i]["lon"],           df.iloc[i]["lat"]],
                        [df.iloc[min(i+step,n-1)]["lon"], df.iloc[min(i+step,n-1)]["lat"]],
                    ],
                    "color": [99, 102, 241, alpha],   # #6366F1 Indigo
                })

            path_layer = pdk.Layer(
                "PathLayer",
                data=segments,
                get_path="path",
                get_color="color",
                get_width=4,
                width_min_pixels=2,
                width_max_pixels=8,
                rounded=True,
                joint_rounded=True,
            )

            scatter = pdk.Layer(
                "ScatterplotLayer",
                data=df,
                get_position=["lon", "lat"],
                get_fill_color="color",
                get_radius=45,
                radius_min_pixels=3,
                radius_max_pixels=10,
                pickable=True,
                auto_highlight=True,
                highlight_color=[99, 102, 241, 255],
            )

            endpoints = pd.DataFrame([
                {"lon": df.iloc[0]["lon"],   "lat": df.iloc[0]["lat"],
                 "color": [16, 185, 129, 255], "r": 100},
                {"lon": df.iloc[-1]["lon"],  "lat": df.iloc[-1]["lat"],
                 "color": [239, 68, 68, 255],  "r": 100},
            ])
            ep_layer = pdk.Layer(
                "ScatterplotLayer",
                data=endpoints,
                get_position=["lon", "lat"],
                get_fill_color="color",
                get_radius="r",
                radius_min_pixels=8,
                radius_max_pixels=18,
                stroked=True,
                get_line_color=[255, 255, 255, 220],
                line_width_min_pixels=2,
                pickable=False,
            )

            deck = pdk.Deck(
                layers=[path_layer, scatter, ep_layer],
                initial_view_state=pdk.ViewState(
                    latitude=df["lat"].mean(),
                    longitude=df["lon"].mean(),
                    zoom=13, pitch=30,
                ),
                tooltip={
                    "html": (
                        "<div style='font-family:Inter,sans-serif;padding:4px 2px'>"
                        "<b style='color:#6366F1'>{status}</b><br/>"
                        "⚡ {speed:.1f} km/h</div>"
                    ),
                    "style": {
                        "background": "#FFFFFF",
                        "color": "#0F172A",
                        "fontSize": "12px",
                        "borderRadius": "8px",
                        "border": "1.5px solid #E2E8F0",
                        "padding": "8px 12px",
                        "boxShadow": "0 4px 12px rgb(0 0 0/.1)",
                    },
                },
                # Fond clair (contraste avec la carte live dark)
                map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
            )

            st.pydeck_chart(deck, use_container_width=True)

            # Légende compacte
            st.markdown(
                "<div style='margin-top:.5rem'>"
                "<span class='legend-item'>"
                "<span class='legend-dot' style='background:#6366F1'></span>Trajectoire (Indigo)</span>"
                "<span class='legend-item'>"
                "<span class='legend-dot' style='background:#FB923C'></span>En livraison</span>"
                "<span class='legend-item'>"
                "<span class='legend-dot' style='background:#34D399'></span>Disponible · Départ</span>"
                "<span class='legend-item'>"
                "<span class='legend-dot' style='background:#EF4444'></span>Arrivée</span>"
                "</div>",
                unsafe_allow_html=True,
            )

    elif "detail" in hist:
        st.info(f"ℹ️ {hist['detail']}")
    else:
        st.markdown(
            "<div style='text-align:center;padding:2.5rem 1rem;"
            "color:#94A3B8;font-size:13px;background:#FAFAFA;"
            "border-radius:12px;border:1.5px dashed #E2E8F0'>"
            "Saisir un identifiant livreur et cliquer sur <strong>Analyser</strong>"
            "</div>",
            unsafe_allow_html=True,
        )

st.divider()

# ════════════════════════════════════════════════════════════════════════════
#  PLACEHOLDERS auto-refresh (créés une fois, mis à jour sans recréer)
# ════════════════════════════════════════════════════════════════════════════
ph_kpis = st.empty()
ph_sla  = st.empty()
ph_cold = st.empty()
ph_ts   = st.empty()

# ════════════════════════════════════════════════════════════════════════════
#  BOUCLE — met à jour les placeholders UNIQUEMENT
# ════════════════════════════════════════════════════════════════════════════
while True:
    stats     = fetch("/stats")
    hp        = stats.get("hot_path", {})
    cp        = stats.get("cold_path", {})
    cts       = hp.get("statuts", {})

    # ── KPIs ─────────────────────────────────────────────────────────────────
    with ph_kpis.container():
        st.markdown("""
        <div class="card">
            <div class="card-title">Hot Path · Redis GEOSEARCH</div>
            <div class="card-heading">⚡ Métriques temps réel</div>
        </div>
        """, unsafe_allow_html=True)
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("🛵 Livreurs actifs",  hp.get("livreurs_actifs", "—"))
        c2.metric("📨 Messages GPS",     f"{hp.get('messages_traites', 0):,}")
        c3.metric("🟠 En livraison",     cts.get("delivering", 0))
        c4.metric("🟢 Disponibles",      cts.get("available",  0))
        c5.metric("⚫ Inactifs",         cts.get("idle",        0))

    # ── SLA ──────────────────────────────────────────────────────────────────
    perf  = fetch("/health/performance", {"samples": 100})
    bench = perf.get("geosearch_benchmark", {})
    rinfo = perf.get("redis_info", {})

    with ph_sla.container():
        p99 = bench.get("p99_ms")
        sla_ok = p99 is not None and p99 < 10
        badge_html = (
            "<span style='background:#D1FAE5;color:#065F46;"
            "padding:3px 10px;border-radius:999px;font-size:11px;"
            "font-weight:700;margin-left:10px'>✅ SLA OK</span>"
            if sla_ok else
            "<span style='background:#FEF3C7;color:#92400E;"
            "padding:3px 10px;border-radius:999px;font-size:11px;"
            "font-weight:700;margin-left:10px'>⚠️ SLA KO</span>"
        )
        st.markdown(f"""
        <div class="card">
            <div class="card-title">Performance · Redis</div>
            <div class="card-heading">📊 SLA GEOSEARCH{badge_html}</div>
            <div class="card-sub">Cible : p99 &lt; 10 ms — Redis Stack 7.2</div>
        </div>
        """, unsafe_allow_html=True)

        if bench:
            bars_html = (
                sla_bar("P50", bench.get("p50_ms")) +
                sla_bar("P95", bench.get("p95_ms")) +
                sla_bar("P99", bench.get("p99_ms")) +
                sla_bar("Max", bench.get("max_ms"), target=15)
            )
            left_col, right_col = st.columns([2, 1])
            with left_col:
                st.markdown(bars_html, unsafe_allow_html=True)
            with right_col:
                if rinfo:
                    with st.expander("🔧 Redis INFO"):
                        r1, r2 = st.columns(2)
                        r1.metric("Version",   rinfo.get("version", "—"))
                        r2.metric("Mémoire",   rinfo.get("used_memory_human", "—"))
                        r1.metric("Ops/sec",   rinfo.get("ops_per_sec", "—"))
                        r2.metric("Clients",   rinfo.get("connected_clients", "—"))

    # ── Cold Path ─────────────────────────────────────────────────────────────
    with ph_cold.container():
        st.markdown("""
        <div class="card">
            <div class="card-title">Batch Layer · Parquet</div>
            <div class="card-heading">🗄️ Data Lake</div>
        </div>
        """, unsafe_allow_html=True)
        cc1, cc2, cc3, cc4 = st.columns(4)
        cc1.metric("Fichiers Parquet",  cp.get("fichiers_parquet", 0))
        cc2.metric("Taille totale",     f"{cp.get('taille_totale_mb', 0):.1f} MB")
        cc3.metric("Compression",       "Snappy")
        cc4.metric("Partitionnement",   "Hive y/m/d/h")

    # ── Timestamp ────────────────────────────────────────────────────────────
    with ph_ts.container():
        st.caption(
            f"Dernière actualisation : {datetime.now().strftime('%H:%M:%S')} "
            f"· Refresh toutes les {REFRESH_SECONDS} s"
        )

    time.sleep(REFRESH_SECONDS)
