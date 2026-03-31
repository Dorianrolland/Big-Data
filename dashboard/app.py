"""
FleetStream — Analytics Dashboard (Streamlit)
===============================================
Design moderne et épuré : cartes blanches, typographie Inter,
palette Indigo/Emeraude/Ambre, sans menu ni footer Streamlit.

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
    "delivering": [251, 146,  60, 220],   # Ambre
    "available":  [ 52, 211, 153, 220],   # Emeraude
    "idle":       [148, 163, 184, 160],   # Slate
}

# ════════════════════════════════════════════════════════════════════════════
#  PAGE CONFIG
# ════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="FleetStream Analytics",
    page_icon="🛵",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ════════════════════════════════════════════════════════════════════════════
#  CSS — Design system complet
# ════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
/* Google Font */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

/* ── Reset Streamlit ─────────────────────────────────────────────────────── */
html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; }

/* Cacher menu hamburger, footer "Made with Streamlit", header deploy */
#MainMenu, footer, header { visibility: hidden !important; height: 0 !important; }
[data-testid="stToolbar"] { display: none !important; }
[data-testid="stDecoration"] { display: none !important; }

/* ── Fond de page — gris très clair ─────────────────────────────────────── */
.stApp { background: #f1f5f9 !important; }
.block-container { padding: 1.5rem 2rem 1rem !important; max-width: 1400px !important; }

/* ── Cards blanches ──────────────────────────────────────────────────────── */
[data-testid="stVerticalBlock"] > [data-testid="stVerticalBlock"],
div[data-testid="stExpander"] {
    background: #ffffff;
    border-radius: 14px;
    border: 1px solid #e2e8f0;
    box-shadow: 0 1px 3px rgba(0,0,0,.06), 0 4px 16px rgba(0,0,0,.04);
    padding: 1.25rem 1.5rem !important;
    margin-bottom: 1rem !important;
}

/* ── Metrics ─────────────────────────────────────────────────────────────── */
[data-testid="metric-container"] {
    background: #f8fafc !important;
    border: 1px solid #e2e8f0 !important;
    border-radius: 12px !important;
    padding: 1rem 1.2rem !important;
    box-shadow: none !important;
}
[data-testid="metric-container"] label {
    font-size: 11px !important; font-weight: 600 !important;
    color: #94a3b8 !important; letter-spacing: .5px !important;
    text-transform: uppercase !important;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size: 1.75rem !important; font-weight: 700 !important;
    color: #1e293b !important;
}

/* ── Titres ──────────────────────────────────────────────────────────────── */
h1 { font-size: 1.5rem !important; font-weight: 700 !important; color: #0f172a !important; margin-bottom: .25rem !important; }
h2 { font-size: 1rem   !important; font-weight: 600 !important; color: #1e293b !important; margin-bottom: .75rem !important; }
h3 { font-size: .875rem !important; font-weight: 600 !important; color: #475569 !important; }
p, li { color: #475569 !important; }

/* ── Boutons ─────────────────────────────────────────────────────────────── */
[data-testid="stButton"] button {
    background: #6366f1 !important;
    color: #ffffff !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    padding: .5rem 1.25rem !important;
    transition: background .15s !important;
}
[data-testid="stButton"] button:hover { background: #4f46e5 !important; }

/* ── Inputs ──────────────────────────────────────────────────────────────── */
[data-testid="stTextInput"] input, [data-testid="stSelectbox"] > div > div {
    border-radius: 8px !important;
    border: 1px solid #cbd5e1 !important;
    font-family: 'Inter', sans-serif !important;
}
[data-testid="stTextInput"] input:focus { border-color: #6366f1 !important; box-shadow: 0 0 0 3px rgba(99,102,241,.15) !important; }

/* ── Success / Warning / Info ────────────────────────────────────────────── */
[data-testid="stSuccess"] { background: #f0fdf4 !important; border: 1px solid #86efac !important; border-radius: 10px !important; color: #166534 !important; }
[data-testid="stWarning"] { background: #fffbeb !important; border: 1px solid #fcd34d !important; border-radius: 10px !important; color: #92400e !important; }
[data-testid="stError"]   { background: #fef2f2 !important; border: 1px solid #fca5a5 !important; border-radius: 10px !important; color: #991b1b !important; }
[data-testid="stInfo"]    { background: #eff6ff !important; border: 1px solid #93c5fd !important; border-radius: 10px !important; color: #1e40af !important; }

/* ── Divider ─────────────────────────────────────────────────────────────── */
hr { border-color: #e2e8f0 !important; margin: 1rem 0 !important; }

/* ── Caption ─────────────────────────────────────────────────────────────── */
[data-testid="stCaptionContainer"] small { color: #94a3b8 !important; font-size: 11px !important; }

/* Pydeck chart */
iframe { border-radius: 12px; border: 1px solid #e2e8f0; }
</style>
""", unsafe_allow_html=True)


def fetch(path: str, params: dict | None = None) -> dict:
    try:
        return requests.get(f"{API_URL}{path}", params=params, timeout=5).json()
    except Exception:
        return {}


# ════════════════════════════════════════════════════════════════════════════
#  HEADER
# ════════════════════════════════════════════════════════════════════════════
col_title, col_link = st.columns([5, 1])
with col_title:
    st.markdown("# 🛵 FleetStream — Analytics")
    st.markdown(
        "<p style='color:#64748b;font-size:.875rem;margin-top:-.25rem'>"
        "Architecture Lambda · Redis Hot Path · DuckDB Cold Path</p>",
        unsafe_allow_html=True,
    )
with col_link:
    st.markdown(
        "<div style='text-align:right;padding-top:1rem'>"
        "<a href='http://localhost:8001/map' target='_blank' "
        "style='display:inline-block;background:#6366f1;color:#fff;"
        "padding:8px 16px;border-radius:8px;font-size:13px;font-weight:600;"
        "text-decoration:none'>🗺️ Carte live</a></div>",
        unsafe_allow_html=True,
    )

st.divider()

# ════════════════════════════════════════════════════════════════════════════
#  SECTION HISTORIQUE — widgets déclarés UNE SEULE FOIS (hors boucle)
# ════════════════════════════════════════════════════════════════════════════
st.markdown("## 📍 Trajectoire historique")
st.markdown(
    "<p style='color:#64748b;font-size:.85rem;margin-top:-.5rem;margin-bottom:1rem'>"
    "Analyse DuckDB sur le Data Lake Parquet — distance Haversine, vitesses, statut dominant</p>",
    unsafe_allow_html=True,
)

ci, ch, cb = st.columns([3, 2, 1])
with ci:
    livreur_id = st.text_input(
        "Identifiant livreur",
        value="L042",
        placeholder="ex: L007",
        key="hist_livreur_id",
        label_visibility="collapsed",
    )
with ch:
    heures = st.selectbox(
        "Fenêtre",
        options=[1, 2, 4, 8, 24],
        format_func=lambda h: f"Dernière{'s' if h>1 else ''} {h}h",
        key="hist_heures",
        label_visibility="collapsed",
    )
with cb:
    analyse = st.button("🔍 Analyser", key="hist_btn", use_container_width=True)

ph_hist = st.empty()

if "last_hist" not in st.session_state:
    st.session_state.last_hist = {}
if "last_lv_id"  not in st.session_state:
    st.session_state.last_lv_id = livreur_id

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

        # KPIs trajectoire — couleurs Indigo/Emeraude/Ambre
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("📌 Points GPS",         f"{r.get('nb_points', 0):,}")
        k2.metric("📏 Distance",           f"{r.get('distance_totale_km', 0):.2f} km")
        k3.metric("⚡ Vitesse moyenne",    f"{r.get('vitesse_moy_kmh', 0):.1f} km/h")
        k4.metric("🏁 Statut dominant",    r.get("statut_dominant", "—").capitalize())

        if traj:
            df = pd.DataFrame([
                {
                    "lat":   p["lat"], "lon": p["lon"],
                    "speed": p.get("speed_kmh") or 0.0,
                    "status": p.get("status", "idle"),
                    "color": STATUS_COLOR_RGB.get(p.get("status","idle"), [148,163,184,160]),
                    "idx": i,
                }
                for i, p in enumerate(traj)
            ])

            n = len(df)
            # Ligne de trajectoire (PathLayer) — couleur dégradée simulée
            # On découpe en segments courts et on varie l'opacité
            segments = []
            step = max(1, n // 60)
            for i in range(0, n - step, step):
                alpha = int(80 + 160 * (i / n))   # de 80 (début) à 240 (fin)
                segments.append({
                    "path": [
                        [df.iloc[i]["lon"], df.iloc[i]["lat"]],
                        [df.iloc[min(i+step, n-1)]["lon"], df.iloc[min(i+step, n-1)]["lat"]],
                    ],
                    "color": [99, 102, 241, alpha],   # Indigo avec alpha progressif
                })

            path_layer = pdk.Layer(
                "PathLayer",
                data=segments,
                get_path="path",
                get_color="color",
                get_width=3,
                width_min_pixels=2,
                width_max_pixels=6,
                rounded=True,
            )

            # Points GPS colorés par statut
            scatter = pdk.Layer(
                "ScatterplotLayer",
                data=df,
                get_position=["lon", "lat"],
                get_fill_color="color",
                get_radius=40,
                radius_min_pixels=3,
                radius_max_pixels=10,
                pickable=True,
                auto_highlight=True,
                highlight_color=[255, 255, 100, 255],
            )

            # Départ / Arrivée
            endpoints = pd.DataFrame([
                {"lon": df.iloc[0]["lon"],  "lat": df.iloc[0]["lat"],
                 "color": [52, 211, 153, 255], "radius": 120},   # Emeraude départ
                {"lon": df.iloc[-1]["lon"], "lat": df.iloc[-1]["lat"],
                 "color": [239, 68, 68, 255],  "radius": 120},   # Rouge arrivée
            ])
            endpoints_layer = pdk.Layer(
                "ScatterplotLayer",
                data=endpoints,
                get_position=["lon", "lat"],
                get_fill_color="color",
                get_radius="radius",
                radius_min_pixels=8,
                radius_max_pixels=20,
                stroked=True,
                get_line_color=[255, 255, 255, 200],
                line_width_min_pixels=2,
                pickable=False,
            )

            deck = pdk.Deck(
                layers=[path_layer, scatter, endpoints_layer],
                initial_view_state=pdk.ViewState(
                    latitude=df["lat"].mean(),
                    longitude=df["lon"].mean(),
                    zoom=13, pitch=35, bearing=0,
                ),
                tooltip={
                    "html": "<b style='color:#6366f1'>{status}</b><br/>⚡ {speed:.1f} km/h",
                    "style": {
                        "background": "#ffffff",
                        "color": "#1e293b",
                        "fontSize": "12px",
                        "borderRadius": "8px",
                        "border": "1px solid #e2e8f0",
                        "padding": "8px 12px",
                        "fontFamily": "Inter, sans-serif",
                        "boxShadow": "0 4px 16px rgba(0,0,0,.12)",
                    },
                },
                map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
            )

            st.pydeck_chart(deck, use_container_width=True)

            # Légende
            l1, l2, l3, l4 = st.columns(4)
            l1.markdown("🟣 **Trajectoire** (Indigo → dense)")
            l2.markdown("🟠 **En livraison**")
            l3.markdown("🟢 **Disponible** · départ")
            l4.markdown("🔴 **Arrivée**")

    elif "detail" in hist:
        st.info(f"ℹ️ {hist['detail']}")
    else:
        st.markdown(
            "<div style='text-align:center;padding:2rem;color:#94a3b8;"
            "font-size:.875rem'>Saisir un identifiant et cliquer sur Analyser</div>",
            unsafe_allow_html=True,
        )

st.divider()

# ════════════════════════════════════════════════════════════════════════════
#  PLACEHOLDERS auto-refresh
# ════════════════════════════════════════════════════════════════════════════
ph_kpis  = st.empty()
ph_sla   = st.empty()
ph_cold  = st.empty()
ph_ts    = st.empty()

# ════════════════════════════════════════════════════════════════════════════
#  BOUCLE DE REFRESH
# ════════════════════════════════════════════════════════════════════════════
while True:
    stats     = fetch("/stats")
    hp        = stats.get("hot_path", {})
    cp        = stats.get("cold_path", {})
    st_counts = hp.get("statuts", {})

    # ── KPIs temps réel ──────────────────────────────────────────────────────
    with ph_kpis.container():
        st.markdown("## ⚡ Hot Path — Métriques temps réel")
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("🛵 Livreurs actifs",  hp.get("livreurs_actifs", "—"))
        c2.metric("📨 Messages GPS",     f"{hp.get('messages_traites', 0):,}")
        c3.metric("🟠 En livraison",     st_counts.get("delivering", 0))
        c4.metric("🟢 Disponibles",      st_counts.get("available", 0))
        c5.metric("⚫ Inactifs",         st_counts.get("idle", 0))

    # ── SLA Redis ────────────────────────────────────────────────────────────
    perf  = fetch("/health/performance", {"samples": 100})
    bench = perf.get("geosearch_benchmark", {})
    rinfo = perf.get("redis_info", {})

    with ph_sla.container():
        st.markdown("## 📊 SLA Redis GEOSEARCH")
        if bench:
            p99 = bench.get("p99_ms", 999)
            if p99 < 10:
                st.success(f"✅ **SLA respecté** — p99 = {p99} ms  _(cible < 10 ms)_")
            else:
                st.error(f"⚠️ **SLA dépassé** — p99 = {p99} ms")

            b1, b2, b3, b4 = st.columns(4)
            b1.metric("Latence P50", f"{bench.get('p50_ms','—')} ms")
            b2.metric("Latence P95", f"{bench.get('p95_ms','—')} ms")
            b3.metric("Latence P99", f"{bench.get('p99_ms','—')} ms")
            b4.metric("Latence Max", f"{bench.get('max_ms','—')} ms")

            if rinfo:
                with st.expander("🔧 Redis INFO — détails serveur"):
                    r1, r2, r3 = st.columns(3)
                    r1.metric("Version Redis",   rinfo.get("version", "—"))
                    r2.metric("Clients actifs",  rinfo.get("connected_clients", "—"))
                    r3.metric("Mémoire utilisée",rinfo.get("used_memory_human", "—"))
                    r1.metric("Ops / seconde",   rinfo.get("ops_per_sec", "—"))
                    r2.metric("Keyspace hits",   f"{rinfo.get('keyspace_hits',0):,}")
                    r3.metric("Commandes total", f"{rinfo.get('total_commands',0):,}")

    # ── Cold Path ────────────────────────────────────────────────────────────
    with ph_cold.container():
        st.markdown("## 🗄️ Cold Path — Data Lake Parquet")
        cc1, cc2, cc3, cc4 = st.columns(4)
        cc1.metric("Fichiers Parquet",  cp.get("fichiers_parquet", 0))
        cc2.metric("Taille totale",     f"{cp.get('taille_totale_mb', 0):.1f} MB")
        cc3.metric("Compression",       "Snappy")
        cc4.metric("Partitionnement",   "Hive y/m/d/h")

    # ── Timestamp ────────────────────────────────────────────────────────────
    with ph_ts.container():
        st.caption(
            f"Actualisé à {datetime.now().strftime('%H:%M:%S')} · "
            f"Refresh automatique toutes les {REFRESH_SECONDS} s"
        )

    time.sleep(REFRESH_SECONDS)
