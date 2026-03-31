"""
FleetStream — Live Dashboard (Streamlit)
=========================================
Tableau de bord temps réel de la flotte de livreurs.

Hot Path  : Redis GEOSEARCH — positions live (rafraîchissement 2s)
Cold Path : Statistiques du Data Lake Parquet

Usage (depuis docker-compose) : http://localhost:8501
Usage (local)                  : streamlit run dashboard/app.py
"""
import os
import time
from datetime import datetime

import pandas as pd
import pydeck as pdk
import requests
import streamlit as st

# ── Config ──────────────────────────────────────────────────────────────────────
API_URL         = os.getenv("API_URL", "http://api:8000")
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "2"))
PARIS_LAT       = 48.8566
PARIS_LON       = 2.3522

STATUS_COLORS = {
    "delivering": [255, 140, 0,  230],   # Orange — en course
    "available":  [0,   210, 100, 230],  # Vert   — dispo
    "idle":       [160, 160, 160, 180],  # Gris   — inactif
}

# ── Page setup ──────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="FleetStream — Live",
    page_icon="🛵",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    /* Réduit le padding global */
    .block-container { padding-top: 0.8rem; padding-bottom: 0rem; }
    /* Metrics cards */
    [data-testid="metric-container"] {
        background: #1E1E2E;
        border: 1px solid #313244;
        border-radius: 10px;
        padding: 12px 16px;
    }
    /* Titre */
    h1 { margin-bottom: 0 !important; }
</style>
""", unsafe_allow_html=True)


# ── Data fetching (avec cache TTL) ──────────────────────────────────────────────
@st.cache_data(ttl=REFRESH_SECONDS)
def fetch_livreurs() -> dict:
    try:
        r = requests.get(
            f"{API_URL}/livreurs-proches",
            params={"lat": PARIS_LAT, "lon": PARIS_LON, "rayon": 15, "limit": 200},
            timeout=3,
        )
        return r.json()
    except Exception as exc:
        return {"error": str(exc), "livreurs": [], "count": 0}


@st.cache_data(ttl=5)
def fetch_stats() -> dict:
    try:
        return requests.get(f"{API_URL}/stats", timeout=3).json()
    except Exception:
        return {}


@st.cache_data(ttl=10)
def fetch_performance() -> dict:
    try:
        return requests.get(f"{API_URL}/health/performance?samples=100", timeout=8).json()
    except Exception:
        return {}


# ── Header ──────────────────────────────────────────────────────────────────────
hdr_left, hdr_right = st.columns([4, 1])
with hdr_left:
    st.markdown("# 🛵 FleetStream — Architecture Lambda")
    st.caption(
        "**Hot Path** : Redis Stack GEOSEARCH <10ms  ·  "
        "**Cold Path** : Apache Parquet + DuckDB  ·  "
        "**Broker** : Redpanda (Kafka-compatible)"
    )
with hdr_right:
    st.markdown(f"🕐 `{datetime.now().strftime('%H:%M:%S')}`")
    st.caption(f"Auto-refresh: {REFRESH_SECONDS}s")

st.divider()

# ── Fetch data ──────────────────────────────────────────────────────────────────
livreurs_data = fetch_livreurs()
stats_data    = fetch_stats()
perf_data     = fetch_performance()

hp = stats_data.get("hot_path", {})
cp = stats_data.get("cold_path", {})
statuts = hp.get("statuts", {})
bench   = perf_data.get("geosearch_benchmark", {})

# ── KPI Row ─────────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5, k6 = st.columns(6)

k1.metric("🛵 Livreurs actifs",  hp.get("livreurs_actifs", "—"))
k2.metric("📨 Messages traités", f"{hp.get('messages_traites', 0):,}")
k3.metric("🟠 En livraison",     statuts.get("delivering", 0))
k4.metric("🟢 Disponibles",      statuts.get("available", 0))
k5.metric("⚡ Latence P99",      f"{bench.get('p99_ms', '—')} ms" if bench else "—")
k6.metric("🗄️ Fichiers Parquet", cp.get("fichiers_parquet", 0))

st.divider()

# ── Main layout : Map | Stats ───────────────────────────────────────────────────
map_col, stats_col = st.columns([3, 1])

# ── Carte live ──────────────────────────────────────────────────────────────────
with map_col:
    livreurs = livreurs_data.get("livreurs", [])
    error    = livreurs_data.get("error")

    if error:
        st.error(f"API inaccessible : {error}")
    elif not livreurs:
        st.warning("⏳ En attente des données GPS... (les consumers doivent être actifs)")
    else:
        df = pd.DataFrame([
            {
                "lat":    lv["lat"],
                "lon":    lv["lon"],
                "id":     lv["livreur_id"],
                "speed":  round(lv.get("speed_kmh", 0), 1),
                "status": lv.get("status", "unknown"),
                "dist":   lv.get("distance_km", 0),
                "color":  STATUS_COLORS.get(lv.get("status", ""), [200, 200, 200, 200]),
            }
            for lv in livreurs
        ])

        # Couche principale : livreurs (ScatterplotLayer)
        scatter = pdk.Layer(
            "ScatterplotLayer",
            df,
            get_position=["lon", "lat"],
            get_fill_color="color",
            get_radius=100,
            pickable=True,
            auto_highlight=True,
            highlight_color=[255, 255, 0, 255],
            radius_min_pixels=4,
            radius_max_pixels=20,
        )

        # Couche de flou (halo) pour l'effet visuel
        halo = pdk.Layer(
            "ScatterplotLayer",
            df,
            get_position=["lon", "lat"],
            get_fill_color=[[c[0], c[1], c[2], 40] for c in df["color"].tolist()],
            get_radius=300,
            pickable=False,
            radius_min_pixels=8,
        )

        deck = pdk.Deck(
            layers=[halo, scatter],
            initial_view_state=pdk.ViewState(
                latitude=PARIS_LAT,
                longitude=PARIS_LON,
                zoom=11,
                pitch=45,
                bearing=0,
            ),
            tooltip={
                "html": (
                    "<b style='color:#CBA6F7'>🛵 {id}</b><br/>"
                    "📍 {lat:.5f}, {lon:.5f}<br/>"
                    "⚡ <b>{speed}</b> km/h<br/>"
                    "● {status}<br/>"
                    "📏 {dist:.2f} km du centre"
                ),
                "style": {
                    "backgroundColor": "#1E1E2E",
                    "color": "#CDD6F4",
                    "fontSize": "13px",
                    "borderRadius": "8px",
                    "padding": "8px",
                },
            },
            # Fond carte sombre sans token Mapbox (CartoCDN)
            map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
        )

        st.pydeck_chart(deck, use_container_width=True)

        # Légende
        l1, l2, l3 = st.columns(3)
        l1.markdown("🟠 **En livraison**")
        l2.markdown("🟢 **Disponible**")
        l3.markdown("⚫ **Inactif**")

# ── Panneau de droite ───────────────────────────────────────────────────────────
with stats_col:
    st.subheader("⚡ Hot Path (Redis)")
    if bench:
        sla_ok = bench.get("p99_ms", 999) < 10
        sla_badge = "✅ SLA OK" if sla_ok else "⚠️ SLA KO"
        st.info(f"{sla_badge} — Cible : p99 < 10ms")
        bcol1, bcol2 = st.columns(2)
        bcol1.metric("P50", f"{bench.get('p50_ms', '—')} ms")
        bcol2.metric("P95", f"{bench.get('p95_ms', '—')} ms")
        bcol1.metric("P99", f"{bench.get('p99_ms', '—')} ms")
        bcol2.metric("Max", f"{bench.get('max_ms', '—')} ms")
    else:
        st.caption("Benchmark en cours de chargement...")

    st.divider()
    st.subheader("🗄️ Cold Path (Parquet)")
    st.metric("Fichiers Parquet",  cp.get("fichiers_parquet", 0))
    st.metric("Taille Data Lake",  f"{cp.get('taille_totale_mb', 0):.1f} MB")
    st.caption("DuckDB query sur hive-partitions `year/month/day/hour`")

    st.divider()
    st.subheader("🏗️ Architecture Lambda")
    st.code(
        "Producer\n"
        "  └─→ Redpanda (Kafka)\n"
        "       ├─→ hot-consumer\n"
        "       │    └─→ Redis (TTL 30s)\n"
        "       └─→ cold-consumer\n"
        "            └─→ Parquet/Snappy\n"
        "API\n"
        "  ├─→ GEOSEARCH Redis (<10ms)\n"
        "  └─→ DuckDB → Parquet",
        language="text",
    )

    st.divider()
    if st.button("🔄 Forcer le rafraîchissement"):
        st.cache_data.clear()
        st.rerun()

# ── Auto-refresh ─────────────────────────────────────────────────────────────────
time.sleep(REFRESH_SECONDS)
st.rerun()
