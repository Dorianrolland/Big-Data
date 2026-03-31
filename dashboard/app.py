"""
FleetStream — Streamlit Dashboard (Analytics & Cold Path)
==========================================================
Affiche les KPIs temps réel et la trajectoire historique via DuckDB/Parquet.
Pour la carte live sans clignotement : http://localhost:8001/map

Architecture anti-DuplicateWidgetID :
  - Les widgets interactifs (text_input, selectbox) sont déclarés UNE SEULE FOIS
    au niveau module, en dehors de toute boucle, avec des keys explicites.
  - Les sections de données sont mises à jour via st.empty() containers.
  - La boucle de refresh ne recrée jamais les widgets.
"""
import os
import time
from datetime import datetime

import pandas as pd
import pydeck as pdk
import requests
import streamlit as st

API_URL         = os.getenv("API_URL", "http://api:8000")
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "3"))

STATUS_COLOR_RGB = {
    "delivering": [245, 158, 11,  230],
    "available":  [16,  185, 129, 230],
    "idle":       [107, 114, 128, 180],
}

# ── Config page ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="FleetStream — Analytics",
    page_icon="🛵",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
.block-container { padding-top: 1rem; padding-bottom: 0.5rem; }
[data-testid="metric-container"] {
    background: #1E1E2E;
    border: 1px solid #313244;
    border-radius: 10px;
    padding: 12px 16px;
}
h1, h2, h3 { margin-bottom: 0 !important; }
a { color: #7c6af7 !important; }
</style>
""", unsafe_allow_html=True)


def fetch(path: str, params: dict | None = None) -> dict:
    try:
        return requests.get(f"{API_URL}{path}", params=params, timeout=5).json()
    except Exception:
        return {}


# ════════════════════════════════════════════════════════════════════════════
#  HEADER — statique, rendu une seule fois
# ════════════════════════════════════════════════════════════════════════════
st.markdown("# 🛵 FleetStream — Analytics Dashboard")
st.markdown(
    "**Hot Path** : Redis GEOSEARCH &lt;10ms  ·  "
    "**Cold Path** : DuckDB / Parquet  ·  "
    "[🗺️ Carte live →](http://localhost:8001/map)",
    unsafe_allow_html=True,
)
st.divider()

# ════════════════════════════════════════════════════════════════════════════
#  SECTION HISTORIQUE — widgets déclarés UNE SEULE FOIS (hors boucle)
#  Les keys explicites empêchent le DuplicateWidgetID
# ════════════════════════════════════════════════════════════════════════════
st.subheader("📍 Trajectoire historique — Cold Path (DuckDB → Parquet)")

col_id, col_h, col_btn = st.columns([3, 2, 1])
with col_id:
    livreur_id = st.text_input(
        "Identifiant livreur",
        value="L042",
        placeholder="ex: L042",
        key="hist_livreur_id",           # ← key unique, déclaré UNE FOIS
        label_visibility="collapsed",
    )
with col_h:
    heures = st.selectbox(
        "Fenêtre temporelle",
        options=[1, 2, 4, 8, 24],
        format_func=lambda h: f"{h}h",
        key="hist_heures",               # ← key unique, déclaré UNE FOIS
        label_visibility="collapsed",
    )
with col_btn:
    refresh_hist = st.button("🔍 Analyser", key="hist_btn", use_container_width=True)

# Placeholder pour les résultats de l'historique (mis à jour sans recréer les widgets)
ph_history_result = st.empty()

# ── Session state : on mémorise le dernier résultat pour l'afficher même sans clic ──
if "last_hist" not in st.session_state:
    st.session_state.last_hist = None
if "last_livreur_id" not in st.session_state:
    st.session_state.last_livreur_id = livreur_id

# Déclenche si bouton cliqué OU si le livreur_id a changé
should_fetch_hist = refresh_hist or (livreur_id != st.session_state.last_livreur_id)
if should_fetch_hist:
    st.session_state.last_livreur_id = livreur_id
    with st.spinner(f"Requête DuckDB sur Parquet pour {livreur_id}..."):
        st.session_state.last_hist = fetch(f"/analytics/history/{livreur_id}", {"heures": heures})

# ── Affichage du résultat ──────────────────────────────────────────────────
hist = st.session_state.last_hist or {}
with ph_history_result.container():
    if "resume" in hist:
        r    = hist["resume"]
        traj = hist.get("trajectory", [])

        # KPIs de trajectoire
        h1, h2, h3, h4 = st.columns(4)
        h1.metric("📌 Points GPS",         f"{r.get('nb_points', 0):,}")
        h2.metric("📏 Distance parcourue", f"{r.get('distance_totale_km', 0):.2f} km")
        h3.metric("⚡ Vitesse moyenne",    f"{r.get('vitesse_moy_kmh', 0):.1f} km/h")
        h4.metric("🏆 Statut dominant",    r.get("statut_dominant", "—"))

        if traj:
            df = pd.DataFrame([
                {
                    "lat":         p["lat"],
                    "lon":         p["lon"],
                    "speed_kmh":   p.get("speed_kmh") or 0.0,
                    "status":      p.get("status", "idle"),
                    "color":       STATUS_COLOR_RGB.get(p.get("status", "idle"),
                                                        [107, 114, 128, 180]),
                }
                for p in traj
            ])

            # PathLayer : tracé de la trajectoire en blanc semi-transparent
            path_data = [{"path": [[row.lon, row.lat] for row in df.itertuples()]}]
            path_layer = pdk.Layer(
                "PathLayer",
                data=path_data,
                get_path="path",
                get_color=[200, 200, 255, 140],
                get_width=4,
                width_min_pixels=2,
            )

            # ScatterplotLayer : points colorés par statut
            scatter_layer = pdk.Layer(
                "ScatterplotLayer",
                data=df,
                get_position=["lon", "lat"],
                get_fill_color="color",
                get_radius=60,
                radius_min_pixels=4,
                radius_max_pixels=14,
                pickable=True,
                auto_highlight=True,
            )

            # Première et dernière position
            start_end = pd.DataFrame([
                {"lon": df.iloc[0]["lon"],  "lat": df.iloc[0]["lat"],
                 "label": "Départ", "color": [0, 255, 100, 255]},
                {"lon": df.iloc[-1]["lon"], "lat": df.iloc[-1]["lat"],
                 "label": "Arrivée", "color": [255, 80, 80, 255]},
            ])
            marker_layer = pdk.Layer(
                "ScatterplotLayer",
                data=start_end,
                get_position=["lon", "lat"],
                get_fill_color="color",
                get_radius=120,
                radius_min_pixels=8,
                radius_max_pixels=20,
                pickable=True,
            )

            center_lat = df["lat"].mean()
            center_lon = df["lon"].mean()

            deck = pdk.Deck(
                layers=[path_layer, scatter_layer, marker_layer],
                initial_view_state=pdk.ViewState(
                    latitude=center_lat,
                    longitude=center_lon,
                    zoom=13,
                    pitch=40,
                ),
                tooltip={
                    "html": "<b>{status}</b><br/>⚡ {speed_kmh:.1f} km/h",
                    "style": {
                        "backgroundColor": "#1E1E2E",
                        "color": "#CDD6F4",
                        "fontSize": "12px",
                        "borderRadius": "6px",
                    },
                },
                map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
            )

            st.pydeck_chart(deck, use_container_width=True)

            # Légende
            leg1, leg2, leg3 = st.columns(3)
            leg1.markdown("🟠 En livraison")
            leg2.markdown("🟢 Disponible")
            leg3.markdown("⚫ Inactif")
        else:
            st.info("Aucun point de trajectoire dans ce fichier Parquet.")

    elif "detail" in hist:
        st.warning(f"ℹ️ {hist['detail']}")
    elif hist == {}:
        st.caption("Lance une analyse pour afficher la trajectoire.")

st.divider()

# ════════════════════════════════════════════════════════════════════════════
#  PLACEHOLDERS pour les sections à refresh automatique
# ════════════════════════════════════════════════════════════════════════════
ph_kpis  = st.empty()
ph_bench = st.empty()
ph_cold  = st.empty()
ph_ts    = st.empty()

# ════════════════════════════════════════════════════════════════════════════
#  BOUCLE DE REFRESH — ne touche QUE les placeholders, jamais les widgets
# ════════════════════════════════════════════════════════════════════════════
while True:
    stats     = fetch("/stats")
    hp        = stats.get("hot_path", {})
    cp        = stats.get("cold_path", {})
    st_counts = hp.get("statuts", {})

    # ── KPIs ─────────────────────────────────────────────────────────────────
    with ph_kpis.container():
        st.subheader("⚡ Hot Path — Métriques temps réel")
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("🛵 Livreurs actifs",  hp.get("livreurs_actifs", "—"))
        c2.metric("📨 Messages GPS",     f"{hp.get('messages_traites', 0):,}")
        c3.metric("🟠 En livraison",     st_counts.get("delivering", 0))
        c4.metric("🟢 Disponibles",      st_counts.get("available", 0))
        c5.metric("🗄️ Fichiers Parquet", cp.get("fichiers_parquet", 0))

    # ── Benchmark Redis ──────────────────────────────────────────────────────
    perf  = fetch("/health/performance", {"samples": 100})
    bench = perf.get("geosearch_benchmark", {})
    rinfo = perf.get("redis_info", {})

    with ph_bench.container():
        st.subheader("📊 SLA Redis GEOSEARCH")
        if bench:
            sla_ok = bench.get("p99_ms", 999) < 10
            msg = (
                f"✅ SLA respecté — p99 = **{bench['p99_ms']} ms** (cible < 10ms)"
                if sla_ok else
                f"⚠️ SLA dépassé — p99 = **{bench['p99_ms']} ms**"
            )
            (st.success if sla_ok else st.error)(msg)

            bc1, bc2, bc3, bc4 = st.columns(4)
            bc1.metric("P50", f"{bench.get('p50_ms', '—')} ms")
            bc2.metric("P95", f"{bench.get('p95_ms', '—')} ms")
            bc3.metric("P99", f"{bench.get('p99_ms', '—')} ms")
            bc4.metric("Max", f"{bench.get('max_ms', '—')} ms")

            if rinfo:
                with st.expander("🔧 Redis INFO détaillé"):
                    r1, r2, r3 = st.columns(3)
                    r1.metric("Version",        rinfo.get("version", "—"))
                    r2.metric("Clients",        rinfo.get("connected_clients", "—"))
                    r3.metric("Mémoire",        rinfo.get("used_memory_human", "—"))
                    r1.metric("Ops/sec",        rinfo.get("ops_per_sec", "—"))
                    r2.metric("Keyspace hits",  f"{rinfo.get('keyspace_hits', 0):,}")
                    r3.metric("Commandes tot.", f"{rinfo.get('total_commands', 0):,}")

    # ── Cold Path stats ──────────────────────────────────────────────────────
    with ph_cold.container():
        st.subheader("🗄️ Cold Path — Data Lake Parquet")
        cc1, cc2, cc3, cc4 = st.columns(4)
        cc1.metric("Fichiers Parquet",  cp.get("fichiers_parquet", 0))
        cc2.metric("Taille totale",     f"{cp.get('taille_totale_mb', 0):.1f} MB")
        cc3.metric("Compression",       "Snappy")
        cc4.metric("Partitionnement",   "Hive (y/m/d/h)")

    # ── Timestamp ────────────────────────────────────────────────────────────
    with ph_ts.container():
        st.caption(
            f"Dernière mise à jour : {datetime.now().strftime('%H:%M:%S')} "
            f"· Refresh automatique toutes les {REFRESH_SECONDS}s"
        )

    time.sleep(REFRESH_SECONDS)
