"""
FleetStream — Streamlit Dashboard (stats & analytics)
=======================================================
Ce dashboard affiche les KPIs et analytics Cold Path.
Pour la carte live sans clignotement : http://localhost:8001/map

Anti-flickering : les widgets sont mis à jour via st.empty() containers
sans jamais appeler st.rerun() sur toute la page.
"""
import os
import time
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

API_URL         = os.getenv("API_URL", "http://api:8000")
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "3"))

STATUS_COLORS = {
    "delivering": "#f59e0b",
    "available":  "#10b981",
    "idle":       "#6b7280",
}

st.set_page_config(
    page_title="FleetStream — Analytics",
    page_icon="🛵",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
.block-container { padding-top: 1rem; padding-bottom: 0; }
[data-testid="metric-container"] {
    background: #1E1E2E; border: 1px solid #313244;
    border-radius: 10px; padding: 12px 16px;
}
h1 { margin-bottom: 0 !important; }
a { color: #7c6af7 !important; }
</style>
""", unsafe_allow_html=True)


def fetch(path: str, params: dict | None = None) -> dict:
    try:
        return requests.get(f"{API_URL}{path}", params=params, timeout=4).json()
    except Exception:
        return {}


# ── Header (statique — ne se re-render pas) ──────────────────────────────────
st.markdown("# 🛵 FleetStream — Analytics Dashboard")
st.markdown(
    "**Hot Path** : Redis GEOSEARCH  ·  "
    "**Cold Path** : DuckDB / Parquet  ·  "
    "[🗺️ Carte live sans clignotement →](http://localhost:8001/map)"
)
st.divider()

# ── Placeholders (créés une seule fois) ──────────────────────────────────────
ph_kpis      = st.empty()
ph_bench     = st.empty()
ph_cold      = st.empty()
ph_history   = st.empty()
ph_ts        = st.empty()

# ── Boucle de refresh — mise à jour des placeholders uniquement ──────────────
while True:
    stats = fetch("/stats")
    hp    = stats.get("hot_path", {})
    cp    = stats.get("cold_path", {})
    st_counts = hp.get("statuts", {})

    # ── KPIs ─────────────────────────────────────────────────────────────────
    with ph_kpis.container():
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("🛵 Livreurs actifs",   hp.get("livreurs_actifs", "—"))
        c2.metric("📨 Messages GPS",      f"{hp.get('messages_traites', 0):,}")
        c3.metric("🟠 En livraison",      st_counts.get("delivering", 0))
        c4.metric("🟢 Disponibles",       st_counts.get("available", 0))
        c5.metric("🗄️ Fichiers Parquet",  cp.get("fichiers_parquet", 0))

    # ── Benchmark Redis ──────────────────────────────────────────────────────
    perf = fetch("/health/performance", {"samples": 100})
    bench = perf.get("geosearch_benchmark", {})
    rinfo = perf.get("redis_info", {})

    with ph_bench.container():
        st.subheader("⚡ Hot Path — SLA Redis GEOSEARCH")
        if bench:
            sla_ok = bench.get("p99_ms", 999) < 10
            if sla_ok:
                st.success(f"✅ SLA respecté — p99 = **{bench['p99_ms']} ms** (cible < 10ms)")
            else:
                st.error(f"⚠️ SLA dépassé — p99 = **{bench['p99_ms']} ms**")

            bc1, bc2, bc3, bc4 = st.columns(4)
            bc1.metric("P50", f"{bench.get('p50_ms', '—')} ms")
            bc2.metric("P95", f"{bench.get('p95_ms', '—')} ms")
            bc3.metric("P99", f"{bench.get('p99_ms', '—')} ms")
            bc4.metric("Max", f"{bench.get('max_ms', '—')} ms")

            if rinfo:
                with st.expander("📊 Redis INFO"):
                    r1, r2, r3 = st.columns(3)
                    r1.metric("Version",       rinfo.get("version", "—"))
                    r2.metric("Clients",       rinfo.get("connected_clients", "—"))
                    r3.metric("Mémoire",       rinfo.get("used_memory_human", "—"))
                    r1.metric("Ops/sec",       rinfo.get("ops_per_sec", "—"))
                    r2.metric("Keyspace hits", f"{rinfo.get('keyspace_hits', 0):,}")
                    r3.metric("Commands tot.", f"{rinfo.get('total_commands', 0):,}")

    st.divider()

    # ── Cold Path ────────────────────────────────────────────────────────────
    with ph_cold.container():
        st.subheader("🗄️ Cold Path — Data Lake Parquet")
        cc1, cc2, cc3, cc4 = st.columns(4)
        cc1.metric("Fichiers Parquet",  cp.get("fichiers_parquet", 0))
        cc2.metric("Taille totale",     f"{cp.get('taille_totale_mb', 0):.1f} MB")
        cc3.metric("Compression",       "Snappy")
        cc4.metric("Partitionnement",   "Hive (y/m/d/h)")

    # ── Historique livreur ───────────────────────────────────────────────────
    with ph_history.container():
        st.subheader("📍 Trajectoire historique (Cold Path — DuckDB)")

        col_input, col_btn = st.columns([3, 1])
        with col_input:
            livreur_id = st.text_input("Livreur ID", value="L042", label_visibility="collapsed")
        with col_btn:
            heures = st.selectbox("Fenêtre", [1, 2, 4, 8, 24], label_visibility="collapsed")

        hist = fetch(f"/analytics/history/{livreur_id}", {"heures": heures})
        if "resume" in hist:
            r = hist["resume"]
            h1, h2, h3, h4 = st.columns(4)
            h1.metric("Points GPS",       r.get("nb_points", 0))
            h2.metric("Distance parcourue", f"{r.get('distance_totale_km', 0)} km")
            h3.metric("Vitesse moy.",     f"{r.get('vitesse_moy_kmh', 0)} km/h")
            h4.metric("Statut dominant",  r.get("statut_dominant", "—"))

            traj = hist.get("trajectory", [])
            if traj:
                df = pd.DataFrame([
                    {"lat": p["lat"], "lon": p["lon"],
                     "speed_kmh": p.get("speed_kmh") or 0,
                     "status": p.get("status", "")}
                    for p in traj
                ])
                st.map(df, latitude="lat", longitude="lon", size=20, use_container_width=True)

        elif "detail" in hist:
            st.info(f"ℹ️ {hist['detail']}")

    # ── Timestamp ────────────────────────────────────────────────────────────
    with ph_ts.container():
        st.caption(f"Dernière mise à jour : {datetime.now().strftime('%H:%M:%S')} · Refresh {REFRESH_SECONDS}s")

    time.sleep(REFRESH_SECONDS)
