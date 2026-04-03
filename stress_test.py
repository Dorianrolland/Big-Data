#!/usr/bin/env python3
"""
FleetStream — Stress Test
==========================
Valide l'architecture Lambda sous charge massive.

KAFKA TEST  : simule N livreurs (N msg/s) en continu sur Redpanda
API TEST    : benchmark GEOSEARCH Redis avec requêtes parallèles (p50/p95/p99)

Installation (hors Docker) :
    pip install aiokafka aiohttp

Usage :
    python stress_test.py                         # 1000 livreurs, 30s
    python stress_test.py --livreurs 5000         # 5000 msg/s, 60s
    python stress_test.py --skip-kafka            # benchmark API seulement
    python stress_test.py --livreurs 500 --duration 10 --api http://localhost:8001
"""
import argparse
import asyncio
import json
import math
import random
import sys
import time
from datetime import datetime, timezone

# ── Dependencies optionnelles ────────────────────────────────────────────────────
try:
    from aiokafka import AIOKafkaProducer
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False
    print("[WARN] aiokafka non installé — stress test Kafka désactivé")
    print("       pip install aiokafka")

try:
    import aiohttp
    HAS_HTTP = True
except ImportError:
    HAS_HTTP = False
    print("[WARN] aiohttp non installé — benchmark API désactivé")
    print("       pip install aiohttp")

# ── Géographie Paris ─────────────────────────────────────────────────────────────
PARIS_LAT, PARIS_LON = 48.8566, 2.3522
KAFKA_BOOTSTRAP = "localhost:19092"    # Port externe Redpanda


# ── Simulation GPS minimaliste (optimisée pour la vitesse) ────────────────────────
class FastLivreur:
    """Livreur ultra-léger pour stress test — pas de logique physique complexe."""
    __slots__ = ("id", "lat", "lon", "heading", "speed", "status", "battery")

    def __init__(self, idx: int):
        self.id      = f"S{idx:05d}"
        angle        = random.uniform(0, 2 * math.pi)
        r_km         = random.uniform(0.5, 8.0)
        self.lat     = PARIS_LAT + (r_km / 111.32) * math.cos(angle)
        self.lon     = PARIS_LON + (r_km / (111.32 * 0.669)) * math.sin(angle)
        self.heading = random.uniform(0, 360)
        self.speed   = random.uniform(15, 30)
        self.status  = random.choice(["delivering", "delivering", "available"])
        self.battery = random.uniform(40, 100)

    def next_payload(self) -> bytes:
        self.heading = (self.heading + random.gauss(0, 5)) % 360
        v = self.speed / 3600.0
        hr = math.radians(self.heading)
        self.lat += (v * math.cos(hr)) / 111.32
        self.lon += (v * math.sin(hr)) / (111.32 * 0.669)
        self.battery = max(5.0, self.battery - 0.02)
        return json.dumps({
            "livreur_id": self.id,
            "lat":         round(self.lat, 6),
            "lon":         round(self.lon, 6),
            "speed_kmh":   round(self.speed, 1),
            "heading_deg": round(self.heading, 1),
            "status":      self.status,
            "timestamp":   datetime.now(timezone.utc).isoformat(),
            "accuracy_m":  5.0,
            "battery_pct": round(self.battery, 1),
        }).encode()


def _banner(title: str, width: int = 58) -> None:
    print(f"\n{'═' * width}")
    print(f"  {title}")
    print(f"{'═' * width}")


def _percentile(data: list[float], pct: int) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    return round(s[max(0, int(len(s) * pct / 100) - 1)], 2)


# ════════════════════════════════════════════════════════════════════════════════
#  KAFKA STRESS TEST
# ════════════════════════════════════════════════════════════════════════════════

async def run_kafka_stress(
    num_livreurs: int,
    duration_s: int,
    topic: str = "livreurs-gps",
) -> dict:
    _banner(f"KAFKA STRESS TEST — {num_livreurs:,} livreurs ({num_livreurs:,} msg/s cible)")

    livreurs = [FastLivreur(i) for i in range(num_livreurs)]
    print(f"  {'Livreurs simulés':<28}: {num_livreurs:,}")
    print(f"  {'Durée':<28}: {duration_s}s")
    print(f"  {'Broker':<28}: {KAFKA_BOOTSTRAP}")
    print(f"  {'Topic':<28}: {topic}")
    print()

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        compression_type="lz4",
        max_batch_size=131_072,   # 128KB batch
        linger_ms=5,
        acks=1,
    )

    try:
        await producer.start()
    except Exception as exc:
        print(f"  [ERREUR] Impossible de se connecter à Kafka: {exc}")
        print(f"  → Vérifiez que Redpanda tourne (docker compose ps)")
        return {"error": str(exc)}

    sent        = 0
    errors      = 0
    start       = time.monotonic()
    last_report = start
    throughputs: list[float] = []

    print(f"  {'t (s)':>6} │ {'msg/s':>8} │ {'total envoyés':>14} │ {'erreurs':>7}")
    print(f"  {'─'*6}─┼─{'─'*8}─┼─{'─'*14}─┼─{'─'*7}")

    try:
        while time.monotonic() - start < duration_s:
            t_batch = time.monotonic()

            results = await asyncio.gather(
                *[producer.send(topic, key=lv.id.encode(), value=lv.next_payload())
                  for lv in livreurs],
                return_exceptions=True,
            )

            batch_ok  = sum(1 for r in results if not isinstance(r, Exception))
            batch_err = len(results) - batch_ok
            sent   += batch_ok
            errors += batch_err

            now = time.monotonic()
            elapsed_batch = now - t_batch
            tps = batch_ok / max(elapsed_batch, 0.001)
            throughputs.append(tps)

            if now - last_report >= 5:
                print(
                    f"  {now - start:>6.1f} │ {tps:>8,.0f} │ {sent:>14,} │ {errors:>7}"
                )
                last_report = now

            # Attend pour maintenir 1 batch/seconde
            await asyncio.sleep(max(0.0, 1.0 - elapsed_batch))

    finally:
        await producer.stop()

    total_time = time.monotonic() - start
    return {
        "messages_sent":          sent,
        "errors":                 errors,
        "duration_s":             round(total_time, 2),
        "avg_throughput_msg_s":   round(sent / max(total_time, 0.001), 0),
        "peak_throughput_msg_s":  round(max(throughputs, default=0), 0),
        "loss_rate_pct":          round(errors / max(sent + errors, 1) * 100, 2),
    }


# ════════════════════════════════════════════════════════════════════════════════
#  API BENCHMARK
# ════════════════════════════════════════════════════════════════════════════════

async def run_api_benchmark(api_url: str, num_requests: int = 500) -> dict:
    _banner(f"API BENCHMARK — {num_requests} requêtes GEOSEARCH parallèles")

    url    = f"{api_url}/livreurs-proches"
    params = {"lat": PARIS_LAT, "lon": PARIS_LON, "rayon": 5, "limit": 50}
    print(f"  {'Endpoint':<28}: GET /livreurs-proches?rayon=5km")
    print(f"  {'Requêtes':<28}: {num_requests}")
    print(f"  {'Concurrence max':<28}: 30")
    print()

    latencies_ms: list[float] = []
    errors = 0

    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Warm-up
        for _ in range(10):
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)):
                    pass
            except Exception:
                pass

        semaphore = asyncio.Semaphore(30)

        async def one_request() -> float | None:
            async with semaphore:
                t0 = time.perf_counter()
                try:
                    async with session.get(
                        url, params=params, timeout=aiohttp.ClientTimeout(total=5)
                    ) as resp:
                        await resp.json()
                    return (time.perf_counter() - t0) * 1000
                except Exception:
                    return None

        results = await asyncio.gather(*[one_request() for _ in range(num_requests)])

    for r in results:
        if r is None:
            errors += 1
        else:
            latencies_ms.append(r)

    if not latencies_ms:
        return {"error": "Toutes les requêtes ont échoué — API accessible?"}

    n = len(latencies_ms)
    return {
        "requests":          num_requests,
        "success":           n,
        "errors":            errors,
        "success_rate_pct":  round(n / num_requests * 100, 1),
        "p50_ms":            _percentile(latencies_ms, 50),
        "p75_ms":            _percentile(latencies_ms, 75),
        "p95_ms":            _percentile(latencies_ms, 95),
        "p99_ms":            _percentile(latencies_ms, 99),
        "mean_ms":           round(sum(latencies_ms) / n, 2),
        "max_ms":            round(max(latencies_ms), 2),
        "sla_ok":            _percentile(latencies_ms, 99) < 10.0,
    }


# ════════════════════════════════════════════════════════════════════════════════
#  RAPPORT FINAL
# ════════════════════════════════════════════════════════════════════════════════

def print_report(kafka: dict | None, api: dict | None) -> None:
    _banner("RAPPORT FINAL — FleetStream Stress Test")

    if kafka and "messages_sent" in kafka:
        print("\n  📊 KAFKA / REDPANDA:")
        print(f"     {'Messages envoyés':<30}: {kafka['messages_sent']:,}")
        print(f"     {'Erreurs':<30}: {kafka['errors']}")
        print(f"     {'Durée':<30}: {kafka['duration_s']}s")
        print(f"     {'Débit moyen':<30}: {kafka['avg_throughput_msg_s']:,.0f} msg/s")
        print(f"     {'Débit peak':<30}: {kafka['peak_throughput_msg_s']:,.0f} msg/s")
        print(f"     {'Taux de perte':<30}: {kafka['loss_rate_pct']}%")
    elif kafka and "error" in kafka:
        print(f"\n  ❌ KAFKA: {kafka['error']}")

    if api and "p50_ms" in api:
        sla = "✅ SLA OK (p99 < 10ms)" if api.get("sla_ok") else "⚠️ SLA KO"
        print(f"\n  ⚡ API — GEOSEARCH (Redis Hot Path)  [{sla}]")
        print(f"     {'Requêtes':<30}: {api['requests']}")
        print(f"     {'Taux de succès':<30}: {api['success_rate_pct']}%")
        print(f"     {'Latence P50':<30}: {api['p50_ms']} ms")
        print(f"     {'Latence P75':<30}: {api['p75_ms']} ms")
        print(f"     {'Latence P95':<30}: {api['p95_ms']} ms")
        print(f"     {'Latence P99':<30}: {api['p99_ms']} ms")
        print(f"     {'Latence Max':<30}: {api['max_ms']} ms")
        print(f"     {'Latence Moyenne':<30}: {api['mean_ms']} ms")
    elif api and "error" in api:
        print(f"\n  ❌ API: {api['error']}")

    print(f"\n{'═' * 58}\n")


# ════════════════════════════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    parser = argparse.ArgumentParser(
        description="FleetStream Stress Test — Architecture Lambda sous charge",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--livreurs",    type=int, default=1000,
                        help="Livreurs simulés (= msg/s) [défaut: 1000]")
    parser.add_argument("--duration",    type=int, default=30,
                        help="Durée stress test Kafka en secondes [défaut: 30]")
    parser.add_argument("--api",         default="http://localhost:8001",
                        help="URL de l'API [défaut: http://localhost:8001]")
    parser.add_argument("--api-requests", type=int, default=500,
                        help="Nombre de requêtes pour le benchmark API [défaut: 500]")
    parser.add_argument("--skip-kafka",  action="store_true",
                        help="Ignorer le stress test Kafka")
    parser.add_argument("--skip-api",    action="store_true",
                        help="Ignorer le benchmark API")
    args = parser.parse_args()

    print("\n" + "█" * 58)
    print("█" + " " * 18 + "FLEETSTREAM" + " " * 27 + "█")
    print("█" + " " * 10 + "Architecture Lambda — Stress Test" + " " * 13 + "█")
    print("█" * 58)
    print(f"\n  Démarré à : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    kafka_result: dict | None = None
    api_result:   dict | None = None

    if not args.skip_kafka:
        if not HAS_KAFKA:
            print("[SKIP] aiokafka non installé — stress test Kafka ignoré")
        else:
            kafka_result = await run_kafka_stress(args.livreurs, args.duration)

    if not args.skip_api:
        if not HAS_HTTP:
            print("[SKIP] aiohttp non installé — benchmark API ignoré")
        else:
            api_result = await run_api_benchmark(args.api, args.api_requests)

    print_report(kafka_result, api_result)


if __name__ == "__main__":
    asyncio.run(main())
