"""
FleetStream — Hot Path Consumer (Speed Layer)
=============================================
Consomme le flux GPS Kafka et maintient Redis à jour via GEOADD.

Stratégie Redis :
  - fleet:geo         → Sorted set géospatial (GEOADD) — requêtes GEOSEARCH <1ms
  - fleet:livreur:<id> → Hash avec les métadonnées complètes — TTL 30s
  - fleet:stats:*     → Compteurs de monitoring

Latence cible : <10ms entre réception Kafka et disponibilité dans Redis.
Pipeline Redis : les GEOADD + HSET + EXPIRE sont groupés en une seule
requête réseau par batch (pas de round-trip par message).
"""
import asyncio
import json
import logging
import os
import signal

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("hot-consumer")

# ── Config ──────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "livreurs-gps")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
TTL_SECONDS = int(os.getenv("GPS_TTL_SECONDS", "30"))

CONSUMER_GROUP = "hot-consumer"
GEO_KEY = "fleet:geo"
HASH_PREFIX = "fleet:livreur:"
STATS_MSGS_KEY = "fleet:stats:total_messages"
STATS_ACTIVE_KEY = "fleet:stats:livreurs_actifs"


async def flush_to_redis(r: aioredis.Redis, batch: list[dict]) -> None:
    """
    Écrit un batch de positions GPS en Redis via pipeline.
    Une seule requête réseau pour toutes les commandes du batch.
    """
    pipe = r.pipeline(transaction=False)
    for msg in batch:
        lid = msg["livreur_id"]
        lon = msg["lon"]
        lat = msg["lat"]

        # GEOADD : longitude EN PREMIER (convention Redis)
        pipe.geoadd(GEO_KEY, [lon, lat, lid])

        # Hash : données complètes accessibles par livreur_id
        pipe.hset(
            f"{HASH_PREFIX}{lid}",
            mapping={
                "lat":         lat,
                "lon":         lon,
                "speed_kmh":   msg.get("speed_kmh", 0),
                "heading_deg": msg.get("heading_deg", 0),
                "status":      msg.get("status", "unknown"),
                "accuracy_m":  msg.get("accuracy_m", 0),
                "battery_pct": msg.get("battery_pct", 0),
                "ts":          msg.get("timestamp", ""),
            },
        )
        # TTL : données éphémères — expirent si le livreur disparaît
        pipe.expire(f"{HASH_PREFIX}{lid}", TTL_SECONDS)

    # Compteur de messages pour le monitoring
    pipe.incrby(STATS_MSGS_KEY, len(batch))
    await pipe.execute()


async def main() -> None:
    stop_event = asyncio.Event()

    def _on_signal(*_: object) -> None:
        log.info("Signal reçu — arrêt du hot consumer...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    r = aioredis.from_url(REDIS_URL, decode_responses=True)

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda m: json.loads(m.decode()),
        auto_offset_reset="latest",   # Hot path : on ne s'intéresse qu'au présent
        enable_auto_commit=True,
        fetch_max_wait_ms=50,         # Priorité latence : n'attend pas plus de 50ms
        max_poll_records=500,
    )

    for attempt in range(1, 11):
        try:
            await consumer.start()
            log.info("Hot consumer connecté sur topic '%s'", TOPIC)
            break
        except Exception as exc:
            log.warning("Tentative %d/10 — %s. Nouvel essai dans 3s...", attempt, exc)
            await asyncio.sleep(3)
    else:
        log.error("Impossible de se connecter. Abandon.")
        return

    processed = 0
    try:
        while not stop_event.is_set():
            # getmany : récupère jusqu'à 500 messages sans bloquer plus de 100ms
            raw = await consumer.getmany(timeout_ms=100, max_records=500)
            batch = [msg.value for msgs in raw.values() for msg in msgs]
            if not batch:
                continue

            await flush_to_redis(r, batch)
            processed += len(batch)

            if processed % 10_000 == 0:
                log.info("Traités : %d messages (dernier batch : %d)", processed, len(batch))
    finally:
        await consumer.stop()
        await r.aclose()
        log.info("Hot consumer arrêté. Total traité : %d messages.", processed)


if __name__ == "__main__":
    asyncio.run(main())
