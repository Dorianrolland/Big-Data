"""
FleetStream — Hot Path Consumer (Speed Layer)
=============================================
Consomme le flux GPS Kafka et maintient Redis à jour via GEOADD.

Stratégie Redis :
  - fleet:geo          → Sorted set géospatial (GEOADD) — requêtes GEOSEARCH <1ms
  - fleet:geo:lastseen → Sorted set auxiliaire (score = epoch_ms) — source
                         de vérité pour l'expiration du sorted set GEO
  - fleet:livreur:<id> → Hash avec les métadonnées complètes — TTL 30s
  - fleet:stats:*      → Compteurs de monitoring

Latence cible : <10ms entre réception Kafka et disponibilité dans Redis.
Pipeline Redis : les GEOADD + HSET + EXPIRE sont groupés en une seule
requête réseau par batch (pas de round-trip par message).

Cohérence GEO ↔ Hashes :
  Les hashes fleet:livreur:<id> expirent via TTL natif Redis (commande EXPIRE),
  mais le sorted set fleet:geo ne supporte pas de TTL par membre. Un sorted set
  auxiliaire fleet:geo:lastseen (score = epoch_ms) est maintenu en parallèle :
  une tâche de fond purge toutes les PURGE_INTERVAL_S les entrées dont le score
  est antérieur à (now - GPS_TTL_SECONDS). Cela garantit que ZCARD(fleet:geo)
  et GEOSEARCH reflètent fidèlement les livreurs encore actifs.
"""
import asyncio
import json
import logging
import os
import signal
import time
from typing import Literal

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError

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
GEO_TS_KEY = "fleet:geo:lastseen"   # Sorted set auxiliaire (score = epoch_ms)
HASH_PREFIX = "fleet:livreur:"
STATS_MSGS_KEY = "fleet:stats:total_messages"
STATS_ACTIVE_KEY = "fleet:stats:livreurs_actifs"
STATS_PURGED_KEY = "fleet:stats:ghosts_purged"
STATS_DLQ_KEY = "fleet:stats:dlq_count"
DLQ_KEY = "fleet:dlq"               # List Redis : derniers messages invalides
DLQ_MAX_LEN = 1000                  # LTRIM cap — DLQ n'est pas un stockage long terme

# Fréquence de la boucle de purge des entrées fantômes (< TTL pour rester cohérent)
PURGE_INTERVAL_S = float(os.getenv("GEO_PURGE_INTERVAL_S", "5.0"))


# ── Schéma Pydantic ──────────────────────────────────────────────────────────────
# Validation stricte à l'entrée du Speed Layer. Tout message qui ne passe pas est
# routé vers la DLQ Redis (fleet:dlq) avec le payload brut + la raison du rejet.
# Sans cette barrière, un producer bugué pouvait empoisonner le sorted set GEO
# avec lat/lon NaN ou des status inconnus, et faire diverger les KPIs dashboard.
class Position(BaseModel):
    livreur_id:  str = Field(..., min_length=1, max_length=64)
    lat:         float = Field(..., ge=-90.0,  le=90.0)
    lon:         float = Field(..., ge=-180.0, le=180.0)
    speed_kmh:   float = Field(0.0, ge=0.0,    le=300.0)   # 300 km/h = borne capteur
    heading_deg: float = Field(0.0, ge=0.0,    le=360.0)
    status:      Literal["available", "delivering", "idle", "unknown"] = "unknown"
    accuracy_m:  float = Field(0.0, ge=0.0,    le=10000.0)
    battery_pct: float = Field(0.0, ge=0.0,    le=100.0)
    timestamp:   str   = Field(..., min_length=1)


def _validate_batch(raw_batch: list[dict]) -> tuple[list[Position], list[dict]]:
    """
    Sépare un batch brut en (messages valides, rejets DLQ) en UN seul passage.
    Chaque rejet est un dict {"raw": <payload>, "reason": <message Pydantic>,
    "ts_rejected": <iso>} prêt à être JSON-serialisé pour la DLQ Redis.
    """
    valid: list[Position] = []
    rejected: list[dict] = []
    for raw in raw_batch:
        try:
            valid.append(Position(**raw))
        except ValidationError as exc:
            rejected.append({
                "raw":          raw,
                "reason":       exc.errors(include_url=False, include_input=False),
                "ts_rejected":  time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            })
        except (TypeError, KeyError) as exc:
            # Payload qui n'est même pas un dict cohérent — Pydantic n'a pas pu mapper
            rejected.append({
                "raw":          raw,
                "reason":       str(exc),
                "ts_rejected":  time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            })
    return valid, rejected


async def _push_dlq(r: aioredis.Redis, rejected: list[dict]) -> None:
    """
    Pousse les messages invalides dans la DLQ Redis (LPUSH + LTRIM cap).
    LTRIM garantit que la DLQ ne grossit pas indéfiniment — c'est une fenêtre
    de debug, pas un stockage long terme. Pour persister à long terme, brancher
    Logstash / Vector dessus.
    """
    if not rejected:
        return
    pipe = r.pipeline(transaction=False)
    for rej in rejected:
        pipe.lpush(DLQ_KEY, json.dumps(rej, default=str))
    pipe.ltrim(DLQ_KEY, 0, DLQ_MAX_LEN - 1)
    pipe.incrby(STATS_DLQ_KEY, len(rejected))
    await pipe.execute()
    log.warning("DLQ : %d message(s) invalide(s) routé(s) vers %s", len(rejected), DLQ_KEY)


async def flush_to_redis(r: aioredis.Redis, batch: list[Position]) -> None:
    """
    Écrit un batch de positions GPS DÉJÀ VALIDÉES en Redis via pipeline.
    Une seule requête réseau pour toutes les commandes du batch.

    Pour chaque message :
      1. GEOADD fleet:geo               → indexation géospatiale
      2. ZADD fleet:geo:lastseen        → marque "dernière vue" (score=epoch_ms)
      3. HSET fleet:livreur:<id>        → métadonnées complètes
      4. EXPIRE fleet:livreur:<id>      → TTL natif Redis sur le hash

    Le sorted set auxiliaire (2) est purgé périodiquement par _purge_ghosts_loop.
    Le batch ne contient QUE des Position validées par Pydantic — plus besoin
    de defensive coding (msg.get("lat", 0)) qui masquait des bugs producer.
    """
    now_ms = int(time.time() * 1000)
    pipe = r.pipeline(transaction=False)
    for msg in batch:
        lid = msg.livreur_id

        # GEOADD : longitude EN PREMIER (convention Redis)
        pipe.geoadd(GEO_KEY, [msg.lon, msg.lat, lid])

        # Marqueur "dernière vue" pour la purge déterministe (cohérence GEO ↔ hashes)
        pipe.zadd(GEO_TS_KEY, {lid: now_ms})

        # Hash : données complètes accessibles par livreur_id
        pipe.hset(
            f"{HASH_PREFIX}{lid}",
            mapping={
                "lat":         msg.lat,
                "lon":         msg.lon,
                "speed_kmh":   msg.speed_kmh,
                "heading_deg": msg.heading_deg,
                "status":      msg.status,
                "accuracy_m":  msg.accuracy_m,
                "battery_pct": msg.battery_pct,
                "ts":          msg.timestamp,
            },
        )
        # TTL : données éphémères — expirent si le livreur disparaît
        pipe.expire(f"{HASH_PREFIX}{lid}", TTL_SECONDS)

    # Compteur de messages pour le monitoring
    pipe.incrby(STATS_MSGS_KEY, len(batch))
    await pipe.execute()


async def _purge_ghosts_loop(
    r: aioredis.Redis,
    stop_event: asyncio.Event,
) -> None:
    """
    Boucle de fond : supprime du sorted set GEO les livreurs qui n'ont pas
    envoyé de position depuis plus de TTL_SECONDS.

    Sans cette boucle, fleet:geo grossit indéfiniment et ZCARD(fleet:geo) diverge
    du nombre réel de livreurs actifs (les hashes fleet:livreur:<id> expirent
    via EXPIRE mais les membres du sorted set restent orphelins — bug fantôme
    classique quand on combine GEOADD + TTL sur des hashes séparés).

    Utilise le sorted set auxiliaire fleet:geo:lastseen (score = epoch_ms)
    comme source de vérité pour la dernière mise à jour de chaque livreur.
    """
    while not stop_event.is_set():
        try:
            cutoff_ms = int((time.time() - TTL_SECONDS) * 1000)
            # Récupère les livreurs expirés (score strictement < cutoff)
            expired = await r.zrangebyscore(GEO_TS_KEY, "-inf", f"({cutoff_ms}")
            if expired:
                pipe = r.pipeline(transaction=False)
                pipe.zrem(GEO_KEY, *expired)
                pipe.zrem(GEO_TS_KEY, *expired)
                pipe.incrby(STATS_PURGED_KEY, len(expired))
                await pipe.execute()
                log.info(
                    "Purge fantômes : %d livreur(s) expiré(s) retiré(s) de fleet:geo",
                    len(expired),
                )
        except Exception as exc:
            log.warning("Erreur dans la boucle de purge : %s", exc)

        # Attente interruptible (réagit instantanément au signal d'arrêt)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=PURGE_INTERVAL_S)
        except asyncio.TimeoutError:
            pass


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

    # Tâche de fond : purge des entrées fantômes du sorted set GEO
    purge_task = asyncio.create_task(_purge_ghosts_loop(r, stop_event))
    log.info(
        "Boucle de purge fantômes lancée (interval=%.1fs, TTL=%ds)",
        PURGE_INTERVAL_S, TTL_SECONDS,
    )

    processed = 0
    rejected_total = 0
    try:
        while not stop_event.is_set():
            # getmany : récupère jusqu'à 500 messages sans bloquer plus de 100ms
            raw = await consumer.getmany(timeout_ms=100, max_records=500)
            raw_batch = [msg.value for msgs in raw.values() for msg in msgs]
            if not raw_batch:
                continue

            # Validation Pydantic stricte — sépare valides / rejets DLQ
            valid_batch, rejected_batch = _validate_batch(raw_batch)

            if rejected_batch:
                await _push_dlq(r, rejected_batch)
                rejected_total += len(rejected_batch)

            if valid_batch:
                await flush_to_redis(r, valid_batch)
                processed += len(valid_batch)

            if processed and processed % 10_000 == 0:
                log.info(
                    "Traités : %d messages (dernier batch : %d, rejets cumul : %d)",
                    processed, len(valid_batch), rejected_total,
                )
    finally:
        purge_task.cancel()
        try:
            await purge_task
        except asyncio.CancelledError:
            pass
        await consumer.stop()
        await r.aclose()
        log.info("Hot consumer arrêté. Total traité : %d messages.", processed)


if __name__ == "__main__":
    asyncio.run(main())
