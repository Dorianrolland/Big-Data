"""
FleetStream — Cold Path Consumer (Batch Layer)
==============================================
Consomme le flux GPS Kafka et écrit des fichiers Parquet optimisés
pour l'analytics et le Machine Learning.

Stratégie de stockage (Hive-partitioning par EVENT TIME) :
  /data/parquet/
    year=2024/month=01/day=15/hour=14/
      batch_1705323600123.parquet     ← compression Snappy
      batch_1705323660456.parquet

IMPORTANT — partitionnement par event time (pas par processing time) :
  Chaque record est rangé dans la partition Hive correspondant au champ
  `timestamp` DU MESSAGE, pas à l'heure à laquelle le consumer écrit
  physiquement le fichier. C'est la seule stratégie qui :
    - garantit que le predicate pushdown DuckDB (WHERE ts >= ...) bénéficie
      du partition pruning (même heure logique ↔ même dossier) ;
    - survit à un rejeu depuis `earliest` sans corruption de la data lake
      (autrement tout l'historique rejoué atterrirait dans la partition now()) ;
    - reste aligné avec l'approche Kleppmann / Beam / Flink (event time + watermark).

Avantages de ce format :
  - DuckDB peut filter par partition SANS lire les données (predicate pushdown)
  - Compatible Spark / Hive / Athena pour un passage en production
  - Chaque fichier est autonome (row groups de 10k lignes)

Déclenchement du flush :
  - Toutes les BATCH_INTERVAL_SECONDS secondes (défaut : 60s), OU
  - Dès que le buffer atteint MAX_BATCH_RECORDS enregistrements
"""
import asyncio
import json
import logging
import os
import signal
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.structs import OffsetAndMetadata
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("cold-consumer")

# ── Config ──────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "livreurs-gps")
DATA_PATH = Path(os.getenv("DATA_PATH", "/data/parquet"))
BATCH_INTERVAL_S = int(os.getenv("BATCH_INTERVAL_SECONDS", "60"))
MAX_BATCH_RECORDS = int(os.getenv("MAX_BATCH_RECORDS", "50000"))
CONSUMER_GROUP = "cold-consumer"

# Schéma Arrow explicite — garantit la cohérence entre les fichiers
# 'status' en dictionary encoding : très efficace pour les valeurs répétées
SCHEMA = pa.schema([
    pa.field("livreur_id",  pa.string()),
    pa.field("lat",         pa.float64()),
    pa.field("lon",         pa.float64()),
    pa.field("speed_kmh",   pa.float32()),
    pa.field("heading_deg", pa.float32()),
    pa.field("status",      pa.dictionary(pa.int8(), pa.string())),
    pa.field("accuracy_m",  pa.float32()),
    pa.field("battery_pct", pa.float32()),
    pa.field("ts",          pa.timestamp("ms", tz="UTC")),
])


def _parse_ts(s: str | None) -> datetime | None:
    """Parse une chaîne ISO-8601 → datetime aware UTC. Retourne None si invalide."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _partition_dir(key: tuple[int, int, int, int] | None) -> Path:
    """
    Construit le chemin Hive-partitionné pour une clé (year, month, day, hour).
    La clé None est réservée aux records dont le timestamp n'a pas pu être parsé.
    """
    if key is None:
        return (
            DATA_PATH
            / "year=unknown"
            / "month=unknown"
            / "day=unknown"
            / "hour=unknown"
        )
    y, m, d, h = key
    return (
        DATA_PATH
        / f"year={y}"
        / f"month={m:02d}"
        / f"day={d:02d}"
        / f"hour={h:02d}"
    )


def _write_parquet(
    key: tuple[int, int, int, int] | None,
    records: list[tuple[dict, datetime | None]],
) -> None:
    """
    Sérialise un sous-batch déjà groupé par (year, month, day, hour) en event time.
    Chaque entrée est (record_dict, parsed_datetime) pour éviter un second parse.
    """
    partition = _partition_dir(key)
    partition.mkdir(parents=True, exist_ok=True)

    ts_array = pa.array(
        [dt for _, dt in records],
        type=pa.timestamp("ms", tz="UTC"),
    )

    table = pa.table(
        {
            "livreur_id":  pa.array([r.get("livreur_id", "") for r, _ in records]),
            "lat":         pa.array([r.get("lat",  0.0)       for r, _ in records], type=pa.float64()),
            "lon":         pa.array([r.get("lon",  0.0)       for r, _ in records], type=pa.float64()),
            "speed_kmh":   pa.array([r.get("speed_kmh",   0.0) for r, _ in records], type=pa.float32()),
            "heading_deg": pa.array([r.get("heading_deg", 0.0) for r, _ in records], type=pa.float32()),
            "status":      pa.array([r.get("status", "")       for r, _ in records]).cast(
                pa.dictionary(pa.int8(), pa.string())
            ),
            "accuracy_m":  pa.array([r.get("accuracy_m", 0.0) for r, _ in records], type=pa.float32()),
            "battery_pct": pa.array([r.get("battery_pct", 0.0) for r, _ in records], type=pa.float32()),
            "ts": ts_array,
        },
        schema=SCHEMA,
    )

    path = partition / f"batch_{int(time.time() * 1000)}.parquet"
    pq.write_table(
        table,
        path,
        compression="snappy",     # Snappy : bon ratio compression/vitesse décompression
        row_group_size=10_000,    # Optimal pour DuckDB (predicate pushdown par row group)
    )
    size_kb = path.stat().st_size // 1024
    log.info(
        "Flush → %s | %d enregistrements | %d KB | partition %s",
        path.name,
        len(records),
        size_kb,
        partition.relative_to(DATA_PATH),
    )


def flush(records: list[dict]) -> None:
    """
    Sérialise un buffer en Parquet Snappy, partitionné par EVENT TIME.

    Les records sont groupés par (year, month, day, hour) dérivés du champ
    `timestamp` de chaque message. Chaque bucket produit un fichier Parquet
    distinct dans la bonne partition Hive — indispensable pour :
      - le predicate pushdown DuckDB (WHERE ts >= ...) ;
      - la tolérance aux rejeux Kafka depuis `earliest` ;
      - l'alignement avec les conventions Flink / Beam / Spark Structured
        Streaming (event time vs processing time).
    """
    # Groupage par event time en UN SEUL passage sur le buffer
    buckets: dict[tuple[int, int, int, int] | None, list[tuple[dict, datetime | None]]] = defaultdict(list)
    for r in records:
        dt = _parse_ts(r.get("timestamp"))
        if dt is None:
            # Timestamp manquant / invalide → partition "unknown" (jamais silencieux)
            buckets[None].append((r, None))
        else:
            key = (dt.year, dt.month, dt.day, dt.hour)
            buckets[key].append((r, dt))

    nb_invalid = len(buckets.get(None, []))
    if nb_invalid:
        log.warning(
            "flush: %d record(s) sans timestamp valide → partition year=unknown",
            nb_invalid,
        )

    for key, sub_records in buckets.items():
        _write_parquet(key, sub_records)

    if len(buckets) > 1:
        log.info(
            "Flush réparti sur %d partitions event-time (buffer=%d records)",
            len(buckets),
            len(records),
        )


async def main() -> None:
    stop_event = asyncio.Event()

    def _on_signal(*_: object) -> None:
        log.info("Signal reçu — arrêt du cold consumer...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    DATA_PATH.mkdir(parents=True, exist_ok=True)

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda m: json.loads(m.decode()),
        auto_offset_reset="earliest",   # Cold path : lit depuis le début de la rétention
        # IMPORTANT : commit MANUEL après flush Parquet réussi.
        # Avec enable_auto_commit=True, Kafka committait les offsets en arrière-plan
        # (toutes les 5s par défaut) AVANT que le buffer ne soit flushé sur disque.
        # En cas de crash consumer entre l'auto-commit et le prochain flush, tous les
        # records bufferisés étaient perdus définitivement (at-most-once silencieux).
        # Le commit explicite post-flush garantit une sémantique effectively-once :
        # si le flush échoue ou le process meurt, Kafka rejoue le batch au redémarrage.
        enable_auto_commit=False,
        fetch_max_bytes=10_485_760,     # 10MB fetch — optimise le throughput
        max_poll_records=2_000,
    )

    for attempt in range(1, 11):
        try:
            await consumer.start()
            log.info("Cold consumer connecté sur topic '%s'", TOPIC)
            break
        except Exception as exc:
            log.warning("Tentative %d/10 — %s. Nouvel essai dans 3s...", attempt, exc)
            await asyncio.sleep(3)
    else:
        log.error("Impossible de se connecter. Abandon.")
        return

    buffer: list[dict] = []
    # Suit le plus haut offset vu par partition depuis le dernier commit.
    # On commit (offset + 1) — convention Kafka pour "prochaine position à lire".
    pending_offsets: dict[TopicPartition, int] = {}
    last_flush = time.monotonic()
    total_flushed = 0

    async def _flush_and_commit(reason: str) -> None:
        """Flush Parquet, puis commit Kafka. L'ordre est non-négociable."""
        nonlocal buffer, pending_offsets, total_flushed, last_flush
        if not buffer:
            return
        log.info(
            "Déclenchement flush [%s] : %d enregistrements en buffer",
            reason,
            len(buffer),
        )
        # 1. Écriture disque — peut lever une exception (disque plein, S3 down, ...)
        flush(buffer)
        # 2. Commit Kafka UNIQUEMENT si le flush a réussi
        if pending_offsets:
            commit_payload = {
                tp: OffsetAndMetadata(off + 1, "")
                for tp, off in pending_offsets.items()
            }
            await consumer.commit(commit_payload)
            log.info(
                "Offsets committés : %s",
                {str(tp): off + 1 for tp, off in pending_offsets.items()},
            )
        total_flushed += len(buffer)
        buffer = []
        pending_offsets = {}
        last_flush = time.monotonic()

    try:
        while not stop_event.is_set():
            raw = await consumer.getmany(timeout_ms=500, max_records=2_000)
            for tp, msgs in raw.items():
                for m in msgs:
                    buffer.append(m.value)
                    # Max-offset par partition pour le commit explicite post-flush
                    prev = pending_offsets.get(tp, -1)
                    if m.offset > prev:
                        pending_offsets[tp] = m.offset

            now = time.monotonic()
            time_exceeded = (buffer and now - last_flush >= BATCH_INTERVAL_S)
            size_exceeded = len(buffer) >= MAX_BATCH_RECORDS

            if time_exceeded or size_exceeded:
                await _flush_and_commit("taille" if size_exceeded else "timer")

    finally:
        # Flush final pour ne rien perdre à l'arrêt — commit inclus
        if buffer:
            try:
                await _flush_and_commit("arrêt")
            except Exception as exc:
                log.error(
                    "Flush final échoué — offsets NON committés, "
                    "le batch sera rejoué au prochain démarrage : %s",
                    exc,
                )
        await consumer.stop()
        log.info("Cold consumer arrêté. Total flushé : %d enregistrements.", total_flushed)


if __name__ == "__main__":
    asyncio.run(main())
