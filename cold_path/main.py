"""
FleetStream — Cold Path Consumer (Batch Layer)
==============================================
Consomme le flux GPS Kafka et écrit des fichiers Parquet optimisés
pour l'analytics et le Machine Learning.

Stratégie de stockage (Hive-partitioning) :
  /data/parquet/
    year=2024/month=01/day=15/hour=14/
      batch_1705323600123.parquet     ← compression Snappy
      batch_1705323660456.parquet

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
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from aiokafka import AIOKafkaConsumer
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


def flush(records: list[dict]) -> None:
    """
    Sérialise un buffer en Parquet Snappy dans la bonne partition temporelle.
    Utilise PyArrow directement (pas pandas) — ~3x plus rapide pour l'écriture.
    """
    now = datetime.now(timezone.utc)
    partition = (
        DATA_PATH
        / f"year={now.year}"
        / f"month={now.month:02d}"
        / f"day={now.day:02d}"
        / f"hour={now.hour:02d}"
    )
    partition.mkdir(parents=True, exist_ok=True)

    # Conversion des timestamps ISO 8601 → datetime → Arrow timestamp
    # pa.array() ne parse pas les strings ISO directement : on parse manuellement
    def _parse_ts(s: str | None) -> datetime | None:
        if not s:
            return None
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            return None

    try:
        ts_array = pa.array(
            [_parse_ts(r.get("timestamp")) for r in records],
            type=pa.timestamp("ms", tz="UTC"),
        )
    except Exception:
        log.warning("Timestamps invalides dans le batch — fallback None")
        ts_array = pa.array([None] * len(records), type=pa.timestamp("ms", tz="UTC"))

    table = pa.table(
        {
            "livreur_id":  pa.array([r.get("livreur_id", "")  for r in records]),
            "lat":         pa.array([r.get("lat",  0.0)        for r in records], type=pa.float64()),
            "lon":         pa.array([r.get("lon",  0.0)        for r in records], type=pa.float64()),
            "speed_kmh":   pa.array([r.get("speed_kmh",   0.0) for r in records], type=pa.float32()),
            "heading_deg": pa.array([r.get("heading_deg", 0.0) for r in records], type=pa.float32()),
            "status":      pa.array([r.get("status", "")       for r in records]).cast(
                pa.dictionary(pa.int8(), pa.string())
            ),
            "accuracy_m":  pa.array([r.get("accuracy_m", 0.0) for r in records], type=pa.float32()),
            "battery_pct": pa.array([r.get("battery_pct", 0.0) for r in records], type=pa.float32()),
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
        path.name, len(records), size_kb,
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}",
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
        enable_auto_commit=True,
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
    last_flush = time.monotonic()
    total_flushed = 0

    try:
        while not stop_event.is_set():
            raw = await consumer.getmany(timeout_ms=500, max_records=2_000)
            for msgs in raw.values():
                buffer.extend(m.value for m in msgs)

            now = time.monotonic()
            time_exceeded = (buffer and now - last_flush >= BATCH_INTERVAL_S)
            size_exceeded = len(buffer) >= MAX_BATCH_RECORDS

            if time_exceeded or size_exceeded:
                reason = "taille" if size_exceeded else "timer"
                log.info("Déclenchement flush [%s] : %d enregistrements en buffer", reason, len(buffer))
                flush(buffer)
                total_flushed += len(buffer)
                buffer = []
                last_flush = now

    finally:
        # Flush final pour ne rien perdre à l'arrêt
        if buffer:
            log.info("Flush final : %d enregistrements restants...", len(buffer))
            flush(buffer)
            total_flushed += len(buffer)
        await consumer.stop()
        log.info("Cold consumer arrêté. Total flushé : %d enregistrements.", total_flushed)


if __name__ == "__main__":
    asyncio.run(main())
