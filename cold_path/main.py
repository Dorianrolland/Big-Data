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
from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.structs import OffsetAndMetadata
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError

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
DLQ_PATH = Path(os.getenv("DLQ_PATH", "/data/dlq"))
BATCH_INTERVAL_S = int(os.getenv("BATCH_INTERVAL_SECONDS", "60"))
MAX_BATCH_RECORDS = int(os.getenv("MAX_BATCH_RECORDS", "50000"))
CONSUMER_GROUP = "cold-consumer"


# ── Schéma Pydantic ──────────────────────────────────────────────────────────────
# Validation stricte avant écriture Parquet. Sans cette barrière, un message
# malformé corrompait silencieusement le data lake (ex : status='delivring'
# au lieu de 'delivering' cassait le dictionary encoding du Parquet, ou bien
# lat=None faisait crasher PyArrow lors du cast en float64).
# Les rejets sont écrits en JSONL dans /data/dlq pour audit hors-ligne (DuckDB
# peut requêter ces fichiers comme n'importe quel jeu Parquet/JSON).
class Position(BaseModel):
    livreur_id:  str = Field(..., min_length=1, max_length=64)
    lat:         float = Field(..., ge=-90.0,  le=90.0)
    lon:         float = Field(..., ge=-180.0, le=180.0)
    speed_kmh:   float = Field(0.0, ge=0.0,    le=300.0)
    heading_deg: float = Field(0.0, ge=0.0,    le=360.0)
    status:      Literal["available", "delivering", "idle", "unknown"] = "unknown"
    accuracy_m:  float = Field(0.0, ge=0.0,    le=10000.0)
    battery_pct: float = Field(0.0, ge=0.0,    le=100.0)
    timestamp:   str   = Field(..., min_length=1)

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


def _validate(raw: dict) -> tuple[Position | None, str | None]:
    """
    Valide un payload Kafka via Pydantic.
    Retourne (Position validée, None) ou (None, message d'erreur lisible).
    """
    try:
        return Position(**raw), None
    except ValidationError as exc:
        return None, json.dumps(exc.errors(include_url=False, include_input=False), default=str)
    except (TypeError, KeyError) as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _write_dlq_jsonl(rejected: list[tuple[dict, str]]) -> None:
    """
    Append-only JSONL — fichier journalier dans /data/dlq/cold-dlq-YYYY-MM-DD.jsonl.
    Format JSONL = parsable par DuckDB (read_json_auto), Spark, jq, etc.
    Chaque ligne : {"raw": <payload>, "reason": <err>, "ts_rejected": <iso>}.
    """
    if not rejected:
        return
    DLQ_PATH.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = DLQ_PATH / f"cold-dlq-{today}.jsonl"
    now_iso = datetime.now(timezone.utc).isoformat()
    with path.open("a", encoding="utf-8") as f:
        for raw, reason in rejected:
            f.write(json.dumps(
                {"raw": raw, "reason": reason, "ts_rejected": now_iso},
                default=str,
            ) + "\n")
    log.warning(
        "DLQ : %d record(s) invalide(s) écrit(s) dans %s",
        len(rejected), path.name,
    )


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
    total_rejected = 0

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
            rejected_chunk: list[tuple[dict, str]] = []
            for tp, msgs in raw.items():
                for m in msgs:
                    payload = m.value
                    pos, err = _validate(payload)
                    if pos is None:
                        # Rejet DLQ — on N'AVANCE PAS l'offset pour ce message ;
                        # un message invalide ne peut pas être rejoué utilement,
                        # mais on évite quand même de bloquer le commit en
                        # incluant son offset dans pending_offsets : il sera
                        # tracé dans la DLQ mais bien marqué comme "consommé".
                        rejected_chunk.append((payload, err or "validation error"))
                    else:
                        # On stocke le dict normalisé via Pydantic (plus de champs
                        # manquants ni de types douteux pour le writer Parquet).
                        buffer.append(pos.model_dump())
                    # Max-offset par partition pour le commit explicite post-flush.
                    # On l'avance même pour les rejets (le message est dans la DLQ
                    # JSONL → ne sera pas rejoué au prochain démarrage).
                    prev = pending_offsets.get(tp, -1)
                    if m.offset > prev:
                        pending_offsets[tp] = m.offset

            if rejected_chunk:
                # Écriture DLQ dans un thread pour ne pas bloquer la boucle
                # asyncio sur les I/O disque (même si l'append est court).
                await asyncio.to_thread(_write_dlq_jsonl, rejected_chunk)
                total_rejected += len(rejected_chunk)

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
        log.info(
            "Cold consumer arrêté. Total flushé : %d enregistrements (rejets DLQ : %d).",
            total_flushed, total_rejected,
        )


if __name__ == "__main__":
    asyncio.run(main())
