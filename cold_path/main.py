"""
FleetStream cold consumer for lambda batch layer.

- keeps existing position parquet layout for backward compatibility
- writes copilot events to a dedicated parquet_events lake
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

from copilot_events_pb2 import ContextSignalV1, CourierPositionV1, OrderEventV1, OrderOfferV1

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("cold-consumer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
COURIER_TOPIC = os.getenv("KAFKA_TOPIC", "livreurs-gps")
ORDER_OFFERS_TOPIC = os.getenv("ORDER_OFFERS_TOPIC", "order-offers-v1")
ORDER_EVENTS_TOPIC = os.getenv("ORDER_EVENTS_TOPIC", "order-events-v1")
CONTEXT_SIGNALS_TOPIC = os.getenv("CONTEXT_SIGNALS_TOPIC", "context-signals-v1")

DATA_PATH = Path(os.getenv("DATA_PATH", "/data/parquet"))
EVENTS_PATH = Path(os.getenv("EVENTS_PATH", "/data/parquet_events"))
DLQ_PATH = Path(os.getenv("DLQ_PATH", "/data/dlq"))
BATCH_INTERVAL_S = int(os.getenv("BATCH_INTERVAL_SECONDS", "60"))
MAX_BATCH_RECORDS = int(os.getenv("MAX_BATCH_RECORDS", "50000"))
CONSUMER_GROUP = "cold-consumer"

POSITION_SCHEMA = pa.schema(
    [
        pa.field("livreur_id", pa.string()),
        pa.field("lat", pa.float64()),
        pa.field("lon", pa.float64()),
        pa.field("speed_kmh", pa.float32()),
        pa.field("heading_deg", pa.float32()),
        pa.field("status", pa.dictionary(pa.int8(), pa.string())),
        pa.field("accuracy_m", pa.float32()),
        pa.field("battery_pct", pa.float32()),
        pa.field("ts", pa.timestamp("ms", tz="UTC")),
    ]
)

EVENT_SCHEMA = pa.schema(
    [
        pa.field("topic", pa.string()),
        pa.field("event_type", pa.string()),
        pa.field("event_id", pa.string()),
        pa.field("offer_id", pa.string()),
        pa.field("order_id", pa.string()),
        pa.field("courier_id", pa.string()),
        pa.field("status", pa.string()),
        pa.field("zone_id", pa.string()),
        pa.field("pickup_lat", pa.float64()),
        pa.field("pickup_lon", pa.float64()),
        pa.field("dropoff_lat", pa.float64()),
        pa.field("dropoff_lon", pa.float64()),
        pa.field("estimated_fare_eur", pa.float32()),
        pa.field("estimated_distance_km", pa.float32()),
        pa.field("estimated_duration_min", pa.float32()),
        pa.field("actual_fare_eur", pa.float32()),
        pa.field("actual_distance_km", pa.float32()),
        pa.field("actual_duration_min", pa.float32()),
        pa.field("demand_index", pa.float32()),
        pa.field("supply_index", pa.float32()),
        pa.field("weather_factor", pa.float32()),
        pa.field("traffic_factor", pa.float32()),
        pa.field("source", pa.string()),
        pa.field("source_platform", pa.string()),
        pa.field("ts", pa.timestamp("ms", tz="UTC")),
    ]
)


def parse_ts(ts_text: str | None) -> datetime | None:
    if not ts_text:
        return None
    try:
        dt = datetime.fromisoformat(ts_text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def write_dlq_jsonl(rejected: list[tuple[str, bytes, str]]) -> None:
    if not rejected:
        return
    DLQ_PATH.mkdir(parents=True, exist_ok=True)
    path = DLQ_PATH / f"cold-dlq-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.jsonl"
    ts_rejected = datetime.now(timezone.utc).isoformat()
    with path.open("a", encoding="utf-8") as fh:
        for topic, raw, reason in rejected:
            record = {
                "topic": topic,
                "reason": reason,
                "raw_hex": raw.hex()[:8000],
                "ts_rejected": ts_rejected,
            }
            fh.write(json.dumps(record) + "\n")
    log.warning("wrote %d invalid events to %s", len(rejected), path.name)


def partition_dir(root: Path, dt: datetime | None) -> Path:
    if dt is None:
        return root / "year=unknown" / "month=unknown" / "day=unknown" / "hour=unknown"
    return root / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}" / f"hour={dt.hour:02d}"


def _atomic_write_table(table: pa.Table, out_path: Path) -> None:
    tmp_path = out_path.with_name(f".{out_path.stem}.{time.time_ns()}.tmp.parquet")
    try:
        pq.write_table(table, tmp_path, compression="snappy", row_group_size=10_000)
        os.replace(tmp_path, out_path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def write_positions(records: list[dict]) -> None:
    buckets: dict[tuple[int, int, int, int] | None, list[dict]] = defaultdict(list)
    for rec in records:
        dt = parse_ts(rec.get("ts"))
        key = None if dt is None else (dt.year, dt.month, dt.day, dt.hour)
        buckets[key].append(rec)

    for key, items in buckets.items():
        if key is None:
            dt = None
        else:
            dt = datetime(key[0], key[1], key[2], key[3], tzinfo=timezone.utc)
        folder = partition_dir(DATA_PATH, dt)
        folder.mkdir(parents=True, exist_ok=True)

        ts_array = pa.array([parse_ts(x.get("ts")) for x in items], type=pa.timestamp("ms", tz="UTC"))
        table = pa.table(
            {
                "livreur_id": pa.array([x.get("livreur_id", "") for x in items]),
                "lat": pa.array([x.get("lat", 0.0) for x in items], type=pa.float64()),
                "lon": pa.array([x.get("lon", 0.0) for x in items], type=pa.float64()),
                "speed_kmh": pa.array([x.get("speed_kmh", 0.0) for x in items], type=pa.float32()),
                "heading_deg": pa.array([x.get("heading_deg", 0.0) for x in items], type=pa.float32()),
                "status": pa.array([x.get("status", "unknown") for x in items]).cast(pa.dictionary(pa.int8(), pa.string())),
                "accuracy_m": pa.array([x.get("accuracy_m", 0.0) for x in items], type=pa.float32()),
                "battery_pct": pa.array([x.get("battery_pct", 0.0) for x in items], type=pa.float32()),
                "ts": ts_array,
            },
            schema=POSITION_SCHEMA,
        )
        out = folder / f"batch_{int(time.time() * 1000)}.parquet"
        _atomic_write_table(table, out)


def write_events(records: list[dict]) -> None:
    buckets: dict[tuple[int, int, int, int, str] | tuple[None, None, None, None, str], list[dict]] = defaultdict(list)
    for rec in records:
        dt = parse_ts(rec.get("ts"))
        topic = rec.get("topic", "unknown")
        if dt is None:
            key = (None, None, None, None, topic)
        else:
            key = (dt.year, dt.month, dt.day, dt.hour, topic)
        buckets[key].append(rec)

    for key, items in buckets.items():
        if key[0] is None:
            dt = None
            topic = key[4]
        else:
            dt = datetime(key[0], key[1], key[2], key[3], tzinfo=timezone.utc)
            topic = key[4]

        folder = partition_dir(EVENTS_PATH / f"topic={topic}", dt)
        folder.mkdir(parents=True, exist_ok=True)

        ts_array = pa.array([parse_ts(x.get("ts")) for x in items], type=pa.timestamp("ms", tz="UTC"))
        def arr(name: str, typ):
            return pa.array([x.get(name) for x in items], type=typ)

        table = pa.table(
            {
                "topic": pa.array([x.get("topic", "") for x in items]),
                "event_type": pa.array([x.get("event_type", "") for x in items]),
                "event_id": pa.array([x.get("event_id", "") for x in items]),
                "offer_id": pa.array([x.get("offer_id", "") for x in items]),
                "order_id": pa.array([x.get("order_id", "") for x in items]),
                "courier_id": pa.array([x.get("courier_id", "") for x in items]),
                "status": pa.array([x.get("status", "") for x in items]),
                "zone_id": pa.array([x.get("zone_id", "") for x in items]),
                "pickup_lat": arr("pickup_lat", pa.float64()),
                "pickup_lon": arr("pickup_lon", pa.float64()),
                "dropoff_lat": arr("dropoff_lat", pa.float64()),
                "dropoff_lon": arr("dropoff_lon", pa.float64()),
                "estimated_fare_eur": arr("estimated_fare_eur", pa.float32()),
                "estimated_distance_km": arr("estimated_distance_km", pa.float32()),
                "estimated_duration_min": arr("estimated_duration_min", pa.float32()),
                "actual_fare_eur": arr("actual_fare_eur", pa.float32()),
                "actual_distance_km": arr("actual_distance_km", pa.float32()),
                "actual_duration_min": arr("actual_duration_min", pa.float32()),
                "demand_index": arr("demand_index", pa.float32()),
                "supply_index": arr("supply_index", pa.float32()),
                "weather_factor": arr("weather_factor", pa.float32()),
                "traffic_factor": arr("traffic_factor", pa.float32()),
                "source": pa.array([x.get("source", "") for x in items]),
                "source_platform": pa.array([x.get("source_platform", "") for x in items]),
                "ts": ts_array,
            },
            schema=EVENT_SCHEMA,
        )
        out = folder / f"batch_{int(time.time() * 1000)}.parquet"
        _atomic_write_table(table, out)


def parse_position(raw: bytes) -> dict:
    # Legacy producers on livreurs-gps emit plain JSON instead of protobuf.
    # Field mapping: livreur_id (not courier_id), timestamp (not ts).
    if raw and raw[0:1] == b"{":
        data = json.loads(raw)
        return {
            "livreur_id": data["livreur_id"],
            "lat": float(data["lat"]),
            "lon": float(data["lon"]),
            "speed_kmh": float(data.get("speed_kmh", 0.0)),
            "heading_deg": float(data.get("heading_deg", 0.0)),
            "status": data.get("status") or "unknown",
            "accuracy_m": float(data.get("accuracy_m", 0.0)),
            "battery_pct": float(data.get("battery_pct", 0.0)),
            "ts": data.get("timestamp") or data.get("ts"),
        }
    msg = CourierPositionV1()
    msg.ParseFromString(raw)
    return {
        "livreur_id": msg.courier_id,
        "lat": msg.lat,
        "lon": msg.lon,
        "speed_kmh": msg.speed_kmh,
        "heading_deg": msg.heading_deg,
        "status": msg.status or "unknown",
        "accuracy_m": msg.accuracy_m,
        "battery_pct": msg.battery_pct,
        "ts": msg.ts,
    }


def parse_offer(raw: bytes, topic: str) -> dict:
    msg = OrderOfferV1()
    msg.ParseFromString(raw)
    return {
        "topic": topic,
        "event_type": msg.event_type,
        "event_id": msg.event_id,
        "offer_id": msg.offer_id,
        "order_id": "",
        "courier_id": msg.courier_id,
        "status": "offered",
        "zone_id": msg.zone_id,
        "pickup_lat": msg.pickup_lat,
        "pickup_lon": msg.pickup_lon,
        "dropoff_lat": msg.dropoff_lat,
        "dropoff_lon": msg.dropoff_lon,
        "estimated_fare_eur": msg.estimated_fare_eur,
        "estimated_distance_km": msg.estimated_distance_km,
        "estimated_duration_min": msg.estimated_duration_min,
        "actual_fare_eur": None,
        "actual_distance_km": None,
        "actual_duration_min": None,
        "demand_index": msg.demand_index,
        "supply_index": None,
        "weather_factor": msg.weather_factor,
        "traffic_factor": msg.traffic_factor,
        "source": "normalized-order-offer",
        "source_platform": msg.source_platform or "unknown",
        "ts": msg.ts,
    }


def parse_order_event(raw: bytes, topic: str) -> dict:
    msg = OrderEventV1()
    msg.ParseFromString(raw)
    return {
        "topic": topic,
        "event_type": msg.event_type,
        "event_id": msg.event_id,
        "offer_id": msg.offer_id,
        "order_id": msg.order_id,
        "courier_id": msg.courier_id,
        "status": msg.status,
        "zone_id": msg.zone_id,
        "pickup_lat": None,
        "pickup_lon": None,
        "dropoff_lat": None,
        "dropoff_lon": None,
        "estimated_fare_eur": None,
        "estimated_distance_km": None,
        "estimated_duration_min": None,
        "actual_fare_eur": msg.actual_fare_eur,
        "actual_distance_km": msg.actual_distance_km,
        "actual_duration_min": msg.actual_duration_min,
        "demand_index": None,
        "supply_index": None,
        "weather_factor": None,
        "traffic_factor": None,
        "source": "normalized-order-event",
        "source_platform": msg.source_platform or "unknown",
        "ts": msg.ts,
    }


def parse_context(raw: bytes, topic: str) -> dict:
    msg = ContextSignalV1()
    msg.ParseFromString(raw)
    return {
        "topic": topic,
        "event_type": msg.event_type,
        "event_id": msg.event_id,
        "offer_id": "",
        "order_id": "",
        "courier_id": "",
        "status": "signal",
        "zone_id": msg.zone_id,
        "pickup_lat": None,
        "pickup_lon": None,
        "dropoff_lat": None,
        "dropoff_lon": None,
        "estimated_fare_eur": None,
        "estimated_distance_km": None,
        "estimated_duration_min": None,
        "actual_fare_eur": None,
        "actual_distance_km": None,
        "actual_duration_min": None,
        "demand_index": msg.demand_index,
        "supply_index": msg.supply_index,
        "weather_factor": msg.weather_factor,
        "traffic_factor": msg.traffic_factor,
        "source": msg.source,
        "source_platform": "context_poller_public",
        "ts": msg.ts,
    }


async def main() -> None:
    stop_event = asyncio.Event()

    def _on_signal(*_: object) -> None:
        log.info("signal received, stopping cold consumer")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    DATA_PATH.mkdir(parents=True, exist_ok=True)
    EVENTS_PATH.mkdir(parents=True, exist_ok=True)

    topics = [COURIER_TOPIC, ORDER_OFFERS_TOPIC, ORDER_EVENTS_TOPIC, CONTEXT_SIGNALS_TOPIC]

    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        fetch_max_bytes=10_485_760,
        max_poll_records=2000,
    )

    for attempt in range(1, 11):
        try:
            await consumer.start()
            log.info("connected to topics=%s", topics)
            break
        except Exception as exc:  # pragma: no cover
            log.warning("attempt %d/10 failed: %s", attempt, exc)
            await asyncio.sleep(3)
    else:
        log.error("unable to connect to kafka")
        return

    positions_buffer: list[dict] = []
    events_buffer: list[dict] = []
    pending_offsets: dict[TopicPartition, int] = {}
    rejected_total = 0
    flushed_total = 0
    last_flush = time.monotonic()

    async def flush_and_commit(reason: str) -> None:
        nonlocal positions_buffer, events_buffer, pending_offsets, flushed_total, last_flush
        if not positions_buffer and not events_buffer:
            return

        if positions_buffer:
            write_positions(positions_buffer)
        if events_buffer:
            write_events(events_buffer)

        if pending_offsets:
            payload = {tp: OffsetAndMetadata(off + 1, "") for tp, off in pending_offsets.items()}
            await consumer.commit(payload)

        flushed_total += len(positions_buffer) + len(events_buffer)
        log.info(
            "flush[%s] positions=%d events=%d total_flushed=%d",
            reason,
            len(positions_buffer),
            len(events_buffer),
            flushed_total,
        )

        positions_buffer = []
        events_buffer = []
        pending_offsets = {}
        last_flush = time.monotonic()

    try:
        while not stop_event.is_set():
            raw_batch = await consumer.getmany(timeout_ms=500, max_records=2000)
            rejected_chunk: list[tuple[str, bytes, str]] = []

            for tp, msgs in raw_batch.items():
                for msg in msgs:
                    try:
                        if tp.topic == COURIER_TOPIC:
                            positions_buffer.append(parse_position(msg.value))
                        elif tp.topic == ORDER_OFFERS_TOPIC:
                            events_buffer.append(parse_offer(msg.value, tp.topic))
                        elif tp.topic == ORDER_EVENTS_TOPIC:
                            events_buffer.append(parse_order_event(msg.value, tp.topic))
                        elif tp.topic == CONTEXT_SIGNALS_TOPIC:
                            events_buffer.append(parse_context(msg.value, tp.topic))
                        else:
                            raise ValueError(f"unsupported topic={tp.topic}")
                    except Exception as exc:
                        rejected_chunk.append((tp.topic, msg.value, str(exc)))

                    prev = pending_offsets.get(tp, -1)
                    if msg.offset > prev:
                        pending_offsets[tp] = msg.offset

            if rejected_chunk:
                await asyncio.to_thread(write_dlq_jsonl, rejected_chunk)
                rejected_total += len(rejected_chunk)
                by_topic: dict[str, dict[str, int]] = {}
                for _topic, _raw, _reason in rejected_chunk:
                    by_topic.setdefault(_topic, {}).setdefault(_reason, 0)
                    by_topic[_topic][_reason] += 1
                for _topic, _reasons in by_topic.items():
                    for _reason, _count in _reasons.items():
                        log.warning(
                            "DLQ topic=%s reason=%r count=%d total_rejected=%d",
                            _topic, _reason, _count, rejected_total,
                        )

            current_size = len(positions_buffer) + len(events_buffer)
            time_exceeded = current_size > 0 and (time.monotonic() - last_flush >= BATCH_INTERVAL_S)
            size_exceeded = current_size >= MAX_BATCH_RECORDS
            if time_exceeded or size_exceeded:
                await flush_and_commit("size" if size_exceeded else "timer")

    finally:
        if positions_buffer or events_buffer:
            try:
                await flush_and_commit("shutdown")
            except Exception as exc:
                log.error("final flush failed, offsets not committed: %s", exc)

        await consumer.stop()
        log.info("cold consumer stopped flushed_total=%d rejected=%d", flushed_total, rejected_total)


if __name__ == "__main__":
    asyncio.run(main())
