"""
NYC TLC HVFHV (High Volume For-Hire Vehicle) trip replay service.

Downloads a monthly Uber trip parquet from NYC Taxi & Limousine Commission,
filters for Uber only (HV0003), sorts by request_datetime, and streams the
trips into the order-events-v1 Kafka topic at a configurable speed factor.

Each TLC trip becomes one OrderEventV1 with status="dropped_off" and the
real fare/distance/duration/zone values from the dataset.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import signal
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import httpx
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv

from copilot_events_pb2 import OrderEventV1

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("tlc-replay")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
ORDER_EVENTS_TOPIC = os.getenv("ORDER_EVENTS_TOPIC", "order-events-v1")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")

TLC_DATA_DIR = Path(os.getenv("TLC_DATA_DIR", "/data/tlc"))
TLC_MONTH = os.getenv("TLC_MONTH", "2024-01").strip()
TLC_BASE_URL = os.getenv(
    "TLC_BASE_URL",
    "https://d37ci6vzurychx.cloudfront.net/trip-data",
).rstrip("/")
TLC_LICENSE_FILTER = os.getenv("TLC_LICENSE_FILTER", "HV0003").strip()  # HV0003 = Uber
TLC_SPEED_FACTOR = float(os.getenv("TLC_SPEED_FACTOR", "60"))  # 1h real -> 1min wall
TLC_BATCH_SIZE = int(os.getenv("TLC_BATCH_SIZE", "500"))
TLC_LOOP_ON_FINISH = os.getenv("TLC_LOOP_ON_FINISH", "true").lower() in {"1", "true", "yes", "on"}

STATUS_KEY = "copilot:replay:tlc:status"
CURSOR_KEY = "copilot:replay:tlc:cursor"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_event_id(trip_uid: str) -> str:
    return f"tlc_evt_{hashlib.sha1(trip_uid.encode()).hexdigest()[:20]}"


def synth_courier_id(base_num: str | None, location_id: int, ts: str) -> str:
    """TLC anonymizes drivers — synthesize a stable id from (base, zone, day)."""
    base = (base_num or "B00000").strip() or "B00000"
    day = ts[:10] if len(ts) >= 10 else "1970-01-01"
    raw = f"{base}|{location_id}|{day}"
    return f"tlc_drv_{hashlib.sha1(raw.encode()).hexdigest()[:12]}"


def total_fare_usd(row: dict) -> float:
    """Sum the rider-paid components from a TLC row."""
    parts = [
        row.get("base_passenger_fare") or 0,
        row.get("tolls") or 0,
        row.get("bcf") or 0,
        row.get("sales_tax") or 0,
        row.get("congestion_surcharge") or 0,
        row.get("airport_fee") or 0,
    ]
    try:
        return max(0.0, float(sum(parts)))
    except (TypeError, ValueError):
        return 0.0


class TLCReplay:
    def __init__(self) -> None:
        self.producer: AIOKafkaProducer | None = None
        self.redis: aioredis.Redis | None = None
        self.sorted_path = TLC_DATA_DIR / f"hvfhv_sorted_{TLC_MONTH}.parquet"
        self.raw_path = TLC_DATA_DIR / f"fhvhv_tripdata_{TLC_MONTH}.parquet"
        self.duck = duckdb.connect(":memory:")

    async def start(self) -> None:
        TLC_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            compression_type="lz4",
            acks=1,
            linger_ms=5,
            max_batch_size=131_072,
        )
        await self.producer.start()
        self.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        await self._status(state="starting", message="connected_kafka_redis")

    async def close(self) -> None:
        if self.producer is not None:
            await self.producer.stop()
        if self.redis is not None:
            await self.redis.aclose()
        try:
            self.duck.close()
        except Exception:
            pass

    async def _status(self, **mapping) -> None:
        if self.redis is None:
            return
        payload = {k: str(v) for k, v in mapping.items()}
        payload["updated_at"] = utc_now_iso()
        payload["month"] = TLC_MONTH
        payload["speed_factor"] = str(TLC_SPEED_FACTOR)
        await self.redis.hset(STATUS_KEY, mapping=payload)
        await self.redis.expire(STATUS_KEY, 86400)

    async def _ensure_raw_parquet(self) -> bool:
        if self.raw_path.exists() and self.raw_path.stat().st_size > 1_000_000:
            log.info("raw parquet already on disk: %s (%.1f MB)", self.raw_path, self.raw_path.stat().st_size / 1e6)
            return True

        url = f"{TLC_BASE_URL}/fhvhv_tripdata_{TLC_MONTH}.parquet"
        log.info("downloading TLC HVFHV parquet from %s", url)
        await self._status(state="downloading", url=url)

        tmp_path = self.raw_path.with_suffix(".parquet.tmp")
        try:
            async with httpx.AsyncClient(timeout=600, follow_redirects=True) as client:
                async with client.stream("GET", url) as resp:
                    if resp.status_code != 200:
                        body = (await resp.aread()).decode("utf-8", errors="replace")[:300]
                        log.error("download failed status=%d body=%s", resp.status_code, body)
                        await self._status(state="error", reason=f"download_http_{resp.status_code}")
                        return False
                    total = int(resp.headers.get("content-length", "0"))
                    received = 0
                    last_log = 0
                    with tmp_path.open("wb") as f:
                        async for chunk in resp.aiter_bytes(chunk_size=1024 * 256):
                            f.write(chunk)
                            received += len(chunk)
                            if received - last_log > 25 * 1024 * 1024:
                                pct = (received / total * 100) if total else 0
                                log.info("download progress: %.1f MB (%.0f%%)", received / 1e6, pct)
                                last_log = received
            tmp_path.rename(self.raw_path)
            log.info("download complete: %.1f MB -> %s", self.raw_path.stat().st_size / 1e6, self.raw_path)
            return True
        except Exception as exc:
            log.error("download error: %s", exc)
            await self._status(state="error", reason=f"download_{type(exc).__name__}")
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            return False

    def _ensure_sorted_parquet(self) -> int:
        """Filter Uber-only and sort by request_datetime, write to a new parquet."""
        if self.sorted_path.exists() and self.sorted_path.stat().st_size > 100_000:
            count = self.duck.execute(
                f"SELECT COUNT(*) FROM read_parquet('{self.sorted_path.as_posix()}')"
            ).fetchone()[0]
            log.info("sorted parquet ready: %s (%d rows)", self.sorted_path, count)
            return int(count)

        log.info("preprocessing: filter %s + sort by request_datetime", TLC_LICENSE_FILTER)
        self.duck.execute(
            f"""
            COPY (
                SELECT
                    hvfhs_license_num,
                    dispatching_base_num,
                    request_datetime,
                    pickup_datetime,
                    dropoff_datetime,
                    PULocationID,
                    DOLocationID,
                    trip_miles,
                    trip_time,
                    base_passenger_fare,
                    tolls,
                    bcf,
                    sales_tax,
                    congestion_surcharge,
                    airport_fee,
                    tips,
                    driver_pay
                FROM read_parquet('{self.raw_path.as_posix()}')
                WHERE hvfhs_license_num = '{TLC_LICENSE_FILTER}'
                  AND request_datetime IS NOT NULL
                  AND dropoff_datetime IS NOT NULL
                ORDER BY request_datetime
            ) TO '{self.sorted_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
            """
        )
        count = self.duck.execute(
            f"SELECT COUNT(*) FROM read_parquet('{self.sorted_path.as_posix()}')"
        ).fetchone()[0]
        log.info("sorted parquet written: %d rows", count)
        return int(count)

    async def _load_cursor(self) -> int:
        if self.redis is None:
            return 0
        val = await self.redis.hget(CURSOR_KEY, TLC_MONTH)
        try:
            return int(val) if val else 0
        except (TypeError, ValueError):
            return 0

    async def _save_cursor(self, offset: int) -> None:
        if self.redis is None:
            return
        await self.redis.hset(CURSOR_KEY, TLC_MONTH, str(offset))
        await self.redis.expire(CURSOR_KEY, 30 * 86400)

    def _row_to_event(self, row: dict) -> OrderEventV1 | None:
        pickup_ts = row.get("pickup_datetime")
        dropoff_ts = row.get("dropoff_datetime")
        request_ts = row.get("request_datetime")
        if dropoff_ts is None or request_ts is None:
            return None

        ts_iso = dropoff_ts.replace(tzinfo=timezone.utc).isoformat() if hasattr(dropoff_ts, "replace") else str(dropoff_ts)

        pu_loc = int(row.get("PULocationID") or 0)
        do_loc = int(row.get("DOLocationID") or 0)
        zone_id = f"nyc_{pu_loc}" if pu_loc else "nyc_unknown"

        trip_miles = float(row.get("trip_miles") or 0)
        trip_time_sec = float(row.get("trip_time") or 0)

        order_id = f"tlc_{request_ts.timestamp():.0f}_{pu_loc}_{do_loc}_{int(trip_time_sec)}" if hasattr(request_ts, "timestamp") else f"tlc_{pu_loc}_{do_loc}_{int(trip_time_sec)}"

        courier_id = synth_courier_id(row.get("dispatching_base_num"), pu_loc, ts_iso)
        fare_usd = total_fare_usd(row)

        return OrderEventV1(
            event_id=make_event_id(order_id + ts_iso),
            event_type="order.event.v1",
            ts=ts_iso,
            offer_id=f"tlc_offer_{order_id}",
            order_id=order_id,
            courier_id=courier_id,
            status="dropped_off",
            actual_fare_eur=fare_usd,  # field name kept for proto compat; values are USD
            actual_distance_km=round(trip_miles * 1.60934, 3),
            actual_duration_min=round(trip_time_sec / 60.0, 2),
            zone_id=zone_id,
        )

    async def replay(self, total_rows: int) -> None:
        cursor = await self._load_cursor()
        log.info("replay starting at cursor=%d / total=%d (speed_factor=%.1fx)", cursor, total_rows, TLC_SPEED_FACTOR)

        sorted_uri = self.sorted_path.as_posix()
        emitted = 0
        last_event_dt: datetime | None = None
        last_wall = asyncio.get_event_loop().time()

        while cursor < total_rows:
            rows = self.duck.execute(
                f"SELECT * FROM read_parquet('{sorted_uri}') LIMIT {TLC_BATCH_SIZE} OFFSET {cursor}"
            ).fetchall()
            if not rows:
                break

            cols = [d[0] for d in self.duck.description]

            for raw in rows:
                row = dict(zip(cols, raw))
                event = self._row_to_event(row)
                if event is None:
                    cursor += 1
                    continue

                event_dt = row.get("dropoff_datetime")
                if isinstance(event_dt, datetime):
                    if last_event_dt is not None:
                        delta_real = (event_dt - last_event_dt).total_seconds()
                        if delta_real > 0:
                            wall_sleep = delta_real / TLC_SPEED_FACTOR
                            elapsed = asyncio.get_event_loop().time() - last_wall
                            sleep_for = max(0.0, wall_sleep - elapsed)
                            if sleep_for > 0:
                                await asyncio.sleep(min(sleep_for, 5.0))
                    last_event_dt = event_dt
                    last_wall = asyncio.get_event_loop().time()

                assert self.producer is not None
                await self.producer.send(
                    ORDER_EVENTS_TOPIC,
                    key=event.order_id.encode("utf-8", errors="ignore"),
                    value=event.SerializeToString(),
                )
                emitted += 1
                cursor += 1

            await self._save_cursor(cursor)
            await self._status(
                state="running",
                cursor=cursor,
                total=total_rows,
                emitted=emitted,
                progress_pct=round(cursor / max(total_rows, 1) * 100, 2),
                topic=ORDER_EVENTS_TOPIC,
            )
            log.info("replay progress cursor=%d/%d emitted=%d (%.1f%%)", cursor, total_rows, emitted, cursor / total_rows * 100)

        log.info("replay finished: emitted=%d", emitted)
        await self._status(state="finished", emitted=emitted, total=total_rows)

        if TLC_LOOP_ON_FINISH:
            log.info("loop_on_finish=true → resetting cursor and restarting")
            await self._save_cursor(0)


async def main() -> None:
    stop_event = asyncio.Event()

    def _on_signal(*_) -> None:
        log.info("signal received, stopping replay")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    replay = TLCReplay()
    for attempt in range(1, 11):
        try:
            await replay.start()
            log.info("tlc replay started month=%s license=%s speed=%.1fx", TLC_MONTH, TLC_LICENSE_FILTER, TLC_SPEED_FACTOR)
            break
        except Exception as exc:
            log.warning("start attempt %d/10 failed: %s", attempt, exc)
            await asyncio.sleep(3)
    else:
        log.error("unable to start tlc replay")
        return

    try:
        while not stop_event.is_set():
            ok = await replay._ensure_raw_parquet()
            if not ok:
                await asyncio.sleep(60)
                continue

            try:
                total_rows = await asyncio.to_thread(replay._ensure_sorted_parquet)
            except Exception as exc:
                log.error("preprocessing failed: %s", exc)
                await replay._status(state="error", reason=f"preprocess_{type(exc).__name__}")
                await asyncio.sleep(60)
                continue

            if total_rows <= 0:
                log.warning("sorted parquet empty — sleeping before retry")
                await asyncio.sleep(300)
                continue

            await replay.replay(total_rows)

            if not TLC_LOOP_ON_FINISH:
                log.info("loop_on_finish=false → idle until restart")
                while not stop_event.is_set():
                    await asyncio.sleep(60)
    finally:
        await replay.close()
        log.info("tlc replay stopped")


if __name__ == "__main__":
    asyncio.run(main())
