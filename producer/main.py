"""
FleetStream producer with Copilot v1 events.

Publishes protobuf payloads to:
- courier positions (hot path)
- order offers
- order events
- context signals
"""
import asyncio
import logging
import math
import os
import random
import signal
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv

from copilot_events_pb2 import ContextSignalV1, CourierPositionV1, OrderEventV1, OrderOfferV1

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("producer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
COURIER_TOPIC = os.getenv("KAFKA_TOPIC", "livreurs-gps")
ORDER_OFFERS_TOPIC = os.getenv("ORDER_OFFERS_TOPIC", "order-offers-v1")
ORDER_EVENTS_TOPIC = os.getenv("ORDER_EVENTS_TOPIC", "order-events-v1")
CONTEXT_SIGNALS_TOPIC = os.getenv("CONTEXT_SIGNALS_TOPIC", "context-signals-v1")

NUM_COURIERS = int(os.getenv("NUM_LIVREURS", "100"))
EMIT_INTERVAL_MS = int(os.getenv("EMIT_INTERVAL_MS", "1000"))
OFFER_PROBABILITY = float(os.getenv("OFFER_PROBABILITY", "0.14"))

NYC_LAT = 40.7580
NYC_LON = -73.9855
NYC_RADIUS_KM = 18.0
ZONE_STEP = 0.02

HOTSPOTS = [
    (40.7580, -73.9855),  # Times Square
    (40.7484, -73.9857),  # Empire State / Midtown
    (40.7128, -74.0060),  # Lower Manhattan / FiDi
    (40.7831, -73.9712),  # Upper East Side
    (40.7794, -73.9632),  # Upper East Side / 86th
    (40.7505, -73.9934),  # Penn Station / Chelsea
    (40.7411, -74.0018),  # Meatpacking / West Village
    (40.7282, -73.9942),  # SoHo / NoHo
    (40.6782, -73.9442),  # Brooklyn Heights / Williamsburg edge
    (40.7589, -73.9851),  # Times Square (cluster)
    (40.6892, -74.0445),  # Statue area / Battery
    (40.7421, -73.9890),  # Flatiron / NoMad
    (40.7589, -73.9441),  # Roosevelt Island
    (40.7282, -73.7949),  # JFK approach / Queens
    (40.7769, -73.8740),  # LaGuardia approach
]

STATUS_WEIGHTS = {"delivering": 0.58, "available": 0.34, "idle": 0.08}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def event_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:20]}"


def zone_id(lat: float, lon: float) -> str:
    lat_cell = round(round(lat / ZONE_STEP) * ZONE_STEP, 3)
    lon_cell = round(round(lon / ZONE_STEP) * ZONE_STEP, 3)
    return f"{lat_cell:.3f}_{lon_cell:.3f}"


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return r * 2 * math.asin(math.sqrt(a))


@dataclass
class Courier:
    courier_id: str
    lat: float
    lon: float
    speed_kmh: float
    heading_deg: float
    status: str
    battery_pct: float
    status_ttl: float

    @classmethod
    def spawn(cls, idx: int) -> "Courier":
        hotspot_lat, hotspot_lon = random.choice(HOTSPOTS)
        angle = random.uniform(0, 2 * math.pi)
        dist_km = random.uniform(0.1, 1.5)
        lat = hotspot_lat + (dist_km / 111.32) * math.cos(angle)
        lon = hotspot_lon + (dist_km / (111.32 * math.cos(math.radians(hotspot_lat)))) * math.sin(angle)
        status = random.choices(list(STATUS_WEIGHTS), weights=list(STATUS_WEIGHTS.values()))[0]
        return cls(
            courier_id=f"L{idx:03d}",
            lat=round(lat, 6),
            lon=round(lon, 6),
            speed_kmh=random.uniform(12.0, 28.0),
            heading_deg=random.uniform(0, 360),
            status=status,
            battery_pct=round(random.uniform(40.0, 100.0), 1),
            status_ttl=random.uniform(30, 180),
        )

    def tick(self, dt: float) -> None:
        self.heading_deg = (self.heading_deg + random.gauss(0, 8)) % 360
        target = {
            "delivering": random.uniform(18, 35),
            "available": random.uniform(8, 20),
            "idle": random.uniform(0, 4),
        }
        self.speed_kmh = 0.86 * self.speed_kmh + 0.14 * target[self.status]

        v_ms = self.speed_kmh / 3.6
        hr = math.radians(self.heading_deg)
        dlat = (v_ms * math.cos(hr) * dt) / 111_320
        dlon = (v_ms * math.sin(hr) * dt) / (111_320 * max(math.cos(math.radians(self.lat)), 0.01))
        self.lat = round(self.lat + dlat, 6)
        self.lon = round(self.lon + dlon, 6)

        dlat_c = self.lat - NYC_LAT
        dlon_c = self.lon - NYC_LON
        dist_km = math.hypot(dlat_c * 111.32, dlon_c * 111.32 * math.cos(math.radians(self.lat)))
        if dist_km > NYC_RADIUS_KM:
            self.heading_deg = (math.degrees(math.atan2(-dlon_c, -dlat_c)) + random.gauss(0, 15)) % 360

        drain = 0.03 if self.status == "delivering" else 0.015 if self.status == "available" else 0.005
        self.battery_pct = round(max(5.0, self.battery_pct - drain * dt + random.gauss(0, 0.005)), 1)

        self.status_ttl -= dt
        if self.status_ttl <= 0:
            self.status = random.choices(list(STATUS_WEIGHTS), weights=list(STATUS_WEIGHTS.values()))[0]
            self.status_ttl = random.uniform(30, 240)

    def position_message(self) -> CourierPositionV1:
        return CourierPositionV1(
            event_id=event_id("pos"),
            event_type="courier.position.v1",
            ts=utc_now_iso(),
            courier_id=self.courier_id,
            lat=self.lat,
            lon=self.lon,
            speed_kmh=round(self.speed_kmh, 1),
            heading_deg=round(self.heading_deg % 360, 1),
            status=self.status,
            accuracy_m=round(random.uniform(3.0, 12.0), 1),
            battery_pct=self.battery_pct,
        )


def random_dropoff(pickup_lat: float, pickup_lon: float) -> tuple[float, float]:
    angle = random.uniform(0, 2 * math.pi)
    distance_km = random.uniform(0.8, 5.8)
    lat = pickup_lat + (distance_km / 111.32) * math.cos(angle)
    lon = pickup_lon + (distance_km / (111.32 * max(math.cos(math.radians(pickup_lat)), 0.01))) * math.sin(angle)
    return round(lat, 6), round(lon, 6)


def build_offer(courier: Courier) -> OrderOfferV1:
    pickup_lat = courier.lat + random.uniform(-0.005, 0.005)
    pickup_lon = courier.lon + random.uniform(-0.005, 0.005)
    dropoff_lat, dropoff_lon = random_dropoff(pickup_lat, pickup_lon)

    est_distance_km = max(0.4, haversine_km(pickup_lat, pickup_lon, dropoff_lat, dropoff_lon) * random.uniform(1.05, 1.3))
    traffic_factor = random.uniform(0.8, 1.35)
    weather_factor = random.uniform(0.85, 1.2)
    demand_index = random.uniform(0.7, 1.8)

    est_duration_min = max(4.0, (est_distance_km / max(12.0, 20.0 / traffic_factor)) * 60)
    est_fare = max(4.5, 2.1 + est_distance_km * 1.9 + est_duration_min * 0.22)
    est_fare *= (0.82 + 0.18 * demand_index) * weather_factor

    return OrderOfferV1(
        event_id=event_id("offer"),
        event_type="order.offer.v1",
        ts=utc_now_iso(),
        offer_id=event_id("ofr"),
        courier_id=courier.courier_id,
        pickup_lat=pickup_lat,
        pickup_lon=pickup_lon,
        dropoff_lat=dropoff_lat,
        dropoff_lon=dropoff_lon,
        estimated_fare_eur=round(est_fare, 2),
        estimated_distance_km=round(est_distance_km, 3),
        estimated_duration_min=round(est_duration_min, 2),
        demand_index=round(demand_index, 3),
        weather_factor=round(weather_factor, 3),
        traffic_factor=round(traffic_factor, 3),
        zone_id=zone_id(pickup_lat, pickup_lon),
    )


def accepted_probability(offer: OrderOfferV1, courier: Courier) -> float:
    gross_per_hour = (offer.estimated_fare_eur / max(offer.estimated_duration_min, 1.0)) * 60
    demand_bonus = (offer.demand_index - 1.0) * 0.25
    weather_penalty = (1.0 - offer.weather_factor) * 0.2
    battery_penalty = 0.15 if courier.battery_pct < 15 else 0.0
    score = (gross_per_hour - 16.0) / 10.0 + demand_bonus - weather_penalty - battery_penalty
    prob = 1.0 / (1.0 + math.exp(-score))
    return max(0.03, min(prob, 0.97))


def build_order_events(offer: OrderOfferV1, accepted: bool) -> list[OrderEventV1]:
    order_id = event_id("ord")
    accepted_or_rejected = OrderEventV1(
        event_id=event_id("evt"),
        event_type="order.event.v1",
        ts=utc_now_iso(),
        offer_id=offer.offer_id,
        order_id=order_id,
        courier_id=offer.courier_id,
        status="accepted" if accepted else "rejected",
        actual_fare_eur=0.0,
        actual_distance_km=0.0,
        actual_duration_min=0.0,
        zone_id=offer.zone_id,
    )
    if not accepted:
        return [accepted_or_rejected]

    actual_distance = max(0.2, offer.estimated_distance_km * random.uniform(0.92, 1.18))
    actual_duration = max(2.0, offer.estimated_duration_min * random.uniform(0.9, 1.25))
    actual_fare = max(3.5, offer.estimated_fare_eur * random.uniform(0.93, 1.2))

    completed = OrderEventV1(
        event_id=event_id("evt"),
        event_type="order.event.v1",
        ts=utc_now_iso(),
        offer_id=offer.offer_id,
        order_id=order_id,
        courier_id=offer.courier_id,
        status="dropped_off",
        actual_fare_eur=round(actual_fare, 2),
        actual_distance_km=round(actual_distance, 3),
        actual_duration_min=round(actual_duration, 2),
        zone_id=offer.zone_id,
    )
    return [accepted_or_rejected, completed]


def build_context_signals(couriers: list[Courier]) -> list[ContextSignalV1]:
    zone_supply: dict[str, int] = {}
    for courier in couriers:
        zid = zone_id(courier.lat, courier.lon)
        zone_supply[zid] = zone_supply.get(zid, 0) + 1

    signals: list[ContextSignalV1] = []
    for zid, supply in list(zone_supply.items())[:80]:
        demand = random.uniform(0.7, 2.4) * (1.0 + random.uniform(-0.05, 0.2))
        weather = random.uniform(0.85, 1.15)
        traffic = random.uniform(0.8, 1.3)
        signals.append(
            ContextSignalV1(
                event_id=event_id("ctx"),
                event_type="context.signal.v1",
                ts=utc_now_iso(),
                zone_id=zid,
                demand_index=round(demand, 3),
                supply_index=float(max(1, supply)),
                weather_factor=round(weather, 3),
                traffic_factor=round(traffic, 3),
                source="synthetic-marketplace",
            )
        )
    return signals


async def produce_loop(producer: AIOKafkaProducer, couriers: list[Courier], stop_event: asyncio.Event) -> None:
    interval = EMIT_INTERVAL_MS / 1000.0
    tick = 0

    while not stop_event.is_set():
        t0 = asyncio.get_event_loop().time()
        sends = []

        for courier in couriers:
            courier.tick(interval)
            pos = courier.position_message()
            sends.append(producer.send(COURIER_TOPIC, key=courier.courier_id.encode(), value=pos.SerializeToString()))

            if random.random() < OFFER_PROBABILITY and courier.status != "idle":
                offer = build_offer(courier)
                sends.append(producer.send(ORDER_OFFERS_TOPIC, key=courier.courier_id.encode(), value=offer.SerializeToString()))

                accepted = random.random() < accepted_probability(offer, courier)
                for evt in build_order_events(offer, accepted):
                    sends.append(producer.send(ORDER_EVENTS_TOPIC, key=courier.courier_id.encode(), value=evt.SerializeToString()))

        if tick % 5 == 0:
            for signal_msg in build_context_signals(couriers):
                sends.append(producer.send(CONTEXT_SIGNALS_TOPIC, key=signal_msg.zone_id.encode(), value=signal_msg.SerializeToString()))

        await asyncio.gather(*sends)

        tick += 1
        if tick % 30 == 0:
            elapsed = asyncio.get_event_loop().time() - t0
            msg_per_tick = len(sends)
            log.info(
                "tick=%d couriers=%d events_sent=%d rate=%.0f msg/s",
                tick,
                len(couriers),
                msg_per_tick,
                msg_per_tick / max(elapsed, 0.001),
            )

        elapsed = asyncio.get_event_loop().time() - t0
        await asyncio.sleep(max(0.0, interval - elapsed))


async def main() -> None:
    stop_event = asyncio.Event()

    def _on_signal(*_: object) -> None:
        log.info("signal received, stopping producer")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    couriers = [Courier.spawn(i) for i in range(NUM_COURIERS)]
    log.info("initialized simulation with %d couriers", len(couriers))

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        compression_type="lz4",
        linger_ms=5,
        max_batch_size=131_072,
        acks=1,
    )

    for attempt in range(1, 11):
        try:
            await producer.start()
            log.info("connected to kafka bootstrap=%s", KAFKA_BOOTSTRAP)
            break
        except Exception as exc:  # pragma: no cover - runtime retry path
            log.warning("attempt %d/10 failed: %s", attempt, exc)
            await asyncio.sleep(3)
    else:
        log.error("unable to connect to kafka after retries")
        return

    try:
        await produce_loop(producer, couriers, stop_event)
    finally:
        await producer.stop()
        log.info("producer stopped cleanly")


if __name__ == "__main__":
    asyncio.run(main())
