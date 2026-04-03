"""
FleetStream — GPS Producer
==========================
Simule NUM_LIVREURS livreurs se déplaçant de façon réaliste dans Paris.

Modèle de mouvement :
  - Marche aléatoire persistante (cap + vitesse lissés via filtre passe-bas)
  - Départ depuis les hotspots restaurants de Paris
  - 3 statuts : delivering (65%), available (25%), idle (10%)
  - Rappel vers le centre si le livreur sort de la zone (~18km de diamètre)

Throughput cible : 100 msg/s (1 msg/livreur/seconde)
"""
import asyncio
import json
import logging
import math
import os
import random
import signal
from dataclasses import dataclass
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("producer")

# ── Config ──────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "livreurs-gps")
NUM_LIVREURS = int(os.getenv("NUM_LIVREURS", "100"))
EMIT_INTERVAL_MS = int(os.getenv("EMIT_INTERVAL_MS", "1000"))

# ── Géographie Paris ────────────────────────────────────────────────────────────
PARIS_LAT = 48.8566
PARIS_LON = 2.3522
PARIS_RADIUS_KM = 9.0   # ~18km de diamètre, couvre le périphérique

# Hotspots restaurants/livraison — les livreurs démarrent autour de ces zones
HOTSPOTS: list[tuple[float, float]] = [
    (48.8738, 2.2950),  # La Défense
    (48.8848, 2.3440),  # Montmartre
    (48.8602, 2.3477),  # Châtelet / Les Halles
    (48.8533, 2.3692),  # Bastille
    (48.8416, 2.3219),  # Montparnasse
    (48.8672, 2.3631),  # République
    (48.8726, 2.3323),  # Pigalle / 9e
    (48.8640, 2.3820),  # Nation
    (48.8455, 2.3726),  # Bercy / 12e
    (48.8808, 2.3556),  # Sacré-Cœur
    (48.8650, 2.3290),  # Opéra / 2e
    (48.8490, 2.3060),  # Invalides / 7e
]

STATUS_WEIGHTS = {"delivering": 0.65, "available": 0.25, "idle": 0.10}


@dataclass
class Livreur:
    livreur_id: str
    lat: float
    lon: float
    speed_kmh: float
    heading_deg: float
    status: str
    accuracy_m: float
    battery_pct: float
    _status_ttl: float  # secondes avant prochain changement de statut

    @classmethod
    def spawn(cls, idx: int) -> "Livreur":
        """Initialise un livreur proche d'un hotspot aléatoire."""
        hotspot = random.choice(HOTSPOTS)
        angle = random.uniform(0, 2 * math.pi)
        dist_km = random.uniform(0.1, 1.5)
        lat = hotspot[0] + (dist_km / 111.32) * math.cos(angle)
        lon = hotspot[1] + (dist_km / (111.32 * math.cos(math.radians(hotspot[0])))) * math.sin(angle)
        status = random.choices(list(STATUS_WEIGHTS), weights=list(STATUS_WEIGHTS.values()))[0]
        return cls(
            livreur_id=f"L{idx:03d}",
            lat=round(lat, 6),
            lon=round(lon, 6),
            speed_kmh=random.uniform(12.0, 28.0),
            heading_deg=random.uniform(0, 360),
            status=status,
            accuracy_m=round(random.uniform(3.0, 8.0), 1),
            battery_pct=round(random.uniform(40.0, 100.0), 1),
            _status_ttl=random.uniform(30, 180),
        )

    def tick(self, dt: float = 1.0) -> None:
        """Avance la simulation de dt secondes."""
        # Cap : marche aléatoire avec inertie (gyroscope)
        self.heading_deg = (self.heading_deg + random.gauss(0, 8)) % 360

        # Vitesse cible selon statut, lissée via filtre passe-bas
        target: dict[str, float] = {
            "delivering": random.uniform(18, 35),
            "available":  random.uniform(8, 20),
            "idle":       random.uniform(0, 4),
        }
        self.speed_kmh = 0.85 * self.speed_kmh + 0.15 * target[self.status]

        # Mise à jour de position (approximation équirectangulaire)
        v_ms = self.speed_kmh / 3.6
        hr = math.radians(self.heading_deg)
        dlat = (v_ms * math.cos(hr) * dt) / 111_320
        dlon = (v_ms * math.sin(hr) * dt) / (111_320 * math.cos(math.radians(self.lat)))
        self.lat = round(self.lat + dlat, 6)
        self.lon = round(self.lon + dlon, 6)

        # Rappel vers le centre si hors de la zone Paris
        dlat_c = self.lat - PARIS_LAT
        dlon_c = self.lon - PARIS_LON
        dist_km = math.hypot(
            dlat_c * 111.32,
            dlon_c * 111.32 * math.cos(math.radians(self.lat)),
        )
        if dist_km > PARIS_RADIUS_KM:
            # Redirige vers le centre avec un léger bruit
            self.heading_deg = (
                math.degrees(math.atan2(-dlon_c, -dlat_c)) + random.gauss(0, 15)
            ) % 360

        # Bruit GPS
        self.accuracy_m = round(random.uniform(3.0, 12.0), 1)

        # Batterie : décharge ~0.02%/tick (delivering consomme plus)
        drain = 0.03 if self.status == "delivering" else 0.015 if self.status == "available" else 0.005
        self.battery_pct = round(max(5.0, self.battery_pct - drain * dt + random.gauss(0, 0.005)), 1)

        # Rotation de statut
        self._status_ttl -= dt
        if self._status_ttl <= 0:
            self.status = random.choices(
                list(STATUS_WEIGHTS), weights=list(STATUS_WEIGHTS.values())
            )[0]
            self._status_ttl = random.uniform(30, 240)

    def to_payload(self) -> bytes:
        return json.dumps(
            {
                "livreur_id": self.livreur_id,
                "lat": self.lat,
                "lon": self.lon,
                "speed_kmh": round(self.speed_kmh, 1),
                "heading_deg": round(self.heading_deg % 360, 1),
                "status": self.status,
                "accuracy_m": self.accuracy_m,
                "battery_pct": self.battery_pct,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            ensure_ascii=False,
        ).encode()


async def produce_loop(
    producer: AIOKafkaProducer,
    livreurs: list[Livreur],
    stop_event: asyncio.Event,
) -> None:
    interval = EMIT_INTERVAL_MS / 1000.0
    tick = 0

    while not stop_event.is_set():
        t0 = asyncio.get_event_loop().time()

        # Avance chaque livreur et envoie son message en parallèle
        sends = [
            producer.send(
                TOPIC,
                key=lv.livreur_id.encode(),
                value=lv.to_payload(),
            )
            for lv in livreurs
            if not lv.tick(interval) or True  # tick() retourne None, toujours True
        ]
        await asyncio.gather(*sends)

        tick += 1
        if tick % 60 == 0:
            elapsed = asyncio.get_event_loop().time() - t0
            log.info(
                "tick=%d | %d livreurs | %.0f msg/s (batch %.1f ms)",
                tick,
                len(livreurs),
                len(livreurs) / max(elapsed, 0.001),
                elapsed * 1000,
            )

        # Respecte l'intervalle cible
        elapsed = asyncio.get_event_loop().time() - t0
        await asyncio.sleep(max(0.0, interval - elapsed))


async def main() -> None:
    stop_event = asyncio.Event()

    def _on_signal(*_: object) -> None:
        log.info("Signal reçu — arrêt du producer...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    livreurs = [Livreur.spawn(i) for i in range(NUM_LIVREURS)]
    log.info("Simulation initialisée : %d livreurs dans Paris", len(livreurs))

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        compression_type="lz4",   # Compression LZ4 — excellent ratio perf/CPU
        linger_ms=5,               # Agrège les messages pendant 5ms pour le batching
        max_batch_size=65_536,     # 64KB par batch Kafka
        acks=1,                    # Leader ack — bon compromis durabilité/latence
    )

    for attempt in range(1, 11):
        try:
            await producer.start()
            log.info("Connecté à Kafka : %s", KAFKA_BOOTSTRAP)
            break
        except Exception as exc:
            log.warning("Tentative %d/10 — %s. Nouvel essai dans 3s...", attempt, exc)
            await asyncio.sleep(3)
    else:
        log.error("Impossible de se connecter à Kafka après 10 tentatives. Abandon.")
        return

    try:
        await produce_loop(producer, livreurs, stop_event)
    finally:
        await producer.stop()
        log.info("Producer arrêté proprement.")


if __name__ == "__main__":
    asyncio.run(main())
