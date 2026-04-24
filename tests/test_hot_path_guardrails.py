"""Unit tests for hot-path guardrails and recent-track storage."""

from __future__ import annotations

import asyncio
import importlib.util
import json
from pathlib import Path


_HOT_MAIN_PATH = Path(__file__).resolve().parent.parent / "hot_path" / "main.py"
_SPEC = importlib.util.spec_from_file_location("hot_path_main", _HOT_MAIN_PATH)
assert _SPEC and _SPEC.loader
hot_main = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(hot_main)


class _FakePipeline:
    def __init__(self, redis: "_FakeRedis") -> None:
        self.redis = redis
        self.ops: list[tuple] = []

    def hgetall(self, key: str):
        self.ops.append(("hgetall", key))
        return self

    def lrange(self, key: str, start: int, end: int):
        self.ops.append(("lrange", key, start, end))
        return self

    def geoadd(self, key: str, values: list):
        self.ops.append(("geoadd", key, values))
        return self

    def zadd(self, key: str, mapping: dict[str, int]):
        self.ops.append(("zadd", key, mapping))
        return self

    def hset(self, key: str, mapping: dict):
        self.ops.append(("hset", key, mapping))
        return self

    def expire(self, key: str, seconds: int):
        self.ops.append(("expire", key, seconds))
        return self

    def lpush(self, key: str, value: str):
        self.ops.append(("lpush", key, value))
        return self

    def ltrim(self, key: str, start: int, end: int):
        self.ops.append(("ltrim", key, start, end))
        return self

    def incrby(self, key: str, amount: int):
        self.ops.append(("incrby", key, amount))
        return self

    async def execute(self):
        out = []
        for op in self.ops:
            name = op[0]
            if name == "hgetall":
                out.append(dict(self.redis.hashes.get(op[1], {})))
            elif name == "lrange":
                seq = list(self.redis.lists.get(op[1], []))
                start = int(op[2])
                end = int(op[3])
                if end < 0:
                    out.append(seq[start:])
                else:
                    out.append(seq[start : end + 1])
            elif name == "geoadd":
                _name, key, values = op
                lon, lat, courier_id = values
                self.redis.geo[key] = self.redis.geo.get(key, {})
                self.redis.geo[key][courier_id] = (float(lon), float(lat))
                out.append(1)
            elif name == "zadd":
                _name, key, mapping = op
                self.redis.sorted_sets[key] = self.redis.sorted_sets.get(key, {})
                self.redis.sorted_sets[key].update(mapping)
                out.append(1)
            elif name == "hset":
                _name, key, mapping = op
                self.redis.hashes.setdefault(key, {})
                self.redis.hashes[key].update(mapping)
                out.append(1)
            elif name == "expire":
                out.append(True)
            elif name == "lpush":
                _name, key, value = op
                self.redis.lists.setdefault(key, [])
                self.redis.lists[key].insert(0, value)
                out.append(len(self.redis.lists[key]))
            elif name == "ltrim":
                _name, key, start, end = op
                seq = self.redis.lists.get(key, [])
                self.redis.lists[key] = seq[start : end + 1]
                out.append(True)
            elif name == "incrby":
                _name, key, amount = op
                self.redis.kv[key] = int(self.redis.kv.get(key, 0)) + int(amount)
                out.append(self.redis.kv[key])
            else:  # pragma: no cover - defensive
                raise AssertionError(f"unsupported op: {op}")
        return out


class _FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict] = {}
        self.lists: dict[str, list[str]] = {}
        self.geo: dict[str, dict[str, tuple[float, float]]] = {}
        self.sorted_sets: dict[str, dict[str, int]] = {}
        self.kv: dict[str, int] = {}

    def pipeline(self, transaction: bool = False):  # noqa: ARG002
        return _FakePipeline(self)


def _make_position(
    *,
    courier_id: str = "drv_demo_001",
    lat: float = 40.7580,
    lon: float = -73.9855,
    ts: str = "2024-01-01T12:00:00+00:00",
    status: str = "delivering",
    speed_kmh: float = 20.0,
    source_platform: str = "tlc_hvfhv|route=osrm",
) -> hot_main.Position:
    return hot_main.Position(
        courier_id=courier_id,
        lat=lat,
        lon=lon,
        speed_kmh=speed_kmh,
        heading_deg=90.0,
        status=status,
        accuracy_m=5.0,
        battery_pct=90.0,
        ts=ts,
        source_platform=source_platform,
    )


def test_parse_position_accepts_repositioning_and_source_platform():
    msg = hot_main.CourierPositionV1()
    msg.courier_id = "drv_demo_001"
    msg.lat = 40.75
    msg.lon = -73.98
    msg.speed_kmh = 12.0
    msg.heading_deg = 180.0
    msg.status = "repositioning"
    msg.ts = "2024-01-01T12:00:00+00:00"
    msg.source_platform = "tlc_hvfhv|route=hold"

    pos, reject = hot_main.parse_position(msg.SerializeToString())
    assert reject is None
    assert pos is not None
    assert pos.status == "repositioning"
    assert pos.source_platform.endswith("route=hold")


def test_parse_position_accepts_pickup_en_route_status():
    msg = hot_main.CourierPositionV1()
    msg.courier_id = "drv_demo_pickup"
    msg.lat = 40.75
    msg.lon = -73.98
    msg.speed_kmh = 9.0
    msg.heading_deg = 180.0
    msg.status = "pickup_en_route"
    msg.ts = "2024-01-01T12:00:00+00:00"
    msg.source_platform = "driver_ingest"

    pos, reject = hot_main.parse_position(msg.SerializeToString())

    assert reject is None
    assert pos is not None
    assert pos.status == "pickup_en_route"


def test_flush_to_redis_stores_recent_track_and_quality_fields():
    redis = _FakeRedis()
    pos = _make_position(source_platform="tlc_hvfhv|route=hold")

    accepted, rejects = asyncio.run(hot_main.flush_to_redis(redis, [pos]))
    assert accepted == 1
    assert rejects == []

    payload = redis.hashes[f"{hot_main.HASH_PREFIX}{pos.courier_id}"]
    assert payload["route_source"] == "hold"
    assert payload["stale_reason"] == "routing_hold"
    assert payload["anomaly_state"] == "ok"

    raw_track = redis.lists[f"{hot_main.TRACK_KEY_PREFIX}{pos.courier_id}"][0]
    track_point = json.loads(raw_track)
    assert track_point["route_source"] == "hold"
    assert track_point["lat"] == round(pos.lat, 6)


def test_flush_to_redis_rejects_non_monotonic_timestamp_and_marks_hash():
    redis = _FakeRedis()
    courier_id = "drv_demo_002"
    redis.hashes[f"{hot_main.HASH_PREFIX}{courier_id}"] = {
        "lat": 40.7580,
        "lon": -73.9855,
        "ts": "2024-01-01T12:05:00+00:00",
        "last_valid_ts": "2024-01-01T12:05:00+00:00",
    }

    accepted, rejects = asyncio.run(
        hot_main.flush_to_redis(
            redis,
            [_make_position(courier_id=courier_id, ts="2024-01-01T12:04:00+00:00")],
        )
    )

    assert accepted == 0
    assert len(rejects) == 1
    assert rejects[0]["reason"] == "non_monotonic_ts"
    payload = redis.hashes[f"{hot_main.HASH_PREFIX}{courier_id}"]
    assert payload["anomaly_state"] == "anomalous"
    assert payload["stale_reason"] == "non_monotonic_ts"


def test_flush_to_redis_rejects_impossible_jump():
    redis = _FakeRedis()
    courier_id = "drv_demo_003"
    redis.hashes[f"{hot_main.HASH_PREFIX}{courier_id}"] = {
        "lat": 40.7580,
        "lon": -73.9855,
        "ts": "2024-01-01T12:00:00+00:00",
        "last_valid_ts": "2024-01-01T12:00:00+00:00",
    }

    accepted, rejects = asyncio.run(
        hot_main.flush_to_redis(
            redis,
            [
                _make_position(
                    courier_id=courier_id,
                    lat=40.9000,
                    lon=-73.7000,
                    ts="2024-01-01T12:00:10+00:00",
                )
            ],
        )
    )

    assert accepted == 0
    assert len(rejects) == 1
    assert rejects[0]["reason"] in {"jump_too_large", "implied_speed_too_high"}
