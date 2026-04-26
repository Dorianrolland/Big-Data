from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

_API_DIR = Path(__file__).resolve().parent.parent / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))


class _FakeGauge:
    def __init__(self, *_args, **_kwargs) -> None:
        pass

    def set(self, *_args, **_kwargs) -> None:
        return None


class _FakeInstrumentator:
    def __init__(self, *_args, **_kwargs) -> None:
        pass

    def instrument(self, app):  # noqa: ANN001
        return self

    def expose(self, *_args, **_kwargs):  # noqa: ANN001
        return None


sys.modules.setdefault("prometheus_client", types.SimpleNamespace(Gauge=_FakeGauge))
sys.modules.setdefault(
    "prometheus_fastapi_instrumentator",
    types.SimpleNamespace(Instrumentator=_FakeInstrumentator),
)

_API_MAIN_SPEC = importlib.util.spec_from_file_location(
    "api_main_under_test",
    _API_DIR / "main.py",
)
assert _API_MAIN_SPEC is not None and _API_MAIN_SPEC.loader is not None
api_main = importlib.util.module_from_spec(_API_MAIN_SPEC)
sys.modules.setdefault("api_main_under_test", api_main)
_API_MAIN_SPEC.loader.exec_module(api_main)


class _FakePipeline:
    def __init__(self, redis: "_FakeRedis") -> None:
        self.redis = redis
        self.ops: list[tuple] = []

    def hmget(self, key: str, *fields: str):  # noqa: ANN001
        self.ops.append(("hmget", key, list(fields)))
        return self

    def hget(self, key: str, field: str):  # noqa: ANN001
        self.ops.append(("hget", key, field))
        return self

    def lrange(self, key: str, start: int, end: int):  # noqa: ANN001
        self.ops.append(("lrange", key, start, end))
        return self

    async def execute(self) -> list[object]:
        out: list[object] = []
        for op in self.ops:
            kind = op[0]
            if kind == "hmget":
                _, key, fields = op
                raw = self.redis.hashes.get(key, {})
                out.append([raw.get(field) for field in fields])
            elif kind == "hget":
                _, key, field = op
                raw = self.redis.hashes.get(key, {})
                out.append(raw.get(field))
            elif kind == "lrange":
                _, key, start, end = op
                data = self.redis.tracks.get(key, [])
                if end == -1:
                    out.append(list(data[start:]))
                else:
                    out.append(list(data[start : end + 1]))
            else:
                raise AssertionError(f"Unsupported pipeline op: {kind}")
        return out


class _FakeRedis:
    def __init__(
        self,
        *,
        ids: list[str] | None = None,
        hashes: dict[str, dict[str, str]] | None = None,
        tracks: dict[str, list[str]] | None = None,
    ) -> None:
        self.ids = list(ids or [])
        self.hashes = {key: dict(value) for key, value in (hashes or {}).items()}
        self.tracks = {key: list(value) for key, value in (tracks or {}).items()}

    async def ping(self) -> bool:
        return True

    async def zcard(self, _key: str) -> int:
        return len(self.ids)

    async def zrange(self, _key: str, start: int, end: int) -> list[str]:
        if end == -1:
            return list(self.ids[start:])
        return list(self.ids[start : end + 1])

    def pipeline(self, transaction: bool = False) -> _FakePipeline:  # noqa: ARG002
        return _FakePipeline(self)


def _track_point(
    ts: str,
    speed_kmh: float,
    status: str,
    *,
    route_source: str = "osrm",
    anomaly_state: str = "ok",
) -> str:
    return json.dumps(
        {
            "lat": 40.71,
            "lon": -73.98,
            "ts": ts,
            "speed_kmh": speed_kmh,
            "status": status,
            "route_source": route_source,
            "anomaly_state": anomaly_state,
        },
        separators=(",", ":"),
    )


def test_canonical_status_preserves_operational_detail_states():
    assert api_main._canonical_status("pickup_en_route") == "pickup_en_route"
    assert api_main._canonical_status("pickup_arrived") == "pickup_arrived"
    assert api_main._canonical_status("repositioning") == "repositioning"
    assert api_main._canonical_status("busy") == "delivering"
    assert api_main._canonical_status("ready") == "available"


def test_trailing_stationary_duration_requires_delivery_and_multiple_points():
    pickup_points = [
        {"ts": "2024-01-02T12:00:00+00:00", "speed_kmh": 0.0, "status": "pickup_arrived"},
        {"ts": "2024-01-02T12:03:00+00:00", "speed_kmh": 0.0, "status": "pickup_arrived"},
    ]
    assert api_main._trailing_stationary_duration_seconds(
        pickup_points,
        speed_threshold_kmh=1.5,
    ) == (0.0, 0)

    one_snapshot = [
        {"ts": "2024-01-02T12:05:00+00:00", "speed_kmh": 0.0, "status": "delivering"},
    ]
    assert api_main._trailing_stationary_duration_seconds(
        one_snapshot,
        speed_threshold_kmh=1.5,
    ) == (0.0, 1)


def test_fleet_insights_separates_pickup_repositioning_and_duration_based_suspects(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"source": "test"}

    async def _fake_dq(*_args, **_kwargs):  # noqa: ANN001
        return [(4, 11.8, 25.0, 0, 0)]

    redis = _FakeRedis(
        ids=["drv_delivering", "drv_pickup", "drv_repositioning", "drv_available"],
        hashes={
            "fleet:livreur:drv_delivering": {
                "status": "delivering",
                "speed_kmh": "0.0",
                "ts": "2024-01-02T12:04:00+00:00",
                "lat": "40.7100",
                "lon": "-73.9800",
                "battery_pct": "82",
            },
            "fleet:livreur:drv_pickup": {
                "status": "pickup_en_route",
                "speed_kmh": "8.0",
                "ts": "2024-01-02T12:04:00+00:00",
                "lat": "40.7110",
                "lon": "-73.9810",
                "battery_pct": "74",
            },
            "fleet:livreur:drv_repositioning": {
                "status": "repositioning",
                "speed_kmh": "16.0",
                "ts": "2024-01-02T12:04:00+00:00",
                "lat": "40.7120",
                "lon": "-73.9820",
                "battery_pct": "68",
            },
            "fleet:livreur:drv_available": {
                "status": "available",
                "speed_kmh": "0.0",
                "ts": "2024-01-02T12:04:00+00:00",
                "lat": "40.7130",
                "lon": "-73.9830",
                "battery_pct": "91",
            },
        },
        tracks={
            "fleet:track:drv_delivering": [
                _track_point("2024-01-02T12:04:00+00:00", 0.0, "delivering", route_source="hold"),
                _track_point("2024-01-02T12:03:00+00:00", 0.2, "delivering", route_source="hold"),
                _track_point("2024-01-02T12:02:00+00:00", 0.0, "delivering", route_source="hold"),
            ],
            "fleet:track:drv_pickup": [
                _track_point("2024-01-02T12:04:00+00:00", 0.0, "pickup_en_route"),
                _track_point("2024-01-02T12:02:00+00:00", 0.0, "pickup_en_route"),
            ],
        },
    )

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _fake_dq)
    api_main.app.state.redis = redis

    payload = asyncio.run(api_main.fleet_insights(heures=1, reference_ts=None))

    fleet = payload["flotte_temps_reel"]
    assert fleet["statuts"]["delivering"] == 1
    assert fleet["statuts"]["pickup"] == 1
    assert fleet["statuts"]["repositioning"] == 1
    assert fleet["statuts"]["available"] == 1
    assert fleet["statuts_detail"]["pickup_en_route"] == 1
    assert fleet["taux_utilisation_pct"] == 25.0
    assert fleet["taux_engagement_pct"] == 75.0

    alertes = payload["alertes"]
    assert alertes["livreurs_immobiles_en_livraison"] == 1
    assert alertes["detail_suspects"][0]["livreur_id"] == "drv_delivering"
    assert (
        alertes["detail_suspects"][0]["immobile_duration_s"]
        >= api_main.FLEET_INSIGHTS_STATIONARY_MIN_SECONDS
    )


def test_detect_anomalies_uses_stationary_duration_threshold(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"source": "test"}

    async def _unexpected_dq(*_args, **_kwargs):  # noqa: ANN001
        raise AssertionError("detect_anomalies should not use DuckDB anymore")

    redis = _FakeRedis(
        ids=["drv_hold"],
        tracks={
            "fleet:track:drv_hold": [
                _track_point("2024-01-02T12:05:00+00:00", 0.0, "delivering", route_source="hold"),
                _track_point("2024-01-02T12:04:00+00:00", 0.0, "delivering", route_source="hold"),
                _track_point("2024-01-02T12:03:00+00:00", 0.0, "delivering", route_source="hold"),
                _track_point("2024-01-02T12:02:00+00:00", 16.0, "delivering"),
                _track_point("2024-01-02T12:01:00+00:00", 15.0, "delivering"),
            ],
        },
    )

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _unexpected_dq)
    api_main.app.state.redis = redis

    payload = asyncio.run(
        api_main.detect_anomalies(
            fenetre_minutes=10,
            seuil_vitesse=50.0,
            seuil_immobile=2.0,
            reference_ts=None,
        )
    )

    assert payload["analytics_status"]["source"] == "hot_path_live"
    assert (
        payload["seuils"]["immobilisation_duree_min_secondes"]
        == api_main.ANOMALY_STATIONARY_MIN_SECONDS
    )
    assert (
        payload["seuils"]["vitesse_excessive_min_occurrences"]
        == api_main.ANOMALY_SPEED_MIN_EXCEEDANCES
    )
    assert payload["resume"]["anomalies_detectees"] == 1
    assert payload["anomalies"][0]["anomalies"][0]["type"] == "IMMOBILISATION_SUSPECTE"


def test_detect_gps_fraud_uses_live_tracks_and_frozen_duration_thresholds(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"source": "test"}

    async def _unexpected_dq(*_args, **_kwargs):  # noqa: ANN001
        raise AssertionError("detect_gps_fraud should not use DuckDB anymore")

    redis = _FakeRedis(
        ids=["drv_frozen", "drv_short"],
        tracks={
            "fleet:track:drv_frozen": [
                _track_point("2024-01-02T12:05:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T12:04:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T12:03:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T12:02:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T12:01:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T12:00:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T11:59:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T11:58:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T11:57:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T11:56:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T11:55:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T11:54:00+00:00", 0.0, "delivering"),
            ],
            "fleet:track:drv_short": [
                _track_point("2024-01-02T12:05:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T12:04:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T12:03:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T12:02:00+00:00", 0.0, "delivering"),
            ],
        },
    )

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _unexpected_dq)
    api_main.app.state.redis = redis

    payload = asyncio.run(
        api_main.detect_gps_fraud(
            fenetre_minutes=15,
            seuil_teleport_km=2.0,
            vitesse_max_physique_kmh=90.0,
            reference_ts=None,
        )
    )

    assert payload["seuils"]["min_interval_seconds"] == api_main.GPS_FRAUD_MIN_INTERVAL_SECONDS
    assert payload["seuils"]["position_figee_min_rep"] == api_main.GPS_FRAUD_FROZEN_MIN_REPETITIONS
    assert (
        payload["seuils"]["position_figee_min_duration_s"]
        == api_main.GPS_FRAUD_FROZEN_MIN_DURATION_SECONDS
    )
    assert payload["resume"]["teleportations"] == 0
    assert payload["resume"]["positions_figees"] == 1
    assert payload["positions_figees"][0]["livreur_id"] == "drv_frozen"
