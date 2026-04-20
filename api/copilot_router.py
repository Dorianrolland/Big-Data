from __future__ import annotations

import asyncio
import glob
import json
import logging
import math
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

import httpx
import joblib
import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field

from copilot_logic import (
    FEATURE_COLUMNS,
    build_feature_map,
    cost_breakdown,
    dispatch_decision,
    dispatch_strategy_profile,
    explanation_details,
    forecast_zone_metrics,
    heuristic_score,
    hybrid_accept_decision,
    model_score,
    normalize_score_weights,
    reposition_cost_model,
    recommendation_score,
    validate_model_payload,
)

logger = logging.getLogger("copilot-router")

load_dotenv()


def _env_float(name: str, default: float) -> float:
    try:
        out = float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _env_int(name: str, default: int, *, min_value: int | None = None, max_value: int | None = None) -> int:
    try:
        out = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        out = int(default)
    if min_value is not None:
        out = max(int(min_value), out)
    if max_value is not None:
        out = min(int(max_value), out)
    return int(out)


EVENTS_PATH = Path(os.getenv("EVENTS_PATH", "/data/parquet_events"))
MODEL_PATH = Path(os.getenv("MODEL_PATH", "/data/models/copilot_model.joblib"))
OSRM_URL = os.getenv("OSRM_URL", "http://osrm:5000")
MODEL_MIN_ROWS = int(os.getenv("COPILOT_MODEL_MIN_ROWS", "300"))
MODEL_MIN_AUC = float(os.getenv("COPILOT_MODEL_MIN_AUC", "0.62"))
MODEL_MIN_AVG_PRECISION = float(os.getenv("COPILOT_MODEL_MIN_AVG_PRECISION", "0.45"))

WEATHER_KEY = "copilot:context:weather"
GBFS_KEY = "copilot:context:gbfs"
IRVE_KEY = "copilot:context:irve"
TLC_REPLAY_KEY = "copilot:replay:tlc:status"
FUEL_CONTEXT_KEY = "copilot:context:fuel"
CONTEXT_QUALITY_KEY = "copilot:context:quality"
EVENTS_CONTEXT_KEY = "copilot:context:events"
ZONE_CONTEXT_PREFIX = "copilot:context:zone:"
OFFER_KEY_PREFIX = "copilot:offer:"
DRIVER_OFFERS_PREFIX = "copilot:driver:"
COURIER_HASH_PREFIX = "fleet:livreur:"
SINGLE_REPLAY_STATUS_KEY = "copilot:replay:tlc:single:status"
MISSION_JOURNAL_PREFIX = "copilot:mission:journal:"
MISSION_JOURNAL_MAX_ITEMS = int(os.getenv("COPILOT_MISSION_JOURNAL_MAX_ITEMS", "200"))
FUEL_CONTEXT_TTL_SECONDS = int(os.getenv("COPILOT_FUEL_CONTEXT_TTL_SECONDS", "86400"))
DEFAULT_FUEL_PRICE_EUR_L = float(os.getenv("COPILOT_FUEL_PRICE_EUR_L", "1.85"))
DEFAULT_CONSUMPTION_L_100KM = float(os.getenv("COPILOT_CONSUMPTION_L_100KM", "7.5"))
DEFAULT_PLATFORM_FEE_PCT = float(os.getenv("COPILOT_PLATFORM_FEE_PCT", "25.0"))
OSRM_SCORE_TIMEOUT_S = float(os.getenv("COPILOT_OSRM_TIMEOUT_SECONDS", "4.0"))
DUCKDB_QUERY_TIMEOUT_SECONDS = float(os.getenv("COPILOT_DUCKDB_QUERY_TIMEOUT_SECONDS", "12.0"))
REPOSITION_BASE_SPEED_KMH = float(os.getenv("COPILOT_REPOSITION_BASE_SPEED_KMH", "24.0"))
DEFAULT_MAX_REPOSITION_ETA_MIN = float(os.getenv("COPILOT_MAX_REPOSITION_ETA_MIN", "20.0"))
DEFAULT_DISPATCH_FORECAST_HORIZON_MIN = float(os.getenv("COPILOT_DISPATCH_FORECAST_HORIZON_MIN", "30.0"))
REPOSITION_TIME_COST_SHARE = float(os.getenv("COPILOT_REPOSITION_TIME_COST_SHARE", "0.32"))
REPOSITION_RISK_COST_SHARE = float(os.getenv("COPILOT_REPOSITION_RISK_COST_SHARE", "0.22"))
FUEL_PRICE_PROVIDER_URL = os.getenv(
    "COPILOT_FUEL_PRICE_PROVIDER_URL", "https://www.fueleconomy.gov/ws/rest/fuelprices"
)
FUEL_PRICE_PROVIDER_TIMEOUT_S = float(os.getenv("COPILOT_FUEL_PROVIDER_TIMEOUT_SECONDS", "10.0"))
FUEL_REFRESH_INTERVAL_S = int(os.getenv("COPILOT_FUEL_REFRESH_SECONDS", "1800"))
FUEL_USD_TO_EUR_RATE = float(os.getenv("COPILOT_USD_TO_EUR_RATE", "0.92"))
FUEL_GRADE = os.getenv("COPILOT_FUEL_GRADE", "regular").strip().lower()
GALLON_TO_LITER = 3.785411784
SCORE_WEIGHTS = normalize_score_weights(
    {
        "net_hourly": _env_float("COPILOT_SCORE_W_NET_HOURLY", 0.46),
        "net_trip": _env_float("COPILOT_SCORE_W_NET_TRIP", 0.18),
        "fuel_efficiency": _env_float("COPILOT_SCORE_W_FUEL", 0.16),
        "time_efficiency": _env_float("COPILOT_SCORE_W_TIME", 0.12),
        "context": _env_float("COPILOT_SCORE_W_CONTEXT", 0.08),
    }
)
ACCEPT_BASE_THRESHOLD = _env_float("COPILOT_ACCEPT_BASE_THRESHOLD", 0.50)
ACCEPT_BELOW_TARGET_PENALTY_MAX = max(0.0, _env_float("COPILOT_ACCEPT_BELOW_TARGET_PENALTY_MAX", 0.12))
ACCEPT_HIGH_FUEL_PENALTY = max(0.0, _env_float("COPILOT_ACCEPT_HIGH_FUEL_PENALTY", 0.06))
ACCEPT_ABOVE_TARGET_BONUS = max(0.0, _env_float("COPILOT_ACCEPT_ABOVE_TARGET_BONUS", 0.04))
RANK_REJECT_EUR_H_DEFAULT = max(0.0, _env_float("COPILOT_RANK_REJECT_EUR_H_DEFAULT", 9.0))
RANK_TOP_PICK_MIN_EUR_H = max(0.0, _env_float("COPILOT_RANK_TOP_PICK_MIN_EUR_H", 14.0))
RANK_TOP_PICK_MIN_EDGE_EUR_H = max(0.0, _env_float("COPILOT_RANK_TOP_PICK_MIN_EDGE_EUR_H", 1.0))
RANK_BELOW_TARGET_SLACK_EUR_H = max(0.0, _env_float("COPILOT_RANK_BELOW_TARGET_SLACK_EUR_H", 0.75))
SHIFT_PLAN_TOP_K_DEFAULT = _env_int("COPILOT_SHIFT_PLAN_TOP_K_DEFAULT", 6, min_value=3, max_value=20)
SHIFT_PLAN_REFERENCE_KM = _env_float("COPILOT_SHIFT_PLAN_REFERENCE_KM", 15.0)
SHIFT_PLAN_DEFAULT_TARGET_EUR_H = max(0.0, _env_float("COPILOT_SHIFT_PLAN_TARGET_EUR_H", 18.0))

GBFS_STATION_INFO_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
GBFS_STATION_STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
OPENCHARGEMAP_URL = "https://api.openchargemap.io/v3/poi/"
OPENCHARGEMAP_API_KEY = os.getenv("OPENCHARGEMAP_API_KEY", "").strip()

# NYC bounding box covers all 5 boroughs
NYC_LAT_MIN, NYC_LAT_MAX = 40.49, 40.92
NYC_LON_MIN, NYC_LON_MAX = -74.27, -73.68
NYC_CENTER_LAT, NYC_CENTER_LON = 40.7580, -73.9855
ZONE_STEP = 0.02
NYC_ZONE_CENTROID_PATHS = (
    Path(__file__).resolve().parent.parent / "context_poller" / "nyc_zone_centroids.json",
    Path(__file__).resolve().parent.parent / "tlc_replay" / "nyc_zone_centroids.json",
)
_NYC_ZONE_CENTROIDS: dict[str, tuple[float, float]] | None = None

copilot_router = APIRouter(prefix="/copilot", tags=["Copilot"])


class ScoreOfferRequest(BaseModel):
    offer_id: str | None = None
    courier_id: str | None = None
    estimated_fare_eur: float = Field(0.0, ge=0)
    estimated_distance_km: float = Field(0.0, ge=0)
    estimated_duration_min: float = Field(1.0, ge=0.1)
    courier_lat: float | None = Field(None, ge=-90, le=90)
    courier_lon: float | None = Field(None, ge=-180, le=180)
    pickup_lat: float | None = Field(None, ge=-90, le=90)
    pickup_lon: float | None = Field(None, ge=-180, le=180)
    dropoff_lat: float | None = Field(None, ge=-90, le=90)
    dropoff_lon: float | None = Field(None, ge=-180, le=180)
    distance_to_pickup_km: float | None = Field(None, ge=0)
    eta_to_pickup_min: float | None = Field(None, ge=0)
    demand_index: float = Field(1.0, ge=0)
    supply_index: float = Field(1.0, ge=0)
    weather_factor: float = Field(1.0, ge=0)
    traffic_factor: float = Field(1.0, ge=0)
    event_pressure: float = Field(0.0, ge=0)
    event_count_nearby: int | None = Field(None, ge=0, le=5000)
    weather_precip_mm: float | None = Field(None, ge=0, le=200)
    weather_wind_kmh: float | None = Field(None, ge=0, le=250)
    weather_intensity: float | None = Field(None, ge=0, le=1)
    temporal_hour_local: float | None = Field(None, ge=-1, le=24)
    is_peak_hour: bool | None = None
    is_weekend: bool | None = None
    is_holiday: bool | None = None
    temporal_pressure: float | None = Field(None, ge=0, le=2)
    context_fallback_applied: bool | None = None
    context_stale_sources: int | None = Field(None, ge=0, le=12)
    fuel_price_eur_l: float | None = Field(None, ge=0.5, le=5.0)
    vehicle_consumption_l_100km: float | None = Field(None, ge=2.0, le=30.0)
    platform_fee_pct: float | None = Field(None, ge=0.0, le=60.0)
    other_costs_eur: float | None = Field(None, ge=0.0, le=100.0)
    target_hourly_net_eur: float | None = Field(None, ge=0.0, le=150.0)
    use_osrm: bool = False


class ExplanationDetail(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    code: str
    label: str
    impact: Literal["positive", "negative", "neutral"]
    value: float
    unit: str
    source: Literal["cost", "time", "fuel", "target", "context", "routing"]


class ScoreOfferResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    offer_id: str | None = None
    courier_id: str | None = None
    accept_score: float
    decision: str
    decision_threshold: float | None = None
    eur_per_hour_net: float
    estimated_net_eur: float | None = None
    target_hourly_net_eur: float | None = None
    target_gap_eur_h: float | None = None
    costs: dict[str, float] | None = None
    route_source: str | None = None
    route_distance_km: float | None = None
    route_duration_min: float | None = None
    route_notes: list[str] | None = None
    model_used: str
    explanation: list[str]
    explanation_details: list[ExplanationDetail] | None = None


class RankOffersRequest(BaseModel):
    """Batch scoring request: compare several offers at once and return them
    ranked by net €/hour so the driver can pick the most efficient course
    for their time and fuel.
    """
    model_config = ConfigDict(protected_namespaces=())

    offers: list[ScoreOfferRequest] = Field(..., min_length=1, max_length=20)
    rank_by: Literal["eur_per_hour_net", "estimated_net_eur", "accept_score"] = (
        "eur_per_hour_net"
    )
    use_osrm: bool = False
    reject_below_eur_h: float | None = Field(
        None, ge=0.0, le=200.0,
        description="Hard floor: offers with net €/h strictly below this are tagged 'reject'.",
    )


class RankedOfferItem(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    rank: int
    top_pick: bool
    recommendation: Literal["top_pick", "viable", "below_target", "reject"]
    offer_id: str | None = None
    courier_id: str | None = None
    accept_score: float
    decision: str
    decision_threshold: float | None = None
    eur_per_hour_net: float
    estimated_net_eur: float
    delta_vs_top_eur_h: float
    delta_vs_median_eur_h: float
    costs: dict[str, float] | None = None
    route_source: str | None = None
    route_distance_km: float | None = None
    route_duration_min: float | None = None
    model_used: str
    explanation: list[str]
    explanation_details: list[ExplanationDetail] | None = None


class RankOffersResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    count: int
    ranked_by: str
    top_pick_offer_id: str | None = None
    best_eur_h: float
    worst_eur_h: float
    median_eur_h: float
    hourly_gain_vs_worst_eur_h: float
    items: list[RankedOfferItem]


class ShiftPlanZoneItem(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    rank: int
    zone_id: str
    zone_lat: float | None = None
    zone_lon: float | None = None
    shift_score: float
    confidence: float
    estimated_net_eur_h: float
    estimated_gross_eur_h: float
    net_gain_vs_target_eur_h: float
    horizon_min: int
    why_now: str
    reasons: list[str]
    demand_index: float
    supply_index: float
    weather_factor: float
    traffic_factor: float
    event_pressure: float
    temporal_pressure: float
    forecast_pressure_ratio: float
    forecast_volatility: float
    distance_km: float | None = None
    eta_min: float | None = None
    reposition_total_cost_eur: float | None = None
    context_fallback_applied: bool = False
    freshness_policy: str | None = None


class ShiftPlanResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    driver_id: str
    origin_lat: float | None = None
    origin_lon: float | None = None
    horizon_min: Literal[60, 90]
    generated_at: str
    target_hourly_net_eur: float
    source: str
    count: int
    items: list[ShiftPlanZoneItem]


class FuelContextUpdateRequest(BaseModel):
    fuel_price_eur_l: float = Field(..., ge=0.5, le=5.0)
    vehicle_consumption_l_100km: float = Field(..., ge=2.0, le=30.0)
    platform_fee_pct: float = Field(..., ge=0.0, le=60.0)
    source: str = Field("manual", min_length=2, max_length=40)
    lock_manual: bool = False


class MissionReportRequest(BaseModel):
    mission_id: str | None = Field(None, min_length=6, max_length=80)
    zone_id: str = Field(..., min_length=1, max_length=80)
    started_at: str = Field(..., min_length=10, max_length=64)
    completed_at: str = Field(..., min_length=10, max_length=64)
    elapsed_min: float = Field(..., ge=0.0, le=720.0)
    initial_distance_km: float = Field(..., ge=0.0, le=200.0)
    final_distance_km: float = Field(..., ge=0.0, le=200.0)
    predicted_eta_min: float | None = Field(None, ge=0.0, le=240.0)
    predicted_potential_eur_h: float | None = Field(None, ge=0.0, le=500.0)
    baseline_eur_h: float | None = Field(None, ge=0.0, le=500.0)
    realized_best_offer_eur_h: float | None = Field(None, ge=0.0, le=500.0)
    route_source: str | None = Field(None, min_length=2, max_length=24)
    stop_reason: str | None = Field(None, min_length=2, max_length=160)
    success: bool | None = None
    dispatch_strategy: str | None = Field(None, min_length=2, max_length=24)
    arrival_threshold_km: float | None = Field(None, ge=0.05, le=5.0)
    eta_policy_limit_min: float | None = Field(None, ge=1.0, le=480.0)
    target_lat: float | None = Field(None, ge=-90.0, le=90.0)
    target_lon: float | None = Field(None, ge=-180.0, le=180.0)


def _run_duck_query(request: Request, sql: str, params: list[Any] | None = None) -> list[tuple]:
    def _execute() -> list[tuple]:
        cur = request.app.state.duckdb.cursor()
        return cur.execute(sql, params or []).fetchall()

    return _execute()


async def _duck_query(request: Request, sql: str, params: list[Any] | None = None) -> list[tuple]:
    if DUCKDB_QUERY_TIMEOUT_SECONDS <= 0:
        return await asyncio.to_thread(_run_duck_query, request, sql, params)
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_run_duck_query, request, sql, params),
            timeout=DUCKDB_QUERY_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError as exc:
        raise HTTPException(
            status_code=504,
            detail=(
                f"DuckDB timeout ({DUCKDB_QUERY_TIMEOUT_SECONDS:.1f}s). "
                "Reduis la fenetre ou relance la requete."
            ),
        ) from exc


def _as_float(value: Any, default: float) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(out):
        return default
    return out


def _non_negative_float(value: Any, default: float) -> float:
    return max(0.0, _as_float(value, default))


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2.0) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2.0) ** 2
    )
    return r * 2.0 * math.asin(math.sqrt(a))


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(value, high))


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    txt = str(value).strip().lower()
    if txt in {"1", "true", "yes", "y", "on"}:
        return True
    if txt in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _zone_context_payload(zone_raw: dict[str, Any], zone_id_hint: str | None = None) -> dict[str, Any]:
    demand = _as_float(zone_raw.get("demand_index"), 1.0)
    supply = max(_as_float(zone_raw.get("supply_index"), 1.0), 0.2)
    weather = _as_float(zone_raw.get("weather_factor"), 1.0)
    traffic = max(_as_float(zone_raw.get("traffic_factor"), 1.0), 0.2)
    return {
        "zone_id": zone_raw.get("zone_id", zone_id_hint),
        "demand_index": round(demand, 3),
        "supply_index": round(supply, 3),
        "weather_factor": round(weather, 3),
        "traffic_factor": round(traffic, 3),
        "event_pressure": round(_as_float(zone_raw.get("event_pressure"), 0.0), 4),
        "event_count_nearby": int(_as_float(zone_raw.get("event_count_nearby"), 0.0)),
        "weather_precip_mm": round(_as_float(zone_raw.get("weather_precip_mm"), 0.0), 3),
        "weather_wind_kmh": round(_as_float(zone_raw.get("weather_wind_kmh"), 0.0), 3),
        "weather_intensity": round(_as_float(zone_raw.get("weather_intensity"), 0.0), 4),
        "temporal_hour_local": round(_as_float(zone_raw.get("temporal_hour_local"), -1.0), 3),
        "is_peak_hour": _as_bool(zone_raw.get("is_peak_hour"), False),
        "is_weekend": _as_bool(zone_raw.get("is_weekend"), False),
        "is_holiday": _as_bool(zone_raw.get("is_holiday"), False),
        "temporal_pressure": round(_as_float(zone_raw.get("temporal_pressure"), 0.0), 4),
        "context_fallback_applied": _as_bool(zone_raw.get("context_fallback_applied"), False),
        "context_stale_sources": int(_as_float(zone_raw.get("context_stale_sources"), 0.0)),
        "freshness_policy": zone_raw.get("freshness_policy"),
        "age_gbfs_s": round(_as_float(zone_raw.get("age_gbfs_s"), -1.0), 3),
        "age_weather_s": round(_as_float(zone_raw.get("age_weather_s"), -1.0), 3),
        "age_nyc311_s": round(_as_float(zone_raw.get("age_nyc311_s"), -1.0), 3),
        "age_events_s": round(_as_float(zone_raw.get("age_events_s"), -1.0), 3),
        "age_dot_closure_s": round(_as_float(zone_raw.get("age_dot_closure_s"), -1.0), 3),
        "age_dot_speeds_s": round(_as_float(zone_raw.get("age_dot_speeds_s"), -1.0), 3),
        "stale_gbfs": _as_bool(zone_raw.get("stale_gbfs"), False),
        "stale_weather": _as_bool(zone_raw.get("stale_weather"), False),
        "stale_nyc311": _as_bool(zone_raw.get("stale_nyc311"), False),
        "stale_events": _as_bool(zone_raw.get("stale_events"), False),
        "stale_dot_closure": _as_bool(zone_raw.get("stale_dot_closure"), False),
        "stale_dot_speeds": _as_bool(zone_raw.get("stale_dot_speeds"), False),
        "gbfs_demand_boost": round(_as_float(zone_raw.get("gbfs_demand_boost"), 0.0), 3),
        "demand_trend": round(_as_float(zone_raw.get("demand_trend"), 0.0), 3),
        "demand_trend_ema": round(_as_float(zone_raw.get("demand_trend_ema"), 0.0), 3),
        "updated_at": zone_raw.get("updated_at"),
    }


def _compute_heuristic_score(features: dict[str, float]) -> tuple[float, float, list[str]]:
    return heuristic_score(features, weights=SCORE_WEIGHTS)


def _decision_for_offer(features: dict[str, float], accept_prob: float) -> tuple[str, float]:
    return hybrid_accept_decision(
        features=features,
        accept_score=float(accept_prob),
        base_threshold=ACCEPT_BASE_THRESHOLD,
        below_target_penalty_max=ACCEPT_BELOW_TARGET_PENALTY_MAX,
        high_fuel_penalty=ACCEPT_HIGH_FUEL_PENALTY,
        above_target_bonus=ACCEPT_ABOVE_TARGET_BONUS,
    )


def _build_explanation_details(
    *,
    features: dict[str, float],
    accept_prob: float,
    decision_threshold: float,
    route_meta: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    route_source = None
    route_duration_min = None
    if route_meta:
        route_source = str(route_meta.get("route_source", "estimated"))
        route_duration_min = _as_float(
            route_meta.get("route_duration_min"),
            float(features.get("total_duration_min", 0.0)),
        )
    return explanation_details(
        features=features,
        accept_score=float(accept_prob),
        decision_threshold=float(decision_threshold),
        route_source=route_source,
        route_duration_min=route_duration_min,
    )


def _build_scored_offer_snapshot(
    *,
    payload: dict[str, Any],
    features: dict[str, float],
    eur_per_hour: float,
    accept_prob: float,
    decision: str,
    decision_threshold: float,
    breakdown: dict[str, float],
    route_meta: dict[str, Any],
    model_used: str,
    reasons: list[str],
    details: list[dict[str, Any]],
    extras: dict[str, Any] | None = None,
) -> dict[str, Any]:
    record = {
        "offer_id": payload.get("offer_id"),
        "courier_id": payload.get("courier_id"),
        "accept_score": round(float(accept_prob), 4),
        "decision": decision,
        "decision_threshold": round(float(decision_threshold), 4),
        "eur_per_hour_net": round(float(eur_per_hour), 2),
        "estimated_net_eur": round(float(features.get("estimated_net_eur", 0.0)), 2),
        "target_hourly_net_eur": round(float(features.get("target_hourly_net_eur", 0.0)), 2),
        "target_gap_eur_h": round(float(features.get("target_gap_eur_h", 0.0)), 2),
        "costs": breakdown,
        "route_source": str(route_meta.get("route_source", "estimated")),
        "route_distance_km": round(
            float(route_meta.get("route_distance_km", features.get("total_distance_km", 0.0))), 3
        ),
        "route_duration_min": round(
            float(route_meta.get("route_duration_min", features.get("total_duration_min", 0.0))), 3
        ),
        "route_notes": list(route_meta.get("route_notes", [])),
        "model_used": model_used,
        "explanation": reasons,
        "explanation_details": details,
    }
    if extras:
        record.update(extras)
    return record


def _existing_parquet_globs(globs: list[str]) -> list[str]:
    existing: list[str] = []
    for pattern in globs:
        try:
            if glob.glob(pattern):
                existing.append(pattern)
        except OSError:
            continue
    return existing


def _parse_fuel_price_usd_gallon(xml_text: str, grade: str) -> float:
    root = ET.fromstring(xml_text)
    normalized_grade = (grade or "regular").strip().lower()
    nodes: dict[str, float] = {}
    for node in root.iter():
        tag = (node.tag or "").strip().lower()
        text = (node.text or "").strip()
        if not tag or not text:
            continue
        try:
            nodes[tag] = float(text)
        except ValueError:
            continue

    if normalized_grade in nodes:
        return float(nodes[normalized_grade])
    if normalized_grade in {"diesel", "premium", "midgrade", "regular"}:
        fallback_order = [normalized_grade, "regular", "midgrade", "premium", "diesel"]
    else:
        fallback_order = ["regular", "midgrade", "premium", "diesel"]
    for key in fallback_order:
        if key in nodes:
            return float(nodes[key])
    raise ValueError("fuel_price_grade_not_found")


def _usd_gallon_to_eur_liter(usd_per_gallon: float, usd_to_eur_rate: float) -> float:
    safe_usd_gallon = max(float(usd_per_gallon), 0.0)
    safe_fx = max(float(usd_to_eur_rate), 0.01)
    return (safe_usd_gallon * safe_fx) / GALLON_TO_LITER


def _load_nyc_zone_centroids() -> dict[str, tuple[float, float]]:
    global _NYC_ZONE_CENTROIDS
    if _NYC_ZONE_CENTROIDS is not None:
        return _NYC_ZONE_CENTROIDS

    for path in NYC_ZONE_CENTROID_PATHS:
        if not path.exists():
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("failed to load centroid file %s: %s", path, exc)
            continue

        centroids: dict[str, tuple[float, float]] = {}
        for key, val in raw.items():
            if not isinstance(val, (list, tuple)) or len(val) < 2:
                continue
            try:
                lat = float(val[0])
                lon = float(val[1])
            except (TypeError, ValueError):
                continue
            centroids[str(key)] = (lat, lon)

        if centroids:
            logger.info("loaded %d nyc zone centroids from %s", len(centroids), path)
            _NYC_ZONE_CENTROIDS = centroids
            return _NYC_ZONE_CENTROIDS

    _NYC_ZONE_CENTROIDS = {}
    return _NYC_ZONE_CENTROIDS


def _resolve_zone_coordinates(zone_id: str | None) -> tuple[float, float] | None:
    if not zone_id:
        return None
    raw = str(zone_id).strip()
    if not raw:
        return None

    if raw.startswith("nyc_"):
        key = raw.split("_", 1)[1]
        coords = _load_nyc_zone_centroids().get(key)
        if coords is not None:
            return coords
        return None

    if "_" in raw:
        left, right = raw.split("_", 1)
        try:
            lat = float(left)
            lon = float(right)
        except ValueError:
            return None
        if -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0:
            return lat, lon

    return None


async def _resolve_driver_origin(
    redis_client: aioredis.Redis,
    driver_id: str,
    lat: float | None,
    lon: float | None,
) -> tuple[float, float]:
    if lat is not None and lon is not None:
        return float(lat), float(lon)

    state = await redis_client.hgetall(f"{COURIER_HASH_PREFIX}{driver_id}")
    lat_val = _as_float(state.get("lat"), 0.0)
    lon_val = _as_float(state.get("lon"), 0.0)
    if lat_val == 0.0 and lon_val == 0.0:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Position introuvable pour '{driver_id}'. "
                "Fournir lat/lon en query params ou attendre un GPS recent."
            ),
        )
    return lat_val, lon_val


async def _estimate_reposition_leg(
    *,
    origin_lat: float,
    origin_lon: float,
    dest_lat: float,
    dest_lon: float,
    traffic_factor: float,
    use_osrm: bool,
    osrm_client: httpx.AsyncClient | None,
) -> tuple[float, float, str]:
    if use_osrm and osrm_client is not None:
        leg = await _osrm_leg(osrm_client, origin_lat, origin_lon, dest_lat, dest_lon)
        if leg is not None:
            return float(leg[0]), max(float(leg[1]), 1.0), "osrm"

    distance_km = _haversine_km(origin_lat, origin_lon, dest_lat, dest_lon)
    speed_kmh = max(8.0, REPOSITION_BASE_SPEED_KMH / max(traffic_factor, 0.6))
    eta_min = (distance_km / speed_kmh) * 60.0
    return float(distance_km), max(float(eta_min), 1.0), "estimated"


def _dispatch_offer_summary(offer: dict[str, Any]) -> dict[str, Any]:
    signals = offer.get("recommendation_signals")
    reasons = offer.get("explanation")
    return {
        "offer_id": offer.get("offer_id"),
        "zone_id": offer.get("zone_id"),
        "recommendation_action": offer.get("recommendation_action"),
        "recommendation_score": round(float(offer.get("recommendation_score", 0.0)), 4),
        "accept_score": round(float(offer.get("accept_score", 0.0)), 4),
        "eur_per_hour_net": round(float(offer.get("eur_per_hour_net", 0.0)), 2),
        "target_gap_eur_h": round(float(offer.get("target_gap_eur_h", 0.0)), 2),
        "distance_to_pickup_km": round(float(offer.get("distance_to_pickup_km", 0.0)), 3),
        "route_duration_min": round(float(offer.get("route_duration_min", 0.0)), 2),
        "route_source": offer.get("route_source"),
        "signals": list(signals) if isinstance(signals, list) else [],
        "reasons": list(reasons) if isinstance(reasons, list) else [],
    }


def _forecast_zone_metrics(
    *,
    demand_index: float,
    supply_index: float,
    weather_factor: float,
    traffic_factor: float,
    gbfs_demand_boost: float,
    demand_trend: float,
    horizon_minutes: float,
) -> dict[str, float]:
    return forecast_zone_metrics(
        demand_index=demand_index,
        supply_index=supply_index,
        weather_factor=weather_factor,
        traffic_factor=traffic_factor,
        gbfs_demand_boost=gbfs_demand_boost,
        demand_trend=demand_trend,
        horizon_minutes=horizon_minutes,
    )

def _reposition_cost_model(
    *,
    route_distance_km: float,
    eta_min: float,
    fuel_price_eur_l: float,
    vehicle_consumption_l_100km: float,
    target_hourly_net_eur: float,
    traffic_factor: float,
    forecast_volatility: float,
) -> dict[str, float]:
    return reposition_cost_model(
        route_distance_km=route_distance_km,
        eta_min=eta_min,
        fuel_price_eur_l=fuel_price_eur_l,
        vehicle_consumption_l_100km=vehicle_consumption_l_100km,
        target_hourly_net_eur=target_hourly_net_eur,
        traffic_factor=traffic_factor,
        forecast_volatility=forecast_volatility,
        time_cost_share=REPOSITION_TIME_COST_SHARE,
        risk_cost_share=REPOSITION_RISK_COST_SHARE,
    )


def _dispatch_decision(
    *,
    stay_score: float,
    stay_gap_eur_h: float,
    stay_eur_h: float,
    move_score: float,
    move_eta_min: float,
    move_potential_eur_h: float,
    max_reposition_eta_min: float,
    move_net_gain_eur_h: float = 0.0,
    negative_net_gain_guard: float = -1.0,
    delta_threshold: float = 0.1,
    min_net_gain_for_reposition: float = -0.5,
) -> tuple[str, float, list[str]]:
    return dispatch_decision(
        stay_score=stay_score,
        stay_gap_eur_h=stay_gap_eur_h,
        stay_eur_h=stay_eur_h,
        move_score=move_score,
        move_eta_min=move_eta_min,
        move_potential_eur_h=move_potential_eur_h,
        max_reposition_eta_min=max_reposition_eta_min,
        move_net_gain_eur_h=move_net_gain_eur_h,
        negative_net_gain_guard=negative_net_gain_guard,
        delta_threshold=delta_threshold,
        min_net_gain_for_reposition=min_net_gain_for_reposition,
    )


def _mission_journal_key(driver_id: str) -> str:
    return f"{MISSION_JOURNAL_PREFIX}{driver_id}"


async def _merge_cost_context(redis_client: aioredis.Redis, payload: dict[str, Any]) -> dict[str, Any]:
    context = {}
    try:
        context = await redis_client.hgetall(FUEL_CONTEXT_KEY)
    except Exception:
        context = {}

    defaults = {
        "fuel_price_eur_l": DEFAULT_FUEL_PRICE_EUR_L,
        "vehicle_consumption_l_100km": DEFAULT_CONSUMPTION_L_100KM,
        "platform_fee_pct": DEFAULT_PLATFORM_FEE_PCT,
    }
    for key, fallback in defaults.items():
        if payload.get(key) in (None, ""):
            payload[key] = _as_float(context.get(key), fallback)
    return payload


def _coord_from_payload(payload: dict[str, Any], key: str) -> float | None:
    raw = payload.get(key)
    if raw in (None, ""):
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


async def _hydrate_courier_coords(redis_client: aioredis.Redis, payload: dict[str, Any]) -> dict[str, Any]:
    if _coord_from_payload(payload, "courier_lat") is not None and _coord_from_payload(payload, "courier_lon") is not None:
        return payload
    courier_id = payload.get("courier_id")
    if not courier_id:
        return payload
    try:
        state = await redis_client.hgetall(f"{COURIER_HASH_PREFIX}{courier_id}")
    except Exception:
        return payload
    lat = _as_float(state.get("lat"), 0.0)
    lon = _as_float(state.get("lon"), 0.0)
    if lat != 0.0 or lon != 0.0:
        payload.setdefault("courier_lat", lat)
        payload.setdefault("courier_lon", lon)
    return payload


async def _osrm_leg(
    client: httpx.AsyncClient,
    origin_lat: float,
    origin_lon: float,
    dest_lat: float,
    dest_lon: float,
) -> tuple[float, float] | None:
    coords = f"{origin_lon},{origin_lat};{dest_lon},{dest_lat}"
    url = f"{OSRM_URL}/route/v1/driving/{coords}"
    try:
        resp = await client.get(url, params={"overview": "false"})
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return None

    if payload.get("code") != "Ok" or not payload.get("routes"):
        return None
    route = payload["routes"][0]
    return (float(route["distance"]) / 1000.0, float(route["duration"]) / 60.0)


async def _apply_osrm_route_context(
    redis_client: aioredis.Redis,
    payload: dict[str, Any],
    client: httpx.AsyncClient | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    route_meta: dict[str, Any] = {
        "route_source": "estimated",
        "route_notes": [],
    }
    if not bool(payload.get("use_osrm")):
        return payload, route_meta
    if client is None:
        route_meta["route_notes"].append("osrm_client_unavailable")
        return payload, route_meta

    payload = await _hydrate_courier_coords(redis_client, payload)

    courier_lat = _coord_from_payload(payload, "courier_lat")
    courier_lon = _coord_from_payload(payload, "courier_lon")
    pickup_lat = _coord_from_payload(payload, "pickup_lat")
    pickup_lon = _coord_from_payload(payload, "pickup_lon")
    dropoff_lat = _coord_from_payload(payload, "dropoff_lat")
    dropoff_lon = _coord_from_payload(payload, "dropoff_lon")

    if None in (courier_lat, courier_lon, pickup_lat, pickup_lon, dropoff_lat, dropoff_lon):
        route_meta["route_notes"].append("missing_route_coordinates")
        return payload, route_meta

    pickup_leg, dropoff_leg = await asyncio.gather(
        _osrm_leg(client, courier_lat, courier_lon, pickup_lat, pickup_lon),
        _osrm_leg(client, pickup_lat, pickup_lon, dropoff_lat, dropoff_lon),
    )

    if pickup_leg is not None:
        payload["distance_to_pickup_km"] = round(float(pickup_leg[0]), 3)
        payload["eta_to_pickup_min"] = round(float(pickup_leg[1]), 3)
    else:
        route_meta["route_notes"].append("osrm_pickup_leg_unavailable")

    if dropoff_leg is not None:
        payload["estimated_distance_km"] = round(float(dropoff_leg[0]), 3)
        payload["estimated_duration_min"] = round(max(float(dropoff_leg[1]), 1.0), 3)
    else:
        route_meta["route_notes"].append("osrm_dropoff_leg_unavailable")

    if pickup_leg is not None and dropoff_leg is not None:
        route_meta["route_source"] = "osrm"
    elif pickup_leg is not None or dropoff_leg is not None:
        route_meta["route_source"] = "hybrid"

    total_distance = _as_float(payload.get("distance_to_pickup_km"), 0.0) + _as_float(
        payload.get("estimated_distance_km"), 0.0
    )
    total_duration = _as_float(payload.get("eta_to_pickup_min"), 0.0) + _as_float(
        payload.get("estimated_duration_min"), 1.0
    )
    route_meta["route_distance_km"] = round(total_distance, 3)
    route_meta["route_duration_min"] = round(max(total_duration, 1.0), 3)
    return payload, route_meta


def load_copilot_model() -> tuple[dict[str, Any] | None, dict[str, Any]]:
    quality_gate = {
        "accepted": False,
        "reason": "missing_model_file",
        "model_path": str(MODEL_PATH),
        "thresholds": {
            "min_rows": MODEL_MIN_ROWS,
            "min_auc": MODEL_MIN_AUC,
            "min_average_precision": MODEL_MIN_AVG_PRECISION,
        },
    }

    if not MODEL_PATH.exists():
        return None, quality_gate
    try:
        payload = joblib.load(MODEL_PATH)
        if not isinstance(payload, dict):
            quality_gate["reason"] = "invalid_payload_type"
            return None, quality_gate

        is_valid, reason = validate_model_payload(
            payload,
            min_auc=MODEL_MIN_AUC,
            min_avg_precision=MODEL_MIN_AVG_PRECISION,
            min_rows=MODEL_MIN_ROWS,
        )
        quality_gate["reason"] = reason
        quality_gate["trained_rows"] = int(payload.get("trained_rows") or 0)
        quality_gate["metrics"] = payload.get("metrics", {})

        if not is_valid:
            logger.warning("copilot model rejected by quality gate: %s", reason)
            return None, quality_gate

        quality_gate["accepted"] = True
        logger.info("copilot model accepted by quality gate")
        return payload, quality_gate
    except Exception as exc:  # pragma: no cover
        quality_gate["reason"] = f"load_error:{type(exc).__name__}"
        logger.warning("failed to load model from %s: %s", MODEL_PATH, exc)
        return None, quality_gate


async def _weather_loop(app) -> None:
    await asyncio.sleep(3)
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": NYC_CENTER_LAT,
        "longitude": NYC_CENTER_LON,
        "current": "temperature_2m,precipitation,wind_speed_10m",
    }
    async with httpx.AsyncClient(timeout=12) as client:
        while True:
            try:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                payload = resp.json().get("current", {})
                await app.state.redis.hset(
                    WEATHER_KEY,
                    mapping={
                        "temperature_2m": payload.get("temperature_2m", ""),
                        "precipitation": payload.get("precipitation", ""),
                        "wind_speed_10m": payload.get("wind_speed_10m", ""),
                        "updated_at": str(asyncio.get_event_loop().time()),
                    },
                )
                await app.state.redis.expire(WEATHER_KEY, 3600)
            except Exception as exc:
                logger.info("weather loop degraded: %s", exc)
            await asyncio.sleep(600)


async def _refresh_fuel_context_once(
    redis_client: aioredis.Redis,
    *,
    force: bool = False,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    current = await redis_client.hgetall(FUEL_CONTEXT_KEY)
    if _as_bool(current.get("lock_manual"), False) and not force:
        await redis_client.hset(
            FUEL_CONTEXT_KEY,
            mapping={
                "fuel_sync_status": "manual_locked",
                "fuel_sync_checked_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        await redis_client.expire(FUEL_CONTEXT_KEY, FUEL_CONTEXT_TTL_SECONDS)
        return {"updated": False, "status": "manual_locked"}

    own_client = client is None
    http_client = client or httpx.AsyncClient(timeout=FUEL_PRICE_PROVIDER_TIMEOUT_S)
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        response = await http_client.get(FUEL_PRICE_PROVIDER_URL)
        response.raise_for_status()
        usd_per_gallon = _parse_fuel_price_usd_gallon(response.text, FUEL_GRADE)
        eur_per_liter = _usd_gallon_to_eur_liter(usd_per_gallon, FUEL_USD_TO_EUR_RATE)

        vehicle_consumption = _as_float(current.get("vehicle_consumption_l_100km"), DEFAULT_CONSUMPTION_L_100KM)
        platform_fee_pct = _as_float(current.get("platform_fee_pct"), DEFAULT_PLATFORM_FEE_PCT)
        lock_manual = _as_bool(current.get("lock_manual"), False)

        await redis_client.hset(
            FUEL_CONTEXT_KEY,
            mapping={
                "fuel_price_eur_l": round(float(eur_per_liter), 3),
                "fuel_price_usd_gallon": round(float(usd_per_gallon), 3),
                "vehicle_consumption_l_100km": round(float(vehicle_consumption), 3),
                "platform_fee_pct": round(float(platform_fee_pct), 3),
                "source": "fueleconomy_us",
                "provider": "fueleconomy",
                "provider_url": FUEL_PRICE_PROVIDER_URL,
                "fuel_grade": FUEL_GRADE,
                "usd_to_eur_rate": round(float(FUEL_USD_TO_EUR_RATE), 4),
                "lock_manual": "1" if lock_manual else "0",
                "fuel_sync_status": "ok",
                "fuel_sync_checked_at": now_iso,
                "updated_at": now_iso,
            },
        )
        await redis_client.expire(FUEL_CONTEXT_KEY, FUEL_CONTEXT_TTL_SECONDS)
        return {
            "updated": True,
            "status": "ok",
            "fuel_price_eur_l": round(float(eur_per_liter), 3),
            "fuel_price_usd_gallon": round(float(usd_per_gallon), 3),
            "fuel_grade": FUEL_GRADE,
        }
    except Exception as exc:
        logger.warning("fuel context refresh failed: %s", exc)
        await redis_client.hset(
            FUEL_CONTEXT_KEY,
            mapping={
                "fuel_sync_status": f"degraded:{type(exc).__name__}",
                "fuel_sync_checked_at": now_iso,
            },
        )
        await redis_client.expire(FUEL_CONTEXT_KEY, FUEL_CONTEXT_TTL_SECONDS)
        return {"updated": False, "status": f"degraded:{type(exc).__name__}"}
    finally:
        if own_client:
            await http_client.aclose()


async def _fuel_context_loop(app) -> None:
    await asyncio.sleep(8)
    redis_client: aioredis.Redis = app.state.redis
    async with httpx.AsyncClient(timeout=FUEL_PRICE_PROVIDER_TIMEOUT_S) as client:
        while True:
            try:
                await _refresh_fuel_context_once(redis_client, force=False, client=client)
            except Exception as exc:
                logger.info("fuel context loop degraded: %s", exc)
            await asyncio.sleep(max(120, FUEL_REFRESH_INTERVAL_S))


def _zone_id(lat: float, lon: float) -> str:
    lat_cell = round(round(lat / ZONE_STEP) * ZONE_STEP, 3)
    lon_cell = round(round(lon / ZONE_STEP) * ZONE_STEP, 3)
    return f"{lat_cell:.3f}_{lon_cell:.3f}"


async def _gbfs_loop(app) -> None:
    """Poll Citi Bike GBFS station_status every 60s and materialize zone-level demand signals."""
    await asyncio.sleep(5)

    station_coords: dict[str, tuple[float, float]] = {}

    async with httpx.AsyncClient(timeout=15) as client:
        # Load station coordinates once
        try:
            resp = await client.get(GBFS_STATION_INFO_URL)
            resp.raise_for_status()
            for s in resp.json().get("data", {}).get("stations", []):
                sid = str(s.get("station_id", ""))
                lat = float(s.get("lat", 0))
                lon = float(s.get("lon", 0))
                if NYC_LAT_MIN <= lat <= NYC_LAT_MAX and NYC_LON_MIN <= lon <= NYC_LON_MAX:
                    station_coords[sid] = (lat, lon)
            logger.info("gbfs loaded %d Citi Bike stations in NYC", len(station_coords))
        except Exception as exc:
            logger.warning("gbfs station_information fetch failed: %s", exc)

        while True:
            try:
                resp = await client.get(GBFS_STATION_STATUS_URL)
                resp.raise_for_status()
                stations = resp.json().get("data", {}).get("stations", [])

                zone_bikes: dict[str, list[int]] = {}
                zone_docks: dict[str, list[int]] = {}
                total_stations = 0

                for s in stations:
                    sid = str(s.get("station_id", ""))
                    if sid not in station_coords:
                        continue
                    lat, lon = station_coords[sid]
                    zid = _zone_id(lat, lon)
                    avail = int(s.get("num_bikes_available", 0))
                    docks = int(s.get("num_docks_available", 0))
                    zone_bikes.setdefault(zid, []).append(avail)
                    zone_docks.setdefault(zid, []).append(docks)
                    total_stations += 1

                r = app.state.redis
                pipe = r.pipeline(transaction=False)
                zones_updated = 0
                for zid in zone_bikes:
                    bikes = zone_bikes[zid]
                    docks = zone_docks.get(zid, [0])
                    total_capacity = sum(bikes) + sum(docks)
                    occupancy = sum(bikes) / max(total_capacity, 1)
                    # High occupancy = lots of bikes = low taxi demand (people have bikes)
                    # Low occupancy = no bikes = people need transport = higher taxi demand
                    demand_boost = max(0.0, 1.0 - occupancy) * 0.5
                    key = f"copilot:context:gbfs:zone:{zid}"
                    pipe.hset(key, mapping={
                        "zone_id": zid,
                        "stations_count": len(bikes),
                        "bikes_available": sum(bikes),
                        "docks_available": sum(docks),
                        "occupancy_ratio": round(occupancy, 3),
                        "demand_boost": round(demand_boost, 3),
                        "updated_at": str(asyncio.get_event_loop().time()),
                    })
                    pipe.expire(key, 300)

                    # Also update the main zone context with GBFS demand signal
                    ctx_key = f"{ZONE_CONTEXT_PREFIX}{zid}"
                    pipe.hset(ctx_key, mapping={
                        "gbfs_demand_boost": round(demand_boost, 3),
                        "gbfs_occupancy": round(occupancy, 3),
                        "gbfs_stations": len(bikes),
                    })
                    zones_updated += 1
                await pipe.execute()

                await r.hset(GBFS_KEY, mapping={
                    "status": "ok",
                    "stations_polled": str(total_stations),
                    "zones_updated": str(zones_updated),
                    "updated_at": str(asyncio.get_event_loop().time()),
                })
                await r.expire(GBFS_KEY, 600)

                if zones_updated:
                    logger.info("gbfs updated %d zones from %d stations", zones_updated, total_stations)
            except Exception as exc:
                logger.warning("gbfs poll failed: %s", exc)
                try:
                    await app.state.redis.hset(GBFS_KEY, mapping={
                        "status": f"degraded:{type(exc).__name__}",
                        "updated_at": str(asyncio.get_event_loop().time()),
                    })
                except Exception:
                    pass
            await asyncio.sleep(60)


async def _irve_loop(app) -> None:
    """Load NYC EV charging stations from OpenChargeMap and store in Redis.

    Refreshes every 6h. Field name "irve" is kept for Redis key compatibility;
    the source is OpenChargeMap (NYC).
    """
    await asyncio.sleep(10)

    params = {
        "output": "json",
        "countrycode": "US",
        "latitude": NYC_CENTER_LAT,
        "longitude": NYC_CENTER_LON,
        "distance": 30,
        "distanceunit": "KM",
        "maxresults": 2000,
        "compact": "true",
        "verbose": "false",
    }
    headers = {"User-Agent": "FleetStream/1.0 (driver-revenue-copilot)"}
    if OPENCHARGEMAP_API_KEY:
        headers["X-API-Key"] = OPENCHARGEMAP_API_KEY

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        while True:
            try:
                resp = await client.get(OPENCHARGEMAP_URL, params=params, headers=headers)
                resp.raise_for_status()
                stations = resp.json()
                if not isinstance(stations, list):
                    stations = []

                r = app.state.redis
                pipe = r.pipeline(transaction=False)
                count = 0

                for poi in stations:
                    addr = (poi.get("AddressInfo") or {})
                    try:
                        lat = float(addr.get("Latitude") or 0)
                        lon = float(addr.get("Longitude") or 0)
                    except (ValueError, TypeError):
                        continue

                    if not (NYC_LAT_MIN <= lat <= NYC_LAT_MAX and NYC_LON_MIN <= lon <= NYC_LON_MAX):
                        continue

                    station_id = str(poi.get("ID") or f"ocm_{count}")
                    nom = (addr.get("Title") or "")[:100]
                    connections = poi.get("Connections") or []
                    max_kw = 0.0
                    for c in connections:
                        try:
                            pw = float(c.get("PowerKW") or 0)
                            if pw > max_kw:
                                max_kw = pw
                        except (ValueError, TypeError):
                            continue
                    zid = _zone_id(lat, lon)

                    key = f"copilot:context:irve:station:{station_id}"
                    pipe.hset(key, mapping={
                        "station_id": station_id,
                        "lat": lat,
                        "lon": lon,
                        "nom": nom,
                        "puissance_kw": str(max_kw),
                        "zone_id": zid,
                        "source": "openchargemap",
                        "updated_at": str(asyncio.get_event_loop().time()),
                    })
                    pipe.expire(key, 86400)

                    pipe.geoadd("copilot:irve:geo", [lon, lat, station_id])
                    count += 1

                pipe.expire("copilot:irve:geo", 86400)
                await pipe.execute()

                await r.hset(IRVE_KEY, mapping={
                    "status": "ok",
                    "stations_loaded": str(count),
                    "source": "openchargemap_nyc",
                    "updated_at": str(asyncio.get_event_loop().time()),
                })
                await r.expire(IRVE_KEY, 86400)
                logger.info("openchargemap loaded %d EV charging stations in NYC area", count)

            except Exception as exc:
                logger.warning("openchargemap load failed: %s", exc)
                try:
                    await app.state.redis.hset(IRVE_KEY, mapping={
                        "status": f"degraded:{type(exc).__name__}",
                        "updated_at": str(asyncio.get_event_loop().time()),
                    })
                except Exception:
                    pass
            await asyncio.sleep(21600)  # Refresh every 6 hours


def start_copilot_background_tasks(app) -> list[asyncio.Task]:
    model_payload, quality_gate = load_copilot_model()
    app.state.copilot_model = model_payload
    app.state.copilot_model_quality_gate = quality_gate
    tasks = [
        asyncio.create_task(_weather_loop(app)),
        asyncio.create_task(_fuel_context_loop(app)),
        asyncio.create_task(_gbfs_loop(app)),
        asyncio.create_task(_irve_loop(app)),
    ]
    return tasks


def stop_copilot_background_tasks(tasks: list[asyncio.Task]) -> None:
    for task in tasks:
        task.cancel()


@copilot_router.get("", include_in_schema=False)
async def copilot_web() -> FileResponse:
    page = Path(__file__).parent / "static" / "copilot" / "index.html"
    if page.exists():
        return FileResponse(str(page), media_type="text/html")
    raise HTTPException(status_code=404, detail="copilot PWA page not found")


@copilot_router.post("/score-offer", response_model=ScoreOfferResponse)
async def score_offer(request: Request, body: ScoreOfferRequest) -> ScoreOfferResponse:
    redis_client: aioredis.Redis = request.app.state.redis

    payload: dict[str, Any] = {}
    if body.offer_id:
        offer_map = await redis_client.hgetall(f"{OFFER_KEY_PREFIX}{body.offer_id}")
        if offer_map:
            payload.update(offer_map)
    payload.update(body.model_dump(exclude_unset=True, exclude_none=True))
    payload["use_osrm"] = bool(payload.get("use_osrm", False))
    payload = await _merge_cost_context(redis_client, payload)
    route_meta: dict[str, Any] = {"route_source": "estimated", "route_notes": []}
    if payload["use_osrm"]:
        async with httpx.AsyncClient(timeout=OSRM_SCORE_TIMEOUT_S) as osrm_client:
            payload, route_meta = await _apply_osrm_route_context(redis_client, payload, osrm_client)

    features = build_feature_map(payload)
    heuristic_prob, eur_per_hour, reasons = _compute_heuristic_score(features)
    breakdown = cost_breakdown(features)

    model_payload = getattr(request.app.state, "copilot_model", None)
    model_prob, model_used = model_score(model_payload, features)
    accept_prob = model_prob if model_prob is not None else heuristic_prob

    decision, decision_threshold = _decision_for_offer(features, float(accept_prob))
    details = _build_explanation_details(
        features=features,
        accept_prob=float(accept_prob),
        decision_threshold=decision_threshold,
        route_meta=route_meta,
    )

    return ScoreOfferResponse(
        offer_id=payload.get("offer_id"),
        courier_id=payload.get("courier_id"),
        accept_score=round(float(accept_prob), 4),
        decision=decision,
        decision_threshold=round(float(decision_threshold), 4),
        eur_per_hour_net=round(float(eur_per_hour), 2),
        estimated_net_eur=round(float(features.get("estimated_net_eur", 0.0)), 2),
        target_hourly_net_eur=round(float(features.get("target_hourly_net_eur", 0.0)), 2),
        target_gap_eur_h=round(float(features.get("target_gap_eur_h", 0.0)), 2),
        costs=breakdown,
        route_source=str(route_meta.get("route_source", "estimated")),
        route_distance_km=round(float(route_meta.get("route_distance_km", features.get("total_distance_km", 0.0))), 3),
        route_duration_min=round(float(route_meta.get("route_duration_min", features.get("total_duration_min", 0.0))), 3),
        route_notes=list(route_meta.get("route_notes", [])),
        model_used=model_used,
        explanation=reasons,
        explanation_details=details,
    )


def _rank_offer_items(
    scored: list[dict[str, Any]],
    rank_by: str,
    reject_below_eur_h: float | None,
    *,
    default_reject_floor: float = RANK_REJECT_EUR_H_DEFAULT,
    top_pick_min_eur_h: float = RANK_TOP_PICK_MIN_EUR_H,
    top_pick_min_edge_eur_h: float = RANK_TOP_PICK_MIN_EDGE_EUR_H,
    below_target_slack_eur_h: float = RANK_BELOW_TARGET_SLACK_EUR_H,
) -> tuple[list[RankedOfferItem], float, float, float]:
    """Pure ranking + annotation layer, exposed separately from the handler
    so unit tests can exercise the sort/floor/top_pick logic without going
    through FastAPI + Redis + OSRM. Takes the list of already-scored dicts
    and returns `(items, best_eur_h, worst_eur_h, median_eur_h)`.
    """
    metric = rank_by if rank_by in {"eur_per_hour_net", "estimated_net_eur", "accept_score"} else "eur_per_hour_net"
    key_map = {
        "eur_per_hour_net": lambda it: (it["eur_per_hour_net"], it["estimated_net_eur"], it["accept_score"]),
        "estimated_net_eur": lambda it: (it["estimated_net_eur"], it["eur_per_hour_net"], it["accept_score"]),
        "accept_score": lambda it: (it["accept_score"], it["eur_per_hour_net"], it["estimated_net_eur"]),
    }
    tie_sorted = sorted(scored, key=lambda it: str(it.get("offer_id") or ""))
    ordered = sorted(tie_sorted, key=key_map[metric], reverse=True)

    default_reject_floor = _non_negative_float(default_reject_floor, RANK_REJECT_EUR_H_DEFAULT)
    top_pick_min_eur_h = _non_negative_float(top_pick_min_eur_h, RANK_TOP_PICK_MIN_EUR_H)
    top_pick_min_edge_eur_h = _non_negative_float(top_pick_min_edge_eur_h, RANK_TOP_PICK_MIN_EDGE_EUR_H)
    below_target_slack_eur_h = _non_negative_float(below_target_slack_eur_h, RANK_BELOW_TARGET_SLACK_EUR_H)

    hourly_values = [float(it["eur_per_hour_net"]) for it in ordered]
    best_eur_h = hourly_values[0] if hourly_values else 0.0
    worst_eur_h = hourly_values[-1] if hourly_values else 0.0
    median_eur_h = (
        sorted(hourly_values)[len(hourly_values) // 2] if hourly_values else 0.0
    )
    second_best_eur_h = hourly_values[1] if len(hourly_values) > 1 else best_eur_h
    effective_reject_floor = (
        _non_negative_float(reject_below_eur_h, default_reject_floor)
        if reject_below_eur_h is not None
        else default_reject_floor
    )

    items: list[RankedOfferItem] = []
    for idx, it in enumerate(ordered):
        eur_h = float(it["eur_per_hour_net"])
        target = float(it.get("target_hourly_net_eur") or 0.0)
        delta_top = round(eur_h - best_eur_h, 2)
        delta_median = round(eur_h - median_eur_h, 2)
        top_edge = eur_h - second_best_eur_h if idx == 0 else 0.0
        top_pick_eligible = (
            idx == 0
            and eur_h >= top_pick_min_eur_h
            and (
                len(hourly_values) <= 1
                or top_edge >= top_pick_min_edge_eur_h
            )
        )

        if eur_h < effective_reject_floor:
            recommendation = "reject"
        elif top_pick_eligible:
            recommendation = "top_pick"
        elif target > 0 and eur_h < (target - below_target_slack_eur_h):
            recommendation = "below_target"
        else:
            recommendation = "viable"

        items.append(RankedOfferItem(
            rank=idx + 1,
            top_pick=(recommendation == "top_pick"),
            recommendation=recommendation,
            offer_id=it.get("offer_id"),
            courier_id=it.get("courier_id"),
            accept_score=it["accept_score"],
            decision=it["decision"],
            decision_threshold=float(it.get("decision_threshold")) if it.get("decision_threshold") is not None else None,
            eur_per_hour_net=eur_h,
            estimated_net_eur=float(it["estimated_net_eur"]),
            delta_vs_top_eur_h=delta_top,
            delta_vs_median_eur_h=delta_median,
            costs=it.get("costs"),
            route_source=it.get("route_source"),
            route_distance_km=it.get("route_distance_km"),
            route_duration_min=it.get("route_duration_min"),
            model_used=it.get("model_used", "heuristic"),
            explanation=list(it.get("explanation") or []),
            explanation_details=list(it.get("explanation_details") or []),
        ))
    return items, best_eur_h, worst_eur_h, median_eur_h


async def _score_offer_payload(
    redis_client: aioredis.Redis,
    model_payload: dict[str, Any] | None,
    body: ScoreOfferRequest,
    osrm_client: httpx.AsyncClient | None,
    use_osrm: bool,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, float], float, float, list[str], str]:
    """Shared scoring pipeline for both /score-offer and /rank-offers.

    Returns `(payload, route_meta, features, accept_prob, eur_per_hour, reasons,
    model_used)`. Callers are expected to assemble the response model.
    """
    payload: dict[str, Any] = {}
    if body.offer_id:
        offer_map = await redis_client.hgetall(f"{OFFER_KEY_PREFIX}{body.offer_id}")
        if offer_map:
            payload.update(offer_map)
    payload.update(body.model_dump(exclude_unset=True, exclude_none=True))
    payload["use_osrm"] = bool(use_osrm or payload.get("use_osrm", False))
    payload = await _merge_cost_context(redis_client, payload)
    route_meta: dict[str, Any] = {"route_source": "estimated", "route_notes": []}
    if payload["use_osrm"] and osrm_client is not None:
        payload, route_meta = await _apply_osrm_route_context(redis_client, payload, osrm_client)

    features = build_feature_map(payload)
    heuristic_prob, eur_per_hour, reasons = _compute_heuristic_score(features)
    model_prob, model_used = model_score(model_payload, features)
    accept_prob = model_prob if model_prob is not None else heuristic_prob
    return payload, route_meta, features, float(accept_prob), float(eur_per_hour), reasons, model_used


@copilot_router.post("/rank-offers", response_model=RankOffersResponse)
async def rank_offers(request: Request, body: RankOffersRequest) -> RankOffersResponse:
    """Batch-score several offers and return them ranked by net €/hour.

    This is the endpoint a driver's app should call when it receives more than
    one simultaneous offer. Ranking by `eur_per_hour_net` optimises the right
    thing for food delivery: more money *per unit of time spent* — which is
    what moves the needle on daily earnings, and implicitly penalises long
    repositioning, fuel-hungry detours, and low-fare trips with long ETA.
    """
    redis_client: aioredis.Redis = request.app.state.redis
    model_payload = getattr(request.app.state, "copilot_model", None)

    scored: list[dict[str, Any]] = []
    osrm_client: httpx.AsyncClient | None = None
    try:
        if body.use_osrm:
            osrm_client = httpx.AsyncClient(timeout=OSRM_SCORE_TIMEOUT_S)

        for offer in body.offers:
            payload, route_meta, features, accept_prob, eur_per_hour, reasons, model_used = (
                await _score_offer_payload(
                    redis_client, model_payload, offer, osrm_client, body.use_osrm
                )
            )
            decision, decision_threshold = _decision_for_offer(features, float(accept_prob))
            details = _build_explanation_details(
                features=features,
                accept_prob=float(accept_prob),
                decision_threshold=decision_threshold,
                route_meta=route_meta,
            )
            breakdown = cost_breakdown(features)
            scored.append(
                _build_scored_offer_snapshot(
                    payload=payload,
                    features=features,
                    eur_per_hour=eur_per_hour,
                    accept_prob=accept_prob,
                    decision=decision,
                    decision_threshold=decision_threshold,
                    breakdown=breakdown,
                    route_meta=route_meta,
                    model_used=model_used,
                    reasons=reasons,
                    details=details,
                )
            )
    finally:
        if osrm_client is not None:
            await osrm_client.aclose()

    items, best_eur_h, worst_eur_h, median_eur_h = _rank_offer_items(
        scored, body.rank_by, body.reject_below_eur_h
    )
    top_pick_id = items[0].offer_id if items and items[0].top_pick else None

    return RankOffersResponse(
        count=len(items),
        ranked_by=body.rank_by,
        top_pick_offer_id=top_pick_id,
        best_eur_h=round(best_eur_h, 2),
        worst_eur_h=round(worst_eur_h, 2),
        median_eur_h=round(median_eur_h, 2),
        hourly_gain_vs_worst_eur_h=round(best_eur_h - worst_eur_h, 2),
        items=items,
    )


@copilot_router.get("/driver/{driver_id}/position")
async def driver_position(
    request: Request,
    driver_id: str,
    include_zone_context: bool = Query(False, description="Ajoute le contexte de la zone grille."),
):
    redis_client: aioredis.Redis = request.app.state.redis
    raw = await redis_client.hgetall(f"{COURIER_HASH_PREFIX}{driver_id}")

    lat = _as_float(raw.get("lat"), 0.0)
    lon = _as_float(raw.get("lon"), 0.0)
    available = bool(raw) and not (lat == 0.0 and lon == 0.0)

    payload: dict[str, Any] = {
        "driver_id": driver_id,
        "available": available,
        "status": raw.get("status"),
        "speed_kmh": round(_as_float(raw.get("speed_kmh"), 0.0), 2),
        "battery_pct": round(_as_float(raw.get("battery_pct"), 0.0), 2),
        "accuracy_m": round(_as_float(raw.get("accuracy_m"), 0.0), 2),
        "ts": raw.get("ts"),
        "updated_at": raw.get("updated_at"),
        "zone_id": raw.get("zone_id"),
    }
    if not available:
        payload["lat"] = None
        payload["lon"] = None
        payload["zone_cell"] = None
        payload["zone_context"] = None
        return payload

    payload["lat"] = round(lat, 6)
    payload["lon"] = round(lon, 6)
    zone_cell = _zone_id(lat, lon)
    payload["zone_cell"] = zone_cell
    payload["zone_context"] = None

    if include_zone_context:
        zone_raw = await redis_client.hgetall(f"{ZONE_CONTEXT_PREFIX}{zone_cell}")
        if zone_raw:
            payload["zone_context"] = _zone_context_payload(zone_raw, zone_id_hint=zone_cell)
    return payload


@copilot_router.post("/driver/{driver_id}/mission-report")
async def mission_report(request: Request, driver_id: str, body: MissionReportRequest):
    redis_client: aioredis.Redis = request.app.state.redis
    now = datetime.now(timezone.utc)

    mission_id = body.mission_id or f"mission_{int(now.timestamp() * 1000)}"
    predicted_delta: float | None = None
    realized_delta: float | None = None
    if body.predicted_potential_eur_h is not None and body.baseline_eur_h is not None:
        predicted_delta = round(float(body.predicted_potential_eur_h) - float(body.baseline_eur_h), 3)
    if body.realized_best_offer_eur_h is not None and body.baseline_eur_h is not None:
        realized_delta = round(float(body.realized_best_offer_eur_h) - float(body.baseline_eur_h), 3)

    success = body.success
    if success is None:
        if realized_delta is not None:
            success = realized_delta >= 0.0
        else:
            success = bool(body.final_distance_km <= 0.25)

    alignment_score: float | None = None
    if predicted_delta is not None and realized_delta is not None:
        alignment_score = round(_clamp(1.0 - (abs(realized_delta - predicted_delta) / 8.0), 0.0, 1.0), 4)

    strategy_norm = str(dispatch_strategy_profile(body.dispatch_strategy or "balanced").get("strategy", "balanced"))

    entry = {
        "mission_id": mission_id,
        "driver_id": driver_id,
        "zone_id": body.zone_id,
        "dispatch_strategy": strategy_norm,
        "arrival_threshold_km": (
            round(float(body.arrival_threshold_km), 3) if body.arrival_threshold_km is not None else None
        ),
        "eta_policy_limit_min": round(float(body.eta_policy_limit_min), 3) if body.eta_policy_limit_min is not None else None,
        "started_at": body.started_at,
        "completed_at": body.completed_at,
        "recorded_at": now.isoformat(),
        "elapsed_min": round(float(body.elapsed_min), 3),
        "initial_distance_km": round(float(body.initial_distance_km), 3),
        "final_distance_km": round(float(body.final_distance_km), 3),
        "predicted_eta_min": round(float(body.predicted_eta_min), 3) if body.predicted_eta_min is not None else None,
        "predicted_potential_eur_h": (
            round(float(body.predicted_potential_eur_h), 3) if body.predicted_potential_eur_h is not None else None
        ),
        "baseline_eur_h": round(float(body.baseline_eur_h), 3) if body.baseline_eur_h is not None else None,
        "realized_best_offer_eur_h": (
            round(float(body.realized_best_offer_eur_h), 3) if body.realized_best_offer_eur_h is not None else None
        ),
        "predicted_delta_eur_h": predicted_delta,
        "realized_delta_eur_h": realized_delta,
        "alignment_score": alignment_score,
        "route_source": body.route_source,
        "stop_reason": body.stop_reason,
        "success": bool(success),
        "target_lat": round(float(body.target_lat), 6) if body.target_lat is not None else None,
        "target_lon": round(float(body.target_lon), 6) if body.target_lon is not None else None,
    }

    key = _mission_journal_key(driver_id)
    await redis_client.lpush(key, json.dumps(entry, separators=(",", ":")))
    await redis_client.ltrim(key, 0, max(1, MISSION_JOURNAL_MAX_ITEMS) - 1)
    return entry


@copilot_router.get("/driver/{driver_id}/mission-journal")
async def mission_journal(
    request: Request,
    driver_id: str,
    limit: int = Query(20, ge=1, le=200),
):
    redis_client: aioredis.Redis = request.app.state.redis
    key = _mission_journal_key(driver_id)
    raw_items = await redis_client.lrange(key, 0, limit - 1)

    missions: list[dict[str, Any]] = []
    for raw in raw_items:
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        if isinstance(parsed, dict):
            missions.append(parsed)

    elapsed_values = [float(m.get("elapsed_min")) for m in missions if m.get("elapsed_min") is not None]
    predicted_values = [float(m.get("predicted_delta_eur_h")) for m in missions if m.get("predicted_delta_eur_h") is not None]
    realized_values = [float(m.get("realized_delta_eur_h")) for m in missions if m.get("realized_delta_eur_h") is not None]
    alignment_values = [float(m.get("alignment_score")) for m in missions if m.get("alignment_score") is not None]

    success_count = sum(1 for m in missions if bool(m.get("success")))
    realized_win_count = sum(1 for value in realized_values if value >= 0.0)
    strategy_counts: dict[str, int] = {}
    for mission in missions:
        strategy = str(mission.get("dispatch_strategy") or "balanced")
        strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1

    stats = {
        "missions_count": len(missions),
        "success_count": success_count,
        "success_rate_pct": round((success_count / len(missions) * 100), 2) if missions else None,
        "avg_elapsed_min": round(sum(elapsed_values) / len(elapsed_values), 3) if elapsed_values else None,
        "avg_predicted_delta_eur_h": (
            round(sum(predicted_values) / len(predicted_values), 3) if predicted_values else None
        ),
        "avg_realized_delta_eur_h": round(sum(realized_values) / len(realized_values), 3) if realized_values else None,
        "realized_win_rate_pct": (
            round(realized_win_count / len(realized_values) * 100, 2) if realized_values else None
        ),
        "avg_alignment_score": (
            round(sum(alignment_values) / len(alignment_values), 4) if alignment_values else None
        ),
        "strategy_counts": strategy_counts,
        "latest_completed_at": missions[0].get("completed_at") if missions else None,
    }

    return {
        "driver_id": driver_id,
        "count": len(missions),
        "stats": stats,
        "missions": missions,
    }


@copilot_router.get("/driver/{driver_id}/offers")
async def driver_offers(
    request: Request,
    driver_id: str,
    limit: int = Query(20, ge=1, le=200),
    min_accept_score: float = Query(0.0, ge=0.0, le=1.0),
    target_hourly_net_eur: float | None = Query(None, ge=0.0, le=150.0),
    sort_by: Literal["score", "net", "recent"] = Query("score"),
    use_osrm: bool = Query(
        False,
        description="If true, recompute routes via OSRM (more precise, slower).",
    ),
):
    redis_client: aioredis.Redis = request.app.state.redis
    offer_ids = await redis_client.lrange(f"{DRIVER_OFFERS_PREFIX}{driver_id}:offers", 0, limit - 1)
    if not offer_ids:
        return {"driver_id": driver_id, "count": 0, "offers": []}

    pipe = redis_client.pipeline(transaction=False)
    for offer_id in offer_ids:
        pipe.hgetall(f"{OFFER_KEY_PREFIX}{offer_id}")
    raw_offers = await pipe.execute()

    offers = []
    osrm_client: httpx.AsyncClient | None = None
    try:
        if use_osrm:
            osrm_client = httpx.AsyncClient(timeout=OSRM_SCORE_TIMEOUT_S)

        for off in raw_offers:
            if not off:
                continue
            payload = dict(off)
            payload["use_osrm"] = use_osrm
            if target_hourly_net_eur is not None:
                payload["target_hourly_net_eur"] = target_hourly_net_eur
            payload = await _merge_cost_context(redis_client, payload)
            route_meta: dict[str, Any] = {"route_source": "estimated", "route_notes": []}
            if use_osrm:
                payload, route_meta = await _apply_osrm_route_context(redis_client, payload, osrm_client)

            features = build_feature_map(payload)
            heur_prob, eur_per_hour, reasons = _compute_heuristic_score(features)
            model_payload = getattr(request.app.state, "copilot_model", None)
            model_prob, model_used = model_score(model_payload, features)
            accept_prob = model_prob if model_prob is not None else heur_prob
            if accept_prob < min_accept_score:
                continue
            decision, decision_threshold = _decision_for_offer(features, float(accept_prob))
            details = _build_explanation_details(
                features=features,
                accept_prob=float(accept_prob),
                decision_threshold=decision_threshold,
                route_meta=route_meta,
            )
            breakdown = cost_breakdown(features)

            offers.append(
                _build_scored_offer_snapshot(
                    payload=payload,
                    features=features,
                    eur_per_hour=eur_per_hour,
                    accept_prob=accept_prob,
                    decision=decision,
                    decision_threshold=decision_threshold,
                    breakdown=breakdown,
                    route_meta=route_meta,
                    model_used=model_used,
                    reasons=reasons,
                    details=details,
                    extras={
                        "zone_id": off.get("zone_id"),
                        "ts": off.get("ts"),
                    },
                )
            )
    finally:
        if osrm_client is not None:
            await osrm_client.aclose()

    if sort_by == "score":
        offers.sort(key=lambda x: (x["accept_score"], x["eur_per_hour_net"]), reverse=True)
    elif sort_by == "net":
        offers.sort(key=lambda x: (x["eur_per_hour_net"], x["accept_score"]), reverse=True)
    else:
        offers.sort(key=lambda x: x.get("ts") or "", reverse=True)

    accept_count = sum(1 for x in offers if x.get("decision") == "accept")
    return {
        "driver_id": driver_id,
        "count": len(offers),
        "accept_count": accept_count,
        "accept_rate_pct": round((accept_count / len(offers) * 100), 1) if offers else 0.0,
        "offers": offers,
    }


@copilot_router.get("/driver/{driver_id}/best-offers-around")
async def best_offers_around(
    request: Request,
    driver_id: str,
    lat: float | None = Query(None, ge=-90.0, le=90.0),
    lon: float | None = Query(None, ge=-180.0, le=180.0),
    radius_km: float = Query(4.0, ge=0.2, le=20.0),
    limit: int = Query(10, ge=1, le=50),
    scan_limit: int = Query(120, ge=10, le=400),
    min_accept_score: float = Query(0.25, ge=0.0, le=1.0),
    target_hourly_net_eur: float | None = Query(None, ge=0.0, le=150.0),
    use_osrm: bool = Query(
        False,
        description="If true, recompute routes via OSRM (more precise, slower).",
    ),
):
    redis_client: aioredis.Redis = request.app.state.redis

    lat, lon = await _resolve_driver_origin(redis_client, driver_id, lat, lon)

    offer_ids = await redis_client.lrange(f"{DRIVER_OFFERS_PREFIX}{driver_id}:offers", 0, scan_limit - 1)
    if not offer_ids:
        return {
            "driver_id": driver_id,
            "origin": {"lat": lat, "lon": lon},
            "radius_km": radius_km,
            "count": 0,
            "offers": [],
        }

    pipe = redis_client.pipeline(transaction=False)
    for offer_id in offer_ids:
        pipe.hgetall(f"{OFFER_KEY_PREFIX}{offer_id}")
    raw_offers = await pipe.execute()

    model_payload = getattr(request.app.state, "copilot_model", None)
    offers = []
    osrm_client: httpx.AsyncClient | None = None
    try:
        if use_osrm:
            osrm_client = httpx.AsyncClient(timeout=OSRM_SCORE_TIMEOUT_S)

        for off in raw_offers:
            if not off:
                continue

            payload = dict(off)
            payload["courier_id"] = payload.get("courier_id") or driver_id
            payload["courier_lat"] = lat
            payload["courier_lon"] = lon
            payload["use_osrm"] = use_osrm
            if target_hourly_net_eur is not None:
                payload["target_hourly_net_eur"] = target_hourly_net_eur

            pickup_lat = _coord_from_payload(payload, "pickup_lat")
            pickup_lon = _coord_from_payload(payload, "pickup_lon")
            if pickup_lat is not None and pickup_lon is not None:
                base_distance_km = _haversine_km(lat, lon, pickup_lat, pickup_lon)
                # Pre-filter on haversine: skip obviously out-of-range offers
                # BEFORE the expensive Redis merge + OSRM calls. Haversine is a
                # strict under-estimate of OSRM road distance, so anything that
                # fails this bound can never pass the post-OSRM radius filter.
                if base_distance_km > radius_km:
                    continue
                if payload.get("distance_to_pickup_km") in (None, ""):
                    payload["distance_to_pickup_km"] = round(base_distance_km, 3)

            payload = await _merge_cost_context(redis_client, payload)
            route_meta: dict[str, Any] = {"route_source": "estimated", "route_notes": []}
            if use_osrm:
                payload, route_meta = await _apply_osrm_route_context(redis_client, payload, osrm_client)

            features = build_feature_map(payload)
            pickup_distance_km = float(features.get("distance_to_pickup_km", 0.0))
            if pickup_distance_km > radius_km:
                continue

            heur_prob, eur_per_hour, reasons = _compute_heuristic_score(features)
            model_prob, model_used = model_score(model_payload, features)
            accept_prob = model_prob if model_prob is not None else heur_prob
            if accept_prob < min_accept_score:
                continue
            decision, decision_threshold = _decision_for_offer(features, float(accept_prob))
            details = _build_explanation_details(
                features=features,
                accept_prob=float(accept_prob),
                decision_threshold=decision_threshold,
                route_meta=route_meta,
            )

            rec_score, rec_action, rec_signals = recommendation_score(features, accept_prob)
            breakdown = cost_breakdown(features)
            offers.append(
                _build_scored_offer_snapshot(
                    payload=payload,
                    features=features,
                    eur_per_hour=eur_per_hour,
                    accept_prob=accept_prob,
                    decision=decision,
                    decision_threshold=decision_threshold,
                    breakdown=breakdown,
                    route_meta=route_meta,
                    model_used=model_used,
                    reasons=reasons,
                    details=details,
                    extras={
                        "zone_id": payload.get("zone_id"),
                        "ts": payload.get("ts"),
                        "distance_to_pickup_km": round(pickup_distance_km, 3),
                        "recommendation_score": rec_score,
                        "recommendation_action": rec_action,
                        "recommendation_signals": rec_signals,
                    },
                )
            )
    finally:
        if osrm_client is not None:
            await osrm_client.aclose()

    offers.sort(
        key=lambda x: (x["recommendation_score"], x["accept_score"], x["eur_per_hour_net"]),
        reverse=True,
    )
    offers = offers[:limit]
    action_counts = {
        "accept": sum(1 for x in offers if x["recommendation_action"] == "accept"),
        "consider": sum(1 for x in offers if x["recommendation_action"] == "consider"),
        "skip": sum(1 for x in offers if x["recommendation_action"] == "skip"),
    }

    return {
        "driver_id": driver_id,
        "origin": {"lat": round(float(lat), 6), "lon": round(float(lon), 6)},
        "radius_km": radius_km,
        "count": len(offers),
        "action_counts": action_counts,
        "offers": offers,
    }


@copilot_router.get("/driver/{driver_id}/instant-dispatch")
async def instant_dispatch(
    request: Request,
    driver_id: str,
    lat: float | None = Query(None, ge=-90.0, le=90.0),
    lon: float | None = Query(None, ge=-180.0, le=180.0),
    around_radius_km: float = Query(4.0, ge=0.2, le=20.0),
    around_limit: int = Query(8, ge=1, le=30),
    scan_limit: int = Query(120, ge=10, le=400),
    zone_top_k: int = Query(5, ge=1, le=20),
    min_accept_score: float = Query(0.25, ge=0.0, le=1.0),
    max_reposition_eta_min: float = Query(DEFAULT_MAX_REPOSITION_ETA_MIN, ge=3.0, le=60.0),
    forecast_horizon_min: float = Query(DEFAULT_DISPATCH_FORECAST_HORIZON_MIN, ge=10.0, le=90.0),
    dispatch_strategy: Literal["conservative", "balanced", "aggressive"] = Query("balanced"),
    target_hourly_net_eur: float | None = Query(None, ge=0.0, le=150.0),
    use_osrm: bool = Query(
        False,
        description="If true, recompute routes via OSRM (more precise, slower).",
    ),
):
    redis_client: aioredis.Redis = request.app.state.redis
    lat, lon = await _resolve_driver_origin(redis_client, driver_id, lat, lon)

    around_payload = await best_offers_around(
        request,
        driver_id,
        lat=lat,
        lon=lon,
        radius_km=around_radius_km,
        limit=around_limit,
        scan_limit=scan_limit,
        min_accept_score=min_accept_score,
        target_hourly_net_eur=target_hourly_net_eur,
        use_osrm=use_osrm,
    )
    local_offers_raw = around_payload.get("offers", [])
    local_offers = [offer for offer in local_offers_raw if isinstance(offer, dict)]
    local_best = local_offers[0] if local_offers else None

    zone_scan_size = min(50, max(zone_top_k * 3, zone_top_k + 4))
    zone_payload = await next_best_zone(request, driver_id, top_k=zone_scan_size, lat=lat, lon=lon)
    zones_raw = zone_payload.get("recommendations", [])
    zones = [zone for zone in zones_raw if isinstance(zone, dict)]

    fuel_context = await redis_client.hgetall(FUEL_CONTEXT_KEY)
    fuel_price = _as_float(fuel_context.get("fuel_price_eur_l"), DEFAULT_FUEL_PRICE_EUR_L)
    consumption = _as_float(fuel_context.get("vehicle_consumption_l_100km"), DEFAULT_CONSUMPTION_L_100KM)

    target_hourly = target_hourly_net_eur
    if target_hourly is None and local_best is not None:
        target_hourly = _as_float(local_best.get("target_hourly_net_eur"), 18.0)
    if target_hourly is None:
        target_hourly = 18.0

    strategy = dispatch_strategy_profile(dispatch_strategy)
    effective_max_eta = _clamp(
        max_reposition_eta_min * float(strategy.get("eta_limit_multiplier", 1.0)),
        3.0,
        60.0,
    )
    effective_forecast_horizon = _clamp(
        forecast_horizon_min * float(strategy.get("forecast_horizon_multiplier", 1.0)),
        10.0,
        90.0,
    )

    stay_score = _as_float(local_best.get("recommendation_score"), 0.0) if local_best else 0.0
    stay_gap_eur_h = _as_float(local_best.get("target_gap_eur_h"), 0.0) if local_best else -6.0
    stay_eur_h = _as_float(local_best.get("eur_per_hour_net"), 0.0) if local_best else 0.0

    reposition_options: list[dict[str, Any]] = []
    osrm_client: httpx.AsyncClient | None = None
    try:
        if use_osrm:
            osrm_client = httpx.AsyncClient(timeout=OSRM_SCORE_TIMEOUT_S)

        # Resolve every zone's routing leg in parallel. With use_osrm=true and
        # ~15 zones this cuts wall-clock from (15 * ~250ms) sequential down to
        # one concurrent batch. Pure-heuristic legs still go through the same
        # path so the code stays uniform.
        leg_specs: list[tuple[dict[str, Any], str, tuple[float, float], float]] = []
        for zone in zones:
            zone_id = str(zone.get("zone_id") or "")
            coords = _resolve_zone_coordinates(zone_id)
            if coords is None:
                continue
            traffic = max(_as_float(zone.get("traffic_factor"), 1.0), 0.2)
            leg_specs.append((zone, zone_id, coords, traffic))

        leg_results: list[tuple[float, float, str]] = []
        if leg_specs:
            leg_results = await asyncio.gather(
                *[
                    _estimate_reposition_leg(
                        origin_lat=lat,
                        origin_lon=lon,
                        dest_lat=coords[0],
                        dest_lon=coords[1],
                        traffic_factor=traffic,
                        use_osrm=use_osrm,
                        osrm_client=osrm_client,
                    )
                    for (_zone, _zid, coords, traffic) in leg_specs
                ]
            )

        for (zone, zone_id, coords, _), leg in zip(leg_specs, leg_results):
            demand = _as_float(zone.get("demand_index"), 1.0)
            supply = max(_as_float(zone.get("supply_index"), 1.0), 0.2)
            weather = _as_float(zone.get("weather_factor"), 1.0)
            traffic = max(_as_float(zone.get("traffic_factor"), 1.0), 0.2)
            pressure_ratio = demand / supply
            gbfs_demand_boost = _as_float(zone.get("gbfs_demand_boost"), 0.0)
            demand_trend = _as_float(zone.get("demand_trend_ema"), _as_float(zone.get("demand_trend"), (demand - 1.0) * 0.12))
            opportunity_current = max(_as_float(zone.get("opportunity_score"), (pressure_ratio * weather) / traffic), 0.0)

            forecast = _forecast_zone_metrics(
                demand_index=demand,
                supply_index=supply,
                weather_factor=weather,
                traffic_factor=traffic,
                gbfs_demand_boost=gbfs_demand_boost,
                demand_trend=demand_trend,
                horizon_minutes=effective_forecast_horizon,
            )
            opportunity_current_weight = float(strategy.get("opportunity_current_weight", 0.45))
            opportunity_forecast_weight = float(strategy.get("opportunity_forecast_weight", 0.55))
            opportunity = max(
                (opportunity_current * opportunity_current_weight)
                + (forecast["forecast_opportunity_score"] * opportunity_forecast_weight),
                0.0,
            )

            route_distance_km, eta_min, route_source = leg
            if eta_min > effective_max_eta * 1.8:
                continue

            opportunity_component = _clamp((opportunity - 0.8) / 1.4, 0.0, 1.0)
            eta_component = _clamp(1.0 - (eta_min / effective_max_eta), 0.0, 1.0)
            pressure_component = _clamp((forecast["forecast_pressure_ratio"] - 0.85) / 1.2, 0.0, 1.0)
            target_need_component = _clamp((-stay_gap_eur_h + 4.0) / 12.0, 0.0, 1.0)
            cost_model = _reposition_cost_model(
                route_distance_km=route_distance_km,
                eta_min=eta_min,
                fuel_price_eur_l=fuel_price,
                vehicle_consumption_l_100km=consumption,
                target_hourly_net_eur=float(target_hourly),
                traffic_factor=traffic,
                forecast_volatility=forecast["forecast_volatility"],
            )

            potential_factor = _clamp(0.85 + ((opportunity - 1.0) * 0.45), 0.65, 1.65)
            estimated_potential_eur_h = float(target_hourly) * potential_factor
            risk_adjusted_potential_eur_h = max(
                0.0,
                estimated_potential_eur_h - (cost_model["reposition_total_cost_eur"] * 0.85),
            )
            net_gain_vs_stay = risk_adjusted_potential_eur_h - stay_eur_h
            net_gain_component = _clamp((net_gain_vs_stay + 5.0) / 14.0, 0.0, 1.0)

            dispatch_score = _clamp(
                float(strategy.get("weight_opportunity", 0.32)) * opportunity_component
                + float(strategy.get("weight_eta", 0.16)) * eta_component
                + float(strategy.get("weight_pressure", 0.14)) * pressure_component
                + float(strategy.get("weight_target_need", 0.12)) * target_need_component
                + float(strategy.get("weight_net_gain", 0.2)) * net_gain_component
                - float(strategy.get("penalty_cost", 0.14)) * cost_model["reposition_cost_penalty"]
                - float(strategy.get("penalty_volatility", 0.08)) * forecast["forecast_volatility"],
                0.0,
                1.0,
            )

            reposition_options.append(
                {
                    "zone_id": zone_id,
                    "zone_lat": round(float(coords[0]), 6),
                    "zone_lon": round(float(coords[1]), 6),
                    "opportunity_score": round(float(opportunity), 3),
                    "current_opportunity_score": round(float(opportunity_current), 3),
                    "forecast_opportunity_score": round(float(forecast["forecast_opportunity_score"]), 3),
                    "dispatch_score": round(float(dispatch_score), 4),
                    "estimated_potential_eur_h": round(float(estimated_potential_eur_h), 2),
                    "risk_adjusted_potential_eur_h": round(float(risk_adjusted_potential_eur_h), 2),
                    "net_gain_vs_stay_eur_h": round(float(net_gain_vs_stay), 2),
                    "route_distance_km": round(float(route_distance_km), 3),
                    "eta_min": round(float(eta_min), 2),
                    "route_source": route_source,
                    "travel_cost_eur": round(float(cost_model["travel_cost_eur"]), 2),
                    "time_cost_eur": round(float(cost_model["time_cost_eur"]), 2),
                    "risk_cost_eur": round(float(cost_model["risk_cost_eur"]), 2),
                    "reposition_total_cost_eur": round(float(cost_model["reposition_total_cost_eur"]), 2),
                    "reposition_cost_penalty": round(float(cost_model["reposition_cost_penalty"]), 3),
                    "demand_index": round(float(demand), 3),
                    "supply_index": round(float(supply), 3),
                    "gbfs_demand_boost": round(float(gbfs_demand_boost), 3),
                    "demand_trend": round(float(demand_trend), 3),
                    "forecast_demand_index": round(float(forecast["forecast_demand_index"]), 3),
                    "forecast_supply_index": round(float(forecast["forecast_supply_index"]), 3),
                    "forecast_pressure_ratio": round(float(forecast["forecast_pressure_ratio"]), 3),
                    "forecast_volatility": round(float(forecast["forecast_volatility"]), 3),
                    "weather_factor": round(float(weather), 3),
                    "traffic_factor": round(float(traffic), 3),
                }
            )
    finally:
        if osrm_client is not None:
            await osrm_client.aclose()

    reposition_options.sort(
        key=lambda x: (x["dispatch_score"], x["risk_adjusted_potential_eur_h"], -x["eta_min"]),
        reverse=True,
    )
    reposition_options = reposition_options[:zone_top_k]
    best_reposition = reposition_options[0] if reposition_options else None

    move_score = _as_float(best_reposition.get("dispatch_score"), 0.0) if best_reposition else 0.0
    move_eta_min = _as_float(best_reposition.get("eta_min"), 999.0) if best_reposition else 999.0
    move_potential_eur_h = (
        _as_float(best_reposition.get("risk_adjusted_potential_eur_h"), 0.0) if best_reposition else 0.0
    )
    move_net_gain_eur_h = _as_float(best_reposition.get("net_gain_vs_stay_eur_h"), 0.0) if best_reposition else 0.0
    decision, confidence, reasons = _dispatch_decision(
        stay_score=stay_score,
        stay_gap_eur_h=stay_gap_eur_h,
        stay_eur_h=stay_eur_h,
        move_score=move_score,
        move_eta_min=move_eta_min,
        move_potential_eur_h=move_potential_eur_h,
        max_reposition_eta_min=effective_max_eta,
        move_net_gain_eur_h=move_net_gain_eur_h,
        negative_net_gain_guard=float(strategy.get("negative_net_gain_guard", -1.0)),
        delta_threshold=float(strategy.get("delta_threshold", 0.1)),
        min_net_gain_for_reposition=float(strategy.get("min_net_gain_for_reposition", -0.5)),
    )
    if not reposition_options and zones:
        reasons.append("zone_coordinates_unavailable")

    stay_option = _dispatch_offer_summary(local_best) if local_best is not None else None
    local_candidates = [_dispatch_offer_summary(offer) for offer in local_offers[:3]]

    return {
        "driver_id": driver_id,
        "origin": {"lat": round(float(lat), 6), "lon": round(float(lon), 6)},
        "decision": decision,
        "confidence": round(float(confidence), 4),
        "reasons": reasons,
        "dispatch_strategy": str(strategy.get("strategy", "balanced")),
        "target_hourly_net_eur": round(float(target_hourly), 2),
        "max_reposition_eta_min": round(float(max_reposition_eta_min), 2),
        "effective_max_reposition_eta_min": round(float(effective_max_eta), 2),
        "forecast_horizon_min": round(float(forecast_horizon_min), 2),
        "effective_forecast_horizon_min": round(float(effective_forecast_horizon), 2),
        "use_osrm": use_osrm,
        "stay_option": stay_option,
        "reposition_option": best_reposition,
        "local_candidates_count": len(local_offers),
        "reposition_candidates_count": len(reposition_options),
        "local_candidates": local_candidates,
        "reposition_candidates": reposition_options,
    }


def _rank_zone_recommendations(
    zones: list[dict[str, Any]],
    *,
    driver_lat: float | None,
    driver_lon: float | None,
    home_lat: float | None = None,
    home_lon: float | None = None,
    max_distance_km: float | None = None,
    distance_weight: float = 0.0,
    reference_km: float = 15.0,
    fuel_price_eur_l: float = DEFAULT_FUEL_PRICE_EUR_L,
    consumption_l_100km: float = DEFAULT_CONSUMPTION_L_100KM,
) -> list[dict[str, Any]]:
    """Enrich zone dicts with distance/fuel/homeward metadata and sort by adjusted score.

    The pure helper is factored out so it can be unit-tested without Redis.
    When driver position is unknown the function degrades gracefully: zones keep
    their base opportunity_score and original ordering is preserved.
    """
    driver_known = driver_lat is not None and driver_lon is not None
    home_known = home_lat is not None and home_lon is not None
    dist_driver_home = (
        _haversine_km(float(driver_lat), float(driver_lon), float(home_lat), float(home_lon))
        if driver_known and home_known
        else 0.0
    )

    enriched: list[dict[str, Any]] = []
    for zone in zones:
        base = max(float(zone.get("opportunity_score", 0.0)), 0.0)
        zone_id = str(zone.get("zone_id") or "")
        coords = _resolve_zone_coordinates(zone_id) if zone_id else None

        item = dict(zone)
        item["distance_km"] = None
        item["reposition_cost_eur"] = None
        item["reposition_time_min"] = None
        item["homeward_factor"] = None
        item["distance_penalty_pct"] = 0.0
        item["homeward_gain_pct"] = 0.0
        item["adjusted_opportunity_score"] = round(base, 3)

        if not driver_known or coords is None:
            enriched.append(item)
            continue

        dlat, dlon = coords
        distance_km = _haversine_km(float(driver_lat), float(driver_lon), dlat, dlon)
        if max_distance_km is not None and distance_km > float(max_distance_km):
            continue

        fuel_cost = distance_km * (consumption_l_100km / 100.0) * fuel_price_eur_l
        reposition_min = (distance_km / max(REPOSITION_BASE_SPEED_KMH, 1.0)) * 60.0

        normalized = min(distance_km / max(reference_km, 0.5), 1.0)
        penalty_pct = distance_weight * normalized

        homeward_factor = None
        gain_pct = 0.0
        if home_known and dist_driver_home > 0.1:
            dist_zone_home = _haversine_km(dlat, dlon, float(home_lat), float(home_lon))
            progress = (dist_driver_home - dist_zone_home) / dist_driver_home
            homeward_factor = max(-1.0, min(1.0, progress))
            if homeward_factor > 0:
                gain_pct = homeward_factor * distance_weight * 0.6

        adjusted = max(0.0, base * (1.0 - penalty_pct + gain_pct))

        item["distance_km"] = round(distance_km, 3)
        item["reposition_cost_eur"] = round(fuel_cost, 3)
        item["reposition_time_min"] = round(reposition_min, 2)
        item["distance_penalty_pct"] = round(penalty_pct, 4)
        item["homeward_factor"] = round(homeward_factor, 3) if homeward_factor is not None else None
        item["homeward_gain_pct"] = round(gain_pct, 4)
        item["adjusted_opportunity_score"] = round(adjusted, 3)
        enriched.append(item)

    enriched.sort(key=lambda z: z["adjusted_opportunity_score"], reverse=True)
    return enriched


async def _load_zone_context_payloads(redis_client: aioredis.Redis) -> list[dict[str, Any]]:
    zone_keys: list[str] = []
    async for key in redis_client.scan_iter(match=f"{ZONE_CONTEXT_PREFIX}*"):
        zone_keys.append(key)

    zone_hashes: list[dict[str, Any]] = []
    if zone_keys:
        pipe = redis_client.pipeline(transaction=False)
        for key in zone_keys:
            pipe.hgetall(key)
        zone_hashes = await pipe.execute()

    zones: list[dict[str, Any]] = []
    for zone in zone_hashes:
        if not zone:
            continue
        context_payload = _zone_context_payload(zone)
        demand = float(context_payload.get("demand_index") or 1.0)
        supply = max(float(context_payload.get("supply_index") or 1.0), 0.2)
        weather = float(context_payload.get("weather_factor") or 1.0)
        traffic = max(float(context_payload.get("traffic_factor") or 1.0), 0.2)
        opportunity = (demand / supply) * weather / traffic
        context_payload["opportunity_score"] = round(opportunity, 4)
        zones.append(context_payload)
    return zones


def _rank_shift_plan_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    tied = sorted(items, key=lambda it: str(it.get("zone_id") or ""))
    return sorted(
        tied,
        key=lambda it: (
            float(it.get("shift_score", 0.0)),
            float(it.get("estimated_net_eur_h", 0.0)),
            float(it.get("confidence", 0.0)),
        ),
        reverse=True,
    )


def _shift_plan_confidence(
    *,
    shift_score: float,
    forecast_volatility: float,
    context_fallback_applied: bool,
    context_stale_sources: int,
) -> float:
    score_component = _clamp((float(shift_score) - 0.7) / 1.15, 0.0, 1.0)
    volatility_component = 1.0 - _clamp(float(forecast_volatility), 0.0, 1.0)
    stale_penalty = _clamp(float(context_stale_sources) / 5.0, 0.0, 1.0) * 0.12
    fallback_penalty = 0.08 if bool(context_fallback_applied) else 0.0
    confidence = 0.35 + (score_component * 0.43) + (volatility_component * 0.24) - stale_penalty - fallback_penalty
    return round(_clamp(confidence, 0.08, 0.98), 4)


def _shift_plan_reasons(
    *,
    zone: dict[str, Any],
    horizon_min: int,
    eta_min: float,
    forecast_pressure_ratio: float,
    event_pressure: float,
    temporal_pressure: float,
    demand_trend: float,
    context_fallback_applied: bool,
) -> list[str]:
    reasons: list[str] = []
    if forecast_pressure_ratio >= 1.25:
        reasons.append(f"Strong forecast pressure ({forecast_pressure_ratio:.2f}) in next {horizon_min} min.")
    elif forecast_pressure_ratio >= 1.05:
        reasons.append(f"Healthy demand/supply outlook ({forecast_pressure_ratio:.2f}) over {horizon_min} min.")

    if event_pressure >= 0.16:
        reasons.append(f"Nearby events are likely to boost demand now ({event_pressure:.2f}).")
    if temporal_pressure >= 0.1:
        reasons.append(f"Favorable time window signal active ({temporal_pressure:.2f}).")
    if demand_trend >= 0.06:
        reasons.append(f"Demand trend is rising in this zone ({demand_trend:.2f}).")

    if eta_min <= 10.0:
        reasons.append(f"Quick reposition ({eta_min:.1f} min) keeps setup friction low.")
    elif eta_min <= 18.0:
        reasons.append(f"Reposition ETA is manageable ({eta_min:.1f} min).")

    if bool(context_fallback_applied):
        reasons.append("Context freshness fallback is active, confidence slightly reduced.")

    if not reasons:
        zone_id = str(zone.get("zone_id") or "zone")
        reasons.append(f"Balanced context and reachable position for zone {zone_id}.")
    return reasons[:4]


def _build_shift_plan_item(
    *,
    zone: dict[str, Any],
    horizon_min: int,
    target_hourly_net_eur: float,
    route_distance_km: float,
    eta_min: float,
    reposition_cost: dict[str, float],
    forecast: dict[str, float],
) -> dict[str, Any]:
    current_opportunity = max(_as_float(zone.get("opportunity_score"), 0.0), 0.0)
    forecast_opportunity = max(_as_float(forecast.get("forecast_opportunity_score"), 0.0), 0.0)
    forecast_weight = 0.56 if int(horizon_min) == 60 else 0.64
    current_weight = 1.0 - forecast_weight
    shift_score = max((current_opportunity * current_weight) + (forecast_opportunity * forecast_weight), 0.0)

    gross_multiplier = _clamp(0.82 + ((shift_score - 1.0) * 0.48), 0.6, 1.85)
    estimated_gross_eur_h = max(float(target_hourly_net_eur) * gross_multiplier, 0.0)
    reposition_penalty_hourly = max(float(reposition_cost.get("reposition_total_cost_eur", 0.0)), 0.0) * (60.0 / horizon_min)
    estimated_net_eur_h = max(estimated_gross_eur_h - reposition_penalty_hourly, 0.0)
    net_gain_vs_target = estimated_net_eur_h - float(target_hourly_net_eur)

    event_pressure = max(_as_float(zone.get("event_pressure"), 0.0), 0.0)
    temporal_pressure = max(_as_float(zone.get("temporal_pressure"), 0.0), 0.0)
    demand_trend = _as_float(zone.get("demand_trend_ema"), _as_float(zone.get("demand_trend"), 0.0))
    forecast_pressure_ratio = max(_as_float(forecast.get("forecast_pressure_ratio"), 0.0), 0.0)
    forecast_volatility = _clamp(_as_float(forecast.get("forecast_volatility"), 0.0), 0.0, 1.0)
    context_fallback_applied = _as_bool(zone.get("context_fallback_applied"), False)
    context_stale_sources = int(max(_as_float(zone.get("context_stale_sources"), 0.0), 0.0))

    reasons = _shift_plan_reasons(
        zone=zone,
        horizon_min=int(horizon_min),
        eta_min=float(eta_min),
        forecast_pressure_ratio=forecast_pressure_ratio,
        event_pressure=event_pressure,
        temporal_pressure=temporal_pressure,
        demand_trend=demand_trend,
        context_fallback_applied=context_fallback_applied,
    )
    confidence = _shift_plan_confidence(
        shift_score=shift_score,
        forecast_volatility=forecast_volatility,
        context_fallback_applied=context_fallback_applied,
        context_stale_sources=context_stale_sources,
    )

    why_now = reasons[0] if reasons else "Balanced short-term opportunity."
    coords = _resolve_zone_coordinates(str(zone.get("zone_id") or ""))
    zone_lat = float(coords[0]) if coords is not None else None
    zone_lon = float(coords[1]) if coords is not None else None

    return {
        "zone_id": str(zone.get("zone_id") or "unknown_zone"),
        "zone_lat": round(zone_lat, 6) if zone_lat is not None else None,
        "zone_lon": round(zone_lon, 6) if zone_lon is not None else None,
        "shift_score": round(float(shift_score), 4),
        "confidence": float(confidence),
        "estimated_net_eur_h": round(float(estimated_net_eur_h), 2),
        "estimated_gross_eur_h": round(float(estimated_gross_eur_h), 2),
        "net_gain_vs_target_eur_h": round(float(net_gain_vs_target), 2),
        "horizon_min": int(horizon_min),
        "why_now": why_now,
        "reasons": reasons,
        "demand_index": round(_as_float(zone.get("demand_index"), 1.0), 3),
        "supply_index": round(max(_as_float(zone.get("supply_index"), 1.0), 0.2), 3),
        "weather_factor": round(_as_float(zone.get("weather_factor"), 1.0), 3),
        "traffic_factor": round(max(_as_float(zone.get("traffic_factor"), 1.0), 0.2), 3),
        "event_pressure": round(event_pressure, 4),
        "temporal_pressure": round(temporal_pressure, 4),
        "forecast_pressure_ratio": round(forecast_pressure_ratio, 3),
        "forecast_volatility": round(forecast_volatility, 3),
        "distance_km": round(float(route_distance_km), 3),
        "eta_min": round(float(eta_min), 2),
        "reposition_total_cost_eur": round(float(reposition_cost.get("reposition_total_cost_eur", 0.0)), 2),
        "context_fallback_applied": bool(context_fallback_applied),
        "freshness_policy": zone.get("freshness_policy"),
    }


@copilot_router.get("/driver/{driver_id}/shift-plan", response_model=ShiftPlanResponse)
async def shift_plan(
    request: Request,
    driver_id: str,
    horizon_min: int = Query(60, ge=60, le=90),
    top_k: int = Query(SHIFT_PLAN_TOP_K_DEFAULT, ge=3, le=20),
    lat: float | None = Query(None, ge=-90.0, le=90.0),
    lon: float | None = Query(None, ge=-180.0, le=180.0),
    max_distance_km: float | None = Query(None, ge=0.5, le=50.0),
    distance_weight: float = Query(0.3, ge=0.0, le=1.0),
    target_hourly_net_eur: float | None = Query(None, ge=0.0, le=150.0),
    use_osrm: bool = Query(
        False,
        description="If true, use OSRM for reposition leg estimation (more precise, slower).",
    ),
):
    if horizon_min not in (60, 90):
        raise HTTPException(status_code=422, detail="horizon_min must be 60 or 90")

    redis_client: aioredis.Redis = request.app.state.redis
    driver_lat, driver_lon = await _resolve_driver_origin(redis_client, driver_id, lat, lon)

    zones = await _load_zone_context_payloads(redis_client)
    if not zones:
        return {
            "driver_id": driver_id,
            "origin_lat": round(float(driver_lat), 6),
            "origin_lon": round(float(driver_lon), 6),
            "horizon_min": int(horizon_min),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "target_hourly_net_eur": round(
                float(target_hourly_net_eur if target_hourly_net_eur is not None else SHIFT_PLAN_DEFAULT_TARGET_EUR_H),
                2,
            ),
            "source": "zone_context_v2",
            "count": 0,
            "items": [],
        }

    fuel_context = await redis_client.hgetall(FUEL_CONTEXT_KEY)
    fuel_price = _as_float(fuel_context.get("fuel_price_eur_l"), DEFAULT_FUEL_PRICE_EUR_L)
    consumption = _as_float(fuel_context.get("vehicle_consumption_l_100km"), DEFAULT_CONSUMPTION_L_100KM)
    target_hourly = float(
        target_hourly_net_eur if target_hourly_net_eur is not None else SHIFT_PLAN_DEFAULT_TARGET_EUR_H
    )
    if target_hourly <= 0:
        target_hourly = SHIFT_PLAN_DEFAULT_TARGET_EUR_H

    osrm_client: httpx.AsyncClient | None = None
    items: list[dict[str, Any]] = []
    try:
        if use_osrm:
            osrm_client = httpx.AsyncClient(timeout=OSRM_SCORE_TIMEOUT_S)

        leg_specs: list[tuple[dict[str, Any], tuple[float, float], float]] = []
        for zone in zones:
            zone_id = str(zone.get("zone_id") or "")
            coords = _resolve_zone_coordinates(zone_id)
            if coords is None:
                continue
            traffic = max(_as_float(zone.get("traffic_factor"), 1.0), 0.2)
            leg_specs.append((zone, coords, traffic))

        leg_results: list[tuple[float, float, str]] = []
        if leg_specs:
            leg_results = await asyncio.gather(
                *[
                    _estimate_reposition_leg(
                        origin_lat=float(driver_lat),
                        origin_lon=float(driver_lon),
                        dest_lat=coords[0],
                        dest_lon=coords[1],
                        traffic_factor=traffic,
                        use_osrm=use_osrm,
                        osrm_client=osrm_client,
                    )
                    for (_zone, coords, traffic) in leg_specs
                ]
            )

        for (zone, _coords, traffic), leg in zip(leg_specs, leg_results):
            route_distance_km, eta_min, _route_source = leg
            if max_distance_km is not None and route_distance_km > float(max_distance_km):
                continue

            demand = _as_float(zone.get("demand_index"), 1.0)
            supply = max(_as_float(zone.get("supply_index"), 1.0), 0.2)
            weather = _as_float(zone.get("weather_factor"), 1.0)
            gbfs_demand_boost = _as_float(zone.get("gbfs_demand_boost"), 0.0)
            demand_trend = _as_float(zone.get("demand_trend_ema"), _as_float(zone.get("demand_trend"), 0.0))

            forecast = _forecast_zone_metrics(
                demand_index=demand,
                supply_index=supply,
                weather_factor=weather,
                traffic_factor=traffic,
                gbfs_demand_boost=gbfs_demand_boost,
                demand_trend=demand_trend,
                horizon_minutes=float(horizon_min),
            )
            reposition = _reposition_cost_model(
                route_distance_km=route_distance_km,
                eta_min=eta_min,
                fuel_price_eur_l=fuel_price,
                vehicle_consumption_l_100km=consumption,
                target_hourly_net_eur=target_hourly,
                traffic_factor=traffic,
                forecast_volatility=forecast["forecast_volatility"],
            )

            distance_norm = min(route_distance_km / max(SHIFT_PLAN_REFERENCE_KM, 0.5), 1.0)
            distance_penalty = _clamp(float(distance_weight) * distance_norm, 0.0, 0.9)
            zone_payload = dict(zone)
            zone_payload["opportunity_score"] = max(
                _as_float(zone_payload.get("opportunity_score"), 0.0) * (1.0 - distance_penalty),
                0.0,
            )

            item = _build_shift_plan_item(
                zone=zone_payload,
                horizon_min=int(horizon_min),
                target_hourly_net_eur=target_hourly,
                route_distance_km=route_distance_km,
                eta_min=eta_min,
                reposition_cost=reposition,
                forecast=forecast,
            )
            items.append(item)
    finally:
        if osrm_client is not None:
            await osrm_client.aclose()

    ranked = _rank_shift_plan_items(items)
    effective_top_k = max(3, int(top_k))
    selected = ranked[:effective_top_k]
    for idx, item in enumerate(selected, start=1):
        item["rank"] = idx

    return {
        "driver_id": driver_id,
        "origin_lat": round(float(driver_lat), 6),
        "origin_lon": round(float(driver_lon), 6),
        "horizon_min": int(horizon_min),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target_hourly_net_eur": round(float(target_hourly), 2),
        "source": "zone_context_v2",
        "count": len(selected),
        "items": selected,
    }


@copilot_router.get("/driver/{driver_id}/next-best-zone")
async def next_best_zone(
    request: Request,
    driver_id: str,
    top_k: int = Query(5, ge=1, le=20),
    lat: float | None = Query(None, description="Override de la position driver (sinon lue depuis Redis)."),
    lon: float | None = Query(None),
    home_lat: float | None = Query(None, description="Position 'maison' optionnelle pour filtrer homeward."),
    home_lon: float | None = Query(None),
    max_distance_km: float | None = Query(None, ge=0.5, le=50.0, description="Filtre dur: zones au-delà ignorées."),
    distance_weight: float = Query(0.0, ge=0.0, le=1.0, description="Pénalité distance (0=off, 0.3=modéré, 0.6=agressif)."),
):
    redis_client: aioredis.Redis = request.app.state.redis

    zones = await _load_zone_context_payloads(redis_client)

    driver_lat: float | None = lat
    driver_lon: float | None = lon
    if driver_lat is None or driver_lon is None:
        state = await redis_client.hgetall(f"{COURIER_HASH_PREFIX}{driver_id}")
        if state:
            lat_val = _as_float(state.get("lat"), 0.0)
            lon_val = _as_float(state.get("lon"), 0.0)
            if not (lat_val == 0.0 and lon_val == 0.0):
                driver_lat = lat_val
                driver_lon = lon_val

    fuel_context = await redis_client.hgetall(FUEL_CONTEXT_KEY)
    fuel_price = _as_float(fuel_context.get("fuel_price_eur_l"), DEFAULT_FUEL_PRICE_EUR_L)
    consumption = _as_float(fuel_context.get("vehicle_consumption_l_100km"), DEFAULT_CONSUMPTION_L_100KM)

    enriched = _rank_zone_recommendations(
        zones,
        driver_lat=driver_lat,
        driver_lon=driver_lon,
        home_lat=home_lat,
        home_lon=home_lon,
        max_distance_km=max_distance_km,
        distance_weight=distance_weight,
        fuel_price_eur_l=fuel_price,
        consumption_l_100km=consumption,
    )

    return {
        "driver_id": driver_id,
        "driver_lat": driver_lat,
        "driver_lon": driver_lon,
        "home_lat": home_lat,
        "home_lon": home_lon,
        "distance_weight": distance_weight,
        "max_distance_km": max_distance_km,
        "count": len(enriched),
        "recommendations": enriched[:top_k],
    }


@copilot_router.get("/replay")
async def replay(
    request: Request,
    from_ts: str = Query(..., alias="from"),
    to_ts: str = Query(..., alias="to"),
    driver_id: str | None = Query(None),
    limit: int = Query(500, ge=1, le=5000),
):
    if not EVENTS_PATH.exists():
        raise HTTPException(status_code=404, detail=f"events path missing: {EVENTS_PATH}")

    try:
        from_dt = datetime.fromisoformat(from_ts)
        to_dt = datetime.fromisoformat(to_ts)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"invalid timestamp: {exc}") from exc

    if from_dt.tzinfo is None:
        from_dt = from_dt.replace(tzinfo=timezone.utc)
    if to_dt.tzinfo is None:
        to_dt = to_dt.replace(tzinfo=timezone.utc)
    if to_dt < from_dt:
        raise HTTPException(status_code=400, detail="'to' must be >= 'from'")
    if (to_dt - from_dt) > timedelta(days=7):
        raise HTTPException(status_code=400, detail="range too large (>7 days)")

    globs: list[str] = []
    cursor = from_dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    stop = to_dt.astimezone(timezone.utc)
    while cursor <= stop:
        partition_path = (
            EVENTS_PATH
            / "topic=*"
            / f"year={cursor.year}"
            / f"month={cursor.month:02d}"
            / f"day={cursor.day:02d}"
            / f"hour={cursor.hour:02d}"
            / "*.parquet"
        )
        globs.append(str(partition_path).replace("\\", "/"))
        cursor += timedelta(hours=1)

    if not globs:
        return {"from": from_ts, "to": to_ts, "driver_id": driver_id, "count": 0, "events": []}

    existing_globs = _existing_parquet_globs(globs)
    if not existing_globs:
        return {"from": from_ts, "to": to_ts, "driver_id": driver_id, "count": 0, "events": []}

    glob_list_sql = "[" + ", ".join(f"'{g}'" for g in existing_globs) + "]"
    rows = await _duck_query(
        request,
        f"""
        SELECT
            ts,
            topic,
            event_type,
            event_id,
            courier_id,
            offer_id,
            order_id,
            status,
            zone_id,
            estimated_fare_eur,
            actual_fare_eur,
            demand_index,
            supply_index,
            source_platform
        FROM read_parquet({glob_list_sql}, hive_partitioning = true)
        WHERE ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
          AND (? IS NULL OR courier_id = ?)
        ORDER BY ts
        LIMIT ?
        """,
        [from_ts, to_ts, driver_id, driver_id, limit],
    )

    return {
        "from": from_ts,
        "to": to_ts,
        "driver_id": driver_id,
        "count": len(rows),
        "events": [
            {
                "ts": str(r[0]),
                "topic": r[1],
                "event_type": r[2],
                "event_id": r[3],
                "courier_id": r[4],
                "offer_id": r[5],
                "order_id": r[6],
                "status": r[7],
                "zone_id": r[8],
                "estimated_fare_eur": r[9],
                "actual_fare_eur": r[10],
                "demand_index": r[11],
                "supply_index": r[12],
                "source_platform": r[13],
            }
            for r in rows
        ],
    }


@copilot_router.get("/route")
async def osrm_route(
    request: Request,
    origin_lat: float = Query(...),
    origin_lon: float = Query(...),
    dest_lat: float = Query(...),
    dest_lon: float = Query(...),
):
    """Get route information from OSRM (distance, duration, geometry)."""
    coords = f"{origin_lon},{origin_lat};{dest_lon},{dest_lat}"
    url = f"{OSRM_URL}/route/v1/driving/{coords}?overview=simplified&geometries=geojson"
    async with httpx.AsyncClient(timeout=5) as client:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") != "Ok" or not data.get("routes"):
                raise HTTPException(status_code=502, detail=f"OSRM error: {data.get('code')}")
            route = data["routes"][0]
            return {
                "distance_km": round(route["distance"] / 1000, 3),
                "duration_min": round(route["duration"] / 60, 2),
                "geometry": route.get("geometry"),
            }
        except httpx.ConnectError:
            raise HTTPException(
                status_code=503,
                detail="OSRM not available. Run: bash scripts/prepare_osrm.sh && docker compose --profile routing up osrm",
            )


@copilot_router.get("/irve/nearby")
async def irve_nearby(
    request: Request,
    lat: float = Query(..., ge=NYC_LAT_MIN, le=NYC_LAT_MAX),
    lon: float = Query(..., ge=NYC_LON_MIN, le=NYC_LON_MAX),
    radius_km: float = Query(2.0, ge=0.1, le=20.0),
    limit: int = Query(10, ge=1, le=50),
):
    """Find nearest EV charging stations (IRVE) from a given position."""
    redis_client: aioredis.Redis = request.app.state.redis
    results = await redis_client.geosearch(
        "copilot:irve:geo",
        longitude=lon,
        latitude=lat,
        radius=radius_km,
        unit="km",
        sort="ASC",
        count=limit,
        withcoord=True,
        withdist=True,
    )
    stations = []
    for item in results:
        station_id = item[0] if isinstance(item, (list, tuple)) else item
        dist = item[1] if isinstance(item, (list, tuple)) and len(item) > 1 else None
        coord = item[2] if isinstance(item, (list, tuple)) and len(item) > 2 else None
        info = await redis_client.hgetall(f"copilot:context:irve:station:{station_id}")
        stations.append({
            "station_id": station_id,
            "distance_km": round(float(dist), 3) if dist else None,
            "lon": float(coord[0]) if coord else None,
            "lat": float(coord[1]) if coord else None,
            "nom": info.get("nom", ""),
            "puissance_kw": info.get("puissance_kw", ""),
        })
    return {"lat": lat, "lon": lon, "radius_km": radius_km, "count": len(stations), "stations": stations}


@copilot_router.get("/gbfs/zones")
async def gbfs_zones(request: Request, top_k: int = Query(20, ge=1, le=100)):
    """Return Citi Bike zone signals sorted by demand boost (proxy for taxi demand)."""
    redis_client: aioredis.Redis = request.app.state.redis
    zones = []
    async for key in redis_client.scan_iter(match="copilot:context:gbfs:zone:*"):
        z = await redis_client.hgetall(key)
        if z:
            zones.append({
                "zone_id": z.get("zone_id", ""),
                "bikes_available": int(z.get("bikes_available", 0)),
                "docks_available": int(z.get("docks_available", 0)),
                "occupancy_ratio": float(z.get("occupancy_ratio", 0)),
                "demand_boost": float(z.get("demand_boost", 0)),
                "stations_count": int(z.get("stations_count", 0)),
            })
    zones.sort(key=lambda x: x["demand_boost"], reverse=True)
    return {"count": len(zones), "zones": zones[:top_k]}


@copilot_router.get("/fuel-context")
async def fuel_context(request: Request):
    redis_client: aioredis.Redis = request.app.state.redis
    raw = await redis_client.hgetall(FUEL_CONTEXT_KEY)
    return {
        "fuel_price_eur_l": round(_as_float(raw.get("fuel_price_eur_l"), DEFAULT_FUEL_PRICE_EUR_L), 3),
        "fuel_price_usd_gallon": (
            round(_as_float(raw.get("fuel_price_usd_gallon"), 0.0), 3)
            if raw.get("fuel_price_usd_gallon") not in (None, "")
            else None
        ),
        "vehicle_consumption_l_100km": round(
            _as_float(raw.get("vehicle_consumption_l_100km"), DEFAULT_CONSUMPTION_L_100KM), 3
        ),
        "platform_fee_pct": round(_as_float(raw.get("platform_fee_pct"), DEFAULT_PLATFORM_FEE_PCT), 3),
        "source": raw.get("source", "default"),
        "provider": raw.get("provider"),
        "provider_url": raw.get("provider_url"),
        "fuel_grade": raw.get("fuel_grade"),
        "usd_to_eur_rate": (
            round(_as_float(raw.get("usd_to_eur_rate"), FUEL_USD_TO_EUR_RATE), 4)
            if raw.get("usd_to_eur_rate") not in (None, "")
            else None
        ),
        "lock_manual": _as_bool(raw.get("lock_manual"), False),
        "fuel_sync_status": raw.get("fuel_sync_status"),
        "fuel_sync_checked_at": raw.get("fuel_sync_checked_at"),
        "updated_at": raw.get("updated_at"),
    }


@copilot_router.post("/fuel-context")
async def update_fuel_context(request: Request, body: FuelContextUpdateRequest):
    redis_client: aioredis.Redis = request.app.state.redis
    lock_manual = bool(body.lock_manual)
    raw = await redis_client.hgetall(FUEL_CONTEXT_KEY)
    current_grade = raw.get("fuel_grade", FUEL_GRADE) if raw else FUEL_GRADE
    now_iso = datetime.now(timezone.utc).isoformat()
    await redis_client.hset(
        FUEL_CONTEXT_KEY,
        mapping={
            "fuel_price_eur_l": round(float(body.fuel_price_eur_l), 3),
            "vehicle_consumption_l_100km": round(float(body.vehicle_consumption_l_100km), 3),
            "platform_fee_pct": round(float(body.platform_fee_pct), 3),
            "source": body.source,
            "provider": "manual",
            "provider_url": "",
            "fuel_grade": current_grade,
            "usd_to_eur_rate": round(float(FUEL_USD_TO_EUR_RATE), 4),
            "lock_manual": "1" if lock_manual else "0",
            "fuel_sync_status": "manual_override",
            "fuel_sync_checked_at": now_iso,
            "updated_at": now_iso,
        },
    )
    await redis_client.expire(FUEL_CONTEXT_KEY, FUEL_CONTEXT_TTL_SECONDS)
    return await fuel_context(request)


@copilot_router.post("/fuel-context/refresh")
async def refresh_fuel_context(request: Request, force: bool = Query(False)):
    redis_client: aioredis.Redis = request.app.state.redis
    result = await _refresh_fuel_context_once(redis_client, force=force)
    payload = await fuel_context(request)
    payload["refresh_result"] = result
    return payload


@copilot_router.get("/health")
async def copilot_health(request: Request):
    redis_client: aioredis.Redis = request.app.state.redis
    model_payload = getattr(request.app.state, "copilot_model", None)
    quality_gate = getattr(request.app.state, "copilot_model_quality_gate", {})

    weather = await redis_client.hgetall(WEATHER_KEY)
    gbfs = await redis_client.hgetall(GBFS_KEY)
    irve = await redis_client.hgetall(IRVE_KEY)
    events_context = await redis_client.hgetall(EVENTS_CONTEXT_KEY)
    fuel = await redis_client.hgetall(FUEL_CONTEXT_KEY)
    context_quality = await redis_client.hgetall(CONTEXT_QUALITY_KEY)
    tlc_replay = await redis_client.hgetall(TLC_REPLAY_KEY)
    single_replay = await redis_client.hgetall(SINGLE_REPLAY_STATUS_KEY)

    route_requests = int(_as_float(single_replay.get("route_requests"), 0.0))
    route_successes = int(_as_float(single_replay.get("route_successes"), 0.0))
    hold_ticks = int(_as_float(single_replay.get("hold_ticks"), 0.0))
    positions = int(_as_float(single_replay.get("positions"), 0.0))
    routing_success_rate = (
        round(route_successes / route_requests, 4) if route_requests > 0 else None
    )
    hold_rate = round(hold_ticks / positions, 4) if positions > 0 else None

    return {
        "model_loaded": bool(model_payload),
        "feature_columns": (model_payload or {}).get("feature_columns", FEATURE_COLUMNS),
        "model_metrics": (model_payload or {}).get("metrics", {}),
        "model_quality_gate": quality_gate,
        "weather_context": weather,
        "gbfs_context": gbfs,
        "irve_context": irve,
        "events_context": {
            "source": events_context.get("source"),
            "status": events_context.get("status"),
            "events_upcoming_count": int(_as_float(events_context.get("events_upcoming_count"), 0.0)),
            "events_window_start": events_context.get("events_window_start"),
            "events_window_end": events_context.get("events_window_end"),
            "error_type": events_context.get("error_type"),
            "updated_at": events_context.get("updated_at"),
        },
        "fuel_context": {
            "fuel_price_eur_l": round(_as_float(fuel.get("fuel_price_eur_l"), DEFAULT_FUEL_PRICE_EUR_L), 3),
            "fuel_price_usd_gallon": (
                round(_as_float(fuel.get("fuel_price_usd_gallon"), 0.0), 3)
                if fuel.get("fuel_price_usd_gallon") not in (None, "")
                else None
            ),
            "vehicle_consumption_l_100km": round(
                _as_float(fuel.get("vehicle_consumption_l_100km"), DEFAULT_CONSUMPTION_L_100KM), 3
            ),
            "platform_fee_pct": round(_as_float(fuel.get("platform_fee_pct"), DEFAULT_PLATFORM_FEE_PCT), 3),
            "source": fuel.get("source", "default"),
            "provider": fuel.get("provider"),
            "fuel_grade": fuel.get("fuel_grade"),
            "usd_to_eur_rate": (
                round(_as_float(fuel.get("usd_to_eur_rate"), FUEL_USD_TO_EUR_RATE), 4)
                if fuel.get("usd_to_eur_rate") not in (None, "")
                else None
            ),
            "lock_manual": _as_bool(fuel.get("lock_manual"), False),
            "fuel_sync_status": fuel.get("fuel_sync_status"),
            "fuel_sync_checked_at": fuel.get("fuel_sync_checked_at"),
            "updated_at": fuel.get("updated_at"),
        },
        "data_quality": {
            "supply_key": context_quality.get("supply_key"),
            "supply_variance": round(_as_float(context_quality.get("supply_variance"), 0.0), 6),
            "supply_window_span": round(_as_float(context_quality.get("supply_window_span"), 0.0), 6),
            "supply_window_cycles": int(_as_float(context_quality.get("supply_window_cycles"), 0.0)),
            "supply_flat_alert": _as_bool(context_quality.get("supply_flat_alert"), False),
            "traffic_nonzero_rate": round(_as_float(context_quality.get("traffic_nonzero_rate"), 0.0), 4),
            "traffic_mean": round(_as_float(context_quality.get("traffic_mean"), 1.0), 4),
            "sources": context_quality.get("sources"),
            "events_source_active": _as_bool(context_quality.get("events_source_active"), False),
            "events_rows": int(_as_float(context_quality.get("events_rows"), 0.0)),
            "events_status": context_quality.get("events_status"),
            "event_pressure_mean": round(_as_float(context_quality.get("event_pressure_mean"), 0.0), 4),
            "event_pressure_max": round(_as_float(context_quality.get("event_pressure_max"), 0.0), 4),
            "event_pressure_nonzero_rate": round(
                _as_float(context_quality.get("event_pressure_nonzero_rate"), 0.0), 4
            ),
            "dot_advisory_status": context_quality.get("dot_advisory_status"),
            "dot_advisory_rows": int(_as_float(context_quality.get("dot_advisory_rows"), 0.0)),
            "dot_speeds_status": context_quality.get("dot_speeds_status"),
            "dot_speeds_rows": int(_as_float(context_quality.get("dot_speeds_rows"), 0.0)),
            "closure_pressure_mean": round(_as_float(context_quality.get("closure_pressure_mean"), 0.0), 4),
            "closure_pressure_max": round(_as_float(context_quality.get("closure_pressure_max"), 0.0), 4),
            "speed_pressure_mean": round(_as_float(context_quality.get("speed_pressure_mean"), 0.0), 4),
            "speed_pressure_max": round(_as_float(context_quality.get("speed_pressure_max"), 0.0), 4),
            "freshness_policy": context_quality.get("freshness_policy"),
            "context_fallback_applied": _as_bool(context_quality.get("context_fallback_applied"), False),
            "stale_sources_count": int(_as_float(context_quality.get("stale_sources_count"), 0.0)),
            "age_gbfs_s": round(_as_float(context_quality.get("age_gbfs_s"), -1.0), 3),
            "age_weather_s": round(_as_float(context_quality.get("age_weather_s"), -1.0), 3),
            "age_nyc311_s": round(_as_float(context_quality.get("age_nyc311_s"), -1.0), 3),
            "age_events_s": round(_as_float(context_quality.get("age_events_s"), -1.0), 3),
            "age_dot_closure_s": round(_as_float(context_quality.get("age_dot_closure_s"), -1.0), 3),
            "age_dot_speeds_s": round(_as_float(context_quality.get("age_dot_speeds_s"), -1.0), 3),
            "stale_gbfs": _as_bool(context_quality.get("stale_gbfs"), False),
            "stale_weather": _as_bool(context_quality.get("stale_weather"), False),
            "stale_nyc311": _as_bool(context_quality.get("stale_nyc311"), False),
            "stale_events": _as_bool(context_quality.get("stale_events"), False),
            "stale_dot_closure": _as_bool(context_quality.get("stale_dot_closure"), False),
            "stale_dot_speeds": _as_bool(context_quality.get("stale_dot_speeds"), False),
            "updated_at": context_quality.get("updated_at"),
        },
        "routing_quality": {
            "driver_id": single_replay.get("driver_id"),
            "state": single_replay.get("state"),
            "routing_degraded": _as_bool(single_replay.get("routing_degraded"), False),
            "routing_last_error": single_replay.get("routing_last_error"),
            "route_requests": route_requests,
            "route_successes": route_successes,
            "routing_success_rate": routing_success_rate,
            "hold_ticks": hold_ticks,
            "hold_rate": hold_rate,
            "routing_errors": int(_as_float(single_replay.get("routing_errors"), 0.0)),
            "providers": [p for p in (single_replay.get("routing_providers") or "").split(",") if p],
            "updated_at": single_replay.get("updated_at"),
        },
        "tlc_replay": tlc_replay,
        "events_path": str(EVENTS_PATH),
        "model_path": str(MODEL_PATH),
    }

