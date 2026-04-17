const state = {
  health: null,
  offers: [],
  bestOffers: [],
  bestOffersError: null,
  dispatch: null,
  dispatchError: null,
  refreshHealthInFlight: false,
  refreshDriverInFlight: false,
  refreshDriverQueued: false,
  mission: null,
  missionLoopTimer: null,
  missionLastPollTs: 0,
  missionPollInFlight: false,
  missionJournal: [],
  missionJournalStats: null,
  missionJournalError: null,
  missionJournalDriver: null,
  zones: [],
  replay: [],
  autoRefreshTimer: null,
};

// Inputs users tweak in quick succession (numeric spinners, sliders) get
// debounced so we don't fire four parallel API calls per keystroke. The
// trailing edge is honoured so the final value is always reflected.
const REFRESH_DEBOUNCE_MS = 250;

function debounce(fn, waitMs) {
  let handle = null;
  const wrapped = function debounced(...args) {
    if (handle) clearTimeout(handle);
    handle = setTimeout(() => {
      handle = null;
      fn(...args);
    }, waitMs);
  };
  wrapped.flush = function flush(...args) {
    if (handle) clearTimeout(handle);
    handle = null;
    fn(...args);
  };
  return wrapped;
}

const $ = (id) => document.getElementById(id);

const healthMode = $('healthMode');
const healthGate = $('healthGate');
const healthMetrics = $('healthMetrics');

const offersEl = $('offers');
const bestOffersEl = $('bestOffers');
const dispatchSummaryEl = $('dispatchSummary');
const dispatchPlanEl = $('dispatchPlan');
const dispatchMapEl = $('dispatchMap');
const dispatchLegendEl = $('dispatchLegend');
const zonesEl = $('zones');
const replaySummary = $('replaySummary');
const replayTimeline = $('replayTimeline');

const scoreProb = $('scoreProb');
const scoreDecision = $('scoreDecision');
const scoreEur = $('scoreEur');
const scoreReasons = $('scoreReasons');

const driverInput = $('driverId');
const fromInput = $('fromTs');
const toInput = $('toTs');

const kpiOffers = $('kpiOffers');
const kpiAcceptRate = $('kpiAcceptRate');
const kpiAvgNet = $('kpiAvgNet');
const kpiReplay = $('kpiReplay');

const offerFilter = $('offerFilter');
const offerLimit = $('offerLimit');
const autoRefreshBox = $('autoRefresh');

const aroundRadiusInput = $('aroundRadius');
const aroundLimitInput = $('aroundLimit');
const aroundMinScoreInput = $('aroundMinScore');
const aroundUseOsrmInput = $('aroundUseOsrm');
const dispatchMaxEtaInput = $('dispatchMaxEta');
const dispatchForecastHorizonInput = $('dispatchForecastHorizon');
const dispatchZoneTopKInput = $('dispatchZoneTopK');
const dispatchStrategyInput = $('dispatchStrategy');
const zoneDistanceWeightInput = $('zoneDistanceWeight');
const zoneMaxDistanceKmInput = $('zoneMaxDistanceKm');
const zoneHomeLatInput = $('zoneHomeLat');
const zoneHomeLonInput = $('zoneHomeLon');
const missionStartBestBtn = $('missionStartBestBtn');
const missionStopBtn = $('missionStopBtn');
const missionStatusEl = $('missionStatus');
const missionTargetEl = $('missionTarget');
const missionElapsedEl = $('missionElapsed');
const missionRemainingEl = $('missionRemaining');
const missionEtaEl = $('missionEta');
const missionMetaEl = $('missionMeta');
const missionProgressFillEl = $('missionProgressFill');
const missionJournalRefreshBtn = $('missionJournalRefreshBtn');
const refreshFuelBtn = $('refreshFuelBtn');
const missionJournalSummaryEl = $('missionJournalSummary');
const missionJournalListEl = $('missionJournalList');
const journalSuccessRateEl = $('journalSuccessRate');
const journalRealizedDeltaEl = $('journalRealizedDelta');
const journalPredictedDeltaEl = $('journalPredictedDelta');
const journalAvgElapsedEl = $('journalAvgElapsed');

const API_TIMEOUT_MS = 12000;
const API_RETRY_DELAY_MS = 350;
const API_RETRY_GET = 1;

function waitMs(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeApiError(error) {
  if (error && error.name === 'AbortError') {
    return new Error('Request timeout');
  }
  return error instanceof Error ? error : new Error(String(error || 'Request failed'));
}

async function api(path, opts = {}) {
  const method = String(opts.method || 'GET').toUpperCase();
  const retries = Number.isFinite(Number(opts.retries)) ? Number(opts.retries) : method === 'GET' ? API_RETRY_GET : 0;
  const timeoutMs = Number.isFinite(Number(opts.timeoutMs)) ? Number(opts.timeoutMs) : API_TIMEOUT_MS;
  const { retries: _retries, timeoutMs: _timeoutMs, ...fetchOpts } = opts;

  let lastError = new Error('Request failed');
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), Math.max(1000, timeoutMs));
    try {
      const response = await fetch(path, {
        headers: { 'Content-Type': 'application/json', ...(fetchOpts.headers || {}) },
        ...fetchOpts,
        cache: fetchOpts.cache || 'no-store',
        signal: controller.signal,
      });

      if (!response.ok) {
        let detail = '';
        try {
          const payload = await response.json();
          detail = payload?.detail ? ` - ${payload.detail}` : '';
        } catch (_) {
          detail = '';
        }
        throw new Error(`${response.status} ${response.statusText}${detail}`);
      }

      if (response.status === 204) {
        return null;
      }
      return await response.json();
    } catch (error) {
      lastError = normalizeApiError(error);
      if (attempt < retries) {
        await waitMs(API_RETRY_DELAY_MS * (attempt + 1));
        continue;
      }
    } finally {
      clearTimeout(timeoutId);
    }
  }
  throw lastError;
}

function fmt(num, digits = 2) {
  if (!Number.isFinite(Number(num))) return '-';
  return Number(num).toFixed(digits);
}

function toIso(ts) {
  const d = new Date(ts);
  return Number.isNaN(d.getTime()) ? '' : d.toISOString();
}

function setWindowMinutes(minutes) {
  if (!fromInput || !toInput) return;
  const now = new Date();
  const from = new Date(now.getTime() - minutes * 60 * 1000);
  fromInput.value = toIso(from).slice(0, 19) + 'Z';
  toInput.value = toIso(now).slice(0, 19) + 'Z';
}

function setTodayWindow() {
  if (!fromInput || !toInput) return;
  const now = new Date();
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0));
  fromInput.value = toIso(start).slice(0, 19) + 'Z';
  toInput.value = toIso(now).slice(0, 19) + 'Z';
}

function asDecisionClass(decision) {
  return decision === 'accept' ? 'accept' : 'reject';
}

function asRecommendationClass(action) {
  if (action === 'accept') return 'accept';
  if (action === 'consider') return 'consider';
  return 'skip';
}

function asDispatchClass(decision) {
  if (decision === 'stay') return 'accept';
  if (decision === 'reposition') return 'consider';
  return 'skip';
}

function humanizeReason(reason) {
  const mapping = {
    high_estimated_net_revenue: 'high net revenue',
    low_estimated_net_revenue: 'low net revenue',
    above_target_hourly_goal: 'above hourly target',
    below_target_hourly_goal: 'below hourly target',
    strong_demand_pressure: 'strong demand pressure',
    weak_demand_pressure: 'weak demand pressure',
    weather_downside: 'weather downside',
    traffic_penalty: 'traffic penalty',
    high_platform_fee: 'high platform fee',
    fuel_cost_penalty: 'fuel cost penalty',
    long_offer_duration: 'long offer duration',
    short_offer_efficiency: 'short offer efficiency',
    long_pickup_detour: 'long pickup detour',
    balanced_offer_profile: 'balanced profile',
    zone_high_demand_pressure: 'zone high demand',
    zone_low_demand_pressure: 'zone low demand',
    pickup_far: 'pickup far',
    pickup_near: 'pickup near',
    missing_route_coordinates: 'missing route coordinates',
    osrm_pickup_leg_unavailable: 'pickup route unavailable',
    osrm_dropoff_leg_unavailable: 'dropoff route unavailable',
    osrm_client_unavailable: 'route service unavailable',
    insufficient_real_time_candidates: 'insufficient real-time candidates',
    no_profitable_local_offer: 'no profitable local offer',
    reposition_eta_too_high: 'reposition ETA too high',
    reposition_net_gain_negative: 'reposition net gain negative',
    best_local_offer_available: 'best local offer available',
    strong_local_offer_available: 'strong local offer available',
    zone_opportunity_outweighs_local_offer: 'zone opportunity beats local offer',
    below_hourly_target_reposition_can_help: 'below hourly target: reposition can help',
    local_offer_weak_reposition_has_positive_edge: 'local offer weak, reposition has edge',
    zone_coordinates_unavailable: 'zone coordinates unavailable',
  };
  return mapping[reason] || String(reason || '').replaceAll('_', ' ');
}

function formatExplanationDetail(detail) {
  if (!detail || typeof detail !== 'object') return '';
  const label = String(detail.label || humanizeReason(detail.code || 'detail')).trim();
  const unit = String(detail.unit || '').trim();
  const value = Number(detail.value);
  const hasNumber = Number.isFinite(value);
  let renderedValue = hasNumber ? fmt(value, unit === 'probability' || unit === 'normalized' ? 3 : 1) : String(detail.value || '');

  let unitLabel = '';
  if (unit === 'pct') unitLabel = '%';
  else if (unit === 'eur_per_hour') unitLabel = 'EUR/h';
  else if (unit === 'probability' || unit === 'normalized') unitLabel = '';
  else if (unit) unitLabel = unit.replaceAll('_', ' ');

  if (unitLabel) {
    renderedValue = unitLabel === '%' ? `${renderedValue}${unitLabel}` : `${renderedValue} ${unitLabel}`;
  }
  const impact = String(detail.impact || 'neutral').toUpperCase();
  return `${label}: ${renderedValue} (${impact})`;
}

function explanationDetailChips(details, limit = 2) {
  const rows = Array.isArray(details) ? details.slice(0, Math.max(0, limit)) : [];
  return rows
    .map((d) => `<span>${formatExplanationDetail(d)}</span>`)
    .join('');
}

function routeSummary(offer) {
  const src = offer.route_source || 'estimated';
  const dist = Number(offer.route_distance_km);
  const dur = Number(offer.route_duration_min);
  if (!Number.isFinite(dist) || !Number.isFinite(dur)) {
    return `route ${src}`;
  }
  return `route ${src} - ${fmt(dist, 1)} km / ${fmt(dur, 1)} min`;
}

const dispatchMapState = {
  map: null,
  zoneLayer: null,
  routeLayer: null,
  originLayer: null,
  clusterEnabled: false,
  canvasId: 'dispatchLeafletCanvas',
};

const DISPATCH_MAP_DEFAULT = {
  lat: 40.758,
  lon: -73.9855,
  zoom: 12,
};
const MISSION_LOOP_INTERVAL_MS = 3000;
const MISSION_POLL_INTERVAL_MS = 9000;
const MISSION_ARRIVAL_THRESHOLD_KM = 0.2;
const MISSION_FALLBACK_SPEED_KMH = 22;
const MISSION_STRATEGY_POLICIES = {
  conservative: {
    arrivalThresholdKm: 0.16,
    etaLimitMultiplier: 1.45,
    etaLimitFloorMin: 8,
  },
  balanced: {
    arrivalThresholdKm: 0.2,
    etaLimitMultiplier: 1.8,
    etaLimitFloorMin: 10,
  },
  aggressive: {
    arrivalThresholdKm: 0.3,
    etaLimitMultiplier: 2.25,
    etaLimitFloorMin: 12,
  },
};

function asFloatOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function clamp(value, low, high) {
  return Math.max(low, Math.min(high, value));
}

function haversineKm(lat1, lon1, lat2, lon2) {
  const toRad = (deg) => (deg * Math.PI) / 180;
  const r = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return r * 2 * Math.asin(Math.sqrt(a));
}

function fmtDuration(mins) {
  const m = Math.max(0, Math.round(Number(mins || 0)));
  const h = Math.floor(m / 60);
  const r = m % 60;
  return h > 0 ? `${h}h ${String(r).padStart(2, '0')}m` : `${r}m`;
}

function fmtSigned(value, digits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return '-';
  const sign = n > 0 ? '+' : '';
  return `${sign}${n.toFixed(digits)}`;
}

function nowIsoUtc() {
  return new Date().toISOString();
}

function mapsDirectionsUrl(originLat, originLon, destLat, destLon) {
  return `https://www.google.com/maps/dir/?api=1&origin=${encodeURIComponent(`${originLat},${originLon}`)}&destination=${encodeURIComponent(`${destLat},${destLon}`)}&travelmode=driving`;
}

function currentDriverId() {
  return ((driverInput && driverInput.value) || 'L001').trim();
}

function resolveMissionPolicy(strategyRaw) {
  const strategy = String(strategyRaw || 'balanced').toLowerCase();
  if (MISSION_STRATEGY_POLICIES[strategy]) {
    return { strategy, ...MISSION_STRATEGY_POLICIES[strategy] };
  }
  return { strategy: 'balanced', ...MISSION_STRATEGY_POLICIES.balanced };
}

function canUseLeaflet() {
  return typeof window !== 'undefined' && Boolean(window.L && typeof window.L.map === 'function');
}

function clearDispatchLeafletLayers() {
  if (dispatchMapState.zoneLayer && typeof dispatchMapState.zoneLayer.clearLayers === 'function') {
    dispatchMapState.zoneLayer.clearLayers();
  }
  if (dispatchMapState.routeLayer) {
    dispatchMapState.routeLayer.clearLayers();
  }
  if (dispatchMapState.originLayer) {
    dispatchMapState.originLayer.clearLayers();
  }
}

function ensureDispatchZoneLayer(map) {
  const clusterEnabled = typeof window.L.markerClusterGroup === 'function';
  if (dispatchMapState.zoneLayer && dispatchMapState.clusterEnabled === clusterEnabled) {
    return dispatchMapState.zoneLayer;
  }
  if (dispatchMapState.zoneLayer) {
    map.removeLayer(dispatchMapState.zoneLayer);
    dispatchMapState.zoneLayer = null;
  }

  dispatchMapState.clusterEnabled = clusterEnabled;
  dispatchMapState.zoneLayer = clusterEnabled
    ? window.L.markerClusterGroup({
        showCoverageOnHover: false,
        spiderfyOnMaxZoom: true,
        removeOutsideVisibleBounds: true,
      })
    : window.L.layerGroup();
  dispatchMapState.zoneLayer.addTo(map);
  return dispatchMapState.zoneLayer;
}

function ensureDispatchLeafletMap() {
  if (!dispatchMapEl || !canUseLeaflet()) return null;
  if (dispatchMapState.map) return dispatchMapState.map;

  dispatchMapEl.classList.add('map-ready');
  dispatchMapEl.innerHTML = `<div id="${dispatchMapState.canvasId}" class="dispatch-leaflet"></div>`;

  const map = window.L.map(dispatchMapState.canvasId, {
    zoomControl: true,
    attributionControl: true,
    preferCanvas: true,
  });
  window.L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; OpenStreetMap contributors',
  }).addTo(map);

  dispatchMapState.map = map;
  dispatchMapState.originLayer = window.L.layerGroup().addTo(map);
  dispatchMapState.routeLayer = window.L.layerGroup().addTo(map);
  ensureDispatchZoneLayer(map);

  map.setView([DISPATCH_MAP_DEFAULT.lat, DISPATCH_MAP_DEFAULT.lon], DISPATCH_MAP_DEFAULT.zoom);
  setTimeout(() => map.invalidateSize(), 0);
  return map;
}

function buildDispatchMapPayload(plan) {
  if (!plan || typeof plan !== 'object') return null;
  const originLat = asFloatOrNull(plan.origin?.lat);
  const originLon = asFloatOrNull(plan.origin?.lon);
  const strategy = String(plan.dispatch_strategy || 'balanced').toLowerCase();
  const effectiveMaxEtaMin = asFloatOrNull(plan.effective_max_reposition_eta_min);
  const rawMoves = Array.isArray(plan.reposition_candidates) ? plan.reposition_candidates : [];

  const moves = rawMoves
    .map((z, idx) => {
      const lat = asFloatOrNull(z.zone_lat);
      const lon = asFloatOrNull(z.zone_lon);
      if (lat == null || lon == null) return null;
      return {
        idx,
        zone_id: z.zone_id || 'zone',
        lat,
        lon,
        dispatch_score: Number(z.dispatch_score || 0),
        eta_min: Number(z.eta_min || 0),
        distance_km: Number(z.route_distance_km || 0),
        potential: Number(z.estimated_potential_eur_h || 0),
        risk_potential: Number(z.risk_adjusted_potential_eur_h || z.estimated_potential_eur_h || 0),
        total_cost: Number(z.reposition_total_cost_eur || z.travel_cost_eur || 0),
      };
    })
    .filter(Boolean);

  return { originLat, originLon, strategy, effectiveMaxEtaMin, moves };
}

function renderDispatchLegend(payload) {
  if (!dispatchLegendEl) return;
  if (!payload || payload.originLat == null || payload.originLon == null) {
    dispatchLegendEl.innerHTML = '';
    return;
  }

  const { originLat, originLon, moves } = payload;
  if (!moves.length) {
    dispatchLegendEl.innerHTML = '<div class="muted">No reposition candidates for current filters.</div>';
    return;
  }

  dispatchLegendEl.innerHTML = moves
    .map((move, idx) => {
      const label = idx === 0 ? 'Best zone' : `Zone #${idx + 1}`;
      const score = fmt(move.dispatch_score, 3);
      const eta = fmt(move.eta_min, 1);
      const dist = fmt(move.distance_km, 1);
      const potential = fmt(move.potential, 1);
      const riskPotential = fmt(move.risk_potential, 1);
      const totalCost = fmt(move.total_cost, 2);
      return `
        <article class="map-row">
          <div class="map-row-top">
            <strong>${label} - ${move.zone_id}</strong>
            <span class="badge ${idx === 0 ? 'consider' : 'accept'}">${score}</span>
          </div>
          <div class="map-row-meta">ETA ${eta} min - ${dist} km - gross ${potential} EUR/h - net ${riskPotential} EUR/h - cost ${totalCost} EUR</div>
          <div class="offer-actions">
            <button
              class="ghost"
              data-action="go-zone"
              data-origin-lat="${originLat}"
              data-origin-lon="${originLon}"
              data-dest-lat="${move.lat}"
              data-dest-lon="${move.lon}"
            >Go Zone</button>
            <button
              class="ghost"
              data-action="start-mission"
              data-zone-id="${move.zone_id}"
              data-dest-lat="${move.lat}"
              data-dest-lon="${move.lon}"
              data-score="${move.dispatch_score}"
              data-potential="${move.potential}"
              data-eta="${move.eta_min}"
              data-dispatch-strategy="${payload.strategy || 'balanced'}"
              data-effective-max-eta="${payload.effectiveMaxEtaMin == null ? '' : payload.effectiveMaxEtaMin}"
            >Start Mission</button>
          </div>
        </article>
      `;
    })
    .join('');
}

function renderDispatchMapSvg(payload) {
  dispatchMapEl.classList.remove('map-ready');

  if (!payload || payload.originLat == null || payload.originLon == null) {
    dispatchMapEl.textContent = 'Driver location missing for map.';
    return;
  }
  if (!payload.moves.length) {
    dispatchMapEl.textContent = 'No reposition candidates to plot.';
    return;
  }

  const { originLat, originLon, moves } = payload;
  const width = 620;
  const height = 260;
  const pad = 26;
  const lats = [originLat, ...moves.map((m) => m.lat)];
  const lons = [originLon, ...moves.map((m) => m.lon)];
  let minLat = Math.min(...lats);
  let maxLat = Math.max(...lats);
  let minLon = Math.min(...lons);
  let maxLon = Math.max(...lons);
  if (Math.abs(maxLat - minLat) < 0.003) {
    minLat -= 0.002;
    maxLat += 0.002;
  }
  if (Math.abs(maxLon - minLon) < 0.003) {
    minLon -= 0.002;
    maxLon += 0.002;
  }

  const project = (lat, lon) => {
    const x = pad + ((lon - minLon) / (maxLon - minLon)) * (width - pad * 2);
    const y = height - pad - ((lat - minLat) / (maxLat - minLat)) * (height - pad * 2);
    return { x, y };
  };

  const originPoint = project(originLat, originLon);
  const grid = [0.2, 0.4, 0.6, 0.8]
    .map(
      (p) =>
        `<line x1="${pad}" y1="${(height * p).toFixed(1)}" x2="${width - pad}" y2="${(height * p).toFixed(1)}" stroke="rgba(15,23,42,0.12)" stroke-dasharray="3 5"/>`
    )
    .join('');

  const lines = moves
    .map((move, idx) => {
      const p = project(move.lat, move.lon);
      const stroke = idx === 0 ? '#d97706' : '#0ea5a4';
      const opacity = idx === 0 ? '0.95' : '0.55';
      return `<line x1="${originPoint.x.toFixed(1)}" y1="${originPoint.y.toFixed(1)}" x2="${p.x.toFixed(1)}" y2="${p.y.toFixed(1)}" stroke="${stroke}" stroke-width="${idx === 0 ? 2.6 : 1.4}" stroke-opacity="${opacity}"/>`;
    })
    .join('');

  const markers = moves
    .map((move, idx) => {
      const p = project(move.lat, move.lon);
      const fill = idx === 0 ? '#d97706' : '#0ea5a4';
      const label = idx === 0 ? 'BEST' : `#${idx + 1}`;
      const lx = Math.min(width - 70, p.x + 7);
      const ly = Math.max(16, p.y - 8);
      return `
        <circle cx="${p.x.toFixed(1)}" cy="${p.y.toFixed(1)}" r="${idx === 0 ? 6.2 : 4.8}" fill="${fill}" stroke="#ffffff" stroke-width="2"/>
        <text x="${lx.toFixed(1)}" y="${ly.toFixed(1)}" font-size="10" font-weight="700" fill="#0f172a">${label}</text>
      `;
    })
    .join('');

  const originMarker = `
    <circle cx="${originPoint.x.toFixed(1)}" cy="${originPoint.y.toFixed(1)}" r="6.8" fill="#2563eb" stroke="#ffffff" stroke-width="2.2"/>
    <text x="${Math.min(width - 66, originPoint.x + 8).toFixed(1)}" y="${Math.max(16, originPoint.y - 10).toFixed(1)}" font-size="10" font-weight="700" fill="#0f172a">YOU</text>
  `;

  dispatchMapEl.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Dispatch map">
      <rect x="0" y="0" width="${width}" height="${height}" fill="rgba(255,255,255,0.85)"></rect>
      ${grid}
      ${lines}
      ${originMarker}
      ${markers}
    </svg>
  `;
}

function renderDispatchMapLeaflet(payload) {
  const map = ensureDispatchLeafletMap();
  if (!map || !payload || payload.originLat == null || payload.originLon == null) {
    return false;
  }

  const zoneLayer = ensureDispatchZoneLayer(map);
  clearDispatchLeafletLayers();

  const originIcon = window.L.divIcon({
    className: '',
    html: '<div class="origin-pin"></div>',
    iconSize: [14, 14],
    iconAnchor: [7, 7],
    popupAnchor: [0, -8],
  });
  const originMarker = window.L.marker([payload.originLat, payload.originLon], { icon: originIcon }).bindPopup('YOU');
  dispatchMapState.originLayer.addLayer(originMarker);

  const points = [[payload.originLat, payload.originLon]];
  payload.moves.forEach((move, idx) => {
    const zoneIcon = window.L.divIcon({
      className: '',
      html: `<div class="zone-pin ${idx === 0 ? 'best' : 'alt'}"></div>`,
      iconSize: [14, 14],
      iconAnchor: [7, 7],
      popupAnchor: [0, -8],
    });
    const marker = window.L.marker([move.lat, move.lon], { icon: zoneIcon }).bindPopup(
      `<strong>${idx === 0 ? 'Best zone' : `Zone #${idx + 1}`}</strong><br/>${move.zone_id}<br/>ETA ${fmt(move.eta_min, 1)} min`
    );
    zoneLayer.addLayer(marker);

    const route = window.L.polyline(
      [
        [payload.originLat, payload.originLon],
        [move.lat, move.lon],
      ],
      {
        color: idx === 0 ? '#d97706' : '#0ea5a4',
        weight: idx === 0 ? 4 : 2,
        opacity: idx === 0 ? 0.85 : 0.55,
      }
    );
    dispatchMapState.routeLayer.addLayer(route);
    points.push([move.lat, move.lon]);
  });

  if (points.length > 1) {
    const bounds = window.L.latLngBounds(points);
    map.fitBounds(bounds.pad(0.2), { maxZoom: 14 });
  } else {
    map.setView([payload.originLat, payload.originLon], 13);
  }
  map.invalidateSize();
  return true;
}

function renderDispatchMap(plan) {
  if (!dispatchMapEl || !dispatchLegendEl) return;

  if (!plan) {
    if (canUseLeaflet() && dispatchMapState.map) {
      clearDispatchLeafletLayers();
      dispatchMapState.map.setView([DISPATCH_MAP_DEFAULT.lat, DISPATCH_MAP_DEFAULT.lon], DISPATCH_MAP_DEFAULT.zoom);
    } else {
      dispatchMapEl.classList.remove('map-ready');
      dispatchMapEl.textContent = 'Map pending...';
    }
    dispatchLegendEl.innerHTML = '';
    return;
  }

  const payload = buildDispatchMapPayload(plan);
  renderDispatchLegend(payload);

  if (!payload || payload.originLat == null || payload.originLon == null) {
    dispatchMapEl.classList.remove('map-ready');
    dispatchMapEl.textContent = 'Driver location missing for map.';
    return;
  }

  if (canUseLeaflet() && renderDispatchMapLeaflet(payload)) {
    return;
  }

  renderDispatchMapSvg(payload);
}

function resetMissionView() {
  if (missionStatusEl) missionStatusEl.textContent = 'Mission inactive.';
  if (missionTargetEl) missionTargetEl.textContent = '-';
  if (missionElapsedEl) missionElapsedEl.textContent = '-';
  if (missionRemainingEl) missionRemainingEl.textContent = '-';
  if (missionEtaEl) missionEtaEl.textContent = '-';
  if (missionMetaEl) missionMetaEl.textContent = 'Start from "Best zone" or from a zone row in Dispatch map.';
  if (missionProgressFillEl) missionProgressFillEl.style.width = '0%';
}

function renderMissionPanel() {
  if (!missionStatusEl) return;
  const mission = state.mission;
  const canStartFromBest = Boolean(state.dispatch && state.dispatch.reposition_option);
  if (missionStartBestBtn) missionStartBestBtn.disabled = !canStartFromBest;
  if (!mission) {
    if (missionStopBtn) missionStopBtn.disabled = true;
    resetMissionView();
    return;
  }

  const now = Date.now();
  const elapsedMin = Math.max(0, (now - Number(mission.startedAtMs || now)) / 60000);
  const remainingKm = Number.isFinite(Number(mission.remainingKm)) ? Number(mission.remainingKm) : null;
  const etaMin = Number.isFinite(Number(mission.etaMin)) ? Number(mission.etaMin) : null;
  const progressPct = clamp(Number(mission.progressPct || 0), 0, 100);
  const zoneLabel = mission.zoneId || 'zone';
  const strategy = String(mission.dispatchStrategy || 'balanced').toUpperCase();
  const arrivalThreshold = Number.isFinite(Number(mission.arrivalThresholdKm))
    ? Number(mission.arrivalThresholdKm)
    : MISSION_ARRIVAL_THRESHOLD_KM;
  const etaPolicyLimit = Number.isFinite(Number(mission.etaPolicyLimitMin)) ? Number(mission.etaPolicyLimitMin) : null;

  if (missionTargetEl) missionTargetEl.textContent = zoneLabel;
  if (missionElapsedEl) missionElapsedEl.textContent = fmtDuration(elapsedMin);
  if (missionRemainingEl) missionRemainingEl.textContent = remainingKm == null ? '-' : `${fmt(remainingKm, 2)} km`;
  if (missionEtaEl) missionEtaEl.textContent = etaMin == null ? '-' : fmtDuration(etaMin);
  if (missionProgressFillEl) missionProgressFillEl.style.width = `${progressPct.toFixed(1)}%`;

  const lastUpdate = mission.lastUpdateTs ? new Date(mission.lastUpdateTs).toLocaleTimeString() : '-';
  if (missionMetaEl) {
    const etaPolicyText = etaPolicyLimit == null ? '-' : fmtDuration(etaPolicyLimit);
    missionMetaEl.textContent = `Strategy ${strategy} - route ${mission.routeSource || 'estimated'} - progress ${fmt(progressPct, 1)}% - arrival <= ${fmt(arrivalThreshold, 2)} km - ETA policy ${etaPolicyText} - last update ${lastUpdate}`;
  }

  if (mission.completed) {
    if (mission.reportSubmitting) {
      missionStatusEl.textContent = `Mission complete on ${zoneLabel}. Saving report...`;
    } else if (mission.reportError) {
      missionStatusEl.textContent = `Mission complete on ${zoneLabel}, but report failed: ${mission.reportError}`;
    } else if (mission.reportSubmitted) {
      missionStatusEl.textContent = `Mission complete on ${zoneLabel}. Report saved.`;
    } else {
      missionStatusEl.textContent = `Mission complete on ${zoneLabel}. Great move.`;
    }
    if (missionStopBtn) missionStopBtn.disabled = true;
    return;
  }

  if (mission.active) {
    missionStatusEl.textContent = `Mission running to ${zoneLabel} (${strategy}) - keep moving.`;
    if (missionStopBtn) missionStopBtn.disabled = false;
    return;
  }

  if (mission.reportSubmitting) {
    missionStatusEl.textContent = `${mission.stopReason || `Mission paused for ${zoneLabel}.`} Saving report...`;
  } else if (mission.reportError) {
    missionStatusEl.textContent = `${mission.stopReason || `Mission paused for ${zoneLabel}.`} Report error: ${mission.reportError}`;
  } else if (mission.reportSubmitted) {
    missionStatusEl.textContent = `${mission.stopReason || `Mission paused for ${zoneLabel}.`} Report saved.`;
  } else {
    missionStatusEl.textContent = mission.stopReason || `Mission paused for ${zoneLabel}.`;
  }
  if (missionStopBtn) missionStopBtn.disabled = true;
}

function stopMissionLoop() {
  if (state.missionLoopTimer) {
    clearInterval(state.missionLoopTimer);
    state.missionLoopTimer = null;
  }
}

function stopMission(reason = 'Mission stopped.') {
  if (!state.mission) return;
  state.mission.active = false;
  state.mission.stopReason = reason;
  if (!state.mission.completedAtIso) {
    state.mission.completedAtIso = nowIsoUtc();
  }
  stopMissionLoop();
  renderMissionPanel();
  submitMissionReportIfNeeded(state.mission).catch(() => {});
}

async function resolveMissionOrigin(driverId) {
  const dispatchLat = asFloatOrNull(state.dispatch?.origin?.lat);
  const dispatchLon = asFloatOrNull(state.dispatch?.origin?.lon);
  if (dispatchLat != null && dispatchLon != null) {
    return { lat: dispatchLat, lon: dispatchLon, source: 'dispatch' };
  }
  try {
    const pos = await api(`/copilot/driver/${encodeURIComponent(driverId)}/position`);
    const lat = asFloatOrNull(pos.lat);
    const lon = asFloatOrNull(pos.lon);
    if (pos.available && lat != null && lon != null) {
      return { lat, lon, source: 'gps' };
    }
  } catch (_) {}
  return null;
}

function parseMissionTargetFromAction(btn) {
  const targetLat = asFloatOrNull(btn.getAttribute('data-dest-lat'));
  const targetLon = asFloatOrNull(btn.getAttribute('data-dest-lon'));
  if (targetLat == null || targetLon == null) return null;
  return {
    zoneId: btn.getAttribute('data-zone-id') || 'zone',
    lat: targetLat,
    lon: targetLon,
    sourceScore: asFloatOrNull(btn.getAttribute('data-score')) || 0,
    predictedPotentialEurH: asFloatOrNull(btn.getAttribute('data-potential')),
    predictedEtaMin: asFloatOrNull(btn.getAttribute('data-eta')),
    dispatchStrategy: String(btn.getAttribute('data-dispatch-strategy') || 'balanced'),
    effectiveMaxEtaMin: asFloatOrNull(btn.getAttribute('data-effective-max-eta')),
  };
}

async function startMission(target) {
  if (!target) return;
  const driverId = currentDriverId();
  const origin = await resolveMissionOrigin(driverId);
  if (!origin) {
    if (missionStatusEl) missionStatusEl.textContent = 'Cannot start mission: driver position unavailable.';
    return;
  }

  const initialDistanceKm = Math.max(0.05, haversineKm(origin.lat, origin.lon, target.lat, target.lon));
  const policy = resolveMissionPolicy(target.dispatchStrategy || state.dispatch?.dispatch_strategy || 'balanced');
  const baseEtaMin =
    target.predictedEtaMin != null ? target.predictedEtaMin : (initialDistanceKm / MISSION_FALLBACK_SPEED_KMH) * 60;
  const etaPolicyLimitMin = clamp(
    Math.max(
      policy.etaLimitFloorMin,
      baseEtaMin * policy.etaLimitMultiplier,
      target.effectiveMaxEtaMin != null ? target.effectiveMaxEtaMin * 1.35 : 0
    ),
    policy.etaLimitFloorMin,
    240
  );
  state.mission = {
    missionId: `ms_${Date.now()}_${Math.floor(Math.random() * 1000)}`,
    active: true,
    completed: false,
    stopReason: null,
    driverId,
    zoneId: target.zoneId,
    targetLat: target.lat,
    targetLon: target.lon,
    sourceScore: target.sourceScore,
    originLat: origin.lat,
    originLon: origin.lon,
    currentLat: origin.lat,
    currentLon: origin.lon,
    startedAtMs: Date.now(),
    startedAtIso: nowIsoUtc(),
    lastUpdateTs: Date.now(),
    initialDistanceKm,
    initialEtaMin: baseEtaMin,
    etaPolicyLimitMin,
    dispatchStrategy: policy.strategy,
    arrivalThresholdKm: policy.arrivalThresholdKm,
    remainingKm: initialDistanceKm,
    etaMin: (initialDistanceKm / MISSION_FALLBACK_SPEED_KMH) * 60,
    baselineEurH: asFloatOrNull(state.dispatch?.stay_option?.eur_per_hour_net),
    predictedPotentialEurH: target.predictedPotentialEurH,
    progressPct: 0,
    routeSource: 'estimated',
    reportSubmitted: false,
    reportSubmitting: false,
    reportError: null,
  };
  state.missionLastPollTs = 0;
  renderMissionPanel();
  stopMissionLoop();
  state.missionLoopTimer = setInterval(() => {
    refreshMissionTracking(false).catch(() => {});
  }, MISSION_LOOP_INTERVAL_MS);
  await refreshMissionTracking(true);
}

async function refreshMissionTracking(forcePoll) {
  const mission = state.mission;
  if (!mission) {
    renderMissionPanel();
    return;
  }
  if (!mission.active) {
    renderMissionPanel();
    return;
  }

  const now = Date.now();
  const shouldPoll = forcePoll || now - state.missionLastPollTs >= MISSION_POLL_INTERVAL_MS;
  if (!shouldPoll || state.missionPollInFlight) {
    renderMissionPanel();
    return;
  }

  state.missionPollInFlight = true;
  try {
    let currentLat = mission.currentLat;
    let currentLon = mission.currentLon;
    try {
      const pos = await api(`/copilot/driver/${encodeURIComponent(mission.driverId)}/position`);
      const lat = asFloatOrNull(pos.lat);
      const lon = asFloatOrNull(pos.lon);
      if (pos.available && lat != null && lon != null) {
        currentLat = lat;
        currentLon = lon;
      }
    } catch (_) {}

    let remainingKm = haversineKm(currentLat, currentLon, mission.targetLat, mission.targetLon);
    let etaMin = (remainingKm / MISSION_FALLBACK_SPEED_KMH) * 60;
    let routeSource = 'estimated';

    if (aroundUseOsrmInput.checked) {
      try {
        const route = await api(
          `/copilot/route?origin_lat=${encodeURIComponent(currentLat)}&origin_lon=${encodeURIComponent(currentLon)}&dest_lat=${encodeURIComponent(mission.targetLat)}&dest_lon=${encodeURIComponent(mission.targetLon)}`
        );
        const dist = asFloatOrNull(route.distance_km);
        const dur = asFloatOrNull(route.duration_min);
        if (dist != null && dur != null) {
          remainingKm = dist;
          etaMin = dur;
          routeSource = 'osrm';
        }
      } catch (_) {}
    }

    mission.currentLat = currentLat;
    mission.currentLon = currentLon;
    mission.remainingKm = remainingKm;
    mission.etaMin = etaMin;
    mission.routeSource = routeSource;
    mission.lastUpdateTs = now;
    mission.progressPct = clamp((1 - remainingKm / mission.initialDistanceKm) * 100, 0, 100);
    const arrivalThresholdKm = Number.isFinite(Number(mission.arrivalThresholdKm))
      ? Number(mission.arrivalThresholdKm)
      : MISSION_ARRIVAL_THRESHOLD_KM;
    const elapsedMin = Math.max(0, (now - Number(mission.startedAtMs || now)) / 60000);
    const etaPolicyLimitMin = Number.isFinite(Number(mission.etaPolicyLimitMin))
      ? Number(mission.etaPolicyLimitMin)
      : null;

    if (etaPolicyLimitMin != null && elapsedMin >= etaPolicyLimitMin && remainingKm > arrivalThresholdKm) {
      stopMission(
        `Mission auto-stopped: ETA policy exceeded (${fmt(elapsedMin, 1)}m >= ${fmt(etaPolicyLimitMin, 1)}m).`
      );
      return;
    }

    if (remainingKm <= arrivalThresholdKm) {
      mission.active = false;
      mission.completed = true;
      mission.completedAtIso = nowIsoUtc();
      mission.progressPct = 100;
      mission.etaMin = 0;
      mission.remainingKm = 0;
      mission.stopReason = mission.stopReason || 'Arrived on target zone.';
      stopMissionLoop();
      await submitMissionReportIfNeeded(mission);
    }
    state.missionLastPollTs = now;
  } finally {
    state.missionPollInFlight = false;
    renderMissionPanel();
  }
}

async function startBestMission() {
  const move = state.dispatch?.reposition_option;
  if (!move) {
    if (missionStatusEl) missionStatusEl.textContent = 'No best dispatch zone available to start mission.';
    return;
  }
  const lat = asFloatOrNull(move.zone_lat);
  const lon = asFloatOrNull(move.zone_lon);
  if (lat == null || lon == null) {
    if (missionStatusEl) missionStatusEl.textContent = 'Best zone coordinates are unavailable.';
    return;
  }
  await startMission({
    zoneId: move.zone_id || 'zone',
    lat,
    lon,
    sourceScore: asFloatOrNull(move.dispatch_score) || 0,
    predictedPotentialEurH: asFloatOrNull(move.estimated_potential_eur_h),
    predictedEtaMin: asFloatOrNull(move.eta_min),
    dispatchStrategy: String(state.dispatch?.dispatch_strategy || 'balanced'),
    effectiveMaxEtaMin: asFloatOrNull(state.dispatch?.effective_max_reposition_eta_min),
  });
}

function buildMissionReportPayload(mission, realizedBestOfferEurH) {
  const now = Date.now();
  const elapsedMin = Math.max(0, (now - Number(mission.startedAtMs || now)) / 60000);
  const arrivalThresholdKm = Number.isFinite(Number(mission.arrivalThresholdKm))
    ? Number(mission.arrivalThresholdKm)
    : MISSION_ARRIVAL_THRESHOLD_KM;
  const finalDistanceKm = Number.isFinite(Number(mission.remainingKm))
    ? Number(mission.remainingKm)
    : haversineKm(
        Number(mission.currentLat || mission.originLat || 0),
        Number(mission.currentLon || mission.originLon || 0),
        Number(mission.targetLat || 0),
        Number(mission.targetLon || 0)
      );
  const completedAtIso = mission.completedAtIso || nowIsoUtc();
  return {
    mission_id: mission.missionId,
    zone_id: mission.zoneId || 'zone',
    started_at: mission.startedAtIso || nowIsoUtc(),
    completed_at: completedAtIso,
    elapsed_min: elapsedMin,
    initial_distance_km: Number(mission.initialDistanceKm || 0),
    final_distance_km: Math.max(0, finalDistanceKm),
    predicted_eta_min: Number.isFinite(Number(mission.initialEtaMin)) ? Number(mission.initialEtaMin) : null,
    predicted_potential_eur_h: Number.isFinite(Number(mission.predictedPotentialEurH))
      ? Number(mission.predictedPotentialEurH)
      : null,
    baseline_eur_h: Number.isFinite(Number(mission.baselineEurH)) ? Number(mission.baselineEurH) : null,
    realized_best_offer_eur_h: Number.isFinite(Number(realizedBestOfferEurH)) ? Number(realizedBestOfferEurH) : null,
    route_source: mission.routeSource || 'estimated',
    stop_reason: mission.stopReason || null,
    success: Boolean(mission.completed || finalDistanceKm <= arrivalThresholdKm),
    dispatch_strategy: mission.dispatchStrategy || 'balanced',
    arrival_threshold_km: arrivalThresholdKm,
    eta_policy_limit_min: Number.isFinite(Number(mission.etaPolicyLimitMin)) ? Number(mission.etaPolicyLimitMin) : null,
    target_lat: Number(mission.targetLat || 0),
    target_lon: Number(mission.targetLon || 0),
  };
}

async function fetchRealizedBestOfferEurH(driverId) {
  const fallback = asFloatOrNull(state.bestOffers?.[0]?.eur_per_hour_net);
  const radius = Math.max(0.2, Number(aroundRadiusInput.value || 4.0));
  const useOsrm = aroundUseOsrmInput.checked ? 'true' : 'false';
  try {
    const payload = await api(
      `/copilot/driver/${encodeURIComponent(driverId)}/best-offers-around?radius_km=${encodeURIComponent(radius)}&limit=1&min_accept_score=0&use_osrm=${useOsrm}`
    );
    const best = Array.isArray(payload.offers) && payload.offers.length ? payload.offers[0] : null;
    const value = asFloatOrNull(best?.eur_per_hour_net);
    return value != null ? value : fallback;
  } catch (_) {
    return fallback;
  }
}

async function submitMissionReportIfNeeded(mission) {
  if (!mission || mission.reportSubmitted || mission.reportSubmitting) return;
  mission.reportSubmitting = true;
  mission.reportError = null;
  try {
    const realized = await fetchRealizedBestOfferEurH(mission.driverId);
    const payload = buildMissionReportPayload(mission, realized);
    const saved = await api(`/copilot/driver/${encodeURIComponent(mission.driverId)}/mission-report`, {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    mission.reportSubmitted = true;
    mission.reportSubmitting = false;
    mission.reportError = null;
    mission.reportId = saved.mission_id || mission.missionId;
    await refreshMissionJournal(false);
  } catch (err) {
    mission.reportSubmitting = false;
    mission.reportError = err.message;
  } finally {
    renderMissionPanel();
  }
}

function renderMissionJournal() {
  if (!missionJournalSummaryEl || !missionJournalListEl) return;
  if (state.missionJournalError) {
    missionJournalSummaryEl.textContent = `Mission journal unavailable: ${state.missionJournalError}`;
    missionJournalListEl.innerHTML = '';
    if (journalSuccessRateEl) journalSuccessRateEl.textContent = '-';
    if (journalRealizedDeltaEl) journalRealizedDeltaEl.textContent = '-';
    if (journalPredictedDeltaEl) journalPredictedDeltaEl.textContent = '-';
    if (journalAvgElapsedEl) journalAvgElapsedEl.textContent = '-';
    return;
  }

  const stats = state.missionJournalStats || {};
  const missions = Array.isArray(state.missionJournal) ? state.missionJournal : [];

  if (!missions.length) {
    missionJournalSummaryEl.textContent = 'No mission report yet for this driver.';
    missionJournalListEl.innerHTML = '<div class="muted">Complete or stop a mission to populate this journal.</div>';
    if (journalSuccessRateEl) journalSuccessRateEl.textContent = '-';
    if (journalRealizedDeltaEl) journalRealizedDeltaEl.textContent = '-';
    if (journalPredictedDeltaEl) journalPredictedDeltaEl.textContent = '-';
    if (journalAvgElapsedEl) journalAvgElapsedEl.textContent = '-';
    return;
  }

  const strategyCounts = stats.strategy_counts && typeof stats.strategy_counts === 'object' ? stats.strategy_counts : {};
  const strategyLine = Object.entries(strategyCounts)
    .sort((a, b) => Number(b[1]) - Number(a[1]))
    .map(([strategy, count]) => `${String(strategy).toUpperCase()}:${count}`)
    .join(' - ');
  missionJournalSummaryEl.textContent = `${missions.length} missions tracked - latest ${stats.latest_completed_at || '-'}${strategyLine ? ` - ${strategyLine}` : ''}`;
  if (journalSuccessRateEl) {
    journalSuccessRateEl.textContent =
      Number.isFinite(Number(stats.success_rate_pct)) ? `${fmt(stats.success_rate_pct, 1)}%` : '-';
  }
  if (journalRealizedDeltaEl) {
    journalRealizedDeltaEl.textContent = Number.isFinite(Number(stats.avg_realized_delta_eur_h))
      ? `${fmtSigned(stats.avg_realized_delta_eur_h, 2)} EUR/h`
      : '-';
  }
  if (journalPredictedDeltaEl) {
    journalPredictedDeltaEl.textContent = Number.isFinite(Number(stats.avg_predicted_delta_eur_h))
      ? `${fmtSigned(stats.avg_predicted_delta_eur_h, 2)} EUR/h`
      : '-';
  }
  if (journalAvgElapsedEl) {
    journalAvgElapsedEl.textContent = Number.isFinite(Number(stats.avg_elapsed_min))
      ? fmtDuration(stats.avg_elapsed_min)
      : '-';
  }

  missionJournalListEl.innerHTML = missions
    .slice(0, 8)
    .map((mission, idx) => {
      const success = Boolean(mission.success);
      const completed = mission.completed_at ? new Date(mission.completed_at).toLocaleString() : '-';
      const realizedDelta = Number.isFinite(Number(mission.realized_delta_eur_h))
        ? `${fmtSigned(mission.realized_delta_eur_h, 2)} EUR/h`
        : '-';
      const predictedDelta = Number.isFinite(Number(mission.predicted_delta_eur_h))
        ? `${fmtSigned(mission.predicted_delta_eur_h, 2)} EUR/h`
        : '-';
      const elapsed = Number.isFinite(Number(mission.elapsed_min)) ? fmtDuration(mission.elapsed_min) : '-';
      return `
        <article class="offer">
          <div class="offer-top">
            <strong>#${idx + 1} ${mission.zone_id || 'zone'} - ${String(mission.dispatch_strategy || 'balanced').toUpperCase()}</strong>
            <span class="badge ${success ? 'accept' : 'reject'}">${success ? 'SUCCESS' : 'MISSED'}</span>
          </div>
          <div class="offer-meta">
            <span class="muted">${completed}</span>
            <span>${elapsed}</span>
          </div>
          <div class="offer-meta">
            <span>pred ${predictedDelta}</span>
            <span>real ${realizedDelta}</span>
          </div>
          <div class="muted">reason: ${mission.stop_reason || '-'}</div>
        </article>
      `;
    })
    .join('');
}

async function refreshMissionJournal(showLoading) {
  const driverId = currentDriverId();
  if (showLoading && missionJournalSummaryEl) {
    missionJournalSummaryEl.textContent = 'Loading mission journal...';
  }
  try {
    const payload = await api(`/copilot/driver/${encodeURIComponent(driverId)}/mission-journal?limit=30`);
    state.missionJournal = Array.isArray(payload.missions) ? payload.missions : [];
    state.missionJournalStats = payload.stats || null;
    state.missionJournalError = null;
  } catch (err) {
    state.missionJournal = [];
    state.missionJournalStats = null;
    state.missionJournalError = err.message;
  }
  renderMissionJournal();
}

function renderHealth() {
  const health = state.health;
  if (!health) {
    healthMode.textContent = 'offline';
    healthMode.className = 'chip bad';
    healthGate.textContent = 'quality gate: unknown';
    healthGate.className = 'chip warn';
    healthMetrics.innerHTML = '';
    return;
  }

  const gate = health.model_quality_gate || {};
  const gateAccepted = Boolean(gate.accepted);
  const modelLoaded = Boolean(health.model_loaded);

  if (modelLoaded) {
    healthMode.textContent = 'mode: ml online';
    healthMode.className = 'chip good';
  } else {
    healthMode.textContent = 'mode: heuristic fallback';
    healthMode.className = 'chip warn';
  }

  healthGate.textContent = gateAccepted ? `quality gate: ok (${gate.reason || 'ok'})` : `quality gate: ${gate.reason || 'failed'}`;
  healthGate.className = gateAccepted ? 'chip good' : 'chip bad';

  const metrics = health.model_metrics || {};
  const fuel = health.fuel_context || {};
  const items = [];
  if (Number.isFinite(Number(metrics.roc_auc))) items.push(`AUC ${fmt(metrics.roc_auc, 3)}`);
  if (Number.isFinite(Number(metrics.average_precision))) items.push(`AP ${fmt(metrics.average_precision, 3)}`);
  if (Number.isFinite(Number(metrics.brier_score))) items.push(`Brier ${fmt(metrics.brier_score, 3)}`);
  if (Number.isFinite(Number(gate.trained_rows))) items.push(`rows ${gate.trained_rows}`);
  if (Number.isFinite(Number(fuel.fuel_price_eur_l))) items.push(`fuel ${fmt(fuel.fuel_price_eur_l, 3)} EUR/L`);
  if (fuel.source) items.push(`fuel src ${fuel.source}`);
  if (fuel.fuel_sync_status) items.push(`fuel sync ${fuel.fuel_sync_status}`);

  healthMetrics.innerHTML = '';
  items.forEach((txt) => {
    const node = document.createElement('span');
    node.className = 'chip';
    node.textContent = txt;
    healthMetrics.appendChild(node);
  });
}

function renderKpis() {
  const source = state.bestOffers.length ? state.bestOffers : state.offers;
  kpiOffers.textContent = String(source.length);

  if (!source.length) {
    kpiAcceptRate.textContent = '-';
    kpiAvgNet.textContent = '-';
    return;
  }

  const acceptCount = source.filter((x) => x.decision === 'accept').length;
  const acceptRate = (acceptCount / source.length) * 100;
  const avgNet = source.reduce((acc, x) => acc + Number(x.eur_per_hour_net || 0), 0) / source.length;

  kpiAcceptRate.textContent = `${fmt(acceptRate, 1)}%`;
  kpiAvgNet.textContent = `${fmt(avgNet, 1)}`;
}

function renderOffers() {
  const filter = offerFilter.value;
  const limit = Number(offerLimit.value);

  const rows = state.offers
    .filter((offer) => (filter === 'all' ? true : offer.decision === filter))
    .slice(0, limit);

  offersEl.innerHTML = '';

  if (!rows.length) {
    offersEl.innerHTML = '<div class="muted">No offer matches the current filter.</div>';
    return;
  }

  rows.forEach((offer) => {
    const card = document.createElement('article');
    card.className = 'offer';

    const reasons = Array.isArray(offer.explanation) ? offer.explanation : [];
    const detailChips = explanationDetailChips(offer.explanation_details, 2);
    const zone = offer.zone_id || 'unknown_zone';

    card.innerHTML = `
      <div class="offer-top">
        <strong>${offer.offer_id || 'manual-offer'}</strong>
        <span class="badge ${asDecisionClass(offer.decision)}">${(offer.decision || 'n/a').toUpperCase()}</span>
      </div>
      <div class="offer-meta">
        <span class="muted">zone ${zone}</span>
        <span><strong>${fmt(offer.accept_score, 3)}</strong> score - <strong>${fmt(offer.eur_per_hour_net, 1)}</strong> EUR/h - ${String(offer.model_used || 'heuristic')}</span>
      </div>
      <div class="muted">${routeSummary(offer)}</div>
      <div class="chips">${reasons.map((r) => `<span>${humanizeReason(r)}</span>`).join('')}${detailChips}</div>
      <div class="offer-actions">
        <button class="ghost" data-action="score-offer" data-offer-id="${offer.offer_id || ''}" data-courier-id="${offer.courier_id || ''}">Score This Offer</button>
      </div>
    `;

    offersEl.appendChild(card);
  });
}

function renderBestOffers() {
  bestOffersEl.innerHTML = '';

  if (state.bestOffersError) {
    bestOffersEl.innerHTML = `<div class="muted">Best offers unavailable: ${state.bestOffersError}</div>`;
    return;
  }

  if (!state.bestOffers.length) {
    bestOffersEl.innerHTML = '<div class="muted">No profitable nearby offers for current filters.</div>';
    return;
  }

  state.bestOffers.forEach((offer, idx) => {
    const card = document.createElement('article');
    card.className = 'offer';

    const reasons = Array.isArray(offer.explanation) ? offer.explanation : [];
    const signals = Array.isArray(offer.recommendation_signals) ? offer.recommendation_signals : [];
    const routeNotes = Array.isArray(offer.route_notes) ? offer.route_notes : [];
    const details = explanationDetailChips(offer.explanation_details, 2);

    const chips = [...reasons, ...signals, ...routeNotes]
      .map((r) => `<span>${humanizeReason(r)}</span>`)
      .join('') + details;

    card.innerHTML = `
      <div class="offer-top">
        <strong>#${idx + 1} ${offer.offer_id || 'offer'}</strong>
        <span class="badge ${asRecommendationClass(offer.recommendation_action)}">${String(offer.recommendation_action || 'skip').toUpperCase()}</span>
      </div>
      <div class="offer-meta">
        <span class="muted">pickup ${fmt(offer.distance_to_pickup_km, 2)} km</span>
        <span><strong>${fmt(offer.recommendation_score, 3)}</strong> rec - <strong>${fmt(offer.accept_score, 3)}</strong> ${String(offer.model_used || 'heuristic')}</span>
      </div>
      <div class="offer-meta">
        <span>${routeSummary(offer)}</span>
        <span><strong>${fmt(offer.eur_per_hour_net, 1)}</strong> EUR/h</span>
      </div>
      <div class="chips">${chips}</div>
      <div class="offer-actions">
        <button class="ghost" data-action="score-offer" data-offer-id="${offer.offer_id || ''}" data-courier-id="${offer.courier_id || ''}">Score This Offer</button>
      </div>
    `;

    bestOffersEl.appendChild(card);
  });
}

function renderDispatch() {
  dispatchPlanEl.innerHTML = '';

  if (state.dispatchError) {
    dispatchSummaryEl.textContent = `Dispatch unavailable: ${state.dispatchError}`;
    renderDispatchMap(null);
    return;
  }

  const plan = state.dispatch;
  if (!plan) {
    dispatchSummaryEl.textContent = 'No dispatch recommendation yet.';
    renderDispatchMap(null);
    return;
  }

  const decision = String(plan.decision || 'wait');
  const confidence = fmt(plan.confidence, 3);
  const reasons = Array.isArray(plan.reasons) ? plan.reasons.map(humanizeReason).join(' - ') : '-';
  dispatchSummaryEl.textContent = `Decision ${decision.toUpperCase()} - confidence ${confidence} - ${reasons}`;

  const top = document.createElement('article');
  top.className = 'offer';
  const strategy = String(plan.dispatch_strategy || 'balanced').toUpperCase();
  top.innerHTML = `
    <div class="offer-top">
      <strong>Target ${fmt(plan.target_hourly_net_eur, 1)} EUR/h</strong>
      <span class="badge ${asDispatchClass(decision)}">${decision.toUpperCase()}</span>
    </div>
    <div class="muted">Strategy ${strategy} - Local candidates ${Number(plan.local_candidates_count || 0)} - Reposition candidates ${Number(plan.reposition_candidates_count || 0)} - max ETA ${fmt(plan.max_reposition_eta_min, 1)} min (effective ${fmt(plan.effective_max_reposition_eta_min, 1)} min) - forecast ${fmt(plan.forecast_horizon_min, 0)} min (effective ${fmt(plan.effective_forecast_horizon_min, 0)} min)</div>
  `;
  dispatchPlanEl.appendChild(top);

  const stay = plan.stay_option;
  if (stay) {
    const card = document.createElement('article');
    card.className = 'offer';
    const signals = Array.isArray(stay.signals) ? stay.signals : [];
    const reasonsList = Array.isArray(stay.reasons) ? stay.reasons : [];
    const chips = [...signals, ...reasonsList].slice(0, 8).map((r) => `<span>${humanizeReason(r)}</span>`).join('');

    card.innerHTML = `
      <div class="offer-top">
        <strong>Stay candidate - ${stay.offer_id || 'offer'}</strong>
        <span class="badge ${asRecommendationClass(stay.recommendation_action)}">${String(stay.recommendation_action || 'consider').toUpperCase()}</span>
      </div>
      <div class="offer-meta">
        <span class="muted">zone ${stay.zone_id || '-'}</span>
        <span><strong>${fmt(stay.recommendation_score, 3)}</strong> rec - <strong>${fmt(stay.eur_per_hour_net, 1)}</strong> EUR/h</span>
      </div>
      <div class="offer-meta">
        <span>pickup ${fmt(stay.distance_to_pickup_km, 2)} km</span>
        <span>route ${stay.route_source || 'estimated'} - ${fmt(stay.route_duration_min, 1)} min</span>
      </div>
      <div class="chips">${chips}</div>
      <div class="offer-actions">
        <button class="ghost" data-action="score-offer" data-offer-id="${stay.offer_id || ''}" data-courier-id="${currentDriverId()}">Score This Offer</button>
      </div>
    `;
    dispatchPlanEl.appendChild(card);
  }

  const move = plan.reposition_option;
  if (move) {
    const originLat = asFloatOrNull(plan.origin?.lat);
    const originLon = asFloatOrNull(plan.origin?.lon);
    const zoneLat = asFloatOrNull(move.zone_lat);
    const zoneLon = asFloatOrNull(move.zone_lon);
    const card = document.createElement('article');
    card.className = 'zone';
    card.innerHTML = `
      <div class="zone-top">
        <strong>Reposition candidate - ${move.zone_id || 'zone'}</strong>
        <span>${fmt(move.dispatch_score, 3)}</span>
      </div>
      <div class="muted">ETA ${fmt(move.eta_min, 1)} min - ${fmt(move.route_distance_km, 1)} km - route ${move.route_source || 'estimated'}</div>
      <div class="muted">Gross ${fmt(move.estimated_potential_eur_h, 1)} EUR/h - Risk-adjusted ${fmt(move.risk_adjusted_potential_eur_h, 1)} EUR/h - net gain vs stay ${fmt(move.net_gain_vs_stay_eur_h, 1)} EUR/h</div>
      <div class="muted">fuel ${fmt(move.travel_cost_eur, 2)} EUR - time ${fmt(move.time_cost_eur, 2)} EUR - risk ${fmt(move.risk_cost_eur, 2)} EUR - total ${fmt(move.reposition_total_cost_eur, 2)} EUR</div>
      <div class="muted">demand ${fmt(move.demand_index, 2)} / supply ${fmt(move.supply_index, 2)} - forecast pressure ${fmt(move.forecast_pressure_ratio, 2)} - opportunity ${fmt(move.opportunity_score, 2)}</div>
      <div class="zone-meter"><div style="width:${Math.max(6, Math.min(100, Number(move.dispatch_score || 0) * 100))}%"></div></div>
      <div class="offer-actions">
        <button
          class="ghost"
          data-action="go-zone"
          data-origin-lat="${originLat == null ? '' : originLat}"
          data-origin-lon="${originLon == null ? '' : originLon}"
          data-dest-lat="${zoneLat == null ? '' : zoneLat}"
          data-dest-lon="${zoneLon == null ? '' : zoneLon}"
        >Go Zone</button>
        <button
          class="ghost"
          data-action="start-mission"
          data-zone-id="${move.zone_id || 'zone'}"
          data-dest-lat="${zoneLat == null ? '' : zoneLat}"
          data-dest-lon="${zoneLon == null ? '' : zoneLon}"
          data-score="${fmt(move.dispatch_score, 3)}"
          data-potential="${fmt(move.estimated_potential_eur_h, 3)}"
          data-eta="${fmt(move.eta_min, 3)}"
          data-dispatch-strategy="${String(plan.dispatch_strategy || 'balanced').toLowerCase()}"
          data-effective-max-eta="${asFloatOrNull(plan.effective_max_reposition_eta_min) == null ? '' : asFloatOrNull(plan.effective_max_reposition_eta_min)}"
        >Start Mission</button>
      </div>
    `;
    dispatchPlanEl.appendChild(card);
  }

  renderDispatchMap(plan);
}

function renderZones() {
  zonesEl.innerHTML = '';
  const zones = state.zones.slice(0, 8);

  if (!zones.length) {
    zonesEl.innerHTML = '<div class="muted">No zone recommendations yet.</div>';
    return;
  }

  const hasAdjusted = zones.some((z) => Number.isFinite(Number(z.adjusted_opportunity_score)));
  const scoreKey = hasAdjusted ? 'adjusted_opportunity_score' : 'opportunity_score';
  const maxScore = Math.max(
    ...zones.map((z) => Number(z[scoreKey] != null ? z[scoreKey] : z.opportunity_score) || 0),
    0.001
  );

  zones.forEach((zone, idx) => {
    const row = document.createElement('article');
    row.className = 'zone';

    const baseScore = Number(zone.opportunity_score || 0);
    const adjustedScore = Number(zone[scoreKey] != null ? zone[scoreKey] : baseScore);
    const meter = Math.max(6, Math.min(100, (adjustedScore / maxScore) * 100));

    const distance = Number(zone.distance_km);
    const fuelCost = Number(zone.reposition_cost_eur);
    const repoTime = Number(zone.reposition_time_min);

    const costLine =
      Number.isFinite(distance)
        ? `<div class="muted">${fmt(distance, 2)} km away - ~${fmt(repoTime, 0)} min, fuel ${fmt(fuelCost, 2)} EUR</div>`
        : '';

    const adjustedLine =
      hasAdjusted && Math.abs(adjustedScore - baseScore) > 0.005
        ? `<div class="muted">base ${fmt(baseScore, 2)} -> adjusted ${fmt(adjustedScore, 2)} after distance penalty</div>`
        : '';

    const homewardFactor = Number(zone.homeward_factor);
    const homewardGainPct = Number(zone.homeward_gain_pct);
    const homewardLine =
      Number.isFinite(homewardFactor) && homewardFactor > 0.05 && Number.isFinite(homewardGainPct) && homewardGainPct > 0
        ? `<div class="muted">homeward: closer to home (+${fmt(homewardGainPct * 100, 1)}% bonus)</div>`
        : '';

    row.innerHTML = `
      <div class="zone-top">
        <strong>#${idx + 1} ${zone.zone_id || 'unknown_zone'}</strong>
        <span>${fmt(adjustedScore, 2)}</span>
      </div>
      <div class="muted">demand ${fmt(zone.demand_index, 2)} / supply ${fmt(zone.supply_index, 2)} - weather ${fmt(zone.weather_factor, 2)} - traffic ${fmt(zone.traffic_factor, 2)}</div>
      ${costLine}
      ${adjustedLine}
      ${homewardLine}
      <div class="zone-meter"><div style="width:${meter}%"></div></div>
    `;

    zonesEl.appendChild(row);
  });
}

function renderReplay() {
  const events = state.replay;
  kpiReplay.textContent = String(events.length);
  replayTimeline.innerHTML = '';

  if (!events.length) {
    replaySummary.textContent = 'No replay events in selected window.';
    replayTimeline.innerHTML = '<div class="muted">Try a wider window (Last 2h or Today).</div>';
    return;
  }

  const byType = {};
  events.forEach((evt) => {
    const key = evt.event_type || 'unknown';
    byType[key] = (byType[key] || 0) + 1;
  });

  const summary = Object.entries(byType)
    .sort((a, b) => b[1] - a[1])
    .map(([name, count]) => `${name}: ${count}`)
    .join(' - ');

  const firstTs = events[0].ts;
  const lastTs = events[events.length - 1].ts;
  replaySummary.textContent = `${events.length} events - ${summary} - window ${firstTs} -> ${lastTs}`;

  events.slice(-20).reverse().forEach((evt) => {
    const row = document.createElement('div');
    row.className = 'event-row';
    row.innerHTML = `
      <strong>${evt.event_type || 'event'} - ${evt.status || 'n/a'}</strong>
      <div>offer ${evt.offer_id || '-'} - order ${evt.order_id || '-'} - zone ${evt.zone_id || '-'}</div>
      <div class="muted">${evt.ts} - topic ${evt.topic || '-'}</div>
    `;
    replayTimeline.appendChild(row);
  });
}

function buildAroundQuery(driver) {
  const radius = Math.max(0.2, Number(aroundRadiusInput.value || 4.0));
  const limit = Math.max(1, Math.min(50, Number(aroundLimitInput.value || 10)));
  const minAccept = Math.max(0.0, Math.min(1.0, Number(aroundMinScoreInput.value || 0.25)));
  const useOsrm = aroundUseOsrmInput.checked ? 'true' : 'false';

  return `/copilot/driver/${encodeURIComponent(driver)}/best-offers-around?radius_km=${encodeURIComponent(radius)}&limit=${encodeURIComponent(limit)}&min_accept_score=${encodeURIComponent(minAccept)}&use_osrm=${useOsrm}`;
}

function buildZoneQuery(driver) {
  const topK = 8;
  const rawWeight = Number(zoneDistanceWeightInput?.value);
  const weight = Number.isFinite(rawWeight) ? Math.max(0, Math.min(1, rawWeight)) : 0.3;
  const rawMax = Number(zoneMaxDistanceKmInput?.value);
  let query = `/copilot/driver/${encodeURIComponent(driver)}/next-best-zone?top_k=${topK}&distance_weight=${encodeURIComponent(weight)}`;
  if (Number.isFinite(rawMax) && rawMax > 0) {
    const clamped = Math.max(0.5, Math.min(50, rawMax));
    query += `&max_distance_km=${encodeURIComponent(clamped)}`;
  }
  const rawHomeLat = asFloatOrNull(zoneHomeLatInput?.value);
  const rawHomeLon = asFloatOrNull(zoneHomeLonInput?.value);
  if (rawHomeLat != null && rawHomeLon != null) {
    query += `&home_lat=${encodeURIComponent(rawHomeLat)}&home_lon=${encodeURIComponent(rawHomeLon)}`;
  }
  return query;
}

function buildDispatchQuery(driver) {
  const radius = Math.max(0.2, Number(aroundRadiusInput.value || 4.0));
  const aroundLimit = Math.max(1, Math.min(30, Number(aroundLimitInput.value || 10)));
  const minAccept = Math.max(0.0, Math.min(1.0, Number(aroundMinScoreInput.value || 0.25)));
  const maxEta = Math.max(3, Math.min(60, Number(dispatchMaxEtaInput.value || 20)));
  const forecastHorizon = Math.max(10, Math.min(90, Number(dispatchForecastHorizonInput.value || 30)));
  const zoneTopK = Math.max(1, Math.min(20, Number(dispatchZoneTopKInput.value || 5)));
  const scanLimit = Math.max(10, Math.min(400, aroundLimit * 12));
  const useOsrm = aroundUseOsrmInput.checked ? 'true' : 'false';
  const dispatchStrategy = String(dispatchStrategyInput?.value || 'balanced');

  return `/copilot/driver/${encodeURIComponent(driver)}/instant-dispatch?around_radius_km=${encodeURIComponent(radius)}&around_limit=${encodeURIComponent(aroundLimit)}&scan_limit=${encodeURIComponent(scanLimit)}&zone_top_k=${encodeURIComponent(zoneTopK)}&min_accept_score=${encodeURIComponent(minAccept)}&max_reposition_eta_min=${encodeURIComponent(maxEta)}&forecast_horizon_min=${encodeURIComponent(forecastHorizon)}&dispatch_strategy=${encodeURIComponent(dispatchStrategy)}&use_osrm=${useOsrm}`;
}

async function refreshHealth() {
  if (state.refreshHealthInFlight) return;
  state.refreshHealthInFlight = true;
  try {
    state.health = await api('/copilot/health');
  } catch (_) {
    state.health = null;
  } finally {
    state.refreshHealthInFlight = false;
  }
  renderHealth();
}

async function refreshFuelContextNow(force = false) {
  if (!refreshFuelBtn) return;
  const prev = refreshFuelBtn.textContent;
  refreshFuelBtn.disabled = true;
  refreshFuelBtn.textContent = 'Syncing fuel...';
  try {
    const payload = await api(`/copilot/fuel-context/refresh?force=${force ? 'true' : 'false'}`, { method: 'POST' });
    await refreshHealth();
    const status = payload?.refresh_result?.status || payload?.fuel_sync_status || 'ok';
    if (missionStatusEl) missionStatusEl.textContent = `Fuel context sync status: ${status}.`;
  } catch (err) {
    const msg = String(err?.message || '');
    if (msg.includes('404')) {
      try {
        await api('/copilot/fuel-context');
        await refreshHealth();
        if (missionStatusEl) missionStatusEl.textContent = 'Fuel sync endpoint unavailable on this build. Context loaded.';
      } catch (_) {
        if (missionStatusEl) missionStatusEl.textContent = `Fuel sync failed: ${msg}`;
      }
    } else if (missionStatusEl) {
      missionStatusEl.textContent = `Fuel sync failed: ${msg}`;
    }
  } finally {
    refreshFuelBtn.disabled = false;
    refreshFuelBtn.textContent = prev;
  }
}

async function refreshDriverData() {
  // If a refresh is already running, queue a trailing re-run so the user's
  // latest inputs are never silently dropped.
  if (state.refreshDriverInFlight) {
    state.refreshDriverQueued = true;
    return;
  }
  state.refreshDriverInFlight = true;
  const driver = currentDriverId();
  const limit = Number(offerLimit.value || 12);
  if (state.missionJournalDriver !== driver) {
    state.missionJournalDriver = driver;
    refreshMissionJournal(true).catch(() => {});
  }

  offersEl.innerHTML = '<div class="muted">Loading offers...</div>';
  bestOffersEl.innerHTML = '<div class="muted">Loading nearby recommendations...</div>';
  dispatchSummaryEl.textContent = 'Loading instant dispatch recommendation...';
  dispatchPlanEl.innerHTML = '';
  if (dispatchMapState.map) {
    clearDispatchLeafletLayers();
  } else {
    dispatchMapEl.classList.remove('map-ready');
    dispatchMapEl.textContent = 'Rendering dispatch map...';
  }
  dispatchLegendEl.innerHTML = '';
  zonesEl.innerHTML = '<div class="muted">Loading zones...</div>';

  try {
    const [offersResp, zonesResp, bestResp, dispatchResp] = await Promise.all([
      api(`/copilot/driver/${encodeURIComponent(driver)}/offers?limit=${Math.max(limit, 20)}`),
      api(buildZoneQuery(driver)),
      api(buildAroundQuery(driver)).catch((err) => ({ offers: [], _error: err.message })),
      api(buildDispatchQuery(driver)).catch((err) => ({ _error: err.message })),
    ]);

    state.offers = Array.isArray(offersResp.offers) ? offersResp.offers : [];
    state.zones = Array.isArray(zonesResp.recommendations) ? zonesResp.recommendations : [];
    state.bestOffers = Array.isArray(bestResp.offers) ? bestResp.offers : [];
    state.bestOffersError = bestResp._error || null;
    state.dispatch = dispatchResp._error ? null : dispatchResp;
    state.dispatchError = dispatchResp._error || null;

    renderKpis();
    renderOffers();
    renderBestOffers();
    renderDispatch();
    renderMissionPanel();
    renderZones();
  } catch (err) {
    offersEl.innerHTML = `<div class="muted">Failed loading offers: ${err.message}</div>`;
    bestOffersEl.innerHTML = `<div class="muted">Failed loading nearby recommendations: ${err.message}</div>`;
    dispatchSummaryEl.textContent = `Dispatch failed: ${err.message}`;
    dispatchPlanEl.innerHTML = '';
    if (dispatchMapState.map) {
      clearDispatchLeafletLayers();
      dispatchMapState.map.setView([DISPATCH_MAP_DEFAULT.lat, DISPATCH_MAP_DEFAULT.lon], DISPATCH_MAP_DEFAULT.zoom);
    } else {
      dispatchMapEl.classList.remove('map-ready');
      dispatchMapEl.textContent = 'Dispatch map unavailable.';
    }
    dispatchLegendEl.innerHTML = '';
    zonesEl.innerHTML = `<div class="muted">Failed loading zones: ${err.message}</div>`;
    state.offers = [];
    state.bestOffers = [];
    state.bestOffersError = err.message;
    state.dispatch = null;
    state.dispatchError = err.message;
    state.zones = [];
    renderKpis();
    renderMissionPanel();
  } finally {
    state.refreshDriverInFlight = false;
    if (state.refreshDriverQueued) {
      state.refreshDriverQueued = false;
      // Give the event loop a tick so a burst of change events can coalesce.
      setTimeout(() => refreshDriverData(), 0);
    }
  }
}

const refreshDriverDataDebounced = debounce(refreshDriverData, REFRESH_DEBOUNCE_MS);

async function replayWindow() {
  const driver = currentDriverId();
  const fromTs = fromInput.value.trim();
  const toTs = toInput.value.trim();

  replaySummary.textContent = 'Loading replay...';
  replayTimeline.innerHTML = '';

  try {
    const replay = await api(
      `/copilot/replay?from=${encodeURIComponent(fromTs)}&to=${encodeURIComponent(toTs)}&driver_id=${encodeURIComponent(driver)}&limit=400`
    );
    state.replay = Array.isArray(replay.events) ? replay.events : [];
    renderReplay();
  } catch (err) {
    state.replay = [];
    kpiReplay.textContent = '-';
    replaySummary.textContent = `Replay failed: ${err.message}`;
    replayTimeline.innerHTML = '';
  }
}

async function scoreManualOffer(payloadOverride = null) {
  const payload = payloadOverride || {
    courier_id: currentDriverId(),
    estimated_fare_eur: Number($('fare').value),
    estimated_distance_km: Number($('distance').value),
    estimated_duration_min: Number($('duration').value),
    demand_index: Number($('demand').value),
    supply_index: Number($('supply').value),
    weather_factor: 1.0,
    traffic_factor: Number($('traffic').value),
    use_osrm: Boolean(aroundUseOsrmInput.checked),
  };

  try {
    const score = await api('/copilot/score-offer', {
      method: 'POST',
      body: JSON.stringify(payload),
    });

    scoreProb.textContent = fmt(score.accept_score, 3);
    scoreDecision.textContent = (score.decision || '-').toUpperCase();
    scoreEur.textContent = fmt(score.eur_per_hour_net, 1);

    const expl = Array.isArray(score.explanation) ? score.explanation.map(humanizeReason).join(' - ') : '-';
    const detailSummary = Array.isArray(score.explanation_details)
      ? score.explanation_details.slice(0, 2).map(formatExplanationDetail).filter(Boolean).join(' | ')
      : '';
    const route = score.route_source ? ` | ${routeSummary(score)}` : '';
    const detailsText = detailSummary ? ` | ${detailSummary}` : '';
    scoreReasons.textContent = `[${score.model_used || 'unknown'}] ${expl}${detailsText}${route}`;
  } catch (err) {
    scoreReasons.textContent = `Score failed: ${err.message}`;
  }
}

function applyPreset(name) {
  const presets = {
    safe: { fare: 7.2, distance: 2.5, duration: 21, demand: 0.9, supply: 1.3, traffic: 1.2 },
    balanced: { fare: 12.5, distance: 3.2, duration: 17, demand: 1.3, supply: 0.9, traffic: 1.05 },
    surge: { fare: 19.5, distance: 2.8, duration: 14, demand: 1.8, supply: 0.7, traffic: 0.95 },
  };

  const p = presets[name] || presets.balanced;
  $('fare').value = p.fare;
  $('distance').value = p.distance;
  $('duration').value = p.duration;
  $('demand').value = p.demand;
  $('supply').value = p.supply;
  $('traffic').value = p.traffic;
}

function bindOfferActionDelegation(container) {
  if (!container) return;
  container.addEventListener('click', async (event) => {
    const btn = event.target.closest('button[data-action]');
    if (!btn) return;

    const action = btn.getAttribute('data-action') || '';
    if (action === 'go-zone') {
      const originLat = asFloatOrNull(btn.getAttribute('data-origin-lat'));
      const originLon = asFloatOrNull(btn.getAttribute('data-origin-lon'));
      const destLat = asFloatOrNull(btn.getAttribute('data-dest-lat'));
      const destLon = asFloatOrNull(btn.getAttribute('data-dest-lon'));
      if (originLat == null || originLon == null || destLat == null || destLon == null) return;
      window.open(mapsDirectionsUrl(originLat, originLon, destLat, destLon), '_blank', 'noopener,noreferrer');
      return;
    }

    if (action === 'start-mission') {
      const target = parseMissionTargetFromAction(btn);
      if (!target) return;
      btn.disabled = true;
      const prev = btn.textContent;
      btn.textContent = 'Starting...';
      try {
        await startMission(target);
      } finally {
        btn.disabled = false;
        btn.textContent = prev;
      }
      return;
    }

    if (action !== 'score-offer') return;

    const offerId = btn.getAttribute('data-offer-id') || '';
    const courierId = btn.getAttribute('data-courier-id') || currentDriverId();
    btn.disabled = true;
    btn.textContent = 'Scoring...';

    try {
      await scoreManualOffer({
        offer_id: offerId,
        courier_id: courierId,
        use_osrm: Boolean(aroundUseOsrmInput.checked),
      });
    } finally {
      btn.disabled = false;
      btn.textContent = 'Score This Offer';
    }
  });
}

function safeBind(element, eventName, handler) {
  if (!element || typeof element.addEventListener !== 'function') return;
  element.addEventListener(eventName, handler);
}

function startAutoRefreshLoop() {
  if (state.autoRefreshTimer) {
    clearInterval(state.autoRefreshTimer);
    state.autoRefreshTimer = null;
  }

  if (!autoRefreshBox) {
    return;
  }

  if (!autoRefreshBox.checked) {
    return;
  }

  state.autoRefreshTimer = setInterval(async () => {
    if (state.refreshHealthInFlight || state.refreshDriverInFlight) return;
    await refreshHealth();
    await refreshDriverData();
  }, 12000);
}

async function refreshAll() {
  await refreshHealth();
  await refreshDriverData();
}

if ('serviceWorker' in navigator) {
  navigator.serviceWorker.register('/static/copilot/sw.js').catch(() => {});
}

safeBind($('window30'), 'click', () => {
  setWindowMinutes(30);
  replayWindow();
});

safeBind($('window120'), 'click', () => {
  setWindowMinutes(120);
  replayWindow();
});

safeBind($('windowToday'), 'click', () => {
  setTodayWindow();
  replayWindow();
});

safeBind($('refreshBtn'), 'click', refreshAll);
safeBind($('refreshAroundBtn'), 'click', refreshDriverData);
safeBind(refreshFuelBtn, 'click', () => {
  refreshFuelContextNow(false).catch(() => {});
});
safeBind($('replayBtn'), 'click', replayWindow);
safeBind($('scoreBtn'), 'click', () => scoreManualOffer());
safeBind($('presetSafe'), 'click', () => applyPreset('safe'));
safeBind($('presetBalanced'), 'click', () => applyPreset('balanced'));
safeBind($('presetSurge'), 'click', () => applyPreset('surge'));
safeBind(missionStartBestBtn, 'click', () => {
  startBestMission().catch(() => {});
});
safeBind(missionStopBtn, 'click', () => {
  stopMission('Mission stopped by driver.');
});
safeBind(driverInput, 'change', () => {
  stopMission('Driver changed, mission reset.');
  refreshDriverData();
});
safeBind(offerFilter, 'change', renderOffers);
safeBind(offerLimit, 'change', refreshDriverDataDebounced);
safeBind(aroundRadiusInput, 'change', refreshDriverDataDebounced);
safeBind(aroundLimitInput, 'change', refreshDriverDataDebounced);
safeBind(aroundMinScoreInput, 'change', refreshDriverDataDebounced);
safeBind(aroundUseOsrmInput, 'change', refreshDriverData);
safeBind(dispatchMaxEtaInput, 'change', refreshDriverDataDebounced);
safeBind(dispatchForecastHorizonInput, 'change', refreshDriverDataDebounced);
safeBind(dispatchZoneTopKInput, 'change', refreshDriverDataDebounced);
safeBind(dispatchStrategyInput, 'change', refreshDriverData);
safeBind(zoneDistanceWeightInput, 'change', refreshDriverDataDebounced);
safeBind(zoneMaxDistanceKmInput, 'change', refreshDriverDataDebounced);
safeBind(zoneHomeLatInput, 'change', refreshDriverDataDebounced);
safeBind(zoneHomeLonInput, 'change', refreshDriverDataDebounced);
safeBind(autoRefreshBox, 'change', startAutoRefreshLoop);

bindOfferActionDelegation(offersEl);
bindOfferActionDelegation(bestOffersEl);
bindOfferActionDelegation(dispatchPlanEl);
bindOfferActionDelegation(dispatchLegendEl);
renderMissionPanel();
setWindowMinutes(30);
applyPreset('balanced');
refreshAll().then(() => replayWindow());
startAutoRefreshLoop();
