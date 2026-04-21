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
  shiftPlan: null,
  shiftPlanError: null,
  driverProfile: null,
  driverProfileError: null,
  replay: [],
  replayVisual: null,
  replayCursorIndex: 0,
  selectedOfferKey: null,
  selectedOfferSource: null,
  flowStep: 'choose',
  lastScoredOfferId: null,
  lastActionSummary: null,
  autoRefreshTimer: null,
  objectiveWeights: null,
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
const shiftPlanSummaryEl = $('shiftPlanSummary');
const shiftPlanEl = $('shiftPlanList');
const replaySummary = $('replaySummary');
const replayMapEl = $('replayMap');
const replayScrubberEl = $('replayScrubber');
const replayCursorLabelEl = $('replayCursorLabel');
const replayCursorStatusEl = $('replayCursorStatus');
const replayTimeline = $('replayTimeline');
const uxStatusLine = $('uxStatusLine');

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
const shiftPlanHorizonInput = $('shiftPlanHorizon');
const shiftPlanTopKInput = $('shiftPlanTopK');
const refreshShiftPlanBtn = $('refreshShiftPlanBtn');
const profileTargetEurHInput = $('profileTargetEurH');
const profileConsumptionInput = $('profileConsumption');
const profileRiskAversionInput = $('profileRiskAversion');
const profileMaxEtaInput = $('profileMaxEta');
const profileSaveBtn = $('profileSaveBtn');
const profileStatusEl = $('profileStatus');
const objWeightGainInput = $('objWeightGain');
const objWeightTimeInput = $('objWeightTime');
const objWeightFuelInput = $('objWeightFuel');
const objWeightGainValueEl = $('objWeightGainValue');
const objWeightTimeValueEl = $('objWeightTimeValue');
const objWeightFuelValueEl = $('objWeightFuelValue');
const objectiveWeightStatusEl = $('objectiveWeightStatus');
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
const flowStepChooseEl = $('flowStepChoose');
const flowStepScoreEl = $('flowStepScore');
const flowStepActionEl = $('flowStepAction');
const decisionStatusEl = $('decisionStatus');
const decisionOfferCardEl = $('decisionOfferCard');
const decisionQuickScoreBtn = $('decisionQuickScoreBtn');
const decisionScoreBtn = $('decisionScoreBtn');
const decisionActionBtn = $('decisionActionBtn');

const API_TIMEOUT_MS = 12000;
const API_RETRY_DELAY_MS = 350;
const API_RETRY_GET = 1;
const UI_STATE_TYPES = ['loading', 'error', 'empty', 'success'];
const DRIVER_PROFILE_DEFAULTS = Object.freeze({
  target_eur_h: 18,
  consommation_l_100: 7.5,
  aversion_risque: 0.5,
  max_eta: 20,
});
const DRIVER_PROFILE_LIMITS = Object.freeze({
  target_eur_h: { min: 0, max: 150, digits: 1 },
  consommation_l_100: { min: 2, max: 30, digits: 1 },
  aversion_risque: { min: 0, max: 1, digits: 2 },
  max_eta: { min: 3, max: 60, digits: 1 },
});
const OBJECTIVE_WEIGHT_DEFAULTS = Object.freeze({
  w_gain: 62,
  w_time: 23,
  w_fuel: 15,
});
const OBJECTIVE_WEIGHT_LIMITS = Object.freeze({
  min: 0,
  max: 100,
});

function waitMs(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeApiError(error) {
  if (error && error.name === 'AbortError') {
    return new Error('Request timeout');
  }
  return error instanceof Error ? error : new Error(String(error || 'Request failed'));
}

function errorMessage(error, fallback = 'Request failed') {
  if (error instanceof Error && error.message) return error.message;
  const msg = String(error || '').trim();
  return msg || fallback;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
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

function renderListState(target, type, text) {
  if (!target) return;
  const kind = UI_STATE_TYPES.includes(type) ? type : 'empty';
  target.setAttribute('aria-busy', kind === 'loading' ? 'true' : 'false');
  target.innerHTML = '';
  const node = document.createElement('div');
  node.className = `state-message ${kind}`;
  node.textContent = String(text || '').trim();
  target.appendChild(node);
}

function setUxStatus(type, text) {
  if (!uxStatusLine) return;
  uxStatusLine.className = `state-message ${UI_STATE_TYPES.includes(type) ? type : 'loading'}`;
  uxStatusLine.textContent = text;
}

async function withBusyButton(button, pendingLabel, action) {
  if (typeof action !== 'function') {
    return undefined;
  }
  if (!button) {
    return action();
  }
  const prevDisabled = Boolean(button.disabled);
  const prevLabel = button.textContent;
  button.disabled = true;
  if (pendingLabel) button.textContent = pendingLabel;
  try {
    return await action();
  } finally {
    button.disabled = prevDisabled;
    button.textContent = prevLabel;
  }
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
    GAIN_STRONG: 'strong gain quality',
    GAIN_WEAK: 'weak gain quality',
    TIME_EFFICIENT: 'time efficient',
    TIME_HEAVY: 'time heavy',
    FUEL_EFFICIENT: 'fuel efficient',
    FUEL_HEAVY: 'fuel heavy',
    RISK_LOW: 'low operational risk',
    RISK_HIGH: 'high operational risk',
    TARGET_ABOVE: 'above target',
    TARGET_BELOW: 'below target',
    CONTEXT_FALLBACK: 'context fallback active',
    CONTEXT_STALE: 'context freshness degraded',
    DECISION_CONFIDENT: 'decision confident',
    DECISION_BORDERLINE: 'decision borderline',
    DECISION_BELOW_THRESHOLD: 'decision below threshold',
    PROFILE_BALANCED: 'balanced scoring profile',
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
  return `${escapeHtml(label)}: ${escapeHtml(renderedValue)} (${escapeHtml(impact)})`;
}

function explanationDetailChips(details, limit = 2) {
  const rows = Array.isArray(details) ? details.slice(0, Math.max(0, limit)) : [];
  return rows
    .map((d) => `<span>${formatExplanationDetail(d)}</span>`)
    .join('');
}

function listToChips(values, limit = 8) {
  const rows = Array.isArray(values) ? values : [];
  const seen = new Set();
  const chips = [];
  for (const value of rows) {
    const key = String(value || '').trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    chips.push(`<span>${escapeHtml(humanizeReason(key))}</span>`);
    if (chips.length >= Math.max(0, Number(limit) || 0)) break;
  }
  return chips.join('');
}

function scoreBreakdownText(scoreBreakdown, limit = 4) {
  if (!scoreBreakdown || typeof scoreBreakdown !== 'object') return '';
  const dimensions =
    scoreBreakdown.dimensions && typeof scoreBreakdown.dimensions === 'object'
      ? scoreBreakdown.dimensions
      : null;
  if (!dimensions) return '';

  const preferredAxes = ['gain', 'time', 'fuel', 'risk'];
  const axisOrder = [...preferredAxes, ...Object.keys(dimensions).filter((axis) => !preferredAxes.includes(axis))];
  const chunks = [];

  for (const axis of axisOrder) {
    if (chunks.length >= Math.max(0, Number(limit) || 0)) break;
    const dim = dimensions[axis];
    if (!dim || typeof dim !== 'object') continue;
    const label = String(dim.label || humanizeReason(axis)).trim();
    const score = Number(dim.score);
    const contribution = Number(dim.contribution);
    if (!Number.isFinite(score) || !Number.isFinite(contribution)) continue;
    chunks.push(`${label} ${fmt(score, 2)} (${fmt(contribution, 2)})`);
  }

  if (!chunks.length) return '';
  const totalScore = Number(scoreBreakdown.total_score);
  const totalLabel = Number.isFinite(totalScore) ? fmt(totalScore, 3) : '-';
  return `Explainability ${totalLabel}: ${chunks.join(' | ')}`;
}

function scoreBreakdownHtml(scoreBreakdown, limit = 4) {
  const text = scoreBreakdownText(scoreBreakdown, limit);
  return text ? `<div class="muted">${escapeHtml(text)}</div>` : '';
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

function offerKey(offer, source, indexHint = 0) {
  const offerId = String(offer?.offer_id || '').trim();
  if (offerId) return `offer:${offerId}`;
  const zoneId = String(offer?.zone_id || 'zone').trim();
  return `${source}:${zoneId}:${indexHint}`;
}

function allOfferCandidates() {
  const best = state.bestOffers.map((offer, idx) => ({
    offer,
    source: 'best',
    key: offerKey(offer, 'best', idx),
  }));
  const latest = state.offers.map((offer, idx) => ({
    offer,
    source: 'offers',
    key: offerKey(offer, 'offers', idx),
  }));
  return [...best, ...latest];
}

function getSelectedOfferCandidate() {
  if (!state.selectedOfferKey) return null;
  return allOfferCandidates().find((item) => item.key === state.selectedOfferKey) || null;
}

function getTopOfferCandidate() {
  const candidates = allOfferCandidates();
  return candidates.length ? candidates[0] : null;
}

function ensureSelectedOffer() {
  const selected = getSelectedOfferCandidate();
  if (selected) return selected;
  const fallback = getTopOfferCandidate();
  if (!fallback) {
    state.selectedOfferKey = null;
    state.selectedOfferSource = null;
    return null;
  }
  state.selectedOfferKey = fallback.key;
  state.selectedOfferSource = fallback.source;
  return fallback;
}

function setFlowStep(step) {
  const allowed = ['choose', 'score', 'action'];
  state.flowStep = allowed.includes(step) ? step : 'choose';
  const order = ['choose', 'score', 'action'];
  const level = Math.max(0, order.indexOf(state.flowStep));
  const nodes = [flowStepChooseEl, flowStepScoreEl, flowStepActionEl];
  nodes.forEach((node, idx) => {
    if (!node) return;
    node.classList.toggle('active', idx <= level);
  });
}

function offerMetricsHtml(offer) {
  const costs = offer && typeof offer.costs === 'object' && offer.costs ? offer.costs : {};
  const eta = Number(offer?.route_duration_min);
  const netHourly = Number(offer?.eur_per_hour_net);
  const netTrip = Number(offer?.estimated_net_eur);
  const fuel = Number(costs.fuel_cost_eur);
  const platform = Number(costs.platform_fee_eur);
  const targetGap = Number(offer?.target_gap_eur_h);
  const costTotal = Number.isFinite(fuel) || Number.isFinite(platform)
    ? `${fmt(fuel, 2)} / ${fmt(platform, 2)}`
    : '-';
  const gapText = Number.isFinite(targetGap) ? fmtSigned(targetGap, 1) : '-';

  return `
    <div class="offer-metrics">
      <div class="offer-metric"><div class="k">Net EUR/h</div><div class="v">${fmt(netHourly, 1)}</div></div>
      <div class="offer-metric"><div class="k">Net Trip</div><div class="v">${fmt(netTrip, 2)}</div></div>
      <div class="offer-metric"><div class="k">ETA min</div><div class="v">${fmt(eta, 1)}</div></div>
      <div class="offer-metric"><div class="k">Fuel/Fee</div><div class="v">${costTotal}</div></div>
    </div>
    <div class="muted">target gap ${gapText} EUR/h</div>
  `;
}

function resolveSelectedActionRoute() {
  const selected = getSelectedOfferCandidate();
  if (!selected || !selected.offer) return null;
  const offer = selected.offer;
  const originLat = asFloatOrNull(state.dispatch?.origin?.lat);
  const originLon = asFloatOrNull(state.dispatch?.origin?.lon);

  const candidateLats = [offer.zone_lat, offer.pickup_lat, offer.dest_lat, offer.dropoff_lat];
  const candidateLons = [offer.zone_lon, offer.pickup_lon, offer.dest_lon, offer.dropoff_lon];
  let destLat = candidateLats.map(asFloatOrNull).find((x) => x != null) ?? null;
  let destLon = candidateLons.map(asFloatOrNull).find((x) => x != null) ?? null;
  if ((destLat == null || destLon == null) && state.dispatch && Array.isArray(state.dispatch.reposition_candidates)) {
    const matched = state.dispatch.reposition_candidates.find((z) => String(z.zone_id || '') === String(offer.zone_id || ''));
    if (matched) {
      destLat = asFloatOrNull(matched.zone_lat);
      destLon = asFloatOrNull(matched.zone_lon);
    }
  }
  if (originLat == null || originLon == null || destLat == null || destLon == null) {
    return null;
  }
  return { originLat, originLon, destLat, destLon };
}

function renderDecisionFlow() {
  const selected = ensureSelectedOffer();
  if (!decisionOfferCardEl || !decisionStatusEl || !decisionScoreBtn || !decisionActionBtn) return;

  if (!selected) {
    decisionOfferCardEl.className = 'offer';
    decisionOfferCardEl.innerHTML = '<div class="muted">No offer selected yet.</div>';
    decisionStatusEl.className = 'state-message empty';
    decisionStatusEl.textContent = 'Choose an offer to start the 3-step flow.';
    decisionScoreBtn.disabled = true;
    decisionActionBtn.disabled = true;
    setFlowStep('choose');
    return;
  }

  const offer = selected.offer;
  const action = String(offer.recommendation_action || offer.decision || 'consider').toUpperCase();
  const decisionBreakdown = scoreBreakdownHtml(offer.score_breakdown, 4);
  const decisionReasonChips = listToChips(offer.reason_codes, 4);
  decisionOfferCardEl.className = 'offer selected';
  decisionOfferCardEl.innerHTML = `
    <div class="offer-top">
      <strong>${escapeHtml(offer.offer_id || 'offer')}</strong>
      <span class="badge ${asRecommendationClass(String(offer.recommendation_action || offer.decision || '').toLowerCase())}">${escapeHtml(action)}</span>
    </div>
    <div class="offer-meta">
      <span class="muted">source ${escapeHtml(selected.source)}</span>
      <span><strong>${fmt(offer.accept_score, 3)}</strong> score - ${escapeHtml(String(offer.model_used || 'heuristic'))}</span>
    </div>
    ${offerMetricsHtml(offer)}
    <div class="muted">${escapeHtml(routeSummary(offer))}</div>
    ${decisionBreakdown}
    ${decisionReasonChips ? `<div class="chips">${decisionReasonChips}</div>` : ''}
  `;

  const canRoute = Boolean(resolveSelectedActionRoute());
  decisionScoreBtn.disabled = false;
  decisionActionBtn.disabled = !canRoute;
  decisionActionBtn.textContent = canRoute ? 'Open Action Route' : 'Route Unavailable';
  if (state.flowStep === 'action' && state.lastActionSummary) {
    decisionStatusEl.className = 'state-message success';
    decisionStatusEl.textContent = state.lastActionSummary;
    setFlowStep('action');
    return;
  }
  if (state.lastScoredOfferId && String(state.lastScoredOfferId) === String(offer.offer_id || '')) {
    decisionStatusEl.className = 'state-message success';
    decisionStatusEl.textContent = `Scored ${offer.offer_id || 'offer'}. Final step: run action route.`;
    setFlowStep('score');
    return;
  }
  decisionStatusEl.className = 'state-message loading';
  decisionStatusEl.textContent = `Selected ${offer.offer_id || 'offer'}. Step 2: score it. Step 3: execute route action.`;
  setFlowStep('choose');
}

const dispatchMapState = {
  map: null,
  zoneLayer: null,
  routeLayer: null,
  originLayer: null,
  clusterEnabled: false,
  canvasId: 'dispatchLeafletCanvas',
};

const replayMapState = {
  map: null,
  pathLayer: null,
  eventLayer: null,
  cursorLayer: null,
  cursorMarker: null,
  canvasId: 'replayLeafletCanvas',
  initialized: false,
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
    mission.reportError = errorMessage(err, 'mission report failed');
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
      const safeZone = escapeHtml(mission.zone_id || 'zone');
      const safeStrategy = escapeHtml(String(mission.dispatch_strategy || 'balanced').toUpperCase());
      const safeCompleted = escapeHtml(completed);
      const safeStopReason = escapeHtml(mission.stop_reason || '-');
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
            <strong>#${idx + 1} ${safeZone} - ${safeStrategy}</strong>
            <span class="badge ${success ? 'accept' : 'reject'}">${success ? 'SUCCESS' : 'MISSED'}</span>
          </div>
          <div class="offer-meta">
            <span class="muted">${safeCompleted}</span>
            <span>${elapsed}</span>
          </div>
          <div class="offer-meta">
            <span>pred ${predictedDelta}</span>
            <span>real ${realizedDelta}</span>
          </div>
          <div class="muted">reason: ${safeStopReason}</div>
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
    state.missionJournalError = errorMessage(err, 'mission journal unavailable');
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
  const dq = health.data_quality || {};
  const rq = health.routing_quality || {};
  const thr = health.quality_alert_thresholds || {};

  const items = [];
  if (Number.isFinite(Number(metrics.roc_auc))) items.push({ txt: `AUC ${fmt(metrics.roc_auc, 3)}`, cls: 'chip' });
  if (Number.isFinite(Number(metrics.average_precision))) items.push({ txt: `AP ${fmt(metrics.average_precision, 3)}`, cls: 'chip' });
  if (Number.isFinite(Number(metrics.brier_score))) items.push({ txt: `Brier ${fmt(metrics.brier_score, 3)}`, cls: 'chip' });
  if (Number.isFinite(Number(gate.trained_rows))) items.push({ txt: `rows ${gate.trained_rows}`, cls: 'chip' });
  if (Number.isFinite(Number(fuel.fuel_price_eur_l))) items.push({ txt: `fuel ${fmt(fuel.fuel_price_eur_l, 3)} EUR/L`, cls: 'chip' });
  if (fuel.source) items.push({ txt: `fuel src ${fuel.source}`, cls: 'chip' });
  if (fuel.fuel_sync_status) items.push({ txt: `fuel sync ${fuel.fuel_sync_status}`, cls: 'chip' });

  // Data quality chips
  if (dq.supply_flat_alert) {
    items.push({ txt: 'supply FLAT', cls: 'chip bad' });
  } else if (Number.isFinite(Number(dq.supply_variance))) {
    const ok = dq.supply_variance >= (thr.supply_variance_min ?? 0.002);
    items.push({ txt: `supply var ${fmt(dq.supply_variance, 4)}`, cls: ok ? 'chip good' : 'chip warn' });
  }
  if (Number.isFinite(Number(dq.traffic_nonzero_rate))) {
    const ok = dq.traffic_nonzero_rate >= (thr.traffic_nonzero_rate_min ?? 0.30);
    items.push({ txt: `traffic nz ${fmt(dq.traffic_nonzero_rate, 2)}`, cls: ok ? 'chip good' : 'chip warn' });
  }
  if (dq.context_fallback_applied) items.push({ txt: `fallback (${dq.stale_sources_count ?? 0} stale)`, cls: 'chip warn' });

  // Routing quality chips
  if (rq.routing_degraded) {
    items.push({ txt: 'routing DEGRADED', cls: 'chip bad' });
  } else if (Number.isFinite(Number(rq.routing_success_rate))) {
    const ok = rq.routing_success_rate >= (thr.routing_success_rate_min ?? 0.80);
    items.push({ txt: `routing ok ${fmt(rq.routing_success_rate * 100, 1)}%`, cls: ok ? 'chip good' : 'chip warn' });
  }
  if (Number.isFinite(Number(rq.hold_rate))) {
    const bad = rq.hold_rate > (thr.hold_rate_max ?? 0.30);
    items.push({ txt: `hold ${fmt(rq.hold_rate * 100, 1)}%`, cls: bad ? 'chip warn' : 'chip good' });
  }

  healthMetrics.innerHTML = '';
  items.forEach(({ txt, cls }) => {
    const node = document.createElement('span');
    node.className = cls;
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
    renderListState(offersEl, 'empty', 'No offer matches the current filter.');
    return;
  }

  rows.forEach((offer, idx) => {
    const card = document.createElement('article');
    const key = offerKey(offer, 'offers', idx);
    const isSelected = key === state.selectedOfferKey;
    card.className = `offer selectable${isSelected ? ' selected' : ''}`;
    card.setAttribute('data-offer-key', key);
    card.setAttribute('tabindex', '0');
    card.setAttribute('role', 'button');
    card.setAttribute('aria-pressed', isSelected ? 'true' : 'false');
    card.setAttribute('aria-label', `Select offer ${String(offer.offer_id || idx + 1)}`);

    const reasons = Array.isArray(offer.explanation) ? offer.explanation : [];
    const reasonCodes = Array.isArray(offer.reason_codes) ? offer.reason_codes : [];
    const detailChips = explanationDetailChips(offer.explanation_details, 2);
    const breakdown = scoreBreakdownHtml(offer.score_breakdown, 4);
    const chips = `${listToChips([...reasons, ...reasonCodes], 8)}${detailChips}`;
    const zone = offer.zone_id || 'unknown_zone';

    card.innerHTML = `
      <div class="offer-top">
        <strong>${escapeHtml(offer.offer_id || 'manual-offer')}</strong>
        <span class="badge ${asDecisionClass(offer.decision)}">${escapeHtml((offer.decision || 'n/a').toUpperCase())}</span>
      </div>
      <div class="offer-meta">
        <span class="muted">zone ${escapeHtml(zone)}</span>
        <span><strong>${fmt(offer.accept_score, 3)}</strong> score - <strong>${fmt(offer.objective_score, 3)}</strong> objective - ${escapeHtml(String(offer.model_used || 'heuristic'))}</span>
      </div>
      ${offerMetricsHtml(offer)}
      <div class="muted">${escapeHtml(routeSummary(offer))}</div>
      ${breakdown}
      <div class="chips">${chips}</div>
      <div class="offer-actions">
        <button class="ghost" data-action="pick-offer" data-offer-key="${key}">Choose</button>
        <button data-action="pick-score-offer" data-offer-key="${key}">Pick + Score</button>
        <button class="ghost" data-action="score-offer" data-offer-id="${escapeHtml(offer.offer_id || '')}" data-courier-id="${escapeHtml(offer.courier_id || '')}">Score</button>
      </div>
    `;

    offersEl.appendChild(card);
  });
}

function renderBestOffers() {
  bestOffersEl.innerHTML = '';

  if (state.bestOffersError) {
    renderListState(bestOffersEl, 'error', `Best offers unavailable: ${state.bestOffersError}`);
    return;
  }

  if (!state.bestOffers.length) {
    renderListState(bestOffersEl, 'empty', 'No profitable nearby offers for current filters.');
    return;
  }

  state.bestOffers.forEach((offer, idx) => {
    const card = document.createElement('article');
    const key = offerKey(offer, 'best', idx);
    const isSelected = key === state.selectedOfferKey;
    card.className = `offer selectable${isSelected ? ' selected' : ''}`;
    card.setAttribute('data-offer-key', key);
    card.setAttribute('tabindex', '0');
    card.setAttribute('role', 'button');
    card.setAttribute('aria-pressed', isSelected ? 'true' : 'false');
    card.setAttribute('aria-label', `Select best offer ${String(offer.offer_id || idx + 1)}`);

    const reasons = Array.isArray(offer.explanation) ? offer.explanation : [];
    const reasonCodes = Array.isArray(offer.reason_codes) ? offer.reason_codes : [];
    const signals = Array.isArray(offer.recommendation_signals) ? offer.recommendation_signals : [];
    const routeNotes = Array.isArray(offer.route_notes) ? offer.route_notes : [];
    const details = explanationDetailChips(offer.explanation_details, 2);
    const breakdown = scoreBreakdownHtml(offer.score_breakdown, 4);

    const chips = `${listToChips([...reasons, ...reasonCodes, ...signals, ...routeNotes], 10)}${details}`;

    card.innerHTML = `
      <div class="offer-top">
        <strong>#${idx + 1} ${escapeHtml(offer.offer_id || 'offer')}</strong>
        <span class="badge ${asRecommendationClass(offer.recommendation_action)}">${escapeHtml(String(offer.recommendation_action || 'skip').toUpperCase())}</span>
      </div>
      <div class="offer-meta">
        <span class="muted">pickup ${fmt(offer.distance_to_pickup_km, 2)} km</span>
        <span><strong>${fmt(offer.recommendation_score, 3)}</strong> rec - <strong>${fmt(offer.objective_score, 3)}</strong> objective - <strong>${fmt(offer.accept_score, 3)}</strong> ${escapeHtml(String(offer.model_used || 'heuristic'))}</span>
      </div>
      ${offerMetricsHtml(offer)}
      <div class="offer-meta">
        <span>${escapeHtml(routeSummary(offer))}</span>
        <span><strong>${fmt(offer.target_gap_eur_h, 1)}</strong> gap EUR/h</span>
      </div>
      ${breakdown}
      <div class="chips">${chips}</div>
      <div class="offer-actions">
        <button class="ghost" data-action="pick-offer" data-offer-key="${key}">Choose</button>
        <button data-action="pick-score-offer" data-offer-key="${key}">Pick + Score</button>
        <button class="ghost" data-action="score-offer" data-offer-id="${escapeHtml(offer.offer_id || '')}" data-courier-id="${escapeHtml(offer.courier_id || '')}">Score</button>
      </div>
    `;

    bestOffersEl.appendChild(card);
  });
}

function renderDispatch() {
  dispatchPlanEl.innerHTML = '';

  if (state.dispatchError) {
    dispatchSummaryEl.textContent = `Dispatch unavailable: ${state.dispatchError}`;
    renderListState(dispatchPlanEl, 'error', 'Instant dispatch could not be computed right now.');
    renderDispatchMap(null);
    return;
  }

  const plan = state.dispatch;
  if (!plan) {
    dispatchSummaryEl.textContent = 'No dispatch recommendation yet.';
    renderListState(dispatchPlanEl, 'empty', 'Refresh driver data to compute dispatch recommendation.');
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
  const decisionLabel = escapeHtml(decision.toUpperCase());
  const strategyLabel = escapeHtml(strategy);
  top.innerHTML = `
    <div class="offer-top">
      <strong>Target ${fmt(plan.target_hourly_net_eur, 1)} EUR/h</strong>
      <span class="badge ${asDispatchClass(decision)}">${decisionLabel}</span>
    </div>
    <div class="muted">Strategy ${strategyLabel} - Local candidates ${Number(plan.local_candidates_count || 0)} - Reposition candidates ${Number(plan.reposition_candidates_count || 0)} - max ETA ${fmt(plan.max_reposition_eta_min, 1)} min (effective ${fmt(plan.effective_max_reposition_eta_min, 1)} min) - forecast ${fmt(plan.forecast_horizon_min, 0)} min (effective ${fmt(plan.effective_forecast_horizon_min, 0)} min)</div>
  `;
  dispatchPlanEl.appendChild(top);

  const stay = plan.stay_option;
  if (stay) {
    const card = document.createElement('article');
    card.className = 'offer';
    const signals = Array.isArray(stay.signals) ? stay.signals : [];
    const reasonsList = Array.isArray(stay.reasons) ? stay.reasons : [];
    const reasonCodes = Array.isArray(stay.reason_codes) ? stay.reason_codes : [];
    const breakdown = scoreBreakdownHtml(stay.score_breakdown, 4);
    const chips = listToChips([...signals, ...reasonsList, ...reasonCodes], 8);

    card.innerHTML = `
      <div class="offer-top">
        <strong>Stay candidate - ${escapeHtml(stay.offer_id || 'offer')}</strong>
        <span class="badge ${asRecommendationClass(stay.recommendation_action)}">${escapeHtml(String(stay.recommendation_action || 'consider').toUpperCase())}</span>
      </div>
      <div class="offer-meta">
        <span class="muted">zone ${escapeHtml(stay.zone_id || '-')}</span>
        <span><strong>${fmt(stay.recommendation_score, 3)}</strong> rec - <strong>${fmt(stay.eur_per_hour_net, 1)}</strong> EUR/h</span>
      </div>
      ${offerMetricsHtml(stay)}
      <div class="offer-meta">
        <span>pickup ${fmt(stay.distance_to_pickup_km, 2)} km</span>
        <span>route ${escapeHtml(stay.route_source || 'estimated')} - ${fmt(stay.route_duration_min, 1)} min</span>
      </div>
      ${breakdown}
      <div class="chips">${chips}</div>
      <div class="offer-actions">
        <button class="ghost" data-action="score-offer" data-offer-id="${escapeHtml(stay.offer_id || '')}" data-courier-id="${escapeHtml(currentDriverId())}">Score</button>
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
        <strong>Reposition candidate - ${escapeHtml(move.zone_id || 'zone')}</strong>
        <span>${fmt(move.dispatch_score, 3)}</span>
      </div>
      <div class="muted">ETA ${fmt(move.eta_min, 1)} min - ${fmt(move.route_distance_km, 1)} km - route ${escapeHtml(move.route_source || 'estimated')}</div>
      <div class="offer-metrics">
        <div class="offer-metric"><div class="k">Net EUR/h</div><div class="v">${fmt(move.risk_adjusted_potential_eur_h, 1)}</div></div>
        <div class="offer-metric"><div class="k">Gross EUR/h</div><div class="v">${fmt(move.estimated_potential_eur_h, 1)}</div></div>
        <div class="offer-metric"><div class="k">ETA min</div><div class="v">${fmt(move.eta_min, 1)}</div></div>
        <div class="offer-metric"><div class="k">Reposition Cost</div><div class="v">${fmt(move.reposition_total_cost_eur, 2)}</div></div>
      </div>
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
          data-zone-id="${escapeHtml(move.zone_id || 'zone')}"
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
    renderListState(zonesEl, 'empty', 'No zone recommendations yet.');
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
        <strong>#${idx + 1} ${escapeHtml(zone.zone_id || 'unknown_zone')}</strong>
        <span>${fmt(adjustedScore, 2)}</span>
      </div>
      <div class="offer-metrics">
        <div class="offer-metric"><div class="k">Opportunity</div><div class="v">${fmt(adjustedScore, 2)}</div></div>
        <div class="offer-metric"><div class="k">ETA min</div><div class="v">${fmt(repoTime, 1)}</div></div>
        <div class="offer-metric"><div class="k">Fuel EUR</div><div class="v">${fmt(fuelCost, 2)}</div></div>
        <div class="offer-metric"><div class="k">Distance km</div><div class="v">${fmt(distance, 2)}</div></div>
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

function renderShiftPlan() {
  if (!shiftPlanEl || !shiftPlanSummaryEl) return;
  shiftPlanEl.innerHTML = '';

  if (state.shiftPlanError) {
    shiftPlanSummaryEl.textContent = `Shift plan unavailable: ${state.shiftPlanError}`;
    renderListState(shiftPlanEl, 'error', 'Unable to compute shift plan right now.');
    return;
  }

  const plan = state.shiftPlan;
  const items = Array.isArray(plan?.items) ? plan.items : [];
  if (!items.length) {
    shiftPlanSummaryEl.textContent = 'No shift plan yet.';
    renderListState(shiftPlanEl, 'empty', 'Refresh to compute 60/90 minute hotspots.');
    return;
  }

  const horizon = Number(plan?.horizon_min || shiftPlanHorizonInput?.value || 60);
  const target = Number(plan?.target_hourly_net_eur || 0);
  shiftPlanSummaryEl.textContent = `Top ${items.length} zones for next ${horizon} min - target ${fmt(target, 1)} EUR/h`;

  const maxScore = Math.max(...items.map((item) => Number(item.shift_score || 0)), 0.001);
  const originLat = asFloatOrNull(plan?.origin_lat ?? state.dispatch?.origin?.lat);
  const originLon = asFloatOrNull(plan?.origin_lon ?? state.dispatch?.origin?.lon);

  items.forEach((item, idx) => {
    const zoneLat = asFloatOrNull(item.zone_lat);
    const zoneLon = asFloatOrNull(item.zone_lon);
    const score = Number(item.shift_score || 0);
    const confidence = Number(item.confidence || 0);
    const meter = Math.max(6, Math.min(100, (score / maxScore) * 100));
    const reasons = Array.isArray(item.reasons) ? item.reasons.slice(0, 4) : [];
    const whyNow = escapeHtml(String(item.why_now || reasons[0] || 'Balanced opportunity now.'));
    const reasonChips = reasons.map((reason) => `<span>${escapeHtml(reason)}</span>`).join('');
    const confidencePct = `${fmt(confidence * 100, 0)}%`;

    const goButton =
      originLat != null && originLon != null && zoneLat != null && zoneLon != null
        ? `
          <button
            class="ghost"
            data-action="go-zone"
            data-origin-lat="${originLat}"
            data-origin-lon="${originLon}"
            data-dest-lat="${zoneLat}"
            data-dest-lon="${zoneLon}"
          >Go Zone</button>
        `
        : '';

    const startMissionButton =
      zoneLat != null && zoneLon != null
        ? `
          <button
            class="ghost"
            data-action="start-mission"
            data-zone-id="${escapeHtml(item.zone_id || 'zone')}"
            data-dest-lat="${zoneLat}"
            data-dest-lon="${zoneLon}"
            data-score="${fmt(score, 3)}"
            data-potential="${fmt(item.estimated_gross_eur_h, 3)}"
            data-eta="${fmt(item.eta_min, 3)}"
            data-dispatch-strategy="${String(state.dispatch?.dispatch_strategy || 'balanced').toLowerCase()}"
            data-effective-max-eta="${asFloatOrNull(state.dispatch?.effective_max_reposition_eta_min) == null ? '' : asFloatOrNull(state.dispatch?.effective_max_reposition_eta_min)}"
          >Start Mission</button>
        `
        : '';

    const row = document.createElement('article');
    row.className = 'zone';
    row.innerHTML = `
      <div class="zone-top">
        <strong>#${Number(item.rank || idx + 1)} ${escapeHtml(item.zone_id || 'unknown_zone')}</strong>
        <span>${fmt(score, 3)}</span>
      </div>
      <div class="offer-meta">
        <span class="muted">confidence ${confidencePct}</span>
        <span>${fmt(item.distance_km, 2)} km - ${fmt(item.eta_min, 1)} min</span>
      </div>
      <div class="offer-metrics">
        <div class="offer-metric"><div class="k">Net EUR/h</div><div class="v">${fmt(item.estimated_net_eur_h, 1)}</div></div>
        <div class="offer-metric"><div class="k">Gross EUR/h</div><div class="v">${fmt(item.estimated_gross_eur_h, 1)}</div></div>
        <div class="offer-metric"><div class="k">Gain vs Target</div><div class="v">${fmtSigned(item.net_gain_vs_target_eur_h, 1)}</div></div>
        <div class="offer-metric"><div class="k">Reposition EUR</div><div class="v">${fmt(item.reposition_total_cost_eur, 2)}</div></div>
      </div>
      <div class="muted"><strong>Why now:</strong> ${whyNow}</div>
      <div class="muted">demand ${fmt(item.demand_index, 2)} / supply ${fmt(item.supply_index, 2)} - event ${fmt(item.event_pressure, 2)} - temporal ${fmt(item.temporal_pressure, 2)} - forecast pressure ${fmt(item.forecast_pressure_ratio, 2)}</div>
      <div class="chips">${reasonChips}</div>
      <div class="zone-meter"><div style="width:${meter}%"></div></div>
      <div class="offer-actions">
        ${goButton}
        ${startMissionButton}
      </div>
    `;
    shiftPlanEl.appendChild(row);
  });
}

function parseReplayTsMs(value) {
  const ms = Date.parse(String(value || ''));
  return Number.isFinite(ms) ? ms : null;
}

function isValidReplayCoord(lat, lon) {
  return lat != null && lon != null && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;
}

function replayEventKind(evt) {
  const status = String(evt?.status || '').toLowerCase();
  const eventType = String(evt?.event_type || '').toLowerCase();
  if (status.includes('reject')) return 'reject';
  if (status.includes('accept')) return 'accept';
  if (status.includes('drop')) return 'dropoff';
  if (status.includes('offer') || eventType.includes('offer')) return 'offer';
  return 'event';
}

function nearestReplayPositionIndex(positions, tsMs) {
  if (!Array.isArray(positions) || !positions.length) return -1;
  if (!Number.isFinite(Number(tsMs))) return positions.length - 1;
  const target = Number(tsMs);
  let bestIdx = 0;
  let bestDelta = Math.abs(Number(positions[0].tsMs || 0) - target);
  for (let i = 1; i < positions.length; i += 1) {
    const delta = Math.abs(Number(positions[i].tsMs || 0) - target);
    if (delta < bestDelta) {
      bestDelta = delta;
      bestIdx = i;
    }
  }
  return bestIdx;
}

function buildReplayVisual(events) {
  const normalized = (Array.isArray(events) ? events : [])
    .map((evt, idx) => {
      const tsMs = parseReplayTsMs(evt?.ts);
      const eventType = String(evt?.event_type || '');
      const topic = String(evt?.topic || '');
      const lat = asFloatOrNull(evt?.lat);
      const lon = asFloatOrNull(evt?.lon);
      const hasCoord = isValidReplayCoord(lat, lon);
      const isPosition =
        eventType.toLowerCase() === 'courier.position.v1' ||
        topic.toLowerCase() === 'livreurs-gps' ||
        (hasCoord && !evt?.offer_id && !evt?.order_id);
      return {
        raw: evt || {},
        idx,
        tsMs,
        eventType,
        topic,
        lat,
        lon,
        hasCoord,
        isPosition,
        kind: replayEventKind(evt),
      };
    })
    .sort((a, b) => {
      const ta = a.tsMs == null ? Number.MAX_SAFE_INTEGER : a.tsMs;
      const tb = b.tsMs == null ? Number.MAX_SAFE_INTEGER : b.tsMs;
      if (ta !== tb) return ta - tb;
      return a.idx - b.idx;
    });

  const positions = normalized
    .filter((evt) => evt.isPosition && evt.hasCoord)
    .map((evt, idx) => ({
      idx,
      ts: String(evt.raw?.ts || ''),
      tsMs: evt.tsMs,
      lat: evt.lat,
      lon: evt.lon,
      status: String(evt.raw?.status || 'n/a'),
      speedKmh: asFloatOrNull(evt.raw?.speed_kmh),
      headingDeg: asFloatOrNull(evt.raw?.heading_deg),
      eventType: evt.eventType,
      topic: evt.topic,
    }));

  const keyEvents = normalized
    .filter((evt) => !evt.isPosition)
    .map((evt) => {
      let lat = evt.hasCoord ? evt.lat : null;
      let lon = evt.hasCoord ? evt.lon : null;
      const nearestPositionIndex = nearestReplayPositionIndex(positions, evt.tsMs);
      if (!isValidReplayCoord(lat, lon) && nearestPositionIndex >= 0) {
        lat = positions[nearestPositionIndex].lat;
        lon = positions[nearestPositionIndex].lon;
      }
      return {
        kind: evt.kind,
        ts: String(evt.raw?.ts || ''),
        tsMs: evt.tsMs,
        eventType: evt.eventType,
        status: String(evt.raw?.status || 'n/a'),
        offerId: String(evt.raw?.offer_id || ''),
        orderId: String(evt.raw?.order_id || ''),
        zoneId: String(evt.raw?.zone_id || ''),
        topic: evt.topic,
        lat,
        lon,
        positionIndex: nearestPositionIndex,
      };
    });

  return { events: normalized, positions, keyEvents };
}

function clearReplayLeafletLayers() {
  if (replayMapState.pathLayer) replayMapState.pathLayer.clearLayers();
  if (replayMapState.eventLayer) replayMapState.eventLayer.clearLayers();
  if (replayMapState.cursorLayer) replayMapState.cursorLayer.clearLayers();
  replayMapState.cursorMarker = null;
}

function ensureReplayLeafletMap() {
  if (!replayMapEl || !canUseLeaflet()) return null;
  if (replayMapState.map) return replayMapState.map;

  replayMapEl.classList.add('map-ready');
  replayMapEl.innerHTML = `<div id="${replayMapState.canvasId}" class="dispatch-leaflet replay-leaflet"></div>`;

  const map = window.L.map(replayMapState.canvasId, {
    zoomControl: true,
    attributionControl: true,
  });
  window.L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; OpenStreetMap contributors',
  }).addTo(map);

  replayMapState.map = map;
  replayMapState.pathLayer = window.L.layerGroup().addTo(map);
  replayMapState.eventLayer = window.L.layerGroup().addTo(map);
  replayMapState.cursorLayer = window.L.layerGroup().addTo(map);
  replayMapState.initialized = true;
  map.setView([DISPATCH_MAP_DEFAULT.lat, DISPATCH_MAP_DEFAULT.lon], DISPATCH_MAP_DEFAULT.zoom);
  setTimeout(() => map.invalidateSize(), 0);
  return map;
}

function renderReplayMap(model) {
  if (!replayMapEl) return;

  if (!model.positions.length) {
    if (replayMapState.map) {
      clearReplayLeafletLayers();
      replayMapState.map.setView([DISPATCH_MAP_DEFAULT.lat, DISPATCH_MAP_DEFAULT.lon], DISPATCH_MAP_DEFAULT.zoom);
    } else {
      replayMapEl.classList.remove('map-ready');
      replayMapEl.textContent = 'Replay map unavailable: no courier positions in this window.';
    }
    return;
  }

  if (!canUseLeaflet()) {
    replayMapEl.classList.remove('map-ready');
    replayMapEl.textContent = `Replay fallback: ${model.positions.length} position samples loaded, map engine unavailable.`;
    return;
  }

  const map = ensureReplayLeafletMap();
  if (!map) {
    replayMapEl.classList.remove('map-ready');
    replayMapEl.textContent = `Replay fallback: ${model.positions.length} position samples loaded, map initialization failed.`;
    return;
  }

  replayMapEl.classList.add('map-ready');
  clearReplayLeafletLayers();

  const latLngs = model.positions.map((p) => [p.lat, p.lon]);
  const route = window.L.polyline(latLngs, {
    color: '#0b7285',
    weight: 4,
    opacity: 0.8,
  });
  replayMapState.pathLayer.addLayer(route);

  const start = model.positions[0];
  const end = model.positions[model.positions.length - 1];
  replayMapState.eventLayer.addLayer(
    window.L.circleMarker([start.lat, start.lon], {
      radius: 5,
      color: '#15803d',
      weight: 2,
      fillColor: '#22c55e',
      fillOpacity: 0.9,
    }).bindPopup('start position')
  );
  replayMapState.eventLayer.addLayer(
    window.L.circleMarker([end.lat, end.lon], {
      radius: 5,
      color: '#b45309',
      weight: 2,
      fillColor: '#f59e0b',
      fillOpacity: 0.9,
    }).bindPopup('latest position')
  );

  const kindColor = {
    accept: '#15803d',
    reject: '#b42318',
    dropoff: '#b45309',
    offer: '#2563eb',
    event: '#475569',
  };
  model.keyEvents.slice(-60).forEach((evt) => {
    if (!isValidReplayCoord(evt.lat, evt.lon)) return;
    const color = kindColor[evt.kind] || kindColor.event;
    const marker = window.L.circleMarker([evt.lat, evt.lon], {
      radius: 4,
      color,
      weight: 2,
      fillColor: color,
      fillOpacity: 0.65,
    });
    const label = `${evt.eventType || 'event'} - ${evt.status || 'n/a'}`;
    marker.bindPopup(escapeHtml(label));
    replayMapState.eventLayer.addLayer(marker);
  });

  if (latLngs.length > 1) {
    map.fitBounds(window.L.latLngBounds(latLngs).pad(0.16), { maxZoom: 15 });
  } else {
    map.setView(latLngs[0], 14);
  }
  map.invalidateSize();
}

function setReplayScrubberState(model) {
  if (!replayScrubberEl) return;
  const maxIdx = Math.max(0, model.positions.length - 1);
  const currentIndex = Number.isFinite(Number(state.replayCursorIndex)) ? Number(state.replayCursorIndex) : 0;
  replayScrubberEl.min = '0';
  replayScrubberEl.max = String(maxIdx);
  replayScrubberEl.step = '1';
  if (!model.positions.length) {
    replayScrubberEl.value = '0';
    replayScrubberEl.disabled = true;
    return;
  }
  replayScrubberEl.disabled = false;
  replayScrubberEl.value = String(clamp(currentIndex, 0, maxIdx));
}

function setReplayCursor(index) {
  const model = state.replayVisual;
  if (!model || !Array.isArray(model.positions) || !model.positions.length) {
    state.replayCursorIndex = 0;
    if (replayCursorLabelEl) replayCursorLabelEl.textContent = 't-';
    if (replayCursorStatusEl) replayCursorStatusEl.textContent = 'No position samples available in selected replay window.';
    if (replayScrubberEl) replayScrubberEl.value = '0';
    return;
  }

  const maxIdx = model.positions.length - 1;
  const numericIndex = Number(index);
  const safeIndex = clamp(Number.isFinite(numericIndex) ? numericIndex : maxIdx, 0, maxIdx);
  state.replayCursorIndex = safeIndex;
  if (replayScrubberEl) replayScrubberEl.value = String(safeIndex);

  const point = model.positions[safeIndex];
  const tsLabel = point.ts ? new Date(point.ts).toLocaleTimeString() : `sample #${safeIndex + 1}`;
  if (replayCursorLabelEl) {
    replayCursorLabelEl.textContent = `t ${safeIndex + 1}/${model.positions.length} - ${tsLabel}`;
  }

  const alignedEvents = model.keyEvents.filter((evt) => evt.positionIndex === safeIndex).slice(0, 2);
  const alignedSummary = alignedEvents.length
    ? alignedEvents.map((evt) => `${evt.kind.toUpperCase()} ${evt.status}`).join(' | ')
    : 'no key event on this step';
  const speedText = point.speedKmh == null ? '-' : `${fmt(point.speedKmh, 1)} km/h`;
  const headingText = point.headingDeg == null ? '-' : `${fmt(point.headingDeg, 0)} deg`;
  if (replayCursorStatusEl) {
    replayCursorStatusEl.textContent =
      `lat ${fmt(point.lat, 5)} lon ${fmt(point.lon, 5)} - speed ${speedText} - heading ${headingText} - ${alignedSummary}`;
  }

  if (replayMapState.map && replayMapState.cursorLayer) {
    if (!replayMapState.cursorMarker) {
      replayMapState.cursorMarker = window.L.circleMarker([point.lat, point.lon], {
        radius: 7,
        color: '#0f172a',
        weight: 2,
        fillColor: '#f8fafc',
        fillOpacity: 1.0,
      });
      replayMapState.cursorLayer.addLayer(replayMapState.cursorMarker);
    } else {
      replayMapState.cursorMarker.setLatLng([point.lat, point.lon]);
    }
    const mapBounds = typeof replayMapState.map.getBounds === 'function' ? replayMapState.map.getBounds() : null;
    if (mapBounds && typeof mapBounds.contains === 'function' && !mapBounds.contains([point.lat, point.lon])) {
      replayMapState.map.panTo([point.lat, point.lon], { animate: false });
    }
  }

  if (replayTimeline && replayTimeline.children) {
    Array.from(replayTimeline.children).forEach((row) => {
      const rowIndex = Number(row.getAttribute('data-replay-pos-index'));
      row.classList.toggle('active', Number.isFinite(rowIndex) && rowIndex === safeIndex);
    });
  }
}

function renderReplayTimeline(model) {
  replayTimeline.innerHTML = '';
  const rows = model.keyEvents.length ? model.keyEvents : model.events.filter((evt) => !evt.isPosition);

  if (!rows.length) {
    renderListState(replayTimeline, 'empty', 'Replay contains only position samples in this window.');
    return;
  }

  rows.slice(-32).reverse().forEach((evt) => {
    const row = document.createElement('div');
    row.className = 'event-row';
    if (evt.positionIndex >= 0) {
      row.setAttribute('data-replay-pos-index', String(evt.positionIndex));
      row.setAttribute('tabindex', '0');
      row.setAttribute('role', 'button');
      row.addEventListener('click', () => setReplayCursor(evt.positionIndex));
      row.addEventListener('keydown', (event) => {
        const key = String(event?.key || '');
        if (key !== 'Enter' && key !== ' ') return;
        event.preventDefault();
        setReplayCursor(evt.positionIndex);
      });
    }

    const eventType = escapeHtml(evt.eventType || 'event');
    const status = escapeHtml(evt.status || 'n/a');
    const offerId = escapeHtml(evt.offerId || '-');
    const orderId = escapeHtml(evt.orderId || '-');
    const zoneId = escapeHtml(evt.zoneId || '-');
    const ts = escapeHtml(evt.ts || '-');
    const topic = escapeHtml(evt.topic || '-');
    const scrub = evt.positionIndex >= 0 ? `t+${evt.positionIndex + 1}` : 'n/a';
    row.innerHTML = `
      <strong>${eventType} - ${status}</strong>
      <div>offer ${offerId} - order ${orderId} - zone ${zoneId}</div>
      <div class="muted">${ts} - topic ${topic} - scrub ${scrub}</div>
    `;
    replayTimeline.appendChild(row);
  });
}

function renderReplay() {
  const events = Array.isArray(state.replay) ? state.replay : [];
  kpiReplay.textContent = String(events.length);

  if (!events.length) {
    state.replayVisual = null;
    state.replayCursorIndex = 0;
    if (replayMapEl) {
      replayMapEl.classList.remove('map-ready');
      replayMapEl.textContent = 'Replay map pending...';
    }
    if (replaySummary) replaySummary.textContent = 'No replay events in selected window.';
    setReplayScrubberState({ positions: [] });
    setReplayCursor(0);
    renderListState(replayTimeline, 'empty', 'Try a wider window (Last 2h or Today).');
    return;
  }

  const visual = buildReplayVisual(events);
  state.replayVisual = visual;
  state.replayCursorIndex = Math.max(0, visual.positions.length - 1);

  const byType = {};
  visual.events.forEach((evt) => {
    const key = evt.eventType || 'unknown';
    byType[key] = (byType[key] || 0) + 1;
  });
  const summary = Object.entries(byType)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 4)
    .map(([name, count]) => `${name}: ${count}`)
    .join(' - ');
  const firstTs = visual.events[0]?.raw?.ts || '-';
  const lastTs = visual.events[visual.events.length - 1]?.raw?.ts || '-';
  replaySummary.textContent =
    `${events.length} events (${visual.positions.length} positions, ${visual.keyEvents.length} key events) - ${summary} - window ${firstTs} -> ${lastTs}`;

  renderReplayMap(visual);
  setReplayScrubberState(visual);
  renderReplayTimeline(visual);
  setReplayCursor(state.replayCursorIndex);
}

function buildDriverOffersQuery(driver, limitHint) {
  const limit = Math.max(20, Number(limitHint || offerLimit?.value || 20));
  const objective = objectiveWeightsQueryString(state.objectiveWeights || objectiveWeightsFromInputs());
  return `/copilot/driver/${encodeURIComponent(driver)}/offers?limit=${encodeURIComponent(limit)}&sort_by=objective&${objective}`;
}

function buildAroundQuery(driver) {
  const radius = Math.max(0.2, Number(aroundRadiusInput.value || 4.0));
  const limit = Math.max(1, Math.min(50, Number(aroundLimitInput.value || 10)));
  const minAccept = Math.max(0.0, Math.min(1.0, Number(aroundMinScoreInput.value || 0.25)));
  const useOsrm = aroundUseOsrmInput.checked ? 'true' : 'false';
  const objective = objectiveWeightsQueryString(state.objectiveWeights || objectiveWeightsFromInputs());

  return `/copilot/driver/${encodeURIComponent(driver)}/best-offers-around?radius_km=${encodeURIComponent(radius)}&limit=${encodeURIComponent(limit)}&min_accept_score=${encodeURIComponent(minAccept)}&rank_by=objective&${objective}&use_osrm=${useOsrm}`;
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

function buildShiftPlanQuery(driver) {
  const rawHorizon = Number(shiftPlanHorizonInput?.value || 60);
  const horizon = rawHorizon === 90 ? 90 : 60;
  const rawTopK = Number(shiftPlanTopKInput?.value || 6);
  const topK = Math.max(3, Math.min(20, rawTopK || 6));
  const rawWeight = Number(zoneDistanceWeightInput?.value);
  const weight = Number.isFinite(rawWeight) ? Math.max(0, Math.min(1, rawWeight)) : 0.3;
  const rawMax = Number(zoneMaxDistanceKmInput?.value);
  const useOsrm = aroundUseOsrmInput.checked ? 'true' : 'false';

  let query = `/copilot/driver/${encodeURIComponent(driver)}/shift-plan?horizon_min=${encodeURIComponent(horizon)}&top_k=${encodeURIComponent(topK)}&distance_weight=${encodeURIComponent(weight)}&use_osrm=${useOsrm}`;
  if (Number.isFinite(rawMax) && rawMax > 0) {
    const clamped = Math.max(0.5, Math.min(50, rawMax));
    query += `&max_distance_km=${encodeURIComponent(clamped)}`;
  }
  return query;
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
    const msg = errorMessage(err, 'fuel sync failed');
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

function profileStrategyFromRisk(aversionRisque) {
  const risk = Number(aversionRisque);
  if (!Number.isFinite(risk)) return 'balanced';
  if (risk >= 0.67) return 'conservative';
  if (risk <= 0.33) return 'aggressive';
  return 'balanced';
}

function profileFieldIsFocused() {
  const active = document && 'activeElement' in document ? document.activeElement : null;
  return Boolean(
    active &&
      (active === profileTargetEurHInput ||
        active === profileConsumptionInput ||
        active === profileRiskAversionInput ||
        active === profileMaxEtaInput)
  );
}

function normalizeProfileValue(rawValue, fallback, limits) {
  const fallbackNumber = Number.isFinite(Number(fallback)) ? Number(fallback) : limits.min;
  const parsed = Number(rawValue);
  const candidate = Number.isFinite(parsed) ? parsed : fallbackNumber;
  const bounded = clamp(candidate, limits.min, limits.max);
  return Number(bounded.toFixed(limits.digits));
}

function normalizedProfilePayloadFromInputs() {
  const current = state.driverProfile || {};
  const fallbackTarget = asFloatOrNull(current.target_eur_h) ?? DRIVER_PROFILE_DEFAULTS.target_eur_h;
  const fallbackConsumption = asFloatOrNull(current.consommation_l_100) ?? DRIVER_PROFILE_DEFAULTS.consommation_l_100;
  const fallbackRisk = asFloatOrNull(current.aversion_risque) ?? DRIVER_PROFILE_DEFAULTS.aversion_risque;
  const fallbackMaxEta = asFloatOrNull(current.max_eta) ?? DRIVER_PROFILE_DEFAULTS.max_eta;
  return {
    target_eur_h: normalizeProfileValue(profileTargetEurHInput?.value, fallbackTarget, DRIVER_PROFILE_LIMITS.target_eur_h),
    consommation_l_100: normalizeProfileValue(
      profileConsumptionInput?.value,
      fallbackConsumption,
      DRIVER_PROFILE_LIMITS.consommation_l_100
    ),
    aversion_risque: normalizeProfileValue(
      profileRiskAversionInput?.value,
      fallbackRisk,
      DRIVER_PROFILE_LIMITS.aversion_risque
    ),
    max_eta: normalizeProfileValue(profileMaxEtaInput?.value, fallbackMaxEta, DRIVER_PROFILE_LIMITS.max_eta),
  };
}

function syncProfileInputs(payload) {
  if (!payload || typeof payload !== 'object') return;
  if (profileTargetEurHInput) profileTargetEurHInput.value = String(payload.target_eur_h);
  if (profileConsumptionInput) profileConsumptionInput.value = String(payload.consommation_l_100);
  if (profileRiskAversionInput) profileRiskAversionInput.value = String(payload.aversion_risque);
  if (profileMaxEtaInput) profileMaxEtaInput.value = String(payload.max_eta);
}

function profileUpdatedLabel(updatedAt) {
  const raw = String(updatedAt || '').trim();
  if (!raw) return '';
  const ts = Date.parse(raw);
  if (!Number.isFinite(ts)) return '';
  return new Date(ts).toLocaleString();
}

function renderDriverProfile() {
  const profile = state.driverProfile;
  if (state.driverProfileError) {
    if (profileStatusEl) profileStatusEl.textContent = `Driver profile unavailable: ${state.driverProfileError}`;
    return;
  }
  if (!profile) {
    if (profileStatusEl) profileStatusEl.textContent = 'Driver profile not loaded yet.';
    return;
  }
  const payload = {
    target_eur_h: normalizeProfileValue(
      profile.target_eur_h,
      DRIVER_PROFILE_DEFAULTS.target_eur_h,
      DRIVER_PROFILE_LIMITS.target_eur_h
    ),
    consommation_l_100: normalizeProfileValue(
      profile.consommation_l_100,
      DRIVER_PROFILE_DEFAULTS.consommation_l_100,
      DRIVER_PROFILE_LIMITS.consommation_l_100
    ),
    aversion_risque: normalizeProfileValue(
      profile.aversion_risque,
      DRIVER_PROFILE_DEFAULTS.aversion_risque,
      DRIVER_PROFILE_LIMITS.aversion_risque
    ),
    max_eta: normalizeProfileValue(profile.max_eta, DRIVER_PROFILE_DEFAULTS.max_eta, DRIVER_PROFILE_LIMITS.max_eta),
  };
  if (!profileFieldIsFocused()) {
    syncProfileInputs(payload);
  }
  if (dispatchMaxEtaInput) dispatchMaxEtaInput.value = String(payload.max_eta);
  if (dispatchStrategyInput) dispatchStrategyInput.value = profileStrategyFromRisk(payload.aversion_risque);
  if (profileStatusEl) {
    const source = String(profile.source || 'default');
    const updatedAt = profileUpdatedLabel(profile.updated_at);
    profileStatusEl.textContent = updatedAt
      ? `Driver profile ready (${source}, updated ${updatedAt})`
      : `Driver profile ready (${source})`;
  }
}

async function saveDriverProfile() {
  const driver = currentDriverId();
  const payload = normalizedProfilePayloadFromInputs();
  syncProfileInputs(payload);
  if (profileStatusEl) profileStatusEl.textContent = 'Saving driver profile...';

  try {
    const profile = await api(`/copilot/driver/${encodeURIComponent(driver)}/profile`, {
      method: 'PUT',
      body: JSON.stringify(payload),
    });
    state.driverProfile = profile;
    state.driverProfileError = null;
    renderDriverProfile();
    setUxStatus('success', 'Driver profile saved and applied.');
    await refreshDriverData();
  } catch (err) {
    const msg = errorMessage(err, 'profile update failed');
    state.driverProfileError = msg;
    renderDriverProfile();
    setUxStatus('error', `Profile save failed: ${msg}`);
  }
}

function normalizeObjectiveWeight(rawValue, fallback) {
  const fallbackNumber = Number.isFinite(Number(fallback)) ? Number(fallback) : 0;
  const parsed = Number(rawValue);
  const candidate = Number.isFinite(parsed) ? parsed : fallbackNumber;
  return clamp(candidate, OBJECTIVE_WEIGHT_LIMITS.min, OBJECTIVE_WEIGHT_LIMITS.max);
}

function objectiveWeightsFromInputs() {
  const gainRaw = normalizeObjectiveWeight(objWeightGainInput?.value, OBJECTIVE_WEIGHT_DEFAULTS.w_gain);
  const timeRaw = normalizeObjectiveWeight(objWeightTimeInput?.value, OBJECTIVE_WEIGHT_DEFAULTS.w_time);
  const fuelRaw = normalizeObjectiveWeight(objWeightFuelInput?.value, OBJECTIVE_WEIGHT_DEFAULTS.w_fuel);
  const totalRaw = gainRaw + timeRaw + fuelRaw;
  if (totalRaw <= 0) {
    return {
      w_gain: OBJECTIVE_WEIGHT_DEFAULTS.w_gain / 100,
      w_time: OBJECTIVE_WEIGHT_DEFAULTS.w_time / 100,
      w_fuel: OBJECTIVE_WEIGHT_DEFAULTS.w_fuel / 100,
    };
  }
  return {
    w_gain: gainRaw / totalRaw,
    w_time: timeRaw / totalRaw,
    w_fuel: fuelRaw / totalRaw,
  };
}

function objectiveWeightsToPercents(weights) {
  const normalized = weights || state.objectiveWeights || objectiveWeightsFromInputs();
  return {
    gain: Math.round(clamp(Number(normalized.w_gain || 0), 0, 1) * 100),
    time: Math.round(clamp(Number(normalized.w_time || 0), 0, 1) * 100),
    fuel: Math.round(clamp(Number(normalized.w_fuel || 0), 0, 1) * 100),
  };
}

function normalizeObjectiveWeightsPayload(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const gainRaw = Number(raw.w_gain);
  const timeRaw = Number(raw.w_time);
  const fuelRaw = Number(raw.w_fuel);
  if (!Number.isFinite(gainRaw) || !Number.isFinite(timeRaw) || !Number.isFinite(fuelRaw)) return null;
  const maxValue = Math.max(gainRaw, timeRaw, fuelRaw);
  const scale = maxValue > 1.0 ? 100.0 : 1.0;
  const gain = normalizeObjectiveWeight(gainRaw * (100.0 / scale), OBJECTIVE_WEIGHT_DEFAULTS.w_gain);
  const time = normalizeObjectiveWeight(timeRaw * (100.0 / scale), OBJECTIVE_WEIGHT_DEFAULTS.w_time);
  const fuel = normalizeObjectiveWeight(fuelRaw * (100.0 / scale), OBJECTIVE_WEIGHT_DEFAULTS.w_fuel);
  const total = gain + time + fuel;
  if (total <= 0) return null;
  return {
    w_gain: gain / total,
    w_time: time / total,
    w_fuel: fuel / total,
  };
}

function renderObjectiveWeightControls() {
  if (!state.objectiveWeights) {
    state.objectiveWeights = objectiveWeightsFromInputs();
  }
  const percents = objectiveWeightsToPercents(state.objectiveWeights);
  if (objWeightGainInput) objWeightGainInput.value = String(percents.gain);
  if (objWeightTimeInput) objWeightTimeInput.value = String(percents.time);
  if (objWeightFuelInput) objWeightFuelInput.value = String(percents.fuel);
  if (objWeightGainValueEl) objWeightGainValueEl.textContent = `${percents.gain}%`;
  if (objWeightTimeValueEl) objWeightTimeValueEl.textContent = `${percents.time}%`;
  if (objWeightFuelValueEl) objWeightFuelValueEl.textContent = `${percents.fuel}%`;
  if (objectiveWeightStatusEl) {
    objectiveWeightStatusEl.textContent = `Objective mix: gain ${percents.gain}% / time ${percents.time}% / fuel ${percents.fuel}%.`;
  }
}

function objectiveWeightsQueryString(weights) {
  const percents = objectiveWeightsToPercents(weights || state.objectiveWeights || objectiveWeightsFromInputs());
  return `w_gain=${encodeURIComponent(percents.gain)}&w_time=${encodeURIComponent(percents.time)}&w_fuel=${encodeURIComponent(percents.fuel)}`;
}

function objectiveScoreFromOffer(offer, weights) {
  const normalized = weights || state.objectiveWeights || objectiveWeightsFromInputs();
  const accept = clamp(Number(offer?.accept_score || 0), 0, 1);
  const targetGap = Number(offer?.target_gap_eur_h || 0);
  const targetComponent = clamp((targetGap + 8.0) / 16.0, 0, 1);
  const gainComponent = clamp((accept * 0.72) + (targetComponent * 0.28), 0, 1);

  const duration = Number(offer?.route_duration_min);
  const durationMin = Number.isFinite(duration) ? Math.max(duration, 1) : 20;
  const timeComponent = 1 - clamp((durationMin - 14.0) / 32.0, 0, 1);

  const fare = Number(offer?.costs?.estimated_fare_eur);
  const fuel = Number(offer?.costs?.fuel_cost_eur);
  const fuelShare = Number.isFinite(fare) && fare > 0 && Number.isFinite(fuel) ? fuel / fare : 0.16;
  const fuelComponent = 1 - clamp((fuelShare - 0.06) / 0.25, 0, 1);

  return clamp(
    (normalized.w_gain * gainComponent) + (normalized.w_time * timeComponent) + (normalized.w_fuel * fuelComponent),
    0,
    1
  );
}

function applyObjectiveRankingToCollections() {
  const weights = state.objectiveWeights || objectiveWeightsFromInputs();

  if (Array.isArray(state.offers) && state.offers.length) {
    state.offers = state.offers
      .map((offer) => ({ ...offer, objective_score: objectiveScoreFromOffer(offer, weights) }))
      .sort((a, b) => {
        const aObj = Number(a.objective_score || 0);
        const bObj = Number(b.objective_score || 0);
        if (bObj !== aObj) return bObj - aObj;
        const acceptDelta = Number(b.accept_score || 0) - Number(a.accept_score || 0);
        if (acceptDelta !== 0) return acceptDelta;
        return Number(b.eur_per_hour_net || 0) - Number(a.eur_per_hour_net || 0);
      });
  }

  if (Array.isArray(state.bestOffers) && state.bestOffers.length) {
    state.bestOffers = state.bestOffers
      .map((offer) => ({ ...offer, objective_score: objectiveScoreFromOffer(offer, weights) }))
      .sort((a, b) => {
        const aObj = Number(a.objective_score || 0);
        const bObj = Number(b.objective_score || 0);
        if (bObj !== aObj) return bObj - aObj;
        const recDelta = Number(b.recommendation_score || 0) - Number(a.recommendation_score || 0);
        if (recDelta !== 0) return recDelta;
        return Number(b.accept_score || 0) - Number(a.accept_score || 0);
      });
  }
}

function handleObjectiveWeightChange() {
  state.objectiveWeights = objectiveWeightsFromInputs();
  renderObjectiveWeightControls();
  applyObjectiveRankingToCollections();
  renderKpis();
  ensureSelectedOffer();
  renderOffers();
  renderBestOffers();
  renderDecisionFlow();
  setUxStatus('loading', 'Ranking priorities updated. Recomputing recommendations...');
  refreshDriverDataDebounced();
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
  if (!state.objectiveWeights) {
    state.objectiveWeights = objectiveWeightsFromInputs();
  }
  renderObjectiveWeightControls();
  if (state.missionJournalDriver !== driver) {
    state.missionJournalDriver = driver;
    refreshMissionJournal(true).catch(() => {});
  }

  setUxStatus('loading', `Refreshing offers, score, and action plan for ${driver}...`);
  if (profileStatusEl) profileStatusEl.textContent = 'Loading driver profile...';
  renderListState(offersEl, 'loading', 'Loading offers...');
  renderListState(bestOffersEl, 'loading', 'Loading nearby recommendations...');
  dispatchSummaryEl.textContent = 'Loading instant dispatch recommendation...';
  renderListState(dispatchPlanEl, 'loading', 'Computing instant dispatch recommendation...');
  if (shiftPlanSummaryEl) shiftPlanSummaryEl.textContent = 'Loading shift plan recommendation...';
  if (shiftPlanEl) renderListState(shiftPlanEl, 'loading', 'Computing shift plan hotspots...');
  if (dispatchMapState.map) {
    clearDispatchLeafletLayers();
  } else {
    dispatchMapEl.classList.remove('map-ready');
    dispatchMapEl.textContent = 'Rendering dispatch map...';
  }
  dispatchLegendEl.innerHTML = '';
  renderListState(zonesEl, 'loading', 'Loading zones...');

  try {
    const [profileResp, offersResp, zonesResp, bestResp, dispatchResp, shiftPlanResp] = await Promise.all([
      api(`/copilot/driver/${encodeURIComponent(driver)}/profile`).catch((err) => ({ _error: errorMessage(err, 'profile unavailable') })),
      api(buildDriverOffersQuery(driver, limit)),
      api(buildZoneQuery(driver)),
      api(buildAroundQuery(driver)).catch((err) => ({ offers: [], _error: errorMessage(err, 'best offers unavailable') })),
      api(buildDispatchQuery(driver)).catch((err) => ({ _error: errorMessage(err, 'dispatch unavailable') })),
      api(buildShiftPlanQuery(driver)).catch((err) => ({ _error: errorMessage(err, 'shift plan unavailable') })),
    ]);

    state.driverProfile = profileResp._error ? null : profileResp;
    state.driverProfileError = profileResp._error || null;
    state.offers = Array.isArray(offersResp.offers) ? offersResp.offers : [];
    state.zones = Array.isArray(zonesResp.recommendations) ? zonesResp.recommendations : [];
    state.bestOffers = Array.isArray(bestResp.offers) ? bestResp.offers : [];
    state.bestOffersError = bestResp._error || null;
    state.dispatch = dispatchResp._error ? null : dispatchResp;
    state.dispatchError = dispatchResp._error || null;
    state.shiftPlan = shiftPlanResp._error ? null : shiftPlanResp;
    state.shiftPlanError = shiftPlanResp._error || null;
    const serverObjectiveWeights = normalizeObjectiveWeightsPayload(
      offersResp?.objective_weights || bestResp?.objective_weights
    );
    if (serverObjectiveWeights) {
      state.objectiveWeights = serverObjectiveWeights;
    }
    applyObjectiveRankingToCollections();
    renderObjectiveWeightControls();

    renderKpis();
    ensureSelectedOffer();
    renderOffers();
    renderBestOffers();
    renderDispatch();
    renderMissionPanel();
    renderZones();
    renderShiftPlan();
    renderDriverProfile();
    renderDecisionFlow();
    setUxStatus('success', 'Ready: choose an offer, score it, then trigger the action route.');
  } catch (err) {
    const msg = errorMessage(err, 'driver refresh failed');
    renderListState(offersEl, 'error', `Unable to load offers: ${msg}`);
    renderListState(bestOffersEl, 'error', `Unable to load nearby recommendations: ${msg}`);
    dispatchSummaryEl.textContent = `Dispatch unavailable: ${msg}`;
    dispatchPlanEl.innerHTML = '';
    if (dispatchMapState.map) {
      clearDispatchLeafletLayers();
      dispatchMapState.map.setView([DISPATCH_MAP_DEFAULT.lat, DISPATCH_MAP_DEFAULT.lon], DISPATCH_MAP_DEFAULT.zoom);
    } else {
      dispatchMapEl.classList.remove('map-ready');
      dispatchMapEl.textContent = 'Dispatch map unavailable.';
    }
    dispatchLegendEl.innerHTML = '';
    renderListState(zonesEl, 'error', `Unable to load zones: ${msg}`);
    if (shiftPlanSummaryEl) shiftPlanSummaryEl.textContent = `Shift plan unavailable: ${msg}`;
    if (shiftPlanEl) renderListState(shiftPlanEl, 'error', `Unable to load shift plan: ${msg}`);
    state.offers = [];
    state.bestOffers = [];
    state.bestOffersError = msg;
    state.dispatch = null;
    state.dispatchError = msg;
    state.zones = [];
    state.shiftPlan = null;
    state.shiftPlanError = msg;
    state.driverProfile = null;
    state.driverProfileError = msg;
    state.selectedOfferKey = null;
    state.selectedOfferSource = null;
    state.lastScoredOfferId = null;
    state.lastActionSummary = null;
    renderKpis();
    renderMissionPanel();
    renderDriverProfile();
    renderDecisionFlow();
    setUxStatus('error', `Data refresh failed: ${msg}`);
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

  state.replayVisual = null;
  state.replayCursorIndex = 0;
  replaySummary.textContent = 'Loading replay...';
  renderListState(replayTimeline, 'loading', 'Loading replay events...');
  if (replayMapEl) {
    replayMapEl.classList.remove('map-ready');
    replayMapEl.textContent = 'Loading replay map...';
  }
  setReplayScrubberState({ positions: [] });
  setReplayCursor(0);

  try {
    const replay = await api(
      `/copilot/replay?from=${encodeURIComponent(fromTs)}&to=${encodeURIComponent(toTs)}&driver_id=${encodeURIComponent(driver)}&limit=400`
    );
    state.replay = Array.isArray(replay.events) ? replay.events : [];
    renderReplay();
  } catch (err) {
    const msg = errorMessage(err, 'replay unavailable');
    state.replay = [];
    state.replayVisual = null;
    state.replayCursorIndex = 0;
    kpiReplay.textContent = '-';
    replaySummary.textContent = `Replay unavailable: ${msg}`;
    if (replayMapEl) {
      replayMapEl.classList.remove('map-ready');
      replayMapEl.textContent = 'Replay map unavailable.';
    }
    setReplayScrubberState({ positions: [] });
    setReplayCursor(0);
    renderListState(replayTimeline, 'error', `Replay request failed: ${msg}`);
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
    const breakdownSummary = scoreBreakdownText(score.score_breakdown, 4);
    const reasonCodeSummary = Array.isArray(score.reason_codes)
      ? score.reason_codes.slice(0, 5).map(humanizeReason).join(' | ')
      : '';
    const route = score.route_source ? ` | ${routeSummary(score)}` : '';
    const detailsText = detailSummary ? ` | ${detailSummary}` : '';
    const breakdownText = breakdownSummary ? ` | ${breakdownSummary}` : '';
    const reasonCodeText = reasonCodeSummary ? ` | codes: ${reasonCodeSummary}` : '';
    scoreReasons.textContent = `[${score.model_used || 'unknown'}] ${expl}${detailsText}${breakdownText}${reasonCodeText}${route}`;

    if (payload.offer_id) {
      state.lastScoredOfferId = String(payload.offer_id);
    }
    setFlowStep('score');
    if (decisionStatusEl) {
      decisionStatusEl.className = 'state-message success';
      decisionStatusEl.textContent = `Scored ${payload.offer_id || 'manual offer'}: ${String(score.decision || '-').toUpperCase()} at ${fmt(score.accept_score, 3)}.`;
    }
    setUxStatus('success', `Offer scored: ${String(score.decision || '-').toUpperCase()} (${fmt(score.accept_score, 3)}).`);
    renderDecisionFlow();
  } catch (err) {
    const msg = errorMessage(err, 'score failed');
    scoreReasons.textContent = `Score unavailable: ${msg}`;
    if (decisionStatusEl) {
      decisionStatusEl.className = 'state-message error';
      decisionStatusEl.textContent = `Could not score selected offer: ${msg}`;
    }
    setUxStatus('error', `Score failed: ${msg}`);
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

function selectOfferByKey(key) {
  const target = allOfferCandidates().find((item) => item.key === key);
  if (!target) return null;
  state.selectedOfferKey = target.key;
  state.selectedOfferSource = target.source;
  state.lastActionSummary = null;
  if (state.lastScoredOfferId && String(state.lastScoredOfferId) !== String(target.offer.offer_id || '')) {
    state.lastScoredOfferId = null;
  }
  setFlowStep('choose');
  if (decisionStatusEl) {
    decisionStatusEl.className = 'state-message loading';
    decisionStatusEl.textContent = `Selected ${target.offer.offer_id || 'offer'}. Next: score it.`;
  }
  setUxStatus('loading', 'Offer selected. Step 2: score. Step 3: action route.');
  renderOffers();
  renderBestOffers();
  renderDecisionFlow();
  return target;
}

async function scoreOfferByCandidate(candidate) {
  if (!candidate || !candidate.offer) return;
  const offer = candidate.offer;
  await scoreManualOffer({
    offer_id: offer.offer_id || '',
    courier_id: offer.courier_id || currentDriverId(),
    use_osrm: Boolean(aroundUseOsrmInput.checked),
  });
}

async function pickAndScoreByKey(key) {
  const selected = selectOfferByKey(key);
  if (!selected) return;
  await scoreOfferByCandidate(selected);
}

async function scoreSelectedOffer() {
  const selected = ensureSelectedOffer();
  if (!selected) {
    setUxStatus('error', 'No offer selected to score.');
    return;
  }
  await scoreOfferByCandidate(selected);
}

function runSelectedActionRoute() {
  const route = resolveSelectedActionRoute();
  const selected = getSelectedOfferCandidate();
  if (!route) {
    if (decisionStatusEl) {
      decisionStatusEl.className = 'state-message error';
      decisionStatusEl.textContent = 'Action route unavailable. Refresh dispatch and select a mapped offer.';
    }
    setUxStatus('error', 'Action route unavailable for selected offer.');
    return;
  }
  window.open(mapsDirectionsUrl(route.originLat, route.originLon, route.destLat, route.destLon), '_blank', 'noopener,noreferrer');
  state.lastActionSummary = `Action route opened for ${selected?.offer?.offer_id || 'selected offer'}.`;
  setFlowStep('action');
  if (decisionStatusEl) {
    decisionStatusEl.className = 'state-message success';
    decisionStatusEl.textContent = 'Action launched: route opened in maps.';
  }
  setUxStatus('success', 'Action step completed: route opened.');
  renderDecisionFlow();
}

async function quickScoreTopOffer() {
  const top = getTopOfferCandidate();
  if (!top) {
    setUxStatus('error', 'No offers available for quick scoring.');
    return;
  }
  await pickAndScoreByKey(top.key);
}

function bindOfferActionDelegation(container) {
  if (!container) return;
  container.addEventListener('click', async (event) => {
    const target = event?.target && typeof event.target.closest === 'function' ? event.target : null;
    if (!target) return;

    const card = target.closest('article[data-offer-key]');
    if (card && !target.closest('button')) {
      const key = card.getAttribute('data-offer-key') || '';
      if (key) selectOfferByKey(key);
    }

    const btn = target.closest('button[data-action]');
    if (!btn) return;

    const action = btn.getAttribute('data-action') || '';
    if (action === 'pick-offer') {
      const key = btn.getAttribute('data-offer-key') || '';
      if (!key) return;
      selectOfferByKey(key);
      return;
    }

    if (action === 'pick-score-offer') {
      const key = btn.getAttribute('data-offer-key') || '';
      if (!key) return;
      await withBusyButton(btn, 'Scoring...', async () => {
        await pickAndScoreByKey(key);
      });
      return;
    }

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
      await withBusyButton(btn, 'Starting...', async () => {
        await startMission(target);
      });
      return;
    }

    if (action !== 'score-offer') return;

    const offerId = btn.getAttribute('data-offer-id') || '';
    const courierId = btn.getAttribute('data-courier-id') || currentDriverId();
    await withBusyButton(btn, 'Scoring...', async () => {
      await scoreManualOffer({
        offer_id: offerId,
        courier_id: courierId,
        use_osrm: Boolean(aroundUseOsrmInput.checked),
      });
    });
  });

  container.addEventListener('keydown', (event) => {
    const keyName = String(event?.key || '');
    if (keyName !== 'Enter' && keyName !== ' ') return;

    const target = event?.target && typeof event.target.closest === 'function' ? event.target : null;
    if (!target) return;
    if (target.closest('button')) return;

    const card = target.closest('article[data-offer-key]');
    if (!card) return;

    const key = card.getAttribute('data-offer-key') || '';
    if (!key) return;
    event.preventDefault();
    selectOfferByKey(key);
  });
}

function safeBind(element, eventName, handler) {
  if (!element || typeof element.addEventListener !== 'function') return;
  element.addEventListener(eventName, handler);
}

function bindChangeAndInput(element, handler) {
  safeBind(element, 'change', handler);
  safeBind(element, 'input', handler);
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

// ── COP-029 : Wallboard Flotte (map multi-markers + drill-down) ──────────────
(function setupFleetWallboard() {
  const mapEl = document.getElementById('fleetMap');
  const refreshBtn = document.getElementById('fleetRefreshBtn');
  const activeChip = document.getElementById('fleetActiveChip');
  const pressureChip = document.getElementById('fleetPressureChip');
  const opportunitiesList = document.getElementById('fleetOpportunitiesList');
  const driverDetail = document.getElementById('fleetDriverDetail');
  const alertsList = document.getElementById('fleetAlertsList');

  if (!mapEl || typeof L === 'undefined') return;

  const fleetMap = L.map(mapEl, { zoomControl: true }).setView([40.75, -73.99], 12);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '© OpenStreetMap',
    maxZoom: 18,
  }).addTo(fleetMap);

  const clusterGroup = L.markerClusterGroup({ maxClusterRadius: 40, disableClusteringAtZoom: 15 });
  fleetMap.addLayer(clusterGroup);

  let _fleetMarkers = {};
  let _refreshTimer = null;

  function _scoreColor(score) {
    if (score >= 0.7) return '#22c55e';
    if (score >= 0.45) return '#f59e0b';
    return '#94a3b8';
  }

  function _driverIcon(score) {
    const color = _scoreColor(score);
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 22 22">
      <circle cx="11" cy="11" r="9" fill="${color}" stroke="#0f172a" stroke-width="2"/>
      <text x="11" y="15" font-size="10" text-anchor="middle" fill="#0f172a" font-weight="bold">${Math.round(score * 10)}</text>
    </svg>`;
    return L.divIcon({
      html: svg, className: '', iconSize: [22, 22], iconAnchor: [11, 11],
    });
  }

  function _renderDriverDetail(d) {
    if (!driverDetail) return;
    const color = _scoreColor(d.opportunity_score);
    driverDetail.innerHTML = `
      <div style="font-weight:600;color:#f8fafc">${escapeHtml(d.courier_id)}</div>
      <div>Zone: <b>${escapeHtml(d.zone_id)}</b> — ${escapeHtml(d.status)}</div>
      <div>Score: <b style="color:${color}">${d.opportunity_score}</b> — ${escapeHtml(d.best_action)}</div>
      <div>Demand: ${d.demand_index} / Supply: ${d.supply_index}</div>
      <div>Pression: ${d.pressure_ratio}</div>
      <div style="color:#64748b;font-size:.75rem">${d.updated_at || ''}</div>
    `;
  }

  async function refreshFleet() {
    try {
      const data = await api('/copilot/fleet/overview?limit=300');
      const drivers = Array.isArray(data.drivers) ? data.drivers : [];

      // Update chips
      if (activeChip) activeChip.textContent = `${data.active_drivers} actifs`;
      if (pressureChip) pressureChip.textContent = `pression ${(data.avg_pressure_ratio || 0).toFixed(2)}`;

      // Update map markers
      const seen = new Set();
      for (const d of drivers) {
        if (!d.lat || !d.lon) continue;
        seen.add(d.courier_id);
        if (_fleetMarkers[d.courier_id]) {
          _fleetMarkers[d.courier_id].setLatLng([d.lat, d.lon]);
          _fleetMarkers[d.courier_id].setIcon(_driverIcon(d.opportunity_score));
          _fleetMarkers[d.courier_id]._fleetData = d;
        } else {
          const marker = L.marker([d.lat, d.lon], { icon: _driverIcon(d.opportunity_score) });
          marker._fleetData = d;
          marker.on('click', () => _renderDriverDetail(marker._fleetData));
          marker.bindTooltip(d.courier_id, { permanent: false, direction: 'top' });
          clusterGroup.addLayer(marker);
          _fleetMarkers[d.courier_id] = marker;
        }
      }
      // Remove stale markers
      for (const cid of Object.keys(_fleetMarkers)) {
        if (!seen.has(cid)) {
          clusterGroup.removeLayer(_fleetMarkers[cid]);
          delete _fleetMarkers[cid];
        }
      }

      // Top opportunities panel
      if (opportunitiesList) {
        const top = (data.top_opportunities || []).slice(0, 6);
        opportunitiesList.innerHTML = top.length === 0
          ? '<div style="color:#64748b;font-size:.8rem">Aucune opportunité forte détectée.</div>'
          : top.map(d => {
              const color = _scoreColor(d.opportunity_score);
              return `<div style="display:flex;justify-content:space-between;align-items:center;background:rgba(255,255,255,.07);border-radius:.3rem;padding:.3rem .5rem;cursor:pointer"
                data-cid="${escapeHtml(d.courier_id)}">
                <span style="font-size:.8rem;color:#e2e8f0">${escapeHtml(d.courier_id)} <span style="color:#64748b">${escapeHtml(d.zone_id)}</span></span>
                <span style="font-weight:700;color:${color}">${d.opportunity_score}</span>
              </div>`;
            }).join('');
        // Click on opportunity row
        opportunitiesList.querySelectorAll('[data-cid]').forEach(row => {
          row.addEventListener('click', () => {
            const cid = row.dataset.cid;
            const m = _fleetMarkers[cid];
            if (m) {
              fleetMap.setView(m.getLatLng(), 15);
              _renderDriverDetail(m._fleetData);
            }
          });
        });
      }

      // Alerts
      if (alertsList) {
        const alerts = data.alerts || [];
        alertsList.textContent = alerts.length === 0 ? '—' : alerts.map(a => a.message).join(' | ');
      }

    } catch (err) {
      if (alertsList) alertsList.textContent = `Erreur: ${errorMessage(err)}`;
    }
  }

  if (refreshBtn) refreshBtn.addEventListener('click', refreshFleet);

  // Auto-refresh every 15s alongside main loop
  function startFleetAutoRefresh() {
    clearInterval(_refreshTimer);
    _refreshTimer = setInterval(refreshFleet, 15000);
  }
  refreshFleet();
  startFleetAutoRefresh();
})();

// ── PWA: Service Worker + offline banner (COP-022) ───────────────────────────
if ('serviceWorker' in navigator) {
  navigator.serviceWorker.register('/static/copilot/sw.js').catch(() => {});
}

(function setupOfflineBanner() {
  const banner = document.createElement('div');
  banner.id = 'offlineBanner';
  banner.setAttribute('role', 'alert');
  banner.setAttribute('aria-live', 'assertive');
  banner.style.cssText = [
    'display:none',
    'position:fixed',
    'top:0',
    'left:0',
    'right:0',
    'z-index:9999',
    'background:#dc2626',
    'color:#fff',
    'text-align:center',
    'padding:.45rem 1rem',
    'font-size:.85rem',
    'font-weight:600',
    'letter-spacing:.02em',
  ].join(';');
  banner.textContent = 'Hors ligne — les données affichées peuvent être obsolètes.';
  document.body.prepend(banner);

  function update() {
    banner.style.display = navigator.onLine ? 'none' : 'block';
  }
  window.addEventListener('online', update);
  window.addEventListener('offline', update);
  update();
})();

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
safeBind(refreshShiftPlanBtn, 'click', refreshDriverData);
safeBind(profileSaveBtn, 'click', () => {
  withBusyButton(profileSaveBtn, 'Saving...', async () => {
    await saveDriverProfile();
  }).catch(() => {});
});
safeBind(refreshFuelBtn, 'click', () => {
  refreshFuelContextNow(false).catch(() => {});
});
safeBind($('replayBtn'), 'click', replayWindow);
bindChangeAndInput(replayScrubberEl, () => {
  setReplayCursor(asFloatOrNull(replayScrubberEl?.value));
});
safeBind($('scoreBtn'), 'click', () => scoreManualOffer());
safeBind(decisionQuickScoreBtn, 'click', () => {
  withBusyButton(decisionQuickScoreBtn, 'Scoring...', async () => {
    await quickScoreTopOffer();
  }).catch(() => {});
});
safeBind(decisionScoreBtn, 'click', () => {
  withBusyButton(decisionScoreBtn, 'Scoring...', async () => {
    await scoreSelectedOffer();
  }).catch(() => {});
});
safeBind(decisionActionBtn, 'click', runSelectedActionRoute);
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
  state.selectedOfferKey = null;
  state.selectedOfferSource = null;
  state.lastScoredOfferId = null;
  state.lastActionSummary = null;
  setFlowStep('choose');
  refreshDriverData();
});
safeBind(offerFilter, 'change', () => {
  renderOffers();
  renderDecisionFlow();
});
bindChangeAndInput(offerLimit, refreshDriverDataDebounced);
bindChangeAndInput(aroundRadiusInput, refreshDriverDataDebounced);
bindChangeAndInput(aroundLimitInput, refreshDriverDataDebounced);
bindChangeAndInput(aroundMinScoreInput, refreshDriverDataDebounced);
safeBind(aroundUseOsrmInput, 'change', refreshDriverData);
bindChangeAndInput(dispatchMaxEtaInput, refreshDriverDataDebounced);
bindChangeAndInput(dispatchForecastHorizonInput, refreshDriverDataDebounced);
bindChangeAndInput(dispatchZoneTopKInput, refreshDriverDataDebounced);
safeBind(dispatchStrategyInput, 'change', refreshDriverData);
bindChangeAndInput(zoneDistanceWeightInput, refreshDriverDataDebounced);
bindChangeAndInput(zoneMaxDistanceKmInput, refreshDriverDataDebounced);
bindChangeAndInput(zoneHomeLatInput, refreshDriverDataDebounced);
bindChangeAndInput(zoneHomeLonInput, refreshDriverDataDebounced);
safeBind(shiftPlanHorizonInput, 'change', refreshDriverData);
bindChangeAndInput(shiftPlanTopKInput, refreshDriverDataDebounced);
bindChangeAndInput(objWeightGainInput, handleObjectiveWeightChange);
bindChangeAndInput(objWeightTimeInput, handleObjectiveWeightChange);
bindChangeAndInput(objWeightFuelInput, handleObjectiveWeightChange);
safeBind(autoRefreshBox, 'change', startAutoRefreshLoop);

// ── Session report export (COP-023) ──────────────────────────────────────────
function _triggerDownload(url, filename) {
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

safeBind($('exportSessionJsonBtn'), 'click', () => {
  const driverId = currentDriverId();
  const url = `/copilot/driver/${encodeURIComponent(driverId)}/session-report?format=json`;
  fetch(url).then(r => r.json()).then(data => {
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
    _triggerDownload(URL.createObjectURL(blob), `session_${driverId}.json`);
  }).catch(err => setUxStatus('error', `Export JSON failed: ${errorMessage(err)}`));
});

safeBind($('exportSessionCsvBtn'), 'click', () => {
  const driverId = currentDriverId();
  _triggerDownload(
    `/copilot/driver/${encodeURIComponent(driverId)}/session-report?format=csv`,
    `session_${driverId}.csv`,
  );
});

safeBind($('exportSessionPdfBtn'), 'click', () => {
  const driverId = currentDriverId();
  _triggerDownload(
    `/copilot/driver/${encodeURIComponent(driverId)}/session-report?format=pdf`,
    `session_${driverId}.pdf`,
  );
});

bindOfferActionDelegation(offersEl);
bindOfferActionDelegation(bestOffersEl);
bindOfferActionDelegation(dispatchPlanEl);
bindOfferActionDelegation(dispatchLegendEl);
bindOfferActionDelegation(shiftPlanEl);
setFlowStep('choose');
renderDecisionFlow();
renderMissionPanel();
state.objectiveWeights = objectiveWeightsFromInputs();
renderObjectiveWeightControls();
setWindowMinutes(30);
applyPreset('balanced');
refreshAll()
  .then(() => replayWindow())
  .catch((err) => {
    const msg = errorMessage(err, 'initial load failed');
    setUxStatus('error', `Initial load failed: ${msg}`);
  });
startAutoRefreshLoop();
