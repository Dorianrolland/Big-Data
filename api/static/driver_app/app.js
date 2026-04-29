const DEMO_DRIVER = Object.freeze({
  driverId: 'drv_demo_001',
  email: 'alex.rivera@fleetstream.demo',
  name: 'Alex Rivera',
  vehicle: 'Toyota Prius Hybrid',
});

const STORAGE_KEYS = Object.freeze({
  session: 'fleetstream-driver-copilot-session',
  preset: 'fleetstream-driver-copilot-preset',
  snapshot: 'fleetstream-driver-copilot-snapshot',
  activeJob: 'fleetstream-driver-copilot-active-job',
  journal: 'fleetstream-driver-copilot-journal',
});

const PRESETS = Object.freeze({
  max_earnings: { target_eur_h: 22, vehicle_mpg: 31, aversion_risque: 0.25, max_eta: 18 },
  balanced: { target_eur_h: 18, vehicle_mpg: 34, aversion_risque: 0.5, max_eta: 16 },
  low_fuel: { target_eur_h: 16, vehicle_mpg: 40, aversion_risque: 0.65, max_eta: 12 },
});

const POLL_INTERVAL_MS = 8000;
const DEFAULT_CENTER = [40.758, -73.9855];

const state = {
  stage: 'login',
  tab: 'home',
  brief: null,
  pollingTimer: null,
  activeJob: loadJson(STORAGE_KEYS.activeJob, null),
  journal: loadJson(STORAGE_KEYS.journal, []),
  homeMap: null,
  homeLayer: null,
  homeMarkers: [],
  zonesMap: null,
  zonesLayer: null,
  zonesMarkers: [],
  routeCache: new Map(),
};

function $(id) {
  return document.getElementById(id);
}

function setText(id, value) {
  const node = $(id);
  if (node) node.textContent = value;
}

function setHtml(id, value) {
  const node = $(id);
  if (node) node.innerHTML = value;
}

function setNodeHidden(id, hidden) {
  const node = $(id);
  if (node) node.hidden = hidden;
}

function loadJson(key, fallback) {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return fallback;
    return JSON.parse(raw);
  } catch (_err) {
    return fallback;
  }
}

function saveJson(key, value) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch (_err) {
    // ignore storage failures in demo mode
  }
}

function nowIso() {
  return new Date().toISOString();
}

function fmtNumber(value, digits = 1) {
  const num = Number(value);
  return Number.isFinite(num) ? num.toFixed(digits) : '-';
}

function fmtSigned(value, digits = 1) {
  const num = Number(value);
  if (!Number.isFinite(num)) return '-';
  const abs = num.toFixed(digits);
  return num > 0 ? `+${abs}` : abs;
}

function fmtCurrency(value, digits = 2) {
  const num = Number(value);
  return Number.isFinite(num) ? `$${num.toFixed(digits)}` : '-';
}

function fmtCurrencyPerHour(value, digits = 1) {
  const num = Number(value);
  return Number.isFinite(num) ? `$${num.toFixed(digits)}/h` : '-';
}

function fmtFuelCost(value, digits = 2) {
  const num = Number(value);
  return Number.isFinite(num) ? `$${num.toFixed(digits)}` : '-';
}

function fmtMinutes(value) {
  const num = Number(value);
  return Number.isFinite(num) ? `${num.toFixed(1)} min` : '-';
}

function fmtPercent(value) {
  const num = Number(value);
  return Number.isFinite(num) ? `${Math.round(num * 100)}%` : '-';
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
    ...options,
  });
  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`;
    try {
      const payload = await response.json();
      if (payload && payload.detail) message = String(payload.detail);
    } catch (_err) {
      // ignore parse failures
    }
    throw new Error(message);
  }
  return response.json();
}

function setUxStatus(message) {
  $('uxStatus').textContent = message;
}

function setStage(stage) {
  state.stage = stage;
  $('screenLogin').classList.toggle('active', stage === 'login');
  $('screenOnboarding').classList.toggle('active', stage === 'onboarding');
  $('screenApp').classList.toggle('active', stage === 'app');
  setNodeHidden('tabbar', stage !== 'app');
  setNodeHidden('desktopNav', stage !== 'app');
}

function sectionIdForTab(tab) {
  return `tab${tab.charAt(0).toUpperCase()}${tab.slice(1)}`;
}

function setTab(tab) {
  state.tab = tab;
  document.querySelectorAll('[data-tab]').forEach((button) => {
    button.classList.toggle('active', button.getAttribute('data-tab') === tab);
  });
  if (state.stage === 'app') {
    const section = $(sectionIdForTab(tab));
    if (section) {
      section.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  }
}

function currentRecommendation() {
  return state.activeJob || state.brief?.primary_recommendation || null;
}

function activeSessionExists() {
  const session = loadJson(STORAGE_KEYS.session, null);
  return Boolean(session && session.driver_id === DEMO_DRIVER.driverId);
}

function selectedPreset() {
  return localStorage.getItem(STORAGE_KEYS.preset) || '';
}

function persistActiveJob(job) {
  state.activeJob = job;
  if (job) saveJson(STORAGE_KEYS.activeJob, job);
  else localStorage.removeItem(STORAGE_KEYS.activeJob);
}

function appendJournal(entry) {
  const next = [{ ...entry, id: `${Date.now()}_${Math.floor(Math.random() * 1000)}` }, ...state.journal].slice(0, 12);
  state.journal = next;
  saveJson(STORAGE_KEYS.journal, next);
}

function renderReasons(reasons) {
  const list = Array.isArray(reasons) ? reasons : [];
  if (!list.length) {
    return '<span class="chip">No reason details yet.</span>';
  }
  return list.map((reason) => `<span class="chip">${escapeHtml(reason)}</span>`).join('');
}

function recommendationSubtitle(rec) {
  if (!rec) return 'Waiting for live data';
  if (rec.kind === 'reposition') return `Reposition to ${escapeHtml(rec.zone_id || 'next zone')}`;
  if (rec.kind === 'hold') return 'Hold and watch the next stronger pocket';
  return `${escapeHtml(rec.decision_badge || 'Take')} - delivery zone ${escapeHtml(rec.zone_id || 'unknown')}`;
}

function humanizeUiReason(value, fallback = '') {
  const raw = String(value || '').trim();
  if (!raw) return fallback;
  const normalized = raw.toLowerCase().replaceAll('-', '_').replaceAll(' ', '_');
  const mapping = {
    instant_dispatch_unavailable: 'Live dispatch is warming up. The site is using market context instead.',
    dispatch_unavailable: 'Live dispatch is warming up. The site is using market context instead.',
    best_offers_around_unavailable: 'Nearby offer ranking is refreshing. The last useful live snapshot stays visible.',
    offers_unavailable: 'Offer feed is refreshing. The site keeps the most useful live snapshot visible.',
    shift_plan_unavailable: 'The longer-range plan is refreshing. Market-driven moves stay visible in the meantime.',
    position_missing: 'The live courier position is still warming up.',
  };
  for (const [key, copy] of Object.entries(mapping)) {
    if (normalized.includes(key)) return copy;
  }
  const humanized = raw.replaceAll('_', ' ');
  return humanized.charAt(0).toUpperCase() + humanized.slice(1);
}

function recommendationNarration(rec, demoContext) {
  if (rec?.kind === 'offer') return 'Accept the top delivery when the ETA and earnings are strong.';
  if (rec?.kind === 'reposition') return 'Move toward the stronger demand pocket when the fuel tradeoff stays controlled.';
  if (demoContext?.alias_active) return 'Live fleet context is linked to this courier profile.';
  return 'Watch the market until a stronger delivery appears.';
}

function renderQualityBadges() {
  const badges = state.brief?.system?.quality_badges || [];
  setHtml('qualityBadges', badges.map((badge) => {
    const tone = escapeHtml(badge.tone || 'good');
    return `<span class="chip ${tone === 'warn' ? 'warn' : tone === 'bad' ? 'bad' : 'good'}">${escapeHtml(badge.label || badge.code)}</span>`;
  }).join(''));
}

function renderHeader() {
  const stale = Boolean(state.brief?.system?.stale);
  const demoContext = state.brief?.demo_context || {};
  setText('driverName', DEMO_DRIVER.name);
  setText('driverVehicle', DEMO_DRIVER.vehicle);
  setText(
    'brandSubtitle',
    demoContext.alias_active
        ? `${DEMO_DRIVER.name} - ${DEMO_DRIVER.vehicle} - live context`
        : `${DEMO_DRIVER.name} - ${DEMO_DRIVER.vehicle}`
  );
  setText('staleBadge', stale ? 'Partial live data - cached snapshot kept' : 'Live snapshot ready');
  const staleBadge = $('staleBadge');
  if (staleBadge) staleBadge.className = `chip ${stale ? 'warn' : 'good'}`;
  const sourceBanner = humanizeUiReason(demoContext.note || '', '');
  setNodeHidden('demoSourceBanner', !sourceBanner);
  setText('demoSourceBanner', sourceBanner);
  renderQualityBadges();
}

function renderHome() {
  const recommendation = currentRecommendation();
  const driver = state.brief?.driver || {};
  const driverPosition = driver.position || {};
  const demoContext = state.brief?.demo_context || {};

  setText('homeHeadline', recommendation?.coach_headline || 'Next best action');
  setText(
    'homeSubheadline',
    recommendation?.kind === 'hold'
      ? 'The copilot prefers patience until the next strong opportunity appears.'
      : demoContext.alias_active
        ? 'Live fleet context is shaping the next action.'
        : 'The copilot ranks this move highest for the current courier profile.'
  );
  setText('recommendationBadge', recommendationSubtitle(recommendation));
  setText('recommendationKind', recommendation?.kind ? recommendation.kind.toUpperCase() : 'WAIT');
  setText('metricScore', recommendation ? fmtPercent(recommendation.accept_score) : '-');
  setText('metricProfit', recommendation ? fmtCurrencyPerHour(recommendation.eur_per_hour_net, 1) : '-');
  setText('metricTripNet', recommendation ? fmtCurrency(recommendation.estimated_net_eur, 2) : '-');
  setText('metricFuel', recommendation ? fmtFuelCost(recommendation.fuel_cost_usd, 2) : '-');
  setText('metricPickupEta', recommendation ? fmtMinutes(recommendation.pickup_eta_min) : '-');
  setText('metricFullEta', recommendation ? fmtMinutes(recommendation.full_eta_min) : '-');
  setText('metricTraffic', recommendation?.traffic_level || '-');
  setText('metricRouteSource', recommendation?.route_source || '-');
  setHtml('homeReasons', renderReasons(recommendation?.coach_reasons || []));

  const warning = humanizeUiReason(recommendation?.coach_warning, '');
  setNodeHidden('homeWarning', !warning);
  setText('homeWarning', warning || '');

  const active = state.activeJob;
  if (active) {
    const startedAt = active.started_at || active.accepted_at || nowIso();
    setText('activeJobTitle', `${active.kind === 'reposition' ? 'Reposition' : 'Accepted delivery'} - ${active.zone_id || 'active mission'}`);
    setText('activeJobStatus', 'Live mission');
    setText('activeJobMeta', `Started ${new Date(startedAt).toLocaleTimeString()} | ${fmtCurrencyPerHour(active.eur_per_hour_net, 1)} upside vs target | ${active.traffic_level || 'Traffic unknown'}`);
  } else {
    setText('activeJobTitle', 'No accepted delivery yet.');
    setText('activeJobStatus', recommendation?.kind === 'hold' ? 'Watch mode' : 'Idle');
    setText(
      'activeJobMeta',
      driverPosition.available
        ? recommendation?.kind === 'hold'
        ? 'The copilot is watching the market. Check Zones for the next move.'
          : 'Accept the recommended delivery to start the mission.'
        : 'Waiting for a live courier position before starting a mission.'
    );
  }

  setText('railDecision', recommendation?.decision_badge || 'Hold');
  setText('railTraffic', recommendation?.traffic_level || '-');
  setText('railFuel', recommendation ? fmtFuelCost(recommendation.fuel_cost_usd, 2) : '-');
  setText('railEta', recommendation ? fmtMinutes(recommendation.full_eta_min) : '-');
  setText('railNarration', recommendationNarration(recommendation, demoContext));
  const takeButton = $('takeJobButton');
  if (takeButton) {
    takeButton.textContent = recommendation?.kind === 'hold' ? 'Lock market watch' : 'Accept this delivery';
  }
}

function renderOrderCard(rec, index) {
  return `
    <article class="list-item">
      <div class="list-item-top">
        <div>
          <div class="list-title">${escapeHtml(index === 0 ? 'Top pick' : `Alternative ${index}`)} - ${escapeHtml(rec.zone_id || rec.kind || 'recommendation')}</div>
          <div class="list-subtitle">${escapeHtml(rec.coach_headline || recommendationSubtitle(rec))}</div>
        </div>
        <span class="chip ${rec.decision_badge === 'Take' ? 'good' : rec.decision_badge === 'Skip' ? 'bad' : 'warn'}">${escapeHtml(rec.decision_badge || 'Maybe')}</span>
      </div>
      <div class="mini-grid">
        <div class="mini-stat"><div class="metric-label">Net USD/h</div><div class="metric-value">${escapeHtml(fmtCurrencyPerHour(rec.eur_per_hour_net, 1))}</div></div>
        <div class="mini-stat"><div class="metric-label">Fuel</div><div class="metric-value">${escapeHtml(fmtFuelCost(rec.fuel_cost_usd, 2))}</div></div>
        <div class="mini-stat"><div class="metric-label">ETA</div><div class="metric-value">${escapeHtml(fmtMinutes(rec.full_eta_min))}</div></div>
        <div class="mini-stat"><div class="metric-label">Risk</div><div class="metric-value">${escapeHtml(rec.traffic_level || '-') }</div></div>
      </div>
      <div class="reason-list">${renderReasons(rec.coach_reasons || [])}</div>
      <div class="action-row">
        <button class="solid-button subtle" type="button" data-action="take-order" data-index="${index}">Accept this option</button>
      </div>
    </article>
  `;
}

function renderOrders() {
  const primary = state.brief?.primary_recommendation;
  const alternatives = Array.isArray(state.brief?.alternatives) ? state.brief.alternatives : [];
  const list = [primary, ...alternatives].filter(Boolean).slice(0, 3);
  $('ordersList').innerHTML = list.length
    ? list.map((rec, index) => renderOrderCard(rec, index)).join('')
    : '<div class="empty-state">No ranked delivery options are available yet. The app keeps the last valid snapshot visible.</div>';
}

function renderZoneItem(zone, maxScore, variant) {
  const score = Number(zone.market_score) || 0;
  const demand = Number(zone.demand_index) || 0;
  const supply = Number(zone.supply_index) || 0;
  const traffic = Number(zone.traffic_factor) || 0;
  const ratio = maxScore > 0 ? Math.min(100, Math.max(6, (score / maxScore) * 100)) : 0;
  const low = variant === 'calm';
  return `
    <article class="zone-item">
      <div class="zone-item-head">
        <span class="zone-id">${escapeHtml(zone.zone_id || 'zone')}</span>
        <span class="zone-score${low ? ' low' : ''}">${escapeHtml(fmtNumber(score, 2))}</span>
      </div>
      <div class="zone-bar"><div class="zone-bar-fill${low ? ' low' : ''}" style="width:${ratio.toFixed(1)}%"></div></div>
      <div class="zone-stats">
        <span class="stat"><span class="k">D</span><span class="v">${escapeHtml(fmtNumber(demand, 2))}</span></span>
        <span class="stat"><span class="k">S</span><span class="v">${escapeHtml(fmtNumber(supply, 2))}</span></span>
        <span class="stat"><span class="k">T</span><span class="v">${escapeHtml(fmtNumber(traffic, 2))}</span></span>
      </div>
    </article>
  `;
}

function updateZoneHeader(countId, stateId, list) {
  const countEl = $(countId);
  if (countEl) countEl.textContent = String(list.length || 0);
  const stateEl = $(stateId);
  if (stateEl) {
    const anyStale = list.some((z) => z && z.stale);
    stateEl.hidden = !anyStale;
  }
}

function renderZones() {
  const hot = state.brief?.zones?.hot_zones || [];
  const calm = state.brief?.zones?.calm_zones || [];
  const reposition = state.brief?.zones?.best_reposition || null;
  const maxHot = Math.max(1, ...hot.map((z) => Number(z.market_score) || 0));
  const maxCalm = Math.max(1, ...calm.map((z) => Number(z.market_score) || 0));
  setHtml('hotZonesList', hot.length
    ? hot.map((zone) => renderZoneItem(zone, maxHot, 'hot')).join('')
    : '<div class="empty-state">Live market zones are still warming up.</div>');
  setHtml('calmZonesList', calm.length
    ? calm.map((zone) => renderZoneItem(zone, maxCalm, 'calm')).join('')
    : '<div class="empty-state">Calm zones will appear as soon as full market context is available.</div>');
  updateZoneHeader('hotZonesCount', 'hotZonesState', hot);
  updateZoneHeader('calmZonesCount', 'calmZonesState', calm);
  const bestMetaEl = $('bestZoneMeta');
  if (bestMetaEl) {
    bestMetaEl.textContent = reposition?.zone_id
      ? `Target · ${reposition.zone_id}`
      : 'Top market move';
  }
  setHtml('bestRepositionCard', reposition
    ? renderOrderCard(reposition, 0)
    : '<div class="empty-state">No reposition move currently beats staying put.</div>');
  setHtml('railZones', hot.slice(0, 3).map((zone) => `<div class="chip">${escapeHtml(zone.zone_id)} - ${escapeHtml(fmtNumber(zone.market_score, 2))}</div>`).join(''));
}

function renderShift() {
  const shift = state.brief?.shift || { items: [] };
  const items = Array.isArray(shift.items) ? shift.items.slice(0, 3) : [];
  setHtml('shiftList', items.length
    ? items.map((item) => `
      <article class="list-item">
        <div class="list-item-top">
          <div>
            <div class="list-title">Move ${escapeHtml(String(item.rank || '?'))} - ${escapeHtml(item.zone_id || 'zone')}</div>
            <div class="list-subtitle">${escapeHtml(item.why_now || 'Short-term market play')}</div>
          </div>
          <span class="chip ${item.context_fallback_applied ? 'warn' : 'good'}">${item.context_fallback_applied ? 'fallback' : 'live'}</span>
        </div>
        <div class="mini-grid">
          <div class="mini-stat"><div class="metric-label">Shift score</div><div class="metric-value">${escapeHtml(fmtNumber(item.shift_score, 2))}</div></div>
          <div class="mini-stat"><div class="metric-label">Net USD/h</div><div class="metric-value">${escapeHtml(fmtCurrencyPerHour(item.estimated_net_eur_h, 1))}</div></div>
          <div class="mini-stat"><div class="metric-label">ETA</div><div class="metric-value">${escapeHtml(fmtMinutes(item.eta_min))}</div></div>
          <div class="mini-stat"><div class="metric-label">Confidence</div><div class="metric-value">${escapeHtml(fmtPercent(item.confidence))}</div></div>
        </div>
        <div class="reason-list">${renderReasons(item.reasons || [])}</div>
      </article>
    `).join('')
    : '<div class="empty-state">Shift plan unavailable. The site keeps the rest of the live briefing visible.</div>');

  setHtml('journalList', state.journal.length
    ? state.journal.map((entry) => `
      <article class="list-item">
        <div class="list-item-top">
          <div>
            <div class="list-title">${escapeHtml(entry.title || 'Mission event')}</div>
            <div class="list-subtitle">${escapeHtml(entry.subtitle || '')}</div>
          </div>
          <span class="chip">${escapeHtml(entry.status || 'saved')}</span>
        </div>
        <div class="mini-copy">${escapeHtml(entry.created_at || '')}</div>
      </article>
    `).join('')
    : '<div class="empty-state">Accepted deliveries and mission updates will appear here during the shift.</div>');
}

function populateProfileSheet() {
  const profile = state.brief?.driver?.profile || PRESETS.balanced;
  $('profileTarget').value = String(profile.target_eur_h ?? PRESETS.balanced.target_eur_h);
  $('profileFuel').value = String(profile.vehicle_mpg ?? PRESETS.balanced.vehicle_mpg);
  $('profileRisk').value = String(profile.aversion_risque ?? PRESETS.balanced.aversion_risque);
  $('profileEta').value = String(profile.max_eta ?? PRESETS.balanced.max_eta);
}

function openProfileSheet() {
  populateProfileSheet();
  $('profileSheet').classList.add('open');
}

function closeProfileSheet() {
  $('profileSheet').classList.remove('open');
}

const LIGHT_TILE_URL = 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png';
const LIGHT_TILE_OPTS = {
  maxZoom: 19,
  subdomains: 'abcd',
  attribution: '&copy; OpenStreetMap &copy; CARTO',
};

function ensureMap(targetId) {
  if (typeof L === 'undefined') return null;
  if (targetId === 'homeMap') {
    if (!state.homeMap) {
      state.homeMap = L.map(targetId, { zoomControl: false }).setView(DEFAULT_CENTER, 12);
      L.tileLayer(LIGHT_TILE_URL, LIGHT_TILE_OPTS).addTo(state.homeMap);
      state.homeLayer = L.layerGroup().addTo(state.homeMap);
    }
    setTimeout(() => state.homeMap.invalidateSize(), 60);
    return state.homeMap;
  }
  if (!state.zonesMap) {
    state.zonesMap = L.map(targetId, { zoomControl: false }).setView(DEFAULT_CENTER, 11);
    L.tileLayer(LIGHT_TILE_URL, LIGHT_TILE_OPTS).addTo(state.zonesMap);
    state.zonesLayer = L.layerGroup().addTo(state.zonesMap);
  }
  setTimeout(() => state.zonesMap.invalidateSize(), 60);
  return state.zonesMap;
}

function coordsToLeaflet(geometry) {
  if (!geometry || !Array.isArray(geometry.coordinates)) return [];
  return geometry.coordinates
    .filter((pair) => Array.isArray(pair) && pair.length >= 2)
    .map((pair) => [Number(pair[1]), Number(pair[0])])
    .filter((pair) => Number.isFinite(pair[0]) && Number.isFinite(pair[1]));
}

async function fetchRouteSegment(origin, destination) {
  if (!origin || !destination) return [];
  const key = `${origin.lat},${origin.lon}:${destination.lat},${destination.lon}`;
  if (state.routeCache.has(key)) return state.routeCache.get(key);
  try {
    const payload = await api(
      `/copilot/route?origin_lat=${encodeURIComponent(origin.lat)}&origin_lon=${encodeURIComponent(origin.lon)}&dest_lat=${encodeURIComponent(destination.lat)}&dest_lon=${encodeURIComponent(destination.lon)}`
    );
    const path = coordsToLeaflet(payload.geometry);
    state.routeCache.set(key, path);
    return path;
  } catch (_err) {
    state.routeCache.set(key, []);
    return [];
  }
}

async function renderHomeMap() {
  const map = ensureMap('homeMap');
  if (!map || !state.homeLayer) return;
  state.homeLayer.clearLayers();

  const recommendation = currentRecommendation();
  const position = state.brief?.driver?.position;
  const hotZones = state.brief?.zones?.hot_zones || [];
  const reposition = state.brief?.zones?.best_reposition || null;
  const origin = position && Number.isFinite(Number(position.lat)) && Number.isFinite(Number(position.lon))
    ? { lat: Number(position.lat), lon: Number(position.lon) }
    : null;

  if (!recommendation && !origin) {
    const previewPoints = hotZones.slice(0, 4).map((zone) => [Number(zone.lat), Number(zone.lon)]).filter((pair) => Number.isFinite(pair[0]) && Number.isFinite(pair[1]));
    previewPoints.forEach((pair, index) => {
      L.circleMarker(pair, {
        radius: index === 0 ? 10 : 8,
        color: index === 0 ? '#ffb020' : '#22d3ee',
        weight: 2,
        fillColor: index === 0 ? '#ffb020' : '#22d3ee',
        fillOpacity: 0.72,
      }).addTo(state.homeLayer).bindTooltip(index === 0 ? 'Best live hot zone' : 'Live market zone');
    });
    if (previewPoints.length) {
      map.fitBounds(L.latLngBounds(previewPoints), { padding: [28, 28] });
    } else {
      map.setView(DEFAULT_CENTER, 11);
    }
    return;
  }

  const destination = recommendation?.pickup || recommendation?.dropoff || recommendation?.zone || reposition?.zone || null;
  const pickup = recommendation.pickup || recommendation.zone || destination;
  const dropoff = recommendation.dropoff || recommendation.zone || destination;

  const points = [];
  if (origin) points.push(origin);
  if (pickup) points.push(pickup);
  if (dropoff && (!pickup || dropoff.lat !== pickup.lat || dropoff.lon !== pickup.lon)) points.push(dropoff);

  const latLngs = [];
  for (let index = 0; index < points.length - 1; index += 1) {
    const segment = await fetchRouteSegment(points[index], points[index + 1]);
    if (segment.length) latLngs.push(...segment);
  }
  if (!latLngs.length) {
    points.forEach((point) => latLngs.push([Number(point.lat), Number(point.lon)]));
  }

  if (origin) {
    L.circleMarker([origin.lat, origin.lon], {
      radius: 7,
      color: '#22d3ee',
      weight: 2,
      fillColor: '#22d3ee',
      fillOpacity: 0.8,
    }).addTo(state.homeLayer).bindTooltip('Courier');
  }

  if (pickup) {
    L.circleMarker([pickup.lat, pickup.lon], {
      radius: 7,
      color: '#ffb020',
      weight: 2,
      fillColor: '#ffb020',
      fillOpacity: 0.85,
    }).addTo(state.homeLayer).bindTooltip(recommendation?.kind === 'reposition' ? 'Target zone' : 'Restaurant');
  }

  if (dropoff && recommendation?.kind === 'offer') {
    L.circleMarker([dropoff.lat, dropoff.lon], {
      radius: 7,
      color: '#34d399',
      weight: 2,
      fillColor: '#34d399',
      fillOpacity: 0.82,
    }).addTo(state.homeLayer).bindTooltip('Customer');
  }

  if (latLngs.length >= 2) {
    L.polyline(latLngs, {
      color: recommendation?.kind === 'reposition' ? '#22d3ee' : '#ffb020',
      weight: 4,
      opacity: 0.92,
    }).addTo(state.homeLayer);
  }

  const mapBounds = latLngs.length ? latLngs : points.map((point) => [Number(point.lat), Number(point.lon)]);
  if (mapBounds.length) {
    map.fitBounds(L.latLngBounds(mapBounds), { padding: [24, 24] });
  } else {
    map.setView(DEFAULT_CENTER, 11);
  }
}

function renderZonesMap() {
  const map = ensureMap('zonesMap');
  if (!map || !state.zonesLayer) return;
  state.zonesLayer.clearLayers();

  const hot = state.brief?.zones?.hot_zones || [];
  const calm = state.brief?.zones?.calm_zones || [];
  const best = state.brief?.zones?.best_reposition || null;
  const position = state.brief?.driver?.position || null;
  const bounds = [];

  hot.forEach((zone) => {
    const marker = L.circleMarker([zone.lat, zone.lon], {
      radius: 10,
      color: '#ffb020',
      weight: 2,
      fillColor: '#ffb020',
      fillOpacity: 0.75,
    });
    marker.bindTooltip(`${zone.zone_id} - hot`);
    marker.addTo(state.zonesLayer);
    bounds.push([zone.lat, zone.lon]);
  });

  calm.forEach((zone) => {
    const marker = L.circleMarker([zone.lat, zone.lon], {
      radius: 8,
      color: '#8b93a3',
      weight: 2,
      fillColor: '#8b93a3',
      fillOpacity: 0.48,
    });
    marker.bindTooltip(`${zone.zone_id} - calm`);
    marker.addTo(state.zonesLayer);
    bounds.push([zone.lat, zone.lon]);
  });

  if (best?.zone?.lat && best?.zone?.lon) {
    const marker = L.circleMarker([best.zone.lat, best.zone.lon], {
      radius: 12,
      color: '#22d3ee',
      weight: 2,
      fillColor: '#22d3ee',
      fillOpacity: 0.82,
    });
    marker.bindTooltip(`Best reposition - ${best.zone_id || best.zone.zone_id || 'zone'}`);
    marker.addTo(state.zonesLayer);
    bounds.push([best.zone.lat, best.zone.lon]);
  }

  if (position?.lat && position?.lon) {
    const marker = L.circleMarker([Number(position.lat), Number(position.lon)], {
      radius: 9,
      color: '#22d3ee',
      weight: 2,
      fillColor: '#22d3ee',
      fillOpacity: 0.88,
    });
    marker.bindTooltip('Courier');
    marker.addTo(state.zonesLayer);
    bounds.push([Number(position.lat), Number(position.lon)]);
  }

  if (bounds.length) {
    map.fitBounds(L.latLngBounds(bounds), { padding: [28, 28] });
  } else {
    map.setView(DEFAULT_CENTER, 11);
  }
}

function renderAll() {
  renderHeader();
  renderHome();
  renderOrders();
  renderZones();
  renderShift();
  renderHomeMap().catch(() => {});
  renderZonesMap();
}

async function refreshBrief({ silent = false } = {}) {
  if (!silent) setUxStatus('Refreshing live courier briefing...');
  try {
    const brief = await api(`/copilot/driver/${encodeURIComponent(DEMO_DRIVER.driverId)}/copilot-brief`);
    state.brief = brief;
    saveJson(STORAGE_KEYS.snapshot, brief);
    renderAll();
    setUxStatus('Live courier briefing updated.');
  } catch (err) {
    const fallback = loadJson(STORAGE_KEYS.snapshot, null);
    if (fallback) {
      fallback.system = fallback.system || {};
      fallback.system.stale = true;
      state.brief = fallback;
      renderAll();
      setUxStatus(`Using cached briefing: ${err.message}`);
    } else {
      setUxStatus(`Live courier briefing failed: ${err.message}`);
    }
  }
}

function startPolling() {
  if (state.pollingTimer) clearInterval(state.pollingTimer);
  state.pollingTimer = setInterval(() => {
    if (document.hidden || state.stage !== 'app') return;
    refreshBrief({ silent: true }).catch(() => {});
  }, POLL_INTERVAL_MS);
}

function stopPolling() {
  if (state.pollingTimer) {
    clearInterval(state.pollingTimer);
    state.pollingTimer = null;
  }
}

async function applyPreset(presetName) {
  const payload = PRESETS[presetName];
  if (!payload) return;
  $('onboardingStatus').textContent = 'Applying courier preset...';
  try {
    await api(`/copilot/driver/${encodeURIComponent(DEMO_DRIVER.driverId)}/profile`, {
      method: 'PUT',
      body: JSON.stringify(payload),
    });
    localStorage.setItem(STORAGE_KEYS.preset, presetName);
    setStage('app');
    setTab('home');
    startPolling();
    await refreshBrief();
    $('onboardingStatus').textContent = `Preset ${presetName.replace('_', ' ')} applied.`;
  } catch (err) {
    $('onboardingStatus').textContent = `Preset failed: ${err.message}`;
  }
}

function takeRecommendation(rec) {
  if (!rec) return;
  if (rec.kind === 'hold') {
    const reposition = state.brief?.zones?.best_reposition;
    if (reposition?.kind === 'reposition') {
      takeRecommendation(reposition);
      return;
    }
    appendJournal({
      title: 'Watch mode armed',
      subtitle: humanizeUiReason(rec.coach_warning, 'Waiting for the next strong delivery.'),
      status: 'watch',
      created_at: new Date().toLocaleTimeString(),
    });
    renderAll();
    setUxStatus('No concrete mission to lock yet. The site is keeping the strongest live watch plan visible.');
    return;
  }
  const job = {
    ...rec,
    accepted_at: nowIso(),
    started_at: nowIso(),
    locked: true,
  };
  persistActiveJob(job);
  appendJournal({
    title: rec.kind === 'reposition' ? `Reposition locked - ${rec.zone_id || 'target zone'}` : `Delivery locked - ${rec.offer_id || rec.zone_id || 'offer'}`,
    subtitle: rec.coach_headline || recommendationSubtitle(rec),
    status: 'accepted',
    created_at: new Date().toLocaleTimeString(),
  });
  renderAll();
    setUxStatus('Delivery mission locked in for this shift.');
}

async function submitMissionReport(activeJob, success) {
  if (!activeJob) return;
  try {
    await api(`/copilot/driver/${encodeURIComponent(DEMO_DRIVER.driverId)}/mission-report`, {
      method: 'POST',
      body: JSON.stringify({
        zone_id: activeJob.zone_id || 'unknown_zone',
        started_at: activeJob.started_at || activeJob.accepted_at || nowIso(),
        completed_at: nowIso(),
        elapsed_min: Math.max(1, (Date.now() - new Date(activeJob.started_at || activeJob.accepted_at || Date.now()).getTime()) / 60000),
        initial_distance_km: Number(activeJob.route_distance_km || 0),
        final_distance_km: success ? 0.0 : Number(activeJob.route_distance_km || 0),
        predicted_eta_min: Number(activeJob.full_eta_min || activeJob.route_duration_min || 0),
        predicted_potential_eur_h: Number(activeJob.eur_per_hour_net || 0),
        baseline_eur_h: Number(state.brief?.alternatives?.[0]?.eur_per_hour_net || 0),
        realized_best_offer_eur_h: success ? Number(activeJob.eur_per_hour_net || 0) : null,
        route_source: activeJob.route_source || null,
        stop_reason: success ? 'Completed from courier app' : 'Stopped from courier app',
        success: Boolean(success),
        target_lat: Number(activeJob.dropoff?.lat || activeJob.zone?.lat || activeJob.dropoff?.latitude || 0) || null,
        target_lon: Number(activeJob.dropoff?.lon || activeJob.zone?.lon || activeJob.dropoff?.longitude || 0) || null,
      }),
    });
  } catch (_err) {
    // keep demo resilient even if journal export is unavailable
  }
}

async function completeMission() {
  if (!state.activeJob) return;
  const activeJob = state.activeJob;
  await submitMissionReport(activeJob, true);
  appendJournal({
    title: `Mission completed - ${activeJob.zone_id || activeJob.offer_id || 'delivery'}`,
    subtitle: activeJob.coach_headline || 'Completed from the courier app',
    status: 'completed',
    created_at: new Date().toLocaleTimeString(),
  });
  persistActiveJob(null);
  renderAll();
  setUxStatus('Mission marked as completed.');
}

async function stopMission() {
  if (!state.activeJob) return;
  const activeJob = state.activeJob;
  await submitMissionReport(activeJob, false);
  appendJournal({
    title: `Mission stopped - ${activeJob.zone_id || activeJob.offer_id || 'delivery'}`,
    subtitle: activeJob.coach_warning || 'Stopped from the courier app',
    status: 'stopped',
    created_at: new Date().toLocaleTimeString(),
  });
  persistActiveJob(null);
  renderAll();
  setUxStatus('Mission stopped.');
}

async function saveProfile() {
  const payload = {
    target_eur_h: Number($('profileTarget').value),
    vehicle_mpg: Number($('profileFuel').value),
    aversion_risque: Number($('profileRisk').value),
    max_eta: Number($('profileEta').value),
  };
  $('profileStatus').textContent = 'Saving profile...';
  try {
    await api(`/copilot/driver/${encodeURIComponent(DEMO_DRIVER.driverId)}/profile`, {
      method: 'PUT',
      body: JSON.stringify(payload),
    });
    $('profileStatus').textContent = 'Profile saved. Refreshing courier briefing...';
    await refreshBrief({ silent: true });
    $('profileStatus').textContent = 'Profile saved.';
  } catch (err) {
    $('profileStatus').textContent = `Profile save failed: ${err.message}`;
  }
}

function bindEvents() {
  $('enterDemoButton').addEventListener('click', () => {
    saveJson(STORAGE_KEYS.session, {
      driver_id: DEMO_DRIVER.driverId,
      email: $('loginEmail').value.trim() || DEMO_DRIVER.email,
      entered_at: nowIso(),
    });
    setStage('onboarding');
  });

  document.querySelectorAll('[data-preset]').forEach((button) => {
    button.addEventListener('click', () => {
      applyPreset(button.getAttribute('data-preset')).catch(() => {});
    });
  });

  document.querySelectorAll('[data-tab]').forEach((button) => {
    button.addEventListener('click', () => {
      setTab(button.getAttribute('data-tab'));
    });
  });

  $('refreshButton').addEventListener('click', () => {
    refreshBrief().catch(() => {});
  });
  $('openProfileButton').addEventListener('click', openProfileSheet);
  $('closeProfileButton').addEventListener('click', closeProfileSheet);
  $('saveProfileButton').addEventListener('click', () => {
    saveProfile().catch(() => {});
  });
  $('compareButton').addEventListener('click', () => setTab('orders'));
  $('takeJobButton').addEventListener('click', () => {
    takeRecommendation(currentRecommendation());
  });
  $('completeMissionButton').addEventListener('click', () => {
    completeMission().catch(() => {});
  });
  $('stopMissionButton').addEventListener('click', () => {
    stopMission().catch(() => {});
  });

  $('ordersList').addEventListener('click', (event) => {
    const button = event.target.closest('[data-action="take-order"]');
    if (!button) return;
    const index = Number(button.getAttribute('data-index'));
    const list = [state.brief?.primary_recommendation, ...(state.brief?.alternatives || [])].filter(Boolean).slice(0, 3);
    const rec = list[index];
    if (rec) takeRecommendation(rec);
  });

  $('bestRepositionCard').addEventListener('click', (event) => {
    const button = event.target.closest('[data-action="take-order"]');
    if (!button) return;
    const rec = state.brief?.zones?.best_reposition;
    if (rec) takeRecommendation(rec);
  });

  document.addEventListener('visibilitychange', () => {
    if (document.hidden) stopPolling();
    else if (state.stage === 'app') startPolling();
  });
}

function boot() {
  bindEvents();
  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('/static/driver_app/sw.js').catch(() => {});
  }

  if (activeSessionExists()) {
    const preset = selectedPreset();
    setStage(preset ? 'app' : 'onboarding');
    if (preset) {
      setTab('home');
      const snapshot = loadJson(STORAGE_KEYS.snapshot, null);
      if (snapshot) {
        state.brief = snapshot;
        renderAll();
      }
      startPolling();
      refreshBrief({ silent: true }).catch(() => {});
    }
  } else {
    setStage('login');
  }

  setUxStatus('Courier Copilot ready.');
}

boot();
