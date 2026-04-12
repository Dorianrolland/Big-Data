const state = {
  health: null,
  offers: [],
  zones: [],
  replay: [],
  autoRefreshTimer: null,
};

const $ = (id) => document.getElementById(id);

const healthMode = $('healthMode');
const healthGate = $('healthGate');
const healthMetrics = $('healthMetrics');

const offersEl = $('offers');
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

async function api(path, opts = {}) {
  const response = await fetch(path, {
    headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) },
    ...opts,
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json();
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
  const now = new Date();
  const from = new Date(now.getTime() - minutes * 60 * 1000);
  fromInput.value = toIso(from).slice(0, 19) + 'Z';
  toInput.value = toIso(now).slice(0, 19) + 'Z';
}

function setTodayWindow() {
  const now = new Date();
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0));
  fromInput.value = toIso(start).slice(0, 19) + 'Z';
  toInput.value = toIso(now).slice(0, 19) + 'Z';
}

function asDecisionClass(decision) {
  return decision === 'accept' ? 'accept' : 'reject';
}

function humanizeReason(reason) {
  const mapping = {
    high_estimated_net_revenue: 'high net revenue',
    low_estimated_net_revenue: 'low net revenue',
    strong_demand_pressure: 'strong demand pressure',
    weak_demand_pressure: 'weak demand pressure',
    weather_downside: 'weather downside',
    traffic_penalty: 'traffic penalty',
    balanced_offer_profile: 'balanced profile',
  };
  return mapping[reason] || reason.replaceAll('_', ' ');
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
  const items = [];
  if (Number.isFinite(Number(metrics.roc_auc))) items.push(`AUC ${fmt(metrics.roc_auc, 3)}`);
  if (Number.isFinite(Number(metrics.average_precision))) items.push(`AP ${fmt(metrics.average_precision, 3)}`);
  if (Number.isFinite(Number(metrics.brier_score))) items.push(`Brier ${fmt(metrics.brier_score, 3)}`);
  if (Number.isFinite(Number(gate.trained_rows))) items.push(`rows ${gate.trained_rows}`);

  healthMetrics.innerHTML = '';
  items.forEach((txt) => {
    const node = document.createElement('span');
    node.className = 'chip';
    node.textContent = txt;
    healthMetrics.appendChild(node);
  });
}

function renderKpis() {
  const offers = state.offers;
  kpiOffers.textContent = String(offers.length);

  if (!offers.length) {
    kpiAcceptRate.textContent = '-';
    kpiAvgNet.textContent = '-';
    return;
  }

  const acceptCount = offers.filter((x) => x.decision === 'accept').length;
  const acceptRate = (acceptCount / offers.length) * 100;
  const avgNet = offers.reduce((acc, x) => acc + Number(x.eur_per_hour_net || 0), 0) / offers.length;

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
    const zone = offer.zone_id || 'unknown_zone';

    card.innerHTML = `
      <div class="offer-top">
        <strong>${offer.offer_id || 'manual-offer'}</strong>
        <span class="badge ${asDecisionClass(offer.decision)}">${(offer.decision || 'n/a').toUpperCase()}</span>
      </div>
      <div class="offer-meta">
        <span class="muted">zone ${zone}</span>
        <span><strong>${fmt(offer.accept_score, 3)}</strong> score · <strong>${fmt(offer.eur_per_hour_net, 1)}</strong> EUR/h</span>
      </div>
      <div class="chips">${reasons.map((r) => `<span>${humanizeReason(r)}</span>`).join('')}</div>
      <div class="offer-actions">
        <button class="ghost" data-action="score-offer" data-offer-id="${offer.offer_id || ''}" data-courier-id="${offer.courier_id || ''}">Score This Offer</button>
      </div>
    `;

    offersEl.appendChild(card);
  });
}

function renderZones() {
  zonesEl.innerHTML = '';
  const zones = state.zones.slice(0, 8);

  if (!zones.length) {
    zonesEl.innerHTML = '<div class="muted">No zone recommendations yet.</div>';
    return;
  }

  const maxScore = Math.max(...zones.map((z) => Number(z.opportunity_score || 0)), 0.001);
  zones.forEach((zone, idx) => {
    const row = document.createElement('article');
    row.className = 'zone';

    const score = Number(zone.opportunity_score || 0);
    const meter = Math.max(6, Math.min(100, (score / maxScore) * 100));

    row.innerHTML = `
      <div class="zone-top">
        <strong>#${idx + 1} ${zone.zone_id || 'unknown_zone'}</strong>
        <span>${fmt(score, 2)}</span>
      </div>
      <div class="muted">demand ${fmt(zone.demand_index, 2)} / supply ${fmt(zone.supply_index, 2)} · weather ${fmt(zone.weather_factor, 2)} · traffic ${fmt(zone.traffic_factor, 2)}</div>
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
    .join(' · ');

  const firstTs = events[0].ts;
  const lastTs = events[events.length - 1].ts;
  replaySummary.textContent = `${events.length} events · ${summary} · window ${firstTs} -> ${lastTs}`;

  events.slice(-20).reverse().forEach((evt) => {
    const row = document.createElement('div');
    row.className = 'event-row';
    row.innerHTML = `
      <strong>${evt.event_type || 'event'} · ${evt.status || 'n/a'}</strong>
      <div>offer ${evt.offer_id || '-'} · order ${evt.order_id || '-'} · zone ${evt.zone_id || '-'}</div>
      <div class="muted">${evt.ts} · topic ${evt.topic || '-'}</div>
    `;
    replayTimeline.appendChild(row);
  });
}

async function refreshHealth() {
  try {
    state.health = await api('/copilot/health');
  } catch (_) {
    state.health = null;
  }
  renderHealth();
}

async function refreshDriverData() {
  const driver = (driverInput.value || 'L001').trim();
  const limit = Number(offerLimit.value || 12);

  offersEl.innerHTML = '<div class="muted">Loading offers...</div>';
  zonesEl.innerHTML = '<div class="muted">Loading zones...</div>';

  try {
    const [offersResp, zonesResp] = await Promise.all([
      api(`/copilot/driver/${encodeURIComponent(driver)}/offers?limit=${Math.max(limit, 20)}`),
      api(`/copilot/driver/${encodeURIComponent(driver)}/next-best-zone?top_k=8`),
    ]);

    state.offers = Array.isArray(offersResp.offers) ? offersResp.offers : [];
    state.zones = Array.isArray(zonesResp.recommendations) ? zonesResp.recommendations : [];

    renderKpis();
    renderOffers();
    renderZones();
  } catch (err) {
    offersEl.innerHTML = `<div class="muted">Failed loading offers: ${err.message}</div>`;
    zonesEl.innerHTML = `<div class="muted">Failed loading zones: ${err.message}</div>`;
    state.offers = [];
    state.zones = [];
    renderKpis();
  }
}

async function replayWindow() {
  const driver = (driverInput.value || 'L001').trim();
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
    courier_id: (driverInput.value || 'L001').trim(),
    estimated_fare_eur: Number($('fare').value),
    estimated_distance_km: Number($('distance').value),
    estimated_duration_min: Number($('duration').value),
    demand_index: Number($('demand').value),
    supply_index: Number($('supply').value),
    weather_factor: 1.0,
    traffic_factor: Number($('traffic').value),
  };

  try {
    const score = await api('/copilot/score-offer', {
      method: 'POST',
      body: JSON.stringify(payload),
    });

    scoreProb.textContent = fmt(score.accept_score, 3);
    scoreDecision.textContent = (score.decision || '-').toUpperCase();
    scoreEur.textContent = fmt(score.eur_per_hour_net, 1);

    const expl = Array.isArray(score.explanation) ? score.explanation.map(humanizeReason).join(' · ') : '-';
    scoreReasons.textContent = `[${score.model_used || 'unknown'}] ${expl}`;
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

function bindOfferActionDelegation() {
  offersEl.addEventListener('click', async (event) => {
    const btn = event.target.closest('button[data-action="score-offer"]');
    if (!btn) return;

    const offerId = btn.getAttribute('data-offer-id') || '';
    const courierId = btn.getAttribute('data-courier-id') || (driverInput.value || 'L001').trim();
    btn.disabled = true;
    btn.textContent = 'Scoring...';

    try {
      await scoreManualOffer({ offer_id: offerId, courier_id: courierId });
    } finally {
      btn.disabled = false;
      btn.textContent = 'Score This Offer';
    }
  });
}

function startAutoRefreshLoop() {
  if (state.autoRefreshTimer) {
    clearInterval(state.autoRefreshTimer);
    state.autoRefreshTimer = null;
  }

  if (!autoRefreshBox.checked) {
    return;
  }

  state.autoRefreshTimer = setInterval(async () => {
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

$('window30').addEventListener('click', () => {
  setWindowMinutes(30);
  replayWindow();
});

$('window120').addEventListener('click', () => {
  setWindowMinutes(120);
  replayWindow();
});

$('windowToday').addEventListener('click', () => {
  setTodayWindow();
  replayWindow();
});

$('refreshBtn').addEventListener('click', refreshAll);
$('replayBtn').addEventListener('click', replayWindow);
$('scoreBtn').addEventListener('click', () => scoreManualOffer());
$('presetSafe').addEventListener('click', () => applyPreset('safe'));
$('presetBalanced').addEventListener('click', () => applyPreset('balanced'));
$('presetSurge').addEventListener('click', () => applyPreset('surge'));
offerFilter.addEventListener('change', renderOffers);
offerLimit.addEventListener('change', refreshDriverData);
autoRefreshBox.addEventListener('change', startAutoRefreshLoop);

bindOfferActionDelegation();
setWindowMinutes(30);
applyPreset('balanced');
refreshAll().then(() => replayWindow());
startAutoRefreshLoop();
