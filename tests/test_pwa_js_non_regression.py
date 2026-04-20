from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parent.parent
APP_JS_PATH = ROOT / "api" / "static" / "copilot" / "app.js"


@pytest.mark.skipif(shutil.which("node") is None, reason="node is required for JS runtime regression test")
def test_pwa_main_flow_has_no_runtime_or_console_errors() -> None:
    app_js_escaped = json.dumps(str(APP_JS_PATH))
    harness = f"""
const fs = require('fs');
const vm = require('vm');

const appPath = {app_js_escaped};
const source = fs.readFileSync(appPath, 'utf8');
const consoleErrors = [];
const unhandledErrors = [];
const openedUrls = [];
let lastProfilePutBody = null;

process.on('unhandledRejection', (err) => {{
  unhandledErrors.push(String(err && err.message ? err.message : err));
}});
process.on('uncaughtException', (err) => {{
  unhandledErrors.push(String(err && err.message ? err.message : err));
}});

const realConsole = console;
const consoleProxy = {{
  ...realConsole,
  error: (...args) => {{
    consoleErrors.push(args.map((v) => String(v)).join(' '));
  }},
}};

function makeClassList() {{
  const set = new Set();
  return {{
    add: (...tokens) => tokens.forEach((token) => set.add(String(token))),
    remove: (...tokens) => tokens.forEach((token) => set.delete(String(token))),
    toggle: (token, force) => {{
      const name = String(token);
      if (force === true) {{
        set.add(name);
        return true;
      }}
      if (force === false) {{
        set.delete(name);
        return false;
      }}
      if (set.has(name)) {{
        set.delete(name);
        return false;
      }}
      set.add(name);
      return true;
    }},
    contains: (token) => set.has(String(token)),
  }};
}}

function makeElement(tagName = 'DIV') {{
  const attrs = new Map();
  let innerHtmlValue = '';
  return {{
    tagName,
    value: '',
    checked: false,
    disabled: false,
    textContent: '',
    className: '',
    style: {{}},
    children: [],
    classList: makeClassList(),
    addEventListener: () => {{}},
    get innerHTML() {{
      return innerHtmlValue;
    }},
    set innerHTML(value) {{
      innerHtmlValue = String(value || '');
      this.children = [];
    }},
    appendChild(child) {{
      this.children.push(child);
      return child;
    }},
    setAttribute(name, value) {{
      attrs.set(String(name), String(value));
    }},
    getAttribute(name) {{
      return attrs.has(String(name)) ? attrs.get(String(name)) : null;
    }},
    closest(selector) {{
      if (selector === 'button[data-action]' && this.tagName === 'BUTTON' && attrs.has('data-action')) {{
        return this;
      }}
      if (selector === 'article[data-offer-key]' && this.tagName === 'ARTICLE' && attrs.has('data-offer-key')) {{
        return this;
      }}
      return null;
    }},
  }};
}}

const nodes = new Map();
function getNode(id) {{
  if (!nodes.has(id)) {{
    const node = makeElement('DIV');
    node.id = id;
    if (id === 'autoRefresh') node.checked = true;
    if (id === 'aroundUseOsrm') node.checked = false;
    nodes.set(id, node);
  }}
  return nodes.get(id);
}}

const documentStub = {{
  getElementById: (id) => getNode(String(id)),
  createElement: (tag) => makeElement(String(tag || 'DIV').toUpperCase()),
}};

function ok(payload) {{
  return {{
    ok: true,
    status: 200,
    statusText: 'OK',
    json: async () => payload,
  }};
}}

function notFound(payload) {{
  return {{
    ok: false,
    status: 404,
    statusText: 'Not Found',
    json: async () => payload,
  }};
}}

const nowTs = '2026-04-17T20:00:00Z';
const offerBase = {{
  offer_id: 'off_demo_001',
  courier_id: 'drv_demo_001',
  zone_id: 'midtown',
  decision: 'accept',
  recommendation_action: 'accept',
  accept_score: 0.83,
  recommendation_score: 0.88,
  eur_per_hour_net: 24.5,
  estimated_net_eur: 8.6,
  target_gap_eur_h: 4.1,
  route_duration_min: 15.0,
  route_distance_km: 4.4,
  route_source: 'estimated',
  distance_to_pickup_km: 1.1,
  model_used: 'ml',
  explanation: ['high_estimated_net_revenue', 'above_target_hourly_goal'],
  explanation_details: [
    {{ code: 'net_hourly', label: 'net hourly', impact: 'positive', value: 24.5, unit: 'eur_per_hour', source: 'target' }},
  ],
  costs: {{
    fuel_cost_eur: 1.2,
    platform_fee_eur: 2.9,
    other_costs_eur: 0.3,
    estimated_net_eur: 8.6,
    estimated_net_eur_h: 24.5,
    target_gap_eur_h: 4.1,
  }},
}};

async function fetchStub(url, _opts) {{
  const parsed = new URL(String(url), 'http://localhost');
  const path = parsed.pathname;
  const method = String(_opts?.method || 'GET').toUpperCase();

  if (path === '/copilot/health') {{
    return ok({{
      model_loaded: true,
      model_quality_gate: {{ accepted: true, reason: 'ok', trained_rows: 12345 }},
      model_metrics: {{ roc_auc: 0.81, average_precision: 0.79, brier_score: 0.12 }},
      fuel_context: {{ fuel_price_eur_l: 1.71, source: 'stub', fuel_sync_status: 'ok' }},
    }});
  }}
  if (path.endsWith('/mission-journal')) {{
    return ok({{
      count: 1,
      missions: [{{
        zone_id: '<script>alert(1)</script>',
        dispatch_strategy: 'balanced',
        success: true,
        completed_at: nowTs,
        elapsed_min: 11.2,
        predicted_delta_eur_h: 2.4,
        realized_delta_eur_h: 2.1,
        stop_reason: '<img src=x onerror=1>',
      }}],
      stats: {{
        missions_count: 1,
        latest_completed_at: nowTs,
        success_rate_pct: 100.0,
        avg_predicted_delta_eur_h: 2.4,
        avg_realized_delta_eur_h: 2.1,
        avg_elapsed_min: 11.2,
        strategy_counts: {{ balanced: 1 }},
      }},
    }});
  }}
  if (path.endsWith('/profile')) {{
    if (method === 'PUT') {{
      try {{
        lastProfilePutBody = _opts && _opts.body ? JSON.parse(String(_opts.body)) : null;
      }} catch (_) {{
        lastProfilePutBody = null;
      }}
      return ok({{
        driver_id: 'drv_demo_001',
        target_eur_h: Number(lastProfilePutBody?.target_eur_h ?? 19.5),
        consommation_l_100: Number(lastProfilePutBody?.consommation_l_100 ?? 7.1),
        aversion_risque: Number(lastProfilePutBody?.aversion_risque ?? 0.42),
        max_eta: Number(lastProfilePutBody?.max_eta ?? 18.0),
        source: 'manual',
        updated_at: nowTs,
      }});
    }}
    return ok({{
      driver_id: 'drv_demo_001',
      target_eur_h: 18.0,
      consommation_l_100: 7.5,
      aversion_risque: 0.5,
      max_eta: 20.0,
      source: 'stored',
      updated_at: nowTs,
    }});
  }}
  if (path.endsWith('/offers')) {{
    return ok({{
      driver_id: 'drv_demo_001',
      count: 2,
      accept_count: 1,
      accept_rate_pct: 50,
      offers: [offerBase, {{ ...offerBase, offer_id: 'off_demo_002', decision: 'reject', accept_score: 0.28, eur_per_hour_net: 11.2, target_gap_eur_h: -2.0 }}],
    }});
  }}
  if (path.endsWith('/best-offers-around')) {{
    return ok({{
      driver_id: 'drv_demo_001',
      count: 1,
      action_counts: {{ accept: 1, consider: 0, skip: 0 }},
      offers: [offerBase],
    }});
  }}
  if (path.endsWith('/next-best-zone')) {{
    return ok({{
      count: 1,
      recommendations: [{{
        zone_id: 'midtown',
        opportunity_score: 0.91,
        adjusted_opportunity_score: 0.86,
        distance_km: 2.1,
        reposition_cost_eur: 1.4,
        reposition_time_min: 9.5,
        demand_index: 1.6,
        supply_index: 0.9,
        weather_factor: 1.0,
        traffic_factor: 1.1,
      }}],
    }});
  }}
  if (path.endsWith('/instant-dispatch')) {{
    return ok({{
      decision: 'reposition',
      confidence: 0.74,
      reasons: ['zone_opportunity_outweighs_local_offer'],
      dispatch_strategy: 'balanced',
      target_hourly_net_eur: 18,
      max_reposition_eta_min: 20,
      effective_max_reposition_eta_min: 20,
      forecast_horizon_min: 30,
      effective_forecast_horizon_min: 30,
      local_candidates_count: 1,
      reposition_candidates_count: 1,
      origin: {{ lat: 40.758, lon: -73.9855 }},
      stay_option: offerBase,
      reposition_option: {{
        zone_id: 'midtown',
        zone_lat: 40.7611,
        zone_lon: -73.9776,
        dispatch_score: 0.81,
        eta_min: 10.2,
        route_distance_km: 3.1,
        route_source: 'estimated',
        estimated_potential_eur_h: 25.9,
        risk_adjusted_potential_eur_h: 23.8,
        net_gain_vs_stay_eur_h: 2.7,
        travel_cost_eur: 1.0,
        time_cost_eur: 1.8,
        risk_cost_eur: 0.7,
        reposition_total_cost_eur: 3.5,
        demand_index: 1.5,
        supply_index: 0.9,
        forecast_pressure_ratio: 1.3,
        opportunity_score: 0.92,
      }},
      reposition_candidates: [{{
        zone_id: 'midtown',
        zone_lat: 40.7611,
        zone_lon: -73.9776,
        dispatch_score: 0.81,
        eta_min: 10.2,
        route_distance_km: 3.1,
        estimated_potential_eur_h: 25.9,
        risk_adjusted_potential_eur_h: 23.8,
        reposition_total_cost_eur: 3.5,
      }}],
    }});
  }}
  if (path.endsWith('/shift-plan')) {{
    return ok({{
      driver_id: 'drv_demo_001',
      horizon_min: 60,
      generated_at: nowTs,
      target_hourly_net_eur: 18.0,
      source: 'zone_context_v2',
      count: 3,
      items: [
        {{
          rank: 1,
          zone_id: 'midtown',
          zone_lat: 40.7611,
          zone_lon: -73.9776,
          shift_score: 1.24,
          confidence: 0.82,
          estimated_net_eur_h: 26.1,
          estimated_gross_eur_h: 28.7,
          net_gain_vs_target_eur_h: 8.1,
          horizon_min: 60,
          why_now: 'Strong forecast pressure in next 60 min.',
          reasons: ['Strong forecast pressure', 'Quick reposition'],
          demand_index: 1.6,
          supply_index: 0.9,
          weather_factor: 1.0,
          traffic_factor: 1.1,
          event_pressure: 0.18,
          temporal_pressure: 0.13,
          forecast_pressure_ratio: 1.33,
          forecast_volatility: 0.2,
          distance_km: 2.5,
          eta_min: 9.6,
          reposition_total_cost_eur: 2.3,
          context_fallback_applied: false,
          freshness_policy: 'stale_neutral_v1',
        }},
        {{
          rank: 2,
          zone_id: 'chelsea',
          zone_lat: 40.7465,
          zone_lon: -74.0014,
          shift_score: 1.11,
          confidence: 0.74,
          estimated_net_eur_h: 23.2,
          estimated_gross_eur_h: 25.0,
          net_gain_vs_target_eur_h: 5.2,
          horizon_min: 60,
          why_now: 'Healthy demand/supply outlook over 60 min.',
          reasons: ['Healthy demand/supply outlook'],
          demand_index: 1.4,
          supply_index: 0.95,
          weather_factor: 1.0,
          traffic_factor: 1.1,
          event_pressure: 0.1,
          temporal_pressure: 0.08,
          forecast_pressure_ratio: 1.2,
          forecast_volatility: 0.23,
          distance_km: 3.4,
          eta_min: 11.2,
          reposition_total_cost_eur: 2.8,
          context_fallback_applied: false,
          freshness_policy: 'stale_neutral_v1',
        }},
        {{
          rank: 3,
          zone_id: 'soho',
          zone_lat: 40.7233,
          zone_lon: -74.0030,
          shift_score: 1.01,
          confidence: 0.68,
          estimated_net_eur_h: 20.4,
          estimated_gross_eur_h: 22.4,
          net_gain_vs_target_eur_h: 2.4,
          horizon_min: 60,
          why_now: 'Reposition ETA is manageable.',
          reasons: ['Reposition ETA is manageable'],
          demand_index: 1.25,
          supply_index: 1.0,
          weather_factor: 1.0,
          traffic_factor: 1.15,
          event_pressure: 0.06,
          temporal_pressure: 0.05,
          forecast_pressure_ratio: 1.1,
          forecast_volatility: 0.24,
          distance_km: 4.1,
          eta_min: 13.8,
          reposition_total_cost_eur: 3.4,
          context_fallback_applied: false,
          freshness_policy: 'stale_neutral_v1',
        }},
      ],
    }});
  }}
  if (path === '/copilot/score-offer') {{
    return ok({{
      offer_id: 'off_demo_001',
      decision: 'accept',
      accept_score: 0.88,
      eur_per_hour_net: 25.1,
      model_used: 'ml',
      explanation: ['high_estimated_net_revenue', 'above_target_hourly_goal'],
      explanation_details: [],
      route_source: 'estimated',
      route_distance_km: 4.0,
      route_duration_min: 14.8,
    }});
  }}
  if (path === '/copilot/replay') {{
    return ok({{
      count: 1,
      events: [{{
        event_type: '<img src=x onerror=1>',
        status: 'ok',
        offer_id: 'off_demo_001',
        order_id: 'order_001',
        zone_id: 'midtown',
        ts: nowTs,
        topic: 'copilot',
      }}],
    }});
  }}

  return notFound({{ detail: `unhandled path: ${{path}}` }});
}}

const context = {{
  console: consoleProxy,
  document: documentStub,
  navigator: {{ serviceWorker: {{ register: async () => null }} }},
  fetch: fetchStub,
  AbortController,
  setTimeout,
  clearTimeout,
  setInterval: () => 1,
  clearInterval: () => {{}},
  Date,
  Math,
  Number,
  String,
  Boolean,
  Promise,
  URL,
  URLSearchParams,
}};
context.window = {{
  L: null,
  open: (url) => openedUrls.push(String(url)),
}};
context.window.document = context.document;
context.window.navigator = context.navigator;
context.globalThis = context;
context.self = context.window;

vm.createContext(context);
vm.runInContext(source, context, {{ filename: 'app.js' }});

async function main() {{
  await new Promise((resolve) => setTimeout(resolve, 80));
  if (typeof context.selectOfferByKey === 'function') {{
    context.selectOfferByKey('offer:off_demo_001');
  }}
  if (typeof context.quickScoreTopOffer === 'function') {{
    await context.quickScoreTopOffer();
  }}
  if (typeof context.runSelectedActionRoute === 'function') {{
    context.runSelectedActionRoute();
  }}
  getNode('profileTargetEurH').value = '999';
  getNode('profileConsumption').value = 'abc';
  getNode('profileRiskAversion').value = '-1';
  getNode('profileMaxEta').value = '1000';
  if (typeof context.saveDriverProfile === 'function') {{
    await context.saveDriverProfile();
  }}
  await new Promise((resolve) => setTimeout(resolve, 80));

  const scoreDecisionText = String(getNode('scoreDecision').textContent || '').trim();
  if (scoreDecisionText !== 'ACCEPT') {{
    realConsole.error('expected scoreDecision to be ACCEPT, got', scoreDecisionText);
    process.exit(1);
  }}
  if (!getNode('flowStepAction').classList.contains('active')) {{
    realConsole.error('expected flowStepAction to be active');
    process.exit(1);
  }}
  const decisionStatusText = String(getNode('decisionStatus').textContent || '');
  if (!decisionStatusText.toLowerCase().includes('action')) {{
    realConsole.error('expected decisionStatus to mention action, got', decisionStatusText);
    process.exit(1);
  }}
  const replayRows = getNode('replayTimeline').children || [];
  const replayHtml = replayRows.length ? String(replayRows[0].innerHTML || '') : '';
  if (!replayRows.length) {{
    realConsole.error('expected replay timeline row to be rendered');
    process.exit(1);
  }}
  if (replayHtml.includes('<img') || replayHtml.includes('<script')) {{
    realConsole.error('expected replay HTML to be escaped, got', replayHtml);
    process.exit(1);
  }}
  if (!replayHtml.includes('&lt;img')) {{
    realConsole.error('expected escaped replay marker in HTML, got', replayHtml);
    process.exit(1);
  }}

  const missionHtml = String(getNode('missionJournalList').innerHTML || '');
  if (!missionHtml) {{
    realConsole.error('expected mission journal HTML to be rendered');
    process.exit(1);
  }}
  if (missionHtml.includes('<script') || missionHtml.includes('<img')) {{
    realConsole.error('expected mission journal HTML to be escaped, got', missionHtml);
    process.exit(1);
  }}
  if (!missionHtml.includes('&lt;script') || !missionHtml.includes('&lt;img')) {{
    realConsole.error('expected escaped mission markers in HTML, got', missionHtml);
    process.exit(1);
  }}
  const profileStatus = String(getNode('profileStatus').textContent || '');
  if (!profileStatus.toLowerCase().includes('profile')) {{
    realConsole.error('expected profile status to be rendered, got', profileStatus);
    process.exit(1);
  }}
  if (!lastProfilePutBody) {{
    realConsole.error('expected profile update payload to be captured');
    process.exit(1);
  }}
  if (lastProfilePutBody.target_eur_h !== 150 || lastProfilePutBody.consommation_l_100 !== 7.5 || lastProfilePutBody.aversion_risque !== 0 || lastProfilePutBody.max_eta !== 60) {{
    realConsole.error('expected normalized profile payload, got', JSON.stringify(lastProfilePutBody));
    process.exit(1);
  }}
  const shiftSummary = String(getNode('shiftPlanSummary').textContent || '');
  if (!shiftSummary.toLowerCase().includes('top')) {{
    realConsole.error('expected shift plan summary to be rendered, got', shiftSummary);
    process.exit(1);
  }}
  const shiftRows = getNode('shiftPlanList').children || [];
  if (!shiftRows.length) {{
    realConsole.error('expected shift plan rows to be rendered');
    process.exit(1);
  }}

  if (consoleErrors.length || unhandledErrors.length) {{
    realConsole.error('consoleErrors=', consoleErrors);
    realConsole.error('unhandledErrors=', unhandledErrors);
    process.exit(1);
  }}
  if (!openedUrls.length) {{
    realConsole.error('expected at least one action route open');
    process.exit(1);
  }}
  process.exit(0);
}}

main().catch((err) => {{
  realConsole.error(err);
  process.exit(1);
}});
"""

    proc = subprocess.run(
        ["node", "-e", harness],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert proc.returncode == 0, (
        "PWA JS runtime regression detected.\n"
        f"stdout:\n{proc.stdout}\n"
        f"stderr:\n{proc.stderr}\n"
    )


def test_pwa_index_contains_decision_flow_region() -> None:
    index_path = ROOT / "api" / "static" / "copilot" / "index.html"
    content = index_path.read_text(encoding="utf-8")
    assert 'id="uxStatusLine"' in content
    assert 'id="decisionStatus"' in content
    assert 'id="flowStepChoose"' in content
    assert 'id="flowStepScore"' in content
    assert 'id="flowStepAction"' in content
    assert 'id="decisionQuickScoreBtn"' in content
    assert 'id="decisionScoreBtn"' in content
    assert 'id="decisionActionBtn"' in content
    assert 'id="shiftPlanHorizon"' in content
    assert 'id="shiftPlanTopK"' in content
    assert 'id="refreshShiftPlanBtn"' in content
    assert 'id="shiftPlanSummary"' in content
    assert 'id="shiftPlanList"' in content
    assert 'id="profileTargetEurH"' in content
    assert 'id="profileConsumption"' in content
    assert 'id="profileRiskAversion"' in content
    assert 'id="profileMaxEta"' in content
    assert 'id="profileSaveBtn"' in content
    assert 'id="profileStatus"' in content
    assert 'aria-live="polite"' in content


def test_pwa_app_binds_shift_plan_action_delegation() -> None:
    content = APP_JS_PATH.read_text(encoding="utf-8")
    assert "bindOfferActionDelegation(shiftPlanEl);" in content


@pytest.mark.skipif(shutil.which("node") is None, reason="node is required for syntax validation")
def test_pwa_js_parses_with_node_check() -> None:
    proc = subprocess.run(
        ["node", "--check", str(APP_JS_PATH)],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert proc.returncode == 0, (
        "Node syntax check failed for PWA app.js.\n"
        f"stdout:\n{proc.stdout}\n"
        f"stderr:\n{proc.stderr}\n"
    )
