const CACHE_NAME = 'fleetstream-driver-copilot-v3';
const STATIC_ASSETS = [
  '/copilot/driver-app',
  '/static/driver_app/index.html',
  '/static/driver_app/app.js',
  '/static/driver_app/manifest.webmanifest',
];

const REALTIME_PREFIXES = [
  '/copilot/driver/',
  '/copilot/route',
  '/copilot/health',
];

function isRealtime(url) {
  const path = new URL(url).pathname;
  return REALTIME_PREFIXES.some((prefix) => path.startsWith(prefix));
}

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(STATIC_ASSETS)).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) => Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;
  if (isRealtime(event.request.url)) return;

  event.respondWith(
    fetch(event.request)
      .then((response) => {
        if (response.ok) {
          const copy = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(event.request, copy));
        }
        return response;
      })
      .catch(() =>
        caches.match(event.request).then(
          (cached) =>
            cached ||
            new Response(
              '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Offline</title><style>body{margin:0;background:#111;color:#f4f1e8;font-family:Arial,sans-serif;display:grid;place-items:center;height:100vh}.box{padding:2rem;text-align:center}button{padding:.75rem 1.1rem;border-radius:999px;border:none;background:#ffb100;color:#111;font-weight:700;cursor:pointer}</style></head><body><div class="box"><h1>Courier Copilot is offline</h1><p>Reconnect to refresh the live courier briefing.</p><button onclick="location.reload()">Retry</button></div></body></html>',
              { headers: { 'Content-Type': 'text/html; charset=utf-8' } }
            )
        )
      )
  );
});
