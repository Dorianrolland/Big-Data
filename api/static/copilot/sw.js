const CACHE_NAME = 'fleetstream-copilot-v6';
const STATIC_ASSETS = [
  '/copilot',
  '/static/copilot/index.html',
  '/static/copilot/app.js',
  '/static/copilot/manifest.webmanifest',
];

// API paths that must NEVER be served from cache (realtime data)
const REALTIME_PREFIXES = [
  '/copilot/driver/',
  '/copilot/score-offer',
  '/copilot/health',
  '/copilot/fleet/',
  '/livreurs',
  '/stats',
  '/health',
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

  // Never cache realtime API endpoints
  if (isRealtime(event.request.url)) return;

  // Network-first for static assets with cache fallback (offline UX)
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
              `<!DOCTYPE html><html lang="fr"><head><meta charset="utf-8"><title>Hors ligne</title>
              <meta name="viewport" content="width=device-width,initial-scale=1">
              <style>body{font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#0f172a;color:#f8fafc}
              .box{text-align:center;padding:2rem}.icon{font-size:4rem}.h{font-size:1.4rem;margin:.5rem 0}.sub{color:#94a3b8;font-size:.9rem}</style>
              </head><body><div class="box"><div class="icon">🛵</div>
              <div class="h">FleetStream hors ligne</div>
              <div class="sub">Reconnectez-vous pour utiliser le Copilot livreur en temps réel.</div>
              <br><button onclick="location.reload()" style="padding:.5rem 1.2rem;border-radius:.4rem;border:none;background:#22c55e;color:#fff;cursor:pointer;font-size:1rem">Réessayer</button>
              </div></body></html>`,
              { headers: { 'Content-Type': 'text/html; charset=utf-8' } }
            )
        )
      )
  );
});

// Notify clients when network is restored
self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'SKIP_WAITING') self.skipWaiting();
});
