const CACHE_NAME = 'fleetstream-copilot-v2';
const ASSETS = [
  '/copilot',
  '/static/copilot/index.html',
  '/static/copilot/app.js',
  '/static/copilot/manifest.webmanifest',
];

self.addEventListener('install', (event) => {
  event.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.addAll(ASSETS)));
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) => Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))))
  );
});

self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;

  const url = new URL(event.request.url);
  const isCopilotApi = url.origin === self.location.origin && url.pathname.startsWith('/copilot/') && url.pathname !== '/copilot';
  if (isCopilotApi) {
    // Copilot API responses should stay realtime and never come from cache.
    return;
  }

  event.respondWith(
    fetch(event.request)
      .then((response) => {
        const copy = response.clone();
        caches.open(CACHE_NAME).then((cache) => cache.put(event.request, copy));
        return response;
      })
      .catch(() => caches.match(event.request))
  );
});
