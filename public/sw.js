// epixel Service Worker — App Shell CacheFirst, API chunks CacheFirst (Phase4), other API NetworkFirst
const CACHE_NAME = "epixel-shell-v1";
const CHUNK_CACHE_NAME = "epixel-chunks-v1";
const SHELL_URLS = [
  "/",
  "/manifest.json",
  "/icons/icon-192.png",
  "/icons/icon-512.png",
  "/icons/icon-180.png",
  "/shared/chunk-codec.js"
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_URLS)).then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_NAME && k !== CHUNK_CACHE_NAME).map((k) => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  const req = event.request;
  if (req.method !== "GET") return;
  const url = new URL(req.url);

  // Only handle same-origin
  if (url.origin !== self.location.origin) return;

  // Never cache WebSocket / socket.io polling
  if (url.pathname.startsWith("/socket.io/")) return;

  // Chunks: CacheFirst with StaleWhileRevalidate (Phase4) — ETag revalidation handled by browser
  if (url.pathname === "/api/chunks") {
    event.respondWith(
      caches.open(CHUNK_CACHE_NAME).then((cache) =>
        cache.match(req).then((cached) => {
          const fetchAndCache = fetch(req)
            .then((res) => {
              if (res && res.ok) {
                cache.put(req, res.clone());
              } else if (res && res.status === 304 && cached) {
                return cached;
              }
              // also cache 404 empty as negative cache for 10s via Cache-Control but store to avoid flood
              // do not cache 404 in SW to avoid stale empties — let server handle
              return res;
            })
            .catch(() => cached || null);
          // CacheFirst: return cached immediately, revalidate in background
          if (cached) {
            // background revalidate
            fetchAndCache.catch(()=>{});
            return cached;
          }
          return fetchAndCache;
        })
      )
    );
    return;
  }

  // Other API / health : NetworkFirst
  if (
    url.pathname.startsWith("/api/") ||
    url.pathname === "/health"
  ) {
    event.respondWith(
      fetch(req)
        .then((res) => res)
        .catch(() => caches.match(req))
    );
    return;
  }

  // Navigations (HTML) : NetworkFirst with cache fallback for offline shell
  if (req.mode === "navigate") {
    event.respondWith(
      fetch(req)
        .then((res) => {
          // optionally cache the fresh index.html
          const copy = res.clone();
          caches.open(CACHE_NAME).then((c) => c.put(req, copy));
          return res;
        })
        .catch(() => caches.match("/") || caches.match("/index.html"))
    );
    return;
  }

  // Static assets (icons, shared, etc.) : CacheFirst / StaleWhileRevalidate
  if (
    url.pathname.startsWith("/icons/") ||
    url.pathname.startsWith("/shared/") ||
    url.pathname === "/manifest.json"
  ) {
    event.respondWith(
      caches.match(req).then((cached) => {
        const fetched = fetch(req)
          .then((res) => {
            if (res && res.ok) {
              caches.open(CACHE_NAME).then((c) => c.put(req, res.clone()));
            }
            return res;
          })
          .catch(() => null);
        return cached || fetched;
      })
    );
    return;
  }

  // Default: try network, fallback to cache
  // (Do not intercept other requests aggressively)
});
