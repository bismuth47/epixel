// epixel Service Worker — App Shell CacheFirst, API NetworkOnly/NetworkFirst
const CACHE_NAME = "epixel-shell-v1";
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
      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
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

  // API / health : NetworkFirst (offline時はキャンバスに入れない仕様なのでフォールバック不要だが、SWが無いと404になるのを防ぐ)
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
