// Фрейм PWA Service Worker
const CACHE_NAME = 'frame-v591';
const STATIC_ASSETS = [
    '/',
    '/manifest.json',
    '/icon-192.svg',
    '/icon-512.svg',
];

// Install — кешируем статику
self.addEventListener('install', (event) => {
    event.waitUntil(
        caches.open(CACHE_NAME).then((cache) => cache.addAll(STATIC_ASSETS))
    );
    self.skipWaiting();
});

// Activate — удаляем старые кеши
self.addEventListener('activate', (event) => {
    event.waitUntil(
        caches.keys().then((keys) =>
            Promise.all(
                keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k))
            )
        )
    );
    self.clients.claim();
});

// Безопасное кэширование — игнорирует ошибки cache.put()
function safeCachePut(request, response) {
    caches.open(CACHE_NAME).then((cache) => cache.put(request, response)).catch(() => {});
}

// Fetch — стратегии кеширования
self.addEventListener('fetch', (event) => {
    const { request } = event;
    const url = new URL(request.url);

    // SSE и Auth — пропускаем
    if (url.pathname.startsWith('/api/events/') || url.pathname.startsWith('/api/auth/')) {
        return;
    }

    // API — всегда сеть, без кэширования (данные обновляются каждые 5 мин)
    if (url.pathname.startsWith('/api/')) {
        event.respondWith(fetch(request));
        return;
    }

    // Логотипы тикеров — cache-first. Почти статика, не меняется месяцами.
    // Модалка выбора актива рендерит ~100 <img> сразу → без кэша 100 запросов
    // в сеть за секунду → rate limit. После первого захода всё из SW cache.
    if (url.pathname.startsWith('/logos/')) {
        event.respondWith(
            caches.match(request).then((cached) => {
                if (cached) return cached;
                return fetch(request).then((response) => {
                    if (response.ok) safeCachePut(request, response.clone());
                    return response;
                }).catch(() => cached); // если сеть упала — вернём что было
            })
        );
        return;
    }

    // Статика и навигация — network-first с fallback на кэш
    event.respondWith(
        fetch(request)
            .then((response) => {
                if (response.ok) {
                    safeCachePut(request, response.clone());
                }
                return response;
            })
            .catch(() => caches.match(request).then((cached) => cached || caches.match('/')))
    );
});
