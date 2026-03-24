// Фрейм PWA Service Worker
const CACHE_NAME = 'frame-v11';
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

// Fetch — стратегии кеширования
self.addEventListener('fetch', (event) => {
    const { request } = event;
    const url = new URL(request.url);

    // SSE стримы — пропускаем, нельзя кешировать бесконечные потоки
    if (url.pathname.startsWith('/api/events/')) {
        return; // Браузер обработает напрямую
    }

    // Auth запросы — пропускаем, браузер обработает напрямую
    if (url.pathname.startsWith('/api/auth/')) {
        return;
    }

    // API запросы — network-first (данные должны быть свежие)
    if (url.pathname.startsWith('/api/')) {
        event.respondWith(
            fetch(request)
                .then((response) => {
                    // Кешируем только успешные GET-запросы (не 403/401)
                    if (request.method === 'GET' && response.ok) {
                        const clone = response.clone();
                        caches.open(CACHE_NAME).then((cache) => cache.put(request, clone));
                    }
                    return response;
                })
                .catch(() => caches.match(request))
        );
        return;
    }

    // Статика (JS, CSS, шрифты, изображения) — network-first
    // Vite генерирует уникальные хэши в именах, но SW cache-first мешает обновлениям
    if (
        url.pathname.match(/\.(js|css|woff2?|png|svg|jpg|webp|ico)$/) ||
        url.hostname === 'fonts.googleapis.com' ||
        url.hostname === 'fonts.gstatic.com'
    ) {
        event.respondWith(
            fetch(request)
                .then((response) => {
                    if (response.ok) {
                        const clone = response.clone();
                        caches.open(CACHE_NAME).then((cache) => cache.put(request, clone));
                    }
                    return response;
                })
                .catch(() => caches.match(request))
        );
        return;
    }

    // HTML навигация — network-first, fallback на кеш
    event.respondWith(
        fetch(request)
            .then((response) => {
                if (response.ok) {
                    const clone = response.clone();
                    caches.open(CACHE_NAME).then((cache) => cache.put(request, clone));
                }
                return response;
            })
            .catch(() => caches.match(request).then((cached) => cached || caches.match('/')))
    );
});
