/**
 * usePrefetchLogos — гарантирует загрузку sprite + manifest при mount страницы.
 *
 * Стратегия:
 *   - 1 fetch: /logos/sprite-manifest.json (~1.5 KB)
 *   - 1 fetch: /logos/sprite.png (~656 KB) — pre-decode в Image()
 *   - SW cache (cache-first для /logos/*) → второй заход instant
 *
 * Запускается на страницах с инструмент-селектором (ОИ, Сезонность).
 * При открытии модалки лого уже в SW cache → 0 сетевых запросов
 * (TickerLogo использует CSS background, не делает отдельных GET).
 *
 * Module-level флаг блокирует дублирующий prefetch.
 */
import { useEffect } from 'react';

let prefetched = false;

export function usePrefetchLogos() {
  useEffect(() => {
    if (prefetched) return;
    prefetched = true;

    const id = setTimeout(() => {
      // Manifest — лёгкий, грузим сразу
      fetch('/logos/sprite-manifest.json')
        .then((r) => (r.ok ? r.json() : null))
        .then((data: { sprite?: string } | null) => {
          if (!data?.sprite) return;
          // Sprite — preload через Image() с low priority в фоне
          const img = new Image();
          img.decoding = 'async';
          img.src = data.sprite;
        })
        .catch(() => {
          // Manifest недоступен — TickerLogo покажет fallback circles
        });
    }, 800);

    return () => clearTimeout(id);
  }, []);
}
