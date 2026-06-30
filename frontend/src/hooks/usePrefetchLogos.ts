/**
 * usePrefetchLogos — прогревает реестр логотипов в SW cache при mount страницы.
 *
 * Грузит ТОЛЬКО /logos/sprite-manifest.json (~1.5 KB) — реестр «у какого
 * тикера есть лого». Сам спрайт (sprite.png, ~1.2 MB) НЕ предзагружается:
 * после рефактора TickerLogo рендерит индивидуальные PNG по требованию
 * (<img loading="lazy">), а спрайт нигде не отображается (см. TickerLogo.tsx).
 * Раньше тут был ещё фоновый preload+decode спрайта — это была чистая трата
 * сети и CPU, картинку качали и выбрасывали.
 *
 * Манифест попадает в SW cache (cache-first для /logos/*), и singleton-fetch
 * в TickerLogo потом читает его уже из кэша.
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
      // Только лёгкий манифест-реестр — прогрев SW cache. Спрайт не трогаем.
      fetch('/logos/sprite-manifest.json').catch(() => {
        // Manifest недоступен — TickerLogo покажет fallback circles
      });
    }, 800);

    return () => clearTimeout(id);
  }, []);
}
