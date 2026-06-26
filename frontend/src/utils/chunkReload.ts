/**
 * Восстановление после сбоя загрузки code-split чанка.
 *
 * После деплоя hashed-имена чанков меняются. Долго открытая вкладка (или запрос,
 * попавший в старый контейнер во время recreate) ссылается на чанк, которого
 * новый билд уже не содержит → dynamic import падает с 404 («Failed to fetch
 * dynamically imported module»). Лечится разовой перезагрузкой: index.html отдаётся
 * с no-cache, поэтому свежий load подтягивает актуальный манифест чанков.
 *
 * Единый guard (timestamp в sessionStorage) делят два источника сбоя — событие
 * Vite `vite:preloadError` и ErrorBoundary. Если перезагрузка только что была, а
 * чанк всё равно не грузится — деплой реально сломан, дальше не зацикливаемся,
 * пусть ошибка всплывёт в UI.
 */
const RELOAD_AT_KEY = 'frame:chunk-reload-at';
const RELOAD_COOLDOWN_MS = 30_000;

/** Похоже ли исключение на сбой динамического импорта чанка (формулировки разнятся
    по браузерам: Chrome / Firefox / Safari). */
export function isChunkLoadError(error: unknown): boolean {
  const msg =
    error instanceof Error ? error.message : typeof error === 'string' ? error : '';
  return /dynamically imported module|importing a module script failed|error loading dynamically imported/i.test(
    msg,
  );
}

/**
 * Разово перезагрузить вкладку для подтягивания свежего билда.
 * @returns true — перезагрузка инициирована; false — недавно уже пробовали
 *          (cooldown не истёк), вызывающему стоит показать ошибку.
 */
export function reloadOnceForChunk(): boolean {
  try {
    const last = Number(sessionStorage.getItem(RELOAD_AT_KEY) || '0');
    if (Date.now() - last < RELOAD_COOLDOWN_MS) return false;
    sessionStorage.setItem(RELOAD_AT_KEY, String(Date.now()));
  } catch {
    /* sessionStorage недоступен — всё равно пробуем перезагрузиться один раз */
  }
  window.location.reload();
  return true;
}
