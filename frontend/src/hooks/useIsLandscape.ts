/**
 * useIsLandscape — реактивная ориентация viewport'а через
 * matchMedia('(orientation: landscape)').
 *
 * Почему MQL, а не resize/orientationchange/screen.orientation:
 * - единый источник правды с CSS-блоками `@media (orientation: landscape)`
 *   — JS-логика и CSS переключаются синхронно;
 * - iOS Safari quirk: в момент `orientationchange` window.innerWidth/Height
 *   часто ещё СТАРЫЕ; MQL-событие срабатывает когда media query реально
 *   перевернулась, и в хендлере мы читаем `e.matches`, а не размеры;
 * - screen.orientation появился в iOS только с 16.4.
 *
 * Размеры руками не читаем вообще — финальную геометрию дожёвывают
 * ResizeObserver'ы чартов.
 */
import { useEffect, useState } from 'react';

export function useIsLandscape(): boolean {
  const [isLandscape, setIsLandscape] = useState(
    () => typeof window !== 'undefined' && window.matchMedia('(orientation: landscape)').matches,
  );

  useEffect(() => {
    const mql = window.matchMedia('(orientation: landscape)');
    const handler = (e: MediaQueryListEvent) => setIsLandscape(e.matches);
    mql.addEventListener('change', handler);
    return () => mql.removeEventListener('change', handler);
  }, []);

  return isLandscape;
}
