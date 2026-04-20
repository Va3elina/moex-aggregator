/**
 * Хук для автоматического обновления данных при получении SSE события.
 *
 * Использование:
 *   const loadData = useCallback(async () => { ... }, [deps]);
 *   useRealtimeData(['5min', 'mv_refresh'], loadData);
 *
 * Когда SSE-событие с matching source приходит — вызывается refetch с debounce.
 * Если SSE недоступен — страница работает как раньше (pull-based).
 */

import { useEffect, useRef } from 'react';
import { useSSE } from './useSSE';

export function useRealtimeData(
  sources: string[],
  refetch: () => void,
  debounceMs: number = 2000,
) {
  const { lastEvent } = useSSE();
  const timerRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  useEffect(() => {
    if (!lastEvent) return;
    if (!sources.includes(lastEvent.source)) return;

    // Debounce — если несколько событий приходят быстро, refetch один раз
    clearTimeout(timerRef.current);
    timerRef.current = setTimeout(() => {
      refetch();
    }, debounceMs);

    return () => clearTimeout(timerRef.current);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [lastEvent]);
}
