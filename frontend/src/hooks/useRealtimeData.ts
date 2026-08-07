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

  // Всегда держим АКТУАЛЬНЫЙ refetch/sources в ref. Без этого debounced-таймер от
  // прошлого SSE-события через ~debounceMs звал бы СТАРОЕ замыкание refetch (старая
  // категория/фильтр, если их сменили внутри окна debounce) → рефетч старых данных
  // + инвалидация свежего in-flight запроса (через reqId-guard потребителя). Это и
  // давало «переключил категорию — график не обновился до рефреша» (изредка, везде).
  const refetchRef = useRef(refetch);
  refetchRef.current = refetch;
  const sourcesRef = useRef(sources);
  sourcesRef.current = sources;

  useEffect(() => {
    if (!lastEvent) return;
    if (!sourcesRef.current.includes(lastEvent.source)) return;

    // Debounce — если несколько СОВПАВШИХ событий приходят быстро, refetch один раз.
    clearTimeout(timerRef.current);
    timerRef.current = setTimeout(() => {
      refetchRef.current();
    }, debounceMs);

    // ⚠️ Cleanup здесь НЕ возвращаем: он выполнялся и когда СЛЕДУЮЩЕЕ событие
    // не совпадает с подпиской (ранний return выше), гася уже запланированный
    // рефетч. Пачка NOTIFY разных источников подряд (5min+breadth+buffett за
    // секунду) оставляла рефетч только панелям ПОСЛЕДНЕГО события — остальные
    // молча теряли обновление (замер на проде 2026-08-07). Таймер на unmount
    // гасит отдельный эффект ниже.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [lastEvent]);

  // Гарантированно гасим pending-таймер при unmount.
  useEffect(() => () => clearTimeout(timerRef.current), []);
}
