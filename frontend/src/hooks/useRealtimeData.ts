/**
 * Хук для автоматического обновления данных при получении SSE события.
 *
 * Использование:
 *   const loadData = useCallback(async () => { ... }, [deps]);
 *   useRealtimeData(['5min', 'mv_refresh'], loadData);
 *
 * Когда SSE-событие с matching source приходит — вызывается refetch с debounce.
 * Если SSE недоступен — страница работает как раньше (pull-based).
 *
 * Доставка — через subscribeSSEEvents (каждый фрейм), НЕ через lastEvent-стейт
 * useSSE: пачка NOTIFY разных источников за миллисекунды схлопывалась
 * React-батчем в одно последнее событие, и панели промежуточных источников
 * теряли рефетч (замер на проде 2026-08-07: из 5min+breadth+buffett доезжал
 * только buffett). Бонус: компонент больше не рендерится на каждое SSE-событие.
 */

import { useEffect, useRef } from 'react';
import { subscribeSSEEvents } from './useSSE';

export function useRealtimeData(
  sources: string[],
  refetch: () => void,
  debounceMs: number = 2000,
) {
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
    const off = subscribeSSEEvents((e) => {
      if (!sourcesRef.current.includes(e.source)) return;
      // Debounce — несколько СОВПАВШИХ событий подряд → один refetch.
      // Несовпадающие события таймер не трогают.
      clearTimeout(timerRef.current);
      timerRef.current = setTimeout(() => {
        refetchRef.current();
      }, debounceMs);
    });
    return () => { off(); clearTimeout(timerRef.current); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
}
