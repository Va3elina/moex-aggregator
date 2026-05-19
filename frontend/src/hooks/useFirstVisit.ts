/**
 * useFirstVisit — отслеживает посещения страниц через localStorage.
 *
 * Используется для onboarding-туров: при первом заходе на страницу
 * (по ключу типа `oi`, `buffett`) показываем гайд. После закрытия —
 * маркируется как «видел» и больше не показывается автоматически.
 *
 * Реализация: один key per indicator в localStorage:
 *   localStorage['frame_tour_seen_v3:oi'] = '1'
 *
 * Для старых юзеров (зарегистрированных до релиза тура) — тур всё равно
 * сработает, потому что у них в localStorage этого ключа НЕТ. Это и есть
 * способ дотянуть онбординг до текущей аудитории не трогая бэкенд.
 *
 * VERSIONING: bump prefix (`_v2` → `_v3` → ...) когда хотим показать тур
 * повторно ВСЕМ юзерам (после релиза значительных улучшений контента/UX).
 * Старые ключи остаются в localStorage но игнорируются — нативный механизм
 * cache-bust.
 *
 * История версий:
 *   v1 (`frame_tour_seen:`)    — initial release
 *   v2 (`frame_tour_seen_v2:`) — подробные туры с button-by-button
 *   v3 (`frame_tour_seen_v3:`) — авто-переключение режимов в FundsMoney
 *   v4 (`frame_tour_seen_v4:`) — mobile-туры для каждого индикатора
 *
 * Возвращает:
 *   isFirstVisit: true если ключа в localStorage нет
 *   markAsSeen: функция чтобы прямо сейчас пометить как видевшего
 *   resetSeen: для разработки/тестирования — сбросить флаг
 */
import { useCallback, useEffect, useRef, useState } from 'react';

const STORAGE_PREFIX = 'frame_tour_seen_v4:';

export function useFirstVisit(indicatorKey: string) {
  const storageKey = `${STORAGE_PREFIX}${indicatorKey}`;

  // Initial state — читаем localStorage только при mount
  const [isFirstVisit, setIsFirstVisit] = useState<boolean>(() => {
    if (typeof window === 'undefined') return false; // SSR safety
    try {
      return localStorage.getItem(storageKey) !== '1';
    } catch {
      // localStorage может быть недоступен (private mode, quota) — fallback
      return false;
    }
  });

  const markAsSeen = useCallback(() => {
    try {
      localStorage.setItem(storageKey, '1');
    } catch {
      // silent fail — даже если не сохранилось, в этой сессии больше не покажем
    }
    setIsFirstVisit(false);
  }, [storageKey]);

  const resetSeen = useCallback(() => {
    try {
      localStorage.removeItem(storageKey);
    } catch {
      // ignore
    }
    setIsFirstVisit(true);
  }, [storageKey]);

  return { isFirstVisit, markAsSeen, resetSeen };
}

/**
 * useOnboardingTour — единая обёртка для интеграции тура на страницу.
 *
 * Логика:
 *   1. На mount проверяет localStorage. Если ключа нет (первый визит) —
 *      запускает setTimeout 800ms → open=true.
 *   2. autoOpenedRef гарантирует что auto-open сработает РОВНО ОДИН РАЗ
 *      за mount страницы. Раньше был баг на CBR Flows: при close без
 *      markAsSeen, isFirstVisit оставался true, и потенциальный re-render
 *      мог re-fire useEffect → re-open. Теперь это исключено.
 *   3. close(markSeen): если markSeen=true → markAsSeen + close;
 *      если false → close session-only, при следующем заходе тур опять
 *      покажется (новый mount → autoOpenedRef сбросится).
 *
 * Page usage (1 строка вместо 8):
 *   const tour = useOnboardingTour('oi');
 *   <OnboardingTour open={tour.open} onClose={tour.close} steps={...} />
 */
export function useOnboardingTour(
  indicatorKey: string,
  options?: { gatedBy?: string },
) {
  const { isFirstVisit, markAsSeen } = useFirstVisit(indicatorKey);
  const [open, setOpen] = useState(false);
  const autoOpenedRef = useRef(false);

  useEffect(() => {
    // Gating: per-page туры не показываются пока юзер не прошёл вводный
    // (mobile-intro). Это избегает stacking'а двух модалок на первом
    // визите мобилки. Если gatedBy не задан — gate не активен.
    if (options?.gatedBy) {
      try {
        const guardSeen =
          localStorage.getItem(`${STORAGE_PREFIX}${options.gatedBy}`) === '1';
        if (!guardSeen) return;
      } catch {
        // localStorage недоступен → не блокируем, открываем как обычно
      }
    }
    if (isFirstVisit && !autoOpenedRef.current) {
      autoOpenedRef.current = true;
      const t = setTimeout(() => setOpen(true), 800);
      return () => clearTimeout(t);
    }
  }, [isFirstVisit, options?.gatedBy]);

  const close = useCallback(
    (markSeen: boolean) => {
      setOpen(false);
      if (markSeen) markAsSeen();
    },
    [markAsSeen],
  );

  return { open, close };
}
