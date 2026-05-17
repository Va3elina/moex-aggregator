/**
 * useFirstVisit — отслеживает посещения страниц через localStorage.
 *
 * Используется для onboarding-туров: при первом заходе на страницу
 * (по ключу типа `oi`, `buffett`) показываем гайд. После закрытия —
 * маркируется как «видел» и больше не показывается автоматически.
 *
 * Реализация: один key per indicator в localStorage:
 *   localStorage['frame_tour_seen:oi'] = '1'
 *
 * Для старых юзеров (зарегистрированных до релиза тура) — тур всё равно
 * сработает, потому что у них в localStorage этого ключа НЕТ. Это и есть
 * способ дотянуть онбординг до текущей аудитории не трогая бэкенд.
 *
 * Возвращает:
 *   isFirstVisit: true если ключа в localStorage нет
 *   markAsSeen: функция чтобы прямо сейчас пометить как видевшего
 *   resetSeen: для разработки/тестирования — сбросить флаг
 */
import { useCallback, useState } from 'react';

const STORAGE_PREFIX = 'frame_tour_seen:';

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
