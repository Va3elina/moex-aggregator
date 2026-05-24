/**
 * useRecent — localStorage-based трекинг последних посещённых страниц.
 *
 * Зачем: на overview юзер видит "Недавно смотрел" блок с быстрым возвратом
 * к последнему контексту (актив + индикатор + параметры). Это retention-
 * фича — не надо снова искать тикер в search и заново настраивать.
 *
 * Storage: localStorage key `frame_recent_v1`. Дедупликация по path.
 * Хранится последние 10 — дальше FIFO drop oldest.
 *
 * Дизайн без backend — намеренно. Recent activity это per-device концепция
 * (юзер на телефоне смотрит heatmap, на laptop'е seasonality — не нужно
 * мешать). При смене устройства просто пустой recent — приемлемая UX.
 */
import { useCallback, useEffect, useState } from 'react';

const STORAGE_KEY = 'frame_recent_v1';
const MAX_ITEMS = 10;

export interface RecentEntry {
  /** Уникальный идентификатор для дедупликации (обычно URL pathname+search). */
  path: string;
  /** Что отображается в UI карточки — "Сезонность SBER (месяцы)". */
  label: string;
  /** Тип для иконки в UI: 'indicator' / 'asset' / 'screener'. */
  kind: 'indicator' | 'asset' | 'screener';
  /** Опционально — тикер (для отображения сектора-иконки). */
  ticker?: string;
  /** Опционально — ID индикатора ('heatmap', 'seasonality', ...). */
  indicatorId?: string;
  /** Unix timestamp (ms) когда добавили. */
  at: number;
}

function readStorage(): RecentEntry[] {
  if (typeof window === 'undefined') return [];
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter(
      (x): x is RecentEntry =>
        x && typeof x.path === 'string' && typeof x.label === 'string' && typeof x.at === 'number',
    );
  } catch {
    return [];
  }
}

function writeStorage(entries: RecentEntry[]): void {
  if (typeof window === 'undefined') return;
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(entries));
    // Notify other tabs/windows — useRecent в overview обновится без reload.
    window.dispatchEvent(new CustomEvent('frame:recent-updated'));
  } catch {
    /* localStorage может быть disabled (приватный режим Safari) — silently ignore */
  }
}

/**
 * Add entry to recent. Dedupe by path (если уже есть с тем же path, обновляем at).
 * Trim до MAX_ITEMS — drop oldest.
 */
export function pushRecent(entry: Omit<RecentEntry, 'at'>): void {
  const existing = readStorage();
  const filtered = existing.filter((e) => e.path !== entry.path);
  const next: RecentEntry[] = [{ ...entry, at: Date.now() }, ...filtered].slice(0, MAX_ITEMS);
  writeStorage(next);
}

/**
 * Hook для чтения recent списка (с реактивностью на изменения).
 * Используется в RecentWidget на overview.
 */
export function useRecent(): RecentEntry[] {
  const [entries, setEntries] = useState<RecentEntry[]>(() => readStorage());

  useEffect(() => {
    const reload = () => setEntries(readStorage());
    window.addEventListener('frame:recent-updated', reload);
    // Cross-tab sync через storage event.
    const onStorage = (e: StorageEvent) => {
      if (e.key === STORAGE_KEY) reload();
    };
    window.addEventListener('storage', onStorage);
    return () => {
      window.removeEventListener('frame:recent-updated', reload);
      window.removeEventListener('storage', onStorage);
    };
  }, []);

  return entries;
}

/**
 * Clear all recent. Удобно для "Очистить историю" в UI.
 */
export function clearRecent(): void {
  writeStorage([]);
}

/**
 * Track-helper для use в indicator pages. Вызывается mount-once.
 *
 * Пример:
 *   useTrackRecent({
 *     kind: 'indicator',
 *     indicatorId: 'seasonality',
 *     label: `Сезонность ${selectedName}`,
 *     ticker: selectedStock,
 *   });
 */
export function useTrackRecent(entry: Omit<RecentEntry, 'at' | 'path'> & { path?: string }): void {
  // path по default — текущий location.pathname + search.
  const path = entry.path ?? (typeof window !== 'undefined' ? window.location.pathname + window.location.search : '');
  // Memoize key для useEffect — не запускаем повторно если props те же.
  const key = JSON.stringify({ ...entry, path });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const stableCallback = useCallback(() => {
    if (!path) return;
    pushRecent({ ...entry, path });
  }, [key]);

  useEffect(() => {
    stableCallback();
  }, [stableCallback]);
}
