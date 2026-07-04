/**
 * useInstrumentFilter — переиспользуемая фильтр/группировка-логика пикера активов.
 *
 * Вынесена из дублирующегося кода InstrumentSearchModal (desktop) и
 * MobileAssetSearch (mobile): фильтрация по поиску/категории/типу + дедупликация
 * по sectype + (опц.) сортировка + разбивка на «избранное / остальное».
 *
 * Чистая (без сайд-эффектов), мемоизированная по входам. Generic по форме
 * элемента — пригодна не только для Instrument'ов, но и для группировки
 * любых объектов с {sectype, name, group} по сектору (напр. алертов в кабинете).
 *
 * Все поведенческие различия двух модалок вынесены в options, чтобы рефактор
 * НЕ менял их внешний UI/поведение:
 *   - matchType        — учитывать ли inst.type при матче категории (mobile=да)
 *   - extraFilter      — доп. предикат (mobile: отсечь futures в режиме no-futures)
 *   - dedup            — стратегия тай-брейка дубликатов sectype (desktop=по объёму)
 *   - sort             — компаратор финального списка (desktop=по колонке)
 */
import { useMemo } from 'react';

/** Минимальная форма элемента, по которой работают фильтр/группировка. */
export interface FilterableInstrument {
  sectype: string;
  name: string;
  type?: string | null;
  group?: string | null;
  daily_volume?: number;
  day_change_pct?: number | null;
}

export interface UseInstrumentFilterOptions<T extends FilterableInstrument> {
  /** Список инструментов (может содержать дубликаты sectype — серии контрактов). */
  instruments: T[];
  /** Строка поиска (матч по sectype ИЛИ name, case-insensitive). */
  searchQuery: string;
  /** Активная категория-чип: 'all' | 'Акции' | 'Валюта' | 'futures' | ... */
  categoryFilter: string;
  /** Избранные sectype (из localStorage) — для разбивки на favorites/regular. */
  favorites: string[];
  /** Исключить элементы с этим type (excludeType из props модалки). */
  excludeType?: string;
  /**
   * Учитывать ли `inst.type === categoryFilter` при матче категории.
   * Desktop — нет (только group + 'Акции'→stock fallback);
   * mobile — да (категория-чип 'futures' матчится по type).
   */
  matchType?: boolean;
  /**
   * Доп. предикат-фильтр поверх поиска/категории/excludeType. Возврат false →
   * элемент отсеивается. Mobile использует для отсечения futures в no-futures.
   */
  extraFilter?: (inst: T) => boolean;
  /**
   * Тай-брейк при дубликатах sectype: вернуть true, если `candidate` должен
   * вытеснить уже найденный `existing`. По умолчанию — first-wins (mobile):
   * первый встреченный остаётся. Desktop передаёт объём/change-aware стратегию.
   */
  dedup?: (candidate: T, existing: T) => boolean;
  /**
   * Компаратор для финальной сортировки уникального списка. По умолчанию —
   * preserve order (mobile: сортировка уже на бэкенде). Desktop сортирует по
   * выбранной колонке.
   */
  sort?: (a: T, b: T) => number;
}

export interface UseInstrumentFilterResult<T extends FilterableInstrument> {
  /** Отфильтрованный + дедуплицированный (+ отсортированный) список. */
  unique: T[];
  /** Избранные из unique (пусто при активном поиске — избранные не прячутся). */
  favoriteItems: T[];
  /** Остальные из unique (при поиске — весь unique, избранные не отделяются). */
  regularItems: T[];
}

/**
 * Матч категории. group — основной путь (backend проставляет
 * 'Акции'/'Валюта'/'Индексы'/'Сырьё'). 'Акции'→type==='stock' — fallback для
 * акций без заполненной group (видели такие на проде). matchType добавляет
 * прямой матч по type (mobile: чип 'futures').
 */
function matchesCategory(
  group: string | null | undefined,
  type: string | null | undefined,
  categoryFilter: string,
  matchType: boolean,
): boolean {
  if (categoryFilter === 'all') return true;
  if (group === categoryFilter) return true;
  if (matchType && type === categoryFilter) return true;
  if (categoryFilter === 'Акции' && type === 'stock') return true;
  return false;
}

export function useInstrumentFilter<T extends FilterableInstrument>(
  opts: UseInstrumentFilterOptions<T>,
): UseInstrumentFilterResult<T> {
  const {
    instruments,
    searchQuery,
    categoryFilter,
    favorites,
    excludeType,
    matchType = false,
    extraFilter,
    dedup,
    sort,
  } = opts;

  return useMemo(() => {
    const q = searchQuery.toLowerCase();

    // 1) Фильтрация: excludeType → extraFilter → поиск → категория.
    const filtered = instruments.filter((inst) => {
      if (excludeType && inst.type === excludeType) return false;
      if (extraFilter && !extraFilter(inst)) return false;
      const matchesSearch =
        !searchQuery ||
        inst.sectype.toLowerCase().includes(q) ||
        inst.name.toLowerCase().includes(q);
      return matchesSearch && matchesCategory(inst.group, inst.type, categoryFilter, matchType);
    });

    // 2) Дедупликация по sectype. dedup определяет, кто выживает при коллизии
    //    (default: first-wins). Индекс хранится в Map для O(1) замены.
    const indexBySectype = new Map<string, number>();
    const deduped: T[] = [];
    for (const inst of filtered) {
      const existingIdx = indexBySectype.get(inst.sectype);
      if (existingIdx === undefined) {
        indexBySectype.set(inst.sectype, deduped.length);
        deduped.push(inst);
      } else if (dedup && dedup(inst, deduped[existingIdx])) {
        deduped[existingIdx] = inst;
      }
    }

    // 3) Сортировка (опц.) — НЕ мутируем deduped (slice), хотя он уже локальный.
    const unique = sort ? [...deduped].sort(sort) : deduped;

    // 4) Разбивка на favorites/regular. При активном поиске избранные НЕ
    //    отделяются (все в одном списке) — как в обоих модалках.
    const favSet = new Set(favorites);
    const favoriteItems = searchQuery ? [] : unique.filter((i) => favSet.has(i.sectype));
    const regularItems = searchQuery ? unique : unique.filter((i) => !favSet.has(i.sectype));

    return { unique, favoriteItems, regularItems };
  }, [instruments, searchQuery, categoryFilter, favorites, excludeType, matchType, extraFilter, dedup, sort]);
}
