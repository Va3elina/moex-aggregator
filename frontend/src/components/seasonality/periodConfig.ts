/**
 * PeriodConfig — одна серия «Период с YYYY» с индивидуальными настройками.
 *
 * Раньше «Без выбросов» (медиана) и «Без дивидендных гэпов» были глобальными
 * тогглами: медиана вообще считалась по всей истории отдельной серией, игнорируя
 * выбранный период. Теперь каждая серия периода несёт свои флаги — можно держать
 * «С 2000» (среднее) и «С 2000 · медиана» одновременно, отсюда стабильный `id`
 * (а не год) как идентичность серии: дубли года разрешены.
 */
export interface PeriodConfig {
  /** Стабильный уникальный id — идентичность серии (дубли sinceYear разрешены). */
  id: string;
  /** Год, с которого считается сезонность (включительно). */
  sinceYear: number;
  /** «Без выбросов» → aggType: 'median' вместо среднего арифметического. */
  median: boolean;
  /** «Без дивидендных гэпов» → adjusted close + исключение ex-div дней в intraday. */
  excludeDividends: boolean;
}

// Счётчик на уровне модуля — детерминирован между рендерами, без Math.random/Date.now
// (важно: они ломают resume и дают нестабильные React-ключи).
let _pidSeq = 0;
export function makePeriodId(): string {
  return `p${++_pidSeq}`;
}

/**
 * sinceYear-сентинел «с начала истории» — период по умолчанию. resolvePeriods
 * превращает его в первый год данных текущего актива: при смене актива дефолтный
 * период растягивается на всю историю нового инструмента, а явно выбранный год
 * («С 2022 г.») остаётся как есть.
 */
export const FROM_START_YEAR = 0;

export function defaultPeriods(): PeriodConfig[] {
  return [{ id: makePeriodId(), sinceYear: FROM_START_YEAR, median: false, excludeDividends: false }];
}

/** JSON из localStorage без падений (нет ключа / битый формат / private mode → null). */
export function readStoredJson(key: string): unknown {
  if (typeof window === 'undefined') return null;
  try {
    const raw = window.localStorage.getItem(key);
    return raw == null ? null : JSON.parse(raw);
  } catch {
    return null;
  }
}

/**
 * Разбор сохранённых периодов (localStorage / синк настроек). Понимает и старый
 * формат мобилки — голый массив годов. Невалидные записи отбрасываются, id
 * выдаются заново: счётчик модульный и после перезагрузки снова стартует с p1,
 * сохранённые id столкнулись бы с новыми (React-ключи, патчи и удаление по id).
 */
export function parseStoredPeriods(raw: unknown): PeriodConfig[] {
  if (!Array.isArray(raw)) return [];
  const out: PeriodConfig[] = [];
  for (const item of raw) {
    const p = (typeof item === 'number' ? { sinceYear: item } : item) as Partial<PeriodConfig> | null;
    if (!p || typeof p !== 'object' || !Number.isInteger(p.sinceYear)) continue;
    out.push({
      id: makePeriodId(),
      sinceYear: p.sinceYear as number,
      median: p.median === true,
      excludeDividends: p.excludeDividends === true,
    });
  }
  return out;
}

/**
 * Сохранённые периоды → периоды для текущего актива. Выбор не привязан к активу:
 * год зажимается в историю данных (раньше первого года → первый год, позже
 * последнего → последний), «Без дивидендных гэпов» гасится у инструментов без
 * дивидендов (тумблера там нет, выключить было бы нечем). Хранимое значение не
 * трогаем — вернувшись на актив с длинной историей, увидим исходный год.
 * Пока годы актива не загружены — пусто.
 */
export function resolvePeriods(periods: PeriodConfig[], years: number[], hasDividends: boolean): PeriodConfig[] {
  if (years.length === 0) return [];
  const minY = Math.min(...years);
  const maxY = Math.max(...years);
  return periods.map((p) => {
    const sinceYear = Math.min(Math.max(p.sinceYear, minY), maxY);
    const excludeDividends = p.excludeDividends && hasDividends;
    return sinceYear === p.sinceYear && excludeDividends === p.excludeDividends
      ? p
      : { ...p, sinceYear, excludeDividends };
  });
}
