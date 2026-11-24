/**
 * Общая логика ленты «Скринер сигналов» — без вёрстки.
 *
 * Сюда вынесено то, что обязано совпадать на десктопе (OiScreenerTable) и на
 * телефоне (MobileOiScreener): состав и порядок строк, калибровка головы
 * кометы, форматы чисел и короткий текст сигнала. Вёрстка у поверхностей своя
 * (таблица 1000px против двухстрочной строки), а лента — одна.
 *
 * EmbedScreener (панель песочницы) пока держит свою копию тех же правил —
 * при изменении методологии синхронизировать и его.
 */
import { t as tt, getLang } from '../../i18n';
import type { OiScreenerRow } from '../../services/api';

export type ScreenerClgroup = 'FIZ' | 'YUR';
export type ScreenerSortKey = 'ratio' | 'skew';
export type ScreenerSortDir = 1 | -1;

// Реальные значения instruments.group (как в пикере активов). Ключи — данные,
// не переводятся; подписи — через t().
export const GROUP_OPTIONS = [
  { key: 'all', label: 'Все категории' },
  { key: 'Индексы', label: 'Индексы' },
  { key: 'Валюта', label: 'Валюта' },
  { key: 'Сырьё', label: 'Сырьё' },
  { key: 'Акции', label: 'Акции' },
  { key: 'Крипто', label: 'Крипто' },
] as const;

export const FAVORITES_KEY = 'favoriteInstruments'; // общий ключ с пикером активов

// Порог визуального акцента числа «Силы» (чёрный вместо серого) — одинаков для
// дня и 2 недель. Не путать с порогом «резко» (sharpThr), от которого зависят
// тексты и алерты.
export const STRENGTH_ACCENT_THR = 2;

export function readFavorites(): string[] {
  try {
    const saved = localStorage.getItem(FAVORITES_KEY);
    return saved ? JSON.parse(saved) : [];
  } catch { return []; }
}

// «4,3×» в русском, «4.3×» в английском.
export function fmtRatio(r: number): string {
  return r.toLocaleString(getLang() === 'en' ? 'en-GB' : 'ru-RU', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + '×';
}

/** «1 актив / 2 актива / 5 активов» */
export function pluralAssets(n: number): string {
  if (getLang() === 'en') return n === 1 ? '1 asset' : `${n} assets`;
  const mod10 = n % 10, mod100 = n % 100;
  if (mod10 === 1 && mod100 !== 11) return `${n} актив`;
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) return `${n} актива`;
  return `${n} активов`;
}

/** Доля стороны перевеса, %: (100 ± net) / 2 — та же точка, где голова кометы. */
export function sideShare(netPct: number): number {
  return Math.round((100 + Math.abs(netPct)) / 2);
}

/** Число «Силы» рисуем акцентным (чёрным), а не серым? Сравниваем ПОКАЗАННОЕ
 *  значение (до 0,1): 1,96 выводится как «2,0×» и не должно быть серым рядом
 *  с чёрным «2,0×». */
export function isStrengthAccent(ratio: number): boolean {
  return Math.round(ratio * 10) / 10 >= STRENGTH_ACCENT_THR;
}

/**
 * Видимые строки ленты: фильтр категории/избранных + сквозная сортировка.
 *
 * Порядок СКВОЗНОЙ по силе. Ни полос по статусу, ни подъёма избранных:
 *  - полосы (sharp → normal → …) ставили тихую sharp-строку выше громкой
 *    обычной, и лента противоречила колонке «Сила»; переворот порядка не
 *    выносил наверх слабейшие — сверху оставалась та же полоса, из-за чего
 *    сортировка по силе читалась как сломанная;
 *  - избранные наверху разрывали ряд по силе в произвольных местах. Для
 *    «только избранные» есть отдельный фильтр ★, он и решает эту задачу.
 * sortDir = −1 → сильные сверху (дефолт).
 * По перекосу: −1 → самый лонговый сверху, +1 → самый шортовый сверху.
 * Строки без значения — в конце ОБЕИХ сторон вручную: через общий ключ
 * (ratio ?? −1) по возрастанию они всплыли бы наверх, и первая страница
 * состояла бы из «мало участников».
 */
export function filterSortScreenerRows(
  rows: OiScreenerRow[],
  opts: {
    group: string;
    onlyFav: boolean;
    favorites: string[];
    sortKey: ScreenerSortKey;
    sortDir: ScreenerSortDir;
  },
): OiScreenerRow[] {
  const { group, onlyFav, favorites, sortKey, sortDir } = opts;
  const favSet = new Set(favorites);
  const out = rows.filter((r) =>
    (group === 'all' || r.group === group) &&
    (!onlyFav || favSet.has(r.sectype)),
  );
  const value = (r: OiScreenerRow) => (sortKey === 'skew' ? r.net_pct : r.ratio);
  return out.sort((a, b) => {
    const va = value(a), vb = value(b);
    if (va == null || vb == null) {
      if (va == null && vb == null) return 0;
      return va == null ? 1 : -1;
    }
    return sortDir * (va - vb);
  });
}

/**
 * Калибровка размера головы кометы: размах ×N sharp-строк по ВИДИМОЙ выборке —
 * фильтр категории меняет и калибровку, строки сравниваются с соседями по
 * экрану. Длина хвоста не нормируется — она геометрическая.
 */
export function sharpRatioRange(visible: OiScreenerRow[]): { ratioLo: number | null; ratioHi: number | null } {
  const sharp = visible.filter((r) => r.status === 'sharp' && r.ratio != null).map((r) => r.ratio!);
  return {
    ratioLo: sharp.length ? Math.min(...sharp) : null,
    ratioHi: sharp.length ? Math.max(...sharp) : null,
  };
}

const periodWord = (period: string): string => {
  switch (period) {
    case 'all': return tt('за всё время');
    case '5y': return tt('за 5 лет');
    case '4y': return tt('за 4 года');
    case '3y': return tt('за 3 года');
    case '2y': return tt('за 2 года');
    case '1y': return tt('за год');
    case '6m': return tt('за полгода');
    default: return tt('за квартал');
  }
};

/**
 * Короткий текст сигнала строки — без подсказок (на телефоне наводить нечем).
 * Приоритет как в таблице: рекорд → глагол резкого движения → служебная
 * пометка. Рекорд перебивает всё: «обычный день» рядом с историческим
 * минимумом читался как противоречие, хотя по важности выигрывает второе.
 * `quiet` — пометка, а не событие: рисуется приглушённо.
 */
export function screenerSignalLabel(r: OiScreenerRow, isMed: boolean): { text: string; quiet: boolean } {
  if (r.net_record) {
    return { text: r.net_record.kind === 'high' ? tt('Ист. макс') : tt('Ист. мин'), quiet: false };
  }
  if (r.record) {
    return { text: `${r.record.kind === 'high' ? tt('Макс') : tt('Мин')} ${periodWord(r.record.period)}`, quiet: false };
  }
  if (r.status === 'sharp' && r.ratio != null && r.direction) {
    // Глагол — по МОДУЛЮ чистой позиции: |net| вырос → «набрали/нарастили»,
    // уменьшился → «сократили» (у шорт-стороны рост net = сокращение шорта).
    const netLong = r.net >= 0;
    const grewExposure = netLong === (r.direction === 'up');
    const text = netLong
      ? (grewExposure ? tt('Набрали лонг') : tt('Сократили лонг'))
      : (grewExposure ? tt('Нарастили шорт') : tt('Сократили шорт'));
    return { text, quiet: false };
  }
  const text = r.status === 'normal'
    ? (isMed ? tt('обычные 2 недели') : tt('обычный день'))
    : r.status === 'illiquid' ? tt('мало участников') : tt('мало истории');
  return { text, quiet: true };
}
