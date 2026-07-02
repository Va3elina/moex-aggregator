import { useSyncExternalStore } from 'react';

/**
 * Глобальные настройки графиков — ОДНО место, действует на весь сайт, живёт
 * в localStorage. В отличие от useChartPalette (CSS-переменные через атрибут
 * на <html>), тип графика читают React-компоненты → нужен общий стор с
 * подпиской: модалка ChartSettings меняет, все SimpleChart на странице
 * перерисовываются сразу. Синк между вкладками — через событие 'storage'.
 *
 * Типы серий:
 *  - 'line' | 'area' | 'columns' — применимы к ЛЮБОМУ ряду {time, value};
 *  - 'candles' | 'bars' | 'heikin' — требуют OHLC в данных; где OHLC нет,
 *    SimpleChart сам откатывается на линию (график никогда не пустой).
 */
export type ChartSeriesType = 'line' | 'area' | 'columns' | 'candles' | 'bars' | 'heikin';

export const OHLC_TYPES: ReadonlyArray<ChartSeriesType> = ['candles', 'bars', 'heikin'];

const ALL_TYPES: ReadonlyArray<ChartSeriesType> = ['line', 'area', 'columns', 'candles', 'bars', 'heikin'];

/** К чему применять тип: только основная серия или все серии графика
 *  (area/columns красят и вторичные линии; OHLC-типы всё равно только там,
 *  где есть OHLC — вторичные серии остаются линиями). */
export type ChartTypeScope = 'primary' | 'all';

const TYPE_KEY = 'frame:chart:type';
const DASH_KEY = 'frame:chart:dash';
const SCOPE_KEY = 'frame:chart:scope';
/** До v2 тип был per-page только на ОИ (usePersistedState → JSON-строка). */
const LEGACY_OI_TYPE_KEY = 'frame:oi:chartType';

export interface ChartSettingsState {
  type: ChartSeriesType;
  scope: ChartTypeScope;
  /** Штрих-режим: вторая/третья линия пунктиром — различие ФОРМОЙ, не цветом
   *  (полная монохромность, которую не закрыть никакой палитрой). Управляется
   *  карточкой «Монохром + штрих» в секции «Палитра». */
  dash: boolean;
}

function read(): ChartSettingsState {
  let type: ChartSeriesType = 'line';
  let scope: ChartTypeScope = 'primary';
  let dash = false;
  try {
    const t = localStorage.getItem(TYPE_KEY) as ChartSeriesType | null;
    if (t && ALL_TYPES.includes(t)) {
      type = t;
    } else {
      const legacy = localStorage.getItem(LEGACY_OI_TYPE_KEY);
      if (legacy === '"area"' || legacy === 'area') type = 'area';
    }
    if (localStorage.getItem(SCOPE_KEY) === 'all') scope = 'all';
    dash = localStorage.getItem(DASH_KEY) === '1';
  } catch { /* private mode / SSR — дефолты */ }
  return { type, scope, dash };
}

let state: ChartSettingsState = read();
const listeners = new Set<() => void>();

function emit(): void {
  listeners.forEach((l) => l());
}

function subscribe(cb: () => void): () => void {
  listeners.add(cb);
  return () => listeners.delete(cb);
}

function getSnapshot(): ChartSettingsState {
  return state;
}

export function setChartType(t: ChartSeriesType): void {
  if (t === state.type) return;
  state = { ...state, type: t };
  try { localStorage.setItem(TYPE_KEY, t); } catch { /* не критично */ }
  emit();
}

export function setChartDash(dash: boolean): void {
  if (dash === state.dash) return;
  state = { ...state, dash };
  try { localStorage.setItem(DASH_KEY, dash ? '1' : '0'); } catch { /* не критично */ }
  emit();
}

export function setChartScope(scope: ChartTypeScope): void {
  if (scope === state.scope) return;
  state = { ...state, scope };
  try { localStorage.setItem(SCOPE_KEY, scope); } catch { /* не критично */ }
  emit();
}

if (typeof window !== 'undefined') {
  window.addEventListener('storage', (e) => {
    if (e.key === TYPE_KEY || e.key === DASH_KEY || e.key === SCOPE_KEY || e.key === null) {
      state = read();
      emit();
    }
  });
}

export function useChartSettings(): ChartSettingsState & {
  setType: (t: ChartSeriesType) => void;
  setDash: (d: boolean) => void;
  setScope: (s: ChartTypeScope) => void;
} {
  const s = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
  return { ...s, setType: setChartType, setDash: setChartDash, setScope: setChartScope };
}
