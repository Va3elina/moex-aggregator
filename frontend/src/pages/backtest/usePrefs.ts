/* eslint-disable @typescript-eslint/no-explicit-any */
// Стенд: «как я оставил». Всё состояние интерфейса — один объект: сразу в localStorage (мгновенно при загрузке)
// и с задержкой на сервер (раскладка default) — чтобы настройки ехали между устройствами и агент мог собрать
// раскладку под задачу: /admin/backtest?ws=<имя>.
import { useCallback, useEffect, useRef, useState } from 'react';
import { btApi } from './api';

export type IndKind = 'sma' | 'ema' | 'wma' | 'bb' | 'rsi' | 'atr';
export interface IndCfg { id: string; kind: IndKind; length: number; mult?: number; color: string; hidden?: boolean }
export interface Cell { st: string; tf: number }
export type Layout = '1' | '2h' | '2v' | '4';
export interface SavedRule { name: string; code: string }
export interface Prefs {
  layout: Layout; cells: Cell[]; active: number; runId: number | null; compareRunId: number | null;
  panel: 'open' | 'closed' | 'max'; panelH: number;
  view: 'overview' | 'trades' | 'signals' | 'checks' | 'robot'; scope: 'symbol' | 'portfolio';
  resultTab: string; tradeTab: string; bucket: string; calKind: string; hidden: string[];
  indicators: IndCfg[]; show: { markers: boolean; lines: boolean; window: boolean; robot: boolean; volume: boolean; labels: boolean };
  recent: string[]; rules: SavedRule[]; editor: { code: string; props: Record<string, any> } | null; tradesOnlyExecuted: boolean;
}
export const DEFAULTS: Prefs = {
  layout: '1', cells: [{ st: 'SS', tf: 5 }, { st: 'Si', tf: 5 }, { st: 'SR', tf: 5 }, { st: 'MX', tf: 5 }], active: 0,
  runId: null, compareRunId: null, panel: 'open', panelH: 360, view: 'overview', scope: 'symbol',
  resultTab: 'dist', tradeTab: 'dist', bucket: 'month', calKind: 'weekday', hidden: [],
  indicators: [], show: { markers: true, lines: true, window: true, robot: true, volume: true, labels: true },
  recent: [], rules: [], editor: null, tradesOnlyExecuted: false,
};
const KEY = 'bt:prefs:v2';

function merge(x: any): Prefs {
  const p = { ...DEFAULTS, ...(x ?? {}) } as Prefs;
  p.show = { ...DEFAULTS.show, ...(x?.show ?? {}) };
  p.cells = DEFAULTS.cells.map((c, i) => ({ ...c, ...(x?.cells?.[i] ?? {}) }));
  return p;
}

export function usePrefs(wsName: string | null) {
  const [prefs, setPrefs] = useState<Prefs>(() => {
    try { return merge(JSON.parse(localStorage.getItem(KEY) ?? 'null')); } catch { return DEFAULTS; }
  });
  const had = useRef(!!localStorage.getItem(KEY));
  const loaded = useRef(false);

  useEffect(() => {                                   // раскладка из ссылки — всегда; default с сервера — только на новом устройстве
    const name = wsName ?? (had.current ? null : 'default');
    if (!name) { loaded.current = true; return; }
    btApi.workspace(name).then(w => setPrefs(merge(w.data))).catch(() => undefined).finally(() => { loaded.current = true; });
  }, [wsName]);

  useEffect(() => {
    try { localStorage.setItem(KEY, JSON.stringify(prefs)); } catch { /* приватный режим */ }
    if (!loaded.current) return;
    const t = setTimeout(() => { btApi.saveWorkspace('default', prefs).catch(() => undefined); }, 2000);
    return () => clearTimeout(t);
  }, [prefs]);

  const set = useCallback((patch: Partial<Prefs> | ((p: Prefs) => Partial<Prefs>)) =>
    setPrefs(p => ({ ...p, ...(typeof patch === 'function' ? patch(p) : patch) })), []);
  return [prefs, set] as const;
}
