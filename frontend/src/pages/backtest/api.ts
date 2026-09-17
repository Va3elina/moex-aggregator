/* eslint-disable @typescript-eslint/no-explicit-any -- результат прогона и правило приходят как свободный JSON движка */
// Стенд: клиент /api/admin/bt. Отдельный файл, чтобы не раздувать общий services/api.ts (сайт не трогаем).
import { apiFetch } from '../../services/api';

const BASE = ((import.meta.env.VITE_API_BASE as string | undefined) ?? '') + '/api/admin/bt';

export interface BtRule { id: string; name: string; frozen: boolean; universe: string[]; rule: any }
export interface BtMeta {
  rules: BtRule[];
  instruments: { st: string; name: string }[];
  tariffs: Record<string, number>;
  spread_daily?: boolean;
  exec: Record<string, string>;
  go: Record<string, string>;
}
export interface BtRun {
  id: number; name: string | null; status: 'queued' | 'running' | 'done' | 'error';
  created_at: string; finished_at: string | null; error: string | null;
  spec: any; spec_full?: any; result?: any; summary?: Record<string, number | null>;
}
export interface BtTrade {
  st: string; d: string; secid: string; side: number; move: number; thr: number | null;
  px_in: number; d_out: string; px_out: number; gross: number; comm: number; spread: number; net: number;
  qty: number | null; notional: number | null; go: number | null; equity_in: number | null;
  comm_rub: number | null; spread_rub: number | null; pnl_rub: number | null; account_skip: string | null; go_cut?: boolean | null;
}
export interface BtEquity { d: string; equity: number; positions: number; notional: number; go_used: number; margin_call: number | null }
export interface BtRealism { st: string; asset: string; broker_coef: number; go_rate: [string, number][]; rub_per_point: [string, number][]; rub_per_point_const: number; spread: [string, number][]; spread_const: number }
export interface BtSignal {
  st: string; d: string; secid: string; pa: number; pb: number; move: number; thr_up: number | null; thr_dn: number | null;
  straight: number | null; side: number; tradable: boolean; skip: string;
}
export interface BtLiveTrade {
  st: string; secid: string; side: number; d: string; t_in: string | null; px_in: number | null; qty: number | null;
  d_out: string | null; t_out: string | null; px_out: number | null; pnl_rub: number | null; move: number | null; note: string | null;
}
export interface BtCandle { time: number; open: number; high: number; low: number; close: number; volume: number; secid: string }
interface CandlesWire { t: number[]; o: number[]; h: number[]; l: number[]; c: number[]; v: number[]; secid: [number, string][] }

async function j<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await apiFetch(BASE + path, init);
  if (!r.ok) {
    let msg = `HTTP ${r.status}`;
    try { const b = await r.json(); const d = b?.detail ?? b?.error?.message; if (d) msg = typeof d === 'string' ? d : JSON.stringify(d); } catch { /* не JSON */ }
    throw new Error(msg);
  }
  return r.json();
}
const json = (method: string, body: unknown): RequestInit => ({ method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });

export const btApi = {
  meta: () => j<BtMeta>('/meta'),
  runs: () => j<BtRun[]>('/runs'),
  run: (id: number) => j<BtRun>(`/runs/${id}`),
  createRun: (spec: any) => j<{ id: number }>('/runs', json('POST', spec)),
  deleteRun: (id: number) => j<{ ok: boolean }>(`/runs/${id}`, { method: 'DELETE' }),
  trades: (id: number) => j<BtTrade[]>(`/runs/${id}/trades`),
  equity: (id: number) => j<BtEquity[]>(`/runs/${id}/equity`),
  signals: (id: number, st: string) => j<BtSignal[]>(`/runs/${id}/signals?st=${st}`),
  live: () => j<BtLiveTrade[]>('/live/trades'),
  realism: (st: string) => j<BtRealism>(`/realism?st=${st}`),
  workspace: (name: string) => j<{ name: string; data: any }>(`/workspaces/${encodeURIComponent(name)}`),
  saveWorkspace: (name: string, data: any) => j<{ ok: boolean }>(`/workspaces/${encodeURIComponent(name)}`, json('PUT', { data })),
  candles: async (st: string, tf: number, from: string, to: string): Promise<BtCandle[]> => {
    const w = await j<CandlesWire>(`/candles?st=${st}&tf=${tf}&from=${from}&to=${to}`);
    const out: BtCandle[] = new Array(w.t.length);
    let k = 0, sec = w.secid[0]?.[1] ?? '';
    for (let i = 0; i < w.t.length; i++) {
      while (k + 1 < w.secid.length && w.secid[k + 1][0] <= i) k++;
      sec = w.secid[k]?.[1] ?? sec;
      out[i] = { time: w.t[i], open: w.o[i], high: w.h[i], low: w.l[i], close: w.c[i], volume: w.v[i], secid: sec };
    }
    return out;
  },
};
