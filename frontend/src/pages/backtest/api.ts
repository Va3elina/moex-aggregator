/* eslint-disable @typescript-eslint/no-explicit-any -- результат прогона и правило приходят как свободный JSON движка */
// Стенд: клиент /api/admin/bt. Отдельный файл, чтобы не раздувать общий services/api.ts (сайт не трогаем).
import { apiFetch } from '../../services/api';

const BASE = ((import.meta.env.VITE_API_BASE as string | undefined) ?? '') + '/api/admin/bt';

export interface BtRule { id: string; name: string; frozen: boolean; universe: string[]; rule: any }
export interface BtMeta {
  rules: BtRule[];
  instruments: { st: string; name: string }[];
  tariffs: Record<string, number>;
  exec: Record<string, string>;
  go: Record<string, string>;
}
export interface BtRunSummary { 'сделок'?: number; 'чистыми_%'?: number; 'годовых_%'?: number; 'просадка_%'?: number; 'Шарп'?: number }
export interface BtRun {
  id: number; name: string | null; status: 'queued' | 'running' | 'done' | 'error';
  created_at: string; finished_at: string | null; error: string | null;
  spec: any; spec_full?: any; result?: any; summary?: BtRunSummary;
}
export interface BtTrade {
  st: string; d: string; secid: string; side: number; move: number; thr: number | null;
  px_in: number; d_out: string; px_out: number; gross: number; comm: number; spread: number; net: number;
  qty: number | null; notional: number | null; go: number | null; equity_in: number | null;
  comm_rub: number | null; spread_rub: number | null; pnl_rub: number | null; account_skip: string | null;
}
export interface BtEquity { d: string; equity: number; positions: number; notional: number; go_used: number }
export interface BtCandle { time: number; secid: string; open: number; high: number; low: number; close: number; volume: number }

async function j<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await apiFetch(BASE + path, init);
  if (!r.ok) {
    let msg = `HTTP ${r.status}`;
    try { const b = await r.json(); if (b?.detail) msg = typeof b.detail === 'string' ? b.detail : JSON.stringify(b.detail); } catch { /* не JSON */ }
    throw new Error(msg);
  }
  return r.json();
}

export const btApi = {
  meta: () => j<BtMeta>('/meta'),
  runs: () => j<BtRun[]>('/runs'),
  run: (id: number) => j<BtRun>(`/runs/${id}`),
  createRun: (spec: any) => j<{ id: number }>('/runs', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(spec),
  }),
  deleteRun: (id: number) => j<{ ok: boolean }>(`/runs/${id}`, { method: 'DELETE' }),
  trades: (id: number) => j<BtTrade[]>(`/runs/${id}/trades`),
  equity: (id: number) => j<BtEquity[]>(`/runs/${id}/equity`),
  candles: (st: string, tf: number, from?: string, to?: string) => {
    const p = new URLSearchParams({ st, tf: String(tf) });
    if (from) p.set('from', from); if (to) p.set('to', to);
    return j<{ st: string; tf: number; from: string; to: string; candles: BtCandle[] }>(`/candles?${p}`);
  },
};
