// Стенд: вся аналитика тестера считается в браузере из сделок и кривой капитала — поэтому любой срез
// (одна бумага / весь счёт, лонги / шорты, период) доступен мгновенно и без новых ручек на сервере.
import type { BtEquity, BtTrade } from './api';

export interface Pt { d: string; v: number }
export interface Stats {
  money: boolean;                      // true — рубли на счёте; false — доли на 1 контракт (счёт не считался)
  capital: number;
  n: number; wins: number; losses: number; flats: number;
  total: number; grossProfit: number; grossLoss: number; pf: number | null;
  costs: number; longPnl: number; shortPnl: number; longN: number; shortN: number;
  expectancy: number; avgWin: number | null; avgLoss: number | null; maxWin: number; maxLoss: number; avgRet: number;
  curve: Pt[]; dd: Pt[]; maxDd: number; maxDdPct: number; maxDdDate: string | null;
  cagr: number | null; totalRet: number; sharpe: number | null; sortino: number | null;
  bestStreak: number; worstStreak: number; streaks: number[]; avgWinStreak: number | null; avgLossStreak: number | null;
  avgDays: number; ddSpans: { from: string; to: string; days: number; depth: number }[]; avgDdDays: number | null; avgUpDays: number | null;
  skipped: Record<string, number>;
}

const dayDiff = (a: string, b: string) => Math.round((Date.parse(b) - Date.parse(a)) / 864e5);

export function pnlOf(t: BtTrade, money: boolean) { return money ? (t.pnl_rub ?? 0) : t.net; }

export function compute(trades: BtTrade[], equity: BtEquity[] | null, capital: number, money: boolean): Stats {
  const skipped: Record<string, number> = {};
  const done = trades.filter(t => {
    if (money && t.account_skip) { skipped[t.account_skip] = (skipped[t.account_skip] ?? 0) + 1; return false; }
    return true;
  }).sort((a, b) => a.d_out < b.d_out ? -1 : a.d_out > b.d_out ? 1 : 0);
  const p = done.map(t => pnlOf(t, money));
  const wins = p.filter(x => x > 0), losses = p.filter(x => x < 0);
  const sum = (a: number[]) => a.reduce((x, y) => x + y, 0);
  const grossProfit = sum(wins), grossLoss = -sum(losses), total = grossProfit - grossLoss;

  // кривая: для счёта целиком — дневная из движка; для среза — накопленный итог по дате выхода
  let curve: Pt[];
  if (equity && equity.length) curve = equity.map(e => ({ d: e.d, v: e.equity }));
  else {
    let acc = money ? capital : 0; const m = new Map<string, number>();
    for (let i = 0; i < done.length; i++) { acc += p[i]; m.set(done[i].d_out, acc); }
    curve = [...m].map(([d, v]) => ({ d, v }));
    if (done.length) curve.unshift({ d: done[0].d, v: money ? capital : 0 });
  }
  let peak = -Infinity, maxDd = 0, maxDdPct = 0, maxDdDate: string | null = null, peakDate = curve[0]?.d ?? '';
  const dd: Pt[] = []; const ddSpans: Stats['ddSpans'] = []; let inDd = false, spanDepth = 0; const upSpans: number[] = []; let upFrom = curve[0]?.d ?? '';
  for (const c of curve) {
    if (c.v >= peak) {
      if (inDd) { ddSpans.push({ from: peakDate, to: c.d, days: dayDiff(peakDate, c.d), depth: spanDepth }); inDd = false; upFrom = c.d; }
      peak = c.v; peakDate = c.d;
    } else {
      if (!inDd) { inDd = true; spanDepth = 0; if (upFrom && upFrom !== peakDate) upSpans.push(dayDiff(upFrom, peakDate)); }
      const depth = peak - c.v; spanDepth = Math.max(spanDepth, depth);
      if (depth > maxDd) { maxDd = depth; maxDdDate = c.d; maxDdPct = money && peak > 0 ? depth / peak : depth; }
    }
    dd.push({ d: c.d, v: money && peak > 0 ? -(peak - c.v) / peak * 100 : -(peak - c.v) * 100 });
  }
  if (inDd && curve.length) ddSpans.push({ from: peakDate, to: curve[curve.length - 1].d, days: dayDiff(peakDate, curve[curve.length - 1].d), depth: spanDepth });

  // дневные доходности → Шарп / Сортино
  let sharpe: number | null = null, sortino: number | null = null, cagr: number | null = null;
  const totalRet = money ? total / capital : total;
  if (money && curve.length > 20) {
    const r: number[] = []; for (let i = 1; i < curve.length; i++) r.push(curve[i].v / curve[i - 1].v - 1);
    const perYear = r.length / Math.max(dayDiff(curve[0].d, curve[curve.length - 1].d) / 365.25, 0.05);
    const mean = sum(r) / r.length, sd = Math.sqrt(sum(r.map(x => (x - mean) ** 2)) / (r.length - 1));
    const dsd = Math.sqrt(sum(r.map(x => Math.min(x, 0) ** 2)) / r.length);
    sharpe = sd > 0 ? mean / sd * Math.sqrt(perYear) : null; sortino = dsd > 0 ? mean / dsd * Math.sqrt(perYear) : null;
    const yrs = dayDiff(curve[0].d, curve[curve.length - 1].d) / 365.25;
    cagr = yrs > 0.05 ? (curve[curve.length - 1].v / curve[0].v) ** (1 / yrs) - 1 : null;
  }

  const streaks: number[] = []; let cur = 0;
  for (const x of p) { const s = x > 0 ? 1 : x < 0 ? -1 : 0; if (s === 0) continue; if (Math.sign(cur) === s) cur += s; else { if (cur) streaks.push(cur); cur = s; } }
  if (cur) streaks.push(cur);
  const ws = streaks.filter(x => x > 0), ls = streaks.filter(x => x < 0).map(x => -x);
  const longs = done.filter(t => t.side > 0), shorts = done.filter(t => t.side < 0);
  return {
    money, capital, n: done.length, wins: wins.length, losses: losses.length, flats: p.length - wins.length - losses.length,
    total, grossProfit, grossLoss, pf: grossLoss > 0 ? grossProfit / grossLoss : null,
    costs: money ? sum(done.map(t => (t.comm_rub ?? 0) + (t.spread_rub ?? 0))) : sum(done.map(t => t.comm + t.spread)),
    longPnl: sum(longs.map(t => pnlOf(t, money))), shortPnl: sum(shorts.map(t => pnlOf(t, money))), longN: longs.length, shortN: shorts.length,
    expectancy: p.length ? total / p.length : 0, avgWin: wins.length ? grossProfit / wins.length : null,
    avgLoss: losses.length ? -grossLoss / losses.length : null, maxWin: Math.max(0, ...p), maxLoss: Math.min(0, ...p),
    avgRet: done.length ? sum(done.map(t => t.net)) / done.length : 0,
    curve, dd, maxDd, maxDdPct, maxDdDate, cagr, totalRet, sharpe, sortino,
    bestStreak: Math.max(0, ...ws), worstStreak: Math.max(0, ...ls), streaks,
    avgWinStreak: ws.length ? sum(ws) / ws.length : null, avgLossStreak: ls.length ? sum(ls) / ls.length : null,
    avgDays: done.length ? sum(done.map(t => dayDiff(t.d, t.d_out))) / done.length : 0,
    ddSpans: ddSpans.sort((a, b) => b.depth - a.depth), avgDdDays: ddSpans.length ? sum(ddSpans.map(s => s.days)) / ddSpans.length : null,
    avgUpDays: upSpans.length ? sum(upSpans) / upSpans.length : null, skipped,
  };
}

export type Bucket = 'day' | 'week' | 'month' | 'quarter' | 'year';
export function bucketKey(d: string, b: Bucket): string {
  if (b === 'day') return d;
  if (b === 'year') return d.slice(0, 4);
  if (b === 'month') return d.slice(0, 7);
  if (b === 'quarter') return `${d.slice(0, 4)} К${Math.floor((+d.slice(5, 7) - 1) / 3) + 1}`;
  const x = new Date(d + 'T00:00:00Z'); x.setUTCDate(x.getUTCDate() - ((x.getUTCDay() + 6) % 7)); return x.toISOString().slice(0, 10);
}
export function byPeriod(trades: BtTrade[], money: boolean, b: Bucket): { k: string; v: number; n: number }[] {
  const m = new Map<string, { v: number; n: number }>();
  for (const t of trades) { if (money && t.account_skip) continue; const k = bucketKey(t.d_out, b); const x = m.get(k) ?? { v: 0, n: 0 }; x.v += pnlOf(t, money); x.n++; m.set(k, x); }
  return [...m].sort((a, c) => a[0] < c[0] ? -1 : 1).map(([k, x]) => ({ k, ...x }));
}
const WD = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'], MN = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек'];
export function byCalendar(trades: BtTrade[], money: boolean, kind: 'weekday' | 'month' | 'year') {
  const keys = kind === 'weekday' ? WD.slice(0, 5) : kind === 'month' ? MN : [...new Set(trades.map(t => t.d.slice(0, 4)))].sort();
  const acc = keys.map(k => ({ k, v: 0, n: 0, w: 0 }));
  for (const t of trades) {
    if (money && t.account_skip) continue;
    const i = kind === 'weekday' ? (new Date(t.d + 'T00:00:00Z').getUTCDay() + 6) % 7 : kind === 'month' ? +t.d.slice(5, 7) - 1 : keys.indexOf(t.d.slice(0, 4));
    if (!acc[i]) continue; const p = pnlOf(t, money); acc[i].v += p; acc[i].n++; if (p > 0) acc[i].w++;
  }
  return acc;
}
export function histogram(trades: BtTrade[], bins = 21) {
  const r = trades.map(t => 100 * t.net); if (!r.length) return [];
  const lim = Math.max(1, Math.min(8, Math.ceil(Math.max(...r.map(Math.abs)))));
  const w = 2 * lim / bins; const out = Array.from({ length: bins }, (_, i) => ({ from: -lim + i * w, to: -lim + (i + 1) * w, n: 0 }));
  for (const x of r) out[Math.max(0, Math.min(bins - 1, Math.floor((x + lim) / w)))].n++;
  return out;
}
