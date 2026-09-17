// Стенд: одна ячейка графика. Свечи грузятся кусками по календарным границам и догружаются при прокрутке влево;
// поверх — сделки бэктеста (стрелки, линии, окно сигнала), сделки робота, индикаторы. Прямо на lightweight-charts:
// общий LwChartPanes сайта не трогаем, из общего берём только чистую математику utils/indicators.
import { useEffect, useMemo, useRef, useState } from 'react';
import {
  createChart, createSeriesMarkers, CandlestickSeries, HistogramSeries, LineSeries, ColorType, CrosshairMode, LineStyle,
  type IChartApi, type ISeriesApi, type SeriesMarker, type Time, type UTCTimestamp, type ISeriesMarkersPluginApi, type LogicalRange,
} from 'lightweight-charts';
import { sma, ema, wma, bollinger, rsi, atr, type IndPoint } from '../../utils/indicators';
import { btApi, type BtCandle, type BtLiveTrade, type BtTrade } from './api';
import { chunkEnd, chunkOf, chunkStep, hm, num, pct, today, ts, tsToIso, tfLabel } from './lib';
import { TradesPrimitive, type TradeShape } from './TradesPrimitive';
import type { IndCfg, Prefs } from './usePrefs';
import { Logo } from './ui';

const C = { bg: '#0f0f0f', text: '#8a8f98', grid: 'rgba(255,255,255,.045)', up: '#26a69a', down: '#ef5350', border: '#1f2228' };
const cache = new Map<string, { at: number; p: Promise<BtCandle[]> }>();

function loadChunk(st: string, tf: number, start: string): Promise<BtCandle[]> {
  const key = `${st}|${tf}|${start}`, end = chunkEnd(tf, start), live = end >= today();
  const hit = cache.get(key);
  if (hit && (!live || Date.now() - hit.at < 60_000)) return hit.p;
  const p = btApi.candles(st, tf, start, end);
  cache.set(key, { at: Date.now(), p }); p.catch(() => cache.delete(key));
  return p;
}
function chunksBetween(tf: number, from: string, to: string): string[] {
  const out = [from]; let c: string | null = from;
  while (c && c < to) { c = chunkStep(tf, c, 1); if (c) out.push(c); }
  return out;
}
function chartOptions(timeVisible: boolean) {
  return {
    autoSize: true,
    layout: { background: { type: ColorType.Solid, color: C.bg }, textColor: C.text, fontSize: 11, attributionLogo: false },
    grid: { vertLines: { color: C.grid }, horzLines: { color: C.grid } },
    crosshair: { mode: CrosshairMode.Normal, vertLine: { color: '#555b66', labelBackgroundColor: '#2a2e39' }, horzLine: { color: '#555b66', labelBackgroundColor: '#2a2e39' } },
    rightPriceScale: { borderColor: C.border }, timeScale: { borderColor: C.border, timeVisible, secondsVisible: false, rightOffset: 8, minBarSpacing: 0.5 },
    localization: { locale: 'ru-RU' },
  } as const;
}
/** Минуты входа/выхода и окна сигнала по развёрнутому правилу прогона — как считает движок. */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function execTimes(rule: any) {
  if (!rule) return { a: 630, b: 1020, tin: 1020, tout: 660 };
  return { a: hm(rule.signal.from), b: hm(rule.signal.to),
    tin: hm(rule.signal.to) + (rule.entry?.delay_min ?? 0) + (rule.entry?.price === 'next_open' ? 5 : 0),
    tout: hm(rule.exit.at) + (rule.exit?.delay_min ?? 0) + (rule.exit?.price === 'next_open' ? 5 : 0) };
}

export interface Focus { d: string; dOut: string; nonce: number }

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export default function ChartCell({ st, tf, name, active, rule, trades, live, show, indicators, focus, onActivate, onIndicators }: {
  st: string; tf: number; name: string; active: boolean; rule: any; trades: BtTrade[]; live: BtLiveTrade[];
  show: Prefs['show']; indicators: IndCfg[]; focus: Focus | null; onActivate: () => void; onIndicators: (next: IndCfg[]) => void;
}) {
  const el = useRef<HTMLDivElement>(null);
  const chart = useRef<IChartApi | null>(null);
  const series = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const volume = useRef<ISeriesApi<'Histogram'> | null>(null);
  const marks = useRef<ISeriesMarkersPluginApi<Time> | null>(null);
  const layer = useRef<TradesPrimitive | null>(null);
  const indSeries = useRef(new Map<string, ISeriesApi<'Line'>[]>());
  const byTime = useRef(new Map<number, BtCandle>());
  const centerTime = useRef<number | null>(null);
  const pending = useRef<{ kind: 'center'; t: number } | { kind: 'focus'; from: number; to: number } | null>(null);
  const edge = useRef({ loading: false, tf, range: { from: '', to: '' } });
  const candlesRef = useRef<BtCandle[]>([]);

  const [range, setRange] = useState(() => { const c = chunkOf(tf, today()); return { from: chunkStep(tf, c, -1) ?? c, to: c }; });
  const [candles, setCandles] = useState<BtCandle[]>([]);
  const [hover, setHover] = useState<BtCandle | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // ─── график ───
  useEffect(() => {
    if (!el.current) return;
    const ch = createChart(el.current, chartOptions(true));
    const s = ch.addSeries(CandlestickSeries, { upColor: C.up, downColor: C.down, borderUpColor: C.up, borderDownColor: C.down, wickUpColor: C.up, wickDownColor: C.down, priceLineVisible: true, priceLineStyle: LineStyle.Dotted });
    const v = ch.addSeries(HistogramSeries, { priceScaleId: 'vol', priceFormat: { type: 'volume' }, lastValueVisible: false, priceLineVisible: false });
    ch.priceScale('vol').applyOptions({ scaleMargins: { top: 0.88, bottom: 0 } });
    const prim = new TradesPrimitive(); s.attachPrimitive(prim);
    ch.subscribeCrosshairMove(p => setHover(p.time ? byTime.current.get(p.time as number) ?? null : null));
    ch.timeScale().subscribeVisibleLogicalRangeChange((r: LogicalRange | null) => {
      const cs = candlesRef.current; if (!r || !cs.length) return;
      const mid = cs[Math.max(0, Math.min(cs.length - 1, Math.round((r.from + r.to) / 2)))]; centerTime.current = mid.time;
      const e = edge.current; if (e.loading) return;
      if (r.from < 40) { const prev = chunkStep(e.tf, e.range.from, -1); if (prev) { e.loading = true; setRange(x => ({ ...x, from: prev })); } }
      else if (r.to > cs.length - 20) { const next = chunkStep(e.tf, e.range.to, 1); if (next) { e.loading = true; setRange(x => ({ ...x, to: next })); } }
    });
    chart.current = ch; series.current = s; volume.current = v; layer.current = prim; marks.current = createSeriesMarkers(s, []);
    const inds = indSeries.current;
    return () => { ch.remove(); chart.current = null; inds.clear(); };
  }, []);

  // ─── смена бумаги / таймфрейма: остаёмся на том же месте по времени ───
  const viewKey = `${st}|${tf}`; const lastKey = useRef(viewKey);
  useEffect(() => {
    if (lastKey.current === viewKey) return; lastKey.current = viewKey;
    const t = centerTime.current; const d = t ? tsToIso(t) : today(); const c = chunkOf(tf, d);
    if (t) pending.current = { kind: 'center', t };
    setRange({ from: chunkStep(tf, c, -1) ?? c, to: chunkStep(tf, c, 1) ?? c });
  }, [viewKey, tf]);

  // ─── переход к сделке ───
  useEffect(() => {
    if (!focus) return;
    const pad = tf === 5 ? 1.2 : tf === 15 ? 4 : tf === 60 ? 15 : 120;
    pending.current = { kind: 'focus', from: ts(focus.d) - pad * 86400, to: ts(focus.dOut, 1440) + pad * 86400 };
    const c = chunkOf(tf, focus.d);
    setRange(r => (c >= r.from && chunkOf(tf, focus.dOut) <= r.to) ? { ...r } : { from: chunkStep(tf, c, -1) ?? c, to: chunkStep(tf, c, 1) ?? c });
  }, [focus, tf]);

  // ─── загрузка кусков ───
  useEffect(() => {
    let dead = false; setLoading(true); edge.current = { loading: true, tf, range };
    Promise.all(chunksBetween(tf, range.from, range.to).map(c => loadChunk(st, tf, c)))
      .then(parts => { if (!dead) { setCandles(parts.flat()); setError(null); } })
      .catch(e => { if (!dead) setError(String(e.message ?? e)); })
      .finally(() => { if (!dead) { setLoading(false); setTimeout(() => { edge.current.loading = false; }, 300); } });
    return () => { dead = true; };
  }, [st, tf, range]);

  // ─── данные в серии ───
  useEffect(() => {
    if (!series.current || !volume.current || !chart.current) return;
    candlesRef.current = candles; byTime.current = new Map(candles.map(c => [c.time, c]));
    series.current.setData(candles.map(c => ({ time: c.time as UTCTimestamp, open: c.open, high: c.high, low: c.low, close: c.close })));
    volume.current.setData(show.volume ? candles.map(c => ({ time: c.time as UTCTimestamp, value: c.volume, color: c.close >= c.open ? 'rgba(38,166,154,.32)' : 'rgba(239,83,80,.32)' })) : []);
    chart.current.applyOptions({ timeScale: { timeVisible: tf !== 1440 } });
    const p = pending.current; if (!p || !candles.length) return;
    pending.current = null; const tsc = chart.current.timeScale();
    if (p.kind === 'center') {
      let i = candles.findIndex(c => c.time >= p.t); if (i < 0) i = candles.length - 1;
      const r = tsc.getVisibleLogicalRange(); const half = r ? Math.max(30, (r.to - r.from) / 2) : 90;
      tsc.setVisibleLogicalRange({ from: i - half, to: i + half });
    } else {
      let i = candles.findIndex(c => c.time >= p.from); if (i < 0) i = 0;
      let k = candles.length - 1; while (k > i && candles[k].time > p.to) k--;
      tsc.setVisibleLogicalRange({ from: i - 3, to: k + 3 });
    }
  }, [candles, show.volume, tf]);

  // ─── сделки ───
  const times = useMemo(() => execTimes(rule), [rule]);
  useEffect(() => {
    if (!marks.current || !layer.current) return;
    const have = byTime.current; const bucket = (d: string, m: number) => tf === 1440 ? ts(d) : ts(d, Math.floor(m / tf) * tf);
    const ms: SeriesMarker<Time>[] = []; const shapes: TradeShape[] = [];
    for (const t of trades) {
      const muted = !!t.account_skip, long = t.side > 0, tIn = bucket(t.d, times.tin), tOut = bucket(t.d_out, times.tout);
      shapes.push({ tIn, pIn: t.px_in, tOut, pOut: t.px_out, side: t.side, good: t.net > 0, muted,
        winFrom: tf <= 60 ? bucket(t.d, times.a) : null, winTo: tf <= 60 ? bucket(t.d, times.b) : null });
      if (!show.markers) continue;
      const thr = t.thr != null ? ` · порог ${pct(long ? t.thr : -t.thr)}` : '';
      if (have.has(tIn)) ms.push({ time: tIn as UTCTimestamp, position: long ? 'belowBar' : 'aboveBar', shape: long ? 'arrowUp' : 'arrowDown',
        color: muted ? '#5d6675' : long ? C.up : C.down, text: show.labels ? `${long ? 'Лонг' : 'Шорт'} ${num(t.px_in, 2)} · ход ${pct(t.move)}${tf <= 15 ? thr : ''}` : undefined });
      if (have.has(tOut)) ms.push({ time: tOut as UTCTimestamp, position: long ? 'aboveBar' : 'belowBar', shape: 'circle',
        color: muted ? '#5d6675' : t.net > 0 ? C.up : C.down, text: show.labels ? `Выход ${num(t.px_out, 2)} · ${pct(t.net)}` : undefined });
    }
    if (show.robot) for (const r of live) {
      if (r.px_in == null) continue;
      const mi = r.t_in ? hm(r.t_in) : times.tin, tIn = bucket(r.d, mi);
      const tOut = r.d_out ? bucket(r.d_out, r.t_out ? hm(r.t_out) : times.tout) : null;
      shapes.push({ tIn, pIn: r.px_in, tOut, pOut: r.px_out, side: r.side, good: (r.pnl_rub ?? 0) > 0, muted: false, winFrom: null, winTo: null, robot: true });
      if (have.has(tIn)) ms.push({ time: tIn as UTCTimestamp, position: r.side > 0 ? 'belowBar' : 'aboveBar', shape: 'square', color: '#4c8dff',
        text: show.labels ? `Робот ${r.side > 0 ? 'купил' : 'продал'} ${num(r.qty)} по ${num(r.px_in, 2)}` : undefined });
      if (tOut != null && have.has(tOut)) ms.push({ time: tOut as UTCTimestamp, position: r.side > 0 ? 'aboveBar' : 'belowBar', shape: 'square', color: '#4c8dff',
        text: show.labels ? `Робот закрыл по ${num(r.px_out, 2)} · ${num(r.pnl_rub)} ₽` : undefined });
    }
    marks.current.setMarkers(ms.sort((a, b) => (a.time as number) - (b.time as number)));
    layer.current.set({ trades: shapes, lines: show.lines, window: show.window });
  }, [trades, live, candles, tf, times, show.markers, show.lines, show.window, show.robot, show.labels]);

  // ─── индикаторы на ценовой шкале ───
  const overlays = useMemo(() => indicators.filter(i => i.kind !== 'rsi' && i.kind !== 'atr'), [indicators]);
  const panes = useMemo(() => indicators.filter(i => (i.kind === 'rsi' || i.kind === 'atr') && !i.hidden), [indicators]);
  useEffect(() => {
    const ch = chart.current; if (!ch) return;
    const live_ = indSeries.current; const keep = new Set(overlays.map(i => i.id));
    for (const [id, arr] of live_) if (!keep.has(id)) { arr.forEach(s => ch.removeSeries(s)); live_.delete(id); }
    const pts: IndPoint<number>[] = candles.map(c => ({ time: c.time, value: c.close }));
    for (const i of overlays) {
      const lines: IndPoint<number>[][] = i.kind === 'bb' ? Object.values(bollinger(pts, i.length, i.mult ?? 2)) : [i.kind === 'sma' ? sma(pts, i.length) : i.kind === 'ema' ? ema(pts, i.length) : wma(pts, i.length)];
      let arr = live_.get(i.id);
      if (!arr || arr.length !== lines.length) {
        arr?.forEach(s => ch.removeSeries(s));
        arr = lines.map((_, k) => ch.addSeries(LineSeries, { lineWidth: 1, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false, lineStyle: i.kind === 'bb' && k > 0 ? LineStyle.Dashed : LineStyle.Solid }));
        live_.set(i.id, arr);
      }
      arr.forEach((s, k) => { s.applyOptions({ color: i.color, visible: !i.hidden }); s.setData(lines[k].map(p => ({ time: p.time as UTCTimestamp, value: p.value }))); });
    }
  }, [overlays, candles]);

  const last = hover ?? candles[candles.length - 1] ?? null;
  const chg = last ? last.close / last.open - 1 : 0;
  const labelOf = (i: IndCfg) => `${i.kind.toUpperCase()} ${i.length}${i.kind === 'bb' ? ` · ${i.mult ?? 2}` : ''}`;
  return (
    <div className={`bt-cell ${active ? 'on' : ''}`} onMouseDown={onActivate}>
      <div className="bt-cellmain">
        <div ref={el} className="bt-fill" />
        <div className="bt-legend">
          <div className="bt-legrow">
            <Logo st={st} size={18} /><b>{name}</b><span className="bt-dim">{last?.secid ?? ''} · {tfLabel(tf)} · MOEX</span>
            {last && <span className="bt-ohlc">О<i className={chg >= 0 ? 'bt-up' : 'bt-down'}>{num(last.open, 2)}</i> В<i className={chg >= 0 ? 'bt-up' : 'bt-down'}>{num(last.high, 2)}</i> Н<i className={chg >= 0 ? 'bt-up' : 'bt-down'}>{num(last.low, 2)}</i> З<i className={chg >= 0 ? 'bt-up' : 'bt-down'}>{num(last.close, 2)}</i> <i className={chg >= 0 ? 'bt-up' : 'bt-down'}>{pct(chg)}</i></span>}
          </div>
          {indicators.map(i => (
            <div key={i.id} className={`bt-legrow ind ${i.hidden ? 'off' : ''}`}>
              <span className="dot" style={{ background: i.color }} />{labelOf(i)}
              <button title={i.hidden ? 'показать' : 'скрыть'} onClick={() => onIndicators(indicators.map(x => x.id === i.id ? { ...x, hidden: !x.hidden } : x))}>{i.hidden ? '◌' : '◉'}</button>
              <button title="убрать" onClick={() => onIndicators(indicators.filter(x => x.id !== i.id))}>✕</button>
            </div>
          ))}
        </div>
        {loading && <div className="bt-loadbar" />}
        {error && <div className="bt-charterr">{error}</div>}
      </div>
      {panes.map(i => <SubPane key={i.id} cfg={i} candles={candles} main={chart} />)}
    </div>
  );
}

/** Панель индикатора под графиком — отдельный экземпляр чарта, синхронизированный по прокрутке (нативные панели v5 не работают). */
function SubPane({ cfg, candles, main }: { cfg: IndCfg; candles: BtCandle[]; main: React.RefObject<IChartApi | null> }) {
  const el = useRef<HTMLDivElement>(null); const ch = useRef<IChartApi | null>(null); const s = useRef<ISeriesApi<'Line'> | null>(null);
  useEffect(() => {
    if (!el.current) return;
    const c = createChart(el.current, { ...chartOptions(true), timeScale: { visible: false, rightOffset: 8, minBarSpacing: 0.5 }, handleScroll: false, handleScale: false });
    s.current = c.addSeries(LineSeries, { lineWidth: 1, priceLineVisible: false, color: cfg.color }); ch.current = c;
    const m = main.current; const sync = (r: LogicalRange | null) => { if (r) c.timeScale().setVisibleLogicalRange(r); };
    m?.timeScale().subscribeVisibleLogicalRangeChange(sync);
    return () => { m?.timeScale().unsubscribeVisibleLogicalRangeChange(sync); c.remove(); ch.current = null; };
  }, [main, cfg.color]);
  useEffect(() => {
    if (!s.current || !ch.current) return;
    const vals = cfg.kind === 'rsi' ? rsi(candles.map(c => ({ time: c.time, value: c.close })), cfg.length)
      : atr(candles.map(c => ({ time: c.time, value: c.close, high: c.high, low: c.low, close: c.close })), cfg.length);
    const m = new Map(vals.map(p => [p.time, p.value]));
    s.current.setData(candles.map(c => m.has(c.time) ? { time: c.time as UTCTimestamp, value: m.get(c.time)! } : { time: c.time as UTCTimestamp }));
    const r = main.current?.timeScale().getVisibleLogicalRange(); if (r) ch.current.timeScale().setVisibleLogicalRange(r);
  }, [candles, cfg.kind, cfg.length, main]);
  return <div className="bt-subpane"><div ref={el} className="bt-fill" /><span className="bt-subname">{cfg.kind.toUpperCase()} {cfg.length}</span></div>;
}

export function EquityChart({ lines, dd, height = 280 }: { lines: { name: string; color: string; points: { d: string; v: number }[] }[]; dd?: { d: string; v: number }[]; height?: number }) {
  const el = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!el.current) return;
    const ch = createChart(el.current, { ...chartOptions(false), rightPriceScale: { borderColor: C.border, scaleMargins: { top: 0.08, bottom: dd ? 0.3 : 0.06 } } });
    const dedupe = (p: { d: string; v: number }[]) => { const m = new Map(p.map(x => [x.d, x.v])); return [...m].sort((a, b) => a[0] < b[0] ? -1 : 1).map(([d, v]) => ({ time: d as Time, value: v })); };
    lines.forEach((l, k) => {
      const s = ch.addSeries(LineSeries, { color: l.color, lineWidth: 2, priceLineVisible: false, title: lines.length > 1 ? l.name : '', lineStyle: k ? LineStyle.Dashed : LineStyle.Solid,
        priceFormat: { type: 'custom', minMove: 0.01, formatter: (v: number) => Math.abs(v) >= 1e5 ? (v / 1e6).toFixed(2) + ' млн' : v.toFixed(2) } });
      s.setData(dedupe(l.points));
    });
    if (dd) {
      const h = ch.addSeries(HistogramSeries, { priceScaleId: 'dd', color: 'rgba(239,83,80,.45)', priceLineVisible: false, lastValueVisible: false, priceFormat: { type: 'custom', formatter: (v: number) => v.toFixed(1) + '%' } });
      ch.priceScale('dd').applyOptions({ scaleMargins: { top: 0.76, bottom: 0 } }); h.setData(dedupe(dd));
    }
    ch.timeScale().fitContent();
    return () => ch.remove();
  }, [lines, dd]);
  return <div ref={el} style={{ height, position: 'relative' }} />;
}
