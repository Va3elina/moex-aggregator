// Стенд: одна ячейка графика — на графическом движке сайта и песочницы (LwChartPanes + движок индикаторов из
// pages/embed/EmbedIndicators): те же индикаторы с настройкой, переносом между панелями и профилем объёма.
// Наше здесь — данные (свечи кусками по календарным границам, догрузка при прокрутке) и слой сделок
// (стрелки, линии вход→выход, окно сигнала, сделки робота), который вешается на ценовую серию через onSeriesBuilt.
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  createChart, createSeriesMarkers, HistogramSeries, LineSeries, ColorType, CrosshairMode, LineStyle,
  type IChartApi, type ISeriesApi, type SeriesMarker, type SeriesType, type Time, type UTCTimestamp, type LogicalRange,
} from 'lightweight-charts';
import LwChartPanes, { type LwPane, type LwSeriesBuiltCtx } from '../../components/LwChartPanes';
import type { LwSeries } from '../../components/chart/lwTypes';
import { IndicatorList, PaneIndicatorList, useIndicatorSeries, useVolumeProfileSpec, type IndicatorsApi } from '../embed/EmbedIndicators';
import { btApi, type BtCandle, type BtLiveTrade, type BtTrade } from './api';
import { chunkEnd, chunkOf, chunkStep, hm, num, pct, today, ts, tsToIso, tfLabel } from './lib';
import { TradesPrimitive, type TradeShape } from './TradesPrimitive';
import type { Prefs } from './usePrefs';

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
  if (!rule?.signal) return { a: 630, b: 1020, tin: 1020, tout: 660 };
  return { a: hm(rule.signal.from), b: hm(rule.signal.to),
    tin: hm(rule.signal.to) + (rule.entry?.delay_min ?? 0) + (rule.entry?.price === 'next_open' ? 5 : 0),
    tout: hm(rule.exit.at) + (rule.exit?.delay_min ?? 0) + (rule.exit?.price === 'next_open' ? 5 : 0) };
}

export interface Focus { d: string; dOut: string; nonce: number }

/** Свеча под курсором активной ячейки — отдельное хранилище, чтобы шапка обновлялась без перерисовки всей страницы. */
type HoverListener = () => void;
export interface HoverInd { label: string; color: string; text: string }
/** Свеча и значения индикаторов на ней. Курсора нет — последняя свеча, как в TradingView. */
export interface Hover { candle: BtCandle; inds: HoverInd[] }
export const hoverStore = {
  value: null as Hover | null, listeners: new Set<HoverListener>(),
  set(v: Hover | null) { if (v === this.value) return; this.value = v; this.listeners.forEach(l => l()); },
  subscribe(l: HoverListener) { hoverStore.listeners.add(l); return () => { hoverStore.listeners.delete(l); }; },
  get() { return hoverStore.value; },
};
const toSec = (t: string) => Number(t);

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export default function ChartCell({ st, tf, active, multi, rule, trades, live, show, inds, focus, onActivate }: {
  st: string; tf: number; active: boolean; multi: boolean; rule: any; trades: BtTrade[]; live: BtLiveTrade[];
  show: Prefs['show']; inds: IndicatorsApi; focus: Focus | null; onActivate: () => void;
}) {
  const ctxRef = useRef<LwSeriesBuiltCtx | null>(null);
  const layer = useRef<TradesPrimitive | null>(null);
  const marks = useRef<ReturnType<typeof createSeriesMarkers<Time>> | null>(null);
  const byTime = useRef(new Map<number, BtCandle>());
  const centerTime = useRef<number | null>(null);
  const pending = useRef<{ kind: 'center'; t: number } | { kind: 'focus'; from: number; to: number } | null>(null);
  const edge = useRef({ loading: false, tf, range: { from: '', to: '' } });
  const candlesRef = useRef<BtCandle[]>([]);
  const activeRef = useRef(active); activeRef.current = active;
  const subscribed = useRef<IChartApi | null>(null); const hovered = useRef(new WeakSet<IChartApi>());
  const seriesRef = useRef<ISeriesApi<SeriesType> | null>(null);

  const [range, setRange] = useState(() => { const c = chunkOf(tf, today()); return { from: chunkStep(tf, c, -1) ?? c, to: c }; });
  const [candles, setCandles] = useState<BtCandle[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

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
      .finally(() => { if (!dead) { setLoading(false); setTimeout(() => { edge.current.loading = false; }, 400); } });
    return () => { dead = true; };
  }, [st, tf, range]);
  const reachStart = useCallback(() => {
    const e = edge.current; if (e.loading) return;
    const prev = chunkStep(e.tf, e.range.from, -1); if (prev) { e.loading = true; setRange(x => ({ ...x, from: prev })); }
  }, []);

  // ─── панели: цена + индикаторы песочницы ───
  const indCandles = useMemo(() => candles.map(c => ({ time: String(c.time), value: c.close, open: c.open, high: c.high, low: c.low, close: c.close, volume: c.volume })), [candles]);
  const indSeries = useIndicatorSeries(inds.list, indCandles, toSec, inds.colorOf, 'right');
  const vpSpec = useVolumeProfileSpec(inds.list, indCandles, 'price', inds.colorOf);
  const volIds = useMemo(() => new Set(inds.list.filter(i => i.kind === 'volume').map(i => i.id)), [inds.list]);   // объём свечи в шапке уже есть — точным числом, не «2т»
  // значения индикаторов на свече под курсором — в шапку страницы; на самих панелях чисел нет (только название и кнопки)
  const indIndex = useMemo(() => indSeries.flat().filter(d => d && d.data.length && !volIds.has(d.id)).map(d => ({ label: d.label, color: d.color, fmt: d.axisFmt, at: new Map(d.data.map(p => [Number(p.time), p.value])) })), [indSeries, volIds]);
  const hoverAt = useCallback((t: number | null): Hover | null => {
    const cs = candlesRef.current; const candle = t != null ? byTime.current.get(t) : cs[cs.length - 1]; if (!candle) return null;
    const out: HoverInd[] = [];
    for (const x of indIndexRef.current) { const v = x.at.get(candle.time); if (v != null && isFinite(v)) out.push({ label: x.label, color: x.color, text: x.fmt ? x.fmt(v) : num(v, 2) }); }
    return { candle, inds: out };
  }, []);
  const indIndexRef = useRef(indIndex); indIndexRef.current = indIndex;
  const paneMap = useMemo(() => [...new Set(inds.list.filter(i => i.pane > 0).map(i => i.pane))].sort((a, b) => a - b), [inds.list]);
  const panes = useMemo<LwPane[]>(() => {
    candlesRef.current = candles; byTime.current = new Map(candles.map(c => [c.time, c]));
    const price: LwSeries = { id: 'price', type: 'candlestick', scale: 'right', color: '#26a69a', label: 'Цена', lastValueVisible: true,
      data: candles.map(c => ({ time: c.time, value: c.close, open: c.open, high: c.high, low: c.low, close: c.close })) };
    const extra = paneMap.map(p => ({ series: indSeries[p] ?? [], flex: 1 }));
    return [{ series: [price, ...(indSeries[0] ?? [])], flex: extra.length ? 2.8 : 1 }, ...extra];
  }, [candles, indSeries, paneMap]);

  // ─── слой сделок: стрелки, линии, окно сигнала ───
  const times = useMemo(() => execTimes(rule), [rule]);
  const paint = useCallback(() => {
    const have = byTime.current; if (!marks.current || !layer.current) return;
    const bucket = (d: string, m: number) => tf === 1440 ? ts(d) : ts(d, Math.floor(m / tf) * tf);
    const ms: SeriesMarker<Time>[] = []; const shapes: TradeShape[] = [];
    for (const t of trades) {
      const muted = !!t.account_skip, long = t.side > 0, tIn = bucket(t.d, t.m_in ?? times.tin), tOut = bucket(t.d_out, t.m_out ?? times.tout);
      const why = t.exit_reason && t.exit_reason !== 'время' ? t.exit_reason[0].toUpperCase() + t.exit_reason.slice(1) : 'Выход';
      shapes.push({ tIn, pIn: t.px_in, tOut, pOut: t.px_out, side: t.side, good: t.net > 0, muted,
        winFrom: tf <= 60 && rule?.signal ? bucket(t.d, times.a) : null, winTo: tf <= 60 && rule?.signal ? bucket(t.d, times.b) : null });
      if (!show.markers) continue;
      const thr = t.thr != null ? ` · порог ${pct(long ? t.thr : -t.thr)}` : ''; const mv = t.move != null ? ` · ход ${pct(t.move)}` : '';
      if (have.has(tIn)) ms.push({ time: tIn as UTCTimestamp, position: long ? 'belowBar' : 'aboveBar', shape: long ? 'arrowUp' : 'arrowDown',
        color: muted ? '#5d6675' : long ? C.up : C.down, text: show.labels ? `${long ? 'Лонг' : 'Шорт'} ${num(t.px_in, 2)}${mv}${tf <= 15 ? thr : ''}` : undefined });
      if (have.has(tOut)) ms.push({ time: tOut as UTCTimestamp, position: long ? 'aboveBar' : 'belowBar', shape: 'circle',
        color: muted ? '#5d6675' : t.net > 0 ? C.up : C.down, text: show.labels ? `${why} ${num(t.px_out, 2)} · ${pct(t.net)}` : undefined });
    }
    if (show.robot) for (const r of live) {
      if (r.px_in == null) continue;
      const tIn = bucket(r.d, r.t_in ? hm(r.t_in) : times.tin), tOut = r.d_out ? bucket(r.d_out, r.t_out ? hm(r.t_out) : times.tout) : null;
      shapes.push({ tIn, pIn: r.px_in, tOut, pOut: r.px_out, side: r.side, good: (r.pnl_rub ?? 0) > 0, muted: false, winFrom: null, winTo: null, robot: true });
      if (have.has(tIn)) ms.push({ time: tIn as UTCTimestamp, position: r.side > 0 ? 'belowBar' : 'aboveBar', shape: 'square', color: '#4c8dff',
        text: show.labels ? `Робот ${r.side > 0 ? 'купил' : 'продал'} ${num(r.qty)} по ${num(r.px_in, 2)}` : undefined });
      if (tOut != null && have.has(tOut)) ms.push({ time: tOut as UTCTimestamp, position: r.side > 0 ? 'aboveBar' : 'belowBar', shape: 'square', color: '#4c8dff',
        text: show.labels ? `Робот закрыл по ${num(r.px_out, 2)} · ${num(r.pnl_rub)} ₽` : undefined });
    }
    marks.current.setMarkers(ms.sort((a, b) => (a.time as number) - (b.time as number)));
    layer.current.set({ trades: shapes, lines: show.lines, window: show.window });
  }, [trades, live, tf, times, rule, show.markers, show.lines, show.window, show.robot, show.labels]);
  const paintRef = useRef(paint); paintRef.current = paint;
  useEffect(() => { paint(); }, [paint]);

  const onBuilt = useCallback((ctx: LwSeriesBuiltCtx) => {
    ctxRef.current = ctx;
    const s = ctx.getSeries(0, 'price') as ISeriesApi<SeriesType> | null; if (!s) return;
    if (seriesRef.current !== s) {                        // серия новая (полная пересборка) — вешаем слой заново
      seriesRef.current = s; const prim = new TradesPrimitive(); s.attachPrimitive(prim); layer.current = prim; marks.current = createSeriesMarkers(s, []);
    }
    paintRef.current();
    const lead = ctx.charts[0];
    // курсор в ЛЮБОЙ панели (цена, объём, RSI…) обновляет шапку; ушёл с графика — показываем последнюю свечу
    for (const ch of ctx.charts) { if (hovered.current.has(ch)) continue; hovered.current.add(ch);
      ch.subscribeCrosshairMove(p => { if (activeRef.current) hoverStore.set(hoverAt(p.time != null ? Number(p.time) : null)); }); }
    if (activeRef.current) hoverStore.set(hoverAt(null));
    if (lead && subscribed.current !== lead) {
      subscribed.current = lead;
      lead.timeScale().subscribeVisibleLogicalRangeChange((r: LogicalRange | null) => {
        const cs = candlesRef.current; if (!r || !cs.length) return;
        centerTime.current = cs[Math.max(0, Math.min(cs.length - 1, Math.round((r.from + r.to) / 2)))].time;
        const e = edge.current;
        if (!e.loading && r.to > cs.length - 20) { const next = chunkStep(e.tf, e.range.to, 1); if (next) { e.loading = true; setRange(x => ({ ...x, to: next })); } }
      });
    }
    const p = pending.current, cs = candlesRef.current; if (!p || !cs.length || !lead) return;
    pending.current = null;
    // после собственной расстановки диапазона в LwChartPanes (он делает её в rAF после пересборки серий)
    setTimeout(() => {
      const tsc = lead.timeScale();
      if (p.kind === 'center') {
        let i = cs.findIndex(c => c.time >= p.t); if (i < 0) i = cs.length - 1;
        const r = tsc.getVisibleLogicalRange(); const half = r ? Math.min(400, Math.max(40, (r.to - r.from) / 2)) : 90;
        tsc.setVisibleLogicalRange({ from: i - half, to: i + half });
      } else {
        let i = cs.findIndex(c => c.time >= p.from); if (i < 0) i = 0;
        let k = cs.length - 1; while (k > i && cs[k].time > p.to) k--;
        tsc.setVisibleLogicalRange({ from: i - 3, to: k + 3 });
      }
    }, 90);
  }, [hoverAt]);
  useEffect(() => { if (active) hoverStore.set(hoverAt(null)); }, [active, hoverAt, candles, indIndex]);   // окно стало активным / данные приехали

  return (
    <div className={`bt-cell ${active && multi ? 'on' : ''}`} onMouseDown={onActivate} data-theme="editorial-dark">
      <div className="bt-cellmain">
        {candles.length > 0 && <LwChartPanes panes={panes} dark hideLegend watermark={false} showTooltip={false} drawPaneIndex={0} fitKey={viewKey} initialBars={220}
          timeVisible={tf !== 1440} volumeProfile={vpSpec} onReachStart={reachStart} onSeriesBuilt={onBuilt}
          paneOverlay={i => (i === 0 ? null : <PaneIndicatorList api={inds} pane={paneMap[i - 1] ?? i} />)} />}
        <IndicatorList api={inds} native={EMPTY_ROWS} visible />
        {multi && <span className="bt-celltag">{st} · {tfLabel(tf)}</span>}
        {loading && <div className="bt-loadbar" />}
        {error && <div className="bt-charterr">{error}</div>}
      </div>
    </div>
  );
}
const EMPTY_ROWS: never[] = [];

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
