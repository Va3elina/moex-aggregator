// График Стенда: свечи склеенного ряда + метки сделок. Прямо на lightweight-charts (общий LwChartPanes сайта не трогаем).
import { useEffect, useRef } from 'react';
import {
  createChart, createSeriesMarkers, CandlestickSeries, HistogramSeries, LineSeries, ColorType, CrosshairMode,
  type IChartApi, type ISeriesApi, type SeriesMarker, type Time, type UTCTimestamp, type ISeriesMarkersPluginApi,
} from 'lightweight-charts';
import type { BtCandle } from './api';

export interface BtMarker { time: number; side: number; kind: 'in' | 'out'; text: string; good?: boolean; muted?: boolean; key: string }

const C = { bg: '#0f1115', text: '#8891a0', grid: '#1a1f29', up: '#26a69a', down: '#ef5350', border: '#262c38' };

function baseOptions() {
  return {
    autoSize: true,
    layout: { background: { type: ColorType.Solid, color: C.bg }, textColor: C.text, fontSize: 11, attributionLogo: false },
    grid: { vertLines: { color: C.grid }, horzLines: { color: C.grid } },
    crosshair: { mode: CrosshairMode.Normal },
    rightPriceScale: { borderColor: C.border },
    timeScale: { borderColor: C.border, timeVisible: true, secondsVisible: false, rightOffset: 6 },
    localization: { locale: 'ru-RU' },
  } as const;
}

export function CandleChart({ candles, markers, focus, onHover }: {
  candles: BtCandle[]; markers: BtMarker[]; focus: { from: number; to: number; nonce: number } | null;
  onHover?: (c: BtCandle | null) => void;
}) {
  const el = useRef<HTMLDivElement>(null);
  const chart = useRef<IChartApi | null>(null);
  const series = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const volume = useRef<ISeriesApi<'Histogram'> | null>(null);
  const marks = useRef<ISeriesMarkersPluginApi<Time> | null>(null);
  const byTime = useRef(new Map<number, BtCandle>());
  const hoverRef = useRef(onHover);
  useEffect(() => { hoverRef.current = onHover; }, [onHover]);

  useEffect(() => {
    if (!el.current) return;
    const ch = createChart(el.current, baseOptions());
    const s = ch.addSeries(CandlestickSeries, {
      upColor: C.up, downColor: C.down, borderUpColor: C.up, borderDownColor: C.down, wickUpColor: C.up, wickDownColor: C.down,
      priceLineVisible: false,
    });
    const v = ch.addSeries(HistogramSeries, { priceScaleId: 'vol', priceFormat: { type: 'volume' }, lastValueVisible: false, priceLineVisible: false });
    ch.priceScale('vol').applyOptions({ scaleMargins: { top: 0.86, bottom: 0 } });
    ch.subscribeCrosshairMove(p => hoverRef.current?.(p.time ? byTime.current.get(p.time as number) ?? null : null));
    chart.current = ch; series.current = s; volume.current = v; marks.current = createSeriesMarkers(s, []);
    return () => { ch.remove(); chart.current = null; };
  }, []);

  useEffect(() => {
    if (!series.current || !volume.current) return;
    byTime.current = new Map(candles.map(c => [c.time, c]));
    series.current.setData(candles.map(c => ({ time: c.time as UTCTimestamp, open: c.open, high: c.high, low: c.low, close: c.close })));
    volume.current.setData(candles.map(c => ({ time: c.time as UTCTimestamp, value: c.volume, color: c.close >= c.open ? 'rgba(38,166,154,.35)' : 'rgba(239,83,80,.35)' })));
  }, [candles]);

  useEffect(() => {
    if (!marks.current) return;
    const have = byTime.current;
    const ms: SeriesMarker<Time>[] = markers.filter(m => have.has(m.time)).sort((a, b) => a.time - b.time).map(m => {
      const long = m.side > 0;
      if (m.kind === 'in') {
        return { time: m.time as UTCTimestamp, position: long ? 'belowBar' : 'aboveBar', shape: long ? 'arrowUp' : 'arrowDown',
                 color: m.muted ? '#5d6675' : long ? C.up : C.down, text: m.text, id: m.key };
      }
      return { time: m.time as UTCTimestamp, position: long ? 'aboveBar' : 'belowBar', shape: 'circle',
               color: m.muted ? '#5d6675' : m.good ? C.up : C.down, text: m.text, id: m.key };
    });
    marks.current.setMarkers(ms);
  }, [markers, candles]);

  useEffect(() => {
    if (!chart.current || !focus || !candles.length) return;
    // по индексам свечей, а не по времени: setVisibleRange с границей за пределами данных молча ничего не делает
    let i = candles.findIndex(c => c.time >= focus.from); if (i < 0) i = 0;
    let j = candles.length - 1; while (j > i && candles[j].time > focus.to) j--;
    chart.current.timeScale().setVisibleLogicalRange({ from: i - 3, to: j + 3 });
  }, [focus, candles]);

  return <div ref={el} style={{ position: 'absolute', inset: 0 }} />;
}

export function EquityChart({ lines }: { lines: { name: string; color: string; points: { d: string; v: number }[] }[] }) {
  const el = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!el.current) return;
    const ch = createChart(el.current, { ...baseOptions(), timeScale: { borderColor: C.border, timeVisible: false } });
    for (const l of lines) {
      const s = ch.addSeries(LineSeries, { color: l.color, lineWidth: 2, priceLineVisible: false, title: l.name,
        priceFormat: { type: 'custom', formatter: (v: number) => (v / 1e6).toFixed(2) + ' млн' } });
      s.setData(l.points.map(p => ({ time: p.d as Time, value: p.v })));
    }
    ch.timeScale().fitContent();
    return () => ch.remove();
  }, [lines]);
  return <div ref={el} style={{ height: 260, position: 'relative' }} />;
}
