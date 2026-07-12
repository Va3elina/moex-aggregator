/**
 * LwChartPanes — N стэкнутых Lightweight-графиков как ОДИН индикатор (§5.7 макета).
 * Для «Силы рынка»: сверху индекс, снизу breadth, между ними:
 *   - общая ось времени: пан/зум синхронны (subscribeVisibleLogicalRangeChange
 *     с guard-флагом от рекурсии);
 *   - общий кроссхэйр: setCrosshairPosition на соседях с _suppress-гейтом
 *     ПЕРВОЙ строкой обработчика (иначе бесконечный цикл вешает вкладку);
 *   - ЕДИНЫЙ тултип: под курсором на любой панели показывает значения ВСЕХ
 *     панелей на этой дате (лукап по time-индексированным Map'ам);
 *   - ось дат видна ТОЛЬКО на нижней панели.
 *
 * Сознательно отдельный компонент, а не ветка внутри LwChart: единственный
 * потребитель — Сила рынка, а боевой одиночный LwChart (ОИ/Баффетт/Фонды/
 * Сезонность) не трогаем вообще. Общие типы (LwSeries) импортируются оттуда.
 */
import { useContext, useEffect, useRef } from 'react';
import {
  createChart, ColorType, LineStyle, CrosshairMode,
  type IChartApi, type ISeriesApi, type UTCTimestamp, type Time, type LogicalRange,
} from 'lightweight-charts';
import { ChartPrefsCtx, hideTvLogo, monthsYearsTickFmt, type LwSeries } from './LwChart';

export interface LwPane {
  series: LwSeries[];
  /** Доля высоты (flex-grow). Дефолт 1. Сила рынка в макете ≈ 1.1 / 0.9. */
  flex?: number;
}

interface LwChartPanesProps {
  panes: LwPane[];
  dark?: boolean;
  fitKey?: string;
  initialBars?: number;
  /** Формат оси времени нижней панели. Дефолт — §5.2 (только месяцы+годы). */
  tickFmt?: (time: number, type: number) => string;
}

function themeColors(dark: boolean) {
  return {
    text: dark ? '#9A958C' : '#6B6760',
    grid: dark ? 'rgba(245,241,232,0.07)' : 'rgba(10,10,10,0.06)',
    cross: dark ? 'rgba(245,241,232,0.42)' : 'rgba(10,10,10,0.42)',
    lab: dark ? '#26262B' : '#E7E2D6',
  };
}

// Canvas НЕ понимает var()/color-mix — резолвим через probe (копия идиомы LwChart).
function resolveColor(box: HTMLElement, color: string | undefined): string {
  if (!color) return '#888888';
  if (!color.includes('var(') && !color.includes('color-mix')) return color;
  try {
    const probe = document.createElement('span');
    probe.style.color = color;
    probe.style.position = 'absolute';
    probe.style.pointerEvents = 'none';
    box.appendChild(probe);
    const rgb = getComputedStyle(probe).color;
    box.removeChild(probe);
    return rgb || color;
  } catch { return color; }
}

type AnySeries = ISeriesApi<'Line' | 'Area' | 'Histogram'>;

export default function LwChartPanes({ panes, dark = true, fitKey, initialBars, tickFmt }: LwChartPanesProps) {
  const rootRef = useRef<HTMLDivElement>(null);
  const chartsRef = useRef<IChartApi[]>([]);
  const apisRef = useRef<AnySeries[][]>([]);          // [pane][series]
  const mapsRef = useRef<Map<number, number>[][]>([]); // [pane][series] time→value
  const panesRef = useRef<LwPane[]>(panes); panesRef.current = panes;
  const tipsRef = useRef<HTMLDivElement[]>([]);
  const legendsRef = useRef<HTMLDivElement[]>([]);
  const lastFitRef = useRef<string | undefined>(undefined);
  const tickFmtRef = useRef(tickFmt); tickFmtRef.current = tickFmt;
  const paneCount = panes.length;
  const chartPrefs = useContext(ChartPrefsCtx);

  // ── создание N чартов + связка (пересоздаётся при смене числа панелей) ──
  useEffect(() => {
    const root = rootRef.current;
    if (!root || paneCount === 0) return;
    const c = themeColors(dark);
    const charts: IChartApi[] = [];
    const tips: HTMLDivElement[] = [];
    const legends: HTMLDivElement[] = [];
    const unsubs: (() => void)[] = [];
    const boxes = Array.from(root.children) as HTMLElement[];

    let syncingRange = false; // guard: программная установка диапазона
    let suppress = false;     // _suppress: программный кроссхэйр соседа

    for (let i = 0; i < paneCount; i++) {
      const box = boxes[i];
      if (!box) continue;
      const isLast = i === paneCount - 1;
      const chart = createChart(box, {
        autoSize: true,
        layout: { background: { type: ColorType.Solid, color: 'transparent' }, textColor: c.text, fontFamily: 'Inter, -apple-system, sans-serif', fontSize: 11 },
        localization: { locale: 'ru-RU' },
        grid: { vertLines: { color: c.grid }, horzLines: { color: c.grid } },
        leftPriceScale: { visible: false, borderVisible: false, scaleMargins: { top: 0.12, bottom: 0.06 } },
        rightPriceScale: { visible: true, borderVisible: false, scaleMargins: { top: 0.12, bottom: 0.06 } },
        timeScale: {
          borderVisible: false, rightOffset: 6, secondsVisible: false,
          visible: isLast, // §5.7: ось дат только на нижней панели
          tickMarkFormatter: (time: Time, type: number) =>
            (tickFmtRef.current ?? monthsYearsTickFmt)(time as unknown as number, type),
        },
        crosshair: {
          mode: CrosshairMode.Normal,
          vertLine: { color: c.cross, width: 1, style: LineStyle.Dotted, labelBackgroundColor: c.lab },
          horzLine: { color: c.cross, width: 1, style: LineStyle.Dotted, labelBackgroundColor: c.lab },
        },
        handleScroll: { mouseWheel: false, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: false },
        handleScale: { mouseWheel: true, pinch: true, axisPressedMouseMove: { time: false, price: false } },
      });
      charts.push(chart);
      hideTvLogo();
      const logoEl = box.querySelector('#tv-attr-logo') as HTMLElement | null;
      if (logoEl) logoEl.style.display = 'none';

      // Единый тултип этой панели (строки всех панелей дописываются в кроссхэйре).
      const tip = document.createElement('div');
      tip.style.cssText = [
        'position:absolute', 'pointer-events:none', 'z-index:6', 'display:none',
        'background:var(--bg-secondary,#17161A)', 'border:1px solid var(--border-color,rgba(245,241,232,0.18))',
        'border-radius:7px', 'padding:5px 8px', 'font-size:10.5px', 'color:var(--text-primary,#F5F1E8)',
        'white-space:nowrap', 'box-shadow:0 8px 22px rgba(0,0,0,0.45)', 'font-family:Inter,-apple-system,sans-serif',
      ].join(';');
      box.appendChild(tip);
      tips.push(tip);

      // Центрированная легенда панели (= заголовок серии, как в макете).
      const legend = document.createElement('div');
      legend.style.cssText = [
        'position:absolute', 'top:7px', 'left:50%', 'transform:translateX(-50%)', 'z-index:5',
        'display:flex', 'flex-wrap:wrap', 'justify-content:center', 'gap:14px', 'pointer-events:none',
        'max-width:calc(100% - 130px)',
      ].join(';');
      box.appendChild(legend);
      legends.push(legend);
    }

    // ── синк диапазона времени (guard от рекурсии) ──
    charts.forEach((chart, i) => {
      const handler = (range: LogicalRange | null) => {
        if (syncingRange || !range) return;
        syncingRange = true;
        try {
          charts.forEach((other, j) => { if (j !== i) other.timeScale().setVisibleLogicalRange(range); });
        } finally { syncingRange = false; }
      };
      chart.timeScale().subscribeVisibleLogicalRangeChange(handler);
      unsubs.push(() => chart.timeScale().unsubscribeVisibleLogicalRangeChange(handler));
    });

    // ── общий кроссхэйр + единый тултип ──
    charts.forEach((chart, i) => {
      const handler = (param: { time?: unknown; point?: { x: number; y: number } }) => {
        if (suppress) return; // ПЕРВОЙ строкой — иначе рекурсия через setCrosshairPosition
        const tip = tips[i];
        const t = typeof param.time === 'number' ? param.time : null;
        if (t == null || !param.point) {
          tips.forEach((tp) => { tp.style.display = 'none'; });
          suppress = true;
          try { charts.forEach((other, j) => { if (j !== i) other.clearCrosshairPosition(); }); }
          finally { suppress = false; }
          return;
        }
        // Строки ВСЕХ панелей на этой дате (лукап по time-Map'ам).
        while (tip.firstChild) tip.removeChild(tip.firstChild);
        let any = false;
        panesRef.current.forEach((pane, pi) => {
          pane.series.forEach((def, si) => {
            const v = mapsRef.current[pi]?.[si]?.get(t);
            if (v == null) return;
            any = true;
            const row = document.createElement('div');
            row.style.cssText = 'display:flex;align-items:center;gap:7px;' + (tip.childNodes.length > 0 ? 'margin-top:4px;' : '');
            const dot = document.createElement('span');
            dot.style.cssText = 'width:7px;height:7px;border-radius:2px;display:inline-block;flex:0 0 auto;background:' + (def.color || '#888');
            const lbl = document.createElement('span');
            lbl.textContent = def.label || '';
            const val = document.createElement('span');
            val.style.cssText = "margin-left:auto;font-weight:700;font-family:'JetBrains Mono',ui-monospace,monospace;font-variant-numeric:tabular-nums;padding-left:14px;color:" + (def.color || 'inherit');
            val.textContent = def.tipFmt ? def.tipFmt(v) : String(Math.round(v));
            row.appendChild(dot); row.appendChild(lbl); row.appendChild(val);
            tip.appendChild(row);
          });
        });
        // Скрыть тултипы неактивных панелей, кроссхэйр — на соседей.
        tips.forEach((tp, j) => { if (j !== i) tp.style.display = 'none'; });
        if (!any) { tip.style.display = 'none'; return; }
        tip.style.display = 'block';
        const box = boxes[i];
        const w = box.clientWidth, tw = tip.offsetWidth;
        // §R2-25: флип по середине ПОЛЯ (без правой оси). Левая ось скрыта →
        // координаты param.point совпадают с боксом, перевод не нужен.
        const paneW = chart.timeScale().width() || w;
        const rawLeft = param.point.x > paneW / 2 ? param.point.x - tw - 16 : param.point.x + 16;
        tip.style.left = Math.max(6, Math.min(w - tw - 6, rawLeft)) + 'px';
        tip.style.top = Math.max(6, param.point.y - 8) + 'px';
        suppress = true;
        try {
          charts.forEach((other, j) => {
            if (j === i) return;
            const firstApi = apisRef.current[j]?.[0];
            const v = mapsRef.current[j]?.[0]?.get(t);
            if (firstApi && v != null) other.setCrosshairPosition(v, t as UTCTimestamp, firstApi);
            else other.clearCrosshairPosition();
          });
        } finally { suppress = false; }
      };
      chart.subscribeCrosshairMove(handler);
      unsubs.push(() => chart.unsubscribeCrosshairMove(handler));
    });

    chartsRef.current = charts;
    tipsRef.current = tips;
    legendsRef.current = legends;
    lastFitRef.current = undefined; // свежие чарты — форсим fit при первом заливе серий

    return () => {
      unsubs.forEach((u) => { try { u(); } catch { /* noop */ } });
      charts.forEach((ch) => ch.remove());
      tips.forEach((tp) => tp.parentNode?.removeChild(tp));
      legends.forEach((lg) => lg.parentNode?.removeChild(lg));
      chartsRef.current = [];
      apisRef.current = [];
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [paneCount]);

  // ── тема + дефолты §9 (сетка/кроссхэйр) ──
  useEffect(() => {
    const c = themeColors(dark);
    const gridCol = chartPrefs?.grid === false ? 'rgba(0,0,0,0)' : c.grid;
    chartsRef.current.forEach((chart) => {
      chart.applyOptions({
        layout: { textColor: c.text },
        grid: { vertLines: { color: gridCol }, horzLines: { color: gridCol } },
        crosshair: chartPrefs?.crosshair === false
          ? { mode: CrosshairMode.Hidden }
          : {
              mode: CrosshairMode.Normal,
              vertLine: { color: c.cross, labelBackgroundColor: c.lab },
              horzLine: { color: c.cross, labelBackgroundColor: c.lab },
            },
      });
    });
  }, [dark, chartPrefs]);

  // ── серии всех панелей ──
  useEffect(() => {
    const charts = chartsRef.current;
    if (charts.length !== paneCount || paneCount === 0) return;
    const root = rootRef.current;
    if (!root) return;
    const boxes = Array.from(root.children) as HTMLElement[];
    const savedRange = charts[charts.length - 1].timeScale().getVisibleLogicalRange();

    apisRef.current.forEach((apis, i) => apis.forEach((s) => { try { charts[i]?.removeSeries(s); } catch { /* removed */ } }));
    apisRef.current = panes.map(() => []);
    mapsRef.current = panes.map(() => []);

    panes.forEach((pane, i) => {
      const chart = charts[i];
      const box = boxes[i];
      if (!chart || !box) return;
      const rc = (col: string | undefined): string => resolveColor(box, col);
      for (const def of pane.series) {
        const priceFormat = def.axisFmt
          ? { type: 'custom' as const, minMove: def.minMove ?? 1, formatter: def.axisFmt }
          : undefined;
        const lw = ((chartPrefs?.lineWidth ?? def.lineWidth ?? 2)) as 1 | 2 | 3 | 4;
        // Явный def.lastValueVisible побеждает глобальный тумблер песочницы (см. LwChart.tsx).
        const lastLine = def.lastValueVisible ?? chartPrefs?.lastValue ?? true;
        const lastHist = def.lastValueVisible ?? chartPrefs?.lastValue ?? false;
        const col = rc(def.color);
        const lineStyle = def.dashed ? LineStyle.Dashed : LineStyle.Solid;
        let s: AnySeries;
        if (def.type === 'line') {
          s = chart.addLineSeries({ color: col, lineWidth: lw, lineStyle, priceScaleId: 'right', priceLineVisible: false, lastValueVisible: lastLine, priceFormat });
        } else if (def.type === 'area') {
          s = chart.addAreaSeries({ lineColor: col, topColor: rc(def.areaTop ?? def.color), bottomColor: def.areaBottom ? rc(def.areaBottom) : 'rgba(0,0,0,0)', lineWidth: lw, lineStyle, priceScaleId: 'right', priceLineVisible: false, lastValueVisible: lastLine, priceFormat });
        } else {
          s = chart.addHistogramSeries({ color: col, base: def.base ?? 0, priceScaleId: 'right', priceLineVisible: false, lastValueVisible: lastHist, priceFormat });
        }
        try {
          s.setData(def.data.map((p) => ({ time: p.time as UTCTimestamp, value: p.value, ...(p.color ? { color: rc(p.color) } : {}) })));
        } catch (err) {
          console.error('LwChartPanes setData failed:', def.id, err);
        }
        if (def.zeroLine) {
          s.createPriceLine({ price: 0, color: col, lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: false, title: '' });
        }
        apisRef.current[i].push(s);
        mapsRef.current[i].push(new Map(def.data.map((p) => [p.time, p.value])));
      }
      // Легенда панели.
      const legend = legendsRef.current[i];
      if (legend) {
        while (legend.firstChild) legend.removeChild(legend.firstChild);
        for (const def of pane.series) {
          const item = document.createElement('div');
          item.style.cssText = 'display:flex;align-items:center;gap:5px';
          const seg = document.createElement('span');
          seg.style.cssText = 'width:12px;height:2.5px;border-radius:2px;flex:0 0 auto;background:' + rc(def.color);
          const lbl = document.createElement('span');
          lbl.style.cssText = 'font-size:11px;font-weight:600;color:var(--text-primary,#F5F1E8);white-space:nowrap';
          lbl.textContent = def.label || '';
          item.appendChild(seg);
          item.appendChild(lbl);
          legend.appendChild(item);
        }
      }
    });

    // Выровнять ширину правой ценовой шкалы между панелями. Каждая панель сама
    // подгоняет ширину шкалы под самый длинный лейбл своих значений (индекс
    // «2 226» шире, чем breadth «4,3%») — из-за этого plot area отличается по
    // ширине между панелями и общая вертикаль кроссхэйра/сетки едет по X. Фикс —
    // штатный приём lightweight-charts для вертикального стека чартов (см. доку
    // minimumWidth): меряем максимум и форсим его на всех панелях. rAF — библиотека
    // пересчитывает фактическую ширину шкалы под новые данные не синхронно с
    // setData, а в своём цикле рендера; без задержки .width() отдаёт старое значение.
    requestAnimationFrame(() => {
      const maxScaleW = Math.max(0, ...charts.map((ch) => ch.priceScale('right').width()));
      if (maxScaleW > 0) {
        charts.forEach((ch) => ch.priceScale('right').applyOptions({ minimumWidth: maxScaleW }));
      }
    });

    // Fit/восстановление зума — через нижнюю панель, синк разнесёт по остальным.
    const lead = charts[charts.length - 1];
    if (fitKey !== lastFitRef.current) {
      lastFitRef.current = fitKey;
      const total = Math.max(0, ...panes.flatMap((p) => p.series.map((s) => s.data.length)));
      if (initialBars && total > initialBars) {
        lead.timeScale().setVisibleLogicalRange({ from: total - initialBars, to: total + 2 });
      } else {
        charts.forEach((ch) => ch.timeScale().fitContent());
      }
    } else if (savedRange) {
      lead.timeScale().setVisibleLogicalRange(savedRange);
    }
  }, [panes, fitKey, initialBars, paneCount, chartPrefs]);

  return (
    <div ref={rootRef} style={{ display: 'flex', flexDirection: 'column', width: '100%', height: '100%' }}>
      {panes.map((p, i) => (
        <div key={i} style={{ position: 'relative', minHeight: 0, flex: `${p.flex ?? 1} 1 0%` }} />
      ))}
    </div>
  );
}
