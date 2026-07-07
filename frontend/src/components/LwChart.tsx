/**
 * LwChart — обёртка над TradingView Lightweight Charts (open-source, Apache-2.0) в
 * скине Фрейма. Единый движок для линейно-временных индикаторов (ОИ, Сезонность,
 * Баффетт, Сила рынка, Фонды): несколько серий на общей оси времени (линия / area /
 * гистограмма), нативный плавный зум/пан/кроссхэйр, тултип без даты (дата — на оси),
 * серый кроссхэйр, вертикальный масштаб колесом (Shift+колесо или колесо над осью).
 *
 * Атрибуция TradingView (по лицензии Lightweight Charts) — строкой в футере/«О проекте».
 *
 * Время — UNIX-секунды (UTCTimestamp): конвертирует вызывающий (embed), т.к. дневные
 * и интрадей-метки имеют разный формат.
 */
import { useEffect, useRef } from 'react';
import {
  createChart, ColorType, LineStyle, CrosshairMode,
  type IChartApi, type ISeriesApi, type UTCTimestamp, type SeriesMarker, type Time,
} from 'lightweight-charts';

export interface LwPoint { time: number; value: number; color?: string }

export interface LwSeries {
  id: string;
  type: 'line' | 'area' | 'histogram';
  data: LwPoint[];
  color: string;
  scale?: 'left' | 'right';
  lineWidth?: number;
  areaTop?: string;
  areaBottom?: string;
  base?: number;
  label: string;
  axisFmt?: (v: number) => string;
  tipFmt?: (v: number) => string;
  lastValueVisible?: boolean;
  zeroLine?: boolean;
}

export interface LwMarker { time: number; text?: string; color?: string; position?: 'aboveBar' | 'belowBar' | 'inBar' }

interface LwChartProps {
  series: LwSeries[];
  height: number;
  dark?: boolean;
  markers?: LwMarker[];
  /** Меняется (напр. инструмент/таймфрейм) → перецентрируем вид; иначе зум сохраняется. */
  fitKey?: string;
  /** Дефолтное окно при перецентровке: показать последние N баров (напр. год ≈ 252).
   *  Не задано / данных меньше → показать всю историю (fitContent). */
  initialBars?: number;
}

function themeColors(dark: boolean) {
  return {
    text: dark ? '#9A958C' : '#6B6760',
    grid: dark ? 'rgba(245,241,232,0.07)' : 'rgba(10,10,10,0.06)',
    cross: dark ? 'rgba(245,241,232,0.42)' : 'rgba(10,10,10,0.42)',
    lab: dark ? '#26262B' : '#E7E2D6',
  };
}

// Canvas НЕ понимает var(--x)/color-mix() — резолвим в конкретный rgb через probe-элемент
// в контексте темы iframe. Иначе линии рисуются невидимым цветом → пустой график.
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
  } catch {
    return color;
  }
}

// Логотип TradingView скрываем; атрибуция (по лицензии Lightweight Charts) — строкой
// в футере/«О проекте». Стиль инжектим один раз (id-селектор ловит все инстансы).
let tvLogoHidden = false;
function hideTvLogo() {
  if (tvLogoHidden || typeof document === 'undefined') return;
  tvLogoHidden = true;
  const st = document.createElement('style');
  st.textContent = 'a#tv-attr-logo{display:none!important}';
  document.head.appendChild(st);
}

export default function LwChart({ series, height, dark = true, markers, fitKey, initialBars }: LwChartProps) {
  const boxRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesApiRef = useRef<ISeriesApi<'Line' | 'Area' | 'Histogram'>[]>([]);
  const defsRef = useRef<LwSeries[]>(series);
  defsRef.current = series;
  const lastFitRef = useRef<string | undefined>(undefined);
  const marginRef = useRef(0.12);

  // ── создание чарта один раз ──
  useEffect(() => {
    const box = boxRef.current;
    if (!box) return;
    const c = themeColors(dark);
    const chart = createChart(box, {
      autoSize: true,
      layout: { background: { type: ColorType.Solid, color: 'transparent' }, textColor: c.text, fontFamily: 'Inter, -apple-system, sans-serif', fontSize: 11 },
      grid: { vertLines: { color: c.grid }, horzLines: { color: c.grid } },
      leftPriceScale: { visible: true, borderVisible: false, scaleMargins: { top: 0.12, bottom: 0.12 } },
      rightPriceScale: { visible: true, borderVisible: false, scaleMargins: { top: 0.12, bottom: 0.12 } },
      timeScale: { borderVisible: false, rightOffset: 6, secondsVisible: false },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: c.cross, width: 1, style: LineStyle.Dotted, labelBackgroundColor: c.lab },
        horzLine: { color: c.cross, width: 1, style: LineStyle.Dotted, labelBackgroundColor: c.lab },
      },
      handleScroll: { mouseWheel: false, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: false },
      handleScale: { mouseWheel: true, pinch: true, axisPressedMouseMove: { time: false, price: false } },
    });
    chartRef.current = chart;
    hideTvLogo();
    const logoEl = box.querySelector('#tv-attr-logo') as HTMLElement | null;
    if (logoEl) logoEl.style.display = 'none';

    // Тултип — строим безопасно через DOM (textContent), без innerHTML. Цвета —
    // CSS-var темы, чтобы адаптировался к светлой/тёмной внутри iframe.
    const tip = document.createElement('div');
    tip.style.cssText = [
      'position:absolute', 'pointer-events:none', 'z-index:6', 'display:none',
      'background:var(--bg-secondary,#17161A)', 'border:1px solid var(--border-color,rgba(245,241,232,0.18))',
      'border-radius:8px', 'padding:7px 10px', 'font-size:11.5px', 'color:var(--text-primary,#F5F1E8)',
      'white-space:nowrap', 'box-shadow:0 8px 22px rgba(0,0,0,0.45)', 'font-family:Inter,-apple-system,sans-serif',
    ].join(';');
    box.appendChild(tip);

    chart.subscribeCrosshairMove((param) => {
      const defs = defsRef.current;
      const apis = seriesApiRef.current;
      if (!param.time || !param.point || apis.length === 0) { tip.style.display = 'none'; return; }
      while (tip.firstChild) tip.removeChild(tip.firstChild);
      let any = false;
      for (let i = 0; i < apis.length; i++) {
        const dp = param.seriesData.get(apis[i]) as { value?: number } | undefined;
        if (!dp || dp.value == null) continue;
        any = true;
        const def = defs[i];
        const row = document.createElement('div');
        row.style.cssText = 'display:flex;align-items:center;gap:7px;' + (i > 0 ? 'margin-top:4px;' : '');
        const dot = document.createElement('span');
        dot.style.cssText = 'width:8px;height:8px;border-radius:2px;display:inline-block;flex:0 0 auto;background:' + (def?.color || '#888');
        const lbl = document.createElement('span');
        lbl.textContent = def?.label || '';
        const val = document.createElement('span');
        val.style.cssText = 'margin-left:auto;font-weight:700;font-variant-numeric:tabular-nums;padding-left:14px;color:' + (def?.color || 'inherit');
        val.textContent = def?.tipFmt ? def.tipFmt(dp.value) : String(Math.round(dp.value));
        row.appendChild(dot); row.appendChild(lbl); row.appendChild(val);
        tip.appendChild(row);
      }
      if (!any) { tip.style.display = 'none'; return; }
      tip.style.display = 'block';
      const w = box.clientWidth, tw = tip.offsetWidth;
      tip.style.left = Math.max(6, Math.min(w - tw - 6, param.point.x + 16)) + 'px';
      tip.style.top = Math.max(6, param.point.y - 8) + 'px';
    });

    // Вертикальный масштаб колесом: Shift+колесо где угодно ИЛИ колесо над осью цифр.
    // scaleMargins обеих осей симметрично → все линии масштабируются вместе.
    const onWheel = (e: WheelEvent) => {
      const box2 = boxRef.current, ch = chartRef.current;
      if (!box2 || !ch) return;
      const r = box2.getBoundingClientRect();
      const x = e.clientX - r.left;
      const overAxis = x < 60 || x > r.width - 60;
      if (!e.shiftKey && !overAxis) return;
      e.preventDefault();
      e.stopImmediatePropagation();
      marginRef.current = Math.min(0.42, Math.max(0, marginRef.current + (e.deltaY > 0 ? 0.028 : -0.028)));
      const sm = { top: marginRef.current, bottom: marginRef.current };
      ch.priceScale('left').applyOptions({ scaleMargins: sm });
      ch.priceScale('right').applyOptions({ scaleMargins: sm });
    };
    box.addEventListener('wheel', onWheel, { capture: true, passive: false });

    return () => {
      box.removeEventListener('wheel', onWheel, true);
      chart.remove();
      chartRef.current = null;
      seriesApiRef.current = [];
      if (tip.parentNode) tip.parentNode.removeChild(tip);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ── тема ──
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    const c = themeColors(dark);
    chart.applyOptions({
      layout: { textColor: c.text },
      grid: { vertLines: { color: c.grid }, horzLines: { color: c.grid } },
      crosshair: {
        vertLine: { color: c.cross, labelBackgroundColor: c.lab },
        horzLine: { color: c.cross, labelBackgroundColor: c.lab },
      },
    });
  }, [dark]);

  // ── серии (пересоздаём при смене набора/данных, зум сохраняем) ──
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    const savedRange = chart.timeScale().getVisibleLogicalRange();

    seriesApiRef.current.forEach((s) => chart.removeSeries(s));
    seriesApiRef.current = [];

    const usesLeft = series.some((s) => (s.scale ?? 'right') === 'left');
    const usesRight = series.some((s) => (s.scale ?? 'right') === 'right');
    chart.priceScale('left').applyOptions({ visible: usesLeft });
    chart.priceScale('right').applyOptions({ visible: usesRight });

    const box = boxRef.current;
    const rc = (col: string | undefined): string => (box ? resolveColor(box, col) : (col ?? '#888888'));

    for (const def of series) {
      const scaleId = def.scale ?? 'right';
      const priceFormat = def.axisFmt
        ? { type: 'custom' as const, minMove: 1, formatter: def.axisFmt }
        : undefined;
      const lw = (def.lineWidth ?? 2) as 1 | 2 | 3 | 4;
      const col = rc(def.color);
      let s: ISeriesApi<'Line' | 'Area' | 'Histogram'>;
      if (def.type === 'line') {
        s = chart.addLineSeries({ color: col, lineWidth: lw, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: def.lastValueVisible ?? true, priceFormat });
      } else if (def.type === 'area') {
        s = chart.addAreaSeries({ lineColor: col, topColor: rc(def.areaTop ?? def.color), bottomColor: def.areaBottom ? rc(def.areaBottom) : 'rgba(0,0,0,0)', lineWidth: lw, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: def.lastValueVisible ?? true, priceFormat });
      } else {
        s = chart.addHistogramSeries({ color: col, base: def.base ?? 0, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: def.lastValueVisible ?? false, priceFormat });
      }
      try {
        s.setData(def.data.map((p) => ({ time: p.time as UTCTimestamp, value: p.value, ...(p.color ? { color: rc(p.color) } : {}) })));
      } catch (err) {
        console.error('LwChart setData failed:', def.id, err);
      }
      if (def.zeroLine) {
        s.createPriceLine({ price: 0, color: col, lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: false, title: '' });
      }
      seriesApiRef.current.push(s);
    }

    if (markers && seriesApiRef.current[0]) {
      const ms: SeriesMarker<Time>[] = markers.map((m) => ({
        time: m.time as UTCTimestamp,
        position: m.position ?? 'aboveBar',
        color: rc(m.color ?? '#9A958C'),
        shape: 'circle',
        text: m.text,
      }));
      seriesApiRef.current[0].setMarkers(ms);
    }

    if (fitKey !== lastFitRef.current) {
      lastFitRef.current = fitKey;
      const total = Math.max(0, ...series.map((s) => s.data.length));
      if (initialBars && total > initialBars) {
        chart.timeScale().setVisibleLogicalRange({ from: total - initialBars, to: total + 2 });
      } else {
        chart.timeScale().fitContent();
      }
    } else if (savedRange) {
      chart.timeScale().setVisibleLogicalRange(savedRange);
    }
  }, [series, markers, fitKey]);

  return <div ref={boxRef} style={{ position: 'relative', width: '100%', height }} />;
}
