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
  /** Пунктирная линия — для прогнозного «хвоста» (Баффетт): проекцию не выдаём
   *  за реальные данные. Действует на line и area. */
  dashed?: boolean;
  areaTop?: string;
  areaBottom?: string;
  base?: number;
  label: string;
  axisFmt?: (v: number) => string;
  tipFmt?: (v: number) => string;
  lastValueVisible?: boolean;
  zeroLine?: boolean;
  /** Мин. шаг цены оси/пилюли. Дефолт 1 (целые — ОИ/Баффетт); проценты/breadth → 0.01/0.1. */
  minMove?: number;
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
  /** Оверрайд формата меток оси времени (§5.2 / категориальная ось Сезонности).
   *  Не задан → русские год/месяц/день/время (ruTickMark). */
  tickFmt?: (time: number, type: number) => string;
  /** Оверрайд авто-легенды (напр. одна серия «Чистый поток» → 2 пункта «Приток/Отток»). */
  legendItems?: { label: string; color: string }[];
  /** Не строить легенду (когда рисуем свою статичную поверх — Сезонность-Календарь). */
  hideLegend?: boolean;
  /** Метки экспираций (§5.6): серые кружки ОТДЕЛЬНЫМ DOM-слоем у оси дат, а не
   *  нативные маркеры на линии. Перерисовываются на зум/пан/ресайз. */
  expTimes?: number[];
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
export function hideTvLogo() {
  if (tvLogoHidden || typeof document === 'undefined') return;
  tvLogoHidden = true;
  const st = document.createElement('style');
  st.textContent = 'a#tv-attr-logo{display:none!important}';
  document.head.appendChild(st);
}

// Ось времени по-русски (как в макете дизайнера песочницы): год / месяц / день / время.
const MONTHS_RU = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек'];
function ruTickMark(time: unknown, type: number): string {
  const t = typeof time === 'number' ? time : 0;
  const d = new Date(t * 1000);
  if (type === 0) return String(d.getUTCFullYear());
  if (type === 1) return MONTHS_RU[d.getUTCMonth()];
  if (type === 2) return String(d.getUTCDate());
  return String(d.getUTCHours()).padStart(2, '0') + ':' + String(d.getUTCMinutes()).padStart(2, '0');
}

/** §5.2 макета: на дневных графиках ось показывает ТОЛЬКО месяцы и годы —
 *  внутримесячные дневные подписи скрыты (мельтешат и встают неровно при
 *  ресайзе). Для интрадея НЕ подходит (там нужно время) — не передавайте. */
export function monthsYearsTickFmt(time: number, type: number): string {
  const d = new Date(time * 1000);
  if (type === 0) return String(d.getUTCFullYear());
  if (type === 1) return MONTHS_RU[d.getUTCMonth()];
  return '';
}

export default function LwChart({ series, height, dark = true, markers, fitKey, initialBars, tickFmt, legendItems, hideLegend, expTimes }: LwChartProps) {
  const boxRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesApiRef = useRef<ISeriesApi<'Line' | 'Area' | 'Histogram'>[]>([]);
  const defsRef = useRef<LwSeries[]>(series);
  defsRef.current = series;
  // Читаем через ref, чтобы смена пропа-функции/массива не пересоздавала чарт/серии.
  const tickFmtRef = useRef(tickFmt); tickFmtRef.current = tickFmt;
  const legendItemsRef = useRef(legendItems); legendItemsRef.current = legendItems;
  const hideLegendRef = useRef(hideLegend); hideLegendRef.current = hideLegend;
  const expTimesRef = useRef(expTimes); expTimesRef.current = expTimes;
  const drawExpRef = useRef<(() => void) | null>(null);
  const lastFitRef = useRef<string | undefined>(undefined);
  const marginRef = useRef(0.12);
  const legendRef = useRef<HTMLDivElement | null>(null);

  // ── создание чарта один раз ──
  useEffect(() => {
    const box = boxRef.current;
    if (!box) return;
    const c = themeColors(dark);
    const chart = createChart(box, {
      autoSize: true,
      layout: { background: { type: ColorType.Solid, color: 'transparent' }, textColor: c.text, fontFamily: 'Inter, -apple-system, sans-serif', fontSize: 11 },
      localization: { locale: 'ru-RU' },
      grid: { vertLines: { color: c.grid }, horzLines: { color: c.grid } },
      leftPriceScale: { visible: true, borderVisible: false, scaleMargins: { top: 0.12, bottom: 0.06 } },
      rightPriceScale: { visible: true, borderVisible: false, scaleMargins: { top: 0.12, bottom: 0.06 } },
      timeScale: { borderVisible: false, rightOffset: 6, secondsVisible: false, tickMarkFormatter: (time: Time, type: number) => (tickFmtRef.current ? tickFmtRef.current(time as unknown as number, type) : ruTickMark(time, type)) },
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
      'border-radius:7px', 'padding:5px 8px', 'font-size:10.5px', 'color:var(--text-primary,#F5F1E8)',
      'white-space:nowrap', 'box-shadow:0 8px 22px rgba(0,0,0,0.45)', 'font-family:Inter,-apple-system,sans-serif',
    ].join(';');
    box.appendChild(tip);

    // Постоянная центрированная легенда (как в макете дизайнера): серия = цветной
    // сегмент + подпись, всегда видна. Оверлей над графиком, событий не перехватывает.
    // Наполняется в эффекте серий.
    const legend = document.createElement('div');
    legend.style.cssText = [
      'position:absolute', 'top:7px', 'left:50%', 'transform:translateX(-50%)', 'z-index:5',
      'display:flex', 'flex-wrap:wrap', 'justify-content:center', 'gap:14px', 'pointer-events:none',
      'max-width:calc(100% - 130px)',
    ].join(';');
    box.appendChild(legend);
    legendRef.current = legend;

    // Слой экспираций (§5.6): серые кружки над осью дат. Позиция — timeToCoordinate,
    // перерисовка на зум/пан (subscribeVisibleLogicalRangeChange) и ресайз.
    // Цвет — CSS-var (DOM, не canvas) → сам следует за темой панели.
    const expLayer = document.createElement('div');
    expLayer.style.cssText = 'position:absolute;left:0;right:0;height:0;pointer-events:none;z-index:4';
    box.appendChild(expLayer);
    const drawExp = () => {
      const ch = chartRef.current;
      if (!ch) return;
      while (expLayer.firstChild) expLayer.removeChild(expLayer.firstChild);
      const times = expTimesRef.current;
      if (!times || times.length === 0) return;
      const ts = ch.timeScale();
      const axisH = ts.height() || 26;
      expLayer.style.bottom = axisH + 3 + 'px';
      for (const t of times) {
        const x = ts.timeToCoordinate(t as UTCTimestamp);
        if (x == null) continue;
        const dot = document.createElement('span');
        dot.style.cssText = 'position:absolute;top:0;width:7px;height:7px;border-radius:50%;'
          + 'transform:translate(-50%,-50%);opacity:0.8;background:var(--text-secondary,#9A958C);left:' + x + 'px';
        expLayer.appendChild(dot);
      }
    };
    drawExpRef.current = drawExp;
    const onRange = () => drawExp();
    chart.timeScale().subscribeVisibleLogicalRangeChange(onRange);
    const expRo = new ResizeObserver(() => drawExp());
    expRo.observe(box);

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
        dot.style.cssText = 'width:7px;height:7px;border-radius:2px;display:inline-block;flex:0 0 auto;background:' + (def?.color || '#888');
        const lbl = document.createElement('span');
        lbl.textContent = def?.label || '';
        const val = document.createElement('span');
        val.style.cssText = "margin-left:auto;font-weight:700;font-family:'JetBrains Mono',ui-monospace,monospace;font-variant-numeric:tabular-nums;padding-left:14px;color:" + (def?.color || 'inherit');
        val.textContent = def?.tipFmt ? def.tipFmt(dp.value) : String(Math.round(dp.value));
        row.appendChild(dot); row.appendChild(lbl); row.appendChild(val);
        tip.appendChild(row);
      }
      if (!any) { tip.style.display = 'none'; return; }
      tip.style.display = 'block';
      const w = box.clientWidth, tw = tip.offsetWidth;
      // §5.4: после середины графика разворачиваем влево, чтобы не упираться в правый край.
      const rawLeft = param.point.x > w / 2 ? param.point.x - tw - 16 : param.point.x + 16;
      tip.style.left = Math.max(6, Math.min(w - tw - 6, rawLeft)) + 'px';
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
      expRo.disconnect();
      try { chart.timeScale().unsubscribeVisibleLogicalRangeChange(onRange); } catch { /* уже снят */ }
      drawExpRef.current = null;
      chart.remove();
      chartRef.current = null;
      seriesApiRef.current = [];
      if (tip.parentNode) tip.parentNode.removeChild(tip);
      if (legend.parentNode) legend.parentNode.removeChild(legend);
      if (expLayer.parentNode) expLayer.parentNode.removeChild(expLayer);
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
        ? { type: 'custom' as const, minMove: def.minMove ?? 1, formatter: def.axisFmt }
        : undefined;
      const lw = (def.lineWidth ?? 2) as 1 | 2 | 3 | 4;
      const col = rc(def.color);
      let s: ISeriesApi<'Line' | 'Area' | 'Histogram'>;
      const lineStyle = def.dashed ? LineStyle.Dashed : LineStyle.Solid;
      if (def.type === 'line') {
        s = chart.addLineSeries({ color: col, lineWidth: lw, lineStyle, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: def.lastValueVisible ?? true, priceFormat });
      } else if (def.type === 'area') {
        s = chart.addAreaSeries({ lineColor: col, topColor: rc(def.areaTop ?? def.color), bottomColor: def.areaBottom ? rc(def.areaBottom) : 'rgba(0,0,0,0)', lineWidth: lw, lineStyle, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: def.lastValueVisible ?? true, priceFormat });
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

    // Легенда: legendItems (оверрайд) либо по сериям; hideLegend → пусто (рисуем свою поверх).
    const legend = legendRef.current;
    if (legend) {
      while (legend.firstChild) legend.removeChild(legend.firstChild);
      const legItems = hideLegendRef.current ? [] : (legendItemsRef.current ?? series.map((d) => ({ label: d.label, color: d.color })));
      for (const it of legItems) {
        const item = document.createElement('div');
        item.style.cssText = 'display:flex;align-items:center;gap:5px';
        const seg = document.createElement('span');
        seg.style.cssText = 'width:12px;height:2.5px;border-radius:2px;flex:0 0 auto;background:' + rc(it.color);
        const lbl = document.createElement('span');
        lbl.style.cssText = 'font-size:11px;font-weight:600;color:var(--text-primary,#F5F1E8);white-space:nowrap';
        lbl.textContent = it.label || '';
        item.appendChild(seg);
        item.appendChild(lbl);
        legend.appendChild(item);
      }
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

    drawExpRef.current?.(); // метки экспираций — после заливки данных и установки окна
  }, [series, markers, fitKey, expTimes]);

  return <div ref={boxRef} style={{ position: 'relative', width: '100%', height }} />;
}
