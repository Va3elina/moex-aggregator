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
import { createContext, useContext, useEffect, useRef } from 'react';
import {
  createChart, ColorType, LineStyle, CrosshairMode,
  type IChartApi, type ISeriesApi, type UTCTimestamp, type SeriesMarker, type Time, type IPriceLine,
  type Logical, type Coordinate,
} from 'lightweight-charts';
import ChartWatermark from './ChartWatermark';

// value ВСЕГДА = close (для OHLC-серий тоже) — чтобы пилс последнего значения,
// сигнатура reveal-анимации и alert-«+» (все читают def.data[].value) работали
// без спец-веток. open/high/low нужны только candlestick/bar при отрисовке.
export interface LwPoint { time: number; value: number; color?: string; open?: number; high?: number; low?: number; close?: number }

export interface LwSeries {
  id: string;
  type: 'line' | 'area' | 'histogram' | 'candlestick' | 'bar';
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

/** §OI-4: метка экспирации (смена контракта) — кружок у оси дат с hover-тултипом,
 *  как на сайте (SimpleChart annotations). label — короткий код нового контракта
 *  (2 символа в кружке), description — полный текст тултипа (напр. «SiM6 → SiU6»). */
export interface LwExpiration { time: number; label: string; description: string }

// ── Рисование (модель TradingView): фигуры живут в координатах {logical, price},
// перепроецируются на зум/пан/ресайз (как метки экспираций) → «ездят» с графиком.
// logical — дробный индекс бара (timeScale.coordinateToLogical) → свободная позиция.
export type LwDrawShape = 'trend' | 'hline' | 'vline' | 'ray' | 'arrow' | 'rect' | 'ellipse' | 'fib' | 'brush' | 'text' | 'ruler';
export type LwDrawTool = 'select' | LwDrawShape;
export interface LwDrawPoint { logical: number; price: number }
export type LwDash = 'solid' | 'dashed' | 'dotted';
export type LwMagnet = 'off' | 'weak' | 'strong';   // магнит TradingView: слабый (рядом) / сильный (всегда)
export interface LwDrawing { id: string; tool: LwDrawShape; pts: LwDrawPoint[]; color: string; width: number; text?: string; dash?: LwDash; opacity?: number; hidden?: boolean }

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
  /** Интрадей-таймфреймы (ОИ 5м/1ч): без этого lightweight-charts никогда не
   *  генерирует тик-марки типа Time — ось всегда только дата, даже если
   *  бары идут внутри одного дня. Не задан → false (дневные графики как раньше). */
  timeVisible?: boolean;
  /** Оверрайд авто-легенды (напр. одна серия «Чистый поток» → 2 пункта «Приток/Отток»). */
  legendItems?: { label: string; color: string }[];
  /** Не строить легенду (когда рисуем свою статичную поверх — Сезонность-Календарь). */
  hideLegend?: boolean;
  /** Метки экспираций (§OI-4): кружок с кодом контракта у оси дат + hover-тултип
   *  (from→to) + пунктирная вертикаль — как на сайте (SimpleChart annotations).
   *  Отдельный DOM-слой; перерисовываются на зум/пан/ресайз. */
  expirations?: LwExpiration[];
  /** Ценовые уровни-линии (§5.6 алерты): пунктир на первой серии указанной оси. */
  priceLines?: { price: number; color?: string; scale?: 'left' | 'right'; title?: string }[];
  /** §OI-3: клик по «+» у ценовой оси → создать алерт на уровне под курсором (как в
   *  TradingView / на сайте). axis — какая ось (left=цена, right=показатель ОИ);
   *  price — уровень под курсором; currentValue — последнее значение серии этой оси. */
  onCreateAlert?: (p: { axis: 'left' | 'right'; price: number; currentValue: number }) => void;
  /** На каких осях показывать кликабельный «+» (рисуется только если серия оси видна). */
  alertAxes?: ('left' | 'right')[];
  /** §OI-6: водяной знак «Фрейм» в левом-нижнем углу графика (как на сайте). По
   *  умолчанию включён; передайте false чтобы скрыть. */
  watermark?: boolean;
  /** §OI-6: анимация появления серий (рост из плоской линии, easeOutCubic) при
   *  первой отрисовке и смене fitKey. Не задан → без анимации (как раньше). */
  animate?: boolean;
  /** Рисование (модель TradingView). Аддитивно — не задано → слой не создаётся,
   *  прочие embed'ы не тронуты. Только ОИ передаёт эти пропы. */
  drawActive?: boolean;                 // карандаш вкл → оверлей ловит события, пан выкл
  drawTool?: LwDrawTool;                // активный инструмент
  drawings?: LwDrawing[];               // фигуры (из embed-стейта, персист снаружи)
  onDrawingsChange?: (d: LwDrawing[]) => void;
  drawColor?: string;                   // цвет для новых фигур
  drawWidth?: number;                   // толщина для новых фигур
  selectedDrawId?: string | null;       // выделенная фигура (для подсветки/удаления)
  onSelectDraw?: (id: string | null) => void;
  drawMagnet?: LwMagnet;                 // магнит: off | weak (рядом) | strong (всегда) — к ближайшему OHLC
  drawHidden?: boolean;                  // временно скрыть все рисунки (глаз)
  drawLocked?: boolean;                  // замок: запретить выделение/перемещение
  drawDash?: LwDash;                     // стиль линии для НОВЫХ фигур
  drawOpacity?: number;                  // прозрачность (0..1) для НОВЫХ фигур
  onToolReset?: () => void;              // после создания фигуры → сбросить инструмент в «выбор» (один-за-раз)
}

/** Глобальные дефолты внешнего вида графиков ПЕСОЧНИЦЫ (§9). Провайдит SandboxPage;
 *  вне песочницы контекст null → поведение движка прежнее. Применяется к LwChart и
 *  LwChartPanes. */
export interface ChartPrefs { lineWidth?: 1 | 2 | 3; crosshair?: boolean; grid?: boolean; lastValue?: boolean }
export const ChartPrefsCtx = createContext<ChartPrefs | null>(null);

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
// Тип тика (0..3) — РОДНАЯ логика lightweight-charts (её же использует сам
// TradingView): у каждого бара есть вес относительно ПРЕДЫДУЩЕГО бара (пересёк
// границу года/месяца/дня → Year/Month/Day; иначе — самая крупная внутрисуточная
// граница: 12ч/6ч/3ч/1ч/30м/5м/1м). Алгоритм расстановки тиков предпочитает
// показывать САМЫЙ «круглый» доступный бар — поэтому на мультидневном интрадей-
// зуме иногда мелькает одинокая «12:00» среди чисел дней (бар ровно в полдень
// имеет более высокий вес, чем соседние часовые бары) — это не баг, а то же
// самое поведение, что и в настоящем TradingView; тип 3 нам приходит уже
// свёрнутым (Time), различить «12:00» и «13:00» на этом уровне API нельзя.
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

export default function LwChart({ series, height, dark = true, markers, fitKey, initialBars, tickFmt, timeVisible, legendItems, hideLegend, expirations, priceLines, onCreateAlert, alertAxes, watermark, animate, drawActive, drawTool, drawings, onDrawingsChange, drawColor, drawWidth, selectedDrawId, onSelectDraw, drawMagnet, drawHidden, drawLocked, drawDash, drawOpacity, onToolReset }: LwChartProps) {
  const boxRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesApiRef = useRef<ISeriesApi<'Line' | 'Area' | 'Histogram' | 'Candlestick' | 'Bar'>[]>([]);
  const defsRef = useRef<LwSeries[]>(series);
  defsRef.current = series;
  // Читаем через ref, чтобы смена пропа-функции/массива не пересоздавала чарт/серии.
  const tickFmtRef = useRef(tickFmt); tickFmtRef.current = tickFmt;
  const legendItemsRef = useRef(legendItems); legendItemsRef.current = legendItems;
  const hideLegendRef = useRef(hideLegend); hideLegendRef.current = hideLegend;
  const expRef = useRef(expirations); expRef.current = expirations;
  const drawExpRef = useRef<(() => void) | null>(null);
  // Рисование — пропы через рефы (setup-эффект `[]` читает актуальные значения).
  const drawActiveRef = useRef(drawActive); drawActiveRef.current = drawActive;
  const drawToolRef = useRef(drawTool); drawToolRef.current = drawTool;
  const drawingsRef = useRef(drawings); drawingsRef.current = drawings;
  const onDrawingsChangeRef = useRef(onDrawingsChange); onDrawingsChangeRef.current = onDrawingsChange;
  const drawColorRef = useRef(drawColor); drawColorRef.current = drawColor;
  const drawWidthRef = useRef(drawWidth); drawWidthRef.current = drawWidth;
  const selectedDrawIdRef = useRef(selectedDrawId); selectedDrawIdRef.current = selectedDrawId;
  const onSelectDrawRef = useRef(onSelectDraw); onSelectDrawRef.current = onSelectDraw;
  const drawMagnetRef = useRef(drawMagnet); drawMagnetRef.current = drawMagnet;
  const drawHiddenRef = useRef(drawHidden); drawHiddenRef.current = drawHidden;
  const drawLockedRef = useRef(drawLocked); drawLockedRef.current = drawLocked;
  const drawDashRef = useRef(drawDash); drawDashRef.current = drawDash;
  const drawOpacityRef = useRef(drawOpacity); drawOpacityRef.current = drawOpacity;
  const onToolResetRef = useRef(onToolReset); onToolResetRef.current = onToolReset;
  const drawShapesRef = useRef<(() => void) | null>(null);
  // §OI-3 axis-алерты: пропы через ref (смена не пересоздаёт чарт); axisInfoRef —
  // первая серия каждой оси + её форматтер + последнее значение (заполняется в
  // эффекте серий); layoutAlertRef — пересчёт геометрии полос у осей.
  const alertAxesRef = useRef(alertAxes); alertAxesRef.current = alertAxes;
  const onCreateAlertRef = useRef(onCreateAlert); onCreateAlertRef.current = onCreateAlert;
  const axisInfoRef = useRef<{ [k in 'left' | 'right']?: { api: ISeriesApi<'Line' | 'Area' | 'Histogram' | 'Candlestick' | 'Bar'>; fmt?: (v: number) => string; last?: number; color?: string; lastVisible?: boolean } }>({});
  const layoutAlertRef = useRef<(() => void) | null>(null);
  // §R2-17: превью-уровень алерта = НАТИВНАЯ price line (серый пунктир как кроссхэйр
  // + цветной лейбл значения, ВЫРОВНЕННЫЙ по числам оси нативно). Одна на ось.
  const previewLineRef = useRef<{ [k in 'left' | 'right']?: IPriceLine }>({});
  const crossColorRef = useRef<string>('rgba(245,241,232,0.42)');   // цвет пунктира = кроссхэйр (тема)
  const dataSigRef = useRef<string>('');   // §R2-6 сигнатура данных для reveal-анимации
  const chartPrefs = useContext(ChartPrefsCtx);
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
      timeScale: {
        borderVisible: false, rightOffset: 6, timeVisible: !!timeVisible, secondsVisible: false,
        tickMarkFormatter: (time: Time, type: number) => (tickFmtRef.current ? tickFmtRef.current(time as unknown as number, type) : ruTickMark(time, type)),
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: c.cross, width: 1, style: LineStyle.Dotted, labelBackgroundColor: c.lab },
        // §R2-13/14: при активных axis-алертах прячем и линию, и ЛЕЙБЛ горизонтали
        // кроссхэйра (её роль играет наш inset-пунктир + пилс алерта; иначе серый
        // лейбл кроссхэйра дублировал пилс под ним).
        horzLine: { color: c.cross, width: 1, style: LineStyle.Dotted, labelBackgroundColor: c.lab, visible: !(alertAxes && alertAxes.length), labelVisible: !(alertAxes && alertAxes.length) },
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

    // ── Слой экспираций (§OI-4): кружок с кодом контракта у оси дат + hover-тултип
    // (from→to) + общая пунктирная вертикаль — как на сайте (SimpleChart annotations).
    // Позиции — timeToCoordinate; перерисовка на зум/пан/ресайз. Цвета — CSS-var (DOM,
    // не canvas) → следуют за темой панели.
    const expLayer = document.createElement('div');
    expLayer.style.cssText = 'position:absolute;left:0;right:0;height:0;pointer-events:none;z-index:4';
    box.appendChild(expLayer);
    const expGuide = document.createElement('div');
    expGuide.style.cssText = 'position:absolute;top:0;width:0;display:none;pointer-events:none;z-index:3;border-left:1px dashed var(--text-secondary,#9A958C);opacity:0.45';
    box.appendChild(expGuide);
    const plotHeight = () => {
      const ch = chartRef.current;
      const axisH = ch ? (ch.timeScale().height() || 26) : 26;
      return Math.max(0, box.clientHeight - axisH);
    };
    const drawExp = () => {
      const ch = chartRef.current;
      if (!ch) return;
      while (expLayer.firstChild) expLayer.removeChild(expLayer.firstChild);
      expGuide.style.display = 'none';
      const exps = expRef.current;
      if (!exps || exps.length === 0) return;
      const ts = ch.timeScale();
      const axisH = ts.height() || 26;
      expLayer.style.bottom = axisH + 'px';
      // §R2-23: timeToCoordinate меряет от ЛЕВОГО КРАЯ ПОЛЯ (после левой ценовой оси),
      // а слой — от края бокса → переводим (+paneL). Кружок, пересёкший ценовую ось
      // (левую или правую), не рисуем вовсе — иначе при ресайзе/пане он наезжает на
      // числа оси.
      const paneL = ch.priceScale('left').width() || 0;
      const paneW = ts.width() || Math.max(0, box.clientWidth - paneL - (ch.priceScale('right').width() || 0));
      const R = 17 / 2;   // половина кружка
      for (const ex of exps) {
        const x = ts.timeToCoordinate(ex.time as UTCTimestamp);
        if (x == null || x < R || x > paneW - R) continue;
        const bx = paneL + x;
        const circle = document.createElement('div');
        circle.style.cssText = [
          'position:absolute', 'bottom:2px', 'left:' + bx + 'px', 'transform:translateX(-50%)',
          'width:17px', 'height:17px', 'border-radius:50%', 'display:flex', 'align-items:center',
          'justify-content:center', 'font-size:8.5px', 'font-weight:700', 'cursor:default',
          'pointer-events:auto', 'opacity:0.5', 'transition:opacity 0.12s', 'box-sizing:border-box',
          'background:var(--bg-secondary,#26262B)', 'color:var(--text-secondary,#9A958C)',
          'border:1px solid var(--border-color,rgba(245,241,232,0.18))',
        ].join(';');
        circle.textContent = (ex.label || '').slice(0, 2);
        const tipEl = document.createElement('div');
        tipEl.style.cssText = [
          'position:absolute', 'bottom:calc(100% + 6px)', 'left:50%', 'transform:translateX(-50%)',
          'display:none', 'white-space:nowrap', 'pointer-events:none', 'z-index:8',
          'background:var(--bg-secondary,#17161A)', 'color:var(--text-primary,#F5F1E8)',
          'border:1px solid var(--border-color,rgba(245,241,232,0.18))', 'border-radius:7px',
          'padding:4px 8px', 'font-size:10.5px', 'font-weight:600', 'box-shadow:0 8px 22px rgba(0,0,0,0.45)',
        ].join(';');
        tipEl.textContent = ex.description || '';
        circle.appendChild(tipEl);
        circle.addEventListener('mouseenter', () => {
          circle.style.opacity = '1';
          tipEl.style.display = 'block';
          expGuide.style.left = bx + 'px';
          expGuide.style.height = plotHeight() + 'px';
          expGuide.style.display = 'block';
        });
        circle.addEventListener('mouseleave', () => {
          circle.style.opacity = '0.5';
          tipEl.style.display = 'none';
          expGuide.style.display = 'none';
        });
        expLayer.appendChild(circle);
      }
    };
    drawExpRef.current = drawExp;

    // ── §OI-3/§R2-21 axis-алерты: превью-уровень = НАТИВНАЯ price line (только серый
    // ПУНКТИР, цвет кроссхэйра, 1:1 с вертикалью) + DOM value-пилс значения ПОВЕРХ оси
    // (z выше канваса → ложится на нативный лейбл последнего значения, не толкает и не
    // прячет его), выровненный по правой колонке чисел оси. Плюс DOM-кружок «+» в поле.
    // DOM-слой pointer-events:none (кроме кликабельного кружка) — не влияет на график.
    const alertChips: { [k in 'left' | 'right']?: HTMLDivElement } = {};
    const alertChipParts: { [k in 'left' | 'right']?: { circle: HTMLDivElement; valpill: HTMLDivElement } } = {};
    const alertStrips: { [k in 'left' | 'right']?: HTMLDivElement } = {};
    const alertPending: { [k in 'left' | 'right']?: { axis: 'left' | 'right'; price: number; currentValue: number } } = {};
    const hidePreview = (side: 'left' | 'right') => {
      const pl = previewLineRef.current[side];
      if (pl) { try { pl.applyOptions({ lineVisible: false, axisLabelVisible: false }); } catch { /* серия снята */ } }
    };
    const hideChips = () => {
      if (alertChips.left) alertChips.left.style.display = 'none';
      if (alertChips.right) alertChips.right.style.display = 'none';
      hidePreview('left'); hidePreview('right');
      alertPending.left = undefined; alertPending.right = undefined;
    };
    const showChipsAt = (rawY: number) => {
      const ch = chartRef.current;
      const axes = alertAxesRef.current;
      if (!ch || !onCreateAlertRef.current || !axes || axes.length === 0) { hideChips(); return; }
      const axisH = ch.timeScale().height() || 26;
      const y = Math.max(0, Math.min(box.clientHeight - axisH, rawY));
      for (const side of ['left', 'right'] as const) {
        const chip = alertChips[side];
        const info = axisInfoRef.current[side];
        if (!chip || !axes.includes(side) || !info) { if (chip) chip.style.display = 'none'; hidePreview(side); alertPending[side] = undefined; continue; }
        const price = info.api.coordinateToPrice(y as number);
        if (price == null) { chip.style.display = 'none'; hidePreview(side); alertPending[side] = undefined; continue; }
        // Серый пунктир уровня — нативная price line (БЕЗ лейбла: значение показывает DOM-пилс).
        const pl = previewLineRef.current[side];
        if (pl) { try { pl.applyOptions({ price: price as number, lineVisible: true, axisLabelVisible: false, color: crossColorRef.current }); } catch { /* серия снята */ } }
        // DOM value-пилс + кружок «+», в цвет линии. Значение — форматтером серии (как тики).
        const parts = alertChipParts[side];
        const col = info.color || 'var(--accent,#FF5C2B)';
        if (parts) {
          parts.circle.style.background = col;
          parts.valpill.style.background = col;
          const txt = info.fmt ? info.fmt(price as number) : String(Math.round(price as number));
          parts.valpill.textContent = txt;
          // §R2-27: ширина = как у нативного лейбла — по строке с цифрами→'0'
          // (defaultReplacementRe библиотеки) через connected-canvas (то же лицо
          // шрифта, что у оси): совпадает с нативным пилсом и не дёргается при
          // движении курсора. Текст прижат к колонке чисел (text-align к оси).
          if (measCtx) {
            const zw = measCtx.measureText(txt.replace(/[2-9]/g, '0')).width;
            // 15.17 = border 1 + paddingInner 4.58 + paddingOuter 4.58 + tick 5;
            // финальное округление до device-пикселей — как битмап нативного.
            const dpr = Math.max(1, window.devicePixelRatio || 1);
            parts.valpill.style.width = (Math.round((Math.ceil(zw) + 15.17) * dpr) / dpr) + 'px';
          }
        }
        chip.style.top = y + 'px';
        chip.style.display = 'flex';
        // §R2-24: пилс сидит ТОЧНО как нативный лейбл — кромкой на границе поля
        // (правая ось: левый край пилса = граница поля; левая ось: правый край =
        // граница). Кружок «+» со стороны поля. z выше канваса → пилс ЛОЖИТСЯ на
        // нативный лейбл последнего значения (не толкает/не прячет).
        // §R2-30: та же гонка layout'а, что и в layoutAlert() — шкала могла только что
        // сменить visible, .width() ещё бросает Error('Value is null')
        let axisW = 54;
        try { axisW = ch.priceScale(side).width() || 54; } catch { /* см. комментарий выше */ }
        chip.style.right = 'auto';
        // §R2-28: якорим кромку САМОГО пилса к границе поля по фактическим rect'ам —
        // сумма ширин частей чипа (кружок+gap) давала ±2px от округлений flex-раскладки,
        // и пилс вставал чуть дальше/ближе к графику, чем нативный лейбл.
        const boxL = box.getBoundingClientRect().left;
        const pr = parts ? parts.valpill.getBoundingClientRect() : null;
        const chipL = chip.getBoundingClientRect().left - boxL;
        if (side === 'right') {
          // §R2-29: граница поля — от ШИРИН ЧАРТА (лев.ось + поле), НЕ от box.clientWidth:
          // TV округляет ширину таблицы вниз при дробной ширине контейнера, и box бывает
          // на 1-2px шире таблицы — пилс вставал дальше от графика, чем нативный.
          let leftW = 0;
          try { leftW = ch.priceScale('left').width() || 0; } catch { /* см. комментарий выше */ }
          const target = leftW + (ch.timeScale().width() || (box.clientWidth - axisW));
          chip.style.left = (pr ? chipL + (target - (pr.left - boxL)) : target - 18) + 'px';
        } else {
          const target = axisW;                            // правая кромка пилса = граница поля
          chip.style.left = (pr ? chipL + (target - (pr.right - boxL)) : Math.max(0, target - 44)) + 'px';
        }
        alertPending[side] = { axis: side, price: price as number, currentValue: info.last ?? (price as number) };
      }
    };
    for (const side of ['left', 'right'] as const) {
      const strip = document.createElement('div');
      strip.style.cssText = 'position:absolute;top:0;display:none;pointer-events:auto;z-index:6;cursor:crosshair;' + side + ':0';
      strip.addEventListener('mousemove', (e) => showChipsAt(e.clientY - box.getBoundingClientRect().top));
      box.appendChild(strip);
      alertStrips[side] = strip;
      // §R2-21: чип = value-пилс (DOM, ложится ПОВЕРХ нативного лейбла последнего значения)
      // + кружок «+» в поле. Контейнер pointer-events:none (не влияет на график); кликается
      // только кружок. Порядок: левая ось [пилс][+], правая [+][пилс] — пилс у оси, + в поле.
      const chip = document.createElement('div');
      chip.style.cssText = 'position:absolute;display:none;align-items:center;gap:3px;'
        + 'transform:translateY(-50%);z-index:7;white-space:nowrap;pointer-events:none';
      const circle = document.createElement('div');
      circle.style.cssText = 'width:15px;height:15px;border-radius:50%;display:flex;align-items:center;'
        + 'justify-content:center;font-size:13px;font-weight:700;line-height:1;color:#fff;flex:0 0 auto;'
        + 'pointer-events:auto;cursor:pointer;box-shadow:0 1px 4px rgba(0,0,0,0.35)';
      circle.textContent = '+';
      circle.title = 'Поставить алерт на этом уровне';
      circle.addEventListener('click', (e) => {
        e.stopPropagation();
        const p = alertPending[side];
        if (p && onCreateAlertRef.current) onCreateAlertRef.current(p);
      });
      const valpill = document.createElement('div');
      // §R2-24: точная копия геометрии НАТИВНОГО лейбла оси (PriceAxisViewRenderer,
      // fontSize 11): высота 11 + 2×2.29 ≈ 15.58; текст в 9.58px от кромки поля
      // (tick 5 + paddingInner 4.58) — та же колонка, что числа-тики; с внешней
      // стороны 5.58 (paddingOuter 4.58 + border 1); скругление 2px ТОЛЬКО с внешней
      // стороны (кромка у поля прямая); шрифт = шрифт оси, без тени и tabular-nums —
      // иначе пилс отличается от нативных по форме/размеру/посадке цифр.
      valpill.style.cssText = 'font:400 11px/15.58px Inter,-apple-system,sans-serif;color:#fff;'
        + 'white-space:nowrap;pointer-events:none;box-sizing:border-box;'
        + (side === 'left'
          ? 'padding:0 9.58px 0 5.58px;border-radius:2px 0 0 2px;text-align:right'
          : 'padding:0 5.58px 0 9.58px;border-radius:0 2px 2px 0;text-align:left');
      if (side === 'left') { chip.appendChild(valpill); chip.appendChild(circle); }
      else { chip.appendChild(circle); chip.appendChild(valpill); }
      alertChipParts[side] = { circle, valpill };
      box.appendChild(chip);
      alertChips[side] = chip;
    }
    box.addEventListener('mouseleave', hideChips);
    // §R2-26: Chromium может резолвить вебфонт 'Inter' в canvas ДРУГИМ лицом, чем в
    // DOM (замерено на проде: та же строка «11px Inter…» в connected-canvas на ~10%
    // шире/крупнее DOM) — тогда цифры DOM-пилса выглядят мельче нативных лейблов оси.
    // Калибруем кегль пилса по фактическому ratio canvas/DOM; в норме ratio=1 и стиль
    // не меняется. Пересчёт после догрузки шрифтов (fonts.ready). Тот же connected-canvas
    // (measCanvas, живёт до unmount) — эталон ШИРИНЫ пилса: нативный лейбл меряет строку
    // с цифрами→'0' (defaultReplacementRe, ширина не дёргается при тике значения) — width
    // пилса считаем в showChipsAt так же, иначе пилс на пару px уже нативного.
    const PILL_FONT = 'Inter, -apple-system, sans-serif';
    const measCanvas = document.createElement('canvas');
    measCanvas.style.cssText = 'position:absolute;width:0;height:0;visibility:hidden;pointer-events:none';
    box.appendChild(measCanvas);
    const measCtx = measCanvas.getContext('2d');
    if (measCtx) measCtx.font = '11px ' + PILL_FONT;
    const setPillFont = () => {
      let ratio = 1;
      try {
        if (measCtx) {
          measCtx.font = '11px ' + PILL_FONT;   // после fonts.ready лицо могло смениться
          const sp = document.createElement('span');
          sp.style.cssText = 'position:absolute;visibility:hidden;white-space:nowrap;font:400 11px ' + PILL_FONT;
          sp.textContent = '0123456789';
          box.appendChild(sp);
          const cw = measCtx.measureText('0123456789').width;
          const dw = sp.getBoundingClientRect().width;
          box.removeChild(sp);
          if (cw > 0 && dw > 0) ratio = Math.max(0.8, Math.min(1.5, cw / dw));
        }
      } catch { /* калибровка не критична */ }
      const fs = Math.round(11 * ratio * 100) / 100;
      // §R2-28: высота = как у нативного лейбла ПОСЛЕ его битмап-округления:
      // round(15.583×dpr) с поправкой до чётности tickHeight (у нативного totalHeight
      // подгоняется под чётность тика) — на ретине это ровно 16.0, а наши прежние
      // 15.58 давали «выглядывание» нативного лейбла из-под пилса сверху/снизу.
      const dpr = Math.max(1, window.devicePixelRatio || 1);
      const tickH = Math.max(1, Math.floor(dpr));
      let hDev = Math.round(15.583 * dpr);
      if (hDev % 2 !== tickH % 2) hDev += 1;
      const lh = Math.round(hDev / dpr * 100) / 100;
      for (const side of ['left', 'right'] as const) {
        const p = alertChipParts[side];
        if (p) p.valpill.style.font = '400 ' + fs + 'px/' + lh + 'px ' + PILL_FONT;
      }
    };
    setPillFont();
    try { document.fonts?.ready?.then(() => setPillFont()); } catch { /* старые браузеры */ }
    const layoutAlert = () => {
      const ch = chartRef.current;
      if (!ch) return;
      const axisH = ch.timeScale().height() || 26;
      const h = Math.max(0, box.clientHeight - axisH);
      const axes = alertAxesRef.current;
      for (const side of ['left', 'right'] as const) {
        const strip = alertStrips[side];
        if (!strip) continue;
        const info = axisInfoRef.current[side];
        const on = !!(onCreateAlertRef.current && axes && axes.includes(side) && info);
        if (!on) { strip.style.display = 'none'; const c = alertChips[side]; if (c) c.style.display = 'none'; continue; }
        // приоритетная шкала могла только что сменить visible → внутренний layout lightweight-charts
        // ещё не пересчитан в этом тике, и .width() бросает Error('Value is null') (§R2-30)
        let w = 54;
        try { w = ch.priceScale(side).width() || 54; } catch { /* см. комментарий выше */ }
        strip.style.display = 'block';
        strip.style.width = Math.max(28, w) + 'px';
        strip.style.height = h + 'px';
      }
    };
    layoutAlertRef.current = layoutAlert;

    // ─────────────────────────── Слой рисования (модель TradingView) ───────────────────────────
    // SVG-оверлей в координатах {logical, price} → перепроецируется на зум/пан/ресайз (как
    // экспирации). Точки хранятся в logical (дробный индекс бара) + price → фигуры «ездят»
    // с графиком. pointer-events включаются только в режиме карандаша (drawActive).
    const SVGNS = 'http://www.w3.org/2000/svg';
    const drawSvg = document.createElementNS(SVGNS, 'svg');
    drawSvg.style.cssText = 'position:absolute;inset:0;z-index:7;overflow:visible;pointer-events:none;touch-action:none';
    box.appendChild(drawSvg);
    // Прозрачный DIV-слой ПЕРЕХВАТА событий поверх SVG: пустой <svg> не хиттестит пустые
    // области → клики проваливались на канвас (фигуры не рисовались). Хиттест фигур у нас
    // по координатам (hitTest), а не по DOM, поэтому ловим всё на этом div.
    const drawHit = document.createElement('div');
    drawHit.style.cssText = 'position:absolute;inset:0;z-index:8;pointer-events:none;touch-action:none';
    box.appendChild(drawHit);

    const drawSeries = () => axisInfoRef.current.left?.api ?? axisInfoRef.current.right?.api ?? seriesApiRef.current[0];
    const paneLeftW = () => { const ch = chartRef.current; if (!ch) return 0; try { return ch.priceScale('left').width() || 0; } catch { return 0; } };
    const lp2xy = (p: LwDrawPoint): { x: number; y: number } | null => {
      const ch = chartRef.current; if (!ch) return null;
      const xc = ch.timeScale().logicalToCoordinate(p.logical as Logical);
      const s = drawSeries(); const yc = s ? s.priceToCoordinate(p.price) : null;
      if (xc == null || yc == null) return null;
      return { x: paneLeftW() + (xc as number), y: yc as number };
    };
    const xy2lp = (bx: number, by: number): LwDrawPoint | null => {
      const ch = chartRef.current; if (!ch) return null;
      const logical = ch.timeScale().coordinateToLogical((bx - paneLeftW()) as Coordinate);
      const s = drawSeries(); const price = s ? s.coordinateToPrice(by as Coordinate) : null;
      if (logical == null || price == null) return null;
      return { logical: logical as number, price: price as number };
    };
    const plotBox = () => {
      const ch = chartRef.current; const pl = paneLeftW();
      const w = ch ? (ch.timeScale().width() || (box.clientWidth - pl)) : (box.clientWidth - pl);
      const axisH = ch ? (ch.timeScale().height() || 26) : 26;
      return { left: pl, width: w, height: Math.max(0, box.clientHeight - axisH) };
    };
    const svgEl = (tag: string, attrs: Record<string, string | number>) => {
      const e = document.createElementNS(SVGNS, tag);
      for (const k in attrs) e.setAttribute(k, String(attrs[k]));
      return e;
    };
    // Snap (магнит): точка → ближайший бар (logical→round) + цена закрытия этого бара.
    const primaryDef = () => defsRef.current.find((s) => (s.scale ?? 'right') === 'left') ?? defsRef.current.find((s) => (s.scale ?? 'right') === 'right') ?? defsRef.current[0];
    const snap = (lp: LwDrawPoint | null, allow = true): LwDrawPoint | null => {
      const mode = drawMagnetRef.current;
      if (!lp || !allow || !mode || mode === 'off') return lp;
      const L = Math.round(lp.logical); const data = primaryDef()?.data;
      if (!data || L < 0 || L >= data.length) return { ...lp, logical: L };
      const pt = data[L];
      // ближайшее из O/H/L/C (value=close-фолбэк) к курсору — как магнит TradingView.
      const cands = [pt.open, pt.high, pt.low, pt.close, pt.value].filter((v): v is number => v != null);
      if (!cands.length) return { ...lp, logical: L };
      let best = cands[0], bd = Infinity;
      for (const v of cands) { const dd = Math.abs(v - lp.price); if (dd < bd) { bd = dd; best = v; } }
      if (mode === 'weak') {
        // слабый: притягиваем ТОЛЬКО если курсор рядом с OHLC (≤12px), иначе точка свободна.
        const yb = priceY(best), yc = priceY(lp.price);
        if (yb == null || yc == null || Math.abs(yb - yc) > 12) return lp;
      }
      return { logical: L, price: best };
    };
    const priceY = (price: number): number | null => { const s = drawSeries(); const y = s ? s.priceToCoordinate(price) : null; return y == null ? null : (y as number); };
    // Луч: продлить a→b до границы поля.
    const rayEnd = (a: { x: number; y: number }, b: { x: number; y: number }, pb: { left: number; width: number; height: number }) => {
      const dx = b.x - a.x, dy = b.y - a.y; if (!dx && !dy) return b;
      let t = Infinity;
      if (dx > 0) t = Math.min(t, (pb.left + pb.width - a.x) / dx);
      if (dx < 0) t = Math.min(t, (pb.left - a.x) / dx);
      if (dy > 0) t = Math.min(t, (pb.height - a.y) / dy);
      if (dy < 0) t = Math.min(t, (0 - a.y) / dy);
      if (!isFinite(t) || t < 1) t = 1;
      return { x: a.x + dx * t, y: a.y + dy * t };
    };
    // Дефолтный набор уровней Фибоначчи TradingView (11 уровней 0..4.236, вкл. расширения).
    const FIB = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1, 1.618, 2.618, 3.618, 4.236];
    const ONE_PT = new Set<string>(['hline', 'vline', 'text', 'brush']);   // старт с 1 точки

    // Стиль линии → stroke-dasharray. Точки = round-cap + нулевой дэш.
    const dashArr = (dash: LwDash | undefined, w: number): string => {
      if (dash === 'dashed') return `${Math.max(w * 3, 4)} ${Math.max(w * 2.5, 3)}`;
      if (dash === 'dotted') return `0.1 ${Math.max(w * 2.4, 4)}`;
      return 'none';
    };
    const renderOne = (d: LwDrawing, sel: boolean, preview = false) => {
      const col = d.color, w = d.width, pb = plotBox();
      const op = String((d.opacity == null ? 1 : d.opacity) * (preview ? 0.7 : 1));
      const da = dashArr(d.dash, w);                          // стиль линии элемента
      const lc = d.dash === 'dotted' ? 'round' : 'butt';      // точки требуют round-cap
      const S = { stroke: col, 'stroke-width': w, opacity: op, 'stroke-dasharray': da };   // общий страйк
      const dot = (x: number, y: number) => drawSvg.appendChild(svgEl('circle', { cx: x, cy: y, r: 4, fill: col }));
      if (d.tool === 'hline') {
        const xy = lp2xy(d.pts[0]); if (!xy) return;
        drawSvg.appendChild(svgEl('line', { x1: pb.left, y1: xy.y, x2: pb.left + pb.width, y2: xy.y, ...S, 'stroke-linecap': lc }));
        if (sel) dot(pb.left + pb.width / 2, xy.y);
      } else if (d.tool === 'vline') {
        const xy = lp2xy(d.pts[0]); if (!xy) return;
        drawSvg.appendChild(svgEl('line', { x1: xy.x, y1: 0, x2: xy.x, y2: pb.height, ...S, 'stroke-linecap': lc }));
        if (sel) dot(xy.x, pb.height / 2);
      } else if (d.tool === 'trend' || d.tool === 'ray' || d.tool === 'arrow') {
        const a = lp2xy(d.pts[0]), b0 = lp2xy(d.pts[1]); if (!a || !b0) return;
        const b = d.tool === 'ray' ? rayEnd(a, b0, pb) : b0;
        drawSvg.appendChild(svgEl('line', { x1: a.x, y1: a.y, x2: b.x, y2: b.y, ...S, 'stroke-linecap': d.dash === 'dotted' ? 'round' : 'round' }));
        if (d.tool === 'arrow') {
          const ang = Math.atan2(b0.y - a.y, b0.x - a.x), ah = 9 + w * 2;
          // наконечник — всегда сплошной (без дэша), чтобы стрелка читалась
          for (const s of [-0.42, 0.42]) drawSvg.appendChild(svgEl('line', { x1: b0.x, y1: b0.y, x2: b0.x - ah * Math.cos(ang - s), y2: b0.y - ah * Math.sin(ang - s), stroke: col, 'stroke-width': w, opacity: op, 'stroke-linecap': 'round' }));
        }
        if (sel) { dot(a.x, a.y); dot(b0.x, b0.y); }
      } else if (d.tool === 'rect' || d.tool === 'ellipse') {
        const a = lp2xy(d.pts[0]), b = lp2xy(d.pts[1]); if (!a || !b) return;
        const x = Math.min(a.x, b.x), y = Math.min(a.y, b.y), rw = Math.abs(a.x - b.x), rh = Math.abs(a.y - b.y);
        const fo = String(0.13 * (d.opacity == null ? 1 : d.opacity));
        if (d.tool === 'rect') drawSvg.appendChild(svgEl('rect', { x, y, width: rw, height: rh, fill: col, 'fill-opacity': fo, ...S }));
        else drawSvg.appendChild(svgEl('ellipse', { cx: x + rw / 2, cy: y + rh / 2, rx: rw / 2, ry: rh / 2, fill: col, 'fill-opacity': fo, ...S }));
        if (sel) { dot(a.x, a.y); dot(b.x, b.y); }
      } else if (d.tool === 'fib') {
        const a = lp2xy(d.pts[0]), b = lp2xy(d.pts[1]); if (!a || !b) return;
        const xL = Math.min(a.x, b.x), xR = Math.max(a.x, b.x), p0 = d.pts[0].price, p1 = d.pts[1].price;
        const fpF = (v: number) => (Math.abs(v) >= 100 ? v.toFixed(0) : Math.abs(v) >= 1 ? v.toFixed(2) : v.toFixed(4));
        for (const lv of FIB) {
          const price = p0 + (p1 - p0) * lv;
          const yy = priceY(price); if (yy == null) continue;
          drawSvg.appendChild(svgEl('line', { x1: xL, y1: yy, x2: xR, y2: yy, stroke: col, 'stroke-width': 1, opacity: String((d.opacity == null ? 0.85 : d.opacity * 0.85) * (preview ? 0.7 : 1)), 'stroke-dasharray': lv === 0 || lv === 1 ? '0' : '3 3' }));
          // подпись = коэффициент + цена (как в TradingView)
          const t = svgEl('text', { x: xL + 3, y: yy - 2, fill: col, 'font-size': 9.5, 'font-family': 'Inter,sans-serif', opacity: op }); t.textContent = `${lv.toFixed(3)} (${fpF(price)})`;
          drawSvg.appendChild(t);
        }
        if (sel) { dot(a.x, a.y); dot(b.x, b.y); }
      } else if (d.tool === 'brush') {
        if (d.pts.length < 2) { const xy = lp2xy(d.pts[0]); if (xy) dot(xy.x, xy.y); return; }
        const pnts = d.pts.map(lp2xy).filter(Boolean) as { x: number; y: number }[];
        drawSvg.appendChild(svgEl('polyline', { points: pnts.map((p) => `${p.x},${p.y}`).join(' '), fill: 'none', ...S, 'stroke-linejoin': 'round', 'stroke-linecap': d.dash === 'dotted' ? 'round' : 'round' }));
      } else if (d.tool === 'ruler') {
        const a = lp2xy(d.pts[0]), b = lp2xy(d.pts[1]); if (!a || !b) return;
        const x = Math.min(a.x, b.x), y = Math.min(a.y, b.y), rw = Math.abs(a.x - b.x), rh = Math.abs(a.y - b.y);
        const up = d.pts[1].price >= d.pts[0].price;
        const mc = resolveColor(box, up ? 'var(--oi-green)' : 'var(--oi-red)');
        drawSvg.appendChild(svgEl('rect', { x, y, width: rw, height: rh, fill: mc, 'fill-opacity': String(0.12 * (d.opacity == null ? 1 : d.opacity)), stroke: mc, 'stroke-width': 1, opacity: op }));
        drawSvg.appendChild(svgEl('line', { x1: a.x, y1: a.y, x2: b.x, y2: b.y, stroke: mc, 'stroke-width': 1, opacity: op, 'stroke-dasharray': '3 3' }));
        const dP = d.pts[1].price - d.pts[0].price;
        const dPct = d.pts[0].price !== 0 ? (dP / Math.abs(d.pts[0].price)) * 100 : 0;
        const dBars = Math.round(Math.abs(d.pts[1].logical - d.pts[0].logical));
        const fmtN = (n: number) => (Math.abs(n) >= 100 ? n.toFixed(0) : Math.abs(n) >= 1 ? n.toFixed(2) : n.toFixed(4));
        // Время-span между точками (как в TradingView: бары + прошедшее время). Время берём из данных серии.
        const timeAt = (logical: number): number | null => { const dt = primaryDef()?.data; if (!dt || !dt.length) return null; const L = Math.max(0, Math.min(dt.length - 1, Math.round(logical))); return dt[L].time; };
        const fmtDur = (secs: number): string => { const s = Math.abs(secs), dd = Math.floor(s / 86400), hh = Math.floor((s % 86400) / 3600), mm = Math.floor((s % 3600) / 60); return dd >= 1 ? (hh > 0 ? `${dd}д ${hh}ч` : `${dd}д`) : hh >= 1 ? (mm > 0 ? `${hh}ч ${mm}м` : `${hh}ч`) : `${mm}м`; };
        const t0 = timeAt(d.pts[0].logical), t1 = timeAt(d.pts[1].logical);
        const span = t0 != null && t1 != null ? ` · ${fmtDur(t1 - t0)}` : '';
        const label = `${dP >= 0 ? '+' : ''}${fmtN(dP)} (${dPct >= 0 ? '+' : ''}${dPct.toFixed(2)}%) · ${dBars} бар${span}`;
        const cx = x + rw / 2, lbW = label.length * 5.6 + 12, top = y - 4;
        drawSvg.appendChild(svgEl('rect', { x: cx - lbW / 2, y: top - 16, width: lbW, height: 16, rx: 4, fill: mc, opacity: op }));
        const t = svgEl('text', { x: cx, y: top - 4.5, fill: '#fff', 'font-size': 10.5, 'font-family': 'Inter,sans-serif', 'font-weight': 600, 'text-anchor': 'middle', opacity: op }); t.textContent = label;
        drawSvg.appendChild(t);
        if (sel) { dot(a.x, a.y); dot(b.x, b.y); }
      } else if (d.tool === 'text') {
        const xy = lp2xy(d.pts[0]); if (!xy) return;
        const t = svgEl('text', { x: xy.x, y: xy.y, fill: col, 'font-size': 13 + w * 2, 'font-family': 'Inter,-apple-system,sans-serif', 'font-weight': 600, opacity: op });
        t.textContent = d.text || 'Текст';
        drawSvg.appendChild(t);
        if (sel) dot(xy.x - 4, xy.y - 5);
      }
    };
    let dragState: null | { mode: 'create' | 'move' | 'vertex'; d: LwDrawing; orig?: LwDrawPoint[]; vi?: number; startXY: { x: number; y: number } } = null;
    const HANDLE_R = 8;   // радиус захвата вершины (ручки) для правки формы
    const uid = () => 'dr_' + Date.now().toString(36) + '_' + Math.floor(Math.random() * 1e6).toString(36);
    const drawShapes = () => {
      if (!chartRef.current) return;
      while (drawSvg.firstChild) drawSvg.removeChild(drawSvg.firstChild);
      if (drawHiddenRef.current) return;   // «глаз»: временно скрыть все рисунки
      const selId = selectedDrawIdRef.current;
      for (const d of (drawingsRef.current ?? [])) { if (d.hidden) continue; renderOne(d, d.id === selId); }   // per-element hide (слои)
      if (dragState) renderOne(dragState.d, false, true);
    };
    const syncDrawInteractivity = () => {
      const on = !!drawActiveRef.current;
      drawHit.style.pointerEvents = on ? 'auto' : 'none';
      drawHit.style.cursor = on ? ((drawToolRef.current && drawToolRef.current !== 'select') ? 'crosshair' : 'default') : 'default';
    };
    drawShapesRef.current = () => { syncDrawInteractivity(); drawShapes(); };

    const distToSeg = (px: number, py: number, ax: number, ay: number, bx: number, by: number) => {
      const dx = bx - ax, dy = by - ay, len2 = dx * dx + dy * dy;
      let t = len2 ? ((px - ax) * dx + (py - ay) * dy) / len2 : 0; t = Math.max(0, Math.min(1, t));
      return Math.hypot(px - (ax + t * dx), py - (ay + t * dy));
    };
    const hitTest = (bx: number, by: number): LwDrawing | null => {
      const list = drawingsRef.current ?? [], pb = plotBox();
      for (let i = list.length - 1; i >= 0; i--) {
        const d = list[i];
        if (d.hidden) continue;   // скрытые (слои) не хиттестим
        if (d.tool === 'hline') { const xy = lp2xy(d.pts[0]); if (xy && Math.abs(by - xy.y) < 6) return d; }
        else if (d.tool === 'vline') { const xy = lp2xy(d.pts[0]); if (xy && Math.abs(bx - xy.x) < 6) return d; }
        else if (d.tool === 'trend' || d.tool === 'arrow') { const a = lp2xy(d.pts[0]), b = lp2xy(d.pts[1]); if (a && b && distToSeg(bx, by, a.x, a.y, b.x, b.y) < 6) return d; }
        else if (d.tool === 'ray') { const a = lp2xy(d.pts[0]), b0 = lp2xy(d.pts[1]); if (a && b0) { const b = rayEnd(a, b0, pb); if (distToSeg(bx, by, a.x, a.y, b.x, b.y) < 6) return d; } }
        else if (d.tool === 'rect' || d.tool === 'ellipse' || d.tool === 'ruler') { const a = lp2xy(d.pts[0]), b = lp2xy(d.pts[1]); if (a && b) { const x = Math.min(a.x, b.x), y = Math.min(a.y, b.y), rw = Math.abs(a.x - b.x), rh = Math.abs(a.y - b.y); if (bx >= x - 5 && bx <= x + rw + 5 && by >= y - 5 && by <= y + rh + 5) return d; } }
        else if (d.tool === 'fib') { const a = lp2xy(d.pts[0]), b = lp2xy(d.pts[1]); if (a && b) { const xL = Math.min(a.x, b.x), xR = Math.max(a.x, b.x); if (bx >= xL - 5 && bx <= xR + 5) { const p0 = d.pts[0].price, p1 = d.pts[1].price; for (const lv of FIB) { const yy = priceY(p0 + (p1 - p0) * lv); if (yy != null && Math.abs(by - yy) < 6) return d; } } } }
        else if (d.tool === 'brush') { const pnts = d.pts.map(lp2xy).filter(Boolean) as { x: number; y: number }[]; for (let j = 1; j < pnts.length; j++) if (distToSeg(bx, by, pnts[j - 1].x, pnts[j - 1].y, pnts[j].x, pnts[j].y) < 6) return d; }
        else if (d.tool === 'text') { const xy = lp2xy(d.pts[0]); if (xy && Math.abs(bx - xy.x) < 44 && Math.abs(by - (xy.y - 6)) < 14) return d; }
      }
      return null;
    };
    const relXY = (e: PointerEvent) => { const r = box.getBoundingClientRect(); return { x: e.clientX - r.left, y: e.clientY - r.top }; };
    const commit = (next: LwDrawing[]) => { drawingsRef.current = next; onDrawingsChangeRef.current?.(next); drawShapes(); };
    const onDrawDown = (e: PointerEvent) => {
      if (!drawActiveRef.current) return;
      const tool = drawToolRef.current ?? 'select';
      const { x, y } = relXY(e);
      if (tool === 'select') {
        if (drawLockedRef.current) { selectedDrawIdRef.current = null; onSelectDrawRef.current?.(null); drawShapes(); return; }   // замок: не выделяем/не двигаем
        const hit = hitTest(x, y);
        selectedDrawIdRef.current = hit ? hit.id : null; onSelectDrawRef.current?.(hit ? hit.id : null);
        if (hit) {
          // клик у ВЕРШИНЫ (ручки) → правка формы (тащим точку); иначе двигаем всю фигуру.
          let vi = -1;
          if (hit.tool !== 'brush') for (let k = 0; k < hit.pts.length; k++) { const xy = lp2xy(hit.pts[k]); if (xy && Math.hypot(x - xy.x, y - xy.y) <= HANDLE_R) { vi = k; break; } }
          dragState = vi >= 0
            ? { mode: 'vertex', d: { ...hit, pts: hit.pts.map((p) => ({ ...p })) }, vi, startXY: { x, y } }
            : { mode: 'move', d: { ...hit, pts: hit.pts.map((p) => ({ ...p })) }, orig: hit.pts.map((p) => ({ ...p })), startXY: { x, y } };
          try { drawHit.setPointerCapture(e.pointerId); } catch { /* нет capture */ }
        }
        drawShapes(); return;
      }
      const lp = snap(xy2lp(x, y), tool !== 'brush'); if (!lp) return;
      const color = drawColorRef.current || '#FF5C2B', width = drawWidthRef.current || 2;
      const dash = drawDashRef.current, opacity = drawOpacityRef.current;   // стиль новых фигур
      if (tool === 'text') {
        // Отложенный ввод: ставим плейсхолдер сразу (без prompt), правка — двойным кликом.
        const id = uid();
        commit([...(drawingsRef.current ?? []), { id, tool: 'text', pts: [lp], color, width, dash, opacity, text: 'Текст' }]);
        selectedDrawIdRef.current = id; onSelectDrawRef.current?.(id); onToolResetRef.current?.();
        return;
      }
      const base = { color, width, dash, opacity };
      const d: LwDrawing = ONE_PT.has(tool) ? { id: uid(), tool, pts: [lp], ...base } : { id: uid(), tool, pts: [lp, lp], ...base };
      dragState = { mode: 'create', d, startXY: { x, y } };
      try { drawHit.setPointerCapture(e.pointerId); } catch { /* нет capture */ }
      drawShapes();
    };
    const onDrawMove = (e: PointerEvent) => {
      if (!dragState) return;
      const { x, y } = relXY(e);
      if (dragState.mode === 'create') {
        const t = dragState.d.tool;
        if (t === 'brush') {
          const lp = xy2lp(x, y); if (!lp) return;   // кисть: без магнита
          const lastXY = lp2xy(dragState.d.pts[dragState.d.pts.length - 1]);
          if (!lastXY || Math.hypot(x - lastXY.x, y - lastXY.y) >= 2.5) dragState.d.pts = [...dragState.d.pts, lp];
        } else {
          // Shift-модификаторы (как в TradingView): тренд/луч/стрелка → угол по 45°;
          // прямоугольник/эллипс → квадрат/круг. Считаем в пикселях от первой точки.
          let px = x, py = y;
          if (e.shiftKey && !ONE_PT.has(t)) {
            const aXY = lp2xy(dragState.d.pts[0]);
            if (aXY) {
              const dx = x - aXY.x, dy = y - aXY.y;
              if (t === 'trend' || t === 'ray' || t === 'arrow') {
                const ang = Math.round(Math.atan2(dy, dx) / (Math.PI / 4)) * (Math.PI / 4), dist = Math.hypot(dx, dy);
                px = aXY.x + dist * Math.cos(ang); py = aXY.y + dist * Math.sin(ang);
              } else if (t === 'rect' || t === 'ellipse') {
                const sz = Math.max(Math.abs(dx), Math.abs(dy));
                px = aXY.x + (dx < 0 ? -sz : sz); py = aXY.y + (dy < 0 ? -sz : sz);
              }
            }
          }
          const lp = snap(xy2lp(px, py)); if (!lp) return;
          dragState.d.pts = ONE_PT.has(t) ? [lp] : [dragState.d.pts[0], lp];
        }
      } else if (dragState.mode === 'vertex') {
        const lp = snap(xy2lp(x, y)); if (!lp) return;
        const pts = dragState.d.pts.slice(); pts[dragState.vi ?? 0] = lp; dragState.d.pts = pts;
      } else {
        const dx = x - dragState.startXY.x, dy = y - dragState.startXY.y;
        dragState.d.pts = (dragState.orig ?? dragState.d.pts).map((p) => { const xy = lp2xy(p); if (!xy) return p; return xy2lp(xy.x + dx, xy.y + dy) ?? p; });
      }
      drawShapes();
    };
    const onDrawUp = (e: PointerEvent) => {
      if (!dragState) return;
      try { drawHit.releasePointerCapture(e.pointerId); } catch { /* уже */ }
      const ds = dragState; dragState = null;
      if (ds.mode === 'create') {
        if (ds.d.tool === 'brush') { if (ds.d.pts.length < 2) { drawShapes(); return; } }
        else if (!ONE_PT.has(ds.d.tool)) { const a = lp2xy(ds.d.pts[0]), b = lp2xy(ds.d.pts[1]); if (a && b && Math.hypot(a.x - b.x, a.y - b.y) < 4) { drawShapes(); return; } }
        commit([...(drawingsRef.current ?? []), ds.d]);
        selectedDrawIdRef.current = ds.d.id; onSelectDrawRef.current?.(ds.d.id);
        onToolResetRef.current?.();   // один-за-раз: после фигуры → инструмент «выбор», фигура выделена
      } else {
        commit((drawingsRef.current ?? []).map((x) => (x.id === ds.d.id ? ds.d : x)));
      }
    };
    // Двойной клик по тексту → правка (отложенный ввод).
    const onDrawDbl = (e: MouseEvent) => {
      if (!drawActiveRef.current) return;
      const { x, y } = relXY(e as unknown as PointerEvent);
      const hit = hitTest(x, y);
      if (hit && hit.tool === 'text') {
        const txt = window.prompt('Текст:', hit.text || '');
        if (txt != null) commit((drawingsRef.current ?? []).map((d) => (d.id === hit.id ? { ...d, text: txt } : d)));
      }
    };
    drawHit.addEventListener('pointerdown', onDrawDown);
    drawHit.addEventListener('pointermove', onDrawMove);
    drawHit.addEventListener('pointerup', onDrawUp);
    drawHit.addEventListener('dblclick', onDrawDbl);
    syncDrawInteractivity();

    const onRange = () => { drawExp(); layoutAlert(); drawShapes(); };
    chart.timeScale().subscribeVisibleLogicalRangeChange(onRange);
    const expRo = new ResizeObserver(() => { drawExp(); layoutAlert(); drawShapes(); });
    expRo.observe(box);

    chart.subscribeCrosshairMove((param) => {
      const defs = defsRef.current;
      const apis = seriesApiRef.current;
      // §OI-3: чипы «+» алертов следуют за курсором над графиком (когда есть point).
      // При уходе в жёлоб оси point=null → чипы держатся полосами (strip), не гасим тут.
      if (param.point) showChipsAt(param.point.y);
      if (!param.time || !param.point || apis.length === 0) { tip.style.display = 'none'; return; }
      while (tip.firstChild) tip.removeChild(tip.firstChild);
      let any = false;
      for (let i = 0; i < apis.length; i++) {
        // OHLC-серии (candlestick/bar) отдают {open,high,low,close} без value → берём close.
        const dp = param.seriesData.get(apis[i]) as { value?: number; close?: number } | undefined;
        const dv = dp ? (dp.value ?? dp.close) : undefined;
        if (dv == null) continue;
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
        val.textContent = def?.tipFmt ? def.tipFmt(dv) : String(Math.round(dv));
        row.appendChild(dot); row.appendChild(lbl); row.appendChild(val);
        tip.appendChild(row);
      }
      if (!any) { tip.style.display = 'none'; return; }
      tip.style.display = 'block';
      const w = box.clientWidth, tw = tip.offsetWidth, GAP = 16;
      // §R2-22/25 (выбор Вадима): СИММЕТРИЧНЫЙ флип по СЕРЕДИНЕ поля — курсор в левой
      // половине → тултип справа от курсора; в правой → слева. Одинаковый зазор GAP от
      // перекрестья с обеих сторон. ВАЖНО: param.point.x — от ЛЕВОГО КРАЯ ПОЛЯ (после
      // левой ценовой оси), а тултип позиционируем в боксе → переводим (+paneL), иначе
      // флип едет и зазор несимметричен ровно на ширину левой оси.
      const chNow = chartRef.current;
      const paneL = chNow ? (chNow.priceScale('left').width() || 0) : 0;
      const paneW = chNow ? (chNow.timeScale().width() || (w - paneL)) : w;
      const cx = paneL + param.point.x;   // курсор в координатах бокса
      const rawLeft = param.point.x < paneW / 2 ? cx + GAP : cx - tw - GAP;
      tip.style.left = Math.max(6, Math.min(w - tw - 6, rawLeft)) + 'px';
      tip.style.top = Math.max(6, Math.min(box.clientHeight - tip.offsetHeight - 6, param.point.y - 8)) + 'px';
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
      box.removeEventListener('mouseleave', hideChips);
      expRo.disconnect();
      try { chart.timeScale().unsubscribeVisibleLogicalRangeChange(onRange); } catch { /* уже снят */ }
      drawExpRef.current = null;
      layoutAlertRef.current = null;
      chart.remove();
      chartRef.current = null;
      seriesApiRef.current = [];
      axisInfoRef.current = {};
      drawShapesRef.current = null;
      drawHit.removeEventListener('pointerdown', onDrawDown);
      drawHit.removeEventListener('pointermove', onDrawMove);
      drawHit.removeEventListener('pointerup', onDrawUp);
      drawHit.removeEventListener('dblclick', onDrawDbl);
      for (const el of [tip, legend, expLayer, expGuide, measCanvas, alertStrips.left, alertStrips.right, alertChips.left, alertChips.right, drawSvg, drawHit]) {
        if (el && el.parentNode) el.parentNode.removeChild(el);
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ── тема + дефолты §9 (сетка/кроссхэйр) ──
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    const c = themeColors(dark);
    const gridCol = chartPrefs?.grid === false ? 'rgba(0,0,0,0)' : c.grid;
    chart.applyOptions({
      layout: { textColor: c.text },
      grid: { vertLines: { color: gridCol }, horzLines: { color: gridCol } },
      crosshair: chartPrefs?.crosshair === false
        ? { mode: CrosshairMode.Hidden }
        : {
            mode: CrosshairMode.Normal,
            vertLine: { color: c.cross, labelBackgroundColor: c.lab },
            // §R2-13/14: прячем линию И лейбл горизонтали кроссхэйра при axis-алертах.
            horzLine: { color: c.cross, labelBackgroundColor: c.lab, visible: !(alertAxes && alertAxes.length), labelVisible: !(alertAxes && alertAxes.length) },
          },
    });
    // §R2-12/17: держим цвет пунктира превью-уровня = цвет кроссхэйра при смене темы (◐).
    crossColorRef.current = c.cross;
    for (const side of ['left', 'right'] as const) {
      const pl = previewLineRef.current[side];
      if (pl) { try { pl.applyOptions({ color: c.cross }); } catch { /* серия снята */ } }
    }
  }, [dark, chartPrefs, alertAxes]);

  // Смена таймфрейма (интрадей ⇄ дневной) не пересоздаёт чарт (нет [] в deps выше) —
  // timeVisible нужно применять реактивно, иначе ось застревает в режиме первого маунта.
  useEffect(() => {
    chartRef.current?.applyOptions({ timeScale: { timeVisible: !!timeVisible } });
  }, [timeVisible]);

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
      const lw = ((chartPrefs?.lineWidth ?? def.lineWidth ?? 2)) as 1 | 2 | 3 | 4;
      // Явный def.lastValueVisible (автор графика знает, что «последнее значение»
      // тут бессмысленно — категориальная гистограмма Сезонности, поток Фондов)
      // побеждает глобальный тумблер песочницы. Тумблер решает только когда
      // сам индикатор не имеет мнения (не задал lastValueVisible).
      const lastLine = def.lastValueVisible ?? chartPrefs?.lastValue ?? true;
      const lastHist = def.lastValueVisible ?? chartPrefs?.lastValue ?? false;
      const col = rc(def.color);
      let s: ISeriesApi<'Line' | 'Area' | 'Histogram' | 'Candlestick' | 'Bar'>;
      const lineStyle = def.dashed ? LineStyle.Dashed : LineStyle.Solid;
      const isOhlc = def.type === 'candlestick' || def.type === 'bar';
      if (def.type === 'line') {
        s = chart.addLineSeries({ color: col, lineWidth: lw, lineStyle, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: lastLine, priceFormat });
      } else if (def.type === 'area') {
        s = chart.addAreaSeries({ lineColor: col, topColor: rc(def.areaTop ?? def.color), bottomColor: def.areaBottom ? rc(def.areaBottom) : 'rgba(0,0,0,0)', lineWidth: lw, lineStyle, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: lastLine, priceFormat });
      } else if (def.type === 'candlestick') {
        // up/down = палитра «Покупки/Продажи» (та же, что у свечей сайта): в editorial
        // это сине-стальной/янтарь, не зелёный/красный (дальтоник-палитра проекта).
        // lastValueVisible:false — нативный last-value label свечи красится по up/down
        // (мигает зелёным/красным на оси цены); наш §R2-пилс при наведении даёт значение
        // в фиксированном цвете. Для OHLC отключаем мигающий лейбл.
        const up = rc('var(--oi-green)'), down = rc('var(--oi-red)');
        s = chart.addCandlestickSeries({ upColor: up, downColor: down, borderUpColor: up, borderDownColor: down, wickUpColor: up, wickDownColor: down, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: false, priceFormat });
      } else if (def.type === 'bar') {
        const up = rc('var(--oi-green)'), down = rc('var(--oi-red)');
        s = chart.addBarSeries({ upColor: up, downColor: down, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: false, priceFormat });
      } else {
        s = chart.addHistogramSeries({ color: col, base: def.base ?? 0, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: lastHist, priceFormat });
      }
      try {
        // union-тип s → .setData требует точного типа данных: касты по ветке.
        if (isOhlc) {
          (s as ISeriesApi<'Candlestick'>).setData(def.data.map((p) => ({ time: p.time as UTCTimestamp, open: p.open ?? p.value, high: p.high ?? p.value, low: p.low ?? p.value, close: p.close ?? p.value })));
        } else {
          (s as ISeriesApi<'Line'>).setData(def.data.map((p) => ({ time: p.time as UTCTimestamp, value: p.value, ...(p.color ? { color: rc(p.color) } : {}) })));
        }
      } catch (err) {
        console.error('LwChart setData failed:', def.id, err);
      }
      if (def.zeroLine) {
        s.createPriceLine({ price: 0, color: col, lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: false, title: '' });
      }
      seriesApiRef.current.push(s);
    }

    // Уровни-линии алертов (§5.6): рисуем на первой серии нужной оси. Живут вместе
    // с серией (removeSeries снимает и линии), поэтому перерисовываются этим же
    // эффектом при смене priceLines.
    if (priceLines && priceLines.length) {
      for (const pl of priceLines) {
        const sc = pl.scale ?? 'left';
        let idx = series.findIndex((d) => (d.scale ?? 'right') === sc);
        if (idx < 0) idx = 0;
        const s = seriesApiRef.current[idx];
        if (!s) continue;
        s.createPriceLine({ price: pl.price, color: rc(pl.color ?? 'var(--accent)'), lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: true, title: pl.title ?? 'алерт' });
      }
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

    const fitChanged = fitKey !== lastFitRef.current;
    if (fitChanged) {
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

    // §R2-6 (как на сайте): анимация появления — CSS-«штора» слева направо (clip-path
    // по всему холсту графика), на ПЕРВОЙ отрисовке и при ЛЮБОЙ смене данных
    // (инструмент/ТФ/показатель/группа). Сигнатура данных отсекает срабатывания на
    // смену priceLines/настроек (данные те же). Без autoscale-рывков — чистый reveal.
    if (animate) {
      const sig = series.map((s) => s.id + '#' + s.data.length + '#' + (s.data.length ? s.data[s.data.length - 1].value + '/' + s.data[0].value : '')).join('|');
      if (sig !== dataSigRef.current) {
        dataSigRef.current = sig;
        const b = boxRef.current;
        if (b) {
          b.style.transition = 'none';
          b.style.clipPath = 'inset(0 100% 0 0)';
          void b.offsetWidth;   // reflow — зафиксировать стартовое (скрытое) состояние
          b.style.transition = 'clip-path 0.55s cubic-bezier(0.22, 1, 0.36, 1)';
          b.style.clipPath = 'inset(0 0 0 0)';
        }
      }
    }

    // §OI-3: первая серия каждой оси + её форматтер + последнее значение — источник
    // цены/пилюли для «+» алертов. Пересобираем вместе с сериями.
    axisInfoRef.current = {};
    for (let i = 0; i < series.length; i++) {
      const sc = series[i].scale ?? 'right';
      if (!axisInfoRef.current[sc]) {
        const d = series[i];
        axisInfoRef.current[sc] = { api: seriesApiRef.current[i], fmt: d.axisFmt, last: d.data.length ? d.data[d.data.length - 1].value : undefined, color: rc(d.color), lastVisible: d.lastValueVisible ?? chartPrefs?.lastValue ?? (d.type === 'histogram' ? false : true) };
      }
    }
    // §R2-17: превью price line алерта на первой серии каждой оси (пересоздаём вместе
    // с сериями). Изначально скрыта; показывается/двигается в showChipsAt при наведении.
    previewLineRef.current = {};
    for (const side of ['left', 'right'] as const) {
      const info = axisInfoRef.current[side];
      if (info) {
        previewLineRef.current[side] = info.api.createPriceLine({
          price: info.last ?? 0, color: crossColorRef.current, lineWidth: 1, lineStyle: LineStyle.Dotted,
          lineVisible: false, axisLabelVisible: false, axisLabelColor: info.color ?? '#888888', axisLabelTextColor: '#ffffff', title: '',
        });
      }
    }
    // §R2-30: priceScale('left'/'right').applyOptions({visible}) выше меняет видимость шкалы
    // синхронно, но внутренний layout lightweight-charts под неё пересчитывается только на
    // следующем кадре — любой .width() в этом же тике на только что показанной шкале даёт
    // Error('Value is null'). И layoutAlert, и drawExp читают priceScale(side).width()
    // (drawExp — строки paneL/paneW), поэтому откладываем ОБА на rAF (refs занулены в cleanup
    // → при размонтировании до кадра вызовы — no-op).
    requestAnimationFrame(() => { layoutAlertRef.current?.(); drawExpRef.current?.(); });
  }, [series, markers, fitKey, expirations, chartPrefs, priceLines, animate]);

  // §OI-3: смена alertAxes (напр. вариант «both» выключает уровневые алерты) —
  // пере-раскладываем полосы, не пересобирая серии.
  useEffect(() => { layoutAlertRef.current?.(); }, [alertAxes]);

  // Рисование: перерисовать фигуры + пересинхронить интерактивность оверлея при смене
  // карандаша/инструмента/фигур/выделения/стиля (сам чарт не пересоздаётся).
  useEffect(() => { drawShapesRef.current?.(); }, [drawActive, drawTool, drawings, selectedDrawId, drawColor, drawWidth, drawMagnet, drawHidden, drawLocked]);

  // 62px — отступ под реально существующую левую ценовую шкалу (индекс/цена
  // слева, как на ОИ/СЧА-Фондах). Если ни одна серия НЕ сидит на 'left' —
  // сама шкала визуально пустая (0 контента → 0 ширины в lightweight-charts,
  // см. Сила рынка/minimumWidth), и watermark на 62px повисал в пустоте
  // посреди графика вместо того чтобы прижаться к левому краю (Притоки/Оттоки
  // и любой другой график с одной серией на правой оси).
  const hasLeftAxisSeries = series.some((s) => s.scale === 'left');

  return (
    <div style={{ position: 'relative', width: '100%', height }}>
      <div ref={boxRef} style={{ position: 'absolute', inset: 0 }} />
      {watermark !== false && (
        <ChartWatermark bottom={30} left={hasLeftAxisSeries ? 62 : 12} size={26} minSize={16} opacity={0.4} />
      )}
    </div>
  );
}
