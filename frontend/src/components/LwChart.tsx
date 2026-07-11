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
} from 'lightweight-charts';
import ChartWatermark from './ChartWatermark';

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

/** §OI-4: метка экспирации (смена контракта) — кружок у оси дат с hover-тултипом,
 *  как на сайте (SimpleChart annotations). label — короткий код нового контракта
 *  (2 символа в кружке), description — полный текст тултипа (напр. «SiM6 → SiU6»). */
export interface LwExpiration { time: number; label: string; description: string }

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

export default function LwChart({ series, height, dark = true, markers, fitKey, initialBars, tickFmt, legendItems, hideLegend, expirations, priceLines, onCreateAlert, alertAxes, watermark, animate }: LwChartProps) {
  const boxRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesApiRef = useRef<ISeriesApi<'Line' | 'Area' | 'Histogram'>[]>([]);
  const defsRef = useRef<LwSeries[]>(series);
  defsRef.current = series;
  // Читаем через ref, чтобы смена пропа-функции/массива не пересоздавала чарт/серии.
  const tickFmtRef = useRef(tickFmt); tickFmtRef.current = tickFmt;
  const legendItemsRef = useRef(legendItems); legendItemsRef.current = legendItems;
  const hideLegendRef = useRef(hideLegend); hideLegendRef.current = hideLegend;
  const expRef = useRef(expirations); expRef.current = expirations;
  const drawExpRef = useRef<(() => void) | null>(null);
  // §OI-3 axis-алерты: пропы через ref (смена не пересоздаёт чарт); axisInfoRef —
  // первая серия каждой оси + её форматтер + последнее значение (заполняется в
  // эффекте серий); layoutAlertRef — пересчёт геометрии полос у осей.
  const alertAxesRef = useRef(alertAxes); alertAxesRef.current = alertAxes;
  const onCreateAlertRef = useRef(onCreateAlert); onCreateAlertRef.current = onCreateAlert;
  const axisInfoRef = useRef<{ [k in 'left' | 'right']?: { api: ISeriesApi<'Line' | 'Area' | 'Histogram'>; fmt?: (v: number) => string; last?: number; color?: string; lastVisible?: boolean } }>({});
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
      timeScale: { borderVisible: false, rightOffset: 6, secondsVisible: false, tickMarkFormatter: (time: Time, type: number) => (tickFmtRef.current ? tickFmtRef.current(time as unknown as number, type) : ruTickMark(time, type)) },
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
      for (const ex of exps) {
        const x = ts.timeToCoordinate(ex.time as UTCTimestamp);
        if (x == null) continue;
        const circle = document.createElement('div');
        circle.style.cssText = [
          'position:absolute', 'bottom:2px', 'left:' + x + 'px', 'transform:translateX(-50%)',
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
          expGuide.style.left = x + 'px';
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

    // ── §OI-3/§R2-17 axis-алерты (как на сайте / TradingView): превью-уровень —
    // НАТИВНАЯ price line на первой серии оси: серый ПУНКТИР (цвет кроссхэйра) в поле
    // графика + цветной ЛЕЙБЛ значения в жёлобе, выровненный по числам оси НАТИВНО (тот
    // же форматтер и колонка, что тики и пилс последнего значения). Плюс DOM-кружок «+»
    // в поле у оси — клик = onCreateAlert. Гейт — onCreateAlert + alertAxes; иначе инертно.
    const alertChips: { [k in 'left' | 'right']?: HTMLDivElement } = {};
    const alertStrips: { [k in 'left' | 'right']?: HTMLDivElement } = {};
    const alertPending: { [k in 'left' | 'right']?: { axis: 'left' | 'right'; price: number; currentValue: number } } = {};
    const hidePreview = (side: 'left' | 'right') => {
      const pl = previewLineRef.current[side];
      if (pl) { try { pl.applyOptions({ lineVisible: false, axisLabelVisible: false }); } catch { /* серия снята */ } }
      // §R2-18/19: вернуть лейбл последнего значения (прятали на время превью, чтобы
      // нативная коллизия лейблов не расталкивала их). ⚠️ applyOptions на СЕРИИ
      // пересчитывает ценовую шкалу — дёргаем ТОЛЬКО при смене состояния (иначе фриз).
      const info = axisInfoRef.current[side];
      if (info) { const want = info.lastVisible ?? true; try { if (info.api.options().lastValueVisible !== want) info.api.applyOptions({ lastValueVisible: want }); } catch { /* серия снята */ } }
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
        // Нативный уровень + лейбл (выровнен по числам оси НАТИВНО): пунктир серый (цвет
        // кроссхэйра), лейбл — фон в цвет линии, текст белый, значение форматтером серии.
        const pl = previewLineRef.current[side];
        if (pl) { try { pl.applyOptions({ price: price as number, lineVisible: true, axisLabelVisible: true, color: crossColorRef.current, axisLabelColor: info.color ?? '#888888', axisLabelTextColor: '#ffffff' }); } catch { /* серия снята */ } }
        // §R2-18/19: пока превью активно, прячем лейбл последнего значения — иначе
        // движок расталкивает пересекающиеся лейблы. ⚠️ Дёргаем applyOptions ТОЛЬКО при
        // смене состояния (не каждый mousemove) — иначе пересчёт шкалы каждый кадр = фриз.
        try { if (info.api.options().lastValueVisible !== false) info.api.applyOptions({ lastValueVisible: false }); } catch { /* серия снята */ }
        // Кружок «+» — в поле у оси (выступает на график), в цвет линии.
        const axisW = ch.priceScale(side).width() || 54;
        const cw = 15;
        chip.style.top = y + 'px';
        chip.style.display = 'flex';
        chip.style.background = info.color || 'var(--accent,#FF5C2B)';
        chip.style.left = (side === 'left' ? axisW + 2 : box.clientWidth - axisW - cw - 2) + 'px';
        alertPending[side] = { axis: side, price: price as number, currentValue: info.last ?? (price as number) };
      }
    };
    for (const side of ['left', 'right'] as const) {
      const strip = document.createElement('div');
      strip.style.cssText = 'position:absolute;top:0;display:none;pointer-events:auto;z-index:6;cursor:crosshair;' + side + ':0';
      strip.addEventListener('mousemove', (e) => showChipsAt(e.clientY - box.getBoundingClientRect().top));
      box.appendChild(strip);
      alertStrips[side] = strip;
      // §R2-17: чип = только кружок «+» (значение показывает нативный лейбл price line,
      // выровненный по числам оси). Позиция/цвет — в showChipsAt.
      const chip = document.createElement('div');
      chip.style.cssText = 'position:absolute;display:none;width:15px;height:15px;border-radius:50%;'
        + 'align-items:center;justify-content:center;font-size:13px;font-weight:700;line-height:1;color:#fff;'
        + 'transform:translateY(-50%);pointer-events:auto;cursor:pointer;z-index:7;box-shadow:0 1px 4px rgba(0,0,0,0.35)';
      chip.textContent = '+';
      chip.title = 'Поставить алерт на этом уровне';
      chip.addEventListener('click', (e) => {
        e.stopPropagation();
        const p = alertPending[side];
        if (p && onCreateAlertRef.current) onCreateAlertRef.current(p);
      });
      box.appendChild(chip);
      alertChips[side] = chip;
    }
    box.addEventListener('mouseleave', hideChips);
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
        const w = ch.priceScale(side).width() || 54;
        strip.style.display = 'block';
        strip.style.width = Math.max(28, w) + 'px';
        strip.style.height = h + 'px';
      }
    };
    layoutAlertRef.current = layoutAlert;

    const onRange = () => { drawExp(); layoutAlert(); };
    chart.timeScale().subscribeVisibleLogicalRangeChange(onRange);
    const expRo = new ResizeObserver(() => { drawExp(); layoutAlert(); });
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
      const w = box.clientWidth, tw = tip.offsetWidth, GAP = 16;
      // §R2-10: по умолчанию тултип справа от курсора с зазором GAP от перекрестья;
      // флип влево ТЕМ ЖЕ зазором только если у правого края не влезает (по переполнению,
      // а не по середине графика) — убирает несимметричный прыжок на w/2.
      let rawLeft = param.point.x + GAP;
      if (rawLeft + tw > w - 6) rawLeft = param.point.x - tw - GAP;
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
      box.removeEventListener('mouseleave', hideChips);
      expRo.disconnect();
      try { chart.timeScale().unsubscribeVisibleLogicalRangeChange(onRange); } catch { /* уже снят */ }
      drawExpRef.current = null;
      layoutAlertRef.current = null;
      chart.remove();
      chartRef.current = null;
      seriesApiRef.current = [];
      axisInfoRef.current = {};
      for (const el of [tip, legend, expLayer, expGuide, alertStrips.left, alertStrips.right, alertChips.left, alertChips.right]) {
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
      const lastLine = chartPrefs?.lastValue ?? def.lastValueVisible ?? true;
      const lastHist = chartPrefs?.lastValue ?? def.lastValueVisible ?? false;
      const col = rc(def.color);
      let s: ISeriesApi<'Line' | 'Area' | 'Histogram'>;
      const lineStyle = def.dashed ? LineStyle.Dashed : LineStyle.Solid;
      if (def.type === 'line') {
        s = chart.addLineSeries({ color: col, lineWidth: lw, lineStyle, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: lastLine, priceFormat });
      } else if (def.type === 'area') {
        s = chart.addAreaSeries({ lineColor: col, topColor: rc(def.areaTop ?? def.color), bottomColor: def.areaBottom ? rc(def.areaBottom) : 'rgba(0,0,0,0)', lineWidth: lw, lineStyle, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: lastLine, priceFormat });
      } else {
        s = chart.addHistogramSeries({ color: col, base: def.base ?? 0, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: lastHist, priceFormat });
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
        axisInfoRef.current[sc] = { api: seriesApiRef.current[i], fmt: d.axisFmt, last: d.data.length ? d.data[d.data.length - 1].value : undefined, color: rc(d.color), lastVisible: chartPrefs?.lastValue ?? d.lastValueVisible ?? (d.type === 'histogram' ? false : true) };
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
    layoutAlertRef.current?.();     // показать/скрыть полосы алертов под текущий набор осей
    drawExpRef.current?.();         // метки экспираций — после заливки данных и установки окна
  }, [series, markers, fitKey, expirations, chartPrefs, priceLines, animate]);

  // §OI-3: смена alertAxes (напр. вариант «both» выключает уровневые алерты) —
  // пере-раскладываем полосы, не пересобирая серии.
  useEffect(() => { layoutAlertRef.current?.(); }, [alertAxes]);

  return (
    <div style={{ position: 'relative', width: '100%', height }}>
      <div ref={boxRef} style={{ position: 'absolute', inset: 0 }} />
      {watermark !== false && (
        <ChartWatermark bottom={30} left={62} size={26} minSize={16} opacity={0.4} />
      )}
    </div>
  );
}
