/**
 * LwChartPanes — N стэкнутых Lightweight-графиков как ОДИН индикатор (§5.7 макета).
 * Для «Силы рынка»: сверху индекс, снизу breadth, между ними:
 *   - общая ось времени: пан/зум синхронны (subscribeVisibleLogicalRangeChange
 *     с guard-флагом от рекурсии);
 *   - общий кроссхэйр: setCrosshairPosition на соседях с _suppress-гейтом
 *     ПЕРВОЙ строкой обработчика (иначе бесконечный цикл вешает вкладку);
 *   - ось дат видна ТОЛЬКО на нижней панели.
 *
 * Сознательно отдельный компонент, а не ветка внутри LwChart: единственный
 * потребитель — Сила рынка, а боевой одиночный LwChart (ОИ/Баффетт/Фонды/
 * Сезонность) не трогаем вообще. Общие типы (LwSeries) импортируются оттуда.
 *
 * Рисование (drawPaneIndex): та же модель TradingView, что у LwChart (SVG-
 * оверлей в координатах {logical,price}), но привязана к ОДНОЙ конкретной
 * панели — рисовать сразу на двух синхронных чартах не нужно, и хит-тест/
 * магнит по OHLC не имеет смысла между панелями с разными шкалами. Логика
 * порт-скопирована из LwChart.tsx (renderOne/hitTest/pointer-обработчики
 * идентичны); отличие — координатные хелперы бьются на `charts[drawPaneIndex]`
 * вместо единственного chartRef, и paneLeftW=0 всегда (левая шкала здесь
 * везде скрыта, в отличие от LwChart, где бывает видимой).
 */
import { forwardRef, useContext, useEffect, useImperativeHandle, useRef } from 'react';
import {
  createChart, ColorType, LineStyle, CrosshairMode,
  LineSeries, AreaSeries, HistogramSeries, CandlestickSeries, BarSeries,
  type IChartApi, type ISeriesApi, type IPriceLine, type UTCTimestamp, type Time, type LogicalRange,
  type Logical, type Coordinate,
} from 'lightweight-charts';
import ChartWatermark from './ChartWatermark';
import { createExpirationsLayer, type ExpirationMark } from './chart/expirationsLayer';
import { VolumeProfilePrimitive, type VolumeProfileOptions } from './chart/volumeProfilePrimitive';
import { BandsPrimitive } from './chart/bandsPrimitive';
import {
  ChartPrefsCtx, hideTvLogo, ruTickMark, type LwSeries,
  type LwDrawing, type LwDrawTool, type LwDrawPoint, type LwDash, type LwMagnet,
} from './chart/lwTypes';
import { captureFontScale } from './chart/chartTypography';

const BASE_FONT_SIZE = 11;

export interface LwPane {
  series: LwSeries[];
  /** Доля высоты (flex-grow). Дефолт 1. Сила рынка в макете ≈ 1.1 / 0.9. */
  flex?: number;
}

export interface VolumeProfileSpec extends VolumeProfileOptions {
  /** id серии панели 0, к которой крепится профиль. */
  seriesId: string;
}

export interface LwChartPanesHandle {
  /** Форс-синк размера + перерисовка фигур ПЕРЕД снятием скриншота — см.
   *  LwChartHandle.syncBeforeCapture в LwChart.tsx (тот же паттерн, включая
   *  временное увеличение layout.fontSize ВСЕХ панелей под captureFontScale). */
  syncBeforeCapture: (width: number, height: number) => void;
  /** Возвращает layout.fontSize всех панелей к базовому — см. LwChartHandle. */
  restoreAfterCapture: () => void;
}

interface LwChartPanesProps {
  panes: LwPane[];
  dark?: boolean;
  fitKey?: string;
  initialBars?: number;
  /** Формат оси времени нижней панели. Дефолт — §5.2 (только месяцы+годы). */
  tickFmt?: (time: number, type: number) => string;
  /** Курсорный тултип со значениями всех панелей. Дефолт true; Сила рынка
   *  отключает — легенда сверху панелей уже даёт статичные значения, курсорный
   *  тултип поверх узкого графика избыточен (Вадим). */
  showTooltip?: boolean;
  /** Индекс панели, на которой доступно рисование (модель TradingView). Без
   *  этого пропа рисование не создаётся вообще — нулевой оверхед для панелей,
   *  которым оно не нужно. */
  drawPaneIndex?: number;
  drawActive?: boolean;
  drawTool?: LwDrawTool;
  drawings?: LwDrawing[];
  onDrawingsChange?: (d: LwDrawing[]) => void;
  drawColor?: string;
  drawWidth?: number;
  selectedDrawId?: string | null;
  /** Клик по «+» у ценовой оси → создать алерт на уровне под курсором. Работает
   *  на панели цены (индекс 0), как и экспирации. */
  onCreateAlert?: (p: { axis: 'left' | 'right'; price: number; currentValue: number }) => void;
  /** На каких осях показывать «+». Пустой массив/undefined — не показывать. */
  alertAxes?: ('left' | 'right')[];
  /** Метки экспираций (смена контракта). Рисуются на панели цены (индекс 0). */
  expirations?: ExpirationMark[];
  /** Профиль объёма на панели 0. `seriesId` — на какую серию вешать: профиль
   *  берёт у неё priceToCoordinate, поэтому серия обязана быть ЦЕНОВОЙ. Если
   *  серии с таким id нет (пользователь скрыл цену) — профиль не рисуется. */
  volumeProfile?: (VolumeProfileSpec) | null;
  /** Что отрисовать ВНУТРИ панели с этим индексом (строка индикатора над своей
   *  панелью). Панель — position:relative, так что абсолютный ребёнок ложится
   *  по её углам, а не по углам всего чарта. */
  paneOverlay?: (paneIndex: number) => React.ReactNode;
  /** Показывать время в подписях оси (интрадей). Без него ось никогда не даёт
   *  тик-марки типа Time, и 5м/1ч физически не отображаются. */
  timeVisible?: boolean;
  /** Уровни-пунктиры (активные алерты). pane — на какой панели, по умолчанию 0. */
  priceLines?: { price: number; color?: string; scale?: 'left' | 'right'; title?: string; pane?: number }[];
  /** Водяной знак «Фрейм» в углу (как в LwChart). false — выключить. */
  watermark?: boolean;
  /** Не рисовать встроенную легенду: её заменяет React-список индикаторов. */
  hideLegend?: boolean;
  /** Оверрайд пунктов легенды панели 0 (одна серия → несколько подписей: у Фондов
   *  «Приток»/«Отток» у одной гистограммы, у Сезонности «Рост»/«Падение»). */
  legendItems?: { label: string; color: string }[];
  /** Формат подписи времени в кроссхэйре (Сезонность: синтетическое время). */
  crosshairTimeFmt?: (time: number) => string;
  onSelectDraw?: (id: string | null) => void;
  /** Бокс выделенной фигуры в пикселях КОРНЕВОГО контейнера (не пейна) — якорь
   *  контекстной панели свойств. См. LwChart.onSelectionRect. */
  onSelectionRect?: (r: { x: number; y: number; w: number; h: number } | null) => void;
  drawMagnet?: LwMagnet;
  drawHidden?: boolean;
  drawLocked?: boolean;
  drawDash?: LwDash;
  drawOpacity?: number;
  onToolReset?: () => void;
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

type AnySeries = ISeriesApi<'Line' | 'Area' | 'Histogram' | 'Candlestick' | 'Bar'>;

const SVGNS = 'http://www.w3.org/2000/svg';

/** Дивы панелей — строго по data-lw-pane, а не по порядку детей корня: в корне
 *  живёт ещё и водяной знак, и любой будущий оверлей. */
function paneBoxes(root: HTMLElement): HTMLElement[] {
  return Array.from(root.querySelectorAll(':scope > [data-lw-pane]')) as HTMLElement[];
}
const FIB = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1, 1.618, 2.618, 3.618, 4.236];
const ONE_PT = new Set<string>(['hline', 'vline', 'text', 'brush']);

const LwChartPanes = forwardRef<LwChartPanesHandle, LwChartPanesProps>(function LwChartPanes({
  panes, dark = true, fitKey, initialBars, tickFmt, showTooltip = true,
  drawPaneIndex, drawActive, drawTool, drawings, onDrawingsChange, drawColor, drawWidth,
  watermark, hideLegend, legendItems, crosshairTimeFmt, timeVisible, priceLines, expirations, volumeProfile, onCreateAlert, alertAxes,
  paneOverlay,
  selectedDrawId, onSelectDraw, onSelectionRect, drawMagnet, drawHidden, drawLocked, drawDash, drawOpacity, onToolReset,
}: LwChartPanesProps, forwardedRef) {
  const rootRef = useRef<HTMLDivElement>(null);
  const chartsRef = useRef<IChartApi[]>([]);
  const apisRef = useRef<AnySeries[][]>([]);          // [pane][series]
  // Невидимые ряды-хребты, по одному на панель: держат общее индексное
  // пространство времени (см. spineTimes в эффекте серий). Хранятся отдельно от
  // apisRef, чтобы не сбить парность apisRef[i][k] ↔ mapsRef[i][k] в тултипе.
  const spinesRef = useRef<AnySeries[]>([]);
  const mapsRef = useRef<Map<number, number>[][]>([]); // [pane][series] time→value
  const panesRef = useRef<LwPane[]>(panes); panesRef.current = panes;
  const tipsRef = useRef<HTMLDivElement[]>([]);
  const legendsRef = useRef<HTMLDivElement[]>([]);
  const lastFitRef = useRef<string | undefined>(undefined);
  const tickFmtRef = useRef(tickFmt); tickFmtRef.current = tickFmt;
  const showTooltipRef = useRef(showTooltip); showTooltipRef.current = showTooltip;
  const paneCount = panes.length;
  const chartPrefs = useContext(ChartPrefsCtx);

  // ── рисование: рефы состояния (та же идиома, что в LwChart.tsx) ──
  const drawActiveRef = useRef(drawActive); drawActiveRef.current = drawActive;
  const drawToolRef = useRef(drawTool); drawToolRef.current = drawTool;
  const drawingsRef = useRef(drawings); drawingsRef.current = drawings;
  const onDrawingsChangeRef = useRef(onDrawingsChange); onDrawingsChangeRef.current = onDrawingsChange;
  const drawColorRef = useRef(drawColor); drawColorRef.current = drawColor;
  const drawWidthRef = useRef(drawWidth); drawWidthRef.current = drawWidth;
  const selectedDrawIdRef = useRef(selectedDrawId); selectedDrawIdRef.current = selectedDrawId;
  const onSelectDrawRef = useRef(onSelectDraw); onSelectDrawRef.current = onSelectDraw;
  const onSelectionRectRef = useRef(onSelectionRect); onSelectionRectRef.current = onSelectionRect;
  const drawMagnetRef = useRef(drawMagnet); drawMagnetRef.current = drawMagnet;
  const drawHiddenRef = useRef(drawHidden); drawHiddenRef.current = drawHidden;
  const drawLockedRef = useRef(drawLocked); drawLockedRef.current = drawLocked;
  const drawDashRef = useRef(drawDash); drawDashRef.current = drawDash;
  const drawOpacityRef = useRef(drawOpacity); drawOpacityRef.current = drawOpacity;
  const onToolResetRef = useRef(onToolReset); onToolResetRef.current = onToolReset;
  const hideLegendRef = useRef(hideLegend); hideLegendRef.current = hideLegend;
  const legendItemsRef = useRef(legendItems); legendItemsRef.current = legendItems;
  // Снапшот на монтаже: включён формат или нет, меняться не может (потребитель
  // либо передаёт его всю жизнь компонента, либо никогда) — меняется только сама
  // подпись, и её читаем через ref.
  const crossFmtRef = useRef(crosshairTimeFmt); crossFmtRef.current = crosshairTimeFmt;
  const expRef = useRef(expirations); expRef.current = expirations;
  // Профиль объёма живёт ВНЕ эффекта серий: держать его в его депсах значило бы
  // пересоздавать все серии на каждую правку числа уровней (сотня миллисекунд
  // ради перерисовки одного слоя). Эффект серий только переприкрепляет примитив
  // после пересоздания серии-носителя, а сами опции доезжают отдельным эффектом.
  const vpSpecRef = useRef(volumeProfile); vpSpecRef.current = volumeProfile;
  const vpRef = useRef<{ prim: VolumeProfilePrimitive; api: AnySeries } | null>(null);
  const syncVpRef = useRef<(() => void) | null>(null);
  const onCreateAlertRef = useRef(onCreateAlert); onCreateAlertRef.current = onCreateAlert;
  const alertAxesRef = useRef(alertAxes); alertAxesRef.current = alertAxes;
  // Превью-уровень = НАТИВНАЯ price line. В отличие от LwChart здесь у неё включён
  // axisLabelVisible: значение рисует сама библиотека. Самодельный DOM-пилс
  // (попиксельная реплика лейбла оси с калибровкой кегля после fonts.ready) не
  // портируется намеренно — в стеке панелей он умножался бы на их число.
  const previewLineRef = useRef<{ [k in 'left' | 'right']?: IPriceLine }>({});
  const axisInfoRef = useRef<{ [k in 'left' | 'right']?: { api: AnySeries; last?: number; color?: string } }>({});
  const layoutAlertRef = useRef<(() => void) | null>(null);
  // Последний видимый диапазон. Эффект создания чартов зависит от paneCount, то
  // есть «вынести индикатор в свою панель» ПЕРЕСОЗДАЁТ все инстансы — и зум
  // слетел бы на fit ровно в момент, когда пользователь этого не просил.
  // Читаем его при восстановлении, если у свежесозданного чарта своего ещё нет.
  const lastRangeRef = useRef<LogicalRange | null>(null);
  const drawExpRef = useRef<(() => void) | null>(null);
  const drawShapesRef = useRef<(() => void) | null>(null);
  const drawPaneIndexRef = useRef(drawPaneIndex); drawPaneIndexRef.current = drawPaneIndex;

  useImperativeHandle(forwardedRef, () => ({
    syncBeforeCapture: () => {
      const pi = drawPaneIndexRef.current;
      if (pi != null) {
        const root = rootRef.current;
        const box = root ? paneBoxes(root)[pi] : null;
        const chart = chartsRef.current[pi];
        if (box && chart) {
          // panes делят h пропорционально flex — сам drawPaneIndex-бокс мог не
          // получить обновлённый размер синхронно; берём его РЕАЛЬНЫЙ текущий
          // clientWidth/Height (уже актуальный к моменту вызова), а не входные w/h
          // (те — размер ВСЕГО стека панелей, не одной панели).
          chart.resize(box.clientWidth, box.clientHeight);
        }
      }
      // Шрифт масштабируем по ширине ВСЕГО стека панелей (общая для всех) —
      // см. LwChartHandle.syncBeforeCapture в LwChart.tsx.
      const stackW = rootRef.current?.clientWidth;
      if (stackW) {
        const fs = Math.round(BASE_FONT_SIZE * captureFontScale(stackW));
        chartsRef.current.forEach((chart) => chart.applyOptions({ layout: { fontSize: fs } }));
      }
      drawShapesRef.current?.();
    },
    restoreAfterCapture: () => {
      chartsRef.current.forEach((chart) => chart.applyOptions({ layout: { fontSize: BASE_FONT_SIZE } }));
    },
  }), []);

  // ── создание N чартов + связка (пересоздаётся при смене числа панелей) ──
  useEffect(() => {
    const root = rootRef.current;
    if (!root || paneCount === 0) return;
    const c = themeColors(dark);
    const charts: IChartApi[] = [];
    const tips: HTMLDivElement[] = [];
    const legends: HTMLDivElement[] = [];
    const unsubs: (() => void)[] = [];
    const boxes = paneBoxes(root);

    let syncingRange = false; // guard: программная установка диапазона
    let suppress = false;     // _suppress: программный кроссхэйр соседа

    for (let i = 0; i < paneCount; i++) {
      const box = boxes[i];
      if (!box) continue;
      const isLast = i === paneCount - 1;
      const chart = createChart(box, {
        autoSize: true,
        layout: { background: { type: ColorType.Solid, color: 'transparent' }, textColor: c.text, fontFamily: 'Inter, -apple-system, sans-serif', fontSize: BASE_FONT_SIZE },
        localization: {
          locale: 'ru-RU',
          // Снапшот на монтаже — как в LwChart: сам факт «формат включён» не
          // меняется на лету, меняется только подпись (через ref).
          ...(crossFmtRef.current ? { timeFormatter: (t: Time) => crossFmtRef.current!(t as unknown as number) } : {}),
        },
        grid: { vertLines: { color: c.grid }, horzLines: { color: c.grid } },
        leftPriceScale: { visible: false, borderVisible: false, scaleMargins: { top: 0.12, bottom: 0.06 } },
        rightPriceScale: { visible: true, borderVisible: false, scaleMargins: { top: 0.12, bottom: 0.06 } },
        timeScale: {
          borderVisible: false, rightOffset: 6, secondsVisible: false,
          visible: isLast, // §5.7: ось дат только на нижней панели
          tickMarkFormatter: (time: Time, type: number) =>
            (tickFmtRef.current ?? ruTickMark)(time as unknown as number, type),
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
      // Тот же класс, что у тултипа LwChart: по нему песочница гасит курсорную
      // подсказку (sandbox.css) и по нему же его вычищает экспорт в PNG. Без
      // класса тултип Panes не попадал ни под одно из этих правил.
      tip.className = 'chart-tooltip-root';
      tip.style.cssText = [
        'position:absolute', 'pointer-events:none', 'z-index:6', 'display:none',
        'background:var(--bg-secondary,#17161A)', 'border:1px solid var(--border-color,rgba(245,241,232,0.18))',
        'border-radius:7px', 'padding:5px 8px', 'font-size:10.5px', 'color:var(--text-primary,#F5F1E8)',
        'white-space:nowrap', 'box-shadow:0 8px 22px rgba(0,0,0,0.45)', 'font-family:Inter,-apple-system,sans-serif',
      ].join(';');
      box.appendChild(tip);
      tips.push(tip);

      // Центрированная легенда панели (= заголовок серии, как в макете).
      // hideLegend — когда её заменяет React-список индикаторов: иначе на экране
      // окажутся две легенды. Элемент всё равно создаём (индексы legends[] должны
      // совпадать с панелями), просто не показываем.
      const legend = document.createElement('div');
      legend.style.cssText = [
        'position:absolute', 'top:7px', 'left:50%', 'transform:translateX(-50%)', 'z-index:5',
        'display:flex', 'flex-wrap:wrap', 'justify-content:center', 'gap:14px', 'pointer-events:none',
        'max-width:calc(100% - 130px)',
      ].join(';');
      // ⚠️ ПОСЛЕ cssText: присвоение style.cssText перезаписывает стиль целиком и
      // затёрло бы display, если поставить его строкой выше.
      if (hideLegendRef.current) legend.style.display = 'none';
      box.appendChild(legend);
      legends.push(legend);
    }

    // ── синк диапазона времени (guard от рекурсии) ──
    charts.forEach((chart, i) => {
      const handler = (range: LogicalRange | null) => {
        if (syncingRange || !range) return;
        lastRangeRef.current = range;
        syncingRange = true;
        try {
          charts.forEach((other, j) => { if (j !== i) other.timeScale().setVisibleLogicalRange(range); });
        } finally { syncingRange = false; }
      };
      chart.timeScale().subscribeVisibleLogicalRangeChange(handler);
      unsubs.push(() => chart.timeScale().unsubscribeVisibleLogicalRangeChange(handler));
      if (i === 0) {
        const redrawExp = () => { drawExpRef.current?.(); layoutAlertRef.current?.(); };
        chart.timeScale().subscribeVisibleLogicalRangeChange(redrawExp);
        unsubs.push(() => chart.timeScale().unsubscribeVisibleLogicalRangeChange(redrawExp));
      }
    });

    // ── метки экспираций (общий модуль, тот же что в LwChart) ──
    // Слой живёт на панели ЦЕНЫ (0), а высоту оси дат берём у НИЖНЕЙ панели:
    // в стеке ось есть только у неё (visible: isLast), и своя высота у панели 0
    // равна нулю — кружки встали бы посреди графика, оторванные от дат.
    let expLayerApi: { draw: () => void; destroy: () => void } | null = null;
    if (boxes[0]) {
      expLayerApi = createExpirationsLayer({
        box: boxes[0],
        getChart: () => chartsRef.current[0] ?? null,
        getMarks: () => expRef.current,
        getAxisHeight: () => {
          const last = chartsRef.current[chartsRef.current.length - 1];
          if (!last) return 26;
          // Ось у нижней панели; на панели цены её высота не входит в её бокс,
          // поэтому в одиночном чарте это своя ось, а в стеке — 0 отступа.
          return chartsRef.current.length > 1 ? 0 : (last.timeScale().height() || 26);
        },
      });
      drawExpRef.current = () => expLayerApi?.draw();
    }

    // ── §OI-3 алерты: «+» у ценовой оси панели цены ──────────────────────────
    // ДЕШЁВЫЙ вариант порта: превью-уровень — нативная price line с
    // axisLabelVisible, значение рисует сама библиотека. Самодельный DOM-пилс из
    // LwChart (реплика лейбла оси с калибровкой кегля по canvas/DOM после
    // fonts.ready и подгонкой чётности) НЕ переносим: в стеке он умножался бы на
    // число панелей плюс вопрос «какая панель под курсором». Внешне уровень
    // выглядит как подпись оси, а не как оранжевый чип.
    const alertStrips: { [k in 'left' | 'right']?: HTMLDivElement } = {};
    const alertPlus: { [k in 'left' | 'right']?: HTMLDivElement } = {};
    const alertPending: { [k in 'left' | 'right']?: { axis: 'left' | 'right'; price: number; currentValue: number } } = {};
    const alertBox = boxes[0];
    if (alertBox) {
      const hidePreview = (side: 'left' | 'right') => {
        const pl = previewLineRef.current[side];
        if (pl) { try { pl.applyOptions({ lineVisible: false, axisLabelVisible: false }); } catch { /* серия снята */ } }
      };
      const hideAll = () => {
        for (const side of ['left', 'right'] as const) {
          const p = alertPlus[side]; if (p) p.style.display = 'none';
          hidePreview(side);
          alertPending[side] = undefined;
        }
      };
      const showAt = (rawY: number) => {
        const ch = chartsRef.current[0];
        const axes = alertAxesRef.current;
        if (!ch || !onCreateAlertRef.current || !axes || axes.length === 0) { hideAll(); return; }
        const axisH = chartsRef.current.length > 1 ? 0 : (ch.timeScale().height() || 26);
        const y = Math.max(0, Math.min(alertBox.clientHeight - axisH, rawY));
        for (const side of ['left', 'right'] as const) {
          const plus = alertPlus[side];
          const info = axisInfoRef.current[side];
          if (!plus || !axes.includes(side) || !info) { if (plus) plus.style.display = 'none'; hidePreview(side); alertPending[side] = undefined; continue; }
          const price = info.api.coordinateToPrice(y as number);
          if (price == null) { plus.style.display = 'none'; hidePreview(side); alertPending[side] = undefined; continue; }
          const pl = previewLineRef.current[side];
          // axisLabelVisible:true — вот и вся замена самодельному пилсу.
          if (pl) { try { pl.applyOptions({ price: price as number, lineVisible: true, axisLabelVisible: true }); } catch { /* серия снята */ } }
          let axisW = 54;
          try { axisW = ch.priceScale(side).width() || 54; } catch { /* §R2-30: «Value is null» сразу после смены visible */ }
          plus.style.background = info.color || 'var(--accent,#FF5C2B)';
          plus.style.top = y + 'px';
          plus.style.display = 'flex';
          plus.style.left = side === 'left' ? axisW + 4 + 'px' : 'auto';
          plus.style.right = side === 'right' ? axisW + 4 + 'px' : 'auto';
          alertPending[side] = { axis: side, price: price as number, currentValue: info.last ?? (price as number) };
        }
      };
      for (const side of ['left', 'right'] as const) {
        const strip = document.createElement('div');
        strip.style.cssText = 'position:absolute;top:0;display:none;pointer-events:auto;z-index:6;cursor:crosshair;' + side + ':0';
        strip.addEventListener('mousemove', (e) => showAt(e.clientY - alertBox.getBoundingClientRect().top));
        alertBox.appendChild(strip);
        alertStrips[side] = strip;

        const plus = document.createElement('div');
        plus.style.cssText = 'position:absolute;display:none;align-items:center;justify-content:center;'
          + 'width:15px;height:15px;border-radius:50%;transform:translateY(-50%);z-index:7;'
          + 'cursor:pointer;pointer-events:auto;color:#fff;font-size:12px;line-height:1;font-weight:700';
        plus.textContent = '+';
        plus.addEventListener('click', (e) => {
          e.stopPropagation();
          const p = alertPending[side];
          if (p) onCreateAlertRef.current?.(p);
        });
        alertBox.appendChild(plus);
        alertPlus[side] = plus;
      }
      alertBox.addEventListener('mouseleave', hideAll);
      unsubs.push(() => alertBox.removeEventListener('mouseleave', hideAll));

      // Геометрия полос-хитзон: ширина = ширина шкалы, высота = поле без оси.
      const layoutAlert = () => {
        const ch = chartsRef.current[0];
        const axes = alertAxesRef.current;
        const axisH = chartsRef.current.length > 1 ? 0 : (ch?.timeScale().height() || 26);
        const h = Math.max(0, alertBox.clientHeight - axisH);
        for (const side of ['left', 'right'] as const) {
          const strip = alertStrips[side];
          if (!strip) continue;
          const on = !!ch && !!onCreateAlertRef.current && !!axes?.includes(side);
          if (!on) { strip.style.display = 'none'; const p = alertPlus[side]; if (p) p.style.display = 'none'; continue; }
          let w = 54;
          try { w = ch!.priceScale(side).width() || 54; } catch { /* см. §R2-30 */ }
          strip.style.display = 'block';
          strip.style.width = Math.max(28, w) + 'px';
          strip.style.height = h + 'px';
        }
      };
      layoutAlertRef.current = layoutAlert;
      requestAnimationFrame(layoutAlert);
    }

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
        // Строки ВСЕХ панелей на этой дате (лукап по time-Map'ам). showTooltip=false
        // (Сила рынка) — курсорный тултип не строим вообще, но кроссхэйр-синк ниже
        // (setCrosshairPosition на соседей) должен остаться активным независимо
        // от тултипа — раньше ранний return по `!any` (всегда true при выключенном
        // тултипе) обрывал функцию ДО синка, и соседняя панель не получала кроссхэйр.
        while (tip.firstChild) tip.removeChild(tip.firstChild);
        let any = false;
        if (showTooltipRef.current) {
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
        }
        // Скрыть тултипы неактивных панелей, кроссхэйр — на соседей.
        tips.forEach((tp, j) => { if (j !== i) tp.style.display = 'none'; });
        if (!any) {
          tip.style.display = 'none';
        } else {
          tip.style.display = 'block';
          const box = boxes[i];
          const w = box.clientWidth, tw = tip.offsetWidth;
          // §R2-25: флип по середине ПОЛЯ (без правой оси). Левая ось скрыта →
          // координаты param.point совпадают с боксом, перевод не нужен.
          const paneW = chart.timeScale().width() || w;
          const rawLeft = param.point.x > paneW / 2 ? param.point.x - tw - 16 : param.point.x + 16;
          tip.style.left = Math.max(6, Math.min(w - tw - 6, rawLeft)) + 'px';
          // Math.min — иначе на низкой панели тултип вылезает за её нижний край.
          tip.style.top = Math.max(6, Math.min(box.clientHeight - tip.offsetHeight - 6, param.point.y - 8)) + 'px';
        }
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

    // ── рисование (модель TradingView), только на panes[drawPaneIndex] ──
    // Порт-копия слоя из LwChart.tsx, привязанная к charts[drawPaneIndex]/
    // apisRef.current[drawPaneIndex] вместо единственного chartRef/seriesApiRef.
    // Координаты движка отсчитываются от левого края ПОЛЯ, а DOM/SVG-слои — от
    // левого края бокса. Пока левая шкала была везде скрыта, разница была нулём;
    // с появлением scale:'left' её надо прибавлять, иначе фигуры, хит-тест и
    // якорь панели свойств уедут на ширину шкалы (в LwChart это paneLeftW).
    let cleanupDraw: (() => void) | null = null;
    const dpi = drawPaneIndexRef.current;
    if (dpi != null && boxes[dpi]) {
      const box = boxes[dpi];
      const chart = () => chartsRef.current[dpi];
      const drawSvg = document.createElementNS(SVGNS, 'svg');
      drawSvg.style.cssText = 'position:absolute;inset:0;z-index:7;overflow:visible;pointer-events:none;touch-action:none';
      box.appendChild(drawSvg);
      const drawHit = document.createElement('div');
      drawHit.style.cssText = 'position:absolute;inset:0;z-index:8;pointer-events:none;touch-action:none';
      box.appendChild(drawHit);

      const isLastPane = dpi === panesRef.current.length - 1;
      const paneLeftW = () => {
        const ch = chart(); if (!ch) return 0;
        try { return ch.priceScale('left').width() || 0; } catch { return 0; }
      };
      const drawSeries = () => apisRef.current[dpi]?.[0] ?? null;
      const primaryDef = () => panesRef.current[dpi]?.series[0];
      const lp2xy = (p: LwDrawPoint): { x: number; y: number } | null => {
        const ch = chart(); if (!ch) return null;
        const xc = ch.timeScale().logicalToCoordinate(p.logical as Logical);
        const s = drawSeries(); const yc = s ? s.priceToCoordinate(p.price) : null;
        if (xc == null || yc == null) return null;
        return { x: paneLeftW() + (xc as number), y: yc as number };
      };
      const xy2lp = (bx: number, by: number): LwDrawPoint | null => {
        const ch = chart(); if (!ch) return null;
        const logical = ch.timeScale().coordinateToLogical((bx - paneLeftW()) as Coordinate);
        const s = drawSeries(); const price = s ? s.coordinateToPrice(by as Coordinate) : null;
        if (logical == null || price == null) return null;
        return { logical: logical as number, price: price as number };
      };
      const plotBox = () => {
        const ch = chart();
        const w = ch ? (ch.timeScale().width() || box.clientWidth) : box.clientWidth;
        // || 26 (а не || 0): на верхних панелях стека оси времени нет, но в первые
        // кадры height() отдаёт 0 и на нижней — тогда поле считалось выше реального,
        // и луч с вертикалью рисовались под осью дат.
        const axisH = ch ? (ch.timeScale().height() || (isLastPane ? 26 : 0)) : 0;
        return { left: paneLeftW(), width: w, height: Math.max(0, box.clientHeight - axisH) };
      };
      const svgEl = (tag: string, attrs: Record<string, string | number>) => {
        const e = document.createElementNS(SVGNS, tag);
        for (const k in attrs) e.setAttribute(k, String(attrs[k]));
        return e;
      };
      const priceY = (price: number): number | null => { const s = drawSeries(); const y = s ? s.priceToCoordinate(price) : null; return y == null ? null : (y as number); };
      const snap = (lp: LwDrawPoint | null, allow = true): LwDrawPoint | null => {
        const mode = drawMagnetRef.current;
        if (!lp || !allow || !mode || mode === 'off') return lp;
        const L = Math.round(lp.logical); const data = primaryDef()?.data;
        if (!data || L < 0 || L >= data.length) return { ...lp, logical: L };
        const pt = data[L];
        const cands = [pt.open, pt.high, pt.low, pt.close, pt.value].filter((v): v is number => v != null);
        if (!cands.length) return { ...lp, logical: L };
        let best = cands[0], bd = Infinity;
        for (const v of cands) { const dd = Math.abs(v - lp.price); if (dd < bd) { bd = dd; best = v; } }
        if (mode === 'weak') {
          const yb = priceY(best), yc = priceY(lp.price);
          if (yb == null || yc == null || Math.abs(yb - yc) > 12) return lp;
        }
        return { logical: L, price: best };
      };
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
      const dashArr = (dash: LwDash | undefined, w: number): string => {
        if (dash === 'dashed') return `${Math.max(w * 3, 4)} ${Math.max(w * 2.5, 3)}`;
        if (dash === 'dotted') return `0.1 ${Math.max(w * 2.4, 4)}`;
        return 'none';
      };
      const renderOne = (d: LwDrawing, sel: boolean, preview = false) => {
        const col = d.color, w = d.width, pb = plotBox();
        const op = String((d.opacity == null ? 1 : d.opacity) * (preview ? 0.7 : 1));
        const da = dashArr(d.dash, w);
        const lc = d.dash === 'dotted' ? 'round' : 'butt';
        const S = { stroke: col, 'stroke-width': w, opacity: op, 'stroke-dasharray': da };
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
      const HANDLE_R = 8;
      const uid = () => 'dr_' + Date.now().toString(36) + '_' + Math.floor(Math.random() * 1e6).toString(36);

      // Габарит фигуры — якорь панели свойств (см. LwChart.shapeRect). Здесь слой
      // живёт в ПЕЙНЕ, а панель рендерится в корневом контейнере embed'а, поэтому
      // добавляем смещение пейна (box.offsetTop/Left) — иначе панель уезжала бы
      // вверх на высоту верхнего пейна.
      const shapeRect = (d: LwDrawing): { x: number; y: number; w: number; h: number } | null => {
        const pb = plotBox(), ox = box.offsetLeft, oy = box.offsetTop;
        const pts = d.pts.map(lp2xy).filter(Boolean) as { x: number; y: number }[];
        if (!pts.length) return null;
        if (d.tool === 'hline') return { x: pb.left + ox, y: pts[0].y + oy, w: pb.width, h: 0 };
        if (d.tool === 'vline') return { x: pts[0].x + ox, y: oy, w: 0, h: pb.height };
        if (d.tool === 'text') {
          const fs = 13 + d.width * 2;
          return { x: pts[0].x + ox, y: pts[0].y - fs + oy, w: Math.max(40, (d.text || 'Текст').length * fs * 0.58), h: fs };
        }
        if (d.tool === 'ray' && pts.length >= 2) pts[1] = rayEnd(pts[0], pts[1], pb);
        const xs = pts.map((p) => p.x), ys = pts.map((p) => p.y);
        const x = Math.min(...xs), y = Math.min(...ys);
        return { x: x + ox, y: y + oy, w: Math.max(...xs) - x, h: Math.max(...ys) - y };
      };
      let lastRectKey = ' ';
      const emitSelRect = (r: { x: number; y: number; w: number; h: number } | null) => {
        const key = r ? `${Math.round(r.x)}|${Math.round(r.y)}|${Math.round(r.w)}|${Math.round(r.h)}` : '';
        if (key === lastRectKey) return;
        lastRectKey = key;
        onSelectionRectRef.current?.(r);
      };

      const drawShapes = () => {
        if (!chart()) return;
        // html2canvas трактует <svg> как replaced-элемент и меряет его через
        // getBoundingClientRect самой svg — без явных width/height это дефолтные
        // 300×150, и экспорт обрезает фигуры (см. LwChart.tsx drawShapes).
        drawSvg.setAttribute('width', String(box.clientWidth));
        drawSvg.setAttribute('height', String(box.clientHeight));
        while (drawSvg.firstChild) drawSvg.removeChild(drawSvg.firstChild);
        if (drawHiddenRef.current) { emitSelRect(null); return; }
        const selId = selectedDrawIdRef.current;
        for (const d of (drawingsRef.current ?? [])) { if (d.hidden) continue; renderOne(d, d.id === selId); }
        if (dragState) renderOne(dragState.d, false, true);
        const live = dragState && dragState.d.id === selId ? dragState.d : (drawingsRef.current ?? []).find((d) => d.id === selId);
        emitSelRect(live && !live.hidden ? shapeRect(live) : null);
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
          if (d.hidden) continue;
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
          if (drawLockedRef.current) { selectedDrawIdRef.current = null; onSelectDrawRef.current?.(null); drawShapes(); return; }
          const hit = hitTest(x, y);
          selectedDrawIdRef.current = hit ? hit.id : null; onSelectDrawRef.current?.(hit ? hit.id : null);
          if (hit && !hit.locked) {   // per-element замок: выделить можно, двигать нельзя
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
        const dash = drawDashRef.current, opacity = drawOpacityRef.current;
        if (tool === 'text') {
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
            const lp = xy2lp(x, y); if (!lp) return;
            const lastXY = lp2xy(dragState.d.pts[dragState.d.pts.length - 1]);
            if (!lastXY || Math.hypot(x - lastXY.x, y - lastXY.y) >= 2.5) dragState.d.pts = [...dragState.d.pts, lp];
          } else {
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
          onToolResetRef.current?.();
        } else {
          commit((drawingsRef.current ?? []).map((x) => (x.id === ds.d.id ? ds.d : x)));
        }
      };
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

      // Перепроецировать фигуры на пан/зум/ресайз этой панели (как экспирации LwChart).
      const onRange = () => drawShapes();
      chart()?.timeScale().subscribeVisibleLogicalRangeChange(onRange);
      const drawRo = new ResizeObserver(() => drawShapes());
      drawRo.observe(box);

      cleanupDraw = () => {
        drawHit.removeEventListener('pointerdown', onDrawDown);
        drawHit.removeEventListener('pointermove', onDrawMove);
        drawHit.removeEventListener('pointerup', onDrawUp);
        drawHit.removeEventListener('dblclick', onDrawDbl);
        try { chart()?.timeScale().unsubscribeVisibleLogicalRangeChange(onRange); } catch { /* chart removed */ }
        drawRo.disconnect();
        drawSvg.parentNode?.removeChild(drawSvg);
        drawHit.parentNode?.removeChild(drawHit);
        drawShapesRef.current = null;
      };
    }

    // Вертикальный масштаб колесом: Shift+колесо где угодно ИЛИ колесо над осью
    // цифр (порт из LwChart). Масштабируем ТОЛЬКО панель под курсором — это и
    // проще, и ожидаемее, чем тянуть общий масштаб на весь стек. Без capture +
    // stopImmediatePropagation библиотека перехватит колесо и начнёт зумить время.
    const wheelOff: (() => void)[] = [];
    boxes.forEach((bx, i) => {
      const margin = { v: 0.12 };
      const onWheel = (e: WheelEvent) => {
        const ch = chartsRef.current[i];
        if (!ch) return;
        const r = bx.getBoundingClientRect();
        const x = e.clientX - r.left;
        const overAxis = x < 60 || x > r.width - 60;
        if (!e.shiftKey && !overAxis) return;
        e.preventDefault();
        e.stopImmediatePropagation();
        margin.v = Math.min(0.42, Math.max(0, margin.v + (e.deltaY > 0 ? 0.028 : -0.028)));
        const sm = { top: margin.v, bottom: margin.v };
        for (const side of ['left', 'right'] as const) {
          try { ch.priceScale(side).applyOptions({ scaleMargins: sm }); } catch { /* шкала скрыта */ }
        }
      };
      bx.addEventListener('wheel', onWheel, { capture: true, passive: false });
      wheelOff.push(() => bx.removeEventListener('wheel', onWheel, true));
    });

    return () => {
      expLayerApi?.destroy();
      drawExpRef.current = null;
      layoutAlertRef.current = null;
      for (const el of [alertStrips.left, alertStrips.right, alertPlus.left, alertPlus.right]) el?.parentNode?.removeChild(el);
      wheelOff.forEach((f) => f());
      unsubs.forEach((u) => { try { u(); } catch { /* noop */ } });
      cleanupDraw?.();
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
              // При активных alertAxes горизонталь кроссхэйра гасим: её роль
              // играет превью-линия уровня, иначе на оси две подписи разом.
              horzLine: {
                color: c.cross, labelBackgroundColor: c.lab,
                visible: !(alertAxes && alertAxes.length),
                labelVisible: !(alertAxes && alertAxes.length),
              },
            },
      });
    });
  }, [dark, chartPrefs, alertAxes]);

  // ── серии всех панелей ──
  useEffect(() => {
    const charts = chartsRef.current;
    if (charts.length !== paneCount || paneCount === 0) return;
    const root = rootRef.current;
    if (!root) return;
    const boxes = paneBoxes(root);
    const savedRange = charts[charts.length - 1].timeScale().getVisibleLogicalRange();

    apisRef.current.forEach((apis, i) => apis.forEach((s) => { try { charts[i]?.removeSeries(s); } catch { /* removed */ } }));
    apisRef.current = panes.map(() => []);
    mapsRef.current = panes.map(() => []);

    // Какие оси заняты хоть где-то в стеке — от этого зависит видимость шкал на
    // ВСЕХ панелях сразу (см. комментарий у applyOptions ниже).
    const sideUsed = {
      left: panes.some((p) => p.series.some((x) => (x.scale ?? 'right') === 'left')),
      right: panes.some((p) => p.series.some((x) => (x.scale ?? 'right') === 'right')),
    };

    // ⚠️ ОБЩАЯ ОСЬ ВРЕМЕНИ ДЛЯ ВСЕГО СТЕКА.
    // Панели синхронизируются ЛОГИЧЕСКИМ диапазоном, а логический индекс — это
    // номер бара внутри данных КОНКРЕТНОЙ панели. У индикаторов прогрев срезан
    // (RSI 14 начинается с 14-го бара), поэтому индекс 0 на нижней панели — уже
    // другая дата, и весь стек уезжает ровно на длину прогрева. Тем же болеют
    // ряды с дырами: «Объёмы» пропускают бары без объёма.
    // Лечится не пересчётом индексов, а тем, что у всех панелей индексное
    // пространство становится ОДНИМ: в каждую добавляется невидимый ряд-хребет
    // из одних времён (whitespace) по объединению всех дат стека.
    // Одиночному графику хребет не нужен — рассинхронизироваться не с чем, а
    // лишняя серия на четырёх остальных embed'ах это лишний риск на ровном месте.
    const spineTimes = panes.length < 2 ? [] : (() => {
      const set = new Set<number>();
      for (const p of panes) for (const s of p.series) for (const pt of s.data) set.add(pt.time);
      return Array.from(set).sort((a, b) => a - b);
    })();
    spinesRef.current.forEach((s, i) => { try { charts[i]?.removeSeries(s); } catch { /* снят вместе с чартом */ } });
    spinesRef.current = [];

    panes.forEach((pane, i) => {
      const chart = charts[i];
      const box = boxes[i];
      if (!chart || !box) return;
      const rc = (col: string | undefined): string => resolveColor(box, col);
      // Хребет ставим ПЕРВЫМ, до реальных серий: он задаёт индексное пространство
      // панели. priceScaleId '' — оверлей без своей шкалы, на оси он не виден и
      // на автомасштаб не влияет; данные — только времена, без значений.
      if (spineTimes.length) {
        try {
          const spine = chart.addSeries(LineSeries, {
            priceScaleId: '', visible: false, lastValueVisible: false,
            priceLineVisible: false, crosshairMarkerVisible: false,
          });
          spine.setData(spineTimes.map((t) => ({ time: t as UTCTimestamp })));
          spinesRef.current[i] = spine;
        } catch (err) { console.error('LwChartPanes spine failed:', err); }
      }
      // ⚠️ Видимость шкал — по ВСЕМУ СТЕКУ, а не по своей панели. Если ось есть
      // только у панели цены (у ОИ так и есть: слева цена, у RSI/ATR слева
      // ничего), её поле оказывается уже соседних ровно на ширину оси, и общая
      // вертикаль кроссхэйра расходится на столько же. Выравнивание minimumWidth
      // ниже эту сторону не спасало: оно ставило ширину СКРЫТОЙ шкале, а скрытая
      // всё равно нулевая. Поэтому пустая шкала остаётся видимой — она просто
      // держит отступ, подписей на ней нет.
      for (const side of ['left', 'right'] as const) {
        try {
          chart.priceScale(side).applyOptions({ visible: sideUsed[side] });
        } catch { /* шкала ещё не готова */ }
      }
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
        const scaleId = def.scale ?? 'right';
        const isOhlc = def.type === 'candlestick' || def.type === 'bar';
        let s: AnySeries;
        if (isOhlc) {
          // up/down = палитра «Покупки/Продажи» (см. LwChart): в editorial это
          // сине-стальной/янтарь, не зелёный/красный. lastValueVisible:false —
          // нативный лейбл свечи красится по up/down и мигает на оси.
          const up = rc('var(--oi-green)'), down = rc('var(--oi-red)');
          s = def.type === 'candlestick'
            ? chart.addSeries(CandlestickSeries, { upColor: up, downColor: down, borderUpColor: up, borderDownColor: down, wickUpColor: up, wickDownColor: down, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: false, priceFormat })
            : chart.addSeries(BarSeries, { upColor: up, downColor: down, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: false, priceFormat });
        } else if (def.type === 'line') {
          s = chart.addSeries(LineSeries, { color: col, lineWidth: lw, lineStyle, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: lastLine, priceFormat });
        } else if (def.type === 'area') {
          s = chart.addSeries(AreaSeries, { lineColor: col, topColor: rc(def.areaTop ?? def.color), bottomColor: def.areaBottom ? rc(def.areaBottom) : 'rgba(0,0,0,0)', lineWidth: lw, lineStyle, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: lastLine, priceFormat });
        } else {
          s = chart.addSeries(HistogramSeries, { color: col, base: def.base ?? 0, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: lastHist, priceFormat });
        }
        try {
          // value ВСЕГДА = close (инвариант LwPoint) — на нём держатся тултип,
          // магнит и сигнатуры; для OHLC-серий подставляем open/high/low отдельно.
          if (isOhlc) {
            (s as ISeriesApi<'Candlestick'>).setData(def.data.map((p) => ({
              time: p.time as UTCTimestamp,
              open: p.open ?? p.value, high: p.high ?? p.value, low: p.low ?? p.value, close: p.close ?? p.value,
            })));
          } else {
            (s as ISeriesApi<'Line'>).setData(def.data.map((p) => ({ time: p.time as UTCTimestamp, value: p.value, ...(p.color ? { color: rc(p.color) } : {}) })));
          }
        } catch (err) {
          console.error('LwChartPanes setData failed:', def.id, err);
        }
        if (def.zeroLine) {
          s.createPriceLine({ price: 0, color: col, lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: false, title: '' });
        }
        // Зоны (RSI 30/70). Примитив живёт на серии и снимается вместе с ней,
        // отдельного цикла жизни не заводим. Цвета собираем из цвета зоны, а не
        // из отдельных токенов: так зона всегда согласована с уровнем.
        if (def.bands) {
          // ⚠️ color-mix резолвим ЧЕРЕЗ ПРОБУ, а не подставляем уже посчитанный
          // rgb внутрь строки: canvas не понимает ни var(), ни color-mix, и
          // fillStyle с таким значением молча не применится.
          const raw = def.bands.color ?? 'var(--text-secondary)';
          try {
            s.attachPrimitive(new BandsPrimitive({
              upper: def.bands.upper,
              lower: def.bands.lower,
              middle: def.bands.middle ?? null,
              lineColor: rc(raw),
              bandColor: def.bands.fill === false ? 'rgba(0,0,0,0)' : rc(`color-mix(in srgb, ${raw} 8%, transparent)`),
              overColor: def.bands.fill === false ? 'rgba(0,0,0,0)' : rc('color-mix(in srgb, var(--oi-green) 12%, transparent)'),
              underColor: def.bands.fill === false ? 'rgba(0,0,0,0)' : rc('color-mix(in srgb, var(--oi-red) 12%, transparent)'),
            }));
          } catch (err) { console.error('LwChartPanes bands failed:', def.id, err); }
        }
        apisRef.current[i].push(s);
        mapsRef.current[i].push(new Map(def.data.map((p) => [p.time, p.value])));
      }
      // Легенда панели. flex align-items:center текст съезжает вниз в PNG-экспорте
      // (html2canvas не воспроизводит flex-центрирование текста, см. LwChart.tsx
      // тот же фикс + #704/#196ce935) — сегмент+подпись инлайн-SVG с
      // dominant-baseline="central" вместо flex div+span.
      const legend = legendsRef.current[i];
      if (legend) {
        while (legend.firstChild) legend.removeChild(legend.firstChild);
        const FS = 11, GAP = 5, SEG_W = 12, SEG_H = 2.5;
        const legTotalH = Math.ceil(FS * 1.35);
        // legendItems — только для панели 0: он описывает смысл СЕРИИ, а не набор
        // панелей (у Фондов одна гистограмма даёт два пункта: приток и отток).
        const legDefs = i === 0 && legendItemsRef.current
          ? legendItemsRef.current.map((x) => ({ label: x.label, color: x.color }))
          : pane.series.map((d) => ({ label: d.label, color: d.color }));
        for (const def of legDefs) {
          const w = SEG_W + GAP + Math.ceil((def.label || '').length * FS * 0.62) + 4;
          const item = document.createElementNS(SVGNS, 'svg');
          item.setAttribute('width', String(w));
          item.setAttribute('height', String(legTotalH));
          item.style.cssText = 'display:block;overflow:visible';
          const seg = document.createElementNS(SVGNS, 'rect');
          seg.setAttribute('x', '0'); seg.setAttribute('y', String((legTotalH - SEG_H) / 2));
          seg.setAttribute('width', String(SEG_W)); seg.setAttribute('height', String(SEG_H));
          seg.setAttribute('rx', '1.25'); seg.setAttribute('fill', rc(def.color));
          const lbl = document.createElementNS(SVGNS, 'text');
          lbl.setAttribute('x', String(SEG_W + GAP)); lbl.setAttribute('y', String(legTotalH / 2));
          lbl.setAttribute('dominant-baseline', 'central');
          lbl.setAttribute('font-size', String(FS)); lbl.setAttribute('font-weight', '600');
          lbl.setAttribute('fill', 'var(--text-primary,#F5F1E8)');
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
      // Обе оси, а не только правая: с левой осью на одной панели и без неё на
      // другой plot-области разъедутся по ширине, и общая вертикаль кроссхэйра
      // поедет по X — ровно тот баг, ради которого это выравнивание и писалось.
      for (const side of ['left', 'right'] as const) {
        let maxW = 0;
        for (const ch of charts) {
          try { maxW = Math.max(maxW, ch.priceScale(side).width()); } catch { /* «Value is null» сразу после смены visible */ }
        }
        if (maxW > 0) {
          for (const ch of charts) {
            try { ch.priceScale(side).applyOptions({ minimumWidth: maxW }); } catch { /* см. выше */ }
          }
        }
        // Ширину ЛЕВОЙ оси публикуем наружу переменной на родителе (а не на своём
        // корне): оверлеи вроде списка индикаторов — СОСЕДИ чарта, внутрь него
        // они не вложены и переменную с корня не унаследовали бы. Иначе такой
        // оверлей может лечь только на глазок и рано или поздно накроет цифры оси.
        if (side === 'left') {
          const host = root.parentElement ?? root;
          host.style.setProperty('--lw-axis-left', `${Math.round(maxW)}px`);
        }
      }
    });

    // Инфо по осям панели цены + превью-линии алерта. Пересоздаются здесь же:
    // createPriceLine привязан к серии, а серии в этом эффекте пересоздаются.
    axisInfoRef.current = {};
    previewLineRef.current = {};
    if (onCreateAlert && panes[0]) {
      const box0 = boxes[0];
      for (const side of ['left', 'right'] as const) {
        const idx = panes[0].series.findIndex((d) => (d.scale ?? 'right') === side);
        const api = idx >= 0 ? apisRef.current[0]?.[idx] : undefined;
        if (!api || !box0) continue;
        const def = panes[0].series[idx];
        const last = def.data.length ? def.data[def.data.length - 1].value : undefined;
        axisInfoRef.current[side] = { api, last, color: resolveColor(box0, def.color) };
        try {
          previewLineRef.current[side] = api.createPriceLine({
            price: last ?? 0, color: resolveColor(box0, 'var(--text-secondary)'),
            lineWidth: 1, lineStyle: LineStyle.Dashed,
            lineVisible: false, axisLabelVisible: false, title: '',
          });
        } catch { /* серия ещё не готова */ }
      }
    }

    // Уровни алертов. Живут ВМЕСТЕ с серией (removeSeries сносит и линии), поэтому
    // создаются здесь же, а не отдельным эффектом. priceLines обязан быть в депсах
    // этого эффекта: он меняется от перезагрузки списка алертов, когда сами серии
    // не менялись, и без него пунктир созданного алерта не появлялся бы до
    // следующей смены данных.
    if (priceLines && priceLines.length) {
      for (const pl of priceLines) {
        const pi = pl.pane ?? 0;
        const paneSeries = panes[pi]?.series ?? [];
        const sc = pl.scale ?? 'left';
        let idx = paneSeries.findIndex((d) => (d.scale ?? 'right') === sc);
        if (idx < 0) idx = 0;
        const api = apisRef.current[pi]?.[idx];
        const bx = boxes[pi];
        if (!api || !bx) continue;
        try {
          api.createPriceLine({
            price: pl.price, color: resolveColor(bx, pl.color ?? 'var(--accent)'),
            lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: true, title: pl.title ?? 'алерт',
          });
        } catch { /* серия уже снята */ }
      }
    }

    // Профиль объёма. Примитив живёт на серии — removeSeries выше уже снёс
    // предыдущий вместе с ней, поэтому старую ссылку просто забываем (detach на
    // снятой серии бросил бы). Функция кладётся в ref: её же зовёт эффект опций,
    // когда серии не менялись.
    vpRef.current = null;
    syncVpRef.current = () => {
      const spec = vpSpecRef.current;
      const prev = vpRef.current;
      const box0 = boxes[0];
      const idx = spec ? panes[0]?.series.findIndex((d) => d.id === spec.seriesId) ?? -1 : -1;
      const api = idx >= 0 ? apisRef.current[0]?.[idx] : undefined;
      // Носитель сменился (пользователь переключил тип цены или скрыл её) — старый
      // примитив снимаем именно с той серии, на которой он висел.
      if (prev && prev.api !== api) {
        try { prev.api.detachPrimitive(prev.prim); } catch { /* серия уже снята */ }
        vpRef.current = null;
      }
      if (!spec || !api || !box0) { vpRef.current = null; return; }
      // Цвета резолвим здесь, а не в примитиве: canvas не понимает var()/color-mix,
      // а probe требует живой узел панели (та же идиома, что у серий).
      const resolved: VolumeProfileSpec = {
        ...spec,
        upColor: resolveColor(box0, spec.upColor),
        downColor: resolveColor(box0, spec.downColor),
        pocColor: resolveColor(box0, spec.pocColor),
      };
      if (vpRef.current) { vpRef.current.prim.applyOptions(resolved); return; }
      const prim = new VolumeProfilePrimitive(resolved);
      try {
        api.attachPrimitive(prim);
        vpRef.current = { prim, api };
      } catch (err) {
        console.error('LwChartPanes attachPrimitive failed:', err);
      }
    };
    syncVpRef.current();

    // Метки экспираций пересчитываем здесь же: expirations в депсах этого
    // эффекта, иначе тумблер «Экспирации» не давал бы эффекта до следующего
    // пана (ровно та дыра, что была бы у priceLines).
    requestAnimationFrame(() => drawExpRef.current?.());

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
    } else if (savedRange ?? lastRangeRef.current) {
      lead.timeScale().setVisibleLogicalRange((savedRange ?? lastRangeRef.current)!);
    }
    drawShapesRef.current?.();
  }, [panes, fitKey, initialBars, paneCount, chartPrefs, priceLines, expirations]);

  // Правки профиля объёма (уровни, сторона, цвет) — БЕЗ пересоздания серий.
  useEffect(() => {
    syncVpRef.current?.();
  }, [volumeProfile]);

  // Смена таймфрейма (интрадей ⇄ дневной) не пересоздаёт чарты, поэтому
  // timeVisible применяем реактивно — иначе ось застревает в режиме маунта.
  useEffect(() => {
    for (const ch of chartsRef.current) {
      try { ch.applyOptions({ timeScale: { timeVisible: !!timeVisible } }); } catch { /* чарт снят */ }
    }
  }, [timeVisible]);

  // ── реагировать на изменение пропов рисования (как в LwChart.tsx) ──
  useEffect(() => {
    drawShapesRef.current?.();
  }, [drawActive, drawTool, drawings, selectedDrawId, drawColor, drawWidth, drawMagnet, drawHidden, drawLocked, drawDash, drawOpacity]);

  // Отступ водяного знака слева = ширина ЛЕВОЙ шкалы первой панели: иначе знак
  // ложится на подписи оси (та же логика, что в LwChart).
  const hasLeftAxis = panes[0]?.series.some((x) => x.scale === 'left');
  return (
    // position:relative — якорь для водяного знака. ⚠️ Пейны ищутся по
    // data-lw-pane, а НЕ по root.children: раньше любой лишний ребёнок корня
    // сдвигал бы индексы боксов, и чарт создался бы поверх водяного знака.
    <div ref={rootRef} style={{ position: 'relative', display: 'flex', flexDirection: 'column', width: '100%', height: '100%' }}>
      {panes.map((p, i) => (
        <div key={i} data-lw-pane="" style={{ position: 'relative', minHeight: 0, flex: `${p.flex ?? 1} 1 0%` }}>
          {/* Оверлей ВНУТРИ панели: строка индикатора должна жить над своим
              графиком, а не в общем углу сверху. Позиционируется по
              --lw-axis-left, как и список панели 0. */}
          {paneOverlay?.(i)}
        </div>
      ))}
      {watermark !== false && chartPrefs?.watermark !== false && (
        <ChartWatermark bottom={30} left={hasLeftAxis ? 62 : 12} size={26} minSize={16} opacity={0.4} />
      )}
    </div>
  );
});

export default LwChartPanes;
