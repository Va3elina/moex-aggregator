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
  /** Клик по «+» у оси → создать алерт на уровне под курсором. `pane` — в какой
   *  панели нажали: ряд, по которому ставят алерт, может уехать из панели цены
   *  в свою (открытый интерес), и одной стороны оси для опознания уже мало. */
  onCreateAlert?: (p: { axis: 'left' | 'right'; pane: number; price: number; currentValue: number }) => void;
  /** На каких осях каких панелей показывать «+». Пустой массив/undefined — нигде. */
  alertAxes?: { pane: number; side: 'left' | 'right' }[];
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
  /** Пользовательские доли высоты панелей (перетаскивание разделителя).
   *  Приоритетнее panes[].flex; длина не совпала с числом панелей — игнор. */
  paneSizes?: number[];
  /** Разделитель отпустили → нормированные доли (сумма = числу панелей).
   *  Зовётся ОДИН раз на жест: во время самого перетаскивания высоты меняются
   *  напрямую в DOM, без React — см. правило №2 кроссхэйра, тот же принцип. */
  onPaneSizesChange?: (sizes: number[]) => void;
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

/** Как resolveColor, но null при неудачной пробе — вызывающий тогда НЕ применяет
 *  цвет, а не красит неизвестно чем. */
function probeColor(box: HTMLElement, color: string | undefined): string | null {
  const v = resolveColor(box, color);
  return v === '#888888' ? null : v;
}

/** Поколение цветов: растёт при смене темы и сбрасывает кэш ниже — иначе
 *  перекраска брала бы из кэша значения ПРОШЛОЙ темы. */
let colorGen = 0;
const colorCache = new WeakMap<HTMLElement, { gen: number; m: Map<string, string> }>();

/**
 * Canvas не понимает var()/color-mix — резолвим пробой в контексте панели.
 *
 * ⚠️ КЭШИРУЕТСЯ, и это не оптимизация «на всякий случай». Каждый промах вставляет
 * узел в DOM и зовёт getComputedStyle, то есть форсит пересчёт стиля. Цвет точки
 * резолвится ПО ТОЧКЕ (у объёмов и потоков он свой на каждом баре), так что на
 * ряду в тысячи баров выходили тысячи пересчётов подряд и вкладка вставала на
 * секунды. Различных цветов при этом два-три — рост и падение.
 *
 * ⚠️ Никогда не возвращаем сырую строку var(...): canvas молча игнорирует
 * невалидный цвет, и серия просто перестаёт рисоваться — тихий отказ без ошибок.
 */
function resolveColor(box: HTMLElement, color: string | undefined): string {
  if (!color) return '#888888';
  if (!color.includes('var(') && !color.includes('color-mix')) return color;
  let e = colorCache.get(box);
  if (!e || e.gen !== colorGen) { e = { gen: colorGen, m: new Map() }; colorCache.set(box, e); }
  const hit = e.m.get(color);
  if (hit !== undefined) return hit;
  let v = '#888888';
  try {
    const probe = document.createElement('span');
    probe.style.color = color;
    probe.style.position = 'absolute';
    probe.style.pointerEvents = 'none';
    box.appendChild(probe);
    const rgb = getComputedStyle(probe).color;
    box.removeChild(probe);
    if (rgb && !rgb.includes('var(') && !rgb.includes('color-mix')) v = rgb;
  } catch { /* узел не в документе — остаётся серый по умолчанию */ }
  e.m.set(color, v);
  return v;
}

/**
 * Цветовые опции серии — ОДИН источник и для создания, и для перекраски.
 *
 * ⚠️ Свечи и бары красятся НЕ из def.color: у них жёсткая пара «покупки/продажи»
 * (в editorial это сине-стальной и янтарь), а def.color у цены — токен линии или
 * пользовательский цвет из ⚙-Формата, и он игнорируется сознательно. Если вывести
 * up/down из def.color, создание и перекраска разойдутся: свечи создадутся
 * правильными, а после первой же смены темы молча перекрасятся в цвет линии.
 */
function seriesColorOpts(box: HTMLElement, def: LwSeries): Record<string, string> {
  const rc = (c: string | undefined) => resolveColor(box, c);
  if (def.type === 'candlestick' || def.type === 'bar') {
    const up = rc('var(--oi-green)'), down = rc('var(--oi-red)');
    return def.type === 'candlestick'
      ? { upColor: up, downColor: down, borderUpColor: up, borderDownColor: down, wickUpColor: up, wickDownColor: down }
      : { upColor: up, downColor: down };
  }
  const col = rc(def.color);
  if (def.type === 'area') {
    return {
      lineColor: col,
      topColor: rc(def.areaTop ?? def.color),
      bottomColor: def.areaBottom ? rc(def.areaBottom) : 'rgba(0,0,0,0)',
    };
  }
  return { color: col };
}

type AnySeries = ISeriesApi<'Line' | 'Area' | 'Histogram' | 'Candlestick' | 'Bar'>;
/** Курсорный пилс одной оси. `pending` — что создаст клик по «плюсу»: сам кружок
 *  мышь не ловит (pointer-events:none), клик снимается хит-тестом с бокса. */
type PillEls = {
  box: HTMLDivElement; val: HTMLSpanElement;
  pending?: { axis: 'left' | 'right'; pane: number; price: number; currentValue: number };
};

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
  paneOverlay, paneSizes, onPaneSizesChange,
  selectedDrawId, onSelectDraw, onSelectionRect, drawMagnet, drawHidden, drawLocked, drawDash, drawOpacity, onToolReset,
}: LwChartPanesProps, forwardedRef) {
  const rootRef = useRef<HTMLDivElement>(null);
  const chartsRef = useRef<IChartApi[]>([]);
  const apisRef = useRef<AnySeries[][]>([]);          // [pane][series]
  // ⚠️ Определения серий храним ПАРАЛЛЕЛЬНО apisRef, а не читаем из panesRef.
  // panesRef присваивается во время рендера, apisRef — в эффекте: это разные
  // фазы, и отложенная на кадр перекраска может застать их рассогласованными
  // при совпавших длинах (сменился, скажем, показатель ОИ). Здесь цветовой
  // источник физически принадлежит той серии, которая существует.
  const seriesDefsRef = useRef<LwSeries[][]>([]);
  // Примитивы зон и созданные price line'ы: applyOptions'ом их не достать иначе —
  // ссылки на них сейчас выбрасывались сразу после создания.
  const bandsRef = useRef<(BandsPrimitive | null)[][]>([]);
  const lineRegRef = useRef<{ line: IPriceLine; token: string; pane: number }[]>([]);
  // Невидимые ряды-хребты, по одному на панель: держат общее индексное
  // пространство времени (см. spineTimes в эффекте серий). Хранятся отдельно от
  // apisRef, чтобы не сбить парность apisRef[i][k] ↔ mapsRef[i][k] в тултипе.
  const spinesRef = useRef<AnySeries[]>([]);
  const mapsRef = useRef<Map<number, number>[][]>([]); // [pane][series] time→value
  const panesRef = useRef<LwPane[]>(panes); panesRef.current = panes;
  const tipsRef = useRef<HTMLDivElement[]>([]);
  // Свои пилсы значения под курсором: [панель][сторона]. Библиотечную подпись
  // кроссхэйра не используем — она одна на весь чарт, а на панели цены две оси
  // с разными цветами, и «плюс» в неё не вставить в принципе (как на сайте).
  const pillsRef = useRef<{ [k in 'left' | 'right']?: PillEls }[]>([]);
  const pillColorRef = useRef<{ [k in 'left' | 'right']?: string }[]>([]);
  const legendsRef = useRef<HTMLDivElement[]>([]);
  const lastFitRef = useRef<string | undefined>(undefined);
  const tickFmtRef = useRef(tickFmt); tickFmtRef.current = tickFmt;
  const showTooltipRef = useRef(showTooltip); showTooltipRef.current = showTooltip;
  const paneCount = panes.length;
  const chartPrefs = useContext(ChartPrefsCtx);
  // Зеркало для обработчиков, созданных в эффекте [paneCount]: из их замыкания
  // chartPrefs остался бы навсегда тем, каким был при создании панелей.
  const chartPrefsRef = useRef(chartPrefs); chartPrefsRef.current = chartPrefs;

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
  const darkRef = useRef(dark); darkRef.current = dark;
  // Профиль объёма живёт ВНЕ эффекта серий: держать его в его депсах значило бы
  // пересоздавать все серии на каждую правку числа уровней (сотня миллисекунд
  // ради перерисовки одного слоя). Эффект серий только переприкрепляет примитив
  // после пересоздания серии-носителя, а сами опции доезжают отдельным эффектом.
  const vpSpecRef = useRef(volumeProfile); vpSpecRef.current = volumeProfile;
  const vpRef = useRef<{ prim: VolumeProfilePrimitive; api: AnySeries } | null>(null);
  const syncVpRef = useRef<(() => void) | null>(null);
  const onCreateAlertRef = useRef(onCreateAlert); onCreateAlertRef.current = onCreateAlert;
  const onPaneSizesChangeRef = useRef(onPaneSizesChange); onPaneSizesChangeRef.current = onPaneSizesChange;
  const alertAxesRef = useRef(alertAxes); alertAxesRef.current = alertAxes;
  // Превью-уровень = НАТИВНАЯ price line. В отличие от LwChart здесь у неё включён
  // axisLabelVisible: значение рисует сама библиотека. Самодельный DOM-пилс
  // (попиксельная реплика лейбла оси с калибровкой кегля после fonts.ready) не
  // портируется намеренно — в стеке панелей он умножался бы на их число.
  // Последний видимый диапазон. Эффект создания чартов зависит от paneCount, то
  // есть «вынести индикатор в свою панель» ПЕРЕСОЗДАЁТ все инстансы — и зум
  // слетел бы на fit ровно в момент, когда пользователь этого не просил.
  // Читаем его при восстановлении, если у свежесозданного чарта своего ещё нет.
  const lastRangeRef = useRef<LogicalRange | null>(null);
  /**
   * Обновление чисел в строках индикаторов. time — точка под курсором, null —
   * вернуть последний бар. Ищем узлы по data-ind-value и пишем textContent:
   * React их не перерисовывает (проп values статичен), так что перетирания нет.
   */
  const paintRowValuesRef = useRef<((time: number | null) => void) | null>(null);
  paintRowValuesRef.current = (time) => {
    const root = rootRef.current;
    const host = root?.parentElement ?? root;
    if (!host) return;
    for (const pane of panesRef.current) {
      for (const def of pane.series) {
        const el = host.querySelector(`[data-ind-value="${def.id}"]`) as HTMLElement | null;
        if (!el) continue;
        const n = def.data.length;
        if (!n) continue;
        let idx = n - 1;
        if (time != null) {
          const found = def.data.findIndex((pt) => pt.time === time);
          if (found >= 0) idx = found;
        }
        const pt = def.data[idx];
        if (!pt) continue;
        const fmt = (v: number) => (def.axisFmt ? def.axisFmt(v) : String(Math.round(v)));
        const prevPt = idx > 0 ? def.data[idx - 1] : undefined;
        const prevClose = prevPt?.close ?? prevPt?.value;
        const isOhlc = pt.open != null && pt.high != null && pt.low != null;
        // Направление: у свечи — против ПРЕДЫДУЩЕГО закрытия (так в терминалах),
        // у линии — против предыдущего значения. Цвет точки (объёмы) главнее.
        const up = prevClose == null ? undefined : (pt.close ?? pt.value) >= prevClose;
        const c = pt.color ?? (up == null ? undefined : up ? 'var(--oi-green)' : 'var(--oi-red)');

        while (el.firstChild) el.removeChild(el.firstChild);
        const num = (v: number) => {
          const sp = document.createElement('span');
          sp.textContent = fmt(v);
          if (c) sp.style.color = c;
          return sp;
        };
        const cap = (t: string) => {
          const sp = document.createElement('span');
          sp.textContent = t;
          sp.style.cssText = 'color:var(--text-secondary);font-weight:600;margin-left:6px';
          return sp;
        };
        if (isOhlc) {
          // Легенда свечи как в терминалах: ОТКР/МАКС/МИН/ЗАКР + изменение.
          el.appendChild(cap('ОТКР')); el.appendChild(num(pt.open as number));
          el.appendChild(cap('МАКС')); el.appendChild(num(pt.high as number));
          el.appendChild(cap('МИН')); el.appendChild(num(pt.low as number));
          el.appendChild(cap('ЗАКР')); el.appendChild(num(pt.close ?? pt.value));
        } else {
          el.appendChild(num(pt.value));
        }
        if (prevClose != null && def.legendChange) {
          const cur = pt.close ?? pt.value;
          const d = cur - prevClose;
          const pct = prevClose === 0 ? 0 : (d / Math.abs(prevClose)) * 100;
          const sp = document.createElement('span');
          sp.style.cssText = 'margin-left:6px';
          if (c) sp.style.color = c;
          const sign = d >= 0 ? '+' : '−';
          sp.textContent = `${sign}${fmt(Math.abs(d))} (${sign}${Math.abs(pct).toFixed(2)}%)`;
          el.appendChild(sp);
        }
      }
    }
  };
  const drawExpRef = useRef<(() => void) | null>(null);
  const drawShapesRef = useRef<(() => void) | null>(null);
  /** Клик по слою рисования ВНЕ режима: текст-зона выделенной линии → правка
   *  текста; фигура → выделение; пусто → снять выделение. true = клик съеден. */
  const drawClickRef = useRef<((x: number, y: number) => boolean) | null>(null);
  const drawPaneIndexRef = useRef(drawPaneIndex); drawPaneIndexRef.current = drawPaneIndex;

  // Ширины осей, действовавшие до экспорта — вернуть в restoreAfterCapture.
  const axisWBeforeRef = useRef<{ left?: number; right?: number }>({});
  useImperativeHandle(forwardedRef, () => ({
    syncBeforeCapture: () => {
      const root = rootRef.current;
      const pi = drawPaneIndexRef.current;
      if (pi != null) {
        const box = root ? paneBoxes(root)[pi] : null;
        const chart = chartsRef.current[pi];
        // ⚠️ Только панель рисования. Прогон resize по ВСЕМ панелям конфликтует
        // с autoSize (движок ведёт размер своим ResizeObserver): панели уезжали
        // и по ширине, и по высоте — нижние наезжали на ценовую.
        // panes делят высоту по flex, и свой РЕАЛЬНЫЙ clientWidth/Height к этому
        // моменту бокс уже знает (входные w/h — размер всего стека).
        if (box && chart) chart.resize(box.clientWidth, box.clientHeight);
      }
      // Шрифт масштабируем по ширине ВСЕГО стека панелей (общая для всех) —
      // см. LwChartHandle.syncBeforeCapture в LwChart.tsx.
      const stackW = root?.clientWidth;
      if (stackW) {
        const fs = Math.round(BASE_FONT_SIZE * captureFontScale(stackW));
        chartsRef.current.forEach((chart) => chart.applyOptions({ layout: { fontSize: fs } }));
        // ⚠️ И СРАЗУ выровнять ширину ценовых осей под новый кегль. Крупный
        // шрифт делает подписи шире, а насколько — зависит от самих чисел:
        // у цены «52 000», у RSI «80.00». Ширины расходятся, вместе с ними
        // разъезжаются поля панелей — на снимке индикаторы выходили шире
        // графика и не совпадали с ним по вертикальным осям. minimumWidth,
        // выставленный при заливке серий, тут не спасает: он МИНИМУМ, а
        // фактические ширины стали разными. Оценка ширины через коэффициент
        // кегля: точность не важна, важно ОДНО значение на все панели.
        const scale = fs / BASE_FONT_SIZE;
        for (const side of ['left', 'right'] as const) {
          let maxW = 0;
          for (const ch of chartsRef.current) {
            try { maxW = Math.max(maxW, ch.priceScale(side).width()); } catch { /* §R2-30 */ }
          }
          if (maxW <= 0) continue;
          axisWBeforeRef.current[side] = maxW;
          const w = Math.ceil(maxW * scale) + 2;
          for (const ch of chartsRef.current) {
            try { ch.priceScale(side).applyOptions({ minimumWidth: w }); } catch { /* §R2-30 */ }
          }
        }
      }
      drawShapesRef.current?.();
    },
    restoreAfterCapture: () => {
      chartsRef.current.forEach((chart) => chart.applyOptions({ layout: { fontSize: BASE_FONT_SIZE } }));
      for (const side of ['left', 'right'] as const) {
        const w = axisWBeforeRef.current[side];
        if (!w) continue;
        for (const ch of chartsRef.current) {
          try { ch.priceScale(side).applyOptions({ minimumWidth: w }); } catch { /* §R2-30 */ }
        }
      }
      axisWBeforeRef.current = {};
      drawShapesRef.current?.();
    },
  }), []);

  // Пользовательские доли высоты применяются ПОСЛЕ КАЖДОГО рендера (без депсов):
  // React пишет в style панелей flex из panes[].flex, и любой ререндер затирал
  // бы перетащенное. Запись того же значения relayout не вызывает — дёшево.
  useEffect(() => {
    const root = rootRef.current;
    if (!root || !paneSizes) return;
    const bxs = paneBoxes(root);
    if (paneSizes.length !== bxs.length) return;
    bxs.forEach((b, i) => { if (b) b.style.flex = `${paneSizes[i]} 1 0%`; });
  });

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
    let buttonsDown = false;  // зажата кнопка → идёт пан, свои кресты не трогаем
    const onBtnDown = () => { buttonsDown = true; };
    const onBtnUp = () => {
      buttonsDown = false;
      // Жест кончился → перерисовать фигуры и дослать замороженный selRect.
      requestAnimationFrame(() => drawShapesRef.current?.());
    };
    window.addEventListener('pointerdown', onBtnDown, true);
    window.addEventListener('pointerup', onBtnUp, true);
    window.addEventListener('pointercancel', onBtnUp, true);
    unsubs.push(() => {
      window.removeEventListener('pointerdown', onBtnDown, true);
      window.removeEventListener('pointerup', onBtnUp, true);
      window.removeEventListener('pointercancel', onBtnUp, true);
    });

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
        // price: true — тянешь ЦЕНОВУЮ ОСЬ и масштаб фиксируется (движок сам
        // снимает autoScale). Без этого шкала была ВСЕГДА автоматической и
        // пересчитывалась под каждое новое окно при пане: у цены и ОИ пределы
        // разные, они «дышали» вразнобой — Вадим видел это как «график странно
        // меняет масштаб при перемещении по времени». Вернуть авто — двойной
        // клик по оси (ниже). time остаётся false: горизонтальный масштаб
        // общий для стека и синхронизируется отдельно.
        handleScale: { mouseWheel: true, pinch: true, axisPressedMouseMove: { time: false, price: true } },
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
        const redrawExp = () => { drawExpRef.current?.(); };
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
    // ── свои пилсы значения под курсором ──
    // Яркий пилс последнего значения рисует движок сам; этот — светлее, он
    // показывает значение в точке, куда наведён курсор. Так на сайте.
    const pills: { [k in 'left' | 'right']?: PillEls }[] = [];
    boxes.forEach((box, i) => {
      const per: { [k in 'left' | 'right']?: PillEls } = {};
      for (const side of ['left', 'right'] as const) {
        const el = document.createElement('div');
        el.style.cssText = 'position:absolute;display:none;align-items:center;z-index:6;'
          + 'transform:translateY(-50%);pointer-events:none;font-size:11px;font-weight:600;color:#fff;'
          + 'white-space:nowrap;' + side + ':0';
        const val = document.createElement('span');
        // ⚠️ Отступ со стороны ПОЛЯ — 10px, с внешней 6px. Движок отбивает
        // цифру оси от кромки поля на ~9.6px (тик 5 + внутренний отступ 4.6,
        // геометрия из исходников v4 PriceAxisViewRenderer, см. §R2-24).
        // При симметричных 6px цифра пилса вставала на 3-4px не в ту колонку,
        // что цифры оси, и это видно глазом.
        val.style.cssText = 'border-radius:4px;font-variant-numeric:tabular-nums;padding:1px '
          + (side === 'left' ? '10px 1px 6px' : '6px 1px 10px');
        el.appendChild(val);
        // «Плюс» создаётся на КАЖДОЙ панели — какие оси реально предлагают
        // алерт, решает alertAxes (ОИ в своей панели — тоже алертная ось).
        const plus = document.createElement('div');
        // ⚠️ Кружок ВНЕ ПОТОКА (absolute). Пока он был флекс-соседом, его
        // ширина входила в габарит группы, и выравнивание по краю шкалы
        // сдвигало САМО ЧИСЛО. Сторона внутренняя, к полю графика.
        // ⚠️ pointer-events:none — ОБЯЗАТЕЛЬНО. Кружок висит ровно на уровне
        // курсора, и стоило ему ловить мышь, как подвод курсора к нему уводил
        // мышь с канваса: движок гасил крест, пилс исчезал, курсор снова
        // оказывался над канвасом, пилс возвращался — мерцание с частотой
        // кадров, «ступени» вертикали и мёртвый пан, если нажать на нём кнопку.
        // На узком окне пилс занимает бОльшую долю поля, поэтому там это
        // ловилось постоянно. Клик — хит-тестом с бокса панели (onPillClick).
        plus.style.cssText = 'position:absolute;width:14px;height:14px;border-radius:50%;display:none;'
          + 'align-items:center;justify-content:center;font-size:12px;line-height:1;'
          + 'pointer-events:none;top:50%;transform:translateY(-50%);'
          + (side === 'left' ? 'left:calc(100% + 4px)' : 'right:calc(100% + 4px)');
        plus.textContent = '+';
        plus.dataset.plus = side;
        el.appendChild(plus);
        box.appendChild(el);
        per[side] = { box: el, val };
      }
      pills[i] = per;
    });
    pillsRef.current = pills;

    // Клик по «плюсу»: кружок мышь не ловит (см. выше), клик проходит в канвас
    // и всплывает до бокса — здесь проверяем попадание в кружок. Цель обязана
    // быть КАНВАСОМ: в режиме рисования клики забирает хит-слой, и алерт не
    // должен создаваться посреди рисования.
    boxes.forEach((box, i) => {
      if (!box) return;
      const onPillClick = (e: MouseEvent) => {
        if ((e.target as HTMLElement | null)?.tagName !== 'CANVAS') return;
        // Вне режима рисования фигуры выделяются обычным кликом (как в
        // TradingView): хит-тест по слою рисования этой панели. В режиме
        // рисования сюда не попадаем — клики забирает его хит-слой (не CANVAS).
        if (i === drawPaneIndexRef.current && drawClickRef.current) {
          const br = box.getBoundingClientRect();
          if (drawClickRef.current(e.clientX - br.left, e.clientY - br.top)) { e.stopPropagation(); return; }
        }
        const per = pillsRef.current[i];
        if (!per) return;
        for (const sd of ['left', 'right'] as const) {
          const q = per[sd];
          if (!q || !q.pending || q.box.style.display === 'none') continue;
          const plus = q.box.querySelector('[data-plus]') as HTMLElement | null;
          if (!plus || plus.style.display === 'none') continue;
          const pr = plus.getBoundingClientRect();
          // Запас 4px вокруг: в кружок 14px иначе непросто попасть.
          if (e.clientX >= pr.left - 4 && e.clientX <= pr.right + 4
            && e.clientY >= pr.top - 4 && e.clientY <= pr.bottom + 4) {
            e.stopPropagation();
            onCreateAlertRef.current?.(q.pending);
            return;
          }
        }
      };
      box.addEventListener('click', onPillClick);
      unsubs.push(() => box.removeEventListener('click', onPillClick));
    });

const showPill = (pi: number, sd: 'left' | 'right', price: number | null) => {
      const q = pillsRef.current[pi]?.[sd];
      if (!q) return;
      const ch = chartsRef.current[pi];
      const defs = panesRef.current[pi]?.series ?? [];
      const idx = defs.findIndex((d) => (d.scale ?? 'right') === sd);
      const api = idx >= 0 ? apisRef.current[pi]?.[idx] : undefined;
      if (price == null || !api || !ch) { q.box.style.display = 'none'; q.pending = undefined; return; }
      const y = api.priceToCoordinate(price);
      if (y == null) { q.box.style.display = 'none'; q.pending = undefined; return; }
      const def = defs[idx];
      q.val.textContent = def.axisFmt ? def.axisFmt(price) : String(Math.round(price));
      // ⚠️ Тот же цвет, что у пилса ДАТЫ на горизонтальной оси
      // (themeColors().lab — движок красит им подпись вертикальной линии
      // кроссхэйра). Оба пилса показывают одно и то же: где сейчас курсор,
      // — и разный цвет у них читался бы как разный смысл.
      const nd = darkRef.current !== false;
      const lab = themeColors(nd).lab;
      q.val.style.background = lab;
      q.val.style.color = nd ? '#E7E2D6' : '#26262B';
      // ⚠️ Прижимаем к кромке ПОЛЯ, а не к краю панели: пилс последнего
      // значения движок рисует именно там, и без этого свой пилс вставал в
      // другую колонку и наезжал на соседний.
      let axisW = 0;
      try { axisW = ch.priceScale(sd).width() || 0; } catch { /* §R2-30 */ }
      const bw = (pillsRef.current[pi]?.[sd]?.box.parentElement as HTMLElement | null)?.clientWidth ?? 0;
      const inset = Math.max(0, bw - axisW);
      if (sd === 'left') { q.box.style.right = inset + 'px'; q.box.style.left = 'auto'; }
      else { q.box.style.left = inset + 'px'; q.box.style.right = 'auto'; }
      q.box.style.top = y + 'px';
      q.box.style.display = 'flex';
      const plus = q.box.querySelector('[data-plus]') as HTMLDivElement | null;
      const on = !!onCreateAlertRef.current && !!alertAxesRef.current?.some((a) => a.pane === pi && a.side === sd);
      q.pending = on ? { axis: sd, pane: pi, price, currentValue: price } : undefined;
      if (plus) {
        plus.style.display = on ? 'flex' : 'none';
        // Тот же цвет, что у пилса: кружок — его продолжение, а не отдельный
        // элемент со своим смыслом.
        plus.style.background = lab;
        plus.style.color = nd ? '#E7E2D6' : '#26262B';
      }
    };

    // ── разделители панелей: перетаскивание высоты (как в TradingView) ──
    // Высоты меняются НАПРЯМУЮ в DOM (style.flex): ререндер во время жеста
    // упирается в правило №2 кроссхэйра (эффект серий восстанавливает диапазон,
    // вид дёргается). autoSize чартов сам подхватывает новую высоту. Наружу
    // (персист) уходит ОДИН вызов на отпускании.
    boxes.forEach((bx, i) => {
      if (!bx || i === 0) return;
      const grip = document.createElement('div');
      grip.dataset.exportIgnore = 'true';
      grip.style.cssText = 'position:absolute;left:0;right:0;top:-3px;height:7px;z-index:9;cursor:row-resize;background:transparent';
      const bar = document.createElement('div');
      bar.style.cssText = 'position:absolute;left:0;right:0;top:3px;height:1px;background:transparent;transition:background .12s;pointer-events:none';
      grip.appendChild(bar);
      grip.addEventListener('mouseenter', () => { bar.style.background = 'var(--accent,#FF5C2B)'; });
      grip.addEventListener('mouseleave', () => { bar.style.background = 'transparent'; });
      grip.addEventListener('pointerdown', (e) => {
        e.preventDefault(); e.stopPropagation();
        const above = boxes[i - 1];
        if (!above) return;
        const h0a = above.getBoundingClientRect().height;
        const h0b = bx.getBoundingClientRect().height;
        const y0 = e.clientY;
        // Все панели переводим на пиксельные доли — иначе изменение двух flex
        // при третьей панели перераспределяло бы и её высоту.
        const all = boxes.map((b) => (b ? b.getBoundingClientRect().height : 0));
        boxes.forEach((b, j) => { if (b) b.style.flex = `${all[j]} 1 0%`; });
        const move = (ev: PointerEvent) => {
          const dy = ev.clientY - y0;
          // Минимумы: цене — чтобы остался читаемый график, индикатору — строка.
          let na = h0a + dy, nb = h0b - dy;
          if (na < 64) { na = 64; nb = h0a + h0b - 64; }
          if (nb < 44) { nb = 44; na = h0a + h0b - 44; }
          above.style.flex = `${na} 1 0%`;
          bx.style.flex = `${nb} 1 0%`;
        };
        const up = () => {
          window.removeEventListener('pointermove', move);
          window.removeEventListener('pointerup', up);
          const hs = boxes.map((b) => (b ? b.getBoundingClientRect().height : 1));
          const sum = hs.reduce((acc, v) => acc + v, 0) || 1;
          onPaneSizesChangeRef.current?.(hs.map((h) => (h / sum) * hs.length));
        };
        window.addEventListener('pointermove', move);
        window.addEventListener('pointerup', up);
      });
      bx.appendChild(grip);
      unsubs.push(() => grip.remove());
    });

    // ── свой крест на время наведения на ЦЕНОВЫЕ ОСИ ──
    //
    // Движок рисует крест только внутри ПОЛЯ: как только курсор уходит на шкалу,
    // панель получает mouseleave и крест гаснет весь, вместе с пилсами. А шкала —
    // ровно то место, куда ведёшь курсор, чтобы прочитать уровень. Поэтому на
    // время наведения на шкалу рисуем крест сами: вертикаль по последнему бару
    // (она не должна прыгать от вертикального движения), горизонталь по курсору.
    const axV: HTMLDivElement[] = [];
    const axH: HTMLDivElement[] = [];
    boxes.forEach((box, i) => {
      if (!box) return;
      const mk = (vert: boolean) => {
        const el = document.createElement('div');
        el.dataset.exportIgnore = 'true';
        el.style.cssText = 'position:absolute;pointer-events:none;z-index:5;display:none;'
          + (vert ? 'top:0;bottom:0;border-left:1px dotted' : 'left:0;right:0;border-top:1px dotted');
        box.appendChild(el);
        return el;
      };
      axV[i] = mk(true);
      axH[i] = mk(false);
    });
    const hideAxisCross = () => {
      for (const el of [...axV, ...axH]) if (el) el.style.display = 'none';
    };
    // X последнего БАРА под курсором (координата бара, не мыши): по нему рисуем
    // вертикаль своего креста при уходе на шкалу. Именно бара — движок снапит
    // свои вертикали к барам, и с сырым x линии панелей расходились на полшага
    // («лесенка» из несведённых пунктиров).
    let lastFieldX: number | null = null;
    let overAxis = false;

    const axisSides = (i: number): { lw: number; rw: number } => {
      const ch = chartsRef.current[i];
      let lw = 0, rw = 0;
      try { lw = ch?.priceScale('left').width() || 0; } catch { /* §R2-30 */ }
      try { rw = ch?.priceScale('right').width() || 0; } catch { /* §R2-30 */ }
      return { lw, rw };
    };

    // Полное гашение курсорного UI. Нужно отдельной функцией, потому что гасить
    // приходится из ДВУХ мест: по событию движка (курсор ушёл с поля вбок) и по
    // своему mousemove (курсор ушёл за пределы графика вовсе). Во втором случае
    // движок молчит — он погасил свой крест раньше, когда мы его придержали, и
    // больше событий не шлёт. Без этого пилсы и вертикаль оставались висеть.
    const hideCursorUi = () => {
      paintRowValuesRef.current?.(null);
      tips.forEach((tp) => { tp.style.display = 'none'; });
      pillsRef.current.forEach((per) => {
        for (const sd of ['left', 'right'] as const) { const q = per?.[sd]; if (q) q.box.style.display = 'none'; }
      });
      hideAxisCross();
    };

    const onRootMove = (e: MouseEvent) => {
      // Во время перетаскивания свой крест не рисуем и ничего не меряем:
      // движок ведёт кроссхэйр сам (пока кнопка зажата, он слушает document),
      // а наши замеры с getBoundingClientRect на каждый кадр пана — лишний
      // принудительный layout ровно тогда, когда дорог каждый кадр.
      if (e.buttons !== 0) {
        if (overAxis) { overAxis = false; hideAxisCross(); }
        return;
      }
      let hit = -1;
      let rect: DOMRect | null = null;
      boxes.forEach((b, i) => {
        if (!b) return;
        const r = b.getBoundingClientRect();
        if (e.clientY >= r.top && e.clientY < r.bottom && e.clientX >= r.left && e.clientX < r.right) { hit = i; rect = r; }
      });
      if (hit < 0 || !rect) {
        if (overAxis) { overAxis = false; hideCursorUi(); }
        return;
      }
      const r = rect as DOMRect;
      const x = e.clientX - r.left, y = e.clientY - r.top;
      // Ниже кромки ПОЛЯ (полоса оси дат нижней панели и её углы под ценовыми
      // шкалами) значений нет: coordinateToPrice охотно экстраполирует за поле,
      // и в этой полосе пилсы показывали числа, к которым курсор не имеет
      // отношения — «под индикатором видно значение цены».
      const axisHBot = hit === boxes.length - 1 ? (chartsRef.current[hit]?.timeScale().height() || 0) : 0;
      if (axisHBot && y > r.height - axisHBot) {
        if (overAxis) { overAxis = false; hideCursorUi(); }
        return;
      }
      const { lw, rw } = axisSides(hit);
      const on = x < lw || x > r.width - rw;
      if (!on) { overAxis = false; hideAxisCross(); return; }
      overAxis = true;
      const nd = darkRef.current !== false;
      const col = themeColors(nd).cross;
      // Вертикаль — на всех панелях (в снятой с бара координате она ложится
      // ровно на вертикали движка у соседей); горизонталь — только на той
      // панели, чью шкалу читают.
      boxes.forEach((b, j) => {
        if (!b) return;
        const v = axV[j], h = axH[j];
        if (v) {
          if (lastFieldX == null) v.style.display = 'none';
          else {
            v.style.borderLeftColor = col;
            v.style.left = (axisSides(j).lw + lastFieldX) + 'px';
            v.style.display = 'block';
          }
        }
        if (h) {
          if (j !== hit) h.style.display = 'none';
          else { h.style.borderTopColor = col; h.style.top = y + 'px'; h.style.display = 'block'; }
        }
      });
      // Пилсы: на этой панели — уровень под курсором, у соседей гасим.
      const defs = panesRef.current[hit]?.series ?? [];
      for (const sd of ['left', 'right'] as const) {
        const idx = defs.findIndex((d) => (d.scale ?? 'right') === sd);
        const api = idx >= 0 ? apisRef.current[hit]?.[idx] : undefined;
        showPill(hit, sd, api ? (api.coordinateToPrice(y as Coordinate) as number | null) : null);
      }
      panesRef.current.forEach((_, pi) => {
        if (pi === hit) return;
        for (const sd of ['left', 'right'] as const) showPill(pi, sd, null);
      });
    };
    const onRootLeave = () => { overAxis = false; hideCursorUi(); };
    // На DOCUMENT, а не на корне чарта: строки индикаторов рендерятся СОСЕДОМ
    // чарта (React-оверлей поверх всей области), и события с них до корня не
    // доходят — а именно их кнопки чаще всего и перехватывают мышь.
    document.addEventListener('mousemove', onRootMove, true);
    root.addEventListener('mouseleave', onRootLeave);
    unsubs.push(() => {
      document.removeEventListener('mousemove', onRootMove, true);
      root.removeEventListener('mouseleave', onRootLeave);
      for (const el of [...axV, ...axH]) el?.parentNode?.removeChild(el);
    });

    // ── общий кроссхэйр + единый тултип ──
    charts.forEach((chart, i) => {
      const handler = (param: { time?: unknown; point?: { x: number; y: number } }) => {
        if (suppress) return; // ПЕРВОЙ строкой — иначе рекурсия через setCrosshairPosition
        const tip = tips[i];
        const t = typeof param.time === 'number' ? param.time : null;
        if (t == null || !param.point) {
          // Курсор ушёл на ценовую шкалу — движок гасит свой крест, но для
          // пользователя это то же наведение: пилсы и тултип держим, крест
          // рисует onRootMove. (Пилсы и «+» мышь не ловят вовсе — см. их
          // pointer-events:none — так что через них сюда не попадаем.)
          if (overAxis) return;
          // Точка ЕСТЬ, времени НЕТ → курсор в поле, но правее последнего бара:
          // пустошь rightOffset. Движок прибивает вертикаль к последнему бару, а
          // мы принимали это за «курсор ушёл с графика» и гасили всё — у правой
          // оси выходила мёртвая зона шириной в rightOffset (слева пустоши нет,
          // потому слева её и не было). Ведём крест здесь сами, как TradingView:
          // вертикаль по курсору на всех панелях, горизонталь и пилсы — на своей.
          if (param.point) {
            if (buttonsDown) return; // посреди пана крестами не рулим
            suppress = true;
            try { charts.forEach((c) => c.clearCrosshairPosition()); }
            finally { suppress = false; }
            const x = param.point.x, y = param.point.y;
            const nd = darkRef.current !== false;
            const col = themeColors(nd).cross;
            boxes.forEach((b, j) => {
              if (!b) return;
              const v = axV[j], h = axH[j];
              if (v) { v.style.borderLeftColor = col; v.style.left = (axisSides(j).lw + x) + 'px'; v.style.display = 'block'; }
              if (h) {
                if (j !== i) h.style.display = 'none';
                else { h.style.borderTopColor = col; h.style.top = y + 'px'; h.style.display = 'block'; }
              }
            });
            const wDefs = panesRef.current[i]?.series ?? [];
            for (const sd of ['left', 'right'] as const) {
              const wIdx = wDefs.findIndex((d) => (d.scale ?? 'right') === sd);
              const wApi = wIdx >= 0 ? apisRef.current[i]?.[wIdx] : undefined;
              showPill(i, sd, wApi ? (wApi.coordinateToPrice(y as Coordinate) as number | null) : null);
            }
            panesRef.current.forEach((_, pi) => {
              if (pi === i) return;
              for (const sd of ['left', 'right'] as const) showPill(pi, sd, null);
            });
            // Данных под курсором нет — строка показывает последний бар, тултип молчит.
            paintRowValuesRef.current?.(null);
            tips.forEach((tp) => { tp.style.display = 'none'; });
            return;
          }
          hideCursorUi();
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
        // Значения под курсором наружу — для строк индикаторов. Собираем из тех
        // же time-Map'ов, что и тултип, чтобы цифры совпадали гарантированно.
        // Число в строке индикатора обновляем НАПРЯМУЮ через DOM.
        // ⚠️ Не через состояние React: любой ререндер во время перетаскивания
        // доходит до эффекта серий, а тот восстанавливает сохранённый диапазон —
        // график «отскакивает назад» (см. память, откат #903).
        paintRowValuesRef.current?.(t);

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
        // Пилсы: на активной панели — значение под курсором, на остальных —
        // значение синхронизированной точки на их первой серии.
        lastFieldX = (chart.timeScale().timeToCoordinate(t as UTCTimestamp) as number | null) ?? param.point.x;
        if (!overAxis) hideAxisCross();
        for (const sd of ['left', 'right'] as const) {
          const defs = panesRef.current[i]?.series ?? [];
          const idx = defs.findIndex((d) => (d.scale ?? 'right') === sd);
          const api = idx >= 0 ? apisRef.current[i]?.[idx] : undefined;
          showPill(i, sd, api ? (api.coordinateToPrice(param.point.y) as number | null) : null);
        }
        // Пилс ОДИН и переезжает за курсором, как в TradingView: на соседних
        // панелях его нет. Их значения видны в общем тултипе, а набор осей у
        // панелей разный — второй пилс на чужой шкале только путал бы.
        panesRef.current.forEach((_, pi) => {
          if (pi === i) return;
          for (const sd of ['left', 'right'] as const) showPill(pi, sd, null);
        });

        suppress = true;
        try {
          charts.forEach((other, j) => {
            if (j === i) return;
            // ⚠️ seriesApi у setCrosshairPosition нужен движку ТОЛЬКО чтобы найти
            // панель. Цену он пересчитывает в координату по ДЕФОЛТНОЙ шкале этой
            // панели — правой, если она видима и не пуста (Pane.defaultPriceScale
            // в исходниках библиотеки). Поэтому и серию берём с той же шкалы:
            // считать по первой серии панели нельзя, на панели цены первая — сама
            // цена на ЛЕВОЙ шкале (PR #923).
            const defs = panesRef.current[j]?.series ?? [];
            let k = defs.findIndex((d) => (d.scale ?? 'right') === 'right');
            if (k < 0) k = 0;
            const firstApi = apisRef.current[j]?.[k];
            if (!firstApi) { other.clearCrosshairPosition(); return; }
            // На соседях нужна только ВЕРТИКАЛЬ. Отключить горизонталь через
            // applyOptions({crosshair}) нельзя (правило №1), поэтому уводим её
            // цену далеко за пределы данных.
            // ⚠️ Считаем от ДАННЫХ, а не от координаты кромки. Раньше тут было
            // coordinateToPrice(-40): цена «на 40px выше поля» ВЕРНА ТОЛЬКО ДЛЯ
            // ТЕКУЩЕГО масштаба, а при перетаскивании шкала автоскейлится каждый
            // кадр — и та же цена оказывалась уже внутри диапазона. Горизонталь
            // соседа выныривала у верхней кромки и дёргалась: Вадим видел
            // «кроссхэйр по горизонтали улетает в самый верх». В маленьком окне
            // перестройка шкалы крупнее относительно высоты — потому и заметнее.
            const dat = defs[k]?.data;
            const base = dat && dat.length ? (dat[dat.length - 1].value ?? 0) : 0;
            const off = base - (Math.abs(base) + 1) * 1e4;
            try {
              other.setCrosshairPosition(off, t as UTCTimestamp, firstApi);
            } catch { /* шкала соседа ещё без данных — пропускаем кадр, но не рвём пан */ }
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
      // ⚠️ overflow:hidden, НЕ visible. Слой живёт в боксе панели цены, и с visible
      // длинная трендовая линия рисовалась поверх панелей индикаторов снизу —
      // перечёркивала RSI. Обрезаем по своей панели, как терминал.
      drawSvg.style.cssText = 'position:absolute;inset:0;z-index:7;overflow:hidden;pointer-events:none;touch-action:none';
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
      // ── текст на линиях/стрелках (как в TradingView): у выделенной линии в
      // середине живёт «+ Добавьте текст», клик по нему — ввод; введённый текст
      // рисуется над серединой цветом фигуры. Инструменты: тренд/луч/стрелка/
      // горизонталь/вертикаль.
      const LINE_TEXT_TOOLS = new Set(['trend', 'ray', 'arrow', 'hline', 'vline']);
      const lineTextAnchor = (d: LwDrawing): { x: number; y: number } | null => {
        const pb = plotBox();
        if (d.tool === 'hline') { const xy = lp2xy(d.pts[0]); return xy ? { x: pb.left + pb.width / 2, y: xy.y } : null; }
        if (d.tool === 'vline') { const xy = lp2xy(d.pts[0]); return xy ? { x: xy.x, y: pb.height / 2 } : null; }
        const a = lp2xy(d.pts[0]), b0 = d.pts[1] ? lp2xy(d.pts[1]) : null;
        if (!a || !b0) return null;
        const b = d.tool === 'ray' ? rayEnd(a, b0, pb) : b0;
        return { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 };
      };
      const lineTextZone = (d: LwDrawing): { x: number; y: number; w: number; h: number } | null => {
        if (!LINE_TEXT_TOOLS.has(d.tool)) return null;
        const anch = lineTextAnchor(d); if (!anch) return null;
        const label = d.text || '+ Добавьте текст';
        const w2 = Math.max(46, label.length * 7 / 2) + 6;
        return { x: anch.x - w2, y: anch.y - 26, w: w2 * 2, h: 26 };
      };
      const renderLineText = (d: LwDrawing, sel: boolean, preview: boolean) => {
        if (!LINE_TEXT_TOOLS.has(d.tool)) return;
        const anch = lineTextAnchor(d); if (!anch) return;
        const op = String((d.opacity == null ? 1 : d.opacity) * (preview ? 0.7 : 1));
        if (d.text) {
          const fs = d.textSize ?? 12.5;
          const fill = d.textColor || d.color;
          const pos = d.textPos ?? 'above';
          // above — над линией, center — по её оси, below — под ней. Отступ от
          // толщины линии, иначе жирная линия наезжает на буквы.
          const dy = pos === 'center' ? fs * 0.36 : pos === 'below' ? fs + 5 + d.width : -(7 + d.width);
          const ty = anch.y + dy;
          if (d.textBg) {
            // Подложка ЗА текстом: на свечах голая подпись читается плохо.
            const bw = d.text.length * fs * 0.56 + 10, bh = fs + 6;
            drawSvg.appendChild(svgEl('rect', {
              x: anch.x - bw / 2, y: ty - fs * 0.82, width: bw, height: bh, rx: 4,
              fill: 'var(--bg-secondary,#17161A)', 'fill-opacity': op, stroke: fill, 'stroke-opacity': String(Number(op) * 0.45), 'stroke-width': 1,
            }));
          }
          const t = svgEl('text', {
            x: anch.x, y: ty, fill, 'font-size': fs,
            'font-family': 'Inter,-apple-system,sans-serif', 'font-weight': d.textBold === false ? 500 : 600,
            'text-anchor': 'middle', opacity: op,
          });
          t.textContent = d.text;
          drawSvg.appendChild(t);
        } else if (sel && !preview) {
          const t = svgEl('text', {
            x: anch.x, y: anch.y - 8, fill: d.color, 'font-size': 12,
            'font-family': 'Inter,-apple-system,sans-serif', 'text-anchor': 'middle', opacity: '0.55',
          });
          t.textContent = '+ Добавьте текст';
          drawSvg.appendChild(t);
        }
      };
      // Инлайн-редактор текста ПРЯМО на графике — вместо window.prompt: браузерный
      // диалог выглядел чужеродно («окно как у Google») и блокировал страницу.
      // Enter/клик мимо — сохранить, Escape — отменить.
      let textEditEl: HTMLInputElement | null = null;
      const closeTextEditor = () => { const el = textEditEl; textEditEl = null; el?.remove(); };
      const openTextEditor = (ax: number, ay: number, initial: string, onDone: (v: string | null) => void) => {
        closeTextEditor();
        const inp = document.createElement('input');
        textEditEl = inp;
        inp.type = 'text';
        inp.value = initial;
        inp.placeholder = 'Текст…';
        inp.dataset.exportIgnore = 'true';
        inp.style.cssText = 'position:absolute;z-index:30;transform:translate(-50%,-50%);width:150px;'
          + 'padding:4px 8px;border-radius:7px;font:600 12.5px Inter,-apple-system,sans-serif;'
          + 'background:var(--bg-secondary,#17161A);color:var(--text-primary,#F5F1E8);'
          + 'border:1.5px solid var(--accent,#FF5C2B);outline:none;box-shadow:0 8px 22px rgba(0,0,0,0.45)';
        inp.style.left = Math.max(84, Math.min(box.clientWidth - 84, ax)) + 'px';
        inp.style.top = Math.max(18, Math.min(box.clientHeight - 18, ay)) + 'px';
        let finished = false;
        const done = (ok: boolean) => {
          if (finished) return;
          finished = true;
          const v = inp.value.trim();
          closeTextEditor();
          onDone(ok ? v : null);
        };
        inp.addEventListener('keydown', (ke) => {
          ke.stopPropagation();
          if (ke.key === 'Enter') done(true);
          else if (ke.key === 'Escape') done(false);
        });
        inp.addEventListener('blur', () => done(true));
        inp.addEventListener('pointerdown', (pe) => pe.stopPropagation());
        box.appendChild(inp);
        requestAnimationFrame(() => { inp.focus(); inp.select(); });
      };

      // Клик в текст-зону УЖЕ выделенной линии → ввод/правка текста. Только
      // выделенной: первый клик по линии выделяет, второй — редактирует, как в
      // терминале; иначе зона у середины мешала бы просто выделять линию.
      const lineTextClick = (bx: number, by: number): boolean => {
        const selId = selectedDrawIdRef.current;
        if (!selId) return false;
        const d = (drawingsRef.current ?? []).find((q) => q.id === selId);
        if (!d || d.hidden) return false;
        const z = lineTextZone(d);
        if (!z || bx < z.x || bx > z.x + z.w || by < z.y || by > z.y + z.h) return false;
        openTextEditor(z.x + z.w / 2, z.y + z.h / 2, d.text || '', (v) => {
          if (v != null) commit((drawingsRef.current ?? []).map((q) => (q.id === selId ? { ...q, text: v || undefined } : q)));
        });
        return true;
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
          renderLineText(d, sel, preview);
        } else if (d.tool === 'vline') {
          const xy = lp2xy(d.pts[0]); if (!xy) return;
          drawSvg.appendChild(svgEl('line', { x1: xy.x, y1: 0, x2: xy.x, y2: pb.height, ...S, 'stroke-linecap': lc }));
          if (sel) dot(xy.x, pb.height / 2);
          renderLineText(d, sel, preview);
        } else if (d.tool === 'trend' || d.tool === 'ray' || d.tool === 'arrow') {
          const a = lp2xy(d.pts[0]), b0 = lp2xy(d.pts[1]); if (!a || !b0) return;
          const b = d.tool === 'ray' ? rayEnd(a, b0, pb) : b0;
          drawSvg.appendChild(svgEl('line', { x1: a.x, y1: a.y, x2: b.x, y2: b.y, ...S, 'stroke-linecap': d.dash === 'dotted' ? 'round' : 'round' }));
          if (d.tool === 'arrow') {
            const ang = Math.atan2(b0.y - a.y, b0.x - a.x), ah = 9 + w * 2;
            for (const s of [-0.42, 0.42]) drawSvg.appendChild(svgEl('line', { x1: b0.x, y1: b0.y, x2: b0.x - ah * Math.cos(ang - s), y2: b0.y - ah * Math.sin(ang - s), stroke: col, 'stroke-width': w, opacity: op, 'stroke-linecap': 'round' }));
          }
          if (sel) { dot(a.x, a.y); dot(b0.x, b0.y); }
          renderLineText(d, sel, preview);
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
          const fs = d.textSize ?? (13 + w * 2);
          const fill = d.textColor || col;
          const label = d.text || 'Текст';
          if (d.textBg) {
            const bw = label.length * fs * 0.56 + 10, bh = fs + 6;
            drawSvg.appendChild(svgEl('rect', {
              x: xy.x - 5, y: xy.y - fs * 0.82, width: bw, height: bh, rx: 4,
              fill: 'var(--bg-secondary,#17161A)', 'fill-opacity': op, stroke: fill, 'stroke-opacity': String(Number(op) * 0.45), 'stroke-width': 1,
            }));
          }
          const t = svgEl('text', { x: xy.x, y: xy.y, fill, 'font-size': fs, 'font-family': 'Inter,-apple-system,sans-serif', 'font-weight': d.textBold === false ? 500 : 600, opacity: op });
          t.textContent = label;
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
          const fs = d.textSize ?? (13 + d.width * 2);
          return { x: pts[0].x + ox, y: pts[0].y - fs + oy, w: Math.max(40, (d.text || 'Текст').length * fs * 0.58), h: fs };
        }
        if (d.tool === 'ray' && pts.length >= 2) pts[1] = rayEnd(pts[0], pts[1], pb);
        const xs = pts.map((p) => p.x), ys = pts.map((p) => p.y);
        const x = Math.min(...xs), y = Math.min(...ys);
        return { x: x + ox, y: y + oy, w: Math.max(...xs) - x, h: Math.max(...ys) - y };
      };
      let lastRectKey = ' ';
      const emitSelRect = (r: { x: number; y: number; w: number; h: number } | null) => {
        // ⚠️ ПОКА ИДЁТ ЖЕСТ (тащат фигуру или панорамируют график с выделением)
        // — НЕ слать. Прямоугольник меняется каждый кадр, а onSelectionRect —
        // это setState embed'а: выходил ререндер на каждый кадр жеста — те
        // самые «фризы и подёргивания только в режиме рисования». Панель
        // свойств на время жеста замирает (как в TradingView), актуальный rect
        // досылается по отпусканию кнопки (см. onBtnUp).
        if (dragState || buttonsDown) return;
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
        // ⚠️ Хит-слой перехватывает мышь ТОЛЬКО когда выбран инструмент рисования.
        // На «выделении» он прозрачен: иначе в режиме рисования график нельзя было
        // ни панорамировать, ни зумить — слой съедал все жесты. Выделение и
        // перетаскивание фигур на «выделении» работают через onOutsideDown
        // (тот же путь, что и вне режима), а он перехватывает строго при
        // попадании в фигуру.
        const on = !!drawActiveRef.current && !!drawToolRef.current && drawToolRef.current !== 'select';
        drawHit.style.pointerEvents = on ? 'auto' : 'none';
        drawHit.style.cursor = on ? 'crosshair' : 'default';
      };
      drawShapesRef.current = () => { syncDrawInteractivity(); drawShapes(); };
      drawClickRef.current = (bx: number, by: number): boolean => {
        if (drawHiddenRef.current || drawLockedRef.current) return false;
        if (lineTextClick(bx, by)) return true;
        const hit = hitTest(bx, by);
        const cur = selectedDrawIdRef.current;
        if (!hit && cur == null) return false;   // пусто и выделения не было — клик не наш
        selectedDrawIdRef.current = hit ? hit.id : null;
        onSelectDrawRef.current?.(hit ? hit.id : null);
        drawShapes();
        return !!hit;
      };

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
          else if (d.tool === 'text') { const xy = lp2xy(d.pts[0]); const fs = d.textSize ?? (13 + d.width * 2); const half = Math.max(30, (d.text || 'Текст').length * fs * 0.3); if (xy && bx > xy.x - 6 && bx < xy.x + half * 2 && Math.abs(by - (xy.y - fs * 0.35)) < fs * 0.8 + 4) return d; }
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
          if (lineTextClick(x, y)) return;
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
          commit([...(drawingsRef.current ?? []), { id, tool: 'text', pts: [lp], color, width, dash, opacity, text: '' }]);
          selectedDrawIdRef.current = id; onSelectDrawRef.current?.(id); onToolResetRef.current?.();
          // Сразу поле ввода на месте будущего текста; пустой ввод — фигура не нужна.
          openTextEditor(x, y - 8, '', (v) => {
            if (v) commit((drawingsRef.current ?? []).map((q) => (q.id === id ? { ...q, text: v } : q)));
            else { commit((drawingsRef.current ?? []).filter((q) => q.id !== id)); selectedDrawIdRef.current = null; onSelectDrawRef.current?.(null); }
          });
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
          const xy = lp2xy(hit.pts[0]);
          openTextEditor(xy?.x ?? x, (xy?.y ?? y) - 8, hit.text || '', (v) => {
            if (v != null) commit((drawingsRef.current ?? []).map((d) => (d.id === hit.id ? { ...d, text: v || d.text } : d)));
          });
        }
      };
      drawHit.addEventListener('pointerdown', onDrawDown);
      drawHit.addEventListener('pointermove', onDrawMove);
      drawHit.addEventListener('pointerup', onDrawUp);
      drawHit.addEventListener('dblclick', onDrawDbl);
      syncDrawInteractivity();

      // ── перетаскивание фигур ВНЕ режима рисования (как в TradingView) ──
      // Зажал фигуру — тащишь целиком, зажал вершину — тянешь вершину. Пан
      // графика не задет: перехват в capture-фазе строго ПРИ попадании в
      // фигуру, мимо — событие живёт своей жизнью. mousedown движка — отдельное
      // событие, его глушим одноразовым флагом, иначе движок начнёт пан
      // параллельно с нашим перетаскиванием.
      let swallowMouse = false;
      const onOutsideDown = (e: PointerEvent) => {
        // В режиме рисования сюда доходим только на «выделении» (при активном
        // инструменте хит-слой ловит события сам и мы до канваса не добираемся).
        if (drawActiveRef.current && drawToolRef.current && drawToolRef.current !== 'select') return;
        if (drawHiddenRef.current || drawLockedRef.current) return;
        if (e.button !== 0) return;
        if ((e.target as HTMLElement | null)?.tagName !== 'CANVAS') return; // строки/кнопки/грипы — не наши
        if (!(drawingsRef.current ?? []).length) return;
        const { x, y } = relXY(e);
        const hit = hitTest(x, y);
        if (!hit) return;
        e.preventDefault(); e.stopPropagation();
        swallowMouse = true;
        selectedDrawIdRef.current = hit.id; onSelectDrawRef.current?.(hit.id);
        if (hit.locked) { drawShapes(); return; }
        let vi = -1;
        if (hit.tool !== 'brush') for (let k = 0; k < hit.pts.length; k++) { const xy = lp2xy(hit.pts[k]); if (xy && Math.hypot(x - xy.x, y - xy.y) <= HANDLE_R) { vi = k; break; } }
        dragState = vi >= 0
          ? { mode: 'vertex', d: { ...hit, pts: hit.pts.map((pp) => ({ ...pp })) }, vi, startXY: { x, y } }
          : { mode: 'move', d: { ...hit, pts: hit.pts.map((pp) => ({ ...pp })) }, orig: hit.pts.map((pp) => ({ ...pp })), startXY: { x, y } };
        drawShapes();
        const mv = (ev: PointerEvent) => onDrawMove(ev);
        const up = (ev: PointerEvent) => {
          window.removeEventListener('pointermove', mv);
          window.removeEventListener('pointerup', up);
          onDrawUp(ev);
        };
        window.addEventListener('pointermove', mv);
        window.addEventListener('pointerup', up);
      };
      const onSwallow = (e: Event) => { if (swallowMouse) { swallowMouse = false; e.stopPropagation(); e.preventDefault(); } };
      box.addEventListener('pointerdown', onOutsideDown, true);
      box.addEventListener('mousedown', onSwallow, true);

      // Перепроецировать фигуры на пан/зум/ресайз этой панели (как экспирации LwChart).
      // rAF-коалесинг: событие диапазона при пане прилетает чаще кадра, а каждый
      // вызов — полная пересборка SVG. Без коалесинга фигуры «плыли» за графиком
      // с отставанием, это выглядело как «нарисованное съезжает при перемещении».
      let shapesRaf = 0;
      const scheduleShapes = () => {
        if (shapesRaf) return;
        shapesRaf = requestAnimationFrame(() => { shapesRaf = 0; drawShapes(); });
      };
      const onRange = () => scheduleShapes();
      chart()?.timeScale().subscribeVisibleLogicalRangeChange(onRange);
      // Двойной тик: RO срабатывает ДО того, как чарт применит новый размер
      // (его autoSize — свой асинхронный RO). Один прогон по старой геометрии,
      // второй кадром позже по новой — иначе после разворота на весь экран
      // фигуры оставались спроецированными по старым координатам.
      const drawRo = new ResizeObserver(() => { drawShapes(); requestAnimationFrame(() => drawShapes()); });
      drawRo.observe(box);

      cleanupDraw = () => {
        box.removeEventListener('pointerdown', onOutsideDown, true);
        box.removeEventListener('mousedown', onSwallow, true);
        closeTextEditor();
        drawHit.removeEventListener('pointerdown', onDrawDown);
        drawHit.removeEventListener('pointermove', onDrawMove);
        drawHit.removeEventListener('pointerup', onDrawUp);
        drawHit.removeEventListener('dblclick', onDrawDbl);
        try { chart()?.timeScale().unsubscribeVisibleLogicalRangeChange(onRange); } catch { /* chart removed */ }
        if (shapesRaf) cancelAnimationFrame(shapesRaf);
        drawRo.disconnect();
        drawSvg.parentNode?.removeChild(drawSvg);
        drawHit.parentNode?.removeChild(drawHit);
        drawShapesRef.current = null;
        drawClickRef.current = null;
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
      // Двойной клик по ценовой оси — вернуть автомасштаб (и сбросить отступы,
      // накрученные колесом). Ровно как в терминалах.
      const onAxisDbl = (e: MouseEvent) => {
        const ch = chartsRef.current[i];
        if (!ch) return;
        const r = bx.getBoundingClientRect();
        const x = e.clientX - r.left;
        let lw = 0, rw = 0;
        try { lw = ch.priceScale('left').width() || 0; } catch { /* §R2-30 */ }
        try { rw = ch.priceScale('right').width() || 0; } catch { /* §R2-30 */ }
        if (x >= lw && x <= r.width - rw) return;   // двойной клик в поле — не наш
        e.preventDefault(); e.stopPropagation();
        margin.v = 0.12;
        for (const side of ['left', 'right'] as const) {
          try { ch.priceScale(side).applyOptions({ autoScale: true, scaleMargins: { top: 0.12, bottom: 0.06 } }); } catch { /* шкала скрыта */ }
        }
      };
      bx.addEventListener('dblclick', onAxisDbl, true);
      wheelOff.push(() => bx.removeEventListener('dblclick', onAxisDbl, true));
    });

    return () => {
      expLayerApi?.destroy();
      drawExpRef.current = null;
      wheelOff.forEach((f) => f());
      unsubs.forEach((u) => { try { u(); } catch { /* noop */ } });
      cleanupDraw?.();
      charts.forEach((ch) => ch.remove());
      tips.forEach((tp) => tp.parentNode?.removeChild(tp));
      legends.forEach((lg) => lg.parentNode?.removeChild(lg));
      // ⚠️ Пилсы тоже. Их не убирали — а боксы панелей React ПЕРЕИСПОЛЬЗУЕТ,
      // так что при смене числа панелей старые оставались в тех же div'ах и
      // висели на экране призраками с прошлыми значениями.
      pills.forEach((per) => {
        if (!per) return;
        for (const sd of ['left', 'right'] as const) per[sd]?.box.parentNode?.removeChild(per[sd]!.box);
      });
      pillsRef.current = [];
      chartsRef.current = [];
      apisRef.current = [];
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [paneCount]);

  // ── тема + дефолты §9 (сетка/кроссхэйр) ──
  // ⚠️ ЭФФЕКТ ТЕМЫ ВОССТАНОВЛЕН В ИСХОДНОМ ВИДЕ.
  // Попытка переписать его (#886) убивала кроссхэйр целиком: тултип со
  // значениями работал, а ни вертикали, ни горизонтали, ни пилсов на осях не
  // появлялось. Причина не в форме опций — полный набор полей линии проверен и
  // не помогает; и не в chartPrefs (в стенде провайдера нет вовсе). Пока не
  // найдена — не переписывать. Проверять ТОЛЬКО реальным вводом: синтетические
  // MouseEvent движок игнорирует, и кроссхэйра «не видно» даже когда он цел.
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
              // ⚠️ Горизонталь видна ВСЕГДА. Раньше она гасилась при непустом
              // alertAxes — «её роль играет превью-линия уровня». Но превью
              // появляется только при наведении на саму ценовую шкалу, а в окне
              // ОИ alertAxes непустой почти всегда: график жил вообще без
              // горизонтальной линии кроссхэйра. Двоения подписей нет — когда
              // курсор на шкале, он не над полем графика.
              // labelVisible:false — подпись рисуем СВОИМ пилсом (см. pillsRef): у движка
              // она одна на чарт, а на панели цены две оси с разными цветами.
              horzLine: { color: c.cross, labelBackgroundColor: c.lab, visible: true, labelVisible: false },
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
    seriesDefsRef.current = panes.map(() => []);
    bandsRef.current = panes.map(() => []);
    // Линии живут вместе с сериями: removeSeries снёс их выше, ссылки забываем.
    lineRegRef.current = [];

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
          const co = seriesColorOpts(box, def);
          s = def.type === 'candlestick'
            ? chart.addSeries(CandlestickSeries, { ...co, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: false, priceFormat })
            : chart.addSeries(BarSeries, { ...co, priceScaleId: scaleId, priceLineVisible: false, lastValueVisible: false, priceFormat });
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
          const zl = s.createPriceLine({ price: 0, color: col, lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: false, title: '' });
          lineRegRef.current.push({ line: zl, token: def.color, pane: i });
        }
        // Зоны (RSI 30/70). Примитив живёт на серии и снимается вместе с ней,
        // отдельного цикла жизни не заводим. Цвета собираем из цвета зоны, а не
        // из отдельных токенов: так зона всегда согласована с уровнем.
        if (def.bands) {
          // ⚠️ color-mix резолвим ЧЕРЕЗ ПРОБУ, а не подставляем уже посчитанный
          // rgb внутрь строки: canvas не понимает ни var(), ни color-mix, и
          // fillStyle с таким значением молча не применится.
          const b = def.bands;
          const raw = b.color ?? 'var(--text-secondary)';
          const noFill = b.fill === false;
          try {
            const prim = new BandsPrimitive({
              upper: b.upper,
              lower: b.lower,
              middle: b.middle ?? null,
              upperColor: rc(b.upperColor ?? raw),
              middleColor: rc(b.middleColor ?? raw),
              lowerColor: rc(b.lowerColor ?? raw),
              bandColor: noFill ? 'rgba(0,0,0,0)' : rc(b.bandFill ?? `color-mix(in srgb, ${raw} 8%, transparent)`),
              overColor: noFill ? 'rgba(0,0,0,0)' : rc(b.overFill ?? 'color-mix(in srgb, var(--oi-green) 12%, transparent)'),
              underColor: noFill ? 'rgba(0,0,0,0)' : rc(b.underFill ?? 'color-mix(in srgb, var(--oi-red) 12%, transparent)'),
            });
            s.attachPrimitive(prim);
            bandsRef.current[i][pane.series.indexOf(def)] = prim;
          } catch (err) { console.error('LwChartPanes bands failed:', def.id, err); }
        }
        apisRef.current[i].push(s);
        seriesDefsRef.current[i].push(def);
        mapsRef.current[i].push(new Map(def.data.map((p) => [p.time, p.value])));
      }
      // Цвет своего пилса: цвет линии этой оси, но светлее — смешиваем с ФОНОМ,
      // тогда одна формула работает и на тёмной теме, и на светлой (как на сайте).
      pillColorRef.current[i] = {};
      for (const side of ['left', 'right'] as const) {
        const d = pane.series.find((x) => (x.scale ?? 'right') === side);
        if (d) pillColorRef.current[i][side] = rc(`color-mix(in srgb, ${d.color} 45%, var(--bg-primary))`);
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
          const al = api.createPriceLine({
            price: pl.price, color: resolveColor(bx, pl.color ?? 'var(--accent)'),
            lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: true, title: pl.title ?? 'алерт',
          });
          lineRegRef.current.push({ line: al, token: pl.color ?? 'var(--accent)', pane: pi });
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

    // Значения в строках индикаторов: в покое (курсор вне графика) показываем
    // последний бар. Иначе после смены данных строка стояла бы пустой до
    // первого наведения — кроссхэйр её единственный другой источник.
    paintRowValuesRef.current?.(null);

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

  /**
   * ПЕРЕКРАСКА ПРИ СМЕНЕ ТЕМЫ — без пересоздания серий.
   *
   * Цвета задаются токенами (var/color-mix) и резолвятся пробой в момент создания
   * серий, а `dark` в депсах того эффекта нет — иначе он пересоздаёт всё (и это
   * ровно та регрессия с пустой панелью цены, см. предупреждение выше). Поэтому
   * при переключении темы линии, нулевая линия, уровни алертов и зоны оставались
   * покрашенными от ПРОШЛОЙ темы: на светлой они сливались с бумагой. Отсюда
   * «на светлой теме нет горизонтальных линий».
   *
   * rAF обязателен: data-theme проставляет СВОЙ эффект темы, а эффекты идут
   * снизу вверх — без отложки проба прочитает старую тему и перекрасит в неё же.
   */
  useEffect(() => {
    const root = rootRef.current;
    if (!root || chartsRef.current.length !== paneCount) return;
    const id = requestAnimationFrame(() => {
      if (!root.isConnected) return;
      // Тема сменилась — прежние резолвы недействительны.
      colorGen++;
      const boxes = paneBoxes(root);
      seriesDefsRef.current.forEach((defs, i) => {
        const apis = apisRef.current[i];
        const box = boxes[i];
        // Пара «серия ↔ её определение» должна совпадать по длине: за кадр мог
        // прийти новый рендер. Не совпало — панель пропускаем, её перекрасит
        // эффект серий.
        if (!box || !apis || apis.length !== defs.length) return;
        defs.forEach((def, k) => {
          const api = apis[k];
          if (!api) return;
          try { api.applyOptions(seriesColorOpts(box, def)); } catch { /* серия снята */ }
          // Пер-точечные цвета (объёмы, сезонность, потоки) живут в данных, и
          // достать их applyOptions'ом нельзя — только setData.
          // ⚠️ Только для НЕ-OHLC: свечной серии {time,value,color} не скормить,
          // движок бросит на валидации и оборвёт перекраску остальных панелей.
          const isOhlc = def.type === 'candlestick' || def.type === 'bar';
          if (!isOhlc && def.data.some((pt) => pt.color)) {
            try {
              (api as ISeriesApi<'Line'>).setData(def.data.map((pt) => ({
                time: pt.time as UTCTimestamp, value: pt.value,
                ...(pt.color ? { color: resolveColor(box, pt.color) } : {}),
              })));
            } catch (err) { console.error('LwChartPanes repaint setData failed:', def.id, err); }
          }
          if (def.bands) {
            const b = def.bands;
            const raw = b.color ?? 'var(--text-secondary)';
            const noFill = b.fill === false;
            const rc = (c: string | undefined) => resolveColor(box, c);
            bandsRef.current[i]?.[k]?.applyOptions({
              upperColor: rc(b.upperColor ?? raw),
              middleColor: rc(b.middleColor ?? raw),
              lowerColor: rc(b.lowerColor ?? raw),
              bandColor: noFill ? 'rgba(0,0,0,0)' : rc(b.bandFill ?? `color-mix(in srgb, ${raw} 8%, transparent)`),
              overColor: noFill ? 'rgba(0,0,0,0)' : rc(b.overFill ?? 'color-mix(in srgb, var(--oi-green) 12%, transparent)'),
              underColor: noFill ? 'rgba(0,0,0,0)' : rc(b.underFill ?? 'color-mix(in srgb, var(--oi-red) 12%, transparent)'),
            });
          }
        });
      });
      for (const reg of lineRegRef.current) {
        const box = boxes[reg.pane];
        const c = box && probeColor(box, reg.token);
        if (c) { try { reg.line.applyOptions({ color: c }); } catch { /* линия снята с серией */ } }
      }
      syncVpRef.current?.();
      drawShapesRef.current?.();
    });
    return () => cancelAnimationFrame(id);
  }, [dark, paneCount]);

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
