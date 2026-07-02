import { useMemo, useState, useRef, useEffect, useLayoutEffect, useCallback } from 'react';
import { Download, BarChart2, TrendingUp } from 'lucide-react';
import ChartNavigator from './ChartNavigator';
import ChartWatermark from './ChartWatermark';
import { easeOutCubic, morphPts, ptsToPath, ptsToArea } from '../utils/chartAnimation';
import { cssVar, GRID, CROSSHAIR, ANIMATION, TOOLTIP } from '../config/chartTheme';
import { fluid } from '../config/fluidScale';
import { computeChartTopLineY } from './chart/datePillLayout';
import ChartDatePill from './chart/ChartDatePill';
import ChartLegend, { type ChartLegendItem } from './chart/ChartLegend';
import { measureText } from './chart/measureText';
import { axisFontSize } from './chart/chartTypography';
import { formatNumber } from '../utils/formatNumber';
import { niceScale } from '../utils/niceTicks';
import { useChartSettings, OHLC_TYPES, type ChartSeriesType } from '../hooks/useChartSettings';

interface DataPoint {
  time: string;
  value: number;
  /** OHLC (опционально): нужны для типов «свечи»/«бары»/«Хайкен-Аши».
   *  value = close (обратная совместимость — все линейные ряды как раньше).
   *  Ряды без OHLC автоматически откатываются на линию. */
  open?: number;
  high?: number;
  low?: number;
}

export interface ChartAnnotation {
  time: string;         // ISO date matching data point
  label: string;        // Short label (e.g. "SiM6")
  description: string;  // Full description for tooltip
  color: string;        // Background color
  textColor: string;    // Text color
}

interface SimpleChartProps {
  data: DataPoint[];
  secondaryData?: DataPoint[];
  thirdData?: DataPoint[];
  height?: number;
  primaryColor?: string;
  secondaryColor?: string;
  thirdColor?: string;
  showSecondary?: boolean;
  showThird?: boolean;
  /** Скрыть основную линию + левую ось. Используется в индикаторах где primary —
   *  «вспомогательный» в user mental model (Открытые позиции: primary=цена,
   *  юзер воспринимает OI главным). При false: primary line/dots/y-axis/tooltip-line
   *  не рисуются; secondary остаётся в полном объёме. */
  showPrimary?: boolean;
  formatValue?: (value: number) => string;
  /** Формат для labels на ЛЕВОЙ оси (primary). Если не задан — fallback на formatValue.
   *  Используется когда formatValue содержит юнит ("123 трлн ₽"), который не
   *  помещается на оси — короткая версия для axis ("123"). Симметрично formatSecondaryAxis. */
  formatPrimaryAxis?: (value: number) => string;
  formatSecondaryValue?: (value: number) => string;
  formatSecondaryAxis?: (value: number) => string;
  formatThirdValue?: (value: number) => string;
  formatTime?: (time: string) => string;
  /** Формат даты в hover-тултипе (date pill). Если не задан — день+месяц+год
   *  (+ время, если оно есть). Задаётся, когда тултип должен показывать,
   *  например, только месяц+год (помесячные данные — фонды). */
  tooltipDateFormat?: (time: string) => string;
  /** Прижать крайние подписи X-оси внутрь (first→start, last→end), чтобы они
   *  не выходили за ширину графика. Default false (центрированные, как было). */
  clampEdgeLabels?: boolean;
  loading?: boolean;
  primaryLabel?: string;
  secondaryLabel?: string;
  thirdLabel?: string;
  allowHistogram?: boolean;
  histogramDisabled?: boolean;
  defaultHistogram?: boolean;
  /** Тип отображения основной серии. НЕ задан (обычный случай) → берётся
   *  ГЛОБАЛЬНАЯ настройка (useChartSettings, модалка-шестерёнка, весь сайт).
   *  Задан явно → оверрайд для графиков, которым глобальный тип не подходит
   *  (headless-экспорт и т.п.). OHLC-типы (candles/bars/heikin) требуют
   *  open/high/low в data — иначе тихий откат на 'line'. */
  primaryType?: ChartSeriesType;
  showValueHeader?: boolean;
  legendPosition?: 'top' | 'bottom';
  showDownloadButton?: boolean;
  showNavigator?: boolean;
  /** Индекс первого видимого элемента при первом рендере и сбросе (смена data).
   *  Для дефолтного окна (напр. «последний год») при showNavigator: навигатор
   *  стартует с этого индекса, пользователь может растянуть назад до полной истории.
   *  undefined (по умолчанию) → 0 → вся история, как раньше (нулевой эффект на
   *  существующие вызовы). */
  initialStartIndex?: number;
  chartPadding?: { left?: number; right?: number };
  /** Мобила: явный правый отступ графика (px). Для single-axis графиков правый
   *  гуттер по умолчанию резервирует ширину Y-лейбла впустую (оси справа нет).
   *  С clampEdgeLabels можно ужать до ~14 — линия станет заметно шире. */
  mobilePadRight?: number;
  annotations?: ChartAnnotation[];
  hideTime?: boolean;
  forecastCount?: number; // reserved for future use
  horizontalLines?: { value: number; color: string; label?: string; axis?: 'primary' | 'secondary' }[];
  /** Развернуть порядок легенды (secondary первым, primary вторым). Нужно когда
   *  primary линия рисуется как «вспомогательная» а secondary как «главная» —
   *  legend должен ставить главное значение первым. См. Buffett swap (cap → primary,
   *  ratio → secondary), но в легенде ratio должно идти первым. */
  reverseLegend?: boolean;
  /** «Круглые» деления ЛЕВОЙ (primary) оси — nice ticks в стиле TradingView
   *  (50 000 / 100 000…), вместо равного деления диапазона. Домен/масштаб НЕ
   *  меняется (линия и пилюля остаются на месте) — меняются только значения и
   *  позиции делений внутри текущего [min,max]. Opt-in, default false. */
  niceTicks?: boolean;
  /** То же для ПРАВОЙ (secondary) оси. */
  niceTicksSecondary?: boolean;
  /** Ось, к делениям которой привязаны горизонтальные линии сетки.
   *  'primary' (default) — линии на делениях ЛЕВОЙ оси (как было исторически).
   *  'secondary' — линии на делениях ПРАВОЙ оси. Нужно на индикаторах, где
   *  secondary воспринимается ГЛАВНЫМ (Открытые позиции: OI справа) — сетка
   *  должна совпадать с круглыми значениями OI, а не цены. Подписи ЛЕВОЙ оси
   *  при этом остаются на своих позициях (visual reference, без линии). */
  gridAxis?: 'primary' | 'secondary';
}

// Алиасы для обратной совместимости с внутренним кодом
const interpolatePoints = morphPts;
const pointsToPath = ptsToPath;
const pointsToAreaPath = ptsToArea;

// Клампит стартовый индекс навигатора в [0, len-1]. undefined → 0 (вся история).
const clampNavStart = (n: number | undefined, len: number) =>
  n != null && len > 0 ? Math.min(Math.max(0, Math.floor(n)), len - 1) : 0;

export default function SimpleChart({
  data,
  secondaryData,
  thirdData,
  height = 450,
  primaryColor = 'var(--accent)',
  secondaryColor = 'var(--funds-flow-positive)',
  thirdColor = 'var(--funds-flow-negative)',
  showSecondary = false,
  showThird = false,
  showPrimary = true,
  formatValue = (v) => formatNumber(v, 0),
  formatPrimaryAxis,
  formatSecondaryValue,
  formatSecondaryAxis,
  formatThirdValue,
  formatTime = (t) => {
    const date = new Date(t);
    // Numeric DD.MM.YY везде — фиксированная ширина 8 символов.
    // С переменной шириной (нояб./февр./июн.) метки "дёргались" при drag
    // navigator: длина текста менялась вместе со средней датой → визуальный
    // shift даже при стабильной X-позиции тика.
    return date.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' });
  },
  loading = false,
  primaryLabel = 'Цена',
  secondaryLabel = 'OI',
  thirdLabel = '',
  allowHistogram = false,
  histogramDisabled = false,
  defaultHistogram = false,
  primaryType,
  reverseLegend = false,
  niceTicks = false,
  niceTicksSecondary = false,
  gridAxis = 'primary',
  showValueHeader = true,
  legendPosition = 'bottom',
  showDownloadButton = true,
  showNavigator = false,
  initialStartIndex,
  chartPadding,
  mobilePadRight,
  annotations,
  hideTime = false,
  tooltipDateFormat,
  clampEdgeLabels = false,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  forecastCount: _forecastCount = 0,
  horizontalLines: _horizontalLines,
}: SimpleChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const chartWrapRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(800);
  const [isMobile, setIsMobile] = useState(window.innerWidth < 768);
  const [vw, setVw] = useState(typeof window !== 'undefined' ? window.innerWidth : 1920);
  const [hoveredAnnotationIdx, setHoveredAnnotationIdx] = useState<number | null>(null);

  // Все размеры — из единой fluid scale (config/fluidScale.ts + index.css :root).
  // SVG требует JS-числа (clamp в строке через cssVar не работает — parseFloat → NaN).
  const tokens = useMemo(() => ({
    // Y-axis labels (числа слева/справа). Discrete per breakpoint —
    // см. chartTypography.ts: 320=9, 375=10, 425=11, 768=13, 1024=15, 1440=17.
    // Match'ит CSS var --chart-font-y media queries.
    fontY: axisFontSize(vw),
    // X-axis labels (даты под графиком) — тот же размер
    fontX: axisFontSize(vw),
    fontYWeight: cssVar('--chart-font-y-weight', 600),
    tooltipWidth: cssVar('--tooltip-width', 200),
    dotPrimaryR: cssVar('--dot-primary-r', 5),
    dotSecondaryR: cssVar('--dot-secondary-r', 5),
    linePrimaryW: cssVar('--line-primary-width', 3),
    lineSecondaryW: cssVar('--line-secondary-width', 2.5),
    dateTopLegendTop: cssVar('--date-top-legend-top', 38),
    dateTopLegendBottom: cssVar('--date-top-legend-bottom', 8),
    // Annotation chips над графиком (звёздочки/цифры событий)
    annSize: fluid.icoSm(vw),
    annFont: fluid.fs2xs(vw),
    annTooltipFont: fluid.fsXs(vw),
    // Зазор между текстом Y-оси и chart area
    axisGap: fluid.sp2(vw),
    // Tooltip card sizing — fluid от viewport.
    // Mobile (vw<768): 0.36*vw width + lineHeight 1.5×fsXs — компактный.
    // Desktop: cap width 200, lineHeight = 1.5×fsXs (fluid тоже, не fixed 26).
    // Раньше desktop'у hardcode 26px — не скейлилось между 1024px и 2560px,
    // что выглядело static на широких мониторах.
    tooltipCardWidth: vw < 768 ? Math.max(100, vw * 0.36) : 200,
    tooltipLineHeight: Math.round(fluid.fsXs(vw) * (vw < 768 ? 1.5 : 1.7)),
  }), [vw]);

  // Navigator: диапазон видимых данных (индексы в массиве data). Старт с
  // initialStartIndex (если задан) — дефолтное окно вроде «последнего года».
  const [navRange, setNavRange] = useState<[number, number]>(
    [clampNavStart(initialStartIndex, data.length), Math.max(0, data.length - 1)]
  );

  // Отслеживаем ссылку на текущий data — если сменилась, navRange устарел
  const prevDataRef = useRef(data);

  // Сброс навигатора при смене данных (новый период/интервал) — снова к
  // дефолтному окну (initialStartIndex), а не жёстко к 0.
  useEffect(() => {
    if (data !== prevDataRef.current) {
      prevDataRef.current = data;
      setNavRange([clampNavStart(initialStartIndex, data.length), Math.max(0, data.length - 1)]);
    }
  }, [data, initialStartIndex]);

  // Срез данных по выбранному диапазону навигатора
  const displayData = useMemo(() => {
    if (!showNavigator || !data.length) return data;
    const isStale = data !== prevDataRef.current;
    const isFullRange = navRange[0] === 0 && navRange[1] >= data.length - 1;
    if (isStale) return data;
    if (isFullRange) return data;
    return data.slice(navRange[0], navRange[1] + 1);
  }, [showNavigator, data, navRange]);

  const displaySecondaryData = useMemo(() => {
    if (!showNavigator || !secondaryData || !displayData.length) return secondaryData;
    // Сравниваем только даты (YYYY-MM-DD) — свечи 00:00, OI 23:50
    const d0 = displayData[0].time.slice(0, 10);
    const d1 = displayData[displayData.length - 1].time.slice(0, 10);
    return secondaryData.filter(d => d.time.slice(0, 10) >= d0 && d.time.slice(0, 10) <= d1);
  }, [showNavigator, secondaryData, displayData]);

  const displayThirdData = useMemo(() => {
    if (!showNavigator || !thirdData || !displayData.length) return thirdData;
    const d0 = displayData[0].time.slice(0, 10);
    const d1 = displayData[displayData.length - 1].time.slice(0, 10);
    return thirdData.filter(d => d.time.slice(0, 10) >= d0 && d.time.slice(0, 10) <= d1);
  }, [showNavigator, thirdData, displayData]);
  const [chartMode, setChartMode] = useState<'line' | 'histogram'>(defaultHistogram ? 'histogram' : 'line');

  useEffect(() => {
    if (histogramDisabled && chartMode === 'histogram') {
      setChartMode('line');
    }
  }, [histogramDisabled, chartMode]);

  // Глобальные настройки графиков (модалка-шестерёнка): тип основной серии +
  // штрих-режим (доступность: различие линий формой, не цветом). Проп
  // primaryType — явный оверрайд для страниц, где глобальный тип не подходит.
  const { type: globalChartType, dash: dashMode, scope: typeScope } = useChartSettings();
  const requestedType: ChartSeriesType = primaryType ?? globalChartType;
  // OHLC есть в данных? Ряды однородны — достаточно первой точки.
  const hasOhlc = data.length > 0 && data[0].open != null && data[0].high != null && data[0].low != null;
  const resolvedType: ChartSeriesType =
    OHLC_TYPES.includes(requestedType) && !hasOhlc ? 'line' : requestedType;
  // Scope — к какой линии применять тип (выбор юзера: 1-я / 2-я / все):
  //  'primary'   → тип у основной линии, вторичные — линии;
  //  'secondary' → основная остаётся ЛИНИЕЙ, area/columns у вторичных;
  //  'all'       → у всех. OHLC-типы (свечи/бары/хайкен) рисуются только там,
  //  где есть OHLC (основная), при любом scope — вторичным не из чего строиться.
  const applyToOi = typeScope === 'all' || typeScope === 'secondary';
  const oiAsArea = applyToOi && resolvedType === 'area';
  const oiAsColumns = applyToOi && resolvedType === 'columns';
  const primaryEffType: ChartSeriesType =
    typeScope === 'secondary' && !OHLC_TYPES.includes(resolvedType) ? 'line' : resolvedType;

  const animationRef = useRef<number | null>(null);
  const [tooltip, setTooltip] = useState<{
    x: number;
    primaryY: number;
    secondaryY: number | null;
    thirdY: number | null;
    value: number;
    secondaryValue?: number;
    thirdValue?: number;
    time: string;
    visible: boolean;
  }>({ x: 0, primaryY: 0, secondaryY: null, thirdY: null, value: 0, time: '', visible: false });

  // Анимированные пути (area* — заливки под линиями для типа «Область»;
  // secondary/third area используются только при scope='all')
  const [animatedPaths, setAnimatedPaths] = useState({
    primary: '',
    area: '',
    secondary: '',
    areaSecondary: '',
    third: '',
    areaThird: '',
  });

  // Opacity для OI линий (для fade-in эффекта)
  const [oiOpacity, setOiOpacity] = useState(1);


  // Предыдущие точки для интерполяции
  const prevPointsRef = useRef<{
    primary: { x: number; y: number }[];
    secondary: { x: number; y: number }[];
    third: { x: number; y: number }[];
  }>({ primary: [], secondary: [], third: [] });

  // Текущие отрисованные точки (обновляются каждый кадр анимации)
  const currentPointsRef = useRef<{
    primary: { x: number; y: number }[];
    secondary: { x: number; y: number }[];
    third: { x: number; y: number }[];
  }>({ primary: [], secondary: [], third: [] });

  // Флаг первой загрузки
  const isFirstRender = useRef(true);
  const [revealed, setRevealed] = useState(false);

  // === Pill width — точное измерение через getBBox (не canvas-предсказание).
  // canvas.measureText() систематически расходится с SVG-рендером (шрифт,
  // kerning, субпиксели) → pill узкий → текст обрезается. Решение: меряем
  // РЕАЛЬНЫЙ отрисованный <text> через getBBox() в useLayoutEffect (two-pass:
  // 1-й рендер по measureText-estimate, 2-й — по точному bbox).
  const pillTextRefs = useRef<Map<string, SVGTextElement>>(new Map());
  const [pillTextW, setPillTextW] = useState<Map<string, number>>(new Map());
  useLayoutEffect(() => {
    const next = new Map<string, number>();
    pillTextRefs.current.forEach((el, key) => {
      if (el && el.isConnected) {
        try { next.set(key, el.getBBox().width); } catch { /* not rendered */ }
      }
    });
    // setState только при реальном изменении — иначе бесконечный re-render.
    setPillTextW((prev) => {
      if (prev.size !== next.size) return next;
      for (const [k, v] of next) {
        if (Math.abs((prev.get(k) ?? -1) - v) > 0.5) return next;
      }
      return prev;
    });
  });

  // Ширина стабилизировалась — не запускаем animateMorph пока не замерим реальный размер
  const widthStableRef = useRef(false);
  const prevWidthRef = useRef(0);

  // true только когда пользователь тащит навигатор — сбрасывается после animateMorph
  const navDragRef = useRef(false);

  useEffect(() => {
    const updateWidth = () => {
      if (containerRef.current) {
        setWidth(containerRef.current.clientWidth);
      }
      setIsMobile(window.innerWidth < 768);
      setVw(window.innerWidth);
    };
    updateWidth();
    window.addEventListener('resize', updateWidth);
    return () => window.removeEventListener('resize', updateWidth);
  }, []);

  // Перемерить ширину когда данные загрузились (containerRef мог переключиться с loading-div на основной)
  useEffect(() => {
    if (containerRef.current && data.length > 0) {
      const w = containerRef.current.clientWidth;
      if (w !== width && w > 0) setWidth(w);
    }
  }, [data.length > 0]);

  // Адаптивные отступы. Padding-left/right считается под МАКСИМАЛЬНУЮ ширину
  // Y-axis label, чтобы 6-знач числа (тысячи/миллионы) влезали без клиппинга.
  // На desktop падаем в CSS vars (там точные значения для конкретных breakpoints).
  const defaultPad = (() => {
    if (isMobile) {
      // ~7.5 chars worth of digits @ fluid font width.
      // Запас: при 6 знаках (например "159 543") и font 11px нужно ~40px.
      const charW = tokens.fontY * 0.62; // approximate digit width
      const labelMax = charW * 7 + tokens.axisGap * 2;
      return {
        top: fluid.sp4(vw),
        bottom: fluid.sp6(vw) + 16,  // место под X-axis даты
        left: Math.max(40, labelMax),
        right: showSecondary
          ? Math.max(44, labelMax + 4)
          : (mobilePadRight ?? Math.max(36, labelMax - 4)),
      };
    }
    return {
      top: cssVar('--chart-pad-top', 19),
      bottom: cssVar('--chart-pad-bottom', 50),
      left: cssVar('--chart-pad-left', 100),
      right: showSecondary
        ? cssVar('--chart-pad-right-dual', 95)
        : cssVar('--chart-pad-right-single', 12),
    };
  })();
  const padding = {
    ...defaultPad,
    ...(chartPadding && !isMobile ? { left: chartPadding.left ?? defaultPad.left, right: chartPadding.right ?? defaultPad.right } : {}),
  };

  // На мобиле ограничиваем высоту графика
  const effectiveHeight = isMobile ? Math.min(height, 350) : height;
  const chartWidth = Math.max(width - padding.left - padding.right, 100);
  const chartHeight = effectiveHeight - padding.top - padding.bottom;

  // Вычисление целевых точек (использует displayData — срез по навигатору)
  const targetCalc = useMemo(() => {
    if (displayData.length === 0) {
      return {
        points: [],
        secondaryPoints: [],
        thirdPoints: [],
        ohlcPoints: [],
        yTicks: [],
        xTicks: [],
      };
    }

    const isOhlcType = OHLC_TYPES.includes(resolvedType);

    // Хайкен-Аши: рекуррентное сглаживание OHLC по видимому окну.
    // haOpen = (prev haOpen + prev haClose)/2, haClose = (o+h+l+c)/4.
    let ohlcData = displayData;
    if (resolvedType === 'heikin') {
      const ha: DataPoint[] = [];
      for (let i = 0; i < displayData.length; i++) {
        const d = displayData[i];
        const o = d.open ?? d.value, h = d.high ?? d.value, l = d.low ?? d.value, c = d.value;
        const haClose = (o + h + l + c) / 4;
        const prev = ha[i - 1];
        const haOpen = prev ? ((prev.open ?? prev.value) + prev.value) / 2 : (o + c) / 2;
        ha.push({
          time: d.time,
          value: haClose,
          open: haOpen,
          high: Math.max(h, haOpen, haClose),
          low: Math.min(l, haOpen, haClose),
        });
      }
      ohlcData = ha;
    }

    // Primary scale (price). Для OHLC-типов диапазон считаем по high/low —
    // иначе тени свечей упираются в chartClip.
    const values = isOhlcType
      ? ohlcData.flatMap((d) => [d.low ?? d.value, d.high ?? d.value])
      : displayData.map((d) => d.value);
    const minVal = Math.min(...values);
    const maxVal = Math.max(...values);
    const range = maxVal - minVal || 1;
    const yPadding = range * 0.1;
    let yMinVal = minVal - yPadding;
    let yMaxVal = maxVal + yPadding;
    // niceTicks: округляем ГРАНИЦЫ оси до кратного шагу (низ вниз, верх вверх) —
    // тогда деления стоят на круглых значениях И верхнее/нижнее число всегда есть.
    let yNiceTicks: number[] | null = null;
    if (niceTicks) {
      // округление наружу само даёт headroom → берём СЫРОЙ диапазон данных
      const ns = niceScale(minVal, maxVal, 4);
      yMinVal = ns.min; yMaxVal = ns.max; yNiceTicks = ns.ticks;
    }

    // Secondary scale (OI) - общая для secondary и third
    let secYMin = 0;
    let secYMax = 1;
    const allSecondaryValues: number[] = [];
    if (showSecondary && displaySecondaryData && displaySecondaryData.length > 0) {
      allSecondaryValues.push(...displaySecondaryData.map((d) => d.value));
    }
    if (showThird && displayThirdData && displayThirdData.length > 0) {
      allSecondaryValues.push(...displayThirdData.map((d) => d.value));
    }
    if (allSecondaryValues.length > 0) {
      const secMin = Math.min(...allSecondaryValues);
      const secMax = Math.max(...allSecondaryValues);
      const secRange = secMax - secMin || 1;
      secYMin = secMin - secRange * 0.1;
      secYMax = secMax + secRange * 0.1;
    }
    let secNiceTicks: number[] | null = null;
    if (niceTicksSecondary && allSecondaryValues.length > 0) {
      const ns = niceScale(Math.min(...allSecondaryValues), Math.max(...allSecondaryValues), 4);
      secYMin = ns.min; secYMax = ns.max; secNiceTicks = ns.ticks;
    }

    const scaleX = (index: number, total: number) => (index / Math.max(total - 1, 1)) * chartWidth;
    const scaleY = (value: number) => chartHeight - ((value - yMinVal) / (yMaxVal - yMinVal)) * chartHeight;
    const scaleSecondaryY = (value: number) =>
      chartHeight - ((value - secYMin) / (secYMax - secYMin)) * chartHeight;

    // Primary points
    const points = displayData.map((d, i) => ({
      x: scaleX(i, displayData.length),
      y: scaleY(d.value),
      value: d.value,
      time: d.time,
    }));

    // OHLC-точки для свечей/баров/Хайкен-Аши (пустые для остальных типов)
    const ohlcPoints = isOhlcType
      ? ohlcData.map((d, i) => {
        const o = d.open ?? d.value, h = d.high ?? d.value, l = d.low ?? d.value, c = d.value;
        return {
          x: scaleX(i, ohlcData.length),
          yOpen: scaleY(o),
          yHigh: scaleY(h),
          yLow: scaleY(l),
          yClose: scaleY(c),
          up: c >= o,
          time: d.time,
        };
      })
      : [];

    // Secondary points
    let secondaryPoints: typeof points = [];
    if (showSecondary && displaySecondaryData && displaySecondaryData.length > 0) {
      secondaryPoints = displaySecondaryData.map((d, i) => ({
        x: scaleX(i, displaySecondaryData.length),
        y: scaleSecondaryY(d.value),
        value: d.value,
        time: d.time,
      }));
    }

    // Third points
    let thirdPoints: typeof points = [];
    if (showThird && displayThirdData && displayThirdData.length > 0) {
      thirdPoints = displayThirdData.map((d, i) => ({
        x: scaleX(i, displayThirdData.length),
        y: scaleSecondaryY(d.value),
        value: d.value,
        time: d.time,
      }));
    }

    // Y ticks (primary - price)
    const yTickCount = 5;
    const yTicks = yNiceTicks
      ? yNiceTicks.map((value) => ({ value, y: scaleY(value) }))
      : Array.from({ length: yTickCount }, (_, i) => {
        const value = yMinVal + ((yMaxVal - yMinVal) * i) / (yTickCount - 1);
        return { value, y: scaleY(value) };
      });

    // Secondary Y ticks (OI - right axis)
    const secYTicks = allSecondaryValues.length > 0
      ? (secNiceTicks
        ? secNiceTicks.map((value) => ({ value, y: scaleSecondaryY(value) }))
        : Array.from({ length: yTickCount }, (_, i) => {
          const value = secYMin + ((secYMax - secYMin) * i) / (yTickCount - 1);
          return { value, y: scaleSecondaryY(value) };
        }))
      : [];

    // X ticks — адаптивно по реальной ширине chart area (без padding'ов осей),
    // чтобы даты не налезали. На 320px viewport chartWidth ≈ 240px → 3 тика.
    //   maxTicks = chartWidth / labelMinWidth, где labelMinWidth — оценка ширины
    //   одной даты на основе текущего fontX. Раньше были hardcoded breakpoints
    //   (rough 200/320/520 → 3/4/5/7 ticks), но при desktop x2 шрифте labels
    //   накладывались друг на друга. Динамическая формула: при больше шрифте —
    //   меньше ticks автоматически.
    //   "31 окт 2025" ~ 12 chars × charW (= fontX * 0.62) → ~labelW.
    const labelMinW = tokens.fontX * 0.62 * 12;
    const maxTicks = Math.max(3, Math.min(7, Math.floor(chartWidth / labelMinW)));
    const xTickCount = Math.min(maxTicks, displayData.length);
    // Equal-spaced positions: x = (i/(N-1)) * chartWidth.
    // Дата подбирается ближайшая к этой позиции. Так ticks стабильны при
    // drag navigator (даже если displayData.length меняется на ±1) — двигается
    // только сама дата, позиция метки и gridline остаётся фиксированной.
    const xTicks = Array.from({ length: xTickCount }, (_, i) => {
      const xRatio = i / Math.max(xTickCount - 1, 1);
      const x = xRatio * chartWidth;
      const index = Math.round(xRatio * (displayData.length - 1));
      return { time: displayData[index].time, x };
    });

    return { points, secondaryPoints, thirdPoints, ohlcPoints, yTicks, secYTicks, xTicks };
  }, [displayData, displaySecondaryData, displayThirdData, chartWidth, chartHeight, showSecondary, showThird, niceTicks, niceTicksSecondary, resolvedType]);

  // Анимация морфинга
  const animateMorph = useCallback(() => {
    if (displayData.length === 0) return;

    // При первом resize (800 → реальная ширина) — не морфим, а перезапускаем reveal
    if (!widthStableRef.current) {
      widthStableRef.current = true;
      // Первый вызов — продолжаем (это первый reveal)
    } else if (prevWidthRef.current !== chartWidth && prevWidthRef.current > 0) {
      // Ширина изменилась (resize) — просто обновляем пути без анимации
      prevWidthRef.current = chartWidth;
      const targetPrimary = targetCalc.points.map(p => ({ x: p.x, y: p.y }));
      const targetSecondary = targetCalc.secondaryPoints.map(p => ({ x: p.x, y: p.y }));
      const targetThird = targetCalc.thirdPoints.map(p => ({ x: p.x, y: p.y }));
      if (animationRef.current) cancelAnimationFrame(animationRef.current);
      setAnimatedPaths({
        primary: pointsToPath(targetPrimary),
        area: pointsToAreaPath(targetPrimary, chartHeight),
        secondary: pointsToPath(targetSecondary),
        areaSecondary: pointsToAreaPath(targetSecondary, chartHeight),
        third: pointsToPath(targetThird),
        areaThird: pointsToAreaPath(targetThird, chartHeight),
      });
      prevPointsRef.current = { primary: targetPrimary, secondary: targetSecondary, third: targetThird };
      currentPointsRef.current = { primary: [], secondary: [], third: [] };
      // Если reveal ещё не запущен — запускаем
      if (!revealed) setRevealed(true);
      return;
    }
    prevWidthRef.current = chartWidth;

    const targetPrimary = targetCalc.points.map(p => ({ x: p.x, y: p.y }));
    const targetSecondary = targetCalc.secondaryPoints.map(p => ({ x: p.x, y: p.y }));
    const targetThird = targetCalc.thirdPoints.map(p => ({ x: p.x, y: p.y }));

    // Навигатор: мгновенное обновление только при реальном перетаскивании.
    // При смене периода/режима навигатор сбрасывает navRange без флага → анимируем.
    const isNavDrag = navDragRef.current;
    navDragRef.current = false;

    if (isNavDrag) {
      if (animationRef.current) cancelAnimationFrame(animationRef.current);
      setAnimatedPaths({
        primary: pointsToPath(targetPrimary),
        area: pointsToAreaPath(targetPrimary, chartHeight),
        secondary: pointsToPath(targetSecondary),
        areaSecondary: pointsToAreaPath(targetSecondary, chartHeight),
        third: pointsToPath(targetThird),
        areaThird: pointsToAreaPath(targetThird, chartHeight),
      });
      prevPointsRef.current = { primary: targetPrimary, secondary: targetSecondary, third: targetThird };
      currentPointsRef.current = { primary: [], secondary: [], third: [] };
      isFirstRender.current = false;
      setOiOpacity(1);
      return;
    }

    // Если первый рендер или нет предыдущих данных — reveal слева направо
    if (isFirstRender.current || prevPointsRef.current.primary.length === 0) {
      isFirstRender.current = false;
      prevPointsRef.current = {
        primary: targetPrimary,
        secondary: targetSecondary,
        third: targetThird,
      };

      // Сразу ставим финальные пути, CSS анимирует reveal
      setAnimatedPaths({
        primary: pointsToPath(targetPrimary),
        area: pointsToAreaPath(targetPrimary, chartHeight),
        secondary: pointsToPath(targetSecondary),
        areaSecondary: pointsToAreaPath(targetSecondary, chartHeight),
        third: pointsToPath(targetThird),
        areaThird: pointsToAreaPath(targetThird, chartHeight),
      });
      setOiOpacity(1);
      setRevealed(true);
      return;
    }

    // Если анимация уже идёт — стартуем с текущей визуальной позиции, не с начальной
    // Это устраняет "дёргание" при быстром переключении периодов
    const hasCurrentState = currentPointsRef.current.primary.length > 0;
    const fromPrimary = hasCurrentState ? currentPointsRef.current.primary : prevPointsRef.current.primary;
    const fromSecondary = hasCurrentState ? currentPointsRef.current.secondary : prevPointsRef.current.secondary;
    const fromThird = hasCurrentState ? currentPointsRef.current.third : prevPointsRef.current.third;

    let startTime: number | null = null;
    const duration = ANIMATION.duration;

    const animate = (timestamp: number) => {
      if (!startTime) startTime = timestamp;
      const elapsed = timestamp - startTime;
      const t = Math.min(elapsed / duration, 1);
      const eased = easeOutCubic(t);

      // Интерполяция primary линии
      const interpolatedPrimary = interpolatePoints(fromPrimary, targetPrimary, eased);

      // Интерполяция secondary - только если есть откуда и куда
      let interpolatedSecondary: { x: number; y: number }[] = [];
      if (targetSecondary.length > 0) {
        if (fromSecondary.length > 0) {
          interpolatedSecondary = interpolatePoints(fromSecondary, targetSecondary, eased);
        } else {
          // Появление с fade
          interpolatedSecondary = targetSecondary;
          setOiOpacity(eased);
        }
      } else if (fromSecondary.length > 0) {
        // Исчезновение с fade
        interpolatedSecondary = fromSecondary;
        setOiOpacity(1 - eased);
      }

      // Интерполяция third - аналогично
      let interpolatedThird: { x: number; y: number }[] = [];
      if (targetThird.length > 0) {
        if (fromThird.length > 0) {
          interpolatedThird = interpolatePoints(fromThird, targetThird, eased);
        } else {
          interpolatedThird = targetThird;
        }
      } else if (fromThird.length > 0) {
        interpolatedThird = fromThird;
      }

      // Сохраняем текущую визуальную позицию для плавного прерывания
      currentPointsRef.current = {
        primary: interpolatedPrimary,
        secondary: interpolatedSecondary,
        third: interpolatedThird,
      };

      setAnimatedPaths({
        primary: pointsToPath(interpolatedPrimary),
        area: pointsToAreaPath(interpolatedPrimary, chartHeight),
        secondary: pointsToPath(interpolatedSecondary),
        areaSecondary: pointsToAreaPath(interpolatedSecondary, chartHeight),
        third: pointsToPath(interpolatedThird),
        areaThird: pointsToAreaPath(interpolatedThird, chartHeight),
      });

      if (t < 1) {
        animationRef.current = requestAnimationFrame(animate);
      } else {
        prevPointsRef.current = {
          primary: targetPrimary,
          secondary: targetSecondary,
          third: targetThird,
        };
        currentPointsRef.current = { primary: [], secondary: [], third: [] };
        setOiOpacity(1);
      }
    };

    // Отменяем предыдущую анимацию
    if (animationRef.current) {
      cancelAnimationFrame(animationRef.current);
    }

    animationRef.current = requestAnimationFrame(animate);
  }, [displayData, targetCalc, chartHeight, chartWidth, showNavigator]);

  // Запуск анимации при изменении данных
  useEffect(() => {
    animateMorph();

    return () => {
      if (animationRef.current) {
        cancelAnimationFrame(animationRef.current);
      }
    };
  }, [animateMorph]);

  // Функция скачивания графика как PNG
  const downloadChart = useCallback(() => {
    if (!svgRef.current) return;

    const svg = svgRef.current;

    // Клонируем SVG для модификации
    const clonedSvg = svg.cloneNode(true) as SVGSVGElement;

    // Добавляем xmlns если нет
    clonedSvg.setAttribute('xmlns', 'http://www.w3.org/2000/svg');

    // Добавляем фон в SVG
    const bgRect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    bgRect.setAttribute('width', '100%');
    bgRect.setAttribute('height', '100%');
    bgRect.setAttribute('fill', '#121523');
    clonedSvg.insertBefore(bgRect, clonedSvg.firstChild);

    // Добавляем inline стили для текста
    const texts = clonedSvg.querySelectorAll('text');
    texts.forEach(text => {
      text.style.fontFamily = 'system-ui, -apple-system, sans-serif';
    });

    const svgData = new XMLSerializer().serializeToString(clonedSvg);

    // Создаём canvas с учётом devicePixelRatio для чёткости
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const scale = 2; // Для ретина-дисплеев
    canvas.width = width * scale;
    canvas.height = height * scale;
    ctx.scale(scale, scale);

    // Создаём изображение из SVG
    const img = new Image();
    const svgBlob = new Blob([svgData], { type: 'image/svg+xml;charset=utf-8' });
    const url = URL.createObjectURL(svgBlob);

    img.onload = () => {
      ctx.drawImage(img, 0, 0, width, height);
      URL.revokeObjectURL(url);

      // Скачиваем PNG
      const link = document.createElement('a');
      link.download = `chart-${new Date().toISOString().slice(0, 10)}.png`;
      link.href = canvas.toDataURL('image/png');
      link.click();
    };

    img.onerror = () => {
      console.error('Ошибка при создании изображения');
      URL.revokeObjectURL(url);
    };

    img.src = url;
  }, [width, height]);

  // Интерполяция точки на линии по X координате (ОПТИМИЗИРОВАНО: бинарный поиск O(log n))
  const interpolatePointOnLine = (points: typeof targetCalc.points, mouseX: number) => {
    if (points.length === 0) return null;
    if (points.length === 1) return points[0];

    // Бинарный поиск вместо линейного (O(log n) вместо O(n))
    let left = 0;
    let right = points.length - 1;

    // Если mouseX за пределами — вернуть ближайшую крайнюю точку
    if (mouseX <= points[0].x) return points[0];
    if (mouseX >= points[right].x) return points[right];

    while (left < right - 1) {
      const mid = Math.floor((left + right) / 2);
      if (points[mid].x <= mouseX) {
        left = mid;
      } else {
        right = mid;
      }
    }

    const p1 = points[left];
    const p2 = points[right];

    // y интерполируем — точка плавно идёт по видимой линии
    // time/value привязываем к ближайшей реальной точке данных
    const t = (mouseX - p1.x) / (p2.x - p1.x);
    const nearest = t < 0.5 ? p1 : p2;
    return {
      x: mouseX,
      y: p1.y + (p2.y - p1.y) * t,
      value: nearest.value,
      time: nearest.time,
    };
  };

  // Общая логика обновления tooltip по X-координате
  const updateTooltipAtX = useCallback((clientX: number, svgElement: SVGSVGElement) => {
    if (targetCalc.points.length === 0) return;

    const rect = svgElement.getBoundingClientRect();
    const mouseX = clientX - rect.left - padding.left;

    if (mouseX < 0 || mouseX > chartWidth) {
      setTooltip(prev => ({ ...prev, visible: false }));
      return;
    }

    const primaryPoint = interpolatePointOnLine(targetCalc.points, mouseX);
    const secondaryPoint = showSecondary ? interpolatePointOnLine(targetCalc.secondaryPoints, mouseX) : null;
    const thirdPoint = showThird ? interpolatePointOnLine(targetCalc.thirdPoints, mouseX) : null;

    if (!primaryPoint) return;

    setTooltip({
      x: primaryPoint.x + padding.left,
      primaryY: primaryPoint.y + padding.top,
      secondaryY: secondaryPoint ? secondaryPoint.y + padding.top : null,
      thirdY: thirdPoint ? thirdPoint.y + padding.top : null,
      value: primaryPoint.value,
      secondaryValue: secondaryPoint?.value,
      thirdValue: thirdPoint?.value,
      time: primaryPoint.time,
      visible: true,
    });
  }, [targetCalc, chartWidth, padding, showSecondary, showThird]);

  // Mouse events
  const handleMouseMove = (e: React.MouseEvent<SVGSVGElement>) => {
    updateTooltipAtX(e.clientX, e.currentTarget);
  };

  const handleMouseLeave = () => {
    setTooltip((prev) => ({ ...prev, visible: false }));
  };

  // Touch events — для мобильных.
  // preventDefault убран: React 17+ привязывает touch listeners как passive
  // → preventDefault() = no-op + console warning ("Unable to preventDefault
  // inside passive event listener invocation"). Скролл предотвращается через
  // CSS `touch-action: none` на SVG (см. style ниже) — это нативный механизм,
  // работает быстрее и без warnings.
  const handleTouchStart = (e: React.TouchEvent<SVGSVGElement>) => {
    if (e.touches.length === 1) {
      updateTooltipAtX(e.touches[0].clientX, e.currentTarget);
    }
  };

  const handleTouchMove = (e: React.TouchEvent<SVGSVGElement>) => {
    if (e.touches.length === 1) {
      updateTooltipAtX(e.touches[0].clientX, e.currentTarget);
    }
  };

  const handleTouchEnd = () => {
    // Скрываем tooltip через 1.5 сек после отпускания пальца
    setTimeout(() => {
      setTooltip((prev) => ({ ...prev, visible: false }));
    }, 1500);
  };

  // Зарезервированная высота loading/empty state'а должна МАТЧИТЬ финальный layout —
  // иначе CLS jump когда данные приедут. Loaded version: p-5 (40) + legend (~36 если top)
  // + svg (effectiveHeight) + navigator (~64 если показан). Зарезервируем эту сумму.
  const placeholderHeight =
    effectiveHeight
    + 40 // p-5 top + bottom
    + (legendPosition === 'top' ? 10 : 0) // legend block (~20px текст + mb 2 минус 12px подтяжки к верху)
    + (showNavigator ? 6 : 0); // navigator (компактный рельс, отрицательные margin)

  // Показываем полный лоадер только если нет данных вообще
  if (data.length === 0 && loading) {
    return (
      <div ref={containerRef} className="rounded-2xl flex items-center justify-center bg-theme-primary border border-theme" style={{ height: placeholderHeight }}>
        <div className="flex items-center text-theme-secondary" style={{ gap: 'var(--sp-3)' }}>
          <div className="w-6 h-6 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
          <span style={{ fontSize: 'var(--fs-base)' }}>Загрузка...</span>
        </div>
      </div>
    );
  }

  if (data.length === 0 && !loading) {
    return (
      <div ref={containerRef} className="rounded-2xl flex items-center justify-center bg-theme-primary border border-theme" style={{ height: placeholderHeight }}>
        <p className="text-theme-secondary text-lg">Нет данных для отображения</p>
      </div>
    );
  }

  const currentValue = displayData[displayData.length - 1]?.value || 0;
  const firstValue = displayData[0]?.value || currentValue;
  const change = currentValue - firstValue;
  const changePercent = firstValue !== 0 ? (change / firstValue) * 100 : 0;
  const isPositive = change >= 0;

  // Блок легенды — fluid scale АГРЕССИВНЕЕ обычного --fs-sm/--sp-4.
  // На Buffett страницах метки длинные ("Капитализация / ВВП" + "Капитализация
  // (трлн ₽)"), и при стандартном font 11-14px они разваливаются на 2 строки
  // на 375px viewport. Поэтому свой clamp с floor 9px (вместо 11) и более
  // агрессивный gap-clamp (6px на mobile вместо 10).
  // SVG-based legend: <circle> + <text dominantBaseline="middle"> рендерится
  // pixel-perfect одинаково в browser и html2canvas. Никаких CSS hacks с
  // padding/transform — только spec-compliant SVG geometry.
  const baseLegendItems: ChartLegendItem[] = [
    ...(showPrimary ? [{ color: primaryColor, label: primaryLabel || '' }] : []),
    ...(showSecondary && secondaryLabel ? [{ color: secondaryColor, label: secondaryLabel }] : []),
    ...(showThird && thirdLabel ? [{ color: thirdColor, label: thirdLabel }] : []),
  ];
  // reverseLegend инвертирует порядок — нужно когда primary/secondary swap'нуты
  // на уровне компонента-вызывателя (Buffett: cap=primary, ratio=secondary),
  // но в легенде главное значение (ratio) должно идти первым.
  const legendItems: ChartLegendItem[] = reverseLegend ? [...baseLegendItems].reverse() : baseLegendItems;
  // Legend size: discrete per breakpoint — handled internally by ChartLegend
  // через chartTypography. SimpleChart не передаёт fontSize/dotSize prop'ы,
  // полагается на responsive defaults.
  const legendBlock = (
    <ChartLegend
      items={legendItems}
      fontWeight={600}
      itemGap={6}
      gap="clamp(6px, 1vw, 16px)"
      style={{ color: 'var(--text-primary)' }}
    />
  );

  return (
    <div className="rounded-2xl p-5 bg-theme-primary border border-theme relative">
      {/* Кнопка переключения линия/гистограмма — editorial-style */}
      {allowHistogram && (
        <button
          onClick={() => !histogramDisabled && setChartMode(m => m === 'line' ? 'histogram' : 'line')}
          disabled={histogramDisabled}
          className={`editorial-press absolute top-4 ${showDownloadButton ? 'right-[4.5rem]' : 'right-4'} z-10 flex items-center justify-center w-11 h-11 rounded-lg ${histogramDisabled ? 'cursor-not-allowed opacity-40' : ''}`}
          style={{
            background: 'var(--bg-primary)',
            border: '1.5px solid var(--text-primary)',
            color: histogramDisabled ? 'var(--text-muted)' : 'var(--text-primary)',
          }}
          title={histogramDisabled ? 'Гистограмма недоступна в этом режиме' : chartMode === 'line' ? 'Переключить на гистограмму' : 'Переключить на линию'}
        >
          {chartMode === 'line' ? <BarChart2 size={18} /> : <TrendingUp size={18} />}
        </button>
      )}

      {/* Кнопка скачивания — editorial-style */}
      {showDownloadButton && (
        <button
          onClick={downloadChart}
          className="editorial-press absolute top-4 right-4 z-10 flex items-center justify-center w-11 h-11 rounded-lg"
          style={{
            background: 'var(--bg-primary)',
            border: '1.5px solid var(--text-primary)',
            color: 'var(--text-primary)',
          }}
          title="Скачать график как PNG"
        >
          <Download size={18} />
        </button>
      )}

      {/* Маленький индикатор загрузки поверх графика — paper-style без glass */}
      {loading && (
        <div className="absolute top-4 left-4 z-10 flex items-center rounded-lg border border-theme shadow-md" style={{ background: 'var(--bg-primary)', padding: 'var(--sp-2) var(--sp-3)', gap: 'var(--sp-2)' }}>
          <div className="w-4 h-4 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
          <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-xs)' }}>Обновление...</span>
        </div>
      )}

      {/* Область измерения ширины (без padding) */}
      <div ref={containerRef}>

      {/* Легенда — вверху, по центру. Negative margin-top компенсирует p-5 (20px)
          контейнера: легенда встаёт на --chart-legend-top-gap от верхней границы
          paper-card — почти вплотную, как в cbr-flows. */}
      {legendPosition === 'top' && (
        <div className="flex justify-center" style={{ marginTop: 'calc(var(--chart-legend-top-gap, 8px) - 20px)', marginBottom: 'var(--chart-legend-mb, 16px)' }}>{legendBlock}</div>
      )}

      {/* Заголовок с текущим значением */}
      {showValueHeader && (
        <div className="mb-2 flex items-baseline gap-4">
          <span className="text-4xl font-bold text-theme-primary tracking-tight">
            {formatValue(currentValue)}
          </span>
          {/* Цвета через theme vars — funds-flow-positive (forest green в editorial-light,
              brighter в dark) и funds-flow-negative (clay/red). bg = тот же цвет 10% opacity
              через color-mix чтобы получить tint на любой теме. */}
          <span
            className="text-base font-semibold px-2 py-0.5 rounded-lg"
            style={{
              color: isPositive ? 'var(--funds-flow-positive)' : 'var(--funds-flow-negative)',
              background: isPositive
                ? 'color-mix(in srgb, var(--funds-flow-positive) 10%, transparent)'
                : 'color-mix(in srgb, var(--funds-flow-negative) 10%, transparent)',
            }}
          >
            {isPositive ? '↑' : '↓'} {Math.abs(changePercent).toFixed(2)}%
          </span>
        </div>
      )}

      {/* SVG График */}
      <div ref={chartWrapRef}
        className={`relative ${revealed ? 'chart-reveal' : ''}`}
        style={revealed ? undefined : { visibility: 'hidden' }}>
        <svg
          ref={svgRef}
          width={width}
          height={effectiveHeight}
          overflow="visible"
          className="cursor-crosshair select-none"
          style={{ touchAction: 'none', backgroundColor: 'var(--bg-primary)' }}
          onMouseMove={handleMouseMove}
          onMouseLeave={handleMouseLeave}
          onTouchStart={handleTouchStart}
          onTouchMove={handleTouchMove}
          onTouchEnd={handleTouchEnd}
        >
          <defs>
            <linearGradient id="primaryGradient" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={primaryColor} stopOpacity="0.3" />
              <stop offset="50%" stopColor={primaryColor} stopOpacity="0.1" />
              <stop offset="100%" stopColor={primaryColor} stopOpacity="0" />
            </linearGradient>
            <clipPath id="chartClip">
              <rect x={0} y={0} width={chartWidth} height={chartHeight} />
            </clipPath>
          </defs>

          {/* dual-axis активен только когда реально рисуется правая ось
              (showSecondary + есть secondary y-ticks). В таком случае красим
              ОБЕ оси своими цветами — симметрия. Иначе single-axis → нейтральный серый. */}
          <g transform={`translate(${padding.left}, ${padding.top})`}>
            {/* Горизонтальные линии сетки. По умолчанию — на делениях ЛЕВОЙ
                (primary) оси. gridAxis="secondary" (или showPrimary=false →
                левой оси нет) кладёт линии на круглые деления ПРАВОЙ оси, чтобы
                сетка совпадала с главной серией (OI), а не с ценой. Линии и
                подписи осей теперь независимы: при разных nice-ticks у осей
                число делений слева/справа не совпадает, общий цикл их «связал»
                бы и сломал выравнивание. */}
            {(() => {
              const secTicks = targetCalc.secYTicks ?? [];
              // gridAxis="secondary" + есть деления OI → сетка по правой оси.
              // Иначе сохраняем прежнее поведение: showPrimary → левая ось,
              // иначе правая (secTicks), как было до prop'а.
              const gridTicks = (gridAxis === 'secondary' && secTicks.length > 0)
                ? secTicks
                : (showPrimary ? targetCalc.yTicks : secTicks);
              return gridTicks.map((tick, i) => (
                <line
                  key={`grid-${i}`}
                  x1={0}
                  y1={tick.y}
                  x2={chartWidth}
                  y2={tick.y}
                  stroke={GRID.major}
                  strokeWidth="1"
                />
              ));
            })()}

            {/* Подписи ЛЕВОЙ (primary) оси — деления цены. Рисуются независимо
                от линий сетки: при gridAxis="secondary" линии стоят на делениях
                OI, а цена остаётся на своих позициях как visual reference.
                Цвет primaryColor зеркалит цвет линии графика и pill. */}
            {showPrimary && targetCalc.yTicks.map((tick, i) => (
              <text
                key={`pri-${i}`}
                x={-tokens.axisGap}
                y={tick.y}
                textAnchor="end"
                dominantBaseline="central"
                fill={primaryColor}
                fontSize={tokens.fontY}
                fontWeight={tokens.fontYWeight}
                opacity={0.9}
              >
                {/* Split на "123.45" + " трлн ₽" — unit рисуется меньшим
                    шрифтом (70%), чтобы длинные axis labels не «давили»
                    на chart area. Match требует space перед unit:
                    "123.45 трлн" → split, "55.43%" → no split. */}
                {(() => {
                  const s = (formatPrimaryAxis || formatValue)(tick.value);
                  const m = s.match(/^(.+?)\s+([а-яА-Яa-zA-Z₽].*?)$/);
                  if (!m) return s;
                  return (
                    <>
                      {m[1]}
                      <tspan dx={2} fontSize={tokens.fontY * 0.7} fontWeight={700} opacity={0.85}>{m[2]}</tspan>
                    </>
                  );
                })()}
              </text>
            ))}

            {/* Правая ось Y (secondary). На мобиле раньше скрывалась из-за маленького
                --chart-pad-right-dual (48px), но с увеличением до 68px labels помещаются. */}
            {showSecondary && targetCalc.secYTicks && targetCalc.secYTicks.map((tick, i) => (
              <text
                key={`sec-${i}`}
                x={chartWidth + tokens.axisGap}
                y={tick.y}
                textAnchor="start"
                dominantBaseline="central"
                fill={secondaryColor}
                fontSize={tokens.fontY}
                fontWeight={tokens.fontYWeight}
                opacity="0.9"
              >
                {formatSecondaryAxis ? formatSecondaryAxis(tick.value) : formatSecondaryValue ? formatSecondaryValue(tick.value) : formatNumber(tick.value, 0)}
              </text>
            ))}

            {/* Вертикальные линии сетки + X метки.
                first/last anchor 'start'/'end' чтобы крайние даты не наезжали
                на Y-оси. Безопасно при фикс-ширине формата DD.MM.YY — текст
                одной длины, поэтому visual gap между крайней и средней меткой
                стабилен (не "дёргается"). */}
            {targetCalc.xTicks.map((tick, i) => {
              const isFirst = i === 0;
              const isLast = i === targetCalc.xTicks.length - 1;
              return (
                <g key={i}>
                  {!isFirst && !isLast && (
                    <line
                      x1={tick.x}
                      y1={0}
                      x2={tick.x}
                      y2={chartHeight}
                      stroke={GRID.major}
                      strokeWidth="1"
                    />
                  )}
                  {/* Все ticks с anchor='middle' для визуально равномерных интервалов.
                      Крайние тексты могут чуть выходить за chart area, но SVG
                      overflow=visible + wrapper padding это позволяет. */}
                  <text
                    x={tick.x}
                    y={chartHeight + 30}
                    textAnchor={clampEdgeLabels ? (isFirst ? 'start' : isLast ? 'end' : 'middle') : 'middle'}
                    fill="var(--axis-color, #9CA3B8)"
                    fontSize={tokens.fontX}
                    fontWeight={tokens.fontYWeight}
                  >
                    {formatTime(tick.time)}
                  </text>
                </g>
              );
            })}

            {/* Область графика с клиппингом */}
            <g clipPath="url(#chartClip)">
              {/* ClipPath для обрезки сплошной линии (без прогнозных точек) */}
              {_forecastCount > 0 && targetCalc.points.length > _forecastCount && (
                <defs>
                  <clipPath id="solidClip">
                    <rect x={0} y={0}
                      width={targetCalc.points[targetCalc.points.length - 1 - _forecastCount].x + 1}
                      height={chartHeight} />
                  </clipPath>
                </defs>
              )}

              {/* Область под основной линией (тип «Область») — градиентная заливка
                  под ценой, чтобы визуально отделить цену от вторичной серии (ОИ).
                  Сама линия рисуется сверху (ниже), как в TradingView area-режиме. */}
              {showPrimary && primaryEffType === 'area' && animatedPaths.area && (
                <>
                  <defs>
                    <linearGradient id="priceAreaGrad" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={primaryColor} stopOpacity="0.28" />
                      <stop offset="100%" stopColor={primaryColor} stopOpacity="0.02" />
                    </linearGradient>
                  </defs>
                  <path
                    d={animatedPaths.area}
                    fill="url(#priceAreaGrad)"
                    stroke="none"
                    clipPath={_forecastCount > 0 ? 'url(#solidClip)' : undefined}
                  />
                </>
              )}

              {/* Основная линия (цена — сплошная до прогноза).
                  showPrimary=false → пропускаем рендер (юзер скрыл primary через toggle).
                  Рисуется только в line/area типах — для столбиков/свечей/баров
                  primary рендерится своими блоками ниже. */}
              {showPrimary && (primaryEffType === 'line' || primaryEffType === 'area') && animatedPaths.primary && (
                <path
                  d={animatedPaths.primary}
                  fill="none"
                  stroke={primaryColor}
                  strokeWidth={tokens.linePrimaryW}
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  clipPath={_forecastCount > 0 ? "url(#solidClip)" : undefined}
                />
              )}

              {/* Тип «Столбики» — колонки значения основной серии (работает для
                  любого ряда, OHLC не нужен). Базовая линия — низ chart area,
                  как у существующей OI-гистограммы. */}
              {showPrimary && primaryEffType === 'columns' && targetCalc.points.length > 0 && (
                <g>
                  {targetCalc.points.map((p, i) => {
                    const barWidth = Math.max((chartWidth / targetCalc.points.length) * 0.55, 1);
                    const barHeight = Math.max(chartHeight - p.y, 0);
                    return (
                      <rect
                        key={`pri-col-${i}`}
                        x={p.x - barWidth / 2}
                        y={p.y}
                        width={barWidth}
                        height={barHeight}
                        fill={primaryColor}
                        fillOpacity={0.55}
                        stroke={primaryColor}
                        strokeWidth={0.5}
                      />
                    );
                  })}
                </g>
              )}

              {/* Типы «Свечи» и «Хайкен-Аши» — тело open↔close + тень high↔low.
                  Цвета up/down = --oi-green/--oi-red → палитры доступности
                  применяются автоматически. */}
              {showPrimary && (primaryEffType === 'candles' || primaryEffType === 'heikin') && targetCalc.ohlcPoints.length > 0 && (
                <g>
                  {targetCalc.ohlcPoints.map((p, i) => {
                    const slot = chartWidth / targetCalc.ohlcPoints.length;
                    const bw = Math.min(Math.max(slot * 0.6, 1.5), 13);
                    const col = p.up ? 'var(--oi-green)' : 'var(--oi-red)';
                    const bodyY = Math.min(p.yOpen, p.yClose);
                    const bodyH = Math.max(Math.abs(p.yClose - p.yOpen), 1);
                    return (
                      <g key={`candle-${i}`}>
                        <line
                          x1={p.x} y1={p.yHigh} x2={p.x} y2={p.yLow}
                          stroke={col}
                          strokeWidth={Math.max(Math.min(bw * 0.15, 2), 1)}
                        />
                        <rect x={p.x - bw / 2} y={bodyY} width={bw} height={bodyH} fill={col} />
                      </g>
                    );
                  })}
                </g>
              )}

              {/* Тип «Бары» (OHLC) — вертикаль high↔low, засечка open слева,
                  close справа. Классика для тех, кому свечи «жирные». */}
              {showPrimary && primaryEffType === 'bars' && targetCalc.ohlcPoints.length > 0 && (
                <g>
                  {targetCalc.ohlcPoints.map((p, i) => {
                    const slot = chartWidth / targetCalc.ohlcPoints.length;
                    const tick = Math.min(Math.max(slot * 0.3, 1.5), 6);
                    const col = p.up ? 'var(--oi-green)' : 'var(--oi-red)';
                    const sw = Math.max(Math.min(slot * 0.15, 2.5), 1.2);
                    return (
                      <g key={`ohlc-bar-${i}`} stroke={col} strokeWidth={sw}>
                        <line x1={p.x} y1={p.yHigh} x2={p.x} y2={p.yLow} />
                        <line x1={p.x - tick} y1={p.yOpen} x2={p.x} y2={p.yOpen} />
                        <line x1={p.x} y1={p.yClose} x2={p.x + tick} y2={p.yClose} />
                      </g>
                    );
                  })}
                </g>
              )}

              {/* Пунктирный прогноз — хаотичная траектория.
                  showPrimary=false → forecast тоже скрываем (он визуализирует продолжение primary). */}
              {showPrimary && _forecastCount > 0 && targetCalc.points.length > _forecastCount && (() => {
                const pPts = targetCalc.points;
                const sPts = targetCalc.secondaryPoints;
                const fc = _forecastCount;
                const pForecast = pPts.slice(pPts.length - fc - 1);
                const sForecast = sPts.length > fc ? sPts.slice(sPts.length - fc - 1) : [];
                if (pForecast.length < 2) return null;
                const pPath = pForecast.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x.toFixed(1)} ${p.y.toFixed(1)}`).join(' ');
                const sPath = sForecast.length >= 2 ? sForecast.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x.toFixed(1)} ${p.y.toFixed(1)}`).join(' ') : '';
                const pLast = pForecast[pForecast.length - 1];
                const sLast = sForecast.length > 0 ? sForecast[sForecast.length - 1] : null;
                // Длина пути для анимации stroke-dashoffset
                const pLen = pForecast.reduce((sum, p, i) => {
                  if (i === 0) return 0;
                  const prev = pForecast[i - 1];
                  return sum + Math.sqrt((p.x - prev.x) ** 2 + (p.y - prev.y) ** 2);
                }, 0);

                return (
                  <g>
                    <path d={pPath} fill="none"
                      stroke={primaryColor} strokeWidth={tokens.lineSecondaryW}
                      strokeDasharray="6 4" strokeLinecap="round" opacity="0.7"
                      strokeDashoffset={pLen}
                    >
                      <animate attributeName="stroke-dashoffset" from={pLen} to="0" dur="2s" fill="freeze" />
                    </path>
                    <circle cx={pLast.x} cy={pLast.y} r="4" fill={primaryColor} opacity="0">
                      <animate attributeName="opacity" from="0" to="0.8" dur="0.4s" begin="1.8s" fill="freeze" />
                    </circle>
                    <text x={pLast.x + 8} y={pLast.y + 4}
                      fill={primaryColor} fontSize={tokens.fontX} fontWeight="700" opacity="0">
                      {pLast.value.toFixed(0)}%
                      <animate attributeName="opacity" from="0" to="0.9" dur="0.4s" begin="1.8s" fill="freeze" />
                    </text>

                    {showSecondary && sPath && sLast && (
                      <>
                        <path d={sPath} fill="none"
                          stroke={secondaryColor} strokeWidth="2"
                          strokeDasharray="6 4" strokeLinecap="round" opacity="0.6"
                          strokeDashoffset={pLen}
                        >
                          <animate attributeName="stroke-dashoffset" from={pLen} to="0" dur="2s" fill="freeze" />
                        </path>
                        <circle cx={sLast.x} cy={sLast.y} r="3.5" fill={secondaryColor} opacity="0">
                          <animate attributeName="opacity" from="0" to="0.7" dur="0.4s" begin="1.8s" fill="freeze" />
                        </circle>
                      </>
                    )}
                  </g>
                );
              })()}

              {/* Заливки под вторичными линиями (тип «Область» + scope «все серии»).
                  Лёгкий градиент цветом серии — линии рисуются сверху. */}
              {showSecondary && oiAsArea && chartMode === 'line' && animatedPaths.areaSecondary && (
                <>
                  <defs>
                    <linearGradient id="secAreaGrad" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={secondaryColor} stopOpacity="0.16" />
                      <stop offset="100%" stopColor={secondaryColor} stopOpacity="0.02" />
                    </linearGradient>
                  </defs>
                  <path d={animatedPaths.areaSecondary} fill="url(#secAreaGrad)" stroke="none" opacity={oiOpacity} />
                </>
              )}
              {showThird && oiAsArea && chartMode === 'line' && animatedPaths.areaThird && (
                <>
                  <defs>
                    <linearGradient id="thirdAreaGrad" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={thirdColor} stopOpacity="0.16" />
                      <stop offset="100%" stopColor={thirdColor} stopOpacity="0.02" />
                    </linearGradient>
                  </defs>
                  <path d={animatedPaths.areaThird} fill="url(#thirdAreaGrad)" stroke="none" opacity={oiOpacity} />
                </>
              )}

              {/* Third данные — гистограмма или линия. Столбики также при
                  scope «все серии» + тип «Столбики». */}
              {showThird && (chartMode === 'histogram' || oiAsColumns) && targetCalc.thirdPoints.length > 0 && (
                <g opacity={oiOpacity} className="transition-opacity duration-300">
                  {targetCalc.thirdPoints.map((p, i) => {
                    const barWidth = Math.max((chartWidth / targetCalc.thirdPoints.length) * 0.35, 1);
                    const barHeight = Math.max(chartHeight - p.y, 0);
                    return (
                      <rect
                        key={`third-bar-${i}`}
                        x={p.x - barWidth / 2 + barWidth * 0.5}
                        y={p.y}
                        width={barWidth}
                        height={barHeight}
                        fill={thirdColor}
                        fillOpacity={0.5}
                        stroke={thirdColor}
                        strokeWidth={0.5}
                      />
                    );
                  })}
                </g>
              )}
              {showThird && chartMode === 'line' && !oiAsColumns && animatedPaths.third && (
                <path
                  d={animatedPaths.third}
                  fill="none"
                  stroke={thirdColor}
                  strokeWidth={tokens.lineSecondaryW}
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  opacity={oiOpacity}
                  strokeDasharray={dashMode ? '2.5 5' : undefined}
                />
              )}

              {/* Secondary данные — гистограмма или линия (или столбики при scope «все серии») */}
              {showSecondary && (chartMode === 'histogram' || oiAsColumns) && targetCalc.secondaryPoints.length > 0 && (
                <g opacity={oiOpacity} className="transition-opacity duration-300">
                  {targetCalc.secondaryPoints.map((p, i) => {
                    const totalSeries = showThird ? 2 : 1;
                    const barWidth = Math.max((chartWidth / targetCalc.secondaryPoints.length) * 0.35, 1);
                    const barHeight = Math.max(chartHeight - p.y, 0);
                    return (
                      <rect
                        key={`sec-bar-${i}`}
                        x={p.x - barWidth / 2 - (totalSeries > 1 ? barWidth * 0.5 : 0)}
                        y={p.y}
                        width={barWidth}
                        height={barHeight}
                        fill={secondaryColor}
                        fillOpacity={0.5}
                        stroke={secondaryColor}
                        strokeWidth={0.5}
                      />
                    );
                  })}
                </g>
              )}
              {/* Штрих-режим (доступность): secondary — длинный штрих, third —
                  точечный. Линии различимы ФОРМОЙ при полной монохромности,
                  когда никакая палитра не помогает. */}
              {showSecondary && chartMode === 'line' && !oiAsColumns && animatedPaths.secondary && (
                <path
                  d={animatedPaths.secondary}
                  fill="none"
                  stroke={secondaryColor}
                  strokeWidth={tokens.lineSecondaryW}
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  opacity={oiOpacity}
                  strokeDasharray={dashMode ? '9 6' : undefined}
                  clipPath={_forecastCount > 0 ? "url(#solidClip)" : undefined}
                />
              )}

              {/* Затемнение области после курсора — theme-aware,
                  лёгкое (~10%) чтобы не делать тёмный прямоугольник на бумаге */}
              {tooltip.visible && (
                <rect
                  x={tooltip.x - padding.left}
                  y={0}
                  width={Math.max(0, chartWidth - (tooltip.x - padding.left))}
                  height={chartHeight}
                  fill="var(--text-primary)"
                  opacity="0.08"
                  className="chart-hover-ui transition-opacity duration-150"
                />
              )}
            </g>

            {/* Вертикальная линия и точки курсора — theme-aware
                (text-primary хорошо виден на любом фоне, не сливается с линиями) */}
            {tooltip.visible && (
              <g className="chart-hover-ui">
                <line
                  x1={tooltip.x - padding.left}
                  y1={0}
                  x2={tooltip.x - padding.left}
                  y2={chartHeight}
                  stroke="var(--text-primary)"
                  strokeWidth={CROSSHAIR.strokeWidth}
                  strokeDasharray="4,4"
                  opacity="0.55"
                />
                {/* Точка на основной линии — скрываем когда primary не показан */}
                {showPrimary && (
                  <circle
                    cx={tooltip.x - padding.left}
                    cy={tooltip.primaryY - padding.top}
                    r={tokens.dotPrimaryR}
                    fill={primaryColor}
                    stroke="#0B0D12"
                    strokeWidth="2"
                    className="drop-shadow-lg"
                  />
                )}
                {/* Точка на secondary линии */}
                {showSecondary && tooltip.secondaryY !== null && (
                  <circle
                    cx={tooltip.x - padding.left}
                    cy={tooltip.secondaryY - padding.top}
                    r={tokens.dotSecondaryR}
                    fill={secondaryColor}
                    stroke="#0B0D12"
                    strokeWidth="2"
                  />
                )}
                {/* Точка на third линии */}
                {showThird && tooltip.thirdY !== null && (
                  <circle
                    cx={tooltip.x - padding.left}
                    cy={tooltip.thirdY - padding.top}
                    r={tokens.dotSecondaryR}
                    fill={thirdColor}
                    stroke="#0B0D12"
                    strokeWidth="2"
                  />
                )}
              </g>
            )}
          </g>

          {/* Тултип: дата вверху вертикальной линии + карточка значений */}
          {tooltip.visible && (() => {
            const cardWidth = tokens.tooltipCardWidth;
            const isRightHalf = tooltip.x > padding.left + chartWidth / 2;
            let cardX = isRightHalf
              ? tooltip.x - cardWidth - 8
              : tooltip.x + 8;
            // Viewport bounds: не выходим за пределы SVG области
            cardX = Math.max(0, Math.min(cardX, chartWidth + padding.left - cardWidth));

            const fmtSecondary = formatSecondaryValue || formatValue;
            const fmtThird = formatThirdValue || formatValue;

            const lines: { color: string; label: string; value: string }[] = [];
            if (showPrimary) {
              lines.push({ color: primaryColor, label: primaryLabel, value: formatValue(tooltip.value) });
            }
            if (showSecondary && tooltip.secondaryValue !== undefined) {
              lines.push({ color: secondaryColor, label: secondaryLabel, value: fmtSecondary(tooltip.secondaryValue) });
            }
            if (showThird && tooltip.thirdValue !== undefined) {
              lines.push({ color: thirdColor, label: thirdLabel, value: fmtThird(tooltip.thirdValue) });
            }

            // Padding 8px сверху/снизу + lineHeight per row, fluid от vw
            const cardHeight = 16 + lines.length * tokens.tooltipLineHeight;

            return (
              <>
                {/* Дата рендерится через HTML div — см. ниже после </svg> */}

                {/* Карточка значений рядом с курсором */}
                <foreignObject
                  className="chart-hover-ui"
                  x={cardX}
                  y={Math.min(Math.max(tooltip.primaryY - cardHeight / 2, padding.top), padding.top + chartHeight - cardHeight)}
                  width={cardWidth}
                  height={cardHeight}
                >
                  <div className={`${TOOLTIP.containerClass} pointer-events-none`} style={TOOLTIP.containerStyle}>
                    {lines.map((line, i) => (
                      <div key={i} className="flex items-center justify-between py-0.5" style={{ gap: 'var(--sp-2)' }}>
                        <div className="flex items-center min-w-0" style={{ gap: 'var(--sp-1)' }}>
                          <span className={TOOLTIP.dotClass} style={{ ...TOOLTIP.dotStyle, backgroundColor: line.color }} />
                          <span className={`${TOOLTIP.labelClass} truncate`} style={TOOLTIP.labelStyle}>{line.label}</span>
                        </div>
                        <span className={`${TOOLTIP.valueClass} text-theme-primary whitespace-nowrap`} style={TOOLTIP.valueStyle}>{line.value}</span>
                      </div>
                    ))}
                  </div>
                </foreignObject>
              </>
            );
          })()}

          {/* Current value labels — TradingView-style на правой оси chart-area.
              SVG-based (rect pill + text dominantBaseline=middle) живёт в той же
              coordinate system что и axis labels — pixel-perfect между browser
              и html2canvas snapshot. Видны ВСЕГДА (включая при hover).
              Раньше были HTML divs с position:absolute + transform:translateY(-50%)
              — html2canvas игнорировал transform → text сидел не по центру pill'а. */}
          {targetCalc.points.length > 0 && (() => {
          // Strip большие units (трлн/млрд/млн/тыс/₽). % оставляем —
          // axis labels показывают "60%" с процентом, pill должен совпадать.
          const stripUnits = (s: string) =>
            s.replace(/\s*(трлн ₽|млрд ₽|млн ₽|тыс ₽|₽)\s*$/g, '').trim();
          // padSign убран — раньше добавлял FIGURE SPACE к positive чтобы
          // align digit columns между +/- pills, но это сдвигало pill text-LEFT
          // правее axis tick text. Без него pill вровень с осью.

          type LabelEntry = { color: string; x: number; y: number; value: string; key: string };
          const labels: LabelEntry[] = [];
          // Primary pill — только если primary видим. showPrimary=false → юзер
          // выключил основную линию через toggle, pill тоже скрываем (иначе на
          // пустом chart висит «фантомное» значение цены/капитализации).
          if (showPrimary) {
            const lastP = targetCalc.points[targetCalc.points.length - 1];
            // Если formatPrimaryAxis задан явно — используем его (он уже compact
            // версия для axis: содержит unit в нужной форме, без stripUnits).
            // Иначе fallback на стрипнутую версию formatValue (старое поведение).
            // Pill text идентичен axis labels — visual alignment гарантирован.
            const primaryPillStr = formatPrimaryAxis
              ? formatPrimaryAxis(lastP.value)
              : stripUnits(formatValue(lastP.value));
            labels.push({
              color: primaryColor,
              x: padding.left + lastP.x,
              y: padding.top + lastP.y,
              value: primaryPillStr,
              key: 'primary',
            });
          }
          if (showSecondary && targetCalc.secondaryPoints.length > 0) {
            const lastS = targetCalc.secondaryPoints[targetCalc.secondaryPoints.length - 1];
            const fmt = formatSecondaryAxis || formatSecondaryValue || formatValue;
            labels.push({
              color: secondaryColor,
              x: padding.left + lastS.x,
              y: padding.top + lastS.y,
              value: stripUnits(fmt(lastS.value)),
              key: 'secondary',
            });
          }
          if (showThird && targetCalc.thirdPoints.length > 0) {
            const lastT = targetCalc.thirdPoints[targetCalc.thirdPoints.length - 1];
            const fmt = formatSecondaryAxis || formatThirdValue || formatValue;
            labels.push({
              color: thirdColor,
              x: padding.left + lastT.x,
              y: padding.top + lastT.y,
              value: stripUnits(fmt(lastT.value)),
              key: 'third',
            });
          }
          // SVG geometry. Каждая pill размещается у СВОЕЙ оси:
          //   primary (главная серия) → ЛЕВАЯ ось → pill справа от axis text
          //                              text textAnchor="end", прижата к axis
          //   secondary/third          → ПРАВАЯ ось → pill слева от axis text
          //                              textAnchor="start", прижата к axis
          // Pill height = fontY + 2*padY. Pill width = textW + 2*padX.
          const fontY = tokens.fontY;
          // Match axis tick weight (typically 600) — главное условие для
          // pixel-align между pill'ом и axis tick'ом выше/ниже.
          const fontWeight = tokens.fontYWeight;
          // padX=6 — равные ~5-6px воздуха с обеих сторон pill'а.
          //
          // 2026-05-15 ИТЕРАЦИЯ 2: pill должен иметь те же font-настройки что
          // и axis tick text (которая рендерится прямо над ним) — тогда они
          // pixel-align'ятся естественно. Раньше pill использовал weight=700
          // для main и 700 для tspan unit'а, но ось — weight 600 main + 700
          // unit. Разница веса → разница ширины → pill смещался от оси.
          // Плюс был баг: unit unit измерялся с весом 400, рендерился с 700.
          // Лечится тремя одновременными изменениями (см. ниже):
          //   1. fontWeight = tokens.fontYWeight (match ось main)
          //   2. unit measure at 700 (match tspan render)
          //   3. убран fontVariantNumeric + textLength (на pill теперь они
          //      не нужны, осi их тоже не используют → natural rendering
          //      сам гарантирует симметрию).
          //
          // 2026-05-21 ФИНАЛ: ширина pill теперь измеряется через getBBox()
          // реально отрисованного <text> (см. pillTextW выше) — это ТОЧНОЕ
          // измерение, а не canvas-предсказание. padX=8 — симметричный воздух
          // с обеих сторон, pill центрируется на тексте.
          const padX = 8;
          const padY = 2;
          const pillH = fontY + padY * 2;
          // AIR — гарантированный зазор между краем ГРАФИКА и ЗАЛИВКОЙ pill'а.
          // Раньше pill центрировался на тексте оси (текст у края графика ±axisGap),
          // а заливка добавляет padX(8) > axisGap(~6-8) → заливка заезжала на ~1-2px
          // ВНУТРЬ графика и садилась прямо на конец линии (повторная жалоба коллеги),
          // на отрицательных значениях минус ещё расширял. Теперь pill живёт В ЖЁЛОБЕ
          // ОСИ: внутренний край заливки прижат к границе графика + AIR и растёт
          // НАРУЖУ, в поле оси. Линию не перекрывает НИКОГДА — ни на пике, ни на минусе.
          const AIR = 4;
          const plotLeft = padding.left;
          const plotRight = padding.left + chartWidth;

          return labels.map((l) => {
            // Split на main + unit (для smaller fontSize у unit) — symmetric с
            // axis labels rendering. Pill width учитывает оба фрагмента.
            const splitMatch = l.value.match(/^(.+?)\s+([а-яА-Яa-zA-Z₽].*?)$/);
            const mainPart = splitMatch ? splitMatch[1] : l.value;
            const unitPart = splitMatch ? splitMatch[2] : '';
            const unitFontY = fontY * 0.7;
            const mainW = measureText(mainPart, fontY, fontWeight);
            const unitW = unitPart ? measureText(unitPart, unitFontY, 700) + 2 : 0;
            // estimateW — canvas-прикидка для ПЕРВОГО рендера. На втором рендере
            // берём ТОЧНУЮ ширину из getBBox (pillTextW) — она измерена с
            // реально отрисованного <text>, совпадает пиксель-в-пиксель.
            const estimateW = mainW + unitW;
            const measuredW = pillTextW.get(l.key);
            const effectiveTextW = measuredW != null ? measuredW : estimateW;
            // padX*2 — симметричный воздух вокруг текста.
            const pillW = Math.ceil(effectiveTextW) + padX * 2;

            // primary → ЛЕВАЯ ось (pill в левом жёлобе, правый край заливки у графика),
            // secondary/third → ПРАВАЯ ось (pill в правом жёлобе, левый край у графика).
            const isLeftSide = l.key === 'primary';
            const rawLeft = isLeftSide
              ? plotLeft - AIR - pillW   // правый край заливки = plotLeft − AIR
              : plotRight + AIR;          // левый край заливки = plotRight + AIR
            // Подстраховка: не вылезти за пределы холста (узкие mobile-жёлоба).
            const pillLeft = Math.min(Math.max(rawLeft, 2), width - pillW - 2);
            // Текст центрируется ВНУТРИ pill'а (anchor middle) — заливка и текст
            // двигаются вместе, выравнивание текста к pill'у гарантировано.
            const textX = pillLeft + pillW / 2;

            return (
              <g key={l.key} pointerEvents="none">
                <rect
                  x={pillLeft}
                  y={l.y - pillH / 2}
                  width={pillW}
                  height={pillH}
                  rx={4}
                  ry={4}
                  fill={l.color}
                />
                <text
                  ref={(el) => {
                    if (el) pillTextRefs.current.set(l.key, el);
                    else pillTextRefs.current.delete(l.key);
                  }}
                  x={textX}
                  y={l.y}
                  textAnchor="middle"
                  dominantBaseline="central"
                  fill="#FFFFFF"
                  fontSize={fontY}
                  fontWeight={fontWeight}
                >
                  {mainPart}
                  {unitPart && (
                    <tspan
                      dx={2}
                      fontSize={unitFontY}
                      fontWeight={700}
                      opacity={0.95}
                    >
                      {unitPart}
                    </tspan>
                  )}
                </text>
              </g>
            );
          });
        })()}
        </svg>

        {/* Водяной знак Фрейма — ПРЯМО на графике, левый нижний угол.
            Лежит внутри chartWrapRef (который relative), т.е. позиционирован
            относительно SVG-области. pointer-events:none — не мешает
            crosshair'у и tooltip'ам.

            Позиция привязана к data area, не к wrapper:
              left  = padding.left + 5   → 5px ВНУТРИ data area от левого края
              bottom = padding.bottom + 5 → 5px над x-axis
            Симметричный gap (5px и слева, и снизу) — visually выглядит как
            одинаковый отступ от левой Y-оси и от нижней X-линии. Раньше было
            +20 слева и +5 снизу — visually прижато к низу, отстранено от левого
            края. Это даёт ОДИНАКОВОЕ визуальное положение на OI page (dual axis,
            padding.right=95) и на Funds Money page (single axis, padding.right=12),
            независимо от ширины wrapper'а или количества Y-осей. */}
        <ChartWatermark left={padding.left + 5} bottom={padding.bottom + 5} />

        {/* Asset name перенесён в legendBlock row — выше chartWrapRef'а в DOM,
            на одном уровне с legend строкой. Раньше был absolute здесь, сидел
            под верхней gridline вместо legend-level. */}

        {/* Аннотации контрактов — кружки под графиком */}
        {annotations && annotations.length > 0 && (() => {
          const visibleAnnotations = annotations
            .map((ann, idx) => {
              // Сначала точное совпадение
              let dataIdx = displayData.findIndex(d => d.time.slice(0, 10) === ann.time.slice(0, 10));
              // Нечёткое: ближайший бар в пределах 7 дней
              if (dataIdx === -1) {
                const annTs = new Date(ann.time).getTime();
                let bestDist = Infinity;
                for (let i = 0; i < displayData.length; i++) {
                  const dist = Math.abs(new Date(displayData[i].time).getTime() - annTs);
                  if (dist < bestDist && dist <= 7 * 86400000) {
                    bestDist = dist;
                    dataIdx = i;
                  }
                }
              }
              if (dataIdx === -1) return null;
              const x = padding.left + (dataIdx / Math.max(displayData.length - 1, 1)) * chartWidth;
              return { ...ann, x, origIdx: idx };
            })
            .filter(Boolean) as (ChartAnnotation & { x: number; origIdx: number })[];

          if (!visibleAnnotations.length) return null;

          return visibleAnnotations.map((ann) => (
            <div
              key={ann.origIdx}
              // Позиционируем кружок чистыми left/top без CSS-трансформаций.
              // По X: left - annSize/2 центрирует его над точкой (раньше был
              // -translate-x-1/2, но Tailwind v4 отдаёт его через CSS-свойство
              // `translate`, которое html2canvas при экспорте по 📷 не читает).
              // По Y: кладём НИЖНИЙ край кружка на нижнюю линию графика
              // (top = chartHeight - annSize), а не центрируем на ней
              // (было - annSize/2). Так кружок сидит над осью с зазором до
              // подписей дат, а не наезжает на них снизу.
              className="absolute group z-30"
              style={{ left: ann.x - tokens.annSize / 2, top: padding.top + chartHeight - tokens.annSize }}
              onMouseEnter={() => setHoveredAnnotationIdx(ann.origIdx)}
              onMouseLeave={() => setHoveredAnnotationIdx(null)}
            >
              <div
                className="rounded-full flex items-center justify-center cursor-pointer transition-opacity opacity-50 hover:opacity-100"
                style={{ width: tokens.annSize, height: tokens.annSize, backgroundColor: 'var(--ann-bg, #3a3f4f)', color: 'var(--axis-color, #9CA3B8)', fontSize: tokens.annFont, fontWeight: 600 }}
              >
                {ann.label.slice(0, 2)}
              </div>
              {/* Тултип при hover */}
              <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 hidden group-hover:block pointer-events-none">
                <div className="rounded-xl border border-theme shadow-md whitespace-nowrap" style={{ background: 'var(--bg-primary)', padding: 'var(--sp-3) var(--sp-4)' }}>
                  <span className="font-semibold text-theme-primary" style={{ fontSize: tokens.annTooltipFont }}>{ann.description}</span>
                </div>
              </div>
            </div>
          ));
        })()}

        {/* Вертикальная линия аннотации при hover */}
        {hoveredAnnotationIdx !== null && annotations && (() => {
          const ann = annotations[hoveredAnnotationIdx];
          if (!ann) return null;
          const dataIdx = displayData.findIndex(d => d.time.slice(0, 10) === ann.time.slice(0, 10));
          if (dataIdx === -1) return null;
          const x = padding.left + (dataIdx / Math.max(displayData.length - 1, 1)) * chartWidth;
          return (
            <div className="absolute pointer-events-none" style={{ left: x, top: padding.top, height: chartHeight, width: 1 }}>
              <div className="w-full h-full" style={{ borderLeft: '1px dashed rgba(156, 163, 184, 0.4)' }} />
            </div>
          );
        })()}

      </div>

      {/* Плавающая дата аннотации — единая логика через computeChartTopLineY/ChartDatePill */}
      {hoveredAnnotationIdx !== null && annotations && (() => {
        const ann = annotations[hoveredAnnotationIdx];
        if (!ann) return null;
        const wrap = chartWrapRef.current;
        if (!wrap) return null;
        const dataIdx = displayData.findIndex(d => d.time.slice(0, 10) === ann.time.slice(0, 10));
        if (dataIdx === -1) return null;
        const x = padding.left + (dataIdx / Math.max(displayData.length - 1, 1)) * chartWidth;
        const annDate = new Date(ann.time).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', year: 'numeric' });
        const topLineY = computeChartTopLineY({ wrapper: wrap, paddingTop: padding.top });
        return (
          <ChartDatePill
            date={annDate}
            x={wrap.offsetLeft + x}
            topLineY={topLineY}
            minX={wrap.offsetLeft + padding.left}
            maxX={wrap.offsetLeft + padding.left + chartWidth}
          />
        );
      })()}

      {/* HTML тултип даты — единая логика */}
      {tooltip.visible && (() => {
        const wrap = chartWrapRef.current;
        if (!wrap) return null;
        const d = new Date(tooltip.time);
        const dateStr = d.toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', year: 'numeric' });
        const hours = d.getHours();
        const minutes = d.getMinutes();
        const htmlDateLabel = tooltipDateFormat
          ? tooltipDateFormat(tooltip.time)
          : (!hideTime && (hours !== 0 || minutes !== 0))
            ? `${dateStr} ${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}`
            : dateStr;
        const topLineY = computeChartTopLineY({ wrapper: wrap, paddingTop: padding.top });
        return (
          <ChartDatePill
            className="chart-hover-ui"
            date={htmlDateLabel}
            x={wrap.offsetLeft + tooltip.x}
            topLineY={topLineY}
            minX={wrap.offsetLeft + padding.left}
            maxX={wrap.offsetLeft + padding.left + chartWidth}
          />
        );
      })()}

      {/* Навигатор временного диапазона.
          data-export-ignore: ChartCaptureButton использует html2canvas с
          ignoreElements который скипает elements с этим атрибутом → navigator
          не попадёт в snapshot, в export'е остаётся только chart. */}
      {showNavigator && (
        <div data-export-ignore="true">
          <ChartNavigator
            data={data}
            onChange={(s, e, isDrag) => { navDragRef.current = isDrag; setNavRange([s, e]); }}
            // Таймлайн всегда классический accent (оранжевый в editorial), не цвет серии.
            color="var(--accent)"
            // Фактический padding (число), не CSS-переменная. padding уже
            // учитывает mobile labelMax, chartPadding-override и desktop CSS —
            // навигатор всегда ровно по ядру графика. CSS-var давал рассинхрон
            // когда страница переопределяла chartPadding (OI/СЧА/Баффетт).
            insetLeft={padding.left}
            insetRight={padding.right}
          />
        </div>
      )}

      {/* Легенда — внизу */}
      {legendPosition === 'bottom' && (
        <div className="mt-4 justify-center">{legendBlock}</div>
      )}

      </div>{/* end containerRef */}
    </div>
  );
}