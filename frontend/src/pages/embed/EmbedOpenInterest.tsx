/**
 * EmbedOpenInterest — самодостаточный виджет ОИ для iframe.
 *
 * Контролы (актив-фьючерс / таймфрейм / период / группа / режим / вариант ОИ /
 * цена / экспирации) — в drawer'е настроек (шестерёнка в заголовке панели).
 * Шапка виджета — только название + «Открытые позиции», чтобы график получал
 * максимум места в узкой панели.
 *
 * Это chromeless full-PRO зеркало OpenInterestPage: серии ОИ
 * (displayMode × oiVariant), alignToCandles, цвета/лейблы и аннотации экспираций
 * портированы VERBATIM по семантике со страницы.
 *
 * ВАЖНО: ОИ живёт на ФЬЮЧЕРСАХ — instrument это код фьючерса (SR), не акции (SBER).
 * Состояние шарится по ключам frame:embed:oi:* (в extension-iframe storage
 * партиционирован → там состояние своё).
 */
import { useCallback, useEffect, useMemo, useRef, useState, lazy, Suspense, type CSSProperties } from 'react';
import { useSearchParams } from 'react-router-dom';
import {
  Camera, Pencil, MousePointer2, TrendingUp, Minus, Square, Type, Trash2,
  MoveUpRight, ArrowUpRight, Brush, Circle, AlignJustify, Magnet, Eye, EyeOff, Lock, LockOpen,
} from 'lucide-react';
import LwChart, { monthsYearsTickFmt, type LwSeries, type LwDrawing, type LwDrawTool, type LwDash } from '../../components/LwChart';
import { useTheme } from '../../contexts/ThemeContext';
import { getChartData, getInstrument, listAlerts, type AlertInfo } from '../../services/api';
import CreateAlertModal, { type AlertMetricOption } from '../../components/alerts/CreateAlertModal';
import { displayTicker } from '../../utils/displayTicker';
import { formatNumber, formatPrice } from '../../utils/formatNumber';
import { EmbedMsg } from './embedUi';
import { DrawerSection, ToggleRow } from './EmbedSettings';
import { FormatSection, applyFormat, useSeriesFormats, OHLC_KINDS } from './EmbedFormat';
import { EmbedFrame, AssetButton, Dropdown, PillGroup, WheelHint } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';

// Компактные лейблы таймфрейма для тулбар-выпадашки (§OI-7: одна кнопка-dropdown).
// Экспорт графика (скрин → превью → рисование → скачать/копировать) — переиспользуем
// движко-агностичный ExportModal сайта на контейнере LwChart. Ленивый чанк (html2canvas
// грузится только при первом открытии).
const ExportModal = lazy(() => import('../../components/export/ExportModal'));

const TF_COMPACT: { id: number; label: string }[] = [
  { id: 5, label: '5 мин' },
  { id: 60, label: '1 час' },
  { id: 24, label: '1 день' },
];

type ChartData = Awaited<ReturnType<typeof getChartData>>;
type OiPoint = ChartData['open_interest'][number];
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type ClGroup = 'FIZ' | 'YUR';
// Режим/вариант ОИ — зеркалит OpenInterestPage, но без 'price' (embed всегда
// показывает серию ОИ; price-only режим заменён тумблером «Цена»).
type DisplayMode = 'positions' | 'participants';
type OIVariant = 'oi' | 'long' | 'short' | 'both' | 'net';

// value = close; open/high/low опциональны (нужны только режимам свечи/бары цены).
type Series = { time: string; value: number; open?: number; high?: number; low?: number; close?: number }[];

// Опции тулбар-выпадашек §OI-2 (группа участников / режим) — перенесены из drawer.
const CLGROUP_OPTS: { id: ClGroup; label: string }[] = [
  { id: 'FIZ', label: 'Физлица' },
  { id: 'YUR', label: 'Юрлица' },
];
const MODE_OPTS: { id: DisplayMode; label: string }[] = [
  { id: 'positions', label: 'Объём позиций' },
  { id: 'participants', label: 'Число трейдеров' },
];

// Инструменты рисования (модель TradingView) — левая панель. rot — поворот иконки (vline).
const DRAW_TOOLS: { id: LwDrawTool; title: string; Icon: typeof MousePointer2; rot?: number }[] = [
  { id: 'select', title: 'Выделение / перемещение', Icon: MousePointer2 },
  { id: 'trend', title: 'Трендовая линия', Icon: TrendingUp },
  { id: 'ray', title: 'Луч', Icon: MoveUpRight },
  { id: 'arrow', title: 'Стрелка', Icon: ArrowUpRight },
  { id: 'hline', title: 'Горизонтальная линия', Icon: Minus },
  { id: 'vline', title: 'Вертикальная линия', Icon: Minus, rot: 90 },
  { id: 'rect', title: 'Прямоугольник', Icon: Square },
  { id: 'ellipse', title: 'Эллипс', Icon: Circle },
  { id: 'fib', title: 'Фибоначчи', Icon: AlignJustify },
  { id: 'brush', title: 'Кисть', Icon: Brush },
  { id: 'text', title: 'Текст', Icon: Type },
];
const DRAW_COLORS = ['#FF5C2B', '#5DA3E9', '#5BD49C', '#EF6F6F', '#E0A34E', '#F5F1E8'];
function drawToolBtn(active: boolean): CSSProperties {
  return {
    width: 30, height: 30, display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
    border: 'none', borderRadius: 7, cursor: 'pointer', padding: 0,
    background: active ? 'color-mix(in srgb, var(--accent) 20%, transparent)' : 'transparent',
    color: active ? 'var(--accent)' : 'var(--text-secondary)',
  };
}

// Единый монолитный график: грузим МАКС историю (дневной — всю; интрадей — месяц),
// а по времени юзер зумит колесом (осевой зум SimpleChart). Дискретных периодов нет.
const loadPeriodFor = (interval: number): string => (interval === 24 ? 'all' : '1m');
// Время → UNIX-секунды для LwChart. Дневной ТФ: UTC-полночь по дате (чтобы не было
// сдвига даты из-за таймзоны); интрадей — полный timestamp.
const toSec = (t: string, intraday: boolean): number => {
  if (!intraday) {
    const [y, m, d] = t.slice(0, 10).split('-').map(Number);
    return Math.floor(Date.UTC(y, m - 1, d) / 1000);
  }
  return Math.floor(new Date(t).getTime() / 1000);
};

// Цвета ОИ — все через CSS-var (адаптируются к теме внутри iframe).
// accent — фирменный оранжевый Фрейма (#FF5C2B): дефолт «Чистой позиции» (Вадим,
// §OI-1). Юзер может перекрасить каждую линию через ⚙-Формат.
const OI_COLORS = {
  primary: 'var(--chart-line-1)',
  amber: 'var(--oi-amber)',
  green: 'var(--oi-green)',
  red: 'var(--oi-red)',
  cyan: 'var(--oi-cyan)',
  accent: 'var(--accent)',
};

const num = (v: number | null): number => v ?? 0;

// Подпись «ноги» величины ОИ (что на правой оси в текущем режиме) — для метрики
// уровня ОИ в алерте по «+» на правой оси. Зеркалит OpenInterestPage.OI_LEG_LABEL.
const OI_LEG_LABEL: Record<'net' | 'long' | 'short' | 'oi' | 'npart', string> = {
  net: 'чистая позиция', long: 'длинные позиции', short: 'короткие позиции',
  oi: 'открытый интерес', npart: 'число участников',
};

// Метрики алерта ОИ для CreateAlertModal: цена + УРОВЕНЬ ОИ (нога+группа, как на
// графике) + резкое движение физ/юр. oi_level вставляется динамически (oiAlertMetrics
// в компоненте) — отражает текущий вид правой оси, как «+» на сайте.
const OI_ALERT_METRICS: AlertMetricOption[] = [
  {
    key: 'price', label: 'Цена', indicator: 'price', metric: 'close', unit: '₽',
    ops: [
      { value: 'cross', label: 'Пересечение (в любую сторону)' },
      { value: 'cross_up', label: '↑ Пересечение (снизу вверх)' },
      { value: 'cross_down', label: '↓ Пересечение (сверху вниз)' },
    ],
    hint: 'Сработает, когда цена фьючерса пересечёт заданный уровень. У ликвидных контрактов проверка каждые несколько минут, у остальных — раз в день после закрытия.',
  },
  {
    key: 'move_fiz', label: 'Резкое движение позиции — физлица',
    indicator: 'oi_move', metric: 'atr', clgroup: 'FIZ', unit: '×', defaultThreshold: 3,
    ops: [{ value: 'gt', label: 'превысит' }],
    hint: 'Сработает, когда чистая позиция физлиц изменится за день во столько-то раз резче обычного (ATR за 14 дней). 2× — заметно, 3× — сильно, 5× — экстремально. Обновляется раз в день.',
  },
  {
    key: 'move_yur', label: 'Резкое движение позиции — юрлица',
    indicator: 'oi_move', metric: 'atr', clgroup: 'YUR', unit: '×', defaultThreshold: 3,
    ops: [{ value: 'gt', label: 'превысит' }],
    hint: 'Сработает, когда чистая позиция юрлиц изменится за день во столько-то раз резче обычного (ATR за 14 дней). Обновляется раз в день.',
  },
];

/** `initialInstrument` — стартовый актив от песочницы (спавн панели по клику на
 *  сигнале). Приоритет: проп → ?instrument= → localStorage. Дальше юзер меняет
 *  его сам, и панель живёт своей жизнью. */
export default function EmbedOpenInterest({ initialInstrument }: { initialInstrument?: string } = {}) {
  const { rd, wr } = useEmbedPersist();
  const [params] = useSearchParams();
  const { theme } = useTheme();
  const dark = theme !== 'editorial-light';

  const sf = useSeriesFormats('frame:embed:oi:fmts');   // §OI-5: формат на каждую линию
  const [instrument, setInstrument] = useState<string>(() =>
    initialInstrument || params.get('instrument') || rd('frame:embed:oi:instrument', 'SR'),
  );
  const [instrumentName, setInstrumentName] = useState<string>(params.get('name') || '');
  // Актуальный фьючерсный контракт (напр. 'SRZ5') для показа в кнопке актива —
  // instrument хранит КОРЕНЬ ('SR'), а видеть надо последний активный контракт.
  const [frontContract, setFrontContract] = useState<string | null>(null);
  const [exportOpen, setExportOpen] = useState(false);   // модалка экспорта графика
  // Рисование (модель TradingView): карандаш → режим, фигуры персистят per-инструмент.
  const [drawMode, setDrawMode] = useState(false);
  const [drawTool, setDrawTool] = useState<LwDrawTool>('select');
  const [drawColor, setDrawColor] = useState('#FF5C2B');
  const [drawings, setDrawings] = useState<LwDrawing[]>([]);
  const [selectedDrawId, setSelectedDrawId] = useState<string | null>(null);
  const [drawMagnet, setDrawMagnet] = useState(false);   // привязка к бару/цене
  const [drawHidden, setDrawHidden] = useState(false);   // скрыть рисунки (глаз)
  const [drawLocked, setDrawLocked] = useState(false);   // замок (запрет перемещения)
  const [drawWidth, setDrawWidth] = useState(2);         // толщина по умолчанию
  const [drawDash, setDrawDash] = useState<LwDash>('solid');  // стиль линии по умолчанию
  const [drawOpacity, setDrawOpacity] = useState(1);     // прозрачность по умолчанию
  const drawSaveReady = useRef(false);   // пропустить первую запись (маунт) → не затереть сохранённое
  // Текущий стиль для тулбара свойств: выделенный элемент (если есть) или дефолты для новых.
  const selectedDraw = drawings.find((d) => d.id === selectedDrawId) || null;
  const curColor = selectedDraw?.color ?? drawColor;
  const curWidth = selectedDraw?.width ?? drawWidth;
  const curDash: LwDash = selectedDraw?.dash ?? drawDash;
  const curOpacity = selectedDraw?.opacity ?? drawOpacity;
  // Применить стиль: меняет дефолт (для новых фигур) И, если выделен элемент — патчит его.
  const applyStyle = (patch: Partial<Pick<LwDrawing, 'color' | 'width' | 'dash' | 'opacity'>>) => {
    if (patch.color !== undefined) setDrawColor(patch.color);
    if (patch.width !== undefined) setDrawWidth(patch.width);
    if (patch.dash !== undefined) setDrawDash(patch.dash);
    if (patch.opacity !== undefined) setDrawOpacity(patch.opacity);
    if (selectedDrawId) setDrawings((ds) => ds.map((d) => (d.id === selectedDrawId ? { ...d, ...patch } : d)));
  };
  const [clgroup, setClgroup] = useState<ClGroup>(() => rd('frame:embed:oi:clgroup', 'FIZ') as ClGroup);
  const [interval, setIntervalValue] = useState<number>(() => Number(rd('frame:embed:oi:interval', '24')) || 24);
  const [displayMode, setDisplayMode] = useState<DisplayMode>(() => rd('frame:embed:oi:displayMode', 'positions') as DisplayMode);
  const [oiVariant, setOiVariant] = useState<OIVariant>(() => rd('frame:embed:oi:oiVariant', 'net') as OIVariant);
  const [showPrice, setShowPrice] = useState<boolean>(() => rd('frame:embed:oi:showPrice', 'true') === 'true');
  const [showExpirations, setShowExpirations] = useState<boolean>(() => rd('frame:embed:oi:showExpirations', 'false') === 'true');

  const [data, setData] = useState<ChartData | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');

  // Алерты §OI-3 (как на сайте): «+» на осях графика → модалка с префиллом уровня.
  // Мои активные алерты этого актива → пунктир на графике (цена слева, ОИ справа).
  // Перезагружаем на смену актива и на закрытие модалки (создали → подтянуть уровень).
  const [myAlerts, setMyAlerts] = useState<AlertInfo[]>([]);
  const [chartAlertPrefill, setChartAlertPrefill] = useState<{ metricKey: string; threshold: number; currentLabel?: string } | null>(null);
  const reloadAlerts = useCallback(() => {
    listAlerts({ limit: 200 }).then((r) => setMyAlerts(r.items)).catch(() => { /* не критично для графика */ });
  }, []);
  useEffect(() => { reloadAlerts(); }, [instrument, reloadAlerts]);

  // Persist выбор.
  useEffect(() => { wr('frame:embed:oi:instrument', instrument); }, [instrument]);
  useEffect(() => { wr('frame:embed:oi:clgroup', clgroup); }, [clgroup]);
  useEffect(() => { wr('frame:embed:oi:interval', String(interval)); }, [interval]);
  useEffect(() => { wr('frame:embed:oi:displayMode', displayMode); }, [displayMode]);
  useEffect(() => { wr('frame:embed:oi:oiVariant', oiVariant); }, [oiVariant]);
  useEffect(() => { wr('frame:embed:oi:showPrice', String(showPrice)); }, [showPrice]);
  useEffect(() => { wr('frame:embed:oi:showExpirations', String(showExpirations)); }, [showExpirations]);

  const changeInterval = (next: number) => setIntervalValue(next);

  // Резолв имени, если пришёл только sec_id.
  useEffect(() => {
    if (instrumentName) return;
    let cancelled = false;
    getInstrument(instrument)
      .then((inst) => { if (!cancelled && inst?.name) setInstrumentName(inst.name); })
      .catch(() => { /* имя не критично */ });
    return () => { cancelled = true; };
  }, [instrument, instrumentName]);

  // Активный фьючерсный контракт — при любой смене инструмента (порт логики сайта,
  // OpenInterestPage): getInstrument отдаёт front_secid из календаря контрактов;
  // для спота/ошибки — null → в кнопке актива fallback на displayTicker(корень).
  useEffect(() => {
    let cancelled = false;
    getInstrument(instrument)
      .then((inst) => { if (!cancelled) setFrontContract(inst?.front_secid || null); })
      .catch(() => { /* контракт не критичен */ });
    return () => { cancelled = true; };
  }, [instrument]);

  // Рисунки — персист per-инструмент («под каждый актив», Вадим). Загрузка при смене
  // инструмента; сохранение при изменении (ключ берём из ref → без затирания при switch).
  const instrumentRef = useRef(instrument); instrumentRef.current = instrument;
  useEffect(() => {
    const raw = rd(`frame:embed:oi:draw:${instrument}`, '');
    try { setDrawings(raw ? (JSON.parse(raw) as LwDrawing[]) : []); } catch { setDrawings([]); }
    setSelectedDrawId(null);
  }, [instrument]);
  useEffect(() => {
    if (!drawSaveReady.current) { drawSaveReady.current = true; return; }  // пропуск маунта
    wr(`frame:embed:oi:draw:${instrumentRef.current}`, JSON.stringify(drawings));
  }, [drawings]);

  // Delete/Backspace в режиме рисования → удалить выделенную фигуру.
  useEffect(() => {
    if (!drawMode) return;
    const onKey = (e: KeyboardEvent) => {
      if ((e.key === 'Delete' || e.key === 'Backspace') && selectedDrawId) {
        setDrawings((ds) => ds.filter((d) => d.id !== selectedDrawId));
        setSelectedDrawId(null);
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [drawMode, selectedDrawId]);

  // Загрузка данных графика. show_oi=true всегда (в embed всегда есть серия ОИ).
  useEffect(() => {
    if (!instrument) { setStatus('empty'); return; }
    let cancelled = false;
    setStatus('loading');
    getChartData(instrument, instrument, 'futures', interval, clgroup, true, loadPeriodFor(interval))
      .then((res) => {
        if (cancelled) return;
        const hasData = (res?.candles?.length ?? 0) > 0;
        setData(res);
        setStatus(hasData ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('embed/oi load failed:', err);
        setStatus('error');
      });
    return () => { cancelled = true; };
  }, [instrument, clgroup, interval]);

  // Резиновая высота графика.
  const chartBoxRef = useRef<HTMLDivElement>(null);
  const [chartH, setChartH] = useState(280);
  useEffect(() => {
    const el = chartBoxRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const h = entries[0]?.contentRect.height;
      if (h && h > 0) setChartH(Math.round(h));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const chartData = useMemo<Series>(
    // value=close + полный OHLC (для режимов свечи/бары; линия/область берут value).
    () => (data?.candles ?? []).map((c) => ({ time: c.time, value: c.close, open: c.open, high: c.high, low: c.low, close: c.close })),
    [data],
  );

  // Выравнивание OI данных по временным меткам свечей (порт alignToCandles из
  // OpenInterestPage). OI имеет меньше точек, чем свечи → index-based X-mapping
  // в SimpleChart сдвигал бы серию во времени. Для каждой свечи берём последнее
  // известное значение OI (forward-fill). Дневной ключ — по дате (свечи
  // T00:00:00, OI T23:50:00); интрадей — по полному timestamp.
  const oiSeries = useMemo(() => {
    if (!data?.open_interest) {
      return { secondary: undefined as Series | undefined, third: undefined as Series | undefined };
    }
    const isPositions = displayMode === 'positions';
    const oi = data.open_interest;

    let secondary: Series | undefined;
    let third: Series | undefined;

    switch (oiVariant) {
      case 'oi':
        secondary = oi.map((o: OiPoint) => ({
          time: o.time,
          value: isPositions
            ? num(o.pos_long) + Math.abs(num(o.pos_short))
            : num(o.pos_long_num) + num(o.pos_short_num),
        }));
        break;
      case 'long':
        secondary = oi.map((o: OiPoint) => ({
          time: o.time,
          value: isPositions ? num(o.pos_long) : num(o.pos_long_num),
        }));
        break;
      case 'short':
        secondary = oi.map((o: OiPoint) => ({
          time: o.time,
          value: isPositions ? Math.abs(num(o.pos_short)) : num(o.pos_short_num),
        }));
        break;
      case 'both':
        secondary = oi.map((o: OiPoint) => ({
          time: o.time,
          value: isPositions ? num(o.pos_long) : num(o.pos_long_num),
        }));
        third = oi.map((o: OiPoint) => ({
          time: o.time,
          value: isPositions ? Math.abs(num(o.pos_short)) : num(o.pos_short_num),
        }));
        break;
      case 'net':
        secondary = oi.map((o: OiPoint) => ({
          time: o.time,
          value: isPositions
            ? (o.net_position ?? (num(o.pos_long) + num(o.pos_short)))
            : num(o.pos_long_num) - num(o.pos_short_num),
        }));
        break;
    }

    const isIntraday = interval !== 24;
    const align = (series: Series | undefined): Series | undefined => {
      if (!series || series.length === 0 || chartData.length === 0) return series;
      const map = new Map<string, number>();
      for (const p of series) {
        const key = isIntraday ? p.time : p.time.slice(0, 10);
        map.set(key, p.value);
      }
      const aligned: Series = [];
      let last: number | null = null;
      for (const candle of chartData) {
        const key = isIntraday ? candle.time : candle.time.slice(0, 10);
        const val = map.get(key);
        if (val !== undefined) last = val;
        if (last !== null) aligned.push({ time: candle.time, value: last });
      }
      return aligned.length > 0 ? aligned : series;
    };

    return { secondary: align(secondary), third: align(third) };
  }, [data, displayMode, oiVariant, interval, chartData]);

  // Цвета серии ОИ (зеркалит getColors). oi=amber, long=green, short=red,
  // net=cyan, both=green/red.
  const colors = useMemo(() => {
    switch (oiVariant) {
      case 'oi': return { secondary: OI_COLORS.amber, third: '' };
      case 'long': return { secondary: OI_COLORS.green, third: '' };
      case 'short': return { secondary: OI_COLORS.red, third: '' };
      case 'both': return { secondary: OI_COLORS.green, third: OI_COLORS.red };
      case 'net': return { secondary: OI_COLORS.accent, third: '' };
      default: return { secondary: OI_COLORS.amber, third: '' };
    }
  }, [oiVariant]);

  // Лейблы серии ОИ (зеркалит getLabels — зависят от displayMode).
  const labels = useMemo(() => {
    const isPositions = displayMode === 'positions';
    switch (oiVariant) {
      case 'oi': return { secondary: 'Открытый интерес', third: '' };
      case 'long': return { secondary: isPositions ? 'Покупки' : 'Покупатели', third: '' };
      case 'short': return { secondary: isPositions ? 'Продажи' : 'Продавцы', third: '' };
      case 'both': return {
        secondary: isPositions ? 'Покупки' : 'Покупатели',
        third: isPositions ? 'Продажи' : 'Продавцы',
      };
      case 'net': return { secondary: 'Чистая позиция', third: '' };
      default: return { secondary: '', third: '' };
    }
  }, [displayMode, oiVariant]);

  // Лейблы вариантов для drawer-сегментов (зависят от displayMode — выводим,
  // а не хардкодим, чтобы совпадало с labels выше).
  const variantOpts = useMemo(() => {
    const isPositions = displayMode === 'positions';
    return [
      { id: 'oi' as OIVariant, label: 'Открытый интерес' },
      { id: 'long' as OIVariant, label: isPositions ? 'Покупки' : 'Покупатели' },
      { id: 'short' as OIVariant, label: isPositions ? 'Продажи' : 'Продавцы' },
      { id: 'both' as OIVariant, label: isPositions ? 'Покупки + Продажи' : 'Покупатели + Продавцы' },
      { id: 'net' as OIVariant, label: 'Чистая позиция' },
    ];
  }, [displayMode]);

  // §OI-4: метки экспираций (смена контракта) — кружок с кодом контракта у оси дат
  // + hover-тултип (from→to), как на сайте. Данные из contract_switches.
  const expirations = useMemo(() => {
    if (!showExpirations) return [] as { time: number; label: string; description: string }[];
    const switches = data?.contract_switches;
    if (!switches || switches.length <= 1) return [];
    const intraday = interval !== 24;
    return switches.slice(1).map((sw) => ({
      time: toSec(sw.date, intraday),
      label: sw.to,
      description: `${sw.from} → ${sw.to}`,
    }));
  }, [data, showExpirations, interval]);

  // §OI-3: какая величина ОИ сейчас на ПРАВОЙ оси (нога) — определяет метрику «+»
  // алерта уровня ОИ и её подпись. Зеркалит OpenInterestPage.oiLeg.
  const oiLeg: 'net' | 'long' | 'short' | 'oi' | 'npart' = useMemo(() => {
    if (displayMode === 'participants') return 'npart';
    switch (oiVariant) {
      case 'long': return 'long';
      case 'short': return 'short';
      case 'oi': return 'oi';
      default: return 'net';   // net + both (both отключает уровневый алерт ниже)
    }
  }, [displayMode, oiVariant]);
  const oiLegLabel = OI_LEG_LABEL[oiLeg];
  const oiLegUnit = oiLeg === 'npart' ? 'участников' : 'контрактов';

  // Реестр метрик с динамическим oi_level (нога+группа = как на правой оси), как на
  // сайте: цена, УРОВЕНЬ ОИ, затем резкое движение физ/юр.
  const oiAlertMetrics = useMemo<AlertMetricOption[]>(() => {
    const oiLevel: AlertMetricOption = {
      key: 'oi_level', label: `Открытый интерес — ${oiLegLabel}`,
      indicator: 'oi_level', metric: oiLeg, clgroup, unit: '',
      ops: [
        { value: 'cross', label: 'Пересечение (в любую сторону)' },
        { value: 'cross_up', label: '↑ Пересечение (снизу вверх)' },
        { value: 'cross_down', label: '↓ Пересечение (сверху вниз)' },
      ],
      hint: `Сработает, когда «${oiLegLabel}» (${clgroup === 'FIZ' ? 'физлица' : 'юрлица'}) пересечёт заданный уровень. Величина — та, что показана на правой оси графика.`,
    };
    return [OI_ALERT_METRICS[0], oiLevel, ...OI_ALERT_METRICS.slice(1)];
  }, [clgroup, oiLeg, oiLegLabel]);

  // §OI-3: клик по «+» у оси → модалка с префиллом (метрика по оси + уровень +
  // «Сейчас: …»). Левая ось = цена (₽), правая = уровень ОИ (контракты/участники).
  const handleCreateAlertFromChart = (p: { axis: 'left' | 'right'; price: number; currentValue: number }) => {
    const isOi = p.axis === 'right';
    const currentLabel = isOi
      ? `${Math.round(p.currentValue).toLocaleString('ru-RU')} ${oiLegUnit}`
      : `${formatPrice(p.currentValue)} ₽`;
    setChartAlertPrefill({ metricKey: isOi ? 'oi_level' : 'price', threshold: p.price, currentLabel });
  };

  // §OI-3: на каких осях кликабельный «+». Вариант «both» (3 линии) — уровневый
  // алерт двусмыслен, выключаем целиком (как на сайте). Иначе: левая — если цена
  // показана; правая (ОИ) — всегда.
  const alertAxes = useMemo<('left' | 'right')[]>(() => {
    if (oiVariant === 'both') return [];
    const ax: ('left' | 'right')[] = [];
    if (showPrice) ax.push('left');
    ax.push('right');
    return ax;
  }, [oiVariant, showPrice]);

  // Активные алерты этого актива → пунктир: цена на ЛЕВОЙ оси, уровень ОИ на ПРАВОЙ.
  const alertLines = useMemo(() => {
    type Line = { price: number; color: string; scale: 'left' | 'right'; title: string };
    const lines: Line[] = myAlerts
      .filter((a) => a.status === 'active' && a.asset === instrument)
      .flatMap((a): Line[] => {
        if (a.indicator === 'price') return [{ price: a.threshold, color: 'var(--accent)', scale: 'left', title: 'алерт' }];
        if (a.indicator === 'oi_level') return [{ price: a.threshold, color: 'var(--accent)', scale: 'right', title: 'алерт' }];
        return [];
      });
    return lines.length ? lines : undefined;
  }, [myAlerts, instrument]);

  const displayName = instrumentName || displayTicker(instrument);

  // Метаданные для шапки экспортируемого PNG (composeFramedCanvas): актив/тикер/детали.
  const exportMeta = useMemo(() => ({
    title: 'Открытый интерес',
    asset: displayName,
    ticker: frontContract || instrument,
    details: [
      TF_COMPACT.find((t) => t.id === interval)?.label,
      clgroup === 'FIZ' ? 'Физлица' : 'Юрлица',
      labels.secondary,
    ].filter((x): x is string => !!x),
  }), [displayName, frontContract, instrument, interval, clgroup, labels.secondary]);

  // Серии для LwChart: цена (линия, левая ось) + показатель ОИ (area/линии, правая ось).
  const lwSeries = useMemo<LwSeries[]>(() => {
    const intraday = interval !== 24;
    const out: LwSeries[] = [];
    // §OI-5: каждая линия окна — со своим форматом (тип/цвет) из sf.get(id).
    if (showPrice && chartData.length > 0) {
      // §R2-15: шаг цены под магнитуду инструмента (зеркалит formatPrice). Дефолт
      // minMove=1 ломал ось у валютных фьючерсов (ED ~1.14: нет целых тиков в
      // диапазоне → левая ось пустая). ≥100 → 1, ≥10 → 0.01, иначе 0.0001.
      const lastPx = Math.abs(chartData[chartData.length - 1].value);
      const pxMinMove = lastPx >= 100 ? 1 : lastPx >= 10 ? 0.01 : 0.0001;
      out.push(applyFormat({
        id: 'price', type: 'line', scale: 'left', color: OI_COLORS.primary, lineWidth: 2, label: displayName, minMove: pxMinMove,
        // OHLC пробрасываем — режимы «Свечи»/«Бары» рисуют по ним; линия/область берут value.
        data: chartData.map((p) => ({ time: toSec(p.time, intraday), value: p.value, open: p.open, high: p.high, low: p.low, close: p.close })),
        tipFmt: (v) => formatPrice(v), axisFmt: (v) => formatPrice(v),
      }, sf.get('price')));
    }
    if (oiSeries.secondary && oiSeries.secondary.length > 0) {
      if (oiVariant === 'both') {
        out.push(applyFormat({
          id: 'oi-long', type: 'line', scale: 'right', color: colors.secondary, lineWidth: 2, label: labels.secondary,
          data: oiSeries.secondary.map((p) => ({ time: toSec(p.time, intraday), value: p.value })),
          tipFmt: (v) => formatNumber(v, 0), axisFmt: (v) => formatNumber(v, 0),
        }, sf.get('oi-long')));
        if (oiSeries.third) {
          out.push(applyFormat({
            id: 'oi-short', type: 'line', scale: 'right', color: colors.third, lineWidth: 2, label: labels.third,
            data: oiSeries.third.map((p) => ({ time: toSec(p.time, intraday), value: p.value })),
            tipFmt: (v) => formatNumber(v, 0), axisFmt: (v) => formatNumber(v, 0),
          }, sf.get('oi-short')));
        }
      } else {
        out.push(applyFormat({
          id: 'oi', type: 'line', scale: 'right', color: colors.secondary, lineWidth: 2, label: labels.secondary,
          zeroLine: oiVariant === 'net',
          data: oiSeries.secondary.map((p) => ({ time: toSec(p.time, intraday), value: p.value })),
          tipFmt: (v) => formatNumber(v, 0), axisFmt: (v) => formatNumber(v, 0),
        }, sf.get('oi')));
      }
    }
    return out;
  }, [chartData, oiSeries, oiVariant, colors, labels, showPrice, displayName, interval, sf.get]);

  return (
    <EmbedFrame
      lead={
        <AssetButton
          ticker={frontContract || displayTicker(instrument)}
          filterType="futures"
          hideLowActivity
          current={instrument}
          onSelect={(secid, name) => { setInstrument(secid); setInstrumentName(name); }}
        />
      }
      toolbar={
        <>
          {/* ТФ — компактный дропдаун (тулбар был слишком широк с пилюлями). Физ/Юр —
              горизонтальные пилюли (2 пункта). Вид графика убран из тулбара → настраивается
              per-линия в ⚙ Формат (цена/покупки-продажи/ОИ — по тому, что на графике). */}
          <Dropdown value={interval} options={TF_COMPACT} onChange={changeInterval} title="Таймфрейм" />
          <PillGroup value={clgroup} options={CLGROUP_OPTS} onChange={setClgroup} />
          <Dropdown value={displayMode} options={MODE_OPTS} onChange={setDisplayMode} title="Режим" />
          <Dropdown value={oiVariant} options={variantOpts} onChange={setOiVariant} title="Показатель ОИ" />
        </>
      }
      actions={
        status === 'ok' && data && lwSeries.length > 0 ? (
          <>
            <button
              type="button"
              onClick={() => { setDrawMode((v) => !v); setSelectedDrawId(null); }}
              title="Рисование на графике"
              aria-label="Рисование на графике"
              style={{
                width: 28, height: 28, display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                border: 'none', borderRadius: 7, cursor: 'pointer', flexShrink: 0, padding: 0,
                background: drawMode ? 'color-mix(in srgb, var(--accent) 16%, transparent)' : 'transparent',
                color: drawMode ? 'var(--accent)' : 'var(--text-secondary)',
              }}
            >
              <Pencil size={15} />
            </button>
            <button
              type="button"
              onClick={() => setExportOpen(true)}
              title="Экспорт графика"
              aria-label="Экспорт графика"
              style={{
                width: 28, height: 28, display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                border: 'none', borderRadius: 7, background: 'transparent', color: 'var(--text-secondary)',
                cursor: 'pointer', flexShrink: 0, padding: 0,
              }}
            >
              <Camera size={15} />
            </button>
          </>
        ) : undefined
      }
      more={
        <>
          <DrawerSection label="Слои">
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              <ToggleRow label="Цена" checked={showPrice} onChange={setShowPrice} hint="Линия цены фьючерса" />
              <ToggleRow label="Экспирации" checked={showExpirations} onChange={setShowExpirations} hint="Метки смены контракта" />
            </div>
          </DrawerSection>
          {/* §OI-5: формат КАЖДОЙ линии окна отдельно (тип + цвет) — разбивка по тому, что на
              графике: Цена (линия/область/свечи/бары), линии ОИ (линия/область). Подписи ниже
              адаптивны: Чистая позиция / Открытый интерес / Покупки / Продажи (или Покупки+Продажи). */}
          {showPrice && (
            <FormatSection label="Цена" kinds={OHLC_KINDS} fmt={sf.get('price')} onKind={(k) => sf.setKind('price', k)} onColor={(c) => sf.setColor('price', c)} />
          )}
          {oiVariant === 'both' ? (
            <>
              <FormatSection label={labels.secondary} fmt={sf.get('oi-long')} onKind={(k) => sf.setKind('oi-long', k)} onColor={(c) => sf.setColor('oi-long', c)} />
              <FormatSection label={labels.third} fmt={sf.get('oi-short')} onKind={(k) => sf.setKind('oi-short', k)} onColor={(c) => sf.setColor('oi-short', c)} />
            </>
          ) : (
            <FormatSection label={labels.secondary} fmt={sf.get('oi')} onKind={(k) => sf.setKind('oi', k)} onColor={(c) => sf.setColor('oi', c)} />
          )}
          <WheelHint>
            <b>Алерт</b> — наведи на ценовую ось (слева) или ось ОИ (справа) и нажми
            оранжевый <b>＋</b> на нужном уровне — как на сайте.
          </WheelHint>
        </>
      }
    >
      <div ref={chartBoxRef} style={{ position: 'absolute', inset: 0 }}>
        {status === 'ok' && data && lwSeries.length > 0 && (
          <LwChart
            series={lwSeries}
            expirations={expirations}
            height={chartH}
            dark={dark}
            fitKey={`${instrument}|${interval}`}
            initialBars={interval === 24 ? 252 : 220}
            tickFmt={interval === 24 ? monthsYearsTickFmt : undefined}
            timeVisible={interval !== 24}
            priceLines={alertLines}
            onCreateAlert={handleCreateAlertFromChart}
            alertAxes={alertAxes}
            animate
            drawActive={drawMode}
            drawTool={drawTool}
            drawings={drawings}
            onDrawingsChange={setDrawings}
            drawColor={drawColor}
            drawWidth={drawWidth}
            drawDash={drawDash}
            drawOpacity={drawOpacity}
            selectedDrawId={selectedDrawId}
            onSelectDraw={setSelectedDrawId}
            onToolReset={() => setDrawTool('select')}
            drawMagnet={drawMagnet}
            drawHidden={drawHidden}
            drawLocked={drawLocked}
          />
        )}
        {/* Тулбар свойств (модель TradingView / скрины Вадима): стиль линии · толщина ·
            цвет · прозрачность · удалить. Редактирует ВЫДЕЛЕННЫЙ элемент, иначе — дефолт
            для новых. Горизонтальный, сверху по центру. data-export-ignore. */}
        {drawMode && status === 'ok' && data && lwSeries.length > 0 && (
          <div
            data-export-ignore="true"
            style={{
              position: 'absolute', top: 6, left: '50%', transform: 'translateX(-50%)', zIndex: 9,
              display: 'flex', alignItems: 'center', gap: 3, padding: '4px 7px', borderRadius: 9,
              maxWidth: 'calc(100% - 90px)', flexWrap: 'wrap', justifyContent: 'center',
              background: 'color-mix(in srgb, var(--bg-secondary, #17161A) 92%, transparent)',
              border: '1px solid var(--border-color, rgba(128,128,128,0.35))', backdropFilter: 'blur(3px)',
              boxShadow: '0 6px 20px rgba(0,0,0,0.35)',
            }}
          >
            {(['solid', 'dashed', 'dotted'] as LwDash[]).map((id) => (
              <button key={id} type="button" title={id === 'solid' ? 'Сплошная' : id === 'dashed' ? 'Штриховой пунктир' : 'Точечный пунктир'} onClick={() => applyStyle({ dash: id })} style={drawToolBtn(curDash === id)}>
                <svg width={18} height={12} style={{ display: 'block' }}><line x1={1} y1={6} x2={17} y2={6} stroke="currentColor" strokeWidth={2} strokeDasharray={id === 'solid' ? undefined : id === 'dashed' ? '4 3' : '0.5 3'} strokeLinecap={id === 'dotted' ? 'round' : 'butt'} /></svg>
              </button>
            ))}
            <div style={{ width: 1, height: 18, background: 'var(--border-color,rgba(128,128,128,0.3))', margin: '0 2px' }} />
            {[1, 2, 3, 4].map((wv) => (
              <button key={wv} type="button" title={`${wv}px`} onClick={() => applyStyle({ width: wv })} style={{ ...drawToolBtn(curWidth === wv), width: 24, fontSize: 11, fontWeight: 700 }}>{wv}</button>
            ))}
            <div style={{ width: 1, height: 18, background: 'var(--border-color,rgba(128,128,128,0.3))', margin: '0 2px' }} />
            {DRAW_COLORS.map((c) => (
              <button key={c} type="button" title="Цвет" onClick={() => applyStyle({ color: c })} style={{ width: 20, height: 20, borderRadius: 5, cursor: 'pointer', padding: 0, display: 'inline-flex', alignItems: 'center', justifyContent: 'center', border: curColor === c ? '2px solid var(--text-primary)' : '1px solid transparent' }}>
                <span style={{ width: 12, height: 12, borderRadius: '50%', background: c, display: 'inline-block' }} />
              </button>
            ))}
            <div style={{ width: 1, height: 18, background: 'var(--border-color,rgba(128,128,128,0.3))', margin: '0 2px' }} />
            <input type="range" min={10} max={100} value={Math.round(curOpacity * 100)} title="Прозрачность" onChange={(e) => applyStyle({ opacity: Number(e.target.value) / 100 })} style={{ width: 66, accentColor: 'var(--accent)' }} />
            <span style={{ fontSize: 10, color: 'var(--text-secondary)', minWidth: 30, textAlign: 'right' }}>{Math.round(curOpacity * 100)}%</span>
            {selectedDrawId && (
              <>
                <div style={{ width: 1, height: 18, background: 'var(--border-color,rgba(128,128,128,0.3))', margin: '0 2px' }} />
                <button type="button" title="Удалить выделенное" onClick={() => { setDrawings((ds) => ds.filter((d) => d.id !== selectedDrawId)); setSelectedDrawId(null); }} style={drawToolBtn(false)}><Trash2 size={15} /></button>
              </>
            )}
          </div>
        )}
        {/* Панель инструментов рисования слева (модель TradingView) — только в режиме
            карандаша. data-export-ignore → не попадает в снимок графика. */}
        {drawMode && status === 'ok' && data && lwSeries.length > 0 && (
          <div
            data-export-ignore="true"
            style={{
              position: 'absolute', left: 6, top: '50%', transform: 'translateY(-50%)', zIndex: 8,
              display: 'flex', flexDirection: 'column', gap: 3, padding: 4, borderRadius: 10,
              background: 'color-mix(in srgb, var(--bg-secondary, #17161A) 88%, transparent)',
              border: '1px solid var(--border-color, rgba(128,128,128,0.35))', backdropFilter: 'blur(3px)',
              boxShadow: '0 6px 20px rgba(0,0,0,0.35)',
              maxHeight: 'calc(100% - 16px)', overflowY: 'auto',
            }}
          >
            {DRAW_TOOLS.map((t) => (
              <button
                key={t.id}
                type="button"
                title={t.title}
                aria-label={t.title}
                onClick={() => setDrawTool(t.id)}
                style={drawToolBtn(drawTool === t.id)}
              >
                <t.Icon size={16} style={t.rot ? { transform: `rotate(${t.rot}deg)` } : undefined} />
              </button>
            ))}
            {/* Цвет/стиль/толщина/прозрачность — в горизонтальном тулбаре свойств сверху. */}
            <div style={{ height: 1, background: 'var(--border-color, rgba(128,128,128,0.3))', margin: '2px 3px' }} />
            {/* Утилиты: магнит / скрыть / замок */}
            <button type="button" title="Магнит: привязка к бару и цене" aria-label="Магнит" onClick={() => setDrawMagnet((v) => !v)} style={drawToolBtn(drawMagnet)}>
              <Magnet size={16} />
            </button>
            <button type="button" title={drawHidden ? 'Показать рисунки' : 'Скрыть рисунки'} aria-label="Скрыть рисунки" onClick={() => setDrawHidden((v) => !v)} style={drawToolBtn(drawHidden)}>
              {drawHidden ? <EyeOff size={16} /> : <Eye size={16} />}
            </button>
            <button type="button" title={drawLocked ? 'Разблокировать рисунки' : 'Заблокировать (запрет перемещения)'} aria-label="Замок" onClick={() => setDrawLocked((v) => !v)} style={drawToolBtn(drawLocked)}>
              {drawLocked ? <Lock size={16} /> : <LockOpen size={16} />}
            </button>
            <div style={{ height: 1, background: 'var(--border-color, rgba(128,128,128,0.3))', margin: '2px 3px' }} />
            <button
              type="button"
              title={selectedDrawId ? 'Удалить выделенное' : 'Очистить всё'}
              aria-label="Удалить"
              onClick={() => {
                if (selectedDrawId) { setDrawings((ds) => ds.filter((d) => d.id !== selectedDrawId)); setSelectedDrawId(null); }
                else if (drawings.length && window.confirm('Удалить все рисунки?')) setDrawings([]);
              }}
              style={drawToolBtn(false)}
            >
              <Trash2 size={16} />
            </button>
          </div>
        )}
        {/* Модалка экспорта (триггер 📷 — в тулбаре рядом с ⚙, см. actions). Снимает
            контейнер LwChart → превью → рисование → PNG/буфер. Portal → место в дереве неважно. */}
        {exportOpen && chartBoxRef.current && (
          <Suspense fallback={null}>
            <ExportModal
              targetElement={chartBoxRef.current}
              filename={`frame-oi-${instrument}-${interval}`}
              metadata={exportMeta}
              onClose={() => setExportOpen(false)}
            />
          </Suspense>
        )}
        {/* Цена выключена + у контракта нет OI-данных → серий нет. Без этого был
            пустой холст без объяснения (аудит). */}
        {status === 'ok' && data && lwSeries.length === 0 && (
          <EmbedMsg text="Нет данных для отображения" />
        )}
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && (
          <EmbedMsg text={instrument ? 'Нет данных по этому инструменту' : 'Инструмент не выбран'} />
        )}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
        {chartAlertPrefill && (
          <CreateAlertModal
            indicator="open_interest"
            asset={instrument}
            assetName={displayName}
            metrics={oiAlertMetrics}
            prefill={chartAlertPrefill}
            onClose={() => { setChartAlertPrefill(null); reloadAlerts(); }}
          />
        )}
      </div>
    </EmbedFrame>
  );
}
