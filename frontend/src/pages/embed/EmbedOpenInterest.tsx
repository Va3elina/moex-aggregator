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
import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Clock, User, Building2, BarChart3, Users } from 'lucide-react';
import { monthsYearsTickFmt, type LwSeries } from '../../components/chart/lwTypes';
import LwChartPanes, { type LwChartPanesHandle, type LwPane } from '../../components/LwChartPanes';
import { useTheme } from '../../contexts/ThemeContext';
import { getChartData, getInstrument, listAlerts, type AlertInfo } from '../../services/api';
import CreateAlertModal, { type AlertMetricOption } from '../../components/alerts/CreateAlertModal';
import { displayTicker } from '../../utils/displayTicker';
import { formatNumber, formatPrice } from '../../utils/formatNumber';
import { useChartRealtimeDelta } from '../../hooks/useChartRealtimeDelta';
import { useChartWindowLoader } from './useChartWindowLoader';
import { EmbedMsg } from './embedUi';
import { DrawerSection, ToggleRow } from './EmbedSettings';
import { FormatSection, applyFormat, useSeriesFormats, OHLC_KINDS } from './EmbedFormat';
import { EmbedFrame, AssetButton, Dropdown } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';
import { useToolbarCompact } from './useToolbarCompact';
import { useTierAccess } from '../../contexts/TierFeaturesContext';
import { useDrawTools, DrawExportActions, DrawToolsOverlay, ChartExportModal } from './useDrawTools';
import { useIndicators, useIndicatorSeries, useVolumeProfileSpec, indicatorValues, IndicatorList, PaneIndicatorList, IndicatorsButton, type NativeRow, type BasisOption } from './EmbedIndicators';

// Компактные лейблы таймфрейма для тулбар-выпадашки (§OI-7: одна кнопка-dropdown).
const TF_COMPACT: { id: number; label: string }[] = [
  { id: 5, label: '5 мин' },
  { id: 60, label: '1 час' },
  { id: 24, label: '1 день' },
];

/**
 * Сокращения ТФ для узкой панели: единственный контрол тулбара, чьё значение
 * надо видеть НЕ открывая список (как «5м» на кнопке в TradingView) — срез
 * (участники/режим/показатель) читается прямо по графику, а таймфрейм нет.
 * Схлопывать до иконки-часов, как остальные, здесь нельзя.
 */
const TF_SHORT: Record<number, string> = { 5: '5м', 60: '1ч', 24: '1Д' };

type ChartData = Awaited<ReturnType<typeof getChartData>>;
type OiPoint = ChartData['open_interest'][number];
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type ClGroup = 'FIZ' | 'YUR';
// Режим/вариант ОИ — зеркалит OpenInterestPage, но без 'price' (embed всегда
// показывает серию ОИ; price-only режим заменён тумблером «Цена»).
type DisplayMode = 'positions' | 'participants';
type OIVariant = 'oi' | 'long' | 'short' | 'both' | 'net';

// value = close; open/high/low опциональны (нужны только режимам свечи/бары цены).
type Series = { time: string; value: number; open?: number; high?: number; low?: number; close?: number; volume?: number }[];

/** Стабильная ссылка на пустой ряд: новый литерал на каждый рендер сбрасывал бы
 *  мемоизацию потребителей (профиль объёма пересчитывался бы впустую). */
const EMPTY_CANDLES: Series = [];

// Срез ОИ — три выпадашки тулбара в одном ряду: КТО (группа участников), КАК
// (режим: объём против числа трейдеров) и ЧТО (показатель: нога ОИ). Все три —
// Dropdown с раскрытием ВНИЗ, а не пилюли и не подменю строки: они меняются
// одинаково часто и должны выглядеть и вести себя одинаково (Вадим, 2026-08-15).
const CLGROUP_OPTS: { id: ClGroup; label: string }[] = [
  { id: 'FIZ', label: 'Физлица' },
  { id: 'YUR', label: 'Юрлица' },
];
const CLGROUP_ICON = (v: ClGroup): ReactNode =>
  v === 'FIZ' ? <User size={14} /> : <Building2 size={14} />;
const MODE_OPTS: { id: DisplayMode; label: string }[] = [
  { id: 'positions', label: 'Объём позиций' },
  { id: 'participants', label: 'Число трейдеров' },
];
const MODE_ICON = (v: DisplayMode): ReactNode =>
  v === 'positions' ? <BarChart3 size={14} /> : <Users size={14} />;

/** Цветная точка показателя — иконка дропдауна «Показатель». На узкой панели
 *  лейбл схлопывается, и точка остаётся единственным признаком выбранной ноги,
 *  поэтому цвет берём ФАКТИЧЕСКИЙ (с учётом перекраски линии в ⚙ Формат). */
const Dot = ({ color }: { color: string }) => (
  <span style={{ width: 8, height: 8, borderRadius: '50%', background: color, display: 'inline-block', flexShrink: 0 }} />
);

// Единый монолитный график: грузим МАКС историю (дневной — всю; интрадей — месяц),
// а по времени юзер зумит колесом (осевой зум SimpleChart). Дискретных периодов нет.
// Дневной ТФ раньше был жёстко '1y' — обрезал историю ровно годом для ВСЕХ
// тарифов, включая admin/pro (у которых max_history_days вообще не ограничен).
// Бэкенд (enforce_tier_limits) HARD-REJECT'ит period, если он больше разрешённого
// тарифом — поэтому здесь нельзя слепо слать 'all': guest/free/basic вместо
// урезанного-но-рабочего графика получили бы голую ошибку. Выбираем САМЫЙ
// длинный период, который canUsePeriod разрешает текущему тарифу.
const DAILY_PERIOD_CANDIDATES = ['all', '5y', '2y', '1y', '6m', '3m', '1m'] as const;
function bestDailyPeriod(canUsePeriod: (p: string) => boolean): string {
  for (const p of DAILY_PERIOD_CANDIDATES) {
    if (canUsePeriod(p)) return p;
  }
  return '1m';
}
// Время → UNIX-секунды для LwChart. Дневной ТФ: UTC-полночь по дате (чтобы не было
// сдвига даты из-за таймзоны); интрадей — часы/минуты тоже собираются через Date.UTC.
// ВАЖНО: бэкенд отдаёт НАИВНУЮ московскую строку (datetime.isoformat() без зоны), а
// lightweight-charts рисует ось и кросхейр в UTC (ruTickMark → getUTCHours). Прежний
// new Date(t) парсил строку в зоне БРАУЗЕРА, поэтому у московского юзера весь интрадей
// уезжал на −3ч: последняя 5-минутка 10:45 показывалась как 07:45 и график выглядел
// «застрявшим». Покомпонентный Date.UTC отображает ровно то время, что пришло с бэка.
const toSec = (t: string, intraday: boolean): number => {
  const [y, m, d] = t.slice(0, 10).split('-').map(Number);
  if (!intraday) return Math.floor(Date.UTC(y, m - 1, d) / 1000);
  const [hh, mi, ss] = t.slice(11, 19).split(':').map(Number);
  return Math.floor(Date.UTC(y, m - 1, d, hh || 0, mi || 0, ss || 0) / 1000);
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
  const oiAccess = useTierAccess('open_interest');

  const sf = useSeriesFormats('frame:embed:oi:fmts');   // §OI-5: формат на каждую линию
  const [instrument, setInstrument] = useState<string>(() =>
    initialInstrument || params.get('instrument') || rd('frame:embed:oi:instrument', 'SR'),
  );
  const [instrumentName, setInstrumentName] = useState<string>(params.get('name') || '');
  // Актуальный фьючерсный контракт (напр. 'SRZ5') для показа в кнопке актива —
  // instrument хранит КОРЕНЬ ('SR'), а видеть надо последний активный контракт.
  const [frontContract, setFrontContract] = useState<string | null>(null);
  // Рисование + экспорт — общий хук useDrawTools (он же у Баффетта/Силы/Фондов).
  // Раньше ОИ держал полную копию этого стейта и всей UI рисования у себя; копия
  // разъезжалась с хуком (правки доезжали до трёх индикаторов из четырёх), поэтому
  // переведён на общий. persistKey — per-инструмент, как и было («рисунки под
  // каждый актив»): смена instrument перезагружает фигуры внутри хука.
  const draw = useDrawTools(`frame:embed:oi:draw:${instrument}`);
  // Пользовательские индикаторы (наложения поверх графика). Ключ БЕЗ инструмента:
  // набор индикаторов — это про предпочтения пользователя, а не про актив, и при
  // переключении SR→GAZP он должен остаться.
  // Панель линий ОИ: 0 — на графике цены (правая ось), 1+ — своя панель снизу.
  // Номер общий с индикаторами, поэтому уходит к ним в reserved, а перестановку
  // «Выше/Ниже» обе стороны применяют одну и ту же (см. useIndicators).
  const [oiPane, setOiPane] = useState<number>(() => Number(rd('frame:embed:oi:oiPane', '0')) || 0);
  const reservedPanes = useMemo(() => (oiPane > 0 ? [oiPane] : []), [oiPane]);
  const onSwapPanes = useCallback((a: number, b: number) => {
    setOiPane((p) => (p === a ? b : p === b ? a : p));
  }, []);
  const inds = useIndicators('frame:embed:oi:indicators', { reserved: reservedPanes, onSwapPanes });
  const [clgroup, setClgroup] = useState<ClGroup>(() => rd('frame:embed:oi:clgroup', 'FIZ') as ClGroup);
  const [interval, setIntervalValue] = useState<number>(() => Number(rd('frame:embed:oi:interval', '24')) || 24);
  const [displayMode, setDisplayMode] = useState<DisplayMode>(() => rd('frame:embed:oi:displayMode', 'positions') as DisplayMode);
  const [oiVariant, setOiVariant] = useState<OIVariant>(() => rd('frame:embed:oi:oiVariant', 'net') as OIVariant);
  const [showPrice, setShowPrice] = useState<boolean>(() => rd('frame:embed:oi:showPrice', 'true') === 'true');
  const [showExpirations, setShowExpirations] = useState<boolean>(() => rd('frame:embed:oi:showExpirations', 'false') === 'true');

  // Compact-режим тулбара — см. useToolbarCompact.ts (лейблы «1 день»/«Объём
  // позиций»/… не помещаются рядом с ассет-кнопкой → схлопываются в иконки).
  const { wrapRef: toolbarWrapRef, measureRef: toolbarMeasureRef, compact: toolbarCompact } = useToolbarCompact();

  const [data, setData] = useState<ChartData | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');

  // Алерты §OI-3 (как на сайте): «+» на осях графика → модалка с префиллом уровня.
  // Мои активные алерты этого актива → пунктир на графике (цена слева, ОИ справа).
  // Перезагружаем на смену актива и на закрытие модалки (создали → подтянуть уровень).
  const [myAlerts, setMyAlerts] = useState<AlertInfo[]>([]);
  const [chartAlertPrefill, setChartAlertPrefill] = useState<{ metricKey: string; threshold: number; currentLabel?: string } | null>(null);
  // Индекс панели ОИ в стеке. Через ref: обработчик «+» объявлен выше сборки
  // панелей, а тащить туда весь расчёт ради одного числа — лишний порядок.
  const oiChartIndexRef = useRef(0);
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
  useEffect(() => { wr('frame:embed:oi:oiPane', String(oiPane)); }, [oiPane]);

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

  // Реалтайм: ингест обновил данные (SSE '5min'/'hourly') → инкрементальный
  // догруз. Тянем ТОЛЬКО новые точки (килобайты) вместо полного ряда: на 5м/6м
  // это 6.2 МБ, и раньше их качала каждая открытая панель разом на каждое
  // событие. Если дельта не применима — тихий полный рефетч, как раньше
  // (тихий = без setStatus('loading'): панель не мигает, старый график висит
  // до прихода свежего). SSE-соединение одно на вкладку (синглтон в useSSE),
  // сколько бы панелей ни было открыто.
  const [refreshTick, setRefreshTick] = useState(0);
  const silentRef = useRef(false);
  useChartRealtimeDelta({
    data,
    onMerged: setData,
    onFallback: () => { silentRef.current = true; setRefreshTick((t) => t + 1); },
    controls: { sectype: instrument, interval, clgroup },
  });

  // Оконная подача: прошлое догружаем, когда пользователь долистал к началу.
  const { loadMore, reset: resetWindow } = useChartWindowLoader({
    instrument, interval, clgroup, data, onExtended: setData,
  });
  // Смена актива/ТФ/среза — окно считается заново (счётчик кусков, край).
  useEffect(() => { resetWindow(); }, [instrument, interval, clgroup, resetWindow]);
  // Запас истории тянем СРАЗУ после первой отрисовки, не дожидаясь прокрутки:
  // тогда к моменту, когда пользователь доведёт график до края, данные там уже
  // есть и перехода не видно вовсе. Стартовое окно при этом остаётся лёгким —
  // запас едет фоном и на первый показ не влияет.
  const prefetchedFor = useRef('');
  useEffect(() => {
    if (status !== 'ok' || !data?.candles?.length) return;
    // ⚠️ Ждём, пока data ДОГОНИТ контролы. При смене актива/ТФ статус ещё 'ok'
    // (рефетч тихий), а data — от прежнего инструмента: префетч уходил бы с
    // чужими датами, и ключ отмечался бы как уже сделанный, из-за чего запас
    // для нового инструмента не грузился вовсе.
    if (data.sectype !== instrument || data.interval !== interval || data.clgroup !== clgroup) return;
    const key = `${instrument}:${interval}:${clgroup}`;
    if (prefetchedFor.current === key) return;
    prefetchedFor.current = key;
    // ⚠️ С задержкой, а не сразу: догрузка истории приходит вторым куском и
    // пересобирает серии графика. Если это случается в тот же момент, когда
    // пользователь только что переключил таймфрейм и график появился, два
    // перестроения подряд читаются как дрожание вида (фидбек Вадима). Полсекунды
    // хватает, чтобы первый показ устоялся; на «долистал до края» это не влияет —
    // там loadMore зовёт сам график, без этой паузы.
    const t = window.setTimeout(() => loadMore(), 500);
    return () => window.clearTimeout(t);
  }, [status, data, instrument, interval, clgroup, loadMore]);

  // Загрузка данных графика. show_oi=true всегда (в embed всегда есть серия ОИ).
  useEffect(() => {
    if (!instrument) { setStatus('empty'); return; }
    // Пока тариф не разрешился — безопасный '1y' (прежнее поведение, никого не
    // ломает); как только oiAccess.isLoading станет false, эффект перезапустится
    // (он в deps) и для admin/pro/pro-эквивалентных тарифов подтянется вся история.
    // Интрадей-глубина фиксирована по ТФ и НЕ выводится из тарифа: у
    // open_interest max_history_days на всех тарифах ≥ 5 лет, так что и год
    // часовых, и полгода 5-минуток проходят проверку везде. Границы — по
    // фактическому объёму данных на проде (5м живёт с 2026-02, 60м с 2020).
    // Оконная подача (модель TradingView): стартуем с последнего окна, прошлое
    // догружаем прокруткой (useChartWindowLoader). 5м за полгода — это 26.7 тыс.
    // свечей и 6.2 МБ одним куском; окно в месяц — 4.4 тыс. и ~1 МБ, а глубина
    // при этом не теряется: долистал — доехало.
    const period = interval === 24
      ? (oiAccess.isLoading ? '1y' : bestDailyPeriod(oiAccess.canUsePeriod))
      : interval === 60 ? '6m' : '1m';
    let cancelled = false;
    if (!silentRef.current) setStatus('loading');
    silentRef.current = false;
    getChartData(instrument, instrument, 'futures', interval, clgroup, true, period)
      .then((res) => {
        if (cancelled) return;
        // ТФ персистится per-embed и не сбрасывается при смене актива (клик по
        // сигналу в скринере) — у неликвидных фьючерсов (EOD-only) внутридневного
        // ОИ нет вообще, а цена интрадей грузится всегда (ISS её отдаёт любому
        // активу) → без этой проверки ОИ-линия молча пропадала бы, показывая
        // только цену. available_intervals не зависит от запрошенного interval
        // (считается по sectype+clgroup) — значит он достоверен и для «плохого» ТФ.
        const avail = res?.available_intervals;
        if (avail && avail.length > 0 && !avail.includes(interval)) {
          setIntervalValue(avail.includes(24) ? 24 : avail[avail.length - 1]);
          return;
        }
        const hasData = (res?.candles?.length ?? 0) > 0;
        setData(res);
        setStatus(hasData ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        // Персистентный интрадей-ТФ может быть недоступен ТЕКУЩЕМУ тарифу
        // (Pro настроил 5м → гость на той же машине ловил вечную «Ошибку
        // загрузки», 403 «Таймфрейм недоступен на тарифе»). Откатываемся на
        // дневной — он открыт всем; available_intervals такой случай не
        // ловит (это про наличие данных, а не про тариф).
        if (interval !== 24 && /таймфрейм|тариф/i.test(String(err?.message ?? ''))) {
          setIntervalValue(24);
          return;
        }
        console.error('embed/oi load failed:', err);
        setStatus('error');
      });
    return () => { cancelled = true; };
  // oiAccess.canUsePeriod — НЕ мемоизированная функция (новый reference каждый
  // рендер) — в deps нельзя, будет бесконечный рефетч. tier/isLoading — обычные
  // примитивы, стабильны между рендерами и меняются ровно тогда, когда реально
  // должен пересчитаться разрешённый период.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [instrument, clgroup, interval, oiAccess.isLoading, oiAccess.tier, refreshTick]);

  // Список ТФ в дропдауне — только те, где у ЭТОГО актива реально есть OI-данные
  // (available_intervals из ответа /api/chart, независим от запрошенного interval).
  // У EOD-only фьючерсов (неликвидные мини-контракты вроде PXU6) интрадей-ОИ нет
  // вообще — не предлагаем 5м/1ч в списке, вместо того чтобы дать выбрать и потом
  // молча погасить линию (авто-коррекция interval — в эффекте загрузки выше).
  // Пока данные не пришли (первая загрузка) — показываем все три, не мигаем.
  const tfOptions = useMemo(() => {
    const avail = data?.available_intervals;
    if (!avail || avail.length === 0) return TF_COMPACT;
    const filtered = TF_COMPACT.filter((t) => avail.includes(t.id));
    return filtered.length > 0 ? filtered : TF_COMPACT;
  }, [data]);


  const chartData = useMemo<Series>(
    // value=close + полный OHLC (для режимов свечи/бары; линия/область берут value).
    // volume нужен индикатору «Объёмы» и не стоит ничего: он уже приходит в свече
    // (ChartResponse.candles), а раньше просто терялся при этом маппинге.
    () => (data?.candles ?? []).map((c) => ({ time: c.time, value: c.close, open: c.open, high: c.high, low: c.low, close: c.close, volume: c.volume })),
    [data],
  );

  // Контейнер графика (цель экспорта в PNG) + хэндл движка для форс-синка перед
  // снимком. Высота НЕ считается: LwChartPanes всегда занимает 100% родителя, а
  // родитель — <div position:absolute;inset:0>.
  const chartBoxRef = useRef<HTMLDivElement>(null);
  const panesRef = useRef<LwChartPanesHandle>(null);

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
  const handleCreateAlertFromChart = (p: { axis: 'left' | 'right'; pane: number; price: number; currentValue: number }) => {
    // ОИ = правая ось ТОЙ панели, где он сейчас живёт (он умеет уезжать вниз).
    const isOi = p.axis === 'right' && p.pane === oiChartIndexRef.current;
    const currentLabel = isOi
      ? `${Math.round(p.currentValue).toLocaleString('ru-RU')} ${oiLegUnit}`
      : `${formatPrice(p.currentValue)} ₽`;
    setChartAlertPrefill({ metricKey: isOi ? 'oi_level' : 'price', threshold: p.price, currentLabel });
  };

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
          data: oiSeries.secondary.map((p) => ({ time: toSec(p.time, intraday), value: p.value })),
          tipFmt: (v) => formatNumber(v, 0), axisFmt: (v) => formatNumber(v, 0),
        }, sf.get('oi')));
      }
    }
    return out;
  }, [chartData, oiSeries, oiVariant, colors, labels, showPrice, displayName, interval, sf.get]);

  // «Глаз» нативной серии: у цены это существующий тумблер showPrice (единственный
  // источник правды), у линий ОИ — поле visible в карте форматов.
  const visibleNative = useMemo(() => lwSeries.filter((d) => sf.get(d.id).visible !== false), [lwSeries, sf]);
  // Линии ОИ уезжают в свою панель целиком (их 1–2, и обе на одной шкале).
  const oiOwn = useMemo(() => visibleNative.filter((d) => d.id !== 'price'), [visibleNative]);
  const mainNative = useMemo(() => (oiPane > 0 ? visibleNative.filter((d) => d.id === 'price') : visibleNative), [visibleNative, oiPane]);
  // ⚠️ Если после переезда на основной панели не осталось НИЧЕГО (цена скрыта) —
  // возвращаем ОИ наверх: пустая ценовая панель это полоса пустоты во всю ширину
  // и график без единой линии.
  // Раньше в условие входила ещё и пустота indSeries[0] (нет наложений). Теперь
  // это НЕЛЬЗЯ: наложения по базису ОИ сами садятся в панель oiPaneEff, то есть
  // сборка серий зависит от него — считать его через результат сборки значило бы
  // замкнуть цикл. Проверки по цене достаточно: наложения по цене без самой цены
  // теперь тоже не строятся (базис пуст, см. indCandles ниже), так что «на
  // основной панели пусто» и «цена скрыта» — одно и то же условие.
  const oiPaneEff = mainNative.length === 0 ? 0 : oiPane;

  // Псевдо-свечи ряда ОИ — основание для индикаторов по базису 'oi'. У точки ОИ
  // одно значение, поэтому open=high=low=close=value, а volume отсутствует (его
  // у позиций нет — объёмы и профиль объёма на этом базисе и не предлагаются).
  //
  // В режиме «Покупки + Продажи» базиса ОИ НЕТ намеренно: там две равноправные
  // линии, и одна скользящая, посчитанная молча по одной из них, вводила бы в
  // заблуждение. UI в этом режиме ОИ как цель не предлагает.
  const oiCandles = useMemo<Series | null>(() => {
    const src = oiVariant === 'both' ? undefined : oiSeries.secondary;
    if (!src || src.length === 0) return null;
    return src.map((p) => ({ time: p.time, value: p.value, open: p.value, high: p.value, low: p.value, close: p.value }));
  }, [oiSeries.secondary, oiVariant]);

  // Индикаторы считаются от РЯДОВ ПО БАЗИСАМ. Ценовые кладутся на ЛЕВУЮ (ценовую)
  // ось, по ОИ — на правую, туда, где сейчас сам ОИ (oiPaneEff). Порядок важен:
  // наложения идут ПОСЛЕ нативных серий, чтобы первой в массиве осталась цена —
  // от неё берут отсчёт магнит и линейка слоя рисования.
  // Цена выключена → базис 'price' пуст → ценовые индикаторы не строятся вовсе
  // (тот же приём, что у профиля объёма ниже): рисовать MA цены, когда самой цены
  // на графике нет, значит показывать линию, к которой нечего приложить.
  const toSecFn = useCallback((t: string) => toSec(t, interval !== 24), [interval]);
  const indCandles = useMemo(
    () => ({ price: showPrice ? chartData : EMPTY_CANDLES, oi: oiCandles }),
    [showPrice, chartData, oiCandles],
  );
  const indSeries = useIndicatorSeries(inds.list, indCandles, toSecFn, inds.colorOf, 'left', oiPaneEff);
  // Профиль объёма — не серия, а примитив на ценовой серии (он гистограмма по
  // цене, а не по времени). Крепится к 'price': на линии ОИ он отрисовал бы
  // уровни по чужой шкале. Цена скрыта → серии нет → профиля тоже нет.
  const vpSpec = useVolumeProfileSpec(inds.list, showPrice ? chartData : EMPTY_CANDLES, 'price', inds.colorOf);
  const hasVolume = useMemo(() => chartData.some((p) => p.volume != null), [chartData]);
  // Базисы, доступные в этом окне. Цена — пока показана; ОИ — пока есть ряд.
  // Больше одного → при добавлении индикатора спрашиваем, к чему его применить.
  //
  // Подпись и цвет берём ИЗ САМИХ lwSeries, а не собираем заново: ряд в диалоге
  // должен выглядеть ровно так, как он подписан и покрашен в левом верхнем углу
  // графика (строка легенды nativeRows строится из того же массива). Собери
  // здесь свою пару displayName + sf.get('price').color — и она разойдётся с
  // легендой на первом же месте, где applyFormat решит иначе.
  //
  // Условия наличия базисов при этом НЕ меняются: цена попадает в lwSeries ровно
  // при `showPrice && chartData.length > 0`, а единый ряд ОИ с id 'oi' есть
  // только вне режима «Покупки + Продажи» — там же, где непуст oiCandles
  // (страховка `oiCandles &&` держит эти два условия связанными явно).
  const indBases = useMemo<BasisOption[]>(() => {
    const out: BasisOption[] = [];
    const px = lwSeries.find((d) => d.id === 'price');
    if (px) out.push({ id: 'price', label: px.label, color: px.color });
    const oi = oiCandles ? lwSeries.find((d) => d.id === 'oi') : undefined;
    if (oi) out.push({ id: 'oi', label: oi.label, color: oi.color });
    return out;
  }, [lwSeries, oiCandles]);
  // indSeries сгруппированы по панелям: [0] — наложения на основной график,
  // [1+] — индикаторы со своей шкалой (RSI/ATR/объёмы).
  const allSeries = useMemo(
    () => [...(oiPaneEff > 0 ? mainNative : visibleNative), ...(indSeries[0] ?? [])],
    [visibleNative, mainNative, oiPaneEff, indSeries],
  );
  // ⚠️ Панели держатся по СПИСКУ индикаторов, а не по наличию у них серий.
  // Скрытый «глазом» индикатор серий не даёт — если считать панели по сериям,
  // панель схлопнется вместе со строкой, и вернуть индикатор станет нечем. Ровно
  // тот же тупик, что был при удалении. Заодно это и есть соответствие «панель
  // графика → панель индикатора»: номера панелей не подряд (после удалений
  // остаются дыры), и без него строка искалась бы не в той панели.
  // ⚠️ Наложения по базису ОИ держат в состоянии pane === 0, а рисуются в панели
  // oiPaneEff (см. effectivePane в движке) — отдельного номера они сюда не
  // приносят: панель ОИ и так добавлена строкой ниже. Поэтому фильтр по
  // «своим» панелям остаётся прежним, и множество номеров получается ровно то
  // же, что у сборки серий.
  // ⚠️ Исключение из правила выше: у индикатора, чьего БАЗИСА в текущем режиме
  // нет вовсе (наложение/панель по ОИ, а выбран режим «Покупки + Продажи» —
  // единого ряда ОИ там не существует), панель не держим. Иначе оставалась
  // пустая панель, которую ещё и можно тянуть за разделитель (фидбек Вадима).
  // Строка такого индикатора тоже не рисуется (IndicatorRow), а сам он живёт
  // в состоянии — вернулся режим, вернулись и панель, и строка.
  const hasBasis = useCallback(
    (i: { basis?: 'price' | 'oi' }) => indBases.some((b) => b.id === (i.basis ?? 'price')),
    [indBases],
  );
  const extraPanes = useMemo(() => {
    const used = [...new Set([
      ...inds.list.filter((i) => i.pane > 0 && hasBasis(i)).map((i) => i.pane),
      ...(oiPaneEff > 0 ? [oiPaneEff] : []),
    ])].sort((a, b) => a - b);
    return used.map((pane) => ({
      pane,
      series: [...(pane === oiPaneEff ? oiOwn : []), ...(indSeries[pane] ?? [])],
    }));
  }, [inds.list, indSeries, oiPaneEff, oiOwn, hasBasis]);
  // Номер панели ≠ её индекс в стеке: номера не подряд (после удалений остаются
  // дыры), а осям алертов и уровням нужен именно индекс.
  const oiChartIndex = oiPaneEff === 0 ? 0 : extraPanes.findIndex((x) => x.pane === oiPaneEff) + 1;
  oiChartIndexRef.current = oiChartIndex;

  // §OI-3: на каких осях кликабельный «+». Вариант «both» (3 линии) — уровневый
  // алерт двусмыслен, выключаем целиком (как на сайте). Иначе: левая — если цена
  // показана; правая (ОИ) — всегда.
  const alertAxes = useMemo<{ pane: number; side: 'left' | 'right' }[]>(() => {
    if (oiVariant === 'both') return [];
    const ax: { pane: number; side: 'left' | 'right' }[] = [];
    if (showPrice) ax.push({ pane: 0, side: 'left' });
    ax.push({ pane: oiChartIndex, side: 'right' });
    return ax;
  }, [oiVariant, showPrice, oiChartIndex]);

  // Активные алерты этого актива → пунктир: цена на ЛЕВОЙ оси, уровень ОИ на ПРАВОЙ.
  const alertLines = useMemo(() => {
    type Line = { price: number; color: string; scale: 'left' | 'right'; pane?: number; title: string };
    const lines: Line[] = myAlerts
      .filter((a) => a.status === 'active' && a.asset === instrument)
      .flatMap((a): Line[] => {
        if (a.indicator === 'price') return [{ price: a.threshold, color: 'var(--accent)', scale: 'left', title: 'уведомление' }];
        if (a.indicator === 'oi_level') return [{ price: a.threshold, color: 'var(--accent)', scale: 'right', pane: oiChartIndex, title: 'уведомление' }];
        return [];
      });
    return lines.length ? lines : undefined;
  }, [myAlerts, instrument, oiChartIndex]);

  const chartPanes = useMemo<LwPane[]>(
    // Основной график заметно выше служебных, иначе RSI съедает цену.
    () => [{ series: allSeries, flex: extraPanes.length ? 2.6 : 1 }, ...extraPanes.map((x) => ({ series: x.series, flex: 1 }))],
    [allSeries, extraPanes],
  );
  const indValues = useMemo(() => indicatorValues(indSeries), [indSeries]);

  // Пользовательские высоты панелей (разделитель между ценой и индикаторами).
  // Ключ включает ЧИСЛО панелей: набор индикаторов меняет состав стека, и доли
  // трёх панелей к стеку из двух не относятся.
  const paneCountNow = chartPanes.length;
  const [paneSizes, setPaneSizes] = useState<number[] | undefined>(undefined);
  useEffect(() => {
    const raw = rd(`frame:embed:oi:paneSizes:${paneCountNow}`, '');
    try {
      const v = raw ? (JSON.parse(raw) as number[]) : undefined;
      setPaneSizes(v && v.length === paneCountNow && v.every((n) => Number.isFinite(n) && n > 0) ? v : undefined);
    } catch { setPaneSizes(undefined); }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [paneCountNow]);
  const onPaneSizesChange = useCallback((sizes: number[]) => {
    setPaneSizes(sizes);
    wr(`frame:embed:oi:paneSizes:${sizes.length}`, JSON.stringify(sizes.map((v) => Math.round(v * 1000) / 1000)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Перенос линий ОИ между панелями. Двигаются ВСЕ разом (в варианте
  // «Покупки + Продажи» их две, и на разных панелях они несравнимы).
  const moveOi = useCallback((to: 'up' | 'down' | 'own' | 'main') => {
    if (to === 'main') { setOiPane(0); return; }
    if (to === 'own') { setOiPane(inds.freePane()); return; }
    const occ = inds.occupiedPanes;
    const target = occ[occ.indexOf(oiPane) + (to === 'up' ? -1 : 1)];
    // swapPanes переставит индикаторы и через onSwapPanes — сам oiPane.
    if (target != null) inds.swapPanes(oiPane, target);
  }, [inds, oiPane]);

  const nativeRows = useMemo<NativeRow[]>(() => {
    const rows: NativeRow[] = [{
      id: 'price', label: displayName, color: sf.get('price').color ?? OI_COLORS.primary,
      visible: showPrice, onToggle: () => setShowPrice((v) => !v),
    }];
    // Режим и показатель ОИ здесь БОЛЬШЕ НЕ ЖИВУТ: они вернулись в тулбар
    // отдельными выпадашками (см. controls ниже). В ⋯ строки остаётся только
    // перемещение линии между панелями — иначе одна и та же настройка имела бы
    // два входа и рассинхронилась бы с рядом наверху.
    const at = inds.occupiedPanes.indexOf(oiPaneEff);
    for (const d of lwSeries) {
      if (d.id === 'price') continue;
      rows.push({
        id: d.id, label: d.label, color: d.color,
        visible: sf.get(d.id).visible !== false,
        onToggle: () => sf.setVisible(d.id, sf.get(d.id).visible === false),
        pane: oiPaneEff, onMove: moveOi,
        canUp: at > 0, canDown: at >= 0 && at < inds.occupiedPanes.length - 1,
      });
    }
    return rows;
  }, [lwSeries, sf, showPrice, displayName, oiPaneEff, inds.occupiedPanes, moveOi]);

  // Иконка «Показателя» — точка цветом ТЕКУЩЕЙ линии ОИ (с учётом перекраски в
  // ⚙ Формат, поэтому цвет из sf, а не из палитры). «Покупки + Продажи» — две
  // точки: линии там две, и одна точка врала бы про цвет второй.
  const variantIcon = useMemo<ReactNode>(() => (
    oiVariant === 'both'
      ? (
        <span style={{ display: 'inline-flex', gap: 3, alignItems: 'center' }}>
          <Dot color={sf.get('oi-long').color ?? colors.secondary} />
          <Dot color={sf.get('oi-short').color ?? colors.third} />
        </span>
      )
      : <Dot color={sf.get('oi').color ?? colors.secondary} />
  ), [oiVariant, sf, colors]);

  // Ряд контролов тулбара. ОДНА функция на видимый ряд и на невидимый измеритель
  // (compact=false всегда) — если рендерить их двумя копиями JSX, любая правка
  // одной копии тихо расходится с другой, и compact начинает срабатывать не там.
  const controls = (compact: boolean) => (
    <>
      {/* Список ТФ фильтруется по tfOptions — неликвидные EOD-only фьючерсы
          (напр. PXU6 «Полюс мини») интрадей-ОИ не отдают вовсе, так что 5м/1ч
          для них просто не предлагаем, а не молча гасим линию после выбора. */}
      <Dropdown value={interval} options={tfOptions} onChange={changeInterval} title="Таймфрейм" icon={<Clock size={14} />} compact={compact} compactLabel={(v) => TF_SHORT[v]} />
      <Dropdown value={clgroup} options={CLGROUP_OPTS} onChange={setClgroup} title="Участники" icon={CLGROUP_ICON} compact={compact} />
      <Dropdown value={displayMode} options={MODE_OPTS} onChange={setDisplayMode} title="Режим" icon={MODE_ICON} compact={compact} />
      <Dropdown value={oiVariant} options={variantOpts} onChange={setOiVariant} title="Показатель" icon={variantIcon} compact={compact} />
      <IndicatorsButton api={inds} hasVolume={hasVolume} bases={indBases} compact={compact} />
    </>
  );

  return (
    <EmbedFrame
      toolbarUnified
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
        // position:relative — якорь для невидимого измерителя (position:absolute
        // внутри). flex:1/minWidth:0 — сам берёт себе всё оставшееся место в
        // едином ряду EmbedFrame (toolbarUnified — без вложенного скролл-див'а),
        // иначе clientWidth = ширине контента (сжатого до compact), а не реально
        // доступному месту, и compact никогда не откатится обратно при расширении.
        // overflow:hidden — БЕЗ него, если flex сжал этот div ниже natural-ширины
        // видимых (уже compact) иконок быстрее, чем сработал ResizeObserver, их
        // контент вылезает ЗА рамки div'а и визуально наезжает на соседний блок
        // (⚙⤢◐× справа) — тот красится поверх (позже в DOM), выглядит как «кнопки
        // залезли друг на друга». MINW_BY_TYPE в SandboxPage — первая линия
        // защиты (не даёт панели сжаться настолько), это — вторая (даже если
        // всё равно сожмут более узкого MINW, обрежется, а не наедет).
        <div ref={toolbarWrapRef} style={{ position: 'relative', display: 'flex', alignItems: 'center', gap: 6, flex: 1, minWidth: 0, overflow: 'hidden' }}>
          {/* Невидимый измеритель — ВСЕГДА полные лейблы (compact=false), не зависит
              от текущего toolbarCompact. Если сравнивать видимый ряд сам с собой,
              после схлопывания в иконки он сам себя же и «не переполняет» → compact
              откатывается обратно, полные лейблы уже переполняют → снова compact —
              бесконечное мерцание. Отдельный стабильный эталон убирает эту гонку. */}
          <div
            ref={toolbarMeasureRef}
            aria-hidden
            style={{ position: 'absolute', top: 0, left: 0, visibility: 'hidden', pointerEvents: 'none', display: 'flex', alignItems: 'center', gap: 6, whiteSpace: 'nowrap' }}
          >
            {controls(false)}
          </div>
          {/* Ряд тулбара: [ТФ] [Физ/Юр] [Режим] [Показатель] [Индикаторы] — все
              выпадашки раскрываются ВНИЗ и выглядят одинаково (пилюль Физ/Юр
              больше нет). Режим и показатель ОИ вернулись сюда из ⋯ строки
              легенды: настройка среза нужна часто, а искать её в меню линии
              было неочевидно. Вид графика тут по-прежнему не настраивается —
              он per-линия в ⚙ Формат (цена / покупки-продажи / ОИ).
              icon на каждом контроле — узкая панель схлопывает лейбл в иконку
              (toolbarCompact, см. измеритель выше); title сохраняет текст в тултипе. */}
          {controls(toolbarCompact)}
        </div>
      }
      actions={
        <DrawExportActions draw={draw} visible={status === 'ok' && !!data && lwSeries.length > 0} />
      }
      more={
        <>
          <DrawerSection label="Настройки">
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              <ToggleRow label="Цена" checked={showPrice} onChange={setShowPrice} hint="Линия цены фьючерса" />
              <ToggleRow label="Экспирации" checked={showExpirations} onChange={setShowExpirations} hint="Метки смены контракта" />
            </div>
          </DrawerSection>
          {/* §OI-5: формат КАЖДОЙ линии окна отдельно (тип + цвет) — разбивка по тому, что на
              графике: Цена (линия/область/свечи/бары), линии ОИ (линия/область). Подписи ниже
              адаптивны: Чистая позиция / Открытый интерес / Покупки / Продажи (или Покупки+Продажи). */}
          {showPrice && (
            <FormatSection label="Цена" kinds={OHLC_KINDS} fmt={sf.get('price')} onKind={(k) => sf.setKind('price', k)} onColor={(c) => sf.setColor('price', c)} onOpacity={(o) => sf.setOpacity('price', o)} onWidth={(w) => sf.setWidth('price', w)} defaultColor={OI_COLORS.primary} />
          )}
          {oiVariant === 'both' ? (
            <>
              <FormatSection label={labels.secondary} fmt={sf.get('oi-long')} onKind={(k) => sf.setKind('oi-long', k)} onColor={(c) => sf.setColor('oi-long', c)} onOpacity={(o) => sf.setOpacity('oi-long', o)} onWidth={(w) => sf.setWidth('oi-long', w)} defaultColor={colors.secondary} />
              <FormatSection label={labels.third} fmt={sf.get('oi-short')} onKind={(k) => sf.setKind('oi-short', k)} onColor={(c) => sf.setColor('oi-short', c)} onOpacity={(o) => sf.setOpacity('oi-short', o)} onWidth={(w) => sf.setWidth('oi-short', w)} defaultColor={colors.third} />
            </>
          ) : (
            <FormatSection label={labels.secondary} fmt={sf.get('oi')} onKind={(k) => sf.setKind('oi', k)} onColor={(c) => sf.setColor('oi', c)} onOpacity={(o) => sf.setOpacity('oi', o)} onWidth={(w) => sf.setWidth('oi', w)} defaultColor={colors.secondary} />
          )}
        </>
      }
    >
      <div ref={chartBoxRef} style={{ position: 'absolute', inset: 0 }}>
        {status === 'ok' && data && lwSeries.length > 0 && (
          <LwChartPanes
            ref={panesRef}
            panes={chartPanes}
            onReachStart={loadMore}
            drawPaneIndex={0}
            hideLegend
            expirations={expirations}
            volumeProfile={vpSpec}
            // Строка индикатора живёт над СВОЕЙ панелью, а не в общем углу.
            paneOverlay={(i) => (i === 0 ? null : (
              <PaneIndicatorList api={inds} pane={extraPanes[i - 1]?.pane ?? i} values={indValues} native={nativeRows} oiPane={oiPaneEff} bases={indBases} />
            ))}
            paneSizes={paneSizes}
            onPaneSizesChange={onPaneSizesChange}
            onCreateAlert={handleCreateAlertFromChart}
            alertAxes={alertAxes}
            dark={dark}
            fitKey={`${instrument}|${interval}`}
            initialBars={interval === 24 ? 252 : 220}
            tickFmt={interval === 24 ? monthsYearsTickFmt : undefined}
            timeVisible={interval !== 24}
            priceLines={alertLines}
            drawActive={draw.drawMode}
            drawTool={draw.drawTool}
            drawings={draw.drawings}
            onDrawingsChange={draw.setDrawings}
            drawColor={draw.drawColor}
            drawWidth={draw.drawWidth}
            drawDash={draw.drawDash}
            drawOpacity={draw.drawOpacity}
            drawFill={draw.drawFill}
            drawFillColor={draw.drawFillColor}
            drawFillOpacity={draw.drawFillOpacity}
            selectedDrawId={draw.selectedDrawId}
            onSelectDraw={draw.setSelectedDrawId}
            onSelectionRect={draw.setSelRect}
            onToolReset={draw.onToolReset}
            drawHidden={draw.drawHidden}
            drawLocked={draw.drawLocked}
          />
        )}
        {/* Оверлей рисования (контекстная панель свойств + сайдбар инструментов +
            слои) и модалка экспорта — общие компоненты useDrawTools.tsx. */}
        <IndicatorList api={inds} native={nativeRows} visible={status === 'ok' && !!data && lwSeries.length > 0} values={indValues} oiPane={oiPaneEff} bases={indBases} />
        <DrawToolsOverlay draw={draw} visible={status === 'ok' && !!data && lwSeries.length > 0} />
        <ChartExportModal
          draw={draw}
          targetElement={chartBoxRef.current}
          lwChartRef={panesRef}
          filename={`frame-oi-${instrument}-${interval}`}
          metadata={exportMeta}
        />
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
