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
import { Clock, User, Building2 } from 'lucide-react';
import { monthsYearsTickFmt, type LwSeries } from '../../components/chart/lwTypes';
import LwChartPanes, { type LwChartPanesHandle, type LwPane } from '../../components/LwChartPanes';
import { useTheme } from '../../contexts/ThemeContext';
import { getChartData, getInstrument, listAlerts, type AlertInfo } from '../../services/api';
import CreateAlertModal, { type AlertMetricOption } from '../../components/alerts/CreateAlertModal';
import { displayTicker } from '../../utils/displayTicker';
import { formatNumber, formatPrice } from '../../utils/formatNumber';
import { useRealtimeData } from '../../hooks/useRealtimeData';
import { EmbedMsg } from './embedUi';
import { DrawerSection, ToggleRow } from './EmbedSettings';
import { FormatSection, applyFormat, useSeriesFormats, OHLC_KINDS } from './EmbedFormat';
import { EmbedFrame, AssetButton, Dropdown, PillGroup, WheelHint } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';
import { useToolbarCompact } from './useToolbarCompact';
import { useTierAccess } from '../../contexts/TierFeaturesContext';
import { useDrawTools, DrawExportActions, DrawToolsOverlay, ChartExportModal } from './useDrawTools';
import { useIndicators, useIndicatorSeries, useVolumeProfileSpec, indicatorValues, IndicatorList, PaneIndicatorList, IndicatorsButton, type NativeRow } from './EmbedIndicators';

// Компактные лейблы таймфрейма для тулбар-выпадашки (§OI-7: одна кнопка-dropdown).
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
type Series = { time: string; value: number; open?: number; high?: number; low?: number; close?: number; volume?: number }[];

/** Стабильная ссылка на пустой ряд: новый литерал на каждый рендер сбрасывал бы
 *  мемоизацию потребителей (профиль объёма пересчитывался бы впустую). */
const EMPTY_CANDLES: Series = [];

// Группа участников — единственная выпадашка ОИ, оставшаяся в тулбаре: она
// относится ко всему окну (чьи позиции смотрим), а не к отдельной линии.
const CLGROUP_OPTS: { id: ClGroup; label: string; icon: ReactNode }[] = [
  { id: 'FIZ', label: 'Физлица', icon: <User size={14} /> },
  { id: 'YUR', label: 'Юрлица', icon: <Building2 size={14} /> },
];
const MODE_OPTS: { id: DisplayMode; label: string }[] = [
  { id: 'positions', label: 'Объём позиций' },
  { id: 'participants', label: 'Число трейдеров' },
];
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

  // Реалтайм: ингест обновил данные (SSE '5min'/'hourly') → тихий рефетч.
  // Тихий = без setStatus('loading'): панель не мигает плашкой, старый график
  // висит до прихода свежего. SSE-соединение одно на вкладку (синглтон в
  // useSSE), сколько бы панелей ни было открыто.
  const [refreshTick, setRefreshTick] = useState(0);
  const silentRef = useRef(false);
  useRealtimeData(['5min', 'hourly'], () => { silentRef.current = true; setRefreshTick((t) => t + 1); });

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
    const period = interval === 24
      ? (oiAccess.isLoading ? '1y' : bestDailyPeriod(oiAccess.canUsePeriod))
      : interval === 60 ? '1y' : '6m';
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
          zeroLine: oiVariant === 'net',
          data: oiSeries.secondary.map((p) => ({ time: toSec(p.time, intraday), value: p.value })),
          tipFmt: (v) => formatNumber(v, 0), axisFmt: (v) => formatNumber(v, 0),
        }, sf.get('oi')));
      }
    }
    return out;
  }, [chartData, oiSeries, oiVariant, colors, labels, showPrice, displayName, interval, sf.get]);

  // Индикаторы считаются от свечей и кладутся на ЛЕВУЮ (ценовую) ось. Порядок
  // важен: наложения идут ПОСЛЕ нативных серий, чтобы первой в массиве осталась
  // цена — от неё берут отсчёт магнит и линейка слоя рисования.
  const toSecFn = useCallback((t: string) => toSec(t, interval !== 24), [interval]);
  const indSeries = useIndicatorSeries(inds.list, chartData, toSecFn, inds.colorOf);
  // Профиль объёма — не серия, а примитив на ценовой серии (он гистограмма по
  // цене, а не по времени). Крепится к 'price': на линии ОИ он отрисовал бы
  // уровни по чужой шкале. Цена скрыта → серии нет → профиля тоже нет.
  const vpSpec = useVolumeProfileSpec(inds.list, showPrice ? chartData : EMPTY_CANDLES, 'price', inds.colorOf);
  const hasVolume = useMemo(() => chartData.some((p) => p.volume != null), [chartData]);
  // «Глаз» нативной серии: у цены это существующий тумблер showPrice (единственный
  // источник правды), у линий ОИ — поле visible в карте форматов.
  const visibleNative = useMemo(() => lwSeries.filter((d) => sf.get(d.id).visible !== false), [lwSeries, sf]);
  // indSeries сгруппированы по панелям: [0] — наложения на основной график,
  // [1+] — индикаторы со своей шкалой (RSI/ATR/объёмы).
  // Линии ОИ уезжают в свою панель целиком (их 1–2, и обе на одной шкале).
  // ⚠️ Если после переезда на основной панели не осталось НИЧЕГО (цена скрыта,
  // наложений нет) — возвращаем ОИ наверх: пустая ценовая панель это полоса
  // пустоты во всю ширину и график без единой линии.
  const oiOwn = useMemo(() => visibleNative.filter((d) => d.id !== 'price'), [visibleNative]);
  const mainNative = useMemo(() => (oiPane > 0 ? visibleNative.filter((d) => d.id === 'price') : visibleNative), [visibleNative, oiPane]);
  const oiPaneEff = mainNative.length === 0 && (indSeries[0] ?? []).length === 0 ? 0 : oiPane;
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
  const extraPanes = useMemo(() => {
    const used = [...new Set([
      ...inds.list.filter((i) => i.pane > 0).map((i) => i.pane),
      ...(oiPaneEff > 0 ? [oiPaneEff] : []),
    ])].sort((a, b) => a - b);
    return used.map((pane) => ({
      pane,
      series: [...(pane === oiPaneEff ? oiOwn : []), ...(indSeries[pane] ?? [])],
    }));
  }, [inds.list, indSeries, oiPaneEff, oiOwn]);
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
        if (a.indicator === 'price') return [{ price: a.threshold, color: 'var(--accent)', scale: 'left', title: 'алерт' }];
        if (a.indicator === 'oi_level') return [{ price: a.threshold, color: 'var(--accent)', scale: 'right', pane: oiChartIndex, title: 'алерт' }];
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
    // Настройки самой величины ОИ — здесь, в строке. Раньше это были две
    // выпадашки в тулбаре; см. NativeRowMenu о том, почему они переехали.
    const choices = [
      { label: 'Режим', value: displayMode, options: MODE_OPTS, onChange: (v: string) => setDisplayMode(v as DisplayMode) },
      { label: 'Показатель', value: oiVariant, options: variantOpts, onChange: (v: string) => setOiVariant(v as OIVariant) },
    ];
    const at = inds.occupiedPanes.indexOf(oiPaneEff);
    for (const d of lwSeries) {
      if (d.id === 'price') continue;
      rows.push({
        id: d.id, label: d.label, color: d.color,
        visible: sf.get(d.id).visible !== false,
        onToggle: () => sf.setVisible(d.id, sf.get(d.id).visible === false),
        pane: oiPaneEff, onMove: moveOi, choices,
        canUp: at > 0, canDown: at >= 0 && at < inds.occupiedPanes.length - 1,
      });
    }
    return rows;
  }, [lwSeries, sf, showPrice, displayName, displayMode, oiVariant, variantOpts, oiPaneEff, inds.occupiedPanes, moveOi]);

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
            <Dropdown value={interval} options={tfOptions} onChange={changeInterval} title="Таймфрейм" icon={<Clock size={14} />} />
            <PillGroup value={clgroup} options={CLGROUP_OPTS} onChange={setClgroup} />
            <IndicatorsButton api={inds} hasVolume={hasVolume} />
          </div>
          {/* ТФ — компактный дропдаун (тулбар был слишком широк с пилюлями). Физ/Юр —
              горизонтальные пилюли (2 пункта). Режим и показатель ОИ из тулбара УБРАНЫ:
              они относятся к одной линии, а не ко всему окну, и живут в её ⋯ вместе
              с настройками пользовательских индикаторов. На их месте — «Индикаторы».
              Вид графика убран из тулбара → настраивается
              per-линия в ⚙ Формат (цена/покупки-продажи/ОИ — по тому, что на графике).
              Список фильтруется по tfOptions — неликвидные EOD-only фьючерсы (напр.
              PXU6 «Полюс мини») не отдают интрадей-ОИ вообще, так что 5м/1ч для них
              просто не предлагаем, а не молча гасим линию после выбора.
              icon на каждом контроле — узкая панель схлопывает лейбл в иконку
              (toolbarCompact, см. измеритель выше); title сохраняет текст в тултипе. */}
          <Dropdown value={interval} options={tfOptions} onChange={changeInterval} title="Таймфрейм" icon={<Clock size={14} />} compact={toolbarCompact} />
          <PillGroup value={clgroup} options={CLGROUP_OPTS} onChange={setClgroup} compact={toolbarCompact} />
          <IndicatorsButton api={inds} hasVolume={hasVolume} compact={toolbarCompact} />
        </div>
      }
      actions={
        <DrawExportActions draw={draw} visible={status === 'ok' && !!data && lwSeries.length > 0} />
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
          <LwChartPanes
            ref={panesRef}
            panes={chartPanes}
            drawPaneIndex={0}
            hideLegend
            expirations={expirations}
            volumeProfile={vpSpec}
            // Строка индикатора живёт над СВОЕЙ панелью, а не в общем углу.
            paneOverlay={(i) => (i === 0 ? null : (
              <PaneIndicatorList api={inds} pane={extraPanes[i - 1]?.pane ?? i} values={indValues} native={nativeRows} />
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
        <IndicatorList api={inds} native={nativeRows} visible={status === 'ok' && !!data && lwSeries.length > 0} values={indValues} />
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
