import { useEffect, useState, useMemo, useRef } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { ChevronDown, BarChart3, ListFilter } from 'lucide-react';
import PageHeader from '../components/PageHeader';
import ChartTabs from '../components/ChartTabs';
import OiScreenerTable from '../components/oi/OiScreenerTable';
import InstrumentIcon from '../components/InstrumentIcon';
import { usePrefetchLogos } from '../hooks/usePrefetchLogos';
import { METHODOLOGY } from '../data/methodology';
import { OI_VARIANT_HELP } from '../data/oiVariantHelp';
import { getChartData, getInstrument, listAlerts, type AlertInfo } from '../services/api';
import type { ChartResponse } from '../types';
import type { ChartAnnotation } from '../components/SimpleChart';
import SimpleChart from '../components/SimpleChart';
import ChartCaptureButton from '../components/export/ChartCaptureButton';
import ChartSettings from '../components/chart/ChartSettings';
import CsvExportButton from '../components/export/CsvExportButton';
import { periodToQuery } from '../utils/csvPeriod';
import InstrumentSearchModal from '../components/InstrumentSearchModal';
import Dropdown from '../components/Dropdown';
import SegmentedControl from '../components/SegmentedControl';
import LayersButton from '../components/LayersButton';
import ChartActionsMenu from '../components/ChartActionsMenu';
import { PERIOD_LABELS as ALL_PERIOD_LABELS, INTERVAL_LABELS } from '../config/chartConfig';
import { useAuth } from '../contexts/AuthContext';
import { useTheme } from '../contexts/ThemeContext';
import { isIntervalAllowed, isPeriodAllowed, getDefaultPeriod } from '../config/accessControl';
import { useIndicatorData } from '../hooks/useIndicatorData';
import { usePersistedState } from '../hooks/usePersistedState';
import { useFitToViewport } from '../hooks/useFitToViewport';
import { useOnboardingTour } from '../hooks/useFirstVisit';
import OnboardingTour from '../components/onboarding/OnboardingTour';
import { oiTourSteps } from '../data/tours/oi';
import { oiScreenerTourSteps } from '../data/tours/oiScreener';
import { formatPrice } from '../utils/formatNumber';
import { useUpgradePrompt } from '../components/tier/UpgradeModal';
import { oiTierResolver } from '../utils/tierError';
import { parseOiDeepLink } from '../utils/oiDeepLink';
import AlertBellButton from '../components/alerts/AlertBellButton';
import CreateAlertModal, { type AlertMetricOption } from '../components/alerts/CreateAlertModal';
import { ALERTS_ENABLED } from '../config/alertsConfig';
import { useTierAccess, useCommonFeatures } from '../contexts/TierFeaturesContext';

type DisplayMode = 'price' | 'positions' | 'participants';
type OIVariant = 'oi' | 'long' | 'short' | 'both' | 'net';
type Period = '1w' | '1m' | '1y' | '5y' | 'all';

// Подписи «ног» величины ОИ (что показано на правой оси в текущем режиме).
const OI_LEG_LABEL: Record<'net' | 'long' | 'short' | 'oi' | 'npart', string> = {
  net: 'чистая позиция', long: 'длинные позиции', short: 'короткие позиции',
  oi: 'открытый интерес', npart: 'число участников',
};

// Метрики алертов для «Открытого интереса»: цена (TradingView-стиль: пересечение/
// больше/меньше) + наши аномалии по резкому движению (ATR ×N). Общий список для
// кнопки-колокола и для «+» на графике. Цена — первой (дефолт при клике «+»).
// Направление рекорда для oi_extreme. «Любой» (new_extreme) закрывает случай,
// когда неважно, в какую сторону пробило: сработает и на новом максимуме, и на
// новом минимуме перекоса. Бэк (compute_value) сам определит фактическую сторону.
const EXTREME_OPS = [
  { value: 'new_high', label: 'новый максимум (лонг-рекорд)' },
  { value: 'new_low', label: 'новый минимум (шорт-рекорд)' },
  { value: 'new_extreme', label: 'любой — максимум или минимум' },
];
// Окна рекорда. Короткие мес/полугода убраны — охотимся за редкими рекордами.
// Горизонты 1…5 лет + всё время (данные позиций у основных тикеров с 2019).
// Значение уходит в threshold как дни.
const EXTREME_PERIODS = [
  { value: '365', label: 'за год' },
  { value: '730', label: 'за 2 года' },
  { value: '1095', label: 'за 3 года' },
  { value: '1460', label: 'за 4 года' },
  { value: '1825', label: 'за 5 лет' },
  { value: '0', label: 'за всё время' },
];

const OI_ALERT_METRICS: AlertMetricOption[] = [
  {
    key: 'price', label: 'Цена', indicator: 'price', metric: 'close', unit: '₽',
    ops: [
      { value: 'cross_up', label: '↑ пересечёт (снизу вверх)' },
      { value: 'cross_down', label: '↓ пересечёт (сверху вниз)' },
      { value: 'gt', label: 'станет выше' },
      { value: 'lt', label: 'станет ниже' },
    ],
    hint: 'Сработает, когда цена фьючерса пересечёт заданный уровень или окажется выше/ниже него. У ликвидных контрактов проверка каждые несколько минут, у остальных — раз в день после закрытия.',
  },
  {
    // «move_all» (clgroup ALL) — считается по тем же net-данным (источник net —
    // FIZ), но текст НЕЙТРАЛЬНЫЙ, без роли субъекта. «Общий ракурс» сигнала.
    key: 'move_all', label: 'Резкое движение позиции — в целом',
    indicator: 'oi_move', metric: 'atr', clgroup: 'ALL', unit: '×', defaultThreshold: 3,
    ops: [{ value: 'gt', label: 'превысит' }],
    hint: 'Сработает, когда чистая позиция изменится за день резче обычного — во столько-то раз больше среднего дневного шага за 14 дней (ATR). 2× — заметно, 3× — сильно, 5× — экстремально. Обновляется раз в день после публикации позиций МосБиржи; это описание движения, не прогноз цены.',
  },
  {
    key: 'move_fiz', label: 'Резкое движение позиции — физлица',
    indicator: 'oi_move', metric: 'atr', clgroup: 'FIZ', unit: '×', defaultThreshold: 3,
    ops: [{ value: 'gt', label: 'превысит' }],
    hint: 'Сработает, когда чистая позиция физлиц изменится за день резче обычного — во столько-то раз больше среднего дневного шага за 14 дней (ATR). 2× — заметно, 3× — сильно, 5× — экстремально. Обновляется раз в день после публикации позиций МосБиржи; это описание движения, не прогноз цены.',
  },
  {
    key: 'move_yur', label: 'Резкое движение позиции — юрлица',
    indicator: 'oi_move', metric: 'atr', clgroup: 'YUR', unit: '×', defaultThreshold: 3,
    ops: [{ value: 'gt', label: 'превысит' }],
    hint: 'Сработает, когда чистая позиция юрлиц изменится за день резче обычного — во столько-то раз больше среднего дневного шага за 14 дней (ATR). 2× — заметно, 3× — сильно, 5× — экстремально. Обновляется раз в день после публикации позиций МосБиржи; это описание движения, не прогноз цены.',
  },
  {
    // «Новый экстремум перекоса за период» — ловит ПЛАВНЫЙ дрейф позиционирования,
    // который «резкое движение» (ATR) пропускает. op = направление (макс/мин/любой),
    // period = окно (уходит в threshold как дни: 365/730/1095/1460/1825/0=всё время).
    key: 'extreme_all', label: 'Новый максимум/минимум позиции — в целом',
    indicator: 'oi_extreme', metric: 'net', clgroup: 'ALL',
    ops: EXTREME_OPS,
    periodControl: EXTREME_PERIODS,
    hint: 'Сработает, когда чистая позиция достигнет нового перекоса-рекорда за выбранный период — без разбивки на группы (физ и юр зеркальны). Ловит МЕДЛЕННЫЙ дрейф: позиция может ползти неделями и поставить рекорд без единого резкого дня.',
  },
  {
    key: 'extreme_fiz', label: 'Новый максимум/минимум позиции — физлица',
    indicator: 'oi_extreme', metric: 'net', clgroup: 'FIZ',
    ops: EXTREME_OPS,
    periodControl: EXTREME_PERIODS,
    hint: 'Сработает, когда чистая позиция физлиц достигнет нового перекоса-рекорда за выбранный период. В отличие от «резкого движения», ловит МЕДЛЕННЫЙ дрейф: позиция может ползти неделями и поставить рекорд без единого резкого дня.',
  },
  {
    key: 'extreme_yur', label: 'Новый максимум/минимум позиции — юрлица',
    indicator: 'oi_extreme', metric: 'net', clgroup: 'YUR',
    ops: EXTREME_OPS,
    periodControl: EXTREME_PERIODS,
    hint: 'Сработает, когда чистая позиция юрлиц достигнет нового перекоса-рекорда за выбранный период. Ловит медленный дрейф позиционирования, невидимый «резкому движению».',
  },
];

const PERIOD_LABELS: Record<Period, string> = {
  '1w':  ALL_PERIOD_LABELS['1w'],
  '1m':  ALL_PERIOD_LABELS['1m'],
  '1y':  ALL_PERIOD_LABELS['1y'],
  '5y':  ALL_PERIOD_LABELS['5y'],
  'all': ALL_PERIOD_LABELS['all'],
};

// Локальный INSTRUMENT_ICONS удалён — используется shared
// `<InstrumentIcon sectype={...} />` из components/InstrumentIcon.tsx
// (он сам разруливает фьючерсы → акции → лого, валюты → custom badge).

// Цветовая палитра — все цвета через CSS-переменные чтобы автоматически
// адаптироваться к теме (в editorial-light → muted blue/orange, в OKX dark
// → яркие неоновые). См. --chart-line-1 / --oi-* в index.css.
const COLORS = {
  primary: 'var(--chart-line-1)',     // Цена инструмента (Сбербанк) — blue в editorial, indigo в OKX
  amber:   'var(--oi-amber)',          // ОИ — оранжевый чип
  emerald: 'var(--oi-green)',          // Покупки/Long — зелёный чип
  rose:    'var(--oi-red)',            // Продажи/Short — красный чип
  cyan:    'var(--oi-cyan)',           // Чистая позиция — циан чип
  lime:    'var(--oi-amber)',          // legacy alias
};

export default function OpenInterestPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const { isAuthenticated } = useAuth();
  const { theme: _theme } = useTheme();
  const navigate = useNavigate();

  // Tier upgrade prompt — открывается при 403 от backend (см. loadData catch)
  // и при тапе на locked-опции в Dropdown'ах
  const { showUpgrade } = useUpgradePrompt();
  const oiAccess = useTierAccess('open_interest');

  // Фоновая предзагрузка лого один раз — модалка выбора актива потом
  // открывается мгновенно из SW cache, без 100 запросов.
  usePrefetchLogos();

  // Адаптивная высота графика. Anchor-ref на wrapper самого графика —
  // хук вычитает позицию anchor.top от window.innerHeight, плюс buffer на
  // range-slider внутри SimpleChart и нижний padding страницы.
  // Без хардкода: всё что добавляется/убирается выше графика учтётся
  // автоматически (margins, error плашки, multi-row controls и т.д.),
  // потому что хук смотрит на реальную позицию anchor в DOM.
  const headerRef = useRef<HTMLDivElement>(null);
  const controlsRef = useRef<HTMLDivElement>(null);
  const chartAnchorRef = useRef<HTMLDivElement>(null);
  const chartHeight = useFitToViewport(chartAnchorRef, {
    min: 360,
    max: 720,
    bottomBuffer: 96, // range slider в SimpleChart (~48) + page py-bottom (~32) + safety margin
    watchRefs: [headerRef, controlsRef],
  });

  // Инструмент
  // ВАЖНО: sec_id может прийти из URL-параметра (например `?instrument=IMOEXF`),
  // а instrumentName инициализируется дефолтом 'Сбербанк'. Поэтому ниже стоит
  // useEffect который при первом рендере (или при любом расхождении ticker/name)
  // резолвит имя через /api/instruments/{sec_id} — иначе был баг
  // "Сбербанк [IMOEXF]" в UI-кнопке.
  // Выбранный инструмент персистится в localStorage — чтобы, вернувшись на ОИ,
  // увидеть последний выбранный, а не дефолтный Сбербанк. Приоритет:
  // ?instrument= в URL (шаринг/перезагрузка) > localStorage > 'SR'.
  const [selectedInstrument, setSelectedInstrument] = useState(() => {
    const fromUrl = searchParams.get('instrument');
    if (fromUrl) return fromUrl;
    try { return localStorage.getItem('frame:oi:instrument') || 'SR'; } catch { return 'SR'; }
  });
  const [instrumentName, setInstrumentName] = useState(() => {
    // Имя пусто (→ резолвится эффектом ниже) если инструмент восстановлен из URL
    // или localStorage; иначе дефолт «Сбербанк».
    try {
      const restored = searchParams.get('instrument') || localStorage.getItem('frame:oi:instrument');
      return restored ? '' : 'Сбербанк';
    } catch { return 'Сбербанк'; }
  });
  const [isModalOpen, setIsModalOpen] = useState(false);
  // Актуальный фронт-контракт ('BRN6') выбранного фьючерса — для кнопки пикера и
  // экспорта. Обрезанный sectype ('BR') тикером не является. null → fallback на sectype.
  const [frontContract, setFrontContract] = useState<string | null>(null);

  // Синхронизация имени инструмента с тикером.
  // Срабатывает когда selectedInstrument меняется без вызова handleSelectInstrument:
  //   - при первой загрузке страницы если в URL передан ?instrument=XXX
  //   - при ручном редактировании URL
  // Если имя пусто → резолвим через API. handleSelectInstrument ставит имя сам,
  // так что этот useEffect ничего не ломает при выборе из модалки.
  useEffect(() => {
    if (instrumentName && instrumentName.length > 0) return;
    let cancelled = false;
    getInstrument(selectedInstrument).then((inst) => {
      if (cancelled) return;
      if (inst?.name) {
        setInstrumentName(inst.name);
      } else {
        // API не ответил — fallback на сам тикер, чтобы хоть что-то показать
        setInstrumentName(selectedInstrument);
      }
    });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedInstrument]);

  // Персист выбранного инструмента (см. init выше — приоритет URL > localStorage).
  useEffect(() => {
    try { localStorage.setItem('frame:oi:instrument', selectedInstrument); } catch { /* quota / private */ }
  }, [selectedInstrument]);

  // Резолвим актуальный фронт-контракт при любой смене инструмента (выбор из
  // модалки / URL / localStorage). getInstrument отдаёт front_secid из календаря;
  // для спота/ошибки — null → ниже fallback на selectedInstrument.
  useEffect(() => {
    let cancelled = false;
    getInstrument(selectedInstrument).then((inst) => {
      if (cancelled) return;
      setFrontContract(inst?.front_secid || null);
    });
    return () => { cancelled = true; };
  }, [selectedInstrument]);

  // Данные графика грузятся через useIndicatorData (ниже, после контролов —
  // фетчеру нужны interval/clgroup/showOi/period).
  // Интервал, для которого загружены текущие данные — обновляется атомарно с data
  const [dataInterval, setDataInterval] = useState(24);

  // Onboarding tour — ДВА независимых тура по вкладкам: график ('oi') и
  // скринер ('oi-screener'), каждый со своим ключом первого визита. Рендерим
  // тот, что соответствует активной вкладке — поэтому чартовый тур больше НЕ
  // стартует на вкладке скринера (его якоря там скрыты) и наоборот.
  const tour = useOnboardingTour('oi');
  const screenerTour = useOnboardingTour('oi-screener');

  // Настройки (персистятся в localStorage по индикатору — не сбрасываются на новой сессии)
  const [interval, setIntervalValue] = usePersistedState('frame:oi:interval', 24);
  // raw* — сохранённый пользовательский выбор среза. Для Free он переопределяется
  // дефолтом ниже (settingsLocked), но остаётся в localStorage — после апгрейда
  // на Basic вернётся то, что юзер выбирал. Дефолт clgroup = FIZ (физлица).
  const [rawClgroup, setClgroup] = usePersistedState<'FIZ' | 'YUR'>('frame:oi:clgroup', 'FIZ');
  const [rawDisplayMode, setDisplayMode] = usePersistedState<DisplayMode>('frame:oi:displayMode', 'positions');
  const [rawOiVariant, setOiVariant] = usePersistedState<OIVariant>('frame:oi:oiVariant', 'net');
  const [showExpirations, setShowExpirations] = usePersistedState('frame:oi:showExpirations', false);
  const [showPrice, setShowPrice] = usePersistedState('frame:oi:showPrice', true);
  // Тип графика теперь ГЛОБАЛЬНЫЙ (useChartSettings в SimpleChart/ChartSettings) —
  // старый per-page ключ frame:oi:chartType мигрируется в сторе.
  // Период: диплинк сигнала (URL) приоритетнее сохранённого. Применяем на init
  // (seed), а не эффектом — иначе первый запрос ушёл бы на сохранённом 5y и для
  // 5-минутного сигнала тянул бы годы баров. См. parseOiDeepLink.
  const [period, setPeriod] = usePersistedState<Period>(
    'frame:oi:period', getDefaultPeriod('1y', isAuthenticated) as Period,
    parseOiDeepLink(searchParams).period);

  // ── Free-тариф: индикатор открыт всем и по всем активам, но СРЕЗ данных
  // (категория участников / тип данных / показатель) залочен на дефолт —
  // Физлица · Объём позиций · Чистая позиция. Попытка сменить любой → апселл на
  // Basic. Basic/Pro (settings_customizable=true) — полная свобода. Лок чисто
  // UI-шный: backend отдаёт все срезы (их читает и embed-виджет). Пока tier
  // грузится, считаем НЕ кастомизируемым — не мигаем чужим сохранённым срезом
  // до подтверждения тарифа.
  const canCustomizeOi = !oiAccess.isLoading && oiAccess.canUseFlag('settings_customizable');
  const settingsLocked = !oiAccess.isLoading && !oiAccess.canUseFlag('settings_customizable');
  const settingsUpgradeTier = oiAccess.requiredTierFor({ flag: 'settings_customizable' }) ?? 'basic';
  const clgroup: 'FIZ' | 'YUR' = canCustomizeOi ? rawClgroup : 'FIZ';
  const displayMode: DisplayMode = canCustomizeOi ? rawDisplayMode : 'positions';
  const oiVariant: OIVariant = canCustomizeOi ? rawOiVariant : 'net';
  const promptSettingsUpgrade = (featureName: string) =>
    showUpgrade({ tier: settingsUpgradeTier, featureName, indicator: 'open_interest' });

  // Контекст сигнала из URL (Telegram deep-link ИЛИ in-app тост/колокол аномалии).
  // Применяем при КАЖДОЙ навигации, не только на маунте — иначе клик по второй
  // аномалии того же индикатора /oi, но ДРУГОГО актива, не менял бы ничего: SPA не
  // перемонтирует страницу, а mount-эффект и useState-инициализатор больше не
  // дёргаются. Гард по строке URL + сравнение тикера: пользовательский выбор актива
  // (он тоже пишет ?instrument= через setSearchParams, строка 319) НЕ вызывает
  // лишнего сброса имени.
  const selectedInstrumentRef = useRef(selectedInstrument);
  selectedInstrumentRef.current = selectedInstrument;
  const appliedDeepLinkRef = useRef('');
  useEffect(() => {
    const urlKey = searchParams.toString();
    if (urlKey === appliedDeepLinkRef.current) return;
    appliedDeepLinkRef.current = urlKey;
    const dl = parseOiDeepLink(searchParams);
    if (dl.instrument && dl.instrument !== selectedInstrumentRef.current) {
      setSelectedInstrument(dl.instrument);
      setInstrumentName('');   // переразрешим имя по новому тикеру (эффект-резолвер выше)
    }
    if (dl.clgroup) setClgroup(dl.clgroup);
    if (dl.interval) setIntervalValue(dl.interval);
    if (dl.period) setPeriod(dl.period);   // диплинк аномалии несёт period=1y; seed ловит только маунт
    if (dl.displayMode) setDisplayMode(dl.displayMode);
    if (dl.oiVariant) setOiVariant(dl.oiVariant);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams]);

  // showOi: в режиме 'price' открытый интерес не запрашиваем. Поднято сюда из
  // прежнего места ниже — нужно фетчеру useIndicatorData.
  const showOi = displayMode !== 'price';

  // Загрузка данных графика — generic-хук (вынесен из инлайн-loadData, поведение
  // идентично). onSuccess СИНХРОННО ставит dataInterval в одном React-18 batch с
  // data — атомарность производных filteredData/alignToCandles сохранена.
  const { data, loading, error } = useIndicatorData<ChartResponse>({
    fetcher: () => getChartData(selectedInstrument, selectedInstrument, 'futures', interval, clgroup, showOi, period),
    deps: [selectedInstrument, interval, clgroup, showOi, period],
    enabled: !!selectedInstrument,
    channels: ['5min', 'hourly'],
    tier: {
      showUpgrade,
      indicator: 'open_interest',
      featureName: (msg) => msg.replace(/^.*?: /, ''),
      tierResolver: oiTierResolver,
    },
    onSuccess: (result) => {
      setDataInterval(interval); // обновляем вместе с data (React 18 batches)
      if (result.available_intervals?.length > 0 &&
        !result.available_intervals.includes(interval)) {
        setIntervalValue(Math.max(...result.available_intervals));
      }
    },
  });

  // ── Алерты на графике (TradingView-стиль: «+» на оси) ──────────────────────
  const alertQuota = useCommonFeatures().telegram_alerts_quota;   // 0 / N / null(∞)
  const alertsLocked = alertQuota === 0;                          // Free/гость
  // Префилл модалки при клике «+»: метрика (цена / уровень ОИ) + порог = уровень +
  // текущее значение серии (для «Сейчас: …»).
  const [chartAlertPrefill, setChartAlertPrefill] = useState<{ metricKey: string; threshold: number; currentLabel?: string } | null>(null);
  // Активные алерты юзера на ТЕКУЩЕМ инструменте → горизонтальные линии уровней.
  const [myAlerts, setMyAlerts] = useState<AlertInfo[]>([]);
  const reloadMyAlerts = () => {
    if (!ALERTS_ENABLED || alertsLocked) { setMyAlerts([]); return; }
    listAlerts({ limit: 200 })
      .then((page) => setMyAlerts(page.items))
      .catch(() => setMyAlerts([]));
  };
  useEffect(() => { reloadMyAlerts(); /* eslint-disable-next-line react-hooks/exhaustive-deps */ }, [alertsLocked]);

  // Какая величина ОИ сейчас на правой оси (нога) — от режима и варианта. От этого
  // зависит, что ставит «+» на правой оси и как подписана метрика в модалке.
  const oiLeg: 'net' | 'long' | 'short' | 'oi' | 'npart' = useMemo(() => {
    if (displayMode === 'participants') return 'npart';
    switch (oiVariant) {
      case 'long': return 'long';
      case 'short': return 'short';
      case 'oi': return 'oi';
      case 'both': return 'net';   // две линии — по умолчанию чистая позиция
      default: return 'net';
    }
  }, [displayMode, oiVariant]);
  const oiLegLabel = OI_LEG_LABEL[oiLeg];
  const oiLegUnit = oiLeg === 'npart' ? 'участников' : 'контрактов';

  // Реестр метрик OI: цена + УРОВЕНЬ ОИ (нога+clgroup = как на графике) + аномалии.
  // Общий для кнопки-колокола и «+». oi_level отражает текущий вид графика.
  const oiAlertMetrics = useMemo<AlertMetricOption[]>(() => {
    const oiLevel: AlertMetricOption = {
      key: 'oi_level', label: `Открытый интерес — ${oiLegLabel}`,
      indicator: 'oi_level', metric: oiLeg, clgroup, unit: '',
      ops: [
        { value: 'cross_up', label: '↑ пересечёт (снизу вверх)' },
        { value: 'cross_down', label: '↓ пересечёт (сверху вниз)' },
        { value: 'gt', label: 'станет выше' },
        { value: 'lt', label: 'станет ниже' },
      ],
      hint: `Сработает, когда «${oiLegLabel}» (${clgroup === 'FIZ' ? 'физлица' : 'юрлица'}) пересечёт заданный уровень или окажется выше/ниже него. Величина — та, что показана на правой оси графика.`,
    };
    return [OI_ALERT_METRICS[0], oiLevel, ...OI_ALERT_METRICS.slice(1)];
  }, [clgroup, oiLeg, oiLegLabel]);

  // Уровни алертов текущего актива → пунктир: price на ЛЕВОЙ оси, oi_level на ПРАВОЙ.
  const alertLevels = useMemo(() => {
    if (!ALERTS_ENABLED) return undefined;
    type LevelLine = { value: number; color: string; axis: 'primary' | 'secondary' };
    const lines = myAlerts
      .filter((a) => a.status === 'active' && a.asset === selectedInstrument)
      .flatMap((a): LevelLine[] => {
        if (a.indicator === 'price') return [{ value: a.threshold, color: 'var(--accent)', axis: 'primary' }];
        if (a.indicator === 'oi_level') return [{ value: a.threshold, color: 'var(--accent)', axis: 'secondary' }];
        return [];
      });
    return lines.length ? lines : undefined;
  }, [myAlerts, selectedInstrument]);

  // Клик по «+» пилюле оси → открыть модалку с префиллом уровня + текущим значением.
  const handleCreateAlertFromChart = (p: { axis: 'primary' | 'secondary'; level: number; currentValue: number }) => {
    if (alertsLocked) {
      showUpgrade({ tier: 'basic', featureName: 'Алерты', indicator: 'alerts' });
      return;
    }
    const isOi = p.axis === 'secondary';
    const currentLabel = isOi
      ? `${Math.round(p.currentValue).toLocaleString('ru-RU')} ${oiLegUnit}`
      : `${formatPrice(p.currentValue)} ₽`;
    setChartAlertPrefill({ metricKey: isOi ? 'oi_level' : 'price', threshold: p.level, currentLabel });
  };

  // Алерты-на-графике выключены в режиме «Покупки + Продажи» (3 линии на графике —
  // цена + длинные + короткие; уровневый алерт двусмыслен). Кнопка-будильник для
  // ручного создания остаётся.
  const chartAlertsOn = ALERTS_ENABLED && !(displayMode !== 'price' && oiVariant === 'both');

  // Фильтрация нерабочих дней и пре-маркета.
  // Алгопак возвращает forward-fill данные за выходные, праздники и
  // пре-маркет (07:40-08:55) — значения идентичны предыдущему закрытию.
  const filteredData = useMemo(() => {
    if (!data) return null;

    // Праздники MOEX 2024-2026 (YYYY-MM-DD)
    const MOEX_HOLIDAYS = new Set([
      '2024-01-01','2024-01-02','2024-01-03','2024-01-04','2024-01-05','2024-01-08',
      '2024-02-23','2024-03-08','2024-05-01','2024-05-09','2024-06-12','2024-11-04','2024-12-31',
      '2025-01-01','2025-01-02','2025-01-03','2025-01-06','2025-01-07','2025-01-08',
      '2025-02-24','2025-03-10','2025-05-01','2025-05-02','2025-05-09','2025-06-12','2025-06-13',
      '2025-11-04','2025-12-31',
      '2026-01-01','2026-01-02','2026-01-05','2026-01-06','2026-01-07','2026-01-08',
      '2026-02-23','2026-03-09','2026-05-01','2026-05-11','2026-06-12','2026-11-04','2026-12-31',
    ]);

    const isNonTrading = (timeStr: string): boolean => {
      const d = new Date(timeStr);
      const day = d.getDay(); // 0=Sun, 6=Sat
      if (day === 0 || day === 6) return true;
      // Проверяем праздники по YYYY-MM-DD
      const dateKey = timeStr.slice(0, 10);
      if (MOEX_HOLIDAYS.has(dateKey)) return true;
      // Пре-маркет (до 09:00) — только для 5-мин
      if (dataInterval === 5 && d.getHours() < 9) return true;
      return false;
    };

    const filterItems = <T extends { time: string }>(items: T[]): T[] =>
      items.filter((item) => !isNonTrading(item.time));

    const newCandles = filterItems(data.candles);
    const newOi = filterItems(data.open_interest);

    // Если ничего не отфильтровалось — вернуть оригинал (избежать лишних ре-рендеров)
    if (newCandles.length === data.candles.length && newOi.length === data.open_interest.length) {
      return data;
    }

    return {
      ...data,
      candles: newCandles,
      open_interest: newOi,
    };
  }, [data, dataInterval]);

  const availableIntervals = filteredData?.available_intervals || [24];
  const hasInterval = (int: number) => availableIntervals.includes(int);

  // Ограничения периодов для интервалов (для производительности)
  // 5мин: макс 1 месяц, 1час: макс 6 месяцев, 1день: все
  const MAX_PERIODS_BY_INTERVAL: Record<number, Period[]> = {
    5: ['1w', '1m'],
    60: ['1w', '1m'],
    24: ['1w', '1m', '1y', '5y', 'all']
  };

  const isPeriodAvailable = (p: Period): boolean => {
    const allowed = MAX_PERIODS_BY_INTERVAL[interval] || MAX_PERIODS_BY_INTERVAL[24];
    return allowed.includes(p);
  };

  // Авто-переключение периода при смене интервала
  const handleIntervalChange = (newInterval: number) => {
    const allowed = MAX_PERIODS_BY_INTERVAL[newInterval] || MAX_PERIODS_BY_INTERVAL[24];
    setIntervalValue(newInterval);

    // Если текущий период недоступен — переключаем на максимальный доступный
    if (!allowed.includes(period)) {
      setPeriod(allowed[allowed.length - 1]);
    }
  };

  // Tier-коррекция дефолтного периода: гость дефолтит на '1y' (глобальный
  // GUEST_MAX_PERIOD), но open_interest free-лимит строже (180д) → 403 на первой
  // загрузке. Опускаем до максимального периода, доступного И по тарифу, И по
  // интервалу — гость видит данные, а не upgrade-модалку на входе. Клик в
  // locked-период сам показывает апселл (selector onLockedClick), не трогаем.
  useEffect(() => {
    if (oiAccess.isLoading) return;
    if (oiAccess.canUsePeriod(period)) return;
    const allowed = (MAX_PERIODS_BY_INTERVAL[interval] || MAX_PERIODS_BY_INTERVAL[24])
      .filter(p => oiAccess.canUsePeriod(p));
    if (allowed.length) setPeriod(allowed[allowed.length - 1]);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [oiAccess.isLoading, period, interval]);


  // Выбор инструмента из модалки
  const handleSelectInstrument = (sectype: string, name: string) => {
    setSelectedInstrument(sectype);
    setInstrumentName(name);
    setSearchParams({ instrument: sectype });
    setIsModalOpen(false);
  };

  // ── Вкладки страницы: график ⇄ «Скринер сигналов» ──
  // Состояние в URL (?tab=screener) — переживает перезагрузку и даёт диплинк.
  const activeTab: 'chart' | 'screener' =
    searchParams.get('tab') === 'screener' ? 'screener' : 'chart';
  const setActiveTab = (tab: 'chart' | 'screener') => {
    const next = new URLSearchParams(searchParams);
    if (tab === 'screener') next.set('tab', 'screener');
    else next.delete('tab');
    setSearchParams(next);
  };

  // Клик по строке скринера → вкладка графика с ЭТИМ активом и НАСТРОЙКАМИ
  // скринера: та же группа физ/юр, дневной ТФ (24), период 1 год — чтобы
  // график открывался ровно в том срезе, что показал сигнал. Диплинк
  // (parseOiDeepLink) применяет clgroup/interval/period; tab сбрасывается на
  // график, т.к. setSearchParams перетирает параметры (в т.ч. ?tab=screener).
  const handleScreenerSelect = (sectype: string, screenerClgroup: 'FIZ' | 'YUR') => {
    setSearchParams({
      instrument: sectype,
      clgroup: screenerClgroup,
      interval: '24',
      period: '1y',
    });
  };

  // Предложение поставить алерт из скринера (клик по «Создать алерт» в баннере
  // после добавления в избранное). Открывает ту же CreateAlertModal, что и
  // колокол, но для ВЫБРАННОГО в скринере актива и метрики «резкое движение
  // позиции» его группы (move_fiz/move_yur). Тариф-гейт как у колокола.
  const [screenerAlertAsset, setScreenerAlertAsset] =
    useState<{ sectype: string; name: string; clgroup: 'FIZ' | 'YUR' } | null>(null);
  const handleScreenerAlert = (sectype: string, name: string, clg: 'FIZ' | 'YUR') => {
    if (alertsLocked) {
      showUpgrade({ tier: 'basic', featureName: 'Алерты', indicator: 'alerts' });
      return;
    }
    setScreenerAlertAsset({ sectype, name, clgroup: clg });
  };

  // Данные для графика (мемоизированы — иначе каждый рендер создаёт новый массив,
  // что приводит к ложным перезапускам анимации в SimpleChart)
  const chartData = useMemo(() =>
    filteredData?.candles.map((c) => ({
      time: c.time,
      value: c.close,
      // OHLC — для типов «свечи»/«бары»/«Хайкен-Аши» (API отдаёт полный OHLCV)
      open: c.open,
      high: c.high,
      low: c.low,
    })) || []
    , [filteredData]);

  // Выравнивание OI данных по временным меткам свечей.
  // OI имеет меньше точек в час (нет 08:00, 18:00), что вызывает
  // временной сдвиг при index-based X mapping в SimpleChart.
  // Для каждой свечи берём последнее известное значение OI на этот момент.
  const alignToCandles = (
    oiSeries: { time: string; value: number }[] | undefined
  ): { time: string; value: number }[] | undefined => {
    if (!oiSeries || oiSeries.length === 0 || chartData.length === 0) return oiSeries;

    // Для дневных свечей: ключ по ДАТЕ (свечи T00:00:00, OI T23:50:00).
    // Для интрадей (5мин/1час): ключ по полному timestamp — OI и свечи
    // имеют одинаковые метки, и нужно сохранить внутридневную гранулярность.
    const isIntraday = dataInterval !== 24;

    const oiMap = new Map<string, number>();
    for (const p of oiSeries) {
      const key = isIntraday ? p.time : p.time.slice(0, 10);
      oiMap.set(key, p.value);
    }

    const aligned: { time: string; value: number }[] = [];
    let lastValue: number | null = null;

    for (const candle of chartData) {
      const key = isIntraday ? candle.time : candle.time.slice(0, 10);
      const val = oiMap.get(key);
      if (val !== undefined) {
        lastValue = val;
      }
      if (lastValue !== null) {
        aligned.push({ time: candle.time, value: lastValue });
      }
    }

    return aligned.length > 0 ? aligned : oiSeries;
  };

  const { secondary: oiData, third: oiDataThird } = useMemo(() => {
    if (!filteredData?.open_interest || displayMode === 'price') {
      return { secondary: undefined, third: undefined };
    }
    const isPositions = displayMode === 'positions';
    let secondary: { time: string; value: number }[] | undefined;
    let third: { time: string; value: number }[] | undefined;

    switch (oiVariant) {
      case 'oi':
        secondary = filteredData.open_interest.map((oi) => ({
          time: oi.time,
          value: isPositions
            ? (oi.pos_long || 0) + Math.abs(oi.pos_short || 0)
            : (oi.pos_long_num || 0) + (oi.pos_short_num || 0),
        }));
        break;
      case 'long':
        secondary = filteredData.open_interest.map((oi) => ({
          time: oi.time,
          value: isPositions ? (oi.pos_long || 0) : (oi.pos_long_num || 0),
        }));
        break;
      case 'short':
        secondary = filteredData.open_interest.map((oi) => ({
          time: oi.time,
          value: isPositions ? Math.abs(oi.pos_short || 0) : (oi.pos_short_num || 0),
        }));
        break;
      case 'both':
        secondary = filteredData.open_interest.map((oi) => ({
          time: oi.time,
          value: isPositions ? (oi.pos_long || 0) : (oi.pos_long_num || 0),
        }));
        third = filteredData.open_interest.map((oi) => ({
          time: oi.time,
          value: isPositions ? Math.abs(oi.pos_short || 0) : (oi.pos_short_num || 0),
        }));
        break;
      case 'net':
        secondary = filteredData.open_interest.map((oi) => ({
          time: oi.time,
          value: isPositions
            ? (oi.net_position ?? ((oi.pos_long || 0) + (oi.pos_short || 0)))
            : (oi.pos_long_num || 0) - (oi.pos_short_num || 0),
        }));
        break;
    }

    return {
      secondary: alignToCandles(secondary),
      third: alignToCandles(third),
    };
  }, [filteredData, displayMode, oiVariant, chartData]);

  const getColors = () => {
    switch (oiVariant) {
      case 'oi': return { secondary: COLORS.amber, third: '' };
      case 'long': return { secondary: COLORS.emerald, third: '' };
      case 'short': return { secondary: COLORS.rose, third: '' };
      case 'both': return { secondary: COLORS.emerald, third: COLORS.rose };
      case 'net': return { secondary: COLORS.cyan, third: '' };
      default: return { secondary: COLORS.amber, third: '' };
    }
  };

  const colors = getColors();

  const getLabels = () => {
    const isPositions = displayMode === 'positions';
    switch (oiVariant) {
      case 'oi': return { secondary: 'Открытый интерес', third: '' };
      case 'long': return { secondary: isPositions ? 'Покупки' : 'Покупатели', third: '' };
      case 'short': return { secondary: isPositions ? 'Продажи' : 'Продавцы', third: '' };
      case 'both': return {
        secondary: isPositions ? 'Покупки' : 'Покупатели',
        third: isPositions ? 'Продажи' : 'Продавцы'
      };
      case 'net': return { secondary: 'Чистая позиция', third: '' };
      default: return { secondary: '', third: '' };
    }
  };

  const labels = getLabels();

  // Аннотации экспираций для SimpleChart. Хук ОБЯЗАН жить на верхнем уровне
  // компонента (раньше был инлайном в JSX — с появлением вкладки «Скринер»
  // условный рендер графика менял число хуков между рендерами → крэш
  // «Rendered fewer hooks than expected»).
  const expirationAnnotations = useMemo(() => {
    if (!showExpirations) return undefined;
    const switches = filteredData?.contract_switches;
    if (!switches || switches.length <= 1) return undefined;
    return switches.slice(1).map((sw): ChartAnnotation => {
      return {
        time: sw.date,
        label: sw.to,
        description: `${sw.from} → ${sw.to}`,
        color: '#3a3f4f',
        textColor: '#9CA3B8',
      };
    });
  }, [filteredData?.contract_switches, showExpirations]);

  return (
    <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8">
      <div ref={headerRef}>
        <PageHeader
          icon={BarChart3}
          title="Открытые позиции"
          subtitle="Анализ позиций участников по фьючерсам MOEX"
          help={METHODOLOGY.oi}
          helpLink="/methodology/oi"
        />
      </div>

      {/* Карточка с вкладками: обёртка несёт единую editorial-тень на
          [вкладки + панель] (как на «Деньгах в фондах»). */}
      <div className="tabbed-card">

      {/* Вкладки: график ⇄ Скринер сигналов. Активная сливается с панелью
          ниже (has-tabs), без нижней линии. */}
      <ChartTabs<'chart' | 'screener'>
        value={activeTab}
        onChange={setActiveTab}
        tourId="screener-tabs"
        items={[
          { key: 'chart', label: 'Открытые позиции', Icon: BarChart3 },
          { key: 'screener', label: 'Скринер сигналов', badge: 'Beta', Icon: ListFilter },
        ]}
      />

      {/* Editorial frame — обнимает controls + chart в один контейнер с
          1.5px outline + hard-shadow 5×5×0 (как в design handoff page.jsx).
          В non-editorial темах класс не имеет стилей — структура остаётся
          плоской, как раньше. */}
      <div className="editorial-frame has-tabs">

      {activeTab === 'screener' && (
        <OiScreenerTable
          onSelect={handleScreenerSelect}
          onRequestAlert={ALERTS_ENABLED ? handleScreenerAlert : undefined}
        />
      )}

      {activeTab === 'chart' && (<>

      {/* Контролы в одну строку (на узких экранах wraps), editorial pill-стиль.
          Asset + FIZ/YUR + Interval (плитки) + Period + DisplayMode + OI variant.
          Низкочастотные тумблеры слоёв (Цена/Экспирации) — за иконкой «Слои»
          справа, рядом с камерой и колоколом (ml-auto кластер). */}
      <div ref={controlsRef} className="mb-4 md:mb-6">
        <div className="flex flex-wrap items-center gap-2 md:gap-3">
          {/* Селектор инструмента — открывает модалку */}
          <button
            data-tour="oi-instrument"
            onClick={() => setIsModalOpen(true)}
            title={instrumentName}
            className="widget-flat font-medium transition-colors flex items-center hover:opacity-90"
            style={{
              color: 'var(--text-primary)',
              fontSize: 'var(--fs-sm)',
              padding: 'var(--sp-2) var(--sp-4)',
              gap: 'var(--sp-3)',
              // Длинные имена («Биткоин (Индекс МосБиржи)») НЕ растягивают кнопку:
              // ширина ограничена maxWidth, имя обрезается многоточием (ellipsis
              // ниже + minWidth:0 на flex-обёртке). Полное имя — в title на ховере.
              minWidth: 'clamp(140px, 22vw, 170px)',
              maxWidth: 220,
            }}
          >
            <InstrumentIcon sectype={selectedInstrument} size={24} rounded="full" eager />
            <div className="flex-1 text-left" style={{ minWidth: 0 }}>
              <div className="font-medium" style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{instrumentName}</div>
              <div className="text-theme-secondary" style={{ fontSize: 'var(--fs-2xs)' }}>{frontContract || selectedInstrument}</div>
            </div>
            <ChevronDown size={14} className="text-theme-secondary flex-shrink-0" />
          </button>

          {/* Таймфрейм + Период */}
          <div data-tour="oi-timerange" className="flex" style={{ gap: 'var(--sp-2)', order: 1 }}>
          {/* Таймфрейм — плитки, не Dropdown: переключается часто, нужен один
              клик и видимый активный сегмент (решение 2026-06-12).
              Все три сегмента показываем ВСЕГДА. Если у актива нет внутридневных
              данных (ISS-only: крипта, неликвидные кроссы — только дневка), 5м/1ч
              не выкидываем, а помечаем disabled (серые + тултип, без замочка),
              иначе контрол схлопывался в одну странную кнопку «1д». Дневка (24)
              есть всегда. Замочек (locked) — отдельно, только тарифный гейт. */}
          <SegmentedControl<string>
            options={[5, 60, 24].map((int) => {
                const available = int === 24 || hasInterval(int);
                const allowedLegacy = isIntervalAllowed(int, isAuthenticated);
                const allowedTier = oiAccess.isLoading || oiAccess.canUseInterval(int);
                return {
                  key: String(int),
                  label: INTERVAL_LABELS[int as keyof typeof INTERVAL_LABELS],
                  // Нет данных → серый + тултип, без замочка и без апгрейда.
                  disabled: !available,
                  title: !available ? 'У этого инструмента нет данных в этом таймфрейме' : undefined,
                  // Тарифный лок — только когда данные есть.
                  locked: available && (!allowedLegacy || !allowedTier),
                };
              })}
            value={String(interval)}
            onChange={(k) => {
              const int = Number(k);
              const allowed = isIntervalAllowed(int, isAuthenticated);
              if (!allowed) { navigate('/login'); return; }
              const available = displayMode === 'price' || hasInterval(int);
              if (available) handleIntervalChange(int);
            }}
            onLockedClick={(k) => {
              const int = Number(k);
              // Если заблокировано по tier'у — показываем upgrade modal,
              // а не редиректим на /login (как для legacy guest gate).
              if (!oiAccess.canUseInterval(int)) {
                const requiredTier = oiAccess.requiredTierFor({ interval: int });
                if (requiredTier) {
                  showUpgrade({
                    tier: requiredTier,
                    featureName: `${int === 5 ? '5-минутный' : `${int}-часовой`} таймфрейм`,
                    indicator: 'open_interest',
                  });
                  return;
                }
              }
              // Иначе guest и не залогинен — на /login
              if (!isIntervalAllowed(int, isAuthenticated)) {
                navigate('/login');
              }
            }}
          />

          {/* Период — горизонтальный ряд (1Н / 1М / 1Г / 5Л / Всё) */}
          <SegmentedControl<Period>
            options={(Object.keys(PERIOD_LABELS) as Period[]).map((p) => ({
              key: p,
              label: PERIOD_LABELS[p],
              // Замочек только за тариф/гостевой гейт. Технически недоступный на
              // текущем ТФ период НЕ локаем — он кликабелен и сам переключит ТФ.
              // Гейт = глобальный guest (isPeriodAllowed) ИЛИ пер-индикаторный лимит
              // OI (canUsePeriod = бэковый max_history_days). Раньше был только
              // глобальный → период за пер-индикаторным лимитом кликабелен → 403.
              locked: !isPeriodAllowed(p, isAuthenticated) || !(oiAccess.isLoading || oiAccess.canUsePeriod(p)),
            }))}
            value={period}
            onChange={(p) => {
              // Период не влезает в текущий ТФ → переключаем на самый детальный ТФ,
              // который его поддерживает (есть у инструмента + открыт по тарифу).
              if (!isPeriodAvailable(p)) {
                const target = [5, 60, 24].find((int) =>
                  (MAX_PERIODS_BY_INTERVAL[int] ?? []).includes(p)
                  && hasInterval(int)
                  && (oiAccess.isLoading || oiAccess.canUseInterval(int))
                  && isIntervalAllowed(int, isAuthenticated)
                ) ?? 24;
                if (target !== interval) setIntervalValue(target);
              }
              setPeriod(p);
            }}
            onLockedClick={(p) => {
              // Tier-блокировка → upgrade modal; иначе legacy guest gate → /login.
              if (!oiAccess.canUsePeriod(p)) {
                const tier = oiAccess.requiredTierFor({ period: p });
                if (tier) {
                  showUpgrade({ tier, featureName: `период «${PERIOD_LABELS[p]}»`, indicator: 'open_interest' });
                  return;
                }
              }
              if (!isPeriodAllowed(p, isAuthenticated)) navigate('/login');
            }}
          />
          </div>{/* /oi-timerange */}

          {/* FIZ/YUR — выпадающий список (по запросу: как было раньше). Порядок: после периода. */}
          {displayMode !== 'price' && (
            <div data-tour="oi-clgroup">
            <Dropdown<'FIZ' | 'YUR'>
              options={[
                { key: 'FIZ', label: 'Физлица' },
                { key: 'YUR', label: 'Юрлица', locked: settingsLocked },
              ]}
              value={clgroup}
              onChange={setClgroup}
              onLockedClick={settingsLocked ? () => promptSettingsUpgrade('данные по юрлицам') : undefined}
            />
            </div>
          )}

          {/* Режим отображения — выпадающий список (Объём позиций / Число трейдеров) */}
          <div data-tour="oi-display-mode">
            <Dropdown<DisplayMode>
              options={[
                { key: 'positions', label: 'Объём позиций' },
                { key: 'participants', label: 'Число трейдеров', locked: settingsLocked },
              ]}
              value={displayMode}
              onChange={setDisplayMode}
              onLockedClick={settingsLocked ? () => promptSettingsUpgrade('режим числа трейдеров') : undefined}
            />
          </div>

          {/* Варианты OI — каждый с цветной полоской слева */}
          {displayMode !== 'price' && (
            <div data-tour="oi-variant">
            <Dropdown<OIVariant>
              options={[
                { key: 'oi',    label: 'Открытый интерес',                                                     color: 'var(--oi-amber)', help: OI_VARIANT_HELP.oi, locked: settingsLocked },
                { key: 'long',  label: displayMode === 'positions' ? 'Покупки' : 'Покупатели',                 color: 'var(--oi-green)',  locked: settingsLocked },
                { key: 'short', label: displayMode === 'positions' ? 'Продажи' : 'Продавцы',                   color: 'var(--oi-red)',    locked: settingsLocked },
                { key: 'both',  label: displayMode === 'positions' ? 'Покупки + Продажи' : 'Покупатели + Продавцы', color: 'var(--oi-purple)', locked: settingsLocked },
                { key: 'net',   label: 'Чистая позиция',                                                       color: 'var(--oi-cyan)', help: OI_VARIANT_HELP.net },
              ]}
              value={oiVariant}
              onChange={setOiVariant}
              onLockedClick={settingsLocked ? () => promptSettingsUpgrade('другие показатели открытого интереса') : undefined}
            />
            </div>
          )}

          {/* Действия (Слои/Скриншот/CSV/Алерт) свёрнуты в kebab «⋮». JSX тут,
              в строке контролов (рядом со state), но через portal монтируется
              в угол графика (containerRef=chartAnchorRef) — ряд свободен. */}
          <ChartActionsMenu containerRef={chartAnchorRef} tourId="oi-export">
          {displayMode !== 'price' && (
            <LayersButton
              tourId="oi-layers"
              layers={[
                { key: 'price', label: 'Цена', hint: 'Линия цены фьючерса', checked: showPrice, onChange: setShowPrice },
                { key: 'expirations', label: 'Экспирации', hint: 'Метки смены контракта', checked: showExpirations, onChange: setShowExpirations },
              ]}
            />
          )}
          <CsvExportButton
            indicator="open_interest"
            config={() => {
              const periodDays: Record<string, number> = {
                '1d': 2, '1w': 7, '1m': 30, '3m': 90, '6m': 180,
                '1y': 365, '2y': 730, '5y': 1825, 'all': 7000,
              };
              return {
                indicator: 'open_interest',
                title: `Экспорт: Открытые позиции · ${instrumentName}`,
                layers: [{
                  id: 'oi',
                  label: 'История позиций',
                  description: 'trade_date, trade_time, open_interest, pos_long/short, число участников',
                  defaultSelected: true,
                }],
                selectors: [
                  {
                    kind: 'instrument-picker',
                    id: 'instruments',
                    label: 'Инструменты (фьючерсы)',
                    default: [selectedInstrument],
                    filterType: 'futures',
                    pickerTitle: 'Выберите фьючерсы для экспорта',
                    hint: 'Несколько → ZIP с CSV per инструмент',
                  },
                  {
                    kind: 'multiselect',
                    id: 'clgroups',
                    label: 'Категория участников',
                    default: [clgroup],
                    hint: 'Оба → 2 CSV в ZIP',
                    options: [
                      { value: 'YUR', label: 'Юрлица' },
                      { value: 'FIZ', label: 'Физлица' },
                    ],
                  },
                  {
                    kind: 'multiselect',
                    id: 'intervals',
                    label: 'Таймфрейм',
                    default: [String(interval)],
                    hint: 'Несколько → ZIP с CSV per таймфрейм',
                    options: [
                      { value: '5', label: '5 мин' },
                      { value: '60', label: '1 час' },
                      { value: '24', label: '1 день' },
                    ],
                  },
                  {
                    kind: 'period',
                    id: 'period',
                    label: 'Период',
                    default: { type: 'preset', value: period },
                    presets: [
                      { value: '1m', label: '1М', days: 30 },
                      { value: '1y', label: '1Г', days: 365 },
                      { value: 'all', label: 'Всё', days: 7000 },
                    ],
                  },
                ],
                params: [],
                buildUrl: (_layers, vals) => {
                  const insts = (vals.instruments as string[] ?? [selectedInstrument]).join(',');
                  const cls = (vals.clgroups as string[] ?? [clgroup]).join(',');
                  const ints = (vals.intervals as string[] ?? [String(interval)]).join(',');
                  const periodParam = periodToQuery(vals.period, periodDays[period] ?? 365);
                  return `/api/export/oi.csv?instrument=${encodeURIComponent(insts)}&clgroup=${cls}&interval=${ints}&${periodParam}`;
                },
                buildFilename: () => `oi_${Date.now()}.zip`,
              };
            }}
          />
          <ChartCaptureButton
            getTargetElement={() => chartAnchorRef.current}
            filename={`frame-oi-${selectedInstrument.toLowerCase()}-${interval}-${displayMode}`}
            metadata={{
              // displayMode сразу в title — попадает в первую строку subtitle
              // экспорта («Открытые позиции — Объём позиций · 1 час · ...»), сразу
              // видно режим без копания в tag-list.
              title: `Открытые позиции — ${displayMode === 'price' ? 'Цена' : displayMode === 'positions' ? 'Объём позиций' : 'Число трейдеров'}`,
              // Не фолбэчим на ticker — иначе при ещё-не-загрузившемся instrumentName
              // получим asset=ticker и дубликат в header. composeFramedCanvas сам
              // сделает primary fallback на title если asset undefined.
              asset: instrumentName || undefined,
              ticker: frontContract || selectedInstrument,
              details: [
                INTERVAL_LABELS[interval as keyof typeof INTERVAL_LABELS] || `${interval}ч`,
                PERIOD_LABELS[period],
                clgroup === 'FIZ' ? 'Физлица' : 'Юрлица',
              ].filter(Boolean),
            }}
          />
          <ChartSettings ohlcHere scopeLabels={{ primary: 'Цена', secondary: 'Линии ОИ' }} />
          {ALERTS_ENABLED && (
          <AlertBellButton
            indicator="open_interest"
            asset={selectedInstrument}
            assetName={instrumentName || selectedInstrument}
            metrics={oiAlertMetrics}
          />
          )}
          </ChartActionsMenu>
        </div>
      </div>

      {/* Ошибка */}
      {error && (
        <div className="rounded-xl p-4 mb-6" style={{ backgroundColor: 'color-mix(in srgb, var(--danger) 10%, transparent)', border: '1px solid color-mix(in srgb, var(--danger) 30%, transparent)' }}>
          <p className="text-theme-danger">{error}</p>
        </div>
      )}

      {/* График — фон и border заданы внутри SimpleChart (bg-theme-primary,
          border + hard shadow в editorial). Обёртка убрана чтобы не было
          двойной рамки. chartAnchorRef нужен хуку useFitToViewport для
          расчёта высоты графика «остаток до низа viewport». */}
      <div ref={chartAnchorRef} data-tour="oi-chart" style={{ position: 'relative' }}>
      <SimpleChart
        data={chartData}
        secondaryData={oiData}
        thirdData={oiDataThird}
        showPrimary={displayMode === 'price' || showPrice}
        showSecondary={displayMode !== 'price' && !!oiData}
        showThird={oiVariant === 'both' && !!oiDataThird}
        primaryColor={COLORS.primary}
        secondaryColor={colors.secondary}
        thirdColor={colors.third}
        height={chartHeight}
        loading={loading}
        formatValue={formatPrice}
        niceTicks={true}
        niceTicksSecondary={true}
        gridAxis="secondary"
        primaryLabel={instrumentName || selectedInstrument}
        secondaryLabel={labels.secondary}
        thirdLabel={labels.third}
        // «+» на ОБЕИХ осях: цена слева, ОИ справа. SimpleChart рисует пилюлю только
        // для показанной серии (цена — если showPrimary, ОИ — если showSecondary).
        // В режиме «Покупки + Продажи» (oiVariant='both') на графике 3 линии
        // (цена + длинные + короткие) — уровневый алерт двусмыслен, убираем
        // горизонтальный кросхэйр и «+» целиком.
        onCreateAlert={chartAlertsOn ? handleCreateAlertFromChart : undefined}
        alertAxes={chartAlertsOn ? ['primary', 'secondary'] : undefined}
        horizontalLines={alertLevels}
        showValueHeader={false}
        legendPosition="top"
        showDownloadButton={false}
        showNavigator={true}
        // Симметричные padding'и (left=right=120). Для больших OI значений
        // ("12 345 678" нетто-позиции CR/RI/др. ликвидных, 10+ chars ~85-90px)
        // padRight=100 не помещался — labels уходили за край. 120/120 даёт
        // ~116px labels area с обеих сторон, симметрия chart area сохранена.
        chartPadding={{ left: 120, right: 120 }}
        annotations={expirationAnnotations}
      />
      </div>{/* /chartAnchorRef */}

      </>)}{/* /activeTab === 'chart' */}

      </div>{/* /editorial-frame */}
      </div>{/* /tabbed-card */}

      {/* Модалка выбора инструмента */}
      {isModalOpen && (
        <InstrumentSearchModal
          onSelect={handleSelectInstrument}
          onClose={() => setIsModalOpen(false)}
          filterType="futures"
          indicator="open_interest"
        />
      )}

      {/* Модалка алерта, открытая кликом «+» на графике (префилл: цена + уровень). */}
      {ALERTS_ENABLED && chartAlertPrefill && (
        <CreateAlertModal
          indicator="open_interest"
          asset={selectedInstrument}
          assetName={instrumentName || selectedInstrument}
          metrics={oiAlertMetrics}
          prefill={{ metricKey: chartAlertPrefill.metricKey, threshold: chartAlertPrefill.threshold, currentLabel: chartAlertPrefill.currentLabel }}
          onClose={() => { setChartAlertPrefill(null); reloadMyAlerts(); }}
        />
      )}

      {/* Алерт из скринера — та же модалка, но для выбранного там актива и
          метрики «резкое движение позиции» его группы. */}
      {ALERTS_ENABLED && screenerAlertAsset && (
        <CreateAlertModal
          indicator="open_interest"
          asset={screenerAlertAsset.sectype}
          assetName={screenerAlertAsset.name}
          metrics={oiAlertMetrics}
          prefill={{ metricKey: screenerAlertAsset.clgroup === 'FIZ' ? 'move_fiz' : 'move_yur' }}
          onClose={() => { setScreenerAlertAsset(null); reloadMyAlerts(); }}
        />
      )}

      {/* Onboarding tour — по вкладкам: свой набор шагов и свой ключ первого
          визита для графика и для скринера. Чартовый тур не стартует на
          скринере (его якоря скрыты), и наоборот. */}
      <OnboardingTour
        steps={activeTab === 'screener' ? oiScreenerTourSteps : oiTourSteps}
        open={activeTab === 'screener' ? screenerTour.open : tour.open}
        onClose={activeTab === 'screener' ? screenerTour.close : tour.close}
      />
    </div>
  );
}