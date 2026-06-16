import { useEffect, useState, useMemo, useRef } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { ChevronDown, BarChart3 } from 'lucide-react';
import PageHeader from '../components/PageHeader';
import InstrumentIcon from '../components/InstrumentIcon';
import { usePrefetchLogos } from '../hooks/usePrefetchLogos';
import { METHODOLOGY } from '../data/methodology';
import { getChartData, getInstrument } from '../services/api';
import type { ChartResponse } from '../types';
import type { ChartAnnotation } from '../components/SimpleChart';
import SimpleChart from '../components/SimpleChart';
import ChartCaptureButton from '../components/export/ChartCaptureButton';
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
import { formatPrice } from '../utils/formatNumber';
import { useUpgradePrompt } from '../components/tier/UpgradeModal';
import { oiTierResolver } from '../utils/tierError';
import AlertBellButton from '../components/alerts/AlertBellButton';
import { ALERTS_ENABLED } from '../config/alertsConfig';
import { useTierAccess } from '../contexts/TierFeaturesContext';

type DisplayMode = 'price' | 'positions' | 'participants';
type OIVariant = 'oi' | 'long' | 'short' | 'both' | 'net';
type Period = '1w' | '1m' | '1y' | '5y' | 'all';

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

  // Данные графика грузятся через useIndicatorData (ниже, после контролов —
  // фетчеру нужны interval/clgroup/showOi/period).
  // Интервал, для которого загружены текущие данные — обновляется атомарно с data
  const [dataInterval, setDataInterval] = useState(24);

  // Onboarding tour — единый хук обёртка. Auto-open ровно один раз
  // на mount (autoOpenedRef guard внутри хука).
  const tour = useOnboardingTour('oi');

  // Настройки (персистятся в localStorage по индикатору — не сбрасываются на новой сессии)
  const [interval, setIntervalValue] = usePersistedState('frame:oi:interval', 24);
  const [clgroup, setClgroup] = usePersistedState<'FIZ' | 'YUR'>('frame:oi:clgroup', 'YUR');
  const [displayMode, setDisplayMode] = usePersistedState<DisplayMode>('frame:oi:displayMode', 'positions');
  const [oiVariant, setOiVariant] = usePersistedState<OIVariant>('frame:oi:oiVariant', 'net');
  const [showExpirations, setShowExpirations] = usePersistedState('frame:oi:showExpirations', false);
  const [showPrice, setShowPrice] = usePersistedState('frame:oi:showPrice', true);
  const [period, setPeriod] = usePersistedState<Period>('frame:oi:period', getDefaultPeriod('1y', isAuthenticated) as Period);

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

  // Данные для графика (мемоизированы — иначе каждый рендер создаёт новый массив,
  // что приводит к ложным перезапускам анимации в SimpleChart)
  const chartData = useMemo(() =>
    filteredData?.candles.map((c) => ({
      time: c.time,
      value: c.close,
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

      {/* Editorial frame — обнимает controls + chart в один контейнер с
          1.5px outline + hard-shadow 5×5×0 (как в design handoff page.jsx).
          В non-editorial темах класс не имеет стилей — структура остаётся
          плоской, как раньше. */}
      <div className="editorial-frame">

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
              <div className="text-theme-secondary" style={{ fontSize: 'var(--fs-2xs)' }}>{selectedInstrument}</div>
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
                { key: 'YUR', label: 'Юрлица' },
              ]}
              value={clgroup}
              onChange={setClgroup}
            />
            </div>
          )}

          {/* Режим отображения — выпадающий список (Объём позиций / Число трейдеров) */}
          <div data-tour="oi-display-mode">
            <Dropdown<DisplayMode>
              options={[
                { key: 'positions', label: 'Объём позиций' },
                { key: 'participants', label: 'Число трейдеров' },
              ]}
              value={displayMode}
              onChange={setDisplayMode}
            />
          </div>

          {/* Варианты OI — каждый с цветной полоской слева */}
          {displayMode !== 'price' && (
            <div data-tour="oi-variant">
            <Dropdown<OIVariant>
              options={[
                { key: 'oi',    label: 'Открытый интерес',                                                     color: 'var(--oi-amber)',
                  help: {
                    title: 'Открытый интерес',
                    content: 'Общее число открытых позиций по инструменту — сумма позиций на покупку и на продажу. Каждая сделка считается один раз: у неё есть покупатель и продавец, вместе они образуют одну позицию. Открывают позиции — значение растёт, закрывают — снижается.',
                  } },
                { key: 'long',  label: displayMode === 'positions' ? 'Покупки' : 'Покупатели',                 color: 'var(--oi-green)' },
                { key: 'short', label: displayMode === 'positions' ? 'Продажи' : 'Продавцы',                   color: 'var(--oi-red)' },
                { key: 'both',  label: displayMode === 'positions' ? 'Покупки + Продажи' : 'Покупатели + Продавцы', color: 'var(--oi-purple)' },
                { key: 'net',   label: 'Чистая позиция',                                                       color: 'var(--oi-cyan)',
                  help: {
                    title: 'Чистая позиция',
                    content: 'Разница между объёмом позиций на покупку и на продажу. Выше нуля — перевес покупателей, ниже нуля — перевес продавцов.',
                  } },
              ]}
              value={oiVariant}
              onChange={setOiVariant}
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
              ticker: selectedInstrument,
              details: [
                INTERVAL_LABELS[interval as keyof typeof INTERVAL_LABELS] || `${interval}ч`,
                PERIOD_LABELS[period],
                clgroup === 'FIZ' ? 'Физлица' : 'Юрлица',
              ].filter(Boolean),
            }}
          />
          {ALERTS_ENABLED && (
          <AlertBellButton
            indicator="open_interest"
            asset={selectedInstrument}
            assetName={instrumentName || selectedInstrument}
            metrics={[
              {
                // «move_all» (clgroup ALL) — ПЕРВАЯ, дефолтная. Считается по тем же
                // net-данным (источник net — FIZ), но текст НЕЙТРАЛЬНЫЙ, без роли
                // субъекта (физ/юр). Это «общий ракурс» одного и того же сигнала.
                key: 'move_all',
                label: 'Резкое движение позиции — в целом',
                indicator: 'oi_move', metric: 'atr', clgroup: 'ALL', unit: '×', defaultThreshold: 3,
                ops: [{ value: 'gt', label: 'превысит' }],
                hint: 'Сработает, когда чистая позиция изменится за день резче обычного — во столько-то раз больше среднего дневного шага за 14 дней (ATR). 2× — заметно, 3× — сильно, 5× — экстремально. Обновляется раз в день после публикации позиций МосБиржи; это описание движения, не прогноз цены.',
              },
              {
                key: 'move_fiz',
                label: 'Резкое движение позиции — физлица',
                indicator: 'oi_move', metric: 'atr', clgroup: 'FIZ', unit: '×', defaultThreshold: 3,
                ops: [{ value: 'gt', label: 'превысит' }],
                hint: 'Сработает, когда чистая позиция физлиц изменится за день резче обычного — во столько-то раз больше среднего дневного шага за 14 дней (ATR). 2× — заметно, 3× — сильно, 5× — экстремально. Обновляется раз в день после публикации позиций МосБиржи; это описание движения, не прогноз цены.',
              },
              {
                key: 'move_yur',
                label: 'Резкое движение позиции — юрлица',
                indicator: 'oi_move', metric: 'atr', clgroup: 'YUR', unit: '×', defaultThreshold: 3,
                ops: [{ value: 'gt', label: 'превысит' }],
                hint: 'Сработает, когда чистая позиция юрлиц изменится за день резче обычного — во столько-то раз больше среднего дневного шага за 14 дней (ATR). 2× — заметно, 3× — сильно, 5× — экстремально. Обновляется раз в день после публикации позиций МосБиржи; это описание движения, не прогноз цены.',
              },
              {
                // Участники НЕ зеркальны: счётчики физ и юр независимы (оба
                // положительные) → самостоятельный сигнал, не дубликат part_yur.
                key: 'part_fiz',
                label: 'Резкое изменение числа трейдеров — физлица',
                indicator: 'oi_participants', metric: 'atr', clgroup: 'FIZ', unit: '×', defaultThreshold: 3,
                ops: [{ value: 'gt', label: 'превысит' }],
                hint: 'Сработает, когда число физлиц-участников изменится за день резче обычного — во столько-то раз больше среднего дневного шага за 14 дней (ATR). 2× — заметно, 3× — сильно, 5× — экстремально. Обновляется раз в день после публикации позиций МосБиржи; это описание движения, не прогноз цены.',
              },
              {
                key: 'part_yur',
                label: 'Резкое изменение числа трейдеров — юрлица',
                indicator: 'oi_participants', metric: 'atr', clgroup: 'YUR', unit: '×', defaultThreshold: 3,
                ops: [{ value: 'gt', label: 'превысит' }],
                hint: 'Сработает, когда число юрлиц-участников изменится за день резче обычного — во столько-то раз больше среднего дневного шага за 14 дней (ATR). 2× — заметно, 3× — сильно, 5× — экстремально. Обновляется раз в день после публикации позиций МосБиржи; это описание движения, не прогноз цены.',
              },
            ]}
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
        primaryLabel={instrumentName || selectedInstrument}
        secondaryLabel={labels.secondary}
        thirdLabel={labels.third}
        showValueHeader={false}
        legendPosition="top"
        showDownloadButton={false}
        showNavigator={true}
        // Симметричные padding'и (left=right=120). Для больших OI значений
        // ("12 345 678" нетто-позиции CR/RI/др. ликвидных, 10+ chars ~85-90px)
        // padRight=100 не помещался — labels уходили за край. 120/120 даёт
        // ~116px labels area с обеих сторон, симметрия chart area сохранена.
        chartPadding={{ left: 120, right: 120 }}
        annotations={useMemo(() => {
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
        }, [filteredData?.contract_switches, showExpirations])}
      />
      </div>{/* /chartAnchorRef */}

      </div>{/* /editorial-frame */}

      {/* Легенда — оформлена как editorial card (frame с hard shadow в editorial,
          обычная widget панель в OKX/dark). Inner bg = secondary чтобы выделяться
          на page-bg, в editorial CSS override применит outline + hard shadow. */}
      <div data-tour="oi-legend" className="mt-6 bg-theme-secondary border border-theme rounded-2xl widget" style={{ padding: 'var(--sp-5)' }}>
        <div style={{ fontSize: 'var(--fs-sm)', display: 'flex', flexDirection: 'column', gap: 'var(--sp-3)' }}>
          {/* Линии графика */}
          <div className="flex items-start" style={{ gap: 'var(--sp-3)' }}>
            <span className="legend-dot mt-1.5" style={{ backgroundColor: COLORS.primary }} />
            <div>
              <span className="font-medium" style={{ color: COLORS.primary }}>График цены</span>
              <span className="text-theme-secondary"> — стоимость фьючерса на срочном рынке</span>
            </div>
          </div>

          <div className="flex items-start" style={{ gap: 'var(--sp-3)' }}>
            <span className="legend-dot mt-1.5" style={{ backgroundColor: COLORS.amber }} />
            <div>
              <span className="font-medium" style={{ color: COLORS.amber }}>Открытый интерес</span>
              <span className="text-theme-secondary"> — сумма позиций на покупку и на продажу</span>
            </div>
          </div>

          <div className="flex items-start" style={{ gap: 'var(--sp-3)' }}>
            <span className="legend-dot mt-1.5" style={{ backgroundColor: COLORS.emerald }} />
            <div>
              <span className="font-medium" style={{ color: COLORS.emerald }}>Покупки</span>
              <span className="text-theme-secondary"> — объём позиций на рост / количество покупателей</span>
            </div>
          </div>

          <div className="flex items-start" style={{ gap: 'var(--sp-3)' }}>
            <span className="legend-dot mt-1.5" style={{ backgroundColor: COLORS.rose }} />
            <div>
              <span className="font-medium" style={{ color: COLORS.rose }}>Продажи</span>
              <span className="text-theme-secondary"> — объём позиций на падение / количество продавцов</span>
            </div>
          </div>

          <div className="flex items-start" style={{ gap: 'var(--sp-3)' }}>
            <span className="legend-dot mt-1.5" style={{ backgroundColor: COLORS.cyan }} />
            <div>
              <span className="font-medium" style={{ color: COLORS.cyan }}>Чистая позиция</span>
              <span className="text-theme-secondary"> — разница между покупками и продажами</span>
            </div>
          </div>

          {/* Режимы */}
          <div className="pt-3 border-t border-theme mt-4">
            <div className="text-theme-secondary">
              <span className="font-medium text-theme-primary">Режимы:</span>{' '}
              <span style={{ color: COLORS.primary }}>Объём позиций</span> — сколько контрактов куплено или продано,{' '}
              <span style={{ color: COLORS.primary }}>Число трейдеров</span> — сколько человек или компаний держит позиции
            </div>
          </div>
        </div>
      </div>

      {/* Модалка выбора инструмента */}
      {isModalOpen && (
        <InstrumentSearchModal
          onSelect={handleSelectInstrument}
          onClose={() => setIsModalOpen(false)}
          filterType="futures"
          indicator="open_interest"
        />
      )}

      {/* Onboarding tour — spotlight гайд для первого визита.
          Авто-открывается через useFirstVisit, можно перезапустить
          через методологию (см. /methodology/oi). */}
      <OnboardingTour
        steps={oiTourSteps}
        open={tour.open}
        onClose={tour.close}
      />
    </div>
  );
}