import { useEffect, useState, useMemo, useCallback } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { ChevronDown, BarChart3, Lock } from 'lucide-react';
import PageHeader from '../components/PageHeader';
import InstrumentIcon from '../components/InstrumentIcon';
import { usePrefetchLogos } from '../hooks/usePrefetchLogos';
import { METHODOLOGY } from '../data/methodology';
import { getChartData, getInstrument } from '../services/api';
import type { ChartResponse } from '../types';
import type { ChartAnnotation } from '../components/SimpleChart';
import SimpleChart from '../components/SimpleChart';
import InstrumentSearchModal from '../components/InstrumentSearchModal';
import { PERIOD_LABELS as ALL_PERIOD_LABELS, INTERVAL_LABELS } from '../config/chartConfig';
import { useAuth } from '../contexts/AuthContext';
import { useTheme } from '../contexts/ThemeContext';
import { isIntervalAllowed, isPeriodAllowed, getDefaultPeriod } from '../config/accessControl';
import { useRealtimeData } from '../hooks/useRealtimeData';

type DisplayMode = 'price' | 'positions' | 'participants';
type OIVariant = 'oi' | 'long' | 'short' | 'both' | 'net';
type Period = '1d' | '1w' | '1m' | '3m' | '6m' | '1y' | '2y' | '5y' | 'all';

const PERIOD_LABELS: Record<Period, string> = {
  '1d':  ALL_PERIOD_LABELS['1d'],
  '1w':  ALL_PERIOD_LABELS['1w'],
  '1m':  ALL_PERIOD_LABELS['1m'],
  '3m':  ALL_PERIOD_LABELS['3m'],
  '6m':  ALL_PERIOD_LABELS['6m'],
  '1y':  ALL_PERIOD_LABELS['1y'],
  '2y':  ALL_PERIOD_LABELS['2y'],
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
  const { theme } = useTheme();
  const navigate = useNavigate();
  const isEditorial = theme.startsWith('editorial');

  // Фоновая предзагрузка лого один раз — модалка выбора актива потом
  // открывается мгновенно из SW cache, без 100 запросов.
  usePrefetchLogos();

  // Инструмент
  // ВАЖНО: sec_id может прийти из URL-параметра (например `?instrument=IMOEXF`),
  // а instrumentName инициализируется дефолтом 'Сбербанк'. Поэтому ниже стоит
  // useEffect который при первом рендере (или при любом расхождении ticker/name)
  // резолвит имя через /api/instruments/{sec_id} — иначе был баг
  // "Сбербанк [IMOEXF]" в UI-кнопке.
  const [selectedInstrument, setSelectedInstrument] = useState(
    searchParams.get('instrument') || 'SR'
  );
  const [instrumentName, setInstrumentName] = useState(
    searchParams.get('instrument') ? '' : 'Сбербанк'  // пустое имя если пришло из URL — будет резолвлено
  );
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

  // Данные графика
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<ChartResponse | null>(null);
  // Интервал, для которого загружены текущие данные — обновляется атомарно с data
  const [dataInterval, setDataInterval] = useState(24);

  // Настройки
  const [interval, setIntervalValue] = useState(24);
  const [clgroup, setClgroup] = useState<'FIZ' | 'YUR'>('FIZ');
  const [displayMode, setDisplayMode] = useState<DisplayMode>('positions');
  const [oiVariant, setOiVariant] = useState<OIVariant>('oi');
  const [showExpirations, setShowExpirations] = useState(false);
  const [period, setPeriod] = useState<Period>(getDefaultPeriod('6m', isAuthenticated) as Period);

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
  const showOi = displayMode !== 'price';

  // Ограничения периодов для интервалов (для производительности)
  // 5мин: макс 1 месяц, 1час: макс 6 месяцев, 1день: все
  const MAX_PERIODS_BY_INTERVAL: Record<number, Period[]> = {
    5: ['1d', '1w', '1m'],
    60: ['1d', '1w', '1m', '3m', '6m'],
    24: ['1d', '1w', '1m', '3m', '6m', '1y', '2y', '5y', 'all']
  };

  const isPeriodAvailable = (p: Period): boolean => {
    const allowed = MAX_PERIODS_BY_INTERVAL[interval] || MAX_PERIODS_BY_INTERVAL[24];
    return allowed.includes(p);
  };

  // Авто-переключение периода при смене интервала
  const handleIntervalChange = (newInterval: number) => {
    const allowed = MAX_PERIODS_BY_INTERVAL[newInterval] || MAX_PERIODS_BY_INTERVAL[24];
    setIntervalValue(newInterval);

    // 1Д ТФ + 1Д период не имеет смысла → переключаем на 1Н
    if (newInterval === 24 && period === '1d') {
      setPeriod('1w');
      return;
    }

    // Если текущий период недоступен — переключаем на максимальный доступный
    if (!allowed.includes(period)) {
      setPeriod(allowed[allowed.length - 1]);
    }
  };

  // Загрузка данных графика
  const loadData = useCallback(async () => {
    if (!selectedInstrument) return;

    try {
      setLoading(true);
      setError(null);
      const result = await getChartData(
        selectedInstrument,
        selectedInstrument,
        'futures',
        interval,
        clgroup,
        showOi,
        period
      );
      setData(result);
      setDataInterval(interval); // обновляем вместе с data (React 18 batches)

      if (result.available_intervals?.length > 0 &&
        !result.available_intervals.includes(interval)) {
        setIntervalValue(Math.max(...result.available_intervals));
      }
    } catch (err) {
      setError('Ошибка загрузки данных');
      console.error(err);
    } finally {
      setLoading(false);
    }
  }, [selectedInstrument, interval, clgroup, showOi, period]);

  useEffect(() => { loadData(); }, [loadData]);

  // SSE: автоматическое обновление при новых данных
  useRealtimeData(['5min', 'hourly'], loadData);

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
    <div className="max-w-7xl mx-auto px-4 md:px-6 py-6 md:py-8">
      <PageHeader
        icon={BarChart3}
        title="Открытый интерес"
        subtitle="Анализ позиций участников по фьючерсам MOEX"
        help={METHODOLOGY.oi}
        helpLink="/methodology/oi"
      />

      {/* Editorial frame — обнимает controls + chart в один контейнер с
          1.5px outline + hard-shadow 5×5×0 (как в design handoff page.jsx).
          В non-editorial темах класс не имеет стилей — структура остаётся
          плоской, как раньше. */}
      <div className="editorial-frame">

      {/* Контролы — без widget-wrapper, консистентно с остальными страницами
          (Buffett, Sezonality, Heatmap — controls прямо на page bg) */}
      <div className="mb-4 md:mb-6">
        <div className="flex flex-wrap items-center gap-2 md:gap-3 mb-3 md:mb-4">
          {/* Селектор инструмента — открывает модалку */}
          <button
            onClick={() => setIsModalOpen(true)}
            className="widget-flat px-3 md:px-4 py-2 md:py-2.5 text-sm font-medium transition-colors flex items-center gap-2 md:gap-3 min-w-[160px] md:min-w-[200px] hover:opacity-90"
            style={{ color: 'var(--text-primary)' }}
          >
            <InstrumentIcon sectype={selectedInstrument} size={28} rounded="full" eager />
            <div className="flex-1 text-left">
              <div className="font-medium">{instrumentName}</div>
              <div className="text-xs text-theme-secondary">{selectedInstrument}</div>
            </div>
            <ChevronDown size={16} className="text-theme-secondary" />
          </button>

          {/* FIZ/YUR переключатель */}
          {displayMode !== 'price' && (
            <div className="btn-group-scroll bg-theme-secondary rounded-xl border border-theme p-1">
              <button
                onClick={() => setClgroup('FIZ')}
                className={`px-4 py-2 text-sm font-medium rounded-lg transition-colors duration-200 ${clgroup === 'FIZ'
                  ? 'btn-control active'
                  : 'text-theme-secondary hover:text-theme-primary'
                  }`}
              >
                Физлица
              </button>
              <button
                onClick={() => setClgroup('YUR')}
                className={`px-4 py-2 text-sm font-medium rounded-lg transition-colors duration-200 ${clgroup === 'YUR'
                  ? 'btn-control active'
                  : 'text-theme-secondary hover:text-theme-primary'
                  }`}
              >
                Юрлица
              </button>
            </div>
          )}

          {/* Таймфрейм */}
          <div className="btn-group-scroll bg-theme-secondary rounded-xl border border-theme p-1">
            {[5, 60, 24].map((int) => {
              const available = displayMode === 'price' || hasInterval(int);
              const allowed = isIntervalAllowed(int, isAuthenticated);
              return (
                <button
                  key={int}
                  onClick={() => {
                    if (!allowed) { navigate('/login'); return; }
                    if (available) handleIntervalChange(int);
                  }}
                  disabled={!available && allowed}
                  title={!allowed ? 'Войдите для доступа' : undefined}
                  className={`px-3 py-2 text-sm font-medium rounded-lg transition-colors duration-200 relative ${
                    !allowed
                      ? 'text-theme-muted cursor-not-allowed opacity-50'
                      : interval === int
                        ? 'btn-control active'
                        : available
                          ? 'text-theme-secondary hover:text-theme-primary'
                          : 'text-theme-muted cursor-not-allowed'
                  }`}
                >
                  {INTERVAL_LABELS[int]}
                  {!allowed && <Lock className="inline-block ml-0.5 w-3 h-3" />}
                </button>
              );
            })}
          </div>
        </div>

        {/* Период */}
        <div className="btn-group-scroll gap-1 mb-4 bg-theme-secondary rounded-xl border border-theme p-1">
          {(Object.keys(PERIOD_LABELS) as Period[]).map((p) => {
            const available = isPeriodAvailable(p);
            const allowed = isPeriodAllowed(p, isAuthenticated);
            return (
              <button
                key={p}
                onClick={() => {
                  if (!allowed) { navigate('/login'); return; }
                  if (!available) return;
                  if (p === '1d' && interval === 24) {
                    // 1Д период на дневном ТФ не имеет смысла — переключаем ТФ на 1ч
                    setIntervalValue(60);
                  }
                  setPeriod(p);
                }}
                disabled={!available && allowed}
                title={!allowed ? 'Войдите для доступа' : undefined}
                className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors duration-200 ${
                  !allowed
                    ? 'text-theme-muted cursor-not-allowed opacity-50'
                    : period === p
                      ? 'btn-control active'
                      : available
                        ? 'text-theme-secondary hover:text-theme-primary'
                        : 'text-theme-muted cursor-not-allowed'
                }`}
              >
                {PERIOD_LABELS[p]}
                {!allowed && <Lock className="inline-block ml-0.5 w-3 h-3" />}
              </button>
            );
          })}
        </div>

        {/* Режим отображения. Active state использует --accent (theme-aware)
            вместо hardcoded #6366f1 — иначе в editorial-pumpkin темах
            активная кнопка светилась бы старым indigo, а не текущим accent'ом. */}
        <div className="flex gap-2 mb-4 flex-wrap">
          {([
            { key: 'price', label: 'Только цена', click: () => setDisplayMode('price') },
            { key: 'positions', label: 'Позиции', click: () => { setDisplayMode('positions'); setOiVariant('oi'); } },
            { key: 'participants', label: 'Участники', click: () => { setDisplayMode('participants'); setOiVariant('oi'); } },
          ] as const).map((m) => {
            const active = displayMode === m.key;
            // В editorial mode-кнопки — единый chip-style как period chip'ы:
            // outline pill для inactive (2px black border + secondary text), filled
            // pill с hard-shadow для active. В non-editorial — старая Tailwind-разметка.
            const editorialActiveStyle: React.CSSProperties = {
              backgroundColor: 'var(--accent)',
              color: 'var(--text-inverse)',
              border: '2px solid var(--text-primary)',
              borderRadius: 999,
              boxShadow: 'var(--shadow-hard-chip)',
            };
            const editorialInactiveStyle: React.CSSProperties = {
              backgroundColor: 'var(--bg-secondary)',
              color: 'var(--text-primary)',
              border: '2px solid var(--text-primary)',
              borderRadius: 999,
            };
            return (
              <button
                key={m.key}
                onClick={m.click}
                className={`px-4 py-2 rounded-xl text-sm font-medium transition-colors duration-200 ${
                  isEditorial ? '' : (active ? '' : 'bg-theme-secondary text-theme-secondary hover:text-theme-primary border border-theme')
                }`}
                style={
                  isEditorial
                    ? (active ? editorialActiveStyle : editorialInactiveStyle)
                    : (active
                        ? { backgroundColor: 'var(--accent)', color: 'var(--text-inverse)' }
                        : undefined)
                }
              >
                {m.label}
              </button>
            );
          })}
        </div>

        {/* Варианты OI — нижний ряд кнопок.
            Каждый вариант имеет свой цвет (амбер=OI, зелёный=покупки, красный=продажи,
            фиолетовый=обе, циан=чистая, серый=экспирации). Цвета — theme-aware
            CSS-переменные --oi-* (неон в dark, муть в editorial), не hardcoded. */}
        {displayMode !== 'price' && (
          <div className="flex gap-2 flex-wrap">
            {([
              { key: 'oi',           cssVar: '--oi-amber',  label: 'Открытый интерес',
                isActive: () => oiVariant === 'oi',           click: () => setOiVariant('oi') },
              { key: 'long',         cssVar: '--oi-green',  label: displayMode === 'positions' ? 'Покупки' : 'Покупатели',
                isActive: () => oiVariant === 'long',         click: () => setOiVariant('long') },
              { key: 'short',        cssVar: '--oi-red',    label: displayMode === 'positions' ? 'Продажи' : 'Продавцы',
                isActive: () => oiVariant === 'short',        click: () => setOiVariant('short') },
              { key: 'both',         cssVar: '--oi-purple', label: displayMode === 'positions' ? 'Покупки + Продажи' : 'Покупатели + Продавцы',
                isActive: () => oiVariant === 'both',         click: () => setOiVariant('both') },
              { key: 'net',          cssVar: '--oi-cyan',   label: 'Чистая позиция',
                isActive: () => oiVariant === 'net',          click: () => setOiVariant('net') },
              { key: 'expirations',  cssVar: '--oi-gray',   label: 'Экспирации',
                isActive: () => showExpirations,              click: () => setShowExpirations(!showExpirations) },
            ] as const).map((v) => {
              const active = v.isActive();
              // В editorial — solid pill: заливка цветом, белый текст, 1.5px border, hard shadow.
              // В dark/okx — старая полупрозрачная заливка с цветным ring.
              const activeStyle: React.CSSProperties = isEditorial
                ? {
                    backgroundColor: `var(${v.cssVar})`,
                    color: 'var(--text-inverse)',
                    border: '2px solid var(--text-primary)',
                    borderRadius: 999,
                    boxShadow: 'var(--shadow-hard-chip)',
                    fontWeight: 700,
                  }
                : {
                    backgroundColor: `color-mix(in srgb, var(${v.cssVar}) 20%, transparent)`,
                    color: `var(${v.cssVar})`,
                    boxShadow: `inset 0 0 0 1px color-mix(in srgb, var(${v.cssVar}) 50%, transparent)`,
                  };
              // В editorial inactive — outline pill (как на референсе все 6 chip'ов одинаковой формы);
              // в dark/okx — старый transparent fill.
              const inactiveStyle: React.CSSProperties | undefined = isEditorial
                ? {
                    backgroundColor: 'var(--bg-secondary)',
                    color: 'var(--text-primary)',
                    border: '2px solid var(--text-primary)',
                    borderRadius: 999,
                    fontWeight: 700,
                  }
                : undefined;
              return (
                <button
                  key={v.key}
                  onClick={v.click}
                  className={`px-2 md:px-3 py-1 md:py-1.5 rounded-lg text-xs md:text-sm font-medium transition-colors duration-200 ${
                    active || isEditorial ? '' : 'bg-theme-secondary text-theme-secondary hover:text-theme-primary'
                  }`}
                  style={active ? activeStyle : inactiveStyle}
                >
                  {v.label}
                </button>
              );
            })}
          </div>
        )}
      </div>

      {/* Ошибка */}
      {error && (
        <div className="rounded-xl p-4 mb-6" style={{ backgroundColor: 'color-mix(in srgb, var(--danger) 10%, transparent)', border: '1px solid color-mix(in srgb, var(--danger) 30%, transparent)' }}>
          <p className="text-theme-danger">{error}</p>
        </div>
      )}

      {/* График — внутренний фон совпадает с фоном страницы (бумага в editorial-light,
          тёмный в editorial-dark/okx), чтобы chart не выделялся «панелью» внутри
          editorial-frame'а. На референсах chart inner = page bg. */}
      <div className="bg-theme-primary rounded-2xl border border-theme overflow-hidden">
      <SimpleChart
        data={chartData}
        secondaryData={oiData}
        thirdData={oiDataThird}
        showSecondary={displayMode !== 'price' && !!oiData}
        showThird={oiVariant === 'both' && !!oiDataThird}
        primaryColor={COLORS.primary}
        secondaryColor={colors.secondary}
        thirdColor={colors.third}
        height={600}
        loading={loading}
        formatValue={(v) => v.toLocaleString('ru-RU', { maximumFractionDigits: 0 })}
        primaryLabel={instrumentName || selectedInstrument}
        secondaryLabel={labels.secondary}
        thirdLabel={labels.third}
        showValueHeader={false}
        legendPosition="top"
        showDownloadButton={false}
        showNavigator={true}
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
      </div>

      </div>{/* /editorial-frame */}

      {/* Легенда — оформлена как editorial card (frame с hard shadow в editorial,
          обычная widget панель в OKX/dark). Inner bg = secondary чтобы выделяться
          на page-bg, в editorial CSS override применит outline + hard shadow. */}
      <div className="mt-6 bg-theme-secondary border border-theme rounded-2xl p-5 widget">
        <div className="space-y-3 text-sm">
          {/* Линии графика */}
          <div className="flex items-start gap-3">
            <span className="w-4 h-0.5 mt-2 rounded-full flex-shrink-0" style={{ backgroundColor: COLORS.primary }} />
            <div>
              <span className="font-medium" style={{ color: COLORS.primary }}>График цены</span>
              <span className="text-theme-secondary"> — стоимость фьючерса на срочном рынке</span>
            </div>
          </div>

          <div className="flex items-start gap-3">
            <span className="w-4 h-0.5 mt-2 rounded-full flex-shrink-0" style={{ backgroundColor: COLORS.amber }} />
            <div>
              <span className="font-medium" style={{ color: COLORS.amber }}>Открытый интерес</span>
              <span className="text-theme-secondary"> — сумма позиций на покупку и на продажу</span>
            </div>
          </div>

          <div className="flex items-start gap-3">
            <span className="w-4 h-0.5 mt-2 rounded-full flex-shrink-0" style={{ backgroundColor: COLORS.emerald }} />
            <div>
              <span className="font-medium" style={{ color: COLORS.emerald }}>Покупки</span>
              <span className="text-theme-secondary"> — объём позиций на рост / количество покупателей</span>
            </div>
          </div>

          <div className="flex items-start gap-3">
            <span className="w-4 h-0.5 mt-2 rounded-full flex-shrink-0" style={{ backgroundColor: COLORS.rose }} />
            <div>
              <span className="font-medium" style={{ color: COLORS.rose }}>Продажи</span>
              <span className="text-theme-secondary"> — объём позиций на падение / количество продавцов</span>
            </div>
          </div>

          <div className="flex items-start gap-3">
            <span className="w-4 h-0.5 mt-2 rounded-full flex-shrink-0" style={{ backgroundColor: COLORS.cyan }} />
            <div>
              <span className="font-medium" style={{ color: COLORS.cyan }}>Чистая позиция</span>
              <span className="text-theme-secondary"> — разница между покупками и продажами</span>
            </div>
          </div>

          {/* Режимы */}
          <div className="pt-3 border-t border-theme mt-4">
            <div className="text-theme-secondary">
              <span className="font-medium text-theme-primary">Режимы:</span>{' '}
              <span style={{ color: COLORS.primary }}>Позиции</span> — объём в контрактах,{' '}
              <span style={{ color: COLORS.primary }}>Участники</span> — количество участников торгов
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
        />
      )}
    </div>
  );
}