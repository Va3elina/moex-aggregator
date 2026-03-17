import { useEffect, useState, useMemo, useCallback } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { ChevronDown, BarChart3, Lock } from 'lucide-react';
import { getChartData } from '../services/api';
import type { ChartResponse } from '../types';
import SimpleChart from '../components/SimpleChart';
import InstrumentSearchModal from '../components/InstrumentSearchModal';
import { PERIOD_LABELS as ALL_PERIOD_LABELS, INTERVAL_LABELS, CHART_COLORS } from '../config/chartConfig';
import { useAuth } from '../contexts/AuthContext';
import { isIntervalAllowed, isPeriodAllowed, getDefaultPeriod } from '../config/accessControl';
import { useRealtimeData } from '../hooks/useRealtimeData';

type DisplayMode = 'price' | 'positions' | 'participants';
type OIVariant = 'oi' | 'long' | 'short' | 'both' | 'net';
type Period = '1d' | '1w' | '1m' | '3m' | '6m' | '1y' | 'all';

const PERIOD_LABELS: Record<Period, string> = {
  '1d':  ALL_PERIOD_LABELS['1d'],
  '1w':  ALL_PERIOD_LABELS['1w'],
  '1m':  ALL_PERIOD_LABELS['1m'],
  '3m':  ALL_PERIOD_LABELS['3m'],
  '6m':  ALL_PERIOD_LABELS['6m'],
  '1y':  ALL_PERIOD_LABELS['1y'],
  'all': ALL_PERIOD_LABELS['all'],
};

// Цветовая палитра
const COLORS = {
  primary: CHART_COLORS.primary,
  emerald: CHART_COLORS.emerald,
  rose:    CHART_COLORS.rose,
  amber:   CHART_COLORS.amber,
  cyan:    CHART_COLORS.cyan,
  lime:    CHART_COLORS.lime,
};

// Генерация цвета из строки
const stringToColor = (str: string) => {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = str.charCodeAt(i) + ((hash << 5) - hash);
  }
  const colors = [
    '#2EE59D', '#4DA3FF', '#9D4DFF', '#FF4D4D', '#FFB020',
    '#00D9FF', '#FF6B9D', '#FCD34D', '#14B8A6', '#F97316',
    '#06B6D4', '#3B82F6', '#F59E0B', '#8B5CF6', '#EC4899',
    '#84CC16', '#6366F1', '#A855F7', '#22C55E', '#EF4444'
  ];
  return colors[Math.abs(hash) % colors.length];
};

export default function OpenInterestPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const { isAuthenticated } = useAuth();
  const navigate = useNavigate();

  // Инструмент
  const [selectedInstrument, setSelectedInstrument] = useState(
    searchParams.get('instrument') || 'SR'
  );
  const [instrumentName, setInstrumentName] = useState('Сбербанк');
  const [isModalOpen, setIsModalOpen] = useState(false);

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
    24: ['1d', '1w', '1m', '3m', '6m', '1y', 'all']
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

    const oiMap = new Map<string, number>();
    for (const p of oiSeries) {
      oiMap.set(p.time, p.value);
    }

    const aligned: { time: string; value: number }[] = [];
    let lastValue: number | null = null;

    for (const candle of chartData) {
      const exact = oiMap.get(candle.time);
      if (exact !== undefined) {
        lastValue = exact;
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
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <div className="p-3 bg-gradient-to-br from-[#3b82f6] to-[#6366f1] rounded-xl">
          <BarChart3 className="w-6 h-6 text-white" />
        </div>
        <div>
          <h1 className="text-2xl font-bold text-theme-primary">Открытый интерес</h1>
          <p className="text-theme-secondary text-sm">Анализ позиций участников по фьючерсам MOEX</p>
        </div>
      </div>

      {/* Контролы */}
      <div className="widget p-3 md:p-4 mb-4 md:mb-6">
        <div className="flex flex-wrap items-center gap-2 md:gap-3 mb-3 md:mb-4">
          {/* Селектор инструмента — открывает модалку */}
          <button
            onClick={() => setIsModalOpen(true)}
            className="widget-flat px-3 md:px-4 py-2 md:py-2.5 text-sm font-medium transition-colors flex items-center gap-2 md:gap-3 min-w-[160px] md:min-w-[200px] hover:opacity-90"
            style={{ color: 'var(--text-primary)' }}
          >
            <div
              className="w-8 h-8 rounded-full flex-shrink-0"
              style={{ backgroundColor: stringToColor(selectedInstrument) }}
            />
            <div className="flex-1 text-left">
              <div className="font-medium">{instrumentName}</div>
              <div className="text-xs text-theme-secondary">{selectedInstrument}</div>
            </div>
            <ChevronDown size={16} className="text-theme-secondary" />
          </button>

          {/* FIZ/YUR переключатель */}
          {displayMode !== 'price' && (
            <div className="flex items-center bg-theme-secondary rounded-xl border border-theme p-1">
              <button
                onClick={() => setClgroup('FIZ')}
                className={`px-4 py-2 text-sm font-medium rounded-lg transition-colors duration-200 ${clgroup === 'FIZ'
                  ? 'btn-control active'
                  : 'text-[#A7ADBC] hover:text-theme-primary'
                  }`}
              >
                Физлица
              </button>
              <button
                onClick={() => setClgroup('YUR')}
                className={`px-4 py-2 text-sm font-medium rounded-lg transition-colors duration-200 ${clgroup === 'YUR'
                  ? 'btn-control active'
                  : 'text-[#A7ADBC] hover:text-theme-primary'
                  }`}
              >
                Юрлица
              </button>
            </div>
          )}

          {/* Таймфрейм */}
          <div className="flex items-center bg-theme-secondary rounded-xl border border-theme p-1">
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
                          ? 'text-[#A7ADBC] hover:text-theme-primary'
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
        <div className="flex items-center gap-1 mb-4 bg-theme-secondary rounded-xl border border-theme p-1 w-fit">
          {(Object.keys(PERIOD_LABELS) as Period[]).map((p) => {
            const available = isPeriodAvailable(p);
            const allowed = isPeriodAllowed(p, isAuthenticated);
            return (
              <button
                key={p}
                onClick={() => {
                  if (!allowed) { navigate('/login'); return; }
                  if (available) setPeriod(p);
                }}
                disabled={!available && allowed}
                title={!allowed ? 'Войдите для доступа' : undefined}
                className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors duration-200 ${
                  !allowed
                    ? 'text-theme-muted cursor-not-allowed opacity-50'
                    : period === p
                      ? 'btn-control active'
                      : available
                        ? 'text-[#A7ADBC] hover:text-theme-primary'
                        : 'text-theme-muted cursor-not-allowed'
                }`}
              >
                {PERIOD_LABELS[p]}
                {!allowed && <Lock className="inline-block ml-0.5 w-3 h-3" />}
              </button>
            );
          })}
        </div>

        {/* Режим отображения */}
        <div className="flex gap-2 mb-4 flex-wrap">
          <button
            onClick={() => setDisplayMode('price')}
            className={`px-4 py-2 rounded-xl text-sm font-medium transition-colors duration-200 ${displayMode === 'price'
              ? 'bg-[#6366f1] text-white'
              : 'bg-theme-secondary text-[#A7ADBC] hover:text-theme-primary border border-theme'
              }`}
          >
            Только цена
          </button>
          <button
            onClick={() => { setDisplayMode('positions'); setOiVariant('oi'); }}
            className={`px-4 py-2 rounded-xl text-sm font-medium transition-colors duration-200 ${displayMode === 'positions'
              ? 'bg-[#6366f1] text-white'
              : 'bg-theme-secondary text-[#A7ADBC] hover:text-theme-primary border border-theme'
              }`}
          >
            Позиции
          </button>
          <button
            onClick={() => { setDisplayMode('participants'); setOiVariant('oi'); }}
            className={`px-4 py-2 rounded-xl text-sm font-medium transition-colors duration-200 ${displayMode === 'participants'
              ? 'bg-[#6366f1] text-white'
              : 'bg-theme-secondary text-[#A7ADBC] hover:text-theme-primary border border-theme'
              }`}
          >
            Участники
          </button>
        </div>

        {/* Варианты OI */}
        {displayMode !== 'price' && (
          <div className="flex gap-2 flex-wrap">
            <button
              onClick={() => setOiVariant('oi')}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors duration-200 ${oiVariant === 'oi'
                ? 'bg-[#FFB020]/20 text-[#FFB020] ring-1 ring-[#FFB020]/50'
                : 'bg-theme-secondary text-[#A7ADBC] hover:text-theme-primary'
                }`}
            >
              Открытый интерес
            </button>
            <button
              onClick={() => setOiVariant('long')}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors duration-200 ${oiVariant === 'long'
                ? 'bg-[#2EE59D]/20 text-[#2EE59D] ring-1 ring-[#2EE59D]/50'
                : 'bg-theme-secondary text-[#A7ADBC] hover:text-theme-primary'
                }`}
            >
              {displayMode === 'positions' ? 'Покупки' : 'Покупатели'}
            </button>
            <button
              onClick={() => setOiVariant('short')}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors duration-200 ${oiVariant === 'short'
                ? 'bg-[#FF4D4D]/20 text-[#FF4D4D] ring-1 ring-[#FF4D4D]/50'
                : 'bg-theme-secondary text-[#A7ADBC] hover:text-theme-primary'
                }`}
            >
              {displayMode === 'positions' ? 'Продажи' : 'Продавцы'}
            </button>
            <button
              onClick={() => setOiVariant('both')}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors duration-200 ${oiVariant === 'both'
                ? 'bg-[#A855F7]/20 text-[#A855F7] ring-1 ring-[#A855F7]/50'
                : 'bg-theme-secondary text-[#A7ADBC] hover:text-theme-primary'
                }`}
            >
              {displayMode === 'positions' ? 'Покупки + Продажи' : 'Покупатели + Продавцы'}
            </button>
            <button
              onClick={() => setOiVariant('net')}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors duration-200 ${oiVariant === 'net'
                ? 'bg-[#22D3EE]/20 text-[#22D3EE] ring-1 ring-[#22D3EE]/50'
                : 'bg-theme-secondary text-[#A7ADBC] hover:text-theme-primary'
                }`}
            >
              Чистая позиция
            </button>
          </div>
        )}
      </div>

      {/* Ошибка */}
      {error && (
        <div className="bg-[#FF4D4D]/10 border border-[#FF4D4D]/30 rounded-xl p-4 mb-6">
          <p className="text-[#FF4D4D]">{error}</p>
        </div>
      )}

      {/* График */}
      <SimpleChart
        data={chartData}
        secondaryData={oiData}
        thirdData={oiDataThird}
        showSecondary={displayMode !== 'price' && !!oiData}
        showThird={oiVariant === 'both' && !!oiDataThird}
        primaryColor={COLORS.primary}
        secondaryColor={colors.secondary}
        thirdColor={colors.third}
        height={500}
        loading={loading}
        formatValue={(v) => v.toLocaleString('ru-RU', { maximumFractionDigits: 0 })}
        primaryLabel="Цена"
        secondaryLabel={labels.secondary}
        thirdLabel={labels.third}
        showValueHeader={false}
        legendPosition="top"
        showDownloadButton={false}
        showNavigator={true}
      />

      {/* Легенда */}
      <div className="mt-6 bg-theme-secondary border border-theme rounded-2xl p-5">
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
              <span className="text-theme-secondary"> — сумма лонг и шорт позиций (Long + |Short|)</span>
            </div>
          </div>

          <div className="flex items-start gap-3">
            <span className="w-4 h-0.5 mt-2 rounded-full flex-shrink-0" style={{ backgroundColor: COLORS.emerald }} />
            <div>
              <span className="font-medium" style={{ color: COLORS.emerald }}>Покупки (Long)</span>
              <span className="text-theme-secondary"> — объём лонг позиций / количество покупателей</span>
            </div>
          </div>

          <div className="flex items-start gap-3">
            <span className="w-4 h-0.5 mt-2 rounded-full flex-shrink-0" style={{ backgroundColor: COLORS.rose }} />
            <div>
              <span className="font-medium" style={{ color: COLORS.rose }}>Продажи (Short)</span>
              <span className="text-theme-secondary"> — объём шорт позиций / количество продавцов</span>
            </div>
          </div>

          <div className="flex items-start gap-3">
            <span className="w-4 h-0.5 mt-2 rounded-full flex-shrink-0" style={{ backgroundColor: COLORS.cyan }} />
            <div>
              <span className="font-medium" style={{ color: COLORS.cyan }}>Чистая позиция</span>
              <span className="text-theme-secondary"> — разница между лонг и шорт (Long + Short). Положительное = перевес покупателей</span>
            </div>
          </div>

          {/* Режимы */}
          <div className="pt-3 border-t border-theme mt-4">
            <div className="text-theme-secondary">
              <span className="font-medium text-theme-primary">Режимы:</span>{' '}
              <span style={{ color: COLORS.primary }}>Позиции</span> — объём в контрактах,{' '}
              <span style={{ color: COLORS.primary }}>Участники</span> — количество трейдеров
            </div>
          </div>
        </div>
      </div>

      {/* Модалка выбора инструмента */}
      {isModalOpen && (
        <InstrumentSearchModal
          onSelect={handleSelectInstrument}
          onClose={() => setIsModalOpen(false)}
        />
      )}
    </div>
  );
}