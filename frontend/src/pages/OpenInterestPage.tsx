import { useEffect, useState, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import { ChevronDown, BarChart3 } from 'lucide-react';
import { getChartData } from '../services/api';
import type { ChartResponse } from '../types';
import SimpleChart from '../components/SimpleChart';
import InstrumentSearchModal from '../components/InstrumentSearchModal';

type DisplayMode = 'price' | 'positions' | 'participants';
type OIVariant = 'oi' | 'long' | 'short' | 'both' | 'net';
type Period = '1d' | '1w' | '1m' | '3m' | '6m' | '1y' | 'all';

const PERIOD_LABELS: Record<Period, string> = {
  '1d': '1Д',
  '1w': '1Н',
  '1m': '1М',
  '3m': '3М',
  '6m': '6М',
  '1y': '1Г',
  'all': 'Всё'
};

const INTERVAL_LABELS: Record<number, string> = {
  5: '5м',
  60: '1ч',
  24: '1д'
};

// Цветовая палитра
const COLORS = {
  primary: '#6366f1',
  emerald: '#2EE59D',
  rose: '#FF4D4D',
  amber: '#FFB020',
  cyan: '#22D3EE',  // Для чистой позиции
  lime: '#C8FF2E',
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

  // Настройки
  const [interval, setIntervalValue] = useState(24);
  const [clgroup, setClgroup] = useState<'FIZ' | 'YUR'>('FIZ');
  const [displayMode, setDisplayMode] = useState<DisplayMode>('positions');
  const [oiVariant, setOiVariant] = useState<OIVariant>('oi');
  const [period, setPeriod] = useState<Period>('6m');

  const availableIntervals = data?.available_intervals || [24];
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
  useEffect(() => {
    async function loadData() {
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
    }
    loadData();
  }, [selectedInstrument, interval, clgroup, showOi, period]);

  // Выбор инструмента из модалки
  const handleSelectInstrument = (sectype: string, name: string) => {
    setSelectedInstrument(sectype);
    setInstrumentName(name);
    setSearchParams({ instrument: sectype });
    setIsModalOpen(false);
  };

  // Данные для графика
  const chartData = data?.candles.map((c) => ({
    time: c.time,
    value: c.close,
  })) || [];

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
    if (!data?.open_interest || displayMode === 'price') {
      return { secondary: undefined, third: undefined };
    }
    const isPositions = displayMode === 'positions';
    let secondary: { time: string; value: number }[] | undefined;
    let third: { time: string; value: number }[] | undefined;

    switch (oiVariant) {
      case 'oi':
        secondary = data.open_interest.map((oi) => ({
          time: oi.time,
          value: isPositions
            ? (oi.pos_long || 0) + Math.abs(oi.pos_short || 0)
            : (oi.pos_long_num || 0) + (oi.pos_short_num || 0),
        }));
        break;
      case 'long':
        secondary = data.open_interest.map((oi) => ({
          time: oi.time,
          value: isPositions ? (oi.pos_long || 0) : (oi.pos_long_num || 0),
        }));
        break;
      case 'short':
        secondary = data.open_interest.map((oi) => ({
          time: oi.time,
          value: isPositions ? Math.abs(oi.pos_short || 0) : (oi.pos_short_num || 0),
        }));
        break;
      case 'both':
        secondary = data.open_interest.map((oi) => ({
          time: oi.time,
          value: isPositions ? (oi.pos_long || 0) : (oi.pos_long_num || 0),
        }));
        third = data.open_interest.map((oi) => ({
          time: oi.time,
          value: isPositions ? Math.abs(oi.pos_short || 0) : (oi.pos_short_num || 0),
        }));
        break;
      case 'net':
        secondary = data.open_interest.map((oi) => ({
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
  }, [data, displayMode, oiVariant, chartData]);

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
                className={`px-4 py-2 text-sm font-medium rounded-lg transition-all duration-300 ${clgroup === 'FIZ'
                  ? 'btn-control active'
                  : 'text-[#A7ADBC] hover:text-theme-primary'
                  }`}
              >
                Физлица
              </button>
              <button
                onClick={() => setClgroup('YUR')}
                className={`px-4 py-2 text-sm font-medium rounded-lg transition-all duration-300 ${clgroup === 'YUR'
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
              return (
                <button
                  key={int}
                  onClick={() => available && handleIntervalChange(int)}
                  disabled={!available}
                  className={`px-3 py-2 text-sm font-medium rounded-lg transition-all duration-300 ${interval === int
                    ? 'btn-control active'
                    : available
                      ? 'text-[#A7ADBC] hover:text-theme-primary'
                      : 'text-theme-muted cursor-not-allowed'
                    }`}
                >
                  {INTERVAL_LABELS[int]}
                </button>
              );
            })}
          </div>
        </div>

        {/* Период */}
        <div className="flex items-center gap-1 mb-4 bg-theme-secondary rounded-xl border border-theme p-1 w-fit">
          {(Object.keys(PERIOD_LABELS) as Period[]).map((p) => {
            const available = isPeriodAvailable(p);
            return (
              <button
                key={p}
                onClick={() => available && setPeriod(p)}
                disabled={!available}
                className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-all duration-300 ${period === p
                  ? 'btn-control active'
                  : available
                    ? 'text-[#A7ADBC] hover:text-theme-primary'
                    : 'text-theme-muted cursor-not-allowed'
                  }`}
              >
                {PERIOD_LABELS[p]}
              </button>
            );
          })}
        </div>

        {/* Режим отображения */}
        <div className="flex gap-2 mb-4 flex-wrap">
          <button
            onClick={() => setDisplayMode('price')}
            className={`px-4 py-2 rounded-xl text-sm font-medium transition-all duration-300 ${displayMode === 'price'
              ? 'bg-[#6366f1] text-white'
              : 'bg-theme-secondary text-[#A7ADBC] hover:text-theme-primary border border-theme'
              }`}
          >
            Только цена
          </button>
          <button
            onClick={() => { setDisplayMode('positions'); setOiVariant('oi'); }}
            className={`px-4 py-2 rounded-xl text-sm font-medium transition-all duration-300 ${displayMode === 'positions'
              ? 'bg-[#6366f1] text-white'
              : 'bg-theme-secondary text-[#A7ADBC] hover:text-theme-primary border border-theme'
              }`}
          >
            Позиции
          </button>
          <button
            onClick={() => { setDisplayMode('participants'); setOiVariant('oi'); }}
            className={`px-4 py-2 rounded-xl text-sm font-medium transition-all duration-300 ${displayMode === 'participants'
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
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all duration-300 ${oiVariant === 'oi'
                ? 'bg-[#FFB020]/20 text-[#FFB020] ring-1 ring-[#FFB020]/50'
                : 'bg-theme-secondary text-[#A7ADBC] hover:text-theme-primary'
                }`}
            >
              Открытый интерес
            </button>
            <button
              onClick={() => setOiVariant('long')}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all duration-300 ${oiVariant === 'long'
                ? 'bg-[#2EE59D]/20 text-[#2EE59D] ring-1 ring-[#2EE59D]/50'
                : 'bg-theme-secondary text-[#A7ADBC] hover:text-theme-primary'
                }`}
            >
              {displayMode === 'positions' ? 'Покупки' : 'Покупатели'}
            </button>
            <button
              onClick={() => setOiVariant('short')}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all duration-300 ${oiVariant === 'short'
                ? 'bg-[#FF4D4D]/20 text-[#FF4D4D] ring-1 ring-[#FF4D4D]/50'
                : 'bg-theme-secondary text-[#A7ADBC] hover:text-theme-primary'
                }`}
            >
              {displayMode === 'positions' ? 'Продажи' : 'Продавцы'}
            </button>
            <button
              onClick={() => setOiVariant('both')}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all duration-300 ${oiVariant === 'both'
                ? 'bg-[#A855F7]/20 text-[#A855F7] ring-1 ring-[#A855F7]/50'
                : 'bg-theme-secondary text-[#A7ADBC] hover:text-theme-primary'
                }`}
            >
              {displayMode === 'positions' ? 'Покупки + Продажи' : 'Покупатели + Продавцы'}
            </button>
            <button
              onClick={() => setOiVariant('net')}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all duration-300 ${oiVariant === 'net'
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
        allowHistogram={true}
        histogramDisabled={oiVariant === 'both'}
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