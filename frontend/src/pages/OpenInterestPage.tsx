import { useEffect, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { ChevronDown } from 'lucide-react';
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
  violet: '#8b5cf6',
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

  const getOiData = () => {
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
          value: (oi.pos_long || 0) + Math.abs(oi.pos_short || 0),
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
          value: oi.pos || 0,
        }));
        break;
    }
    return { secondary, third };
  };

  const { secondary: oiData, third: oiDataThird } = getOiData();

  const getColors = () => {
    switch (oiVariant) {
      case 'oi': return { secondary: COLORS.amber, third: '' };
      case 'long': return { secondary: COLORS.emerald, third: '' };
      case 'short': return { secondary: COLORS.rose, third: '' };
      case 'both': return { secondary: COLORS.emerald, third: COLORS.rose };
      case 'net': return { secondary: COLORS.violet, third: '' };
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
    <div className="max-w-7xl mx-auto px-6 py-8">
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-4xl font-bold text-[#F4F6FA] mb-2">Открытый интерес</h1>
        <p className="text-[#A7ADBC]">
          Анализ позиций участников рынка по фьючерсам MOEX
        </p>
      </div>

      {/* Контролы */}
      <div className="bg-[#1A1F2E] border border-white/10 rounded-2xl p-4 mb-6">
        <div className="flex flex-wrap items-center gap-3 mb-4">
          {/* Селектор инструмента — открывает модалку */}
          <button
            onClick={() => setIsModalOpen(true)}
            className="px-4 py-2.5 bg-[#121523] hover:bg-white/5 text-[#F4F6FA] text-sm font-medium rounded-xl border border-white/10 transition-colors flex items-center gap-3 min-w-[200px]"
          >
            <div
              className="w-8 h-8 rounded-full flex-shrink-0"
              style={{ backgroundColor: stringToColor(selectedInstrument) }}
            />
            <div className="flex-1 text-left">
              <div className="font-medium">{instrumentName}</div>
              <div className="text-xs text-[#A7ADBC]">{selectedInstrument}</div>
            </div>
            <ChevronDown size={16} className="text-[#A7ADBC]" />
          </button>

          {/* FIZ/YUR переключатель */}
          {displayMode !== 'price' && (
            <div className="flex items-center bg-[#121523] rounded-xl border border-white/10 p-1">
              <button
                onClick={() => setClgroup('FIZ')}
                className={`px-4 py-2 text-sm font-medium rounded-lg transition-all duration-300 ${
                  clgroup === 'FIZ'
                    ? 'bg-[#C8FF2E] text-[#0B0D12]'
                    : 'text-[#A7ADBC] hover:text-[#F4F6FA]'
                }`}
              >
                Физлица
              </button>
              <button
                onClick={() => setClgroup('YUR')}
                className={`px-4 py-2 text-sm font-medium rounded-lg transition-all duration-300 ${
                  clgroup === 'YUR'
                    ? 'bg-[#C8FF2E] text-[#0B0D12]'
                    : 'text-[#A7ADBC] hover:text-[#F4F6FA]'
                }`}
              >
                Юрлица
              </button>
            </div>
          )}

          {/* Таймфрейм */}
          <div className="flex items-center bg-[#121523] rounded-xl border border-white/10 p-1">
            {[5, 60, 24].map((int) => {
              const available = displayMode === 'price' || hasInterval(int);
              return (
                <button
                  key={int}
                  onClick={() => available && setIntervalValue(int)}
                  disabled={!available}
                  className={`px-3 py-2 text-sm font-medium rounded-lg transition-all duration-300 ${
                    interval === int
                      ? 'bg-[#C8FF2E] text-[#0B0D12]'
                      : available
                        ? 'text-[#A7ADBC] hover:text-[#F4F6FA]'
                        : 'text-[#5E6576] cursor-not-allowed'
                  }`}
                >
                  {INTERVAL_LABELS[int]}
                </button>
              );
            })}
          </div>
        </div>

        {/* Период */}
        <div className="flex items-center gap-1 mb-4 bg-[#121523] rounded-xl border border-white/10 p-1 w-fit">
          {(Object.keys(PERIOD_LABELS) as Period[]).map((p) => (
            <button
              key={p}
              onClick={() => setPeriod(p)}
              className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-all duration-300 ${
                period === p
                  ? 'bg-[#C8FF2E] text-[#0B0D12]'
                  : 'text-[#A7ADBC] hover:text-[#F4F6FA]'
              }`}
            >
              {PERIOD_LABELS[p]}
            </button>
          ))}
        </div>

        {/* Режим отображения */}
        <div className="flex gap-2 mb-4 flex-wrap">
          <button
            onClick={() => setDisplayMode('price')}
            className={`px-4 py-2 rounded-xl text-sm font-medium transition-all duration-300 ${
              displayMode === 'price'
                ? 'bg-[#6366f1] text-white'
                : 'bg-[#121523] text-[#A7ADBC] hover:text-[#F4F6FA] border border-white/10'
            }`}
          >
            Только цена
          </button>
          <button
            onClick={() => { setDisplayMode('positions'); setOiVariant('oi'); }}
            className={`px-4 py-2 rounded-xl text-sm font-medium transition-all duration-300 ${
              displayMode === 'positions'
                ? 'bg-[#6366f1] text-white'
                : 'bg-[#121523] text-[#A7ADBC] hover:text-[#F4F6FA] border border-white/10'
            }`}
          >
            Позиции
          </button>
          <button
            onClick={() => { setDisplayMode('participants'); setOiVariant('oi'); }}
            className={`px-4 py-2 rounded-xl text-sm font-medium transition-all duration-300 ${
              displayMode === 'participants'
                ? 'bg-[#6366f1] text-white'
                : 'bg-[#121523] text-[#A7ADBC] hover:text-[#F4F6FA] border border-white/10'
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
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all duration-300 ${
                oiVariant === 'oi'
                  ? 'bg-[#FFB020]/20 text-[#FFB020] ring-1 ring-[#FFB020]/50'
                  : 'bg-[#121523] text-[#A7ADBC] hover:text-[#F4F6FA]'
              }`}
            >
              Открытый интерес
            </button>
            <button
              onClick={() => setOiVariant('long')}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all duration-300 ${
                oiVariant === 'long'
                  ? 'bg-[#2EE59D]/20 text-[#2EE59D] ring-1 ring-[#2EE59D]/50'
                  : 'bg-[#121523] text-[#A7ADBC] hover:text-[#F4F6FA]'
              }`}
            >
              {displayMode === 'positions' ? 'Покупки' : 'Покупатели'}
            </button>
            <button
              onClick={() => setOiVariant('short')}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all duration-300 ${
                oiVariant === 'short'
                  ? 'bg-[#FF4D4D]/20 text-[#FF4D4D] ring-1 ring-[#FF4D4D]/50'
                  : 'bg-[#121523] text-[#A7ADBC] hover:text-[#F4F6FA]'
              }`}
            >
              {displayMode === 'positions' ? 'Продажи' : 'Продавцы'}
            </button>
            <button
              onClick={() => setOiVariant('both')}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all duration-300 ${
                oiVariant === 'both'
                  ? 'bg-[#A855F7]/20 text-[#A855F7] ring-1 ring-[#A855F7]/50'
                  : 'bg-[#121523] text-[#A7ADBC] hover:text-[#F4F6FA]'
              }`}
            >
              {displayMode === 'positions' ? 'Покупки + Продажи' : 'Покупатели + Продавцы'}
            </button>
            <button
              onClick={() => setOiVariant('net')}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all duration-300 ${
                oiVariant === 'net'
                  ? 'bg-[#8b5cf6]/20 text-[#8b5cf6] ring-1 ring-[#8b5cf6]/50'
                  : 'bg-[#121523] text-[#A7ADBC] hover:text-[#F4F6FA]'
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
      />

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