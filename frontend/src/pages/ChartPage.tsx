import { useEffect, useState } from 'react';
import { useParams, useSearchParams, Link } from 'react-router-dom';
import { getChartData } from '../services/api';
import type { ChartResponse } from '../types';
import SimpleChart from '../componets/SimpleChart';

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
  5: '5 минут',
  60: '1 час',
  24: '1 день'
};

// Цветовая палитра
const COLORS = {
  primary: '#6366f1',    // Indigo - цена
  emerald: '#10b981',    // Emerald - покупки/лонг
  rose: '#f43f5e',       // Rose - продажи/шорт
  amber: '#f59e0b',      // Amber - открытый интерес
  violet: '#8b5cf6',     // Violet - чистая позиция
};

export default function ChartPage() {
  const { secId } = useParams<{ secId: string }>();
  const [searchParams] = useSearchParams();
  const sectype = searchParams.get('sectype') || secId || '';
  const instType = searchParams.get('type') || 'futures';

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<ChartResponse | null>(null);

  const [interval, setIntervalValue] = useState(24);
  const [clgroup, setClgroup] = useState('FIZ');
  const [displayMode, setDisplayMode] = useState<DisplayMode>('positions');
  const [oiVariant, setOiVariant] = useState<OIVariant>('oi');
  const [period, setPeriod] = useState<Period>('6m');

  // Доступные интервалы OI
  const availableIntervals = data?.available_intervals || [24];
  const hasInterval = (int: number) => availableIntervals.includes(int);

  const showOi = displayMode !== 'price';

  // Загрузка данных
  useEffect(() => {
    async function loadData() {
      if (!secId) return;

      try {
        setLoading(true);
        setError(null);
        const result = await getChartData(
          secId,
          sectype,
          instType,
          interval,
          clgroup,
          showOi,
          period
        );
        setData(result);

        // Если текущий интервал недоступен для OI, переключаемся на доступный
        if (result.available_intervals &&
            result.available_intervals.length > 0 &&
            !result.available_intervals.includes(interval)) {
          // Выбираем максимальный доступный (обычно 24)
          const maxAvailable = Math.max(...result.available_intervals);
          setIntervalValue(maxAvailable);
        }
      } catch (err) {
        setError('Ошибка загрузки данных');
        console.error(err);
      } finally {
        setLoading(false);
      }
    }
    void loadData();
  }, [secId, sectype, instType, interval, clgroup, showOi, period]);

  // Данные цены
  const chartData = data?.candles.map((c) => ({
    time: c.time,
    value: c.close,
  })) || [];

  // Подготовка данных OI (ИСПРАВЛЕННЫЕ РАСЧЁТЫ!)
  const getOiData = () => {
    if (!data?.open_interest || displayMode === 'price') {
      return { secondary: undefined, third: undefined };
    }

    const isPositions = displayMode === 'positions';

    let secondary: { time: string; value: number }[] | undefined;
    let third: { time: string; value: number }[] | undefined;

    switch (oiVariant) {
      case 'oi':
        // Открытый интерес = pos_long + abs(pos_short)
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

  // Выбор цветов
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

  // Метки для легенды
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

  // Форматирование периода
  const formatPeriod = () => {
    if (!data) return null;
    if (displayMode === 'price') {
      if (data.candles_start_date && data.candles_end_date) {
        return `${data.candles_start_date} — ${data.candles_end_date}`;
      }
    } else {
      if (data.oi_start_date && data.oi_end_date) {
        return `${data.oi_start_date} — ${data.oi_end_date}`;
      }
    }
    return null;
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950 p-6">
      <div className="max-w-7xl mx-auto">
        {/* Навигация */}
        <Link
          to="/"
          className="inline-flex items-center gap-2 text-indigo-400 hover:text-indigo-300 mb-6 transition-colors group"
        >
          <span className="group-hover:-translate-x-1 transition-transform">←</span>
          <span>Назад к списку</span>
        </Link>

        {/* Заголовок */}
        <div className="mb-6">
          <h1 className="text-4xl font-bold text-white mb-1">
            {data?.sectype || sectype}
          </h1>
          <div className="flex items-center gap-3 flex-wrap">
            <span className={`px-3 py-1 rounded-lg text-sm font-medium ${
              instType === 'futures' 
                ? 'bg-indigo-500/20 text-indigo-300' 
                : 'bg-emerald-500/20 text-emerald-300'
            }`}>
              {instType === 'futures' ? 'Фьючерс' : 'Акция'}
            </span>
            {data?.contracts && data.contracts.length > 0 && (
              <span className="text-slate-500 text-sm">
                Контракты: {data.contracts.join(', ')}
              </span>
            )}
            {/* Индикатор доступных интервалов OI */}
            {instType === 'futures' && availableIntervals.length > 0 && (
              <span className="text-slate-500 text-sm">
                OI: {availableIntervals.map(i => INTERVAL_LABELS[i] || `${i}мин`).join(', ')}
              </span>
            )}
          </div>
        </div>

        {/* Контролы */}
        <div className="bg-slate-800/30 rounded-2xl border border-slate-700/50 p-4 mb-6">
          {/* Таймфрейм и группа */}
          <div className="flex gap-4 mb-4 flex-wrap items-center">
            {/* Кнопки таймфрейма вместо select */}
            <div className="flex gap-1 bg-slate-900/50 p-1 rounded-xl">
              {[5, 60, 24].map((int) => {
                const isOiMode = displayMode !== 'price';
                const available = !isOiMode || hasInterval(int);
                return (
                  <button
                    key={int}
                    onClick={() => available && setIntervalValue(int)}
                    disabled={!available}
                    className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                      interval === int
                        ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-500/30'
                        : available
                          ? 'text-slate-400 hover:text-white hover:bg-slate-700/50'
                          : 'text-slate-600 cursor-not-allowed opacity-50'
                    }`}
                    title={!available ? `Нет данных OI для ${INTERVAL_LABELS[int]}` : undefined}
                  >
                    {INTERVAL_LABELS[int]}
                    {!available && <span className="ml-1 text-xs">⚠️</span>}
                  </button>
                );
              })}
            </div>

            {instType === 'futures' && displayMode !== 'price' && (
              <select
                value={clgroup}
                onChange={(e) => setClgroup(e.target.value)}
                className="bg-slate-800 text-white border border-slate-600 rounded-xl px-4 py-2.5 focus:outline-none focus:ring-2 focus:ring-indigo-500 transition-all"
              >
                <option value="FIZ">Физлица</option>
                <option value="YUR">Юрлица</option>
              </select>
            )}
          </div>

          {/* Период */}
          <div className="flex gap-1 mb-4 bg-slate-900/50 p-1 rounded-xl w-fit">
            {(Object.keys(PERIOD_LABELS) as Period[]).map((p) => (
              <button
                key={p}
                onClick={() => setPeriod(p)}
                className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                  period === p
                    ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-500/30'
                    : 'text-slate-400 hover:text-white hover:bg-slate-700/50'
                }`}
              >
                {PERIOD_LABELS[p]}
              </button>
            ))}
          </div>

          {/* Режим отображения */}
          {instType === 'futures' && (
            <>
              <div className="flex gap-2 mb-4 flex-wrap">
                <button
                  onClick={() => setDisplayMode('price')}
                  className={`px-4 py-2 rounded-xl font-medium transition-all ${
                    displayMode === 'price'
                      ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-500/30'
                      : 'bg-slate-700/50 text-slate-300 hover:bg-slate-600/50'
                  }`}
                >
                  Только цена
                </button>
                <button
                  onClick={() => { setDisplayMode('positions'); setOiVariant('oi'); }}
                  className={`px-4 py-2 rounded-xl font-medium transition-all ${
                    displayMode === 'positions'
                      ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-500/30'
                      : 'bg-slate-700/50 text-slate-300 hover:bg-slate-600/50'
                  }`}
                >
                  Позиции
                </button>
                <button
                  onClick={() => { setDisplayMode('participants'); setOiVariant('oi'); }}
                  className={`px-4 py-2 rounded-xl font-medium transition-all ${
                    displayMode === 'participants'
                      ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-500/30'
                      : 'bg-slate-700/50 text-slate-300 hover:bg-slate-600/50'
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
                    className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                      oiVariant === 'oi'
                        ? 'bg-amber-500/20 text-amber-300 ring-1 ring-amber-500/50'
                        : 'bg-slate-700/50 text-slate-400 hover:bg-slate-600/50'
                    }`}
                  >
                    Открытый интерес
                  </button>
                  <button
                    onClick={() => setOiVariant('long')}
                    className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                      oiVariant === 'long'
                        ? 'bg-emerald-500/20 text-emerald-300 ring-1 ring-emerald-500/50'
                        : 'bg-slate-700/50 text-slate-400 hover:bg-slate-600/50'
                    }`}
                  >
                    {displayMode === 'positions' ? 'Покупки' : 'Покупатели'}
                  </button>
                  <button
                    onClick={() => setOiVariant('short')}
                    className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                      oiVariant === 'short'
                        ? 'bg-rose-500/20 text-rose-300 ring-1 ring-rose-500/50'
                        : 'bg-slate-700/50 text-slate-400 hover:bg-slate-600/50'
                    }`}
                  >
                    {displayMode === 'positions' ? 'Продажи' : 'Продавцы'}
                  </button>
                  <button
                    onClick={() => setOiVariant('both')}
                    className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                      oiVariant === 'both'
                        ? 'bg-purple-500/20 text-purple-300 ring-1 ring-purple-500/50'
                        : 'bg-slate-700/50 text-slate-400 hover:bg-slate-600/50'
                    }`}
                  >
                    {displayMode === 'positions' ? 'Покупки + Продажи' : 'Покупатели + Продавцы'}
                  </button>
                  <button
                    onClick={() => setOiVariant('net')}
                    className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                      oiVariant === 'net'
                        ? 'bg-violet-500/20 text-violet-300 ring-1 ring-violet-500/50'
                        : 'bg-slate-700/50 text-slate-400 hover:bg-slate-600/50'
                    }`}
                  >
                    Чистая позиция
                  </button>
                </div>
              )}
            </>
          )}
        </div>

        {/* Ошибка */}
        {error && (
          <div className="bg-rose-500/10 border border-rose-500/30 rounded-xl p-4 mb-6">
            <p className="text-rose-400">{error}</p>
          </div>
        )}

        {/* Информация */}
        {data && !loading && (
          <div className="flex items-center gap-4 flex-wrap mb-4 text-sm">
            <span className="text-slate-400">
              Свечей: <span className="text-white font-medium">{data.candles_count.toLocaleString()}</span>
              {displayMode !== 'price' && (
                <> | OI: <span className="text-white font-medium">{data.oi_count.toLocaleString()}</span></>
              )}
            </span>
            {formatPeriod() && (
              <span className="text-slate-500">{formatPeriod()}</span>
            )}
          </div>
        )}

        {/* Предупреждения */}
        {data && displayMode !== 'price' && !hasInterval(interval) && (
          <div className="bg-amber-500/10 border border-amber-500/30 rounded-xl p-3 mb-4">
            <p className="text-amber-400 text-sm">
              ⚠️ Для этого инструмента нет данных OI на таймфрейме {INTERVAL_LABELS[interval]}.
              Доступны: {availableIntervals.map(i => INTERVAL_LABELS[i]).join(', ')}
            </p>
          </div>
        )}
        {data && displayMode !== 'price' && data.oi_count === 0 && hasInterval(interval) && data.has_oi_data && (
          <div className="bg-amber-500/10 border border-amber-500/30 rounded-xl p-3 mb-4">
            <p className="text-amber-400 text-sm">
              ⚠️ Нет пересечения данных OI и свечей для этого периода
            </p>
          </div>
        )}
        {data && displayMode !== 'price' && !data.has_oi_data && instType !== 'stock' && (
          <div className="bg-amber-500/10 border border-amber-500/30 rounded-xl p-3 mb-4">
            <p className="text-amber-400 text-sm">
              ⚠️ Нет данных OI для этого инструмента
            </p>
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
      </div>
    </div>
  );
}