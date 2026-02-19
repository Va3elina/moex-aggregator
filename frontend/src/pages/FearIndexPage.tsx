import { useState, useEffect, useMemo } from 'react';
import { TrendingUp, TrendingDown, AlertTriangle, Gauge, DollarSign, BarChart3 } from 'lucide-react';
import { getFearIndex, getFearIndexHistory } from '../services/api';
import type { FearIndexResponse, FearIndexHistoryResponse, FearIndexPeriod } from '../services/api';
import SimpleChart from '../components/SimpleChart';

type Period = '1m' | '3m' | '6m' | '1y' | 'all';

const PERIODS: { key: Period; label: string }[] = [
  { key: '1m', label: '1М' },
  { key: '3m', label: '3М' },
  { key: '6m', label: '6М' },
  { key: '1y', label: '1Г' },
  { key: 'all', label: 'Всё' },
];

// Цвета для зон страха
const FEAR_COLORS: Record<string, string> = {
  'Extreme Greed': '#22c55e',
  'Greed': '#84cc16',
  'Neutral': '#eab308',
  'Fear': '#f97316',
  'Extreme Fear': '#ef4444',
};

const FEAR_LABELS_RU: Record<string, string> = {
  'Extreme Greed': 'Экстремальная жадность',
  'Greed': 'Жадность',
  'Neutral': 'Нейтрально',
  'Fear': 'Страх',
  'Extreme Fear': 'Экстремальный страх',
};

function getFearColor(score: number): string {
  if (score < 25) return FEAR_COLORS['Extreme Greed'];
  if (score < 45) return FEAR_COLORS['Greed'];
  if (score < 55) return FEAR_COLORS['Neutral'];
  if (score < 75) return FEAR_COLORS['Fear'];
  return FEAR_COLORS['Extreme Fear'];
}

function getFearGradient(score: number): string {
  const color = getFearColor(score);
  return `linear-gradient(135deg, ${color}22 0%, ${color}11 100%)`;
}

export default function FearIndexPage() {
  const [period, setPeriod] = useState<Period>('3m');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [current, setCurrent] = useState<FearIndexResponse | null>(null);
  const [history, setHistory] = useState<FearIndexHistoryResponse | null>(null);

  // Загрузка данных
  useEffect(() => {
    async function loadData() {
      try {
        setLoading(true);
        setError(null);
        const [currentData, historyData] = await Promise.all([
          getFearIndex(),
          getFearIndexHistory(period as FearIndexPeriod)
        ]);
        setCurrent(currentData);
        setHistory(historyData);
      } catch (err) {
        setError('Ошибка загрузки данных');
        console.error(err);
      } finally {
        setLoading(false);
      }
    }
    loadData();
  }, [period]);

  // Подготовка данных для графика
  const chartData = useMemo(() => {
    if (!history?.history?.length) return null;

    const primaryData = history.history.map(point => ({
      time: point.date,
      value: point.fear_index,
    }));

    const secondaryData = history.history.map(point => ({
      time: point.date,
      value: point.rotation_ratio * 10, // Масштабируем для наглядности
    }));

    return { primaryData, secondaryData };
  }, [history]);

  // Определение цвета и иконки для текущего значения
  const fearColor = current ? getFearColor(current.fear_index) : '#6366f1';
  const fearGradient = current ? getFearGradient(current.fear_index) : 'transparent';

  // Показываем skeleton только при первой загрузке (когда нет данных)
  const isInitialLoading = loading && !current && !history;

  if (isInitialLoading) {
    return (
      <div className="p-4 md:p-6 max-w-7xl mx-auto">
        <div className="animate-pulse">
          <div className="h-8 bg-white/5 rounded w-64 mb-6" />
          <div className="h-96 bg-white/5 rounded-2xl" />
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-4 md:p-6 max-w-7xl mx-auto">
        <div className="bg-red-500/10 border border-red-500/30 rounded-2xl p-6 text-center">
          <AlertTriangle className="w-12 h-12 text-red-400 mx-auto mb-3" />
          <p className="text-red-400">{error}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="p-4 md:p-6 max-w-7xl mx-auto">
      {/* Заголовок */}
      <div className="flex items-center gap-3 mb-6">
        <div className="p-3 bg-gradient-to-br from-[#f97316] to-[#ef4444] rounded-xl">
          <Gauge className="w-6 h-6 text-white" />
        </div>
        <div>
          <h1 className="text-2xl font-bold text-theme-primary">Индекс страха</h1>
          <p className="text-theme-secondary text-sm">Настроения инвесторов по потокам в фонды</p>
        </div>
      </div>

      {/* Виджет текущего значения */}
      <div
        className="widget p-6 mb-6"
        style={{ background: fearGradient }}
      >
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {/* Главный индикатор */}
          <div className="flex flex-col items-center justify-center">
            <div
              className="text-7xl font-bold mb-2"
              style={{ color: fearColor }}
            >
              {current?.fear_index?.toFixed(0) || '—'}
            </div>
            <div
              className="text-xl font-medium px-4 py-1 rounded-full"
              style={{
                backgroundColor: `${fearColor}22`,
                color: fearColor,
                border: `1px solid ${fearColor}44`
              }}
            >
              {current?.classification ? FEAR_LABELS_RU[current.classification] || current.classification : '—'}
            </div>
            <div className="text-theme-muted text-sm mt-2">
              Шкала: 0 (жадность) → 100 (страх)
            </div>
          </div>

          {/* Компоненты */}
          <div className="space-y-3">
            <h3 className="text-theme-secondary text-sm font-medium mb-3">Компоненты индекса</h3>

            <div className="flex items-center justify-between">
              <span className="text-theme-secondary text-sm">Rotation Ratio</span>
              <div className="flex items-center gap-2">
                <div className="w-24 h-2 bg-[#0B0D12] rounded-full overflow-hidden">
                  <div
                    className="h-full rounded-full transition-all duration-500"
                    style={{
                      width: `${current?.components?.rotation_ratio || 0}%`,
                      backgroundColor: getFearColor(current?.components?.rotation_ratio || 50)
                    }}
                  />
                </div>
                <span className="text-theme-primary text-sm w-8">
                  {current?.components?.rotation_ratio?.toFixed(0) || '—'}
                </span>
              </div>
            </div>

            <div className="flex items-center justify-between">
              <span className="text-theme-secondary text-sm">Приток в MM</span>
              <div className="flex items-center gap-2">
                <div className="w-24 h-2 bg-[#0B0D12] rounded-full overflow-hidden">
                  <div
                    className="h-full rounded-full transition-all duration-500"
                    style={{
                      width: `${current?.components?.money_market_flow || 0}%`,
                      backgroundColor: getFearColor(current?.components?.money_market_flow || 50)
                    }}
                  />
                </div>
                <span className="text-theme-primary text-sm w-8">
                  {current?.components?.money_market_flow?.toFixed(0) || '—'}
                </span>
              </div>
            </div>

            <div className="flex items-center justify-between">
              <span className="text-theme-secondary text-sm">Отток из акций</span>
              <div className="flex items-center gap-2">
                <div className="w-24 h-2 bg-[#0B0D12] rounded-full overflow-hidden">
                  <div
                    className="h-full rounded-full transition-all duration-500"
                    style={{
                      width: `${current?.components?.stocks_flow || 0}%`,
                      backgroundColor: getFearColor(current?.components?.stocks_flow || 50)
                    }}
                  />
                </div>
                <span className="text-theme-primary text-sm w-8">
                  {current?.components?.stocks_flow?.toFixed(0) || '—'}
                </span>
              </div>
            </div>

            <div className="flex items-center justify-between">
              <span className="text-theme-secondary text-sm">Velocity</span>
              <div className="flex items-center gap-2">
                <div className="w-24 h-2 bg-[#0B0D12] rounded-full overflow-hidden">
                  <div
                    className="h-full rounded-full transition-all duration-500"
                    style={{
                      width: `${current?.components?.velocity || 0}%`,
                      backgroundColor: getFearColor(current?.components?.velocity || 50)
                    }}
                  />
                </div>
                <span className="text-theme-primary text-sm w-8">
                  {current?.components?.velocity?.toFixed(0) || '—'}
                </span>
              </div>
            </div>
          </div>

          {/* Сырые данные */}
          <div className="space-y-3">
            <h3 className="text-theme-secondary text-sm font-medium mb-3">Сырые данные</h3>

            <div className="flex items-center justify-between p-2 bg-[#0B0D12]/50 rounded-lg">
              <div className="flex items-center gap-2">
                <BarChart3 size={16} className="text-[#6366f1]" />
                <span className="text-theme-secondary text-sm">MM / Акции</span>
              </div>
              <span className="text-theme-primary font-medium">
                {current?.raw_values?.rotation_ratio?.toFixed(2) || '—'}x
              </span>
            </div>

            <div className="flex items-center justify-between p-2 bg-[#0B0D12]/50 rounded-lg">
              <div className="flex items-center gap-2">
                <DollarSign size={16} className="text-[#22c55e]" />
                <span className="text-theme-secondary text-sm">СЧА MM</span>
              </div>
              <span className="text-theme-primary font-medium">
                {current?.totals?.money_market_nav?.toFixed(1) || '—'} млрд ₽
              </span>
            </div>

            <div className="flex items-center justify-between p-2 bg-[#0B0D12]/50 rounded-lg">
              <div className="flex items-center gap-2">
                <TrendingUp size={16} className="text-[#f97316]" />
                <span className="text-theme-secondary text-sm">СЧА Акции</span>
              </div>
              <span className="text-theme-primary font-medium">
                {current?.totals?.stocks_nav?.toFixed(1) || '—'} млрд ₽
              </span>
            </div>

            <div className="flex items-center justify-between p-2 bg-[#0B0D12]/50 rounded-lg">
              <div className="flex items-center gap-2">
                {(current?.raw_values?.mm_flow_pct || 0) >= 0
                  ? <TrendingUp size={16} className="text-[#22c55e]" />
                  : <TrendingDown size={16} className="text-[#ef4444]" />
                }
                <span className="text-theme-secondary text-sm">Приток MM (5д)</span>
              </div>
              <span className={`font-medium ${(current?.raw_values?.mm_flow_pct || 0) >= 0 ? 'text-[#22c55e]' : 'text-[#ef4444]'}`}>
                {current?.raw_values?.mm_flow_pct?.toFixed(2) || '—'}%
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* Панель периодов */}
      <div className="widget p-4 mb-6">
        <div className="flex flex-wrap items-center gap-3">
          <span className="text-theme-muted text-sm">Период:</span>
          <div className="flex items-center bg-[#0B0D12] rounded-xl border border-white/10 p-1">
            {PERIODS.map(p => (
              <button
                key={p.key}
                onClick={() => setPeriod(p.key)}
                className={`px-4 py-2 text-sm font-medium rounded-lg transition-all duration-300 ${period === p.key
                  ? 'btn-control active'
                  : 'text-theme-secondary hover:text-theme-primary'
                  }`}
              >
                {p.label}
              </button>
            ))}
          </div>

          <div className="flex-1" />

          <div className="text-sm text-theme-muted">
            {history?.count || 0} торговых дней
          </div>
        </div>
      </div>

      {/* График */}
      <div className="widget p-5">
        {chartData ? (
          <SimpleChart
            data={chartData.primaryData}
            secondaryData={chartData.secondaryData}
            height={400}
            primaryColor="#6366f1"
            secondaryColor="#C8FF2E"
            showSecondary={true}
            formatValue={(v) => v.toFixed(1)}
            formatSecondaryValue={(v) => `×${(v / 10).toFixed(2)}`}
            primaryLabel="Fear Index"
            secondaryLabel="Rotation Ratio"
            loading={loading}
            allowHistogram={true}
            showNavigator={true}
          />
        ) : (
          <div className="h-96 flex items-center justify-center text-theme-muted">
            Нет данных для отображения
          </div>
        )}
      </div>

      {/* Легенда */}
      <div className="mt-6 widget p-5">
        <h3 className="text-theme-primary font-medium mb-4">Шкала Fear Index</h3>
        <div className="grid grid-cols-5 gap-2">
          {Object.entries(FEAR_COLORS).map(([classification, color]) => (
            <div
              key={classification}
              className="flex flex-col items-center p-3 rounded-xl"
              style={{ backgroundColor: `${color}11`, border: `1px solid ${color}33` }}
            >
              <div
                className="text-2xl font-bold mb-1"
                style={{ color }}
              >
                {classification === 'Extreme Greed' && '0-25'}
                {classification === 'Greed' && '25-45'}
                {classification === 'Neutral' && '45-55'}
                {classification === 'Fear' && '55-75'}
                {classification === 'Extreme Fear' && '75-100'}
              </div>
              <div className="text-xs text-theme-secondary text-center">
                {FEAR_LABELS_RU[classification]}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Описание методологии */}
      <div className="mt-6 widget p-5">
        <h3 className="text-theme-primary font-medium mb-3">Методология</h3>
        <p className="text-theme-secondary text-sm mb-4">
          Fund Fear Index измеряет уровень страха/жадности инвесторов на основе потоков денег в фонды.
          Когда инвесторы боятся — они выводят деньги из акций и перекладывают в денежный рынок.
        </p>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
          <div className="p-3 bg-[#0B0D12] rounded-lg">
            <div className="text-[#6366f1] font-medium">Rotation Ratio (35%)</div>
            <div className="text-theme-muted">СЧА денежный рынок / СЧА акции</div>
          </div>
          <div className="p-3 bg-[#0B0D12] rounded-lg">
            <div className="text-[#22c55e] font-medium">MM Flow (25%)</div>
            <div className="text-theme-muted">Скорость притока в денежный рынок</div>
          </div>
          <div className="p-3 bg-[#0B0D12] rounded-lg">
            <div className="text-[#f97316] font-medium">Stocks Flow (25%)</div>
            <div className="text-theme-muted">Скорость оттока из акций</div>
          </div>
          <div className="p-3 bg-[#0B0D12] rounded-lg">
            <div className="text-[#eab308] font-medium">Velocity (15%)</div>
            <div className="text-theme-muted">Скорость изменений</div>
          </div>
        </div>
      </div>
    </div>
  );
}
