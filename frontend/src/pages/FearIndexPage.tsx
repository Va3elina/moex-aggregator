import { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import { TrendingUp, TrendingDown, AlertTriangle, Gauge, DollarSign, BarChart3, Lock } from 'lucide-react';
import PageHeader from '../components/PageHeader';
import { METHODOLOGY } from '../data/methodology';
import { useNavigate } from 'react-router-dom';
import { getFearIndex, getFearIndexHistory } from '../services/api';
import type { FearIndexResponse, FearIndexHistoryResponse, FearIndexPeriod } from '../services/api';
import SimpleChart from '../components/SimpleChart';
import { useAuth } from '../contexts/AuthContext';
import { isPeriodAllowed, getDefaultPeriod } from '../config/accessControl';
import { useRealtimeData } from '../hooks/useRealtimeData';
import { useFitToViewport } from '../hooks/useFitToViewport';
import { FEAR_COLORS, FEAR_LABELS_RU, getFearColor, getFearGradient } from '../config/fearConfig';
import { EditorialChip, EditorialFrame, useEditorial } from '../components/editorial';

type Period = '1m' | '3m' | '6m' | '1y' | 'all';

const PERIODS: { key: Period; label: string }[] = [
  { key: '1m', label: '1М' },
  { key: '3m', label: '3М' },
  { key: '6m', label: '6М' },
  { key: '1y', label: '1Г' },
  { key: 'all', label: 'Всё' },
];

export default function FearIndexPage() {
  const { isAuthenticated } = useAuth();
  const { isEditorial } = useEditorial();
  const navigate = useNavigate();
  const [period, setPeriod] = useState<Period>(getDefaultPeriod('3m', isAuthenticated) as Period);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [current, setCurrent] = useState<FearIndexResponse | null>(null);
  const [history, setHistory] = useState<FearIndexHistoryResponse | null>(null);

  // Динамическая высота графика (как в OI/Buffett/Funds-Money)
  const chartAnchorRef = useRef<HTMLDivElement>(null);
  const chartHeight = useFitToViewport(chartAnchorRef, {
    min: 360,
    max: 720,
    bottomBuffer: 96,
  });

  // Загрузка данных
  const loadData = useCallback(async () => {
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
  }, [period]);

  useEffect(() => { loadData(); }, [loadData]);

  // SSE: автоматическое обновление при новых данных
  useRealtimeData(['funds'], loadData);

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
      <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8">
        <div className="animate-pulse">
          <div className="h-8 bg-white/5 rounded w-64 mb-6" />
          <div className="h-96 bg-white/5 rounded-2xl" />
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8">
        <div className="bg-red-500/10 border border-red-500/30 rounded-2xl p-6 text-center">
          <AlertTriangle className="w-12 h-12 text-red-400 mx-auto mb-3" />
          <p className="text-red-400">{error}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8">
      <PageHeader
        icon={Gauge}
        title="Индекс страха"
        subtitle="Настроения инвесторов по потокам в фонды"
        help={METHODOLOGY.fear}
      />

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
                <div className="w-24 h-2 bg-theme-primary rounded-full overflow-hidden">
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
                <div className="w-24 h-2 bg-theme-primary rounded-full overflow-hidden">
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
                <div className="w-24 h-2 bg-theme-primary rounded-full overflow-hidden">
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
                <div className="w-24 h-2 bg-theme-primary rounded-full overflow-hidden">
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

            <div className="flex items-center justify-between p-2 rounded-lg" style={{ backgroundColor: 'color-mix(in srgb, var(--bg-primary) 50%, transparent)' }}>
              <div className="flex items-center gap-2">
                <BarChart3 size={16} className="text-theme-accent" />
                <span className="text-theme-secondary text-sm">MM / Акции</span>
              </div>
              <span className="text-theme-primary font-medium">
                {current?.raw_values?.rotation_ratio?.toFixed(2) || '—'}x
              </span>
            </div>

            <div className="flex items-center justify-between p-2 rounded-lg" style={{ backgroundColor: 'color-mix(in srgb, var(--bg-primary) 50%, transparent)' }}>
              <div className="flex items-center gap-2">
                <DollarSign size={16} className="text-theme-success" />
                <span className="text-theme-secondary text-sm">СЧА MM</span>
              </div>
              <span className="text-theme-primary font-medium">
                {current?.totals?.money_market_nav?.toFixed(1) || '—'} млрд ₽
              </span>
            </div>

            <div className="flex items-center justify-between p-2 rounded-lg" style={{ backgroundColor: 'color-mix(in srgb, var(--bg-primary) 50%, transparent)' }}>
              <div className="flex items-center gap-2">
                <TrendingUp size={16} className="text-theme-warning" />
                <span className="text-theme-secondary text-sm">СЧА Акции</span>
              </div>
              <span className="text-theme-primary font-medium">
                {current?.totals?.stocks_nav?.toFixed(1) || '—'} млрд ₽
              </span>
            </div>

            <div className="flex items-center justify-between p-2 rounded-lg" style={{ backgroundColor: 'color-mix(in srgb, var(--bg-primary) 50%, transparent)' }}>
              <div className="flex items-center gap-2">
                {(current?.raw_values?.mm_flow_pct || 0) >= 0
                  ? <TrendingUp size={16} className="text-theme-success" />
                  : <TrendingDown size={16} className="text-theme-danger" />
                }
                <span className="text-theme-secondary text-sm">Приток MM (5д)</span>
              </div>
              <span className={`font-medium ${(current?.raw_values?.mm_flow_pct || 0) >= 0 ? 'text-theme-success' : 'text-theme-danger'}`}>
                {current?.raw_values?.mm_flow_pct?.toFixed(2) || '—'}%
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* Editorial frame — обнимает controls + chart в один контейнер */}
      <div className="editorial-frame">

      {/* Панель периодов */}
      <div className="flex flex-wrap items-center mb-4 md:mb-6" style={{ gap: 'var(--sp-2)' }}>
        <span className="text-theme-muted pl-1" style={{ fontSize: 'var(--fs-sm)' }}>Период:</span>
        <div className={isEditorial ? 'flex flex-wrap items-center gap-2' : 'flex items-center bg-theme-primary rounded-xl border border-theme p-1'}>
          {PERIODS.map(p => {
            const allowed = isPeriodAllowed(p.key, isAuthenticated);
            return (
              <EditorialChip
                key={p.key}
                active={period === p.key}
                disabled={!allowed}
                title={!allowed ? 'Войдите для доступа' : undefined}
                onClick={() => {
                  if (!allowed) { navigate('/login'); return; }
                  setPeriod(p.key);
                }}
              >
                {p.label}
                {!allowed && <Lock className="inline-block ml-1 w-3 h-3" />}
              </EditorialChip>
            );
          })}
        </div>

        <div className="flex-1" />

        <div className="text-theme-muted pr-1" style={{ fontSize: 'var(--fs-sm)' }}>
          {history?.count || 0} торговых дней
        </div>
      </div>

      {/* График */}
      {chartData ? (
        <div ref={chartAnchorRef}>
        <SimpleChart
          data={chartData.primaryData}
          secondaryData={chartData.secondaryData}
          height={chartHeight}
          primaryColor="var(--accent)"
          secondaryColor="var(--accent-secondary)"
          showSecondary={true}
          formatValue={(v) => v.toFixed(1)}
          formatSecondaryValue={(v) => `×${(v / 10).toFixed(2)}`}
          primaryLabel="Fear Index"
          secondaryLabel="Rotation Ratio"
          loading={loading}
          allowHistogram={true}
          showNavigator={true}
        />
        </div>
      ) : (
        <div className="h-96 flex items-center justify-center text-theme-muted">
          Нет данных для отображения
        </div>
      )}

      </div>{/* /editorial-frame */}

      {/* Легенда */}
      <EditorialFrame padding="md" className="mt-6">
        <h3 className="text-theme-primary font-medium mb-4">Шкала Fear Index</h3>
        <div className="grid grid-cols-2 sm:grid-cols-5 gap-2">
          {Object.entries(FEAR_COLORS).map(([classification, color]) => (
            <div
              key={classification}
              className="flex flex-col items-center p-3 rounded-xl"
              style={{ backgroundColor: `${color}11`, border: `1px solid ${color}33` }}
            >
              <div
                className="text-xl sm:text-2xl font-bold mb-1"
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
      </EditorialFrame>

      {/* Описание методологии */}
      <EditorialFrame padding="md" className="mt-6">
        <h3 className="text-theme-primary font-medium mb-3">Методология</h3>
        <p className="text-theme-secondary text-sm mb-4">
          Fund Fear Index измеряет уровень страха/жадности инвесторов на основе потоков денег в фонды.
          Когда инвесторы боятся — они выводят деньги из акций и перекладывают в денежный рынок.
        </p>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
          <div className="p-3 bg-theme-primary rounded-lg">
            <div className="text-theme-accent font-medium">Rotation Ratio (35%)</div>
            <div className="text-theme-muted">СЧА денежный рынок / СЧА акции</div>
          </div>
          <div className="p-3 bg-theme-primary rounded-lg">
            <div className="text-theme-success font-medium">MM Flow (25%)</div>
            <div className="text-theme-muted">Скорость притока в денежный рынок</div>
          </div>
          <div className="p-3 bg-theme-primary rounded-lg">
            <div className="text-theme-warning font-medium">Stocks Flow (25%)</div>
            <div className="text-theme-muted">Скорость оттока из акций</div>
          </div>
          <div className="p-3 bg-theme-primary rounded-lg">
            <div className="font-medium" style={{ color: '#eab308' }}>Velocity (15%)</div>
            <div className="text-theme-muted">Скорость изменений</div>
          </div>
        </div>
      </EditorialFrame>
    </div>
  );
}
