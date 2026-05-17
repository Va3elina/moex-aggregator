/**
 * MobileFundsMoneyPage — мобильная версия «Деньги в фондах».
 *
 * Phase 3 упрощённая версия:
 *   - Category chips (Денежный рынок / Акции / Облигации / Золото)
 *   - Period chips (1М/6М/2Г/Всё)
 *   - Chart с суммарной СЧА категории + индексом-эталоном
 *   - Притоки-Оттоки и таблица фондов — Phase 4
 */
import { useEffect, useMemo, useState } from 'react';
import { Wallet } from 'lucide-react';
import MobileLayout from '../../components/mobile/MobileLayout';
import MobilePageHeader from '../../components/mobile/MobilePageHeader';
import MobileChart from '../../components/mobile/MobileChart';
import MobileSkeleton from '../../components/mobile/MobileSkeleton';
import MobileFlowsHistogram from '../../components/mobile/MobileFlowsHistogram';
import {
  getFundsChartData,
  getFundsFlows,
  type FundsChartResponse,
  type FundsFlowsResponse,
  type FundCategory,
  type FundPeriod,
  type FlowTimeframe,
} from '../../services/api';
import { useOnboardingTour } from '../../hooks/useFirstVisit';
import OnboardingTour from '../../components/onboarding/OnboardingTour';
import { fundsMoneyMobileTour } from '../../data/tours/mobile';

type ViewMode = 'aum' | 'flows';

const CATEGORIES: Array<{ key: FundCategory; label: string }> = [
  { key: 'money_market', label: 'Деньги' },
  { key: 'stocks', label: 'Акции' },
  { key: 'bonds', label: 'Облигации' },
  { key: 'gold', label: 'Золото' },
];

const PERIODS: Array<{ key: FundPeriod; label: string }> = [
  { key: '1m', label: '1М' },
  { key: '6m', label: '6М' },
  { key: '2y', label: '2Г' },
  { key: 'all', label: 'Всё' },
];

export default function MobileFundsMoneyPage() {
  const [category, setCategory] = useState<FundCategory>('money_market');
  const [period, setPeriod] = useState<FundPeriod>('6m');
  const [viewMode, setViewMode] = useState<ViewMode>('aum');
  const [flowTimeframe, setFlowTimeframe] = useState<FlowTimeframe>('1w');
  const [data, setData] = useState<FundsChartResponse | null>(null);
  const [flowsData, setFlowsData] = useState<FundsFlowsResponse | null>(null);
  const [loading, setLoading] = useState(true);

  const tour = useOnboardingTour('funds-money');

  // Загрузка данных СЧА (когда viewMode === 'aum')
  useEffect(() => {
    if (viewMode !== 'aum') return;
    let cancelled = false;
    async function load() {
      try {
        setLoading(true);
        const result = await getFundsChartData(category, period);
        if (!cancelled) setData(result);
      } catch (err) {
        console.error('Ошибка funds aum:', err);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [viewMode, category, period]);

  // Загрузка flows (когда viewMode === 'flows')
  useEffect(() => {
    if (viewMode !== 'flows') return;
    let cancelled = false;
    async function load() {
      try {
        setLoading(true);
        const result = await getFundsFlows(category, flowTimeframe, period);
        if (!cancelled) setFlowsData(result);
      } catch (err) {
        console.error('Ошибка funds flows:', err);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [viewMode, category, flowTimeframe, period]);

  // Series: суммарная СЧА (млрд ₽) + индекс
  const chartSeries = useMemo(() => {
    if (!data) return [];

    // Total NAV в млрд ₽
    const navData = data.total_nav
      .filter((d) => d.nav !== null)
      .map((d) => ({
        time: d.date,
        value: (d.nav as number) / 1e9,
      }));

    const indexData = data.index?.data
      ?.filter((d) => d.close !== null)
      .map((d) => ({ time: d.date, value: d.close as number })) ?? [];

    return [
      {
        data: navData,
        color: 'var(--accent)',
        label: 'СЧА',
        axis: 'left' as const,
        formatValue: (v: number) => `${v.toFixed(0)}`,
      },
      ...(indexData.length > 0
        ? [{
            data: indexData,
            color: 'var(--chart-line-1, #5DA3E9)',
            label: data.index?.secid ?? 'Индекс',
            axis: 'right' as const,
            formatValue: (v: number) => v >= 1000 ? (v / 1000).toFixed(1) + 'K' : v.toFixed(0),
          }]
        : []),
    ];
  }, [data]);

  return (
    <MobileLayout
      bottomActionsTourId="funds-category"
      bottomActions={
        <>
          {CATEGORIES.map((cat) => (
            <button
              key={cat.key}
              className="fm-page-action"
              onClick={() => setCategory(cat.key)}
              style={{
                color: category === cat.key ? 'var(--text-inverse)' : 'var(--text-secondary)',
                background: category === cat.key ? 'var(--accent)' : 'transparent',
                borderColor: category === cat.key ? 'var(--text-primary)' : 'transparent',
                boxShadow: category === cat.key ? '3px 3px 0 var(--text-primary)' : undefined,
              }}
            >
              <span>{cat.label}</span>
            </button>
          ))}
        </>
      }
    >
      <MobilePageHeader
        Icon={Wallet}
        title="Деньги в фондах"
        subtitle={data?.category_name ?? 'Загрузка...'}
        helpLink="/methodology/funds-money"
      />

      {/* ViewMode toggle: СЧА / Притоки */}
      <div style={{ display: 'flex', gap: 6, padding: '0 12px 6px' }}>
        {(['aum', 'flows'] as const).map((m) => (
          <button
            key={m}
            className={`fm-chip ${viewMode === m ? 'active' : ''}`}
            onClick={() => setViewMode(m)}
            style={{ flex: 1, justifyContent: 'center' }}
          >
            {m === 'aum' ? 'СЧА' : 'Притоки'}
          </button>
        ))}
      </div>

      {/* Period chips (в обоих режимах) */}
      <div style={{ display: 'flex', gap: 6, padding: '0 12px 6px' }}>
        {PERIODS.map((p) => (
          <button
            key={p.key}
            className={`fm-chip ${period === p.key ? 'active' : ''}`}
            onClick={() => setPeriod(p.key)}
            style={{ flex: 1, justifyContent: 'center' }}
          >
            {p.label}
          </button>
        ))}
      </div>

      {/* Flow timeframe — только в режиме Притоки */}
      {viewMode === 'flows' && (
        <div style={{ display: 'flex', gap: 6, padding: '0 12px 8px' }}>
          {(['1d', '1w', '1m'] as FlowTimeframe[]).map((tf) => (
            <button
              key={tf}
              className={`fm-chip ${flowTimeframe === tf ? 'active' : ''}`}
              onClick={() => setFlowTimeframe(tf)}
              style={{ flex: 1, justifyContent: 'center' }}
            >
              {tf === '1d' ? 'День' : tf === '1w' ? 'Неделя' : 'Месяц'}
            </button>
          ))}
        </div>
      )}

      {/* Chart area */}
      <div data-tour="funds-chart" className="fm-frame" style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
        {viewMode === 'aum' ? (
          <div style={{ margin: '0 -10px', flex: 1, minHeight: 0 }}>
            <MobileChart series={chartSeries} loading={loading} />
          </div>
        ) : loading ? (
          <MobileSkeleton variant="chart" height="100%" />
        ) : flowsData && flowsData.flows.length > 0 ? (
          <MobileFlowsHistogram flows={flowsData.flows} />
        ) : (
          <div style={{ display: 'grid', placeItems: 'center', height: '100%', color: 'var(--text-muted)' }}>
            Нет данных
          </div>
        )}
      </div>

      <OnboardingTour
        steps={fundsMoneyMobileTour}
        open={tour.open}
        onClose={tour.close}
      />
    </MobileLayout>
  );
}
