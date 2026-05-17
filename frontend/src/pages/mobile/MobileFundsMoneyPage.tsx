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
import {
  getFundsChartData,
  type FundsChartResponse,
  type FundCategory,
  type FundPeriod,
} from '../../services/api';

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
  const [data, setData] = useState<FundsChartResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        setLoading(true);
        const result = await getFundsChartData(category, period);
        if (!cancelled) setData(result);
      } catch (err) {
        console.error('Ошибка funds:', err);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [category, period]);

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
    <MobileLayout>
      <MobilePageHeader
        Icon={Wallet}
        title="Деньги в фондах"
        subtitle={data?.category_name ?? 'Загрузка...'}
        helpLink="/methodology/funds-money"
      />

      {/* Category chips */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1fr',
          gap: 6,
          padding: '0 12px 8px',
        }}
      >
        {CATEGORIES.map((cat) => (
          <button
            key={cat.key}
            className={`fm-chip ${category === cat.key ? 'active' : ''}`}
            onClick={() => setCategory(cat.key)}
            style={{ justifyContent: 'center' }}
          >
            {cat.label}
          </button>
        ))}
      </div>

      {/* Period chips */}
      <div
        style={{
          display: 'flex',
          gap: 6,
          padding: '0 12px 8px',
        }}
      >
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

      {/* Chart */}
      <div className="fm-frame">
        <div style={{ margin: '0 -10px' }}>
          <MobileChart
            series={chartSeries}
            height={300}
            loading={loading}
          />
        </div>
        {data && (
          <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 4 }}>
            Суммарная СЧА категории в млрд руб. Синяя линия — индекс-эталон для сравнения.
          </div>
        )}
      </div>
    </MobileLayout>
  );
}
