/**
 * MobileStrengthPage — мобильная версия «Сила рынка».
 *
 * Phase 3 простая версия:
 *   - EMA-период chips (50/100/200)
 *   - Текущий процент акций выше EMA — большая цифра + классификация
 *   - History chart (percent_above через MobileChart)
 *   - Секторная разбивка — Phase 4
 */
import { useEffect, useMemo, useState } from 'react';
import { Activity } from 'lucide-react';
import MobileLayout from '../../components/mobile/MobileLayout';
import MobilePageHeader from '../../components/mobile/MobilePageHeader';
import MobileChart from '../../components/mobile/MobileChart';
import {
  getBreadthCurrent,
  getBreadthHistory,
  type BreadthCurrentResponse,
  type BreadthHistoryResponse,
} from '../../services/api';
import { useOnboardingTour } from '../../hooks/useFirstVisit';
import OnboardingTour from '../../components/onboarding/OnboardingTour';
import { strengthMobileTour } from '../../data/tours/mobile';

const EMA_PERIODS = [50, 100, 200] as const;

const CLASSIFICATION_LABELS: Record<string, { label: string; color: string }> = {
  overbought: { label: 'Перегрев', color: 'var(--funds-flow-positive, #5BD49C)' },
  bullish: { label: 'Бычий', color: 'var(--accent)' },
  neutral: { label: 'Нейтрально', color: 'var(--text-secondary)' },
  oversold: { label: 'Перепродано', color: 'var(--funds-flow-negative, #FF7A5C)' },
};

export default function MobileStrengthPage() {
  const [emaPeriod, setEmaPeriod] = useState<50 | 100 | 200>(200);
  const [current, setCurrent] = useState<BreadthCurrentResponse | null>(null);
  const [history, setHistory] = useState<BreadthHistoryResponse | null>(null);
  const [loading, setLoading] = useState(true);

  const tour = useOnboardingTour('strength');

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        setLoading(true);
        const [cur, hist] = await Promise.all([
          getBreadthCurrent(emaPeriod, 'all'),
          getBreadthHistory(emaPeriod, 365, 'all'),
        ]);
        if (!cancelled) {
          setCurrent(cur);
          setHistory(hist);
        }
      } catch (err) {
        console.error('Ошибка strength:', err);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [emaPeriod]);

  const classInfo = current ? CLASSIFICATION_LABELS[current.classification] : null;

  // Chart series: percent above EMA + IMOEX (для контекста)
  const chartSeries = useMemo(() => {
    if (!history?.data?.length) return [];
    const breadthData = history.data.map((d) => ({ time: d.date, value: d.percent_above }));
    const imoexData = history.imoex?.map((d) => ({ time: d.date, value: d.close })) ?? [];
    return [
      {
        data: breadthData,
        color: 'var(--accent)',
        label: 'Сила',
        axis: 'left' as const,
        formatValue: (v: number) => `${v.toFixed(0)}%`,
      },
      ...(imoexData.length > 0
        ? [{
            data: imoexData,
            color: 'var(--chart-line-1, #5DA3E9)',
            label: 'IMOEX',
            axis: 'right' as const,
            formatValue: (v: number) => v.toFixed(0),
          }]
        : []),
    ];
  }, [history]);

  return (
    <MobileLayout
      bottomActionsTourId="strength-ema"
      bottomActions={
        <>
          {EMA_PERIODS.map((p) => (
            <button
              key={p}
              className="fm-page-action"
              onClick={() => setEmaPeriod(p)}
              style={{
                color: emaPeriod === p ? 'var(--text-inverse)' : 'var(--text-secondary)',
                background: emaPeriod === p ? 'var(--accent)' : 'transparent',
                borderColor: emaPeriod === p ? 'var(--text-primary)' : 'transparent',
                boxShadow: emaPeriod === p ? '3px 3px 0 var(--text-primary)' : undefined,
              }}
            >
              <span>EMA {p}</span>
            </button>
          ))}
        </>
      }
    >
      <MobilePageHeader
        Icon={Activity}
        title="Сила рынка"
        subtitle={`EMA${emaPeriod} · Все акции`}
        helpLink="/methodology/strength"
      />

      {/* Current value card */}
      <div
        data-tour="strength-current"
        style={{
          margin: '0 12px 12px',
          padding: '16px 20px',
          background: 'var(--bg-secondary)',
          border: '2px solid var(--text-primary)',
          borderRadius: 12,
          boxShadow: '4px 4px 0 var(--text-primary)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
        }}
      >
        <div>
          <div style={{ fontSize: 11, color: 'var(--text-secondary)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 4 }}>
            Сейчас
          </div>
          {current && (
            <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
              <span style={{ fontSize: 36, fontWeight: 800, color: 'var(--accent)' }}>
                {current.percent_above.toFixed(0)}%
              </span>
              <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>
                {current.count_above} из {current.count_total}
              </span>
            </div>
          )}
        </div>
        {classInfo && (
          <div
            style={{
              padding: '6px 12px',
              background: classInfo.color,
              color: 'var(--text-inverse)',
              border: '1.5px solid var(--text-primary)',
              borderRadius: 999,
              fontSize: 11,
              fontWeight: 800,
              letterSpacing: '0.04em',
              textTransform: 'uppercase',
            }}
          >
            {classInfo.label}
          </div>
        )}
      </div>

      {/* History chart — теперь занимает всё доступное место */}
      <div data-tour="strength-chart" className="fm-frame" style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
        <div style={{ margin: '0 -10px', flex: 1, minHeight: 0 }}>
          <MobileChart series={chartSeries} loading={loading} />
        </div>
        <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 4 }}>
          Доля акций выше EMA{emaPeriod}. &gt;70% — широкий рост, &lt;30% — глубокая распродажа.
        </div>
      </div>

      <OnboardingTour
        steps={strengthMobileTour}
        open={tour.open}
        onClose={tour.close}
      />
    </MobileLayout>
  );
}
