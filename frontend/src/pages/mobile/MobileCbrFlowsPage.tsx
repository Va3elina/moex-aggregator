/**
 * MobileCbrFlowsPage — мобильная версия «Потоки участников биржи».
 *
 * Phase 3: упрощённая версия:
 *   - Type tabs (Акции / ОФЗ / Валюта) — chip row
 *   - Period chips (6м / 1г / Всё)
 *   - Existing StackedBidirectionalHistogram переиспользуется (адаптивен)
 *   - Hidden categories через sheet — Phase 4
 */
import { useEffect, useMemo, useState } from 'react';
import { Banknote, Landmark, DollarSign, Building2 } from 'lucide-react';
import MobileLayout from '../../components/mobile/MobileLayout';
import MobilePageHeader from '../../components/mobile/MobilePageHeader';
import StackedBidirectionalHistogram from '../../components/cbr/StackedBidirectionalHistogram';
import { getCbrFlows, type CbrInstrumentType, type CbrFlowsResponse } from '../../services/api';

type PeriodFilter = '6m' | '2y' | 'all';

const TYPE_TABS: Array<{ key: CbrInstrumentType; label: string; Icon: typeof Banknote }> = [
  { key: 'stocks', label: 'Акции', Icon: Building2 },
  { key: 'ofz', label: 'ОФЗ', Icon: Landmark },
  { key: 'fx', label: 'Валюта', Icon: DollarSign },
];

const PERIOD_OPTIONS: Array<{ key: PeriodFilter; label: string; months: number | null }> = [
  { key: '6m', label: '6М', months: 6 },
  { key: '2y', label: '2Г', months: 24 },
  { key: 'all', label: 'Всё', months: null },
];

export default function MobileCbrFlowsPage() {
  const [type, setType] = useState<CbrInstrumentType>('stocks');
  const [period, setPeriod] = useState<PeriodFilter>('6m');
  const [data, setData] = useState<CbrFlowsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Загрузка
  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        setLoading(true);
        setError(null);
        const result = await getCbrFlows(type);
        if (!cancelled) setData(result);
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : 'Ошибка загрузки');
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [type]);

  // Применение фильтра периода
  const visiblePeriods = useMemo(() => {
    if (!data) return [];
    const opt = PERIOD_OPTIONS.find((o) => o.key === period);
    if (!opt || opt.months === null) return data.periods;
    return data.periods.slice(-opt.months);
  }, [data, period]);

  // Все категории видимы (Hidden categories — Phase 4)
  const visibleCategories = data?.categories ?? [];

  return (
    <MobileLayout>
      <MobilePageHeader
        Icon={Banknote}
        title="Потоки ЦБ"
        subtitle={data?.instrument_label ?? 'Загрузка...'}
        helpLink="/methodology/cbr-flows"
      />

      {/* Type tabs */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1fr 1fr',
          gap: 6,
          padding: '0 12px 8px',
        }}
      >
        {TYPE_TABS.map((tab) => {
          const isActive = type === tab.key;
          const Icon = tab.Icon;
          return (
            <button
              key={tab.key}
              onClick={() => setType(tab.key)}
              className={`fm-chip ${isActive ? 'active' : ''}`}
              style={{
                justifyContent: 'center',
                gap: 6,
              }}
            >
              <Icon size={14} strokeWidth={2.2} />
              {tab.label}
            </button>
          );
        })}
      </div>

      {/* Period chips */}
      <div
        style={{
          display: 'flex',
          gap: 6,
          padding: '0 12px 8px',
        }}
      >
        {PERIOD_OPTIONS.map((opt) => (
          <button
            key={opt.key}
            className={`fm-chip ${period === opt.key ? 'active' : ''}`}
            onClick={() => setPeriod(opt.key)}
            style={{ flex: 1, justifyContent: 'center' }}
          >
            {opt.label}
          </button>
        ))}
      </div>

      {/* Chart */}
      <div
        style={{
          margin: '0 8px',
          padding: 8,
          background: 'var(--bg-primary)',
          border: '1.5px solid var(--text-primary)',
          borderRadius: 12,
          minHeight: 380,
        }}
      >
        {error ? (
          <div style={{ padding: 24, color: 'var(--text-muted)', textAlign: 'center' }}>{error}</div>
        ) : loading ? (
          <div style={{ padding: 24, color: 'var(--text-muted)', textAlign: 'center' }}>Загрузка...</div>
        ) : data && visiblePeriods.length > 0 ? (
          <StackedBidirectionalHistogram
            periods={visiblePeriods}
            categories={visibleCategories}
            unit={data.unit}
            height={360}
            loading={loading}
            animTrigger={`${type}|${period}`}
          />
        ) : (
          <div style={{ padding: 24, color: 'var(--text-muted)', textAlign: 'center' }}>Нет данных</div>
        )}
      </div>
    </MobileLayout>
  );
}
