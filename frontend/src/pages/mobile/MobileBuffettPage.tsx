/**
 * MobileBuffettPage — мобильная версия индикатора Баффетта.
 *
 * Простая структура: chart (cap + ratio) + viewMode toggle + period sheet.
 * Прогноз и forecast scenarios — Phase 4.
 */
import { useEffect, useMemo, useState } from 'react';
import { Scale } from 'lucide-react';
import MobileLayout from '../../components/mobile/MobileLayout';
import MobilePageHeader from '../../components/mobile/MobilePageHeader';
import MobileChart from '../../components/mobile/MobileChart';
import MobileQuickActions from '../../components/mobile/MobileQuickActions';
import MobileSheet from '../../components/mobile/MobileSheet';
import {
  getBuffettCapGdp,
  getBuffettCapM2,
  type BuffettCapGdpResponse,
  type BuffettRatioResponse,
  type BuffettPeriod,
} from '../../services/api';

type ViewMode = 'cap-gdp' | 'cap-m2';

const PERIOD_LABELS: Partial<Record<BuffettPeriod, string>> = {
  '5y': '5 лет',
  '10y': '10 лет',
  '20y': '20 лет',
  'all': 'Вся история',
};

export default function MobileBuffettPage() {
  const [viewMode, setViewMode] = useState<ViewMode>('cap-gdp');
  const [period, setPeriod] = useState<BuffettPeriod>('10y');
  const [capGdpData, setCapGdpData] = useState<BuffettCapGdpResponse | null>(null);
  const [capM2Data, setCapM2Data] = useState<BuffettRatioResponse | null>(null);
  const [loading, setLoading] = useState(true);

  const [periodSheetOpen, setPeriodSheetOpen] = useState(false);
  const [modeSheetOpen, setModeSheetOpen] = useState(false);

  // Загрузка данных
  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        setLoading(true);
        if (viewMode === 'cap-gdp') {
          const result = await getBuffettCapGdp(period, false, '1m');
          if (!cancelled) setCapGdpData(result);
        } else {
          const result = await getBuffettCapM2(period, false, '1m');
          if (!cancelled) setCapM2Data(result);
        }
      } catch (err) {
        console.error('Ошибка Buffett:', err);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [viewMode, period]);

  // Series для chart. На мобиле показываем только главную линию (ratio),
  // без серой капитализации — экран узкий, две оси выглядят сжато.
  const chartSeries = useMemo(() => {
    if (viewMode === 'cap-gdp') {
      if (!capGdpData?.data?.length) return [];
      const ratioData = capGdpData.data.map((d) => ({ time: d.date, value: d.buffett }));
      return [{
        data: ratioData,
        color: 'var(--accent)',
        label: 'Кап / ВВП',
        axis: 'left' as const,
        formatValue: (v: number) => `${v.toFixed(0)}%`,
      }];
    }
    if (!capM2Data?.data?.length) return [];
    // Cap/M2 — ratio в decimal, умножаем на 100 для процентов
    const ratioData = capM2Data.data.map((d) => ({ time: d.date, value: d.ratio * 100 }));
    return [{
      data: ratioData,
      color: 'var(--accent)',
      label: 'Кап / M2',
      axis: 'left' as const,
      formatValue: (v: number) => `${v.toFixed(0)}%`,
    }];
  }, [viewMode, capGdpData, capM2Data]);

  return (
    <MobileLayout>
      <MobilePageHeader
        Icon={Scale}
        title="Индикатор Баффетта"
        subtitle={viewMode === 'cap-gdp' ? 'Капитализация / ВВП' : 'Капитализация / M2'}
        helpLink="/methodology/buffett"
      />

      <div className="fm-frame">
        <div style={{ margin: '0 -10px' }}>
          <MobileChart
            series={chartSeries}
            height={280}
            loading={loading}
            formatXLabel={(t) => {
              const d = new Date(t);
              return isNaN(d.getTime()) ? t : d.toLocaleDateString('ru-RU', { month: '2-digit', year: '2-digit' });
            }}
          />
        </div>

        <MobileQuickActions
          asset={{
            name: viewMode === 'cap-gdp' ? 'Кап / ВВП' : 'Кап / M2',
            ticker: PERIOD_LABELS[period] ?? period,
          }}
          timeLabel={PERIOD_LABELS[period]}
          optionsLabel={viewMode === 'cap-gdp' ? 'ВВП' : 'M2'}
          onAsset={() => setModeSheetOpen(true)}
          onTime={() => setPeriodSheetOpen(true)}
          onOptions={() => setModeSheetOpen(true)}
          showFullscreen={false}
          showExport={false}
        />
      </div>

      <MobileSheet
        open={modeSheetOpen}
        onClose={() => setModeSheetOpen(false)}
        title="Версия индикатора"
      >
        <div style={{ padding: 16 }}>
          {(['cap-gdp', 'cap-m2'] as const).map((m) => (
            <button
              key={m}
              onClick={() => {
                setViewMode(m);
                setModeSheetOpen(false);
              }}
              style={{
                display: 'block',
                width: '100%',
                textAlign: 'left',
                padding: 14,
                background: viewMode === m ? 'var(--accent)' : 'var(--bg-secondary)',
                color: viewMode === m ? 'var(--text-inverse)' : 'var(--text-primary)',
                border: '1.5px solid var(--text-primary)',
                borderRadius: 10,
                fontWeight: 700,
                fontSize: 14,
                marginBottom: 8,
                cursor: 'pointer',
              }}
            >
              {m === 'cap-gdp' ? 'Капитализация / ВВП' : 'Капитализация / M2'}
              <div style={{ fontSize: 11, fontWeight: 500, opacity: 0.8, marginTop: 4 }}>
                {m === 'cap-gdp' ? 'Классическая версия. Сравнивает с экономикой.' : 'Альтернативная. Сравнивает с денежной массой.'}
              </div>
            </button>
          ))}
        </div>
      </MobileSheet>

      <MobileSheet
        open={periodSheetOpen}
        onClose={() => setPeriodSheetOpen(false)}
        title="Период"
      >
        <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 8 }}>
          {(Object.keys(PERIOD_LABELS) as BuffettPeriod[]).map((p) => (
            <button
              key={p}
              className={`fm-chip ${period === p ? 'active' : ''}`}
              onClick={() => {
                setPeriod(p);
                setPeriodSheetOpen(false);
              }}
              style={{ justifyContent: 'flex-start', padding: '14px 16px' }}
            >
              {PERIOD_LABELS[p]}
            </button>
          ))}
        </div>
      </MobileSheet>
    </MobileLayout>
  );
}
