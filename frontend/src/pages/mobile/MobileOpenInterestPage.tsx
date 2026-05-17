/**
 * MobileOpenInterestPage — мобильная версия индикатора «Открытый интерес».
 *
 * Архитектура (отличается от десктопа):
 *   - TopBar + PageHeader + Editorial Frame + BottomRail (MobileLayout)
 *   - В frame'е: MobileChart (edge-to-edge) + MobileQuickActions
 *   - Asset/Period/Options открывают slide-up Sheet'ы
 *
 * Phase 2 — упрощённая версия:
 *   - Только режим «positions» с вариантом «net» (Чистая позиция)
 *   - Период через sheet (1д/1н/1м/3м/6м/1г/2г/5л/Всё)
 *   - Один график: цена + net OI
 *   - Экспирации, толкование тура — Phase 4
 */
import { useEffect, useMemo, useState } from 'react';
import { BarChart3 } from 'lucide-react';
import MobileLayout from '../../components/mobile/MobileLayout';
import MobilePageHeader from '../../components/mobile/MobilePageHeader';
import MobileChart from '../../components/mobile/MobileChart';
import MobileQuickActions from '../../components/mobile/MobileQuickActions';
import MobileSheet from '../../components/mobile/MobileSheet';
import MobileAssetSearch from '../../components/mobile/MobileAssetSearch';
import { getChartData, getInstrument } from '../../services/api';
import type { ChartResponse } from '../../types';
import { useAuth } from '../../contexts/AuthContext';
import { useRealtimeData } from '../../hooks/useRealtimeData';
import { getDefaultPeriod } from '../../config/accessControl';

type Period = '1d' | '1w' | '1m' | '3m' | '6m' | '1y' | '2y' | '5y' | 'all';

const PERIOD_LABELS: Record<Period, string> = {
  '1d': '1 день',
  '1w': '1 неделя',
  '1m': '1 месяц',
  '3m': '3 месяца',
  '6m': '6 месяцев',
  '1y': '1 год',
  '2y': '2 года',
  '5y': '5 лет',
  'all': 'Вся история',
};

const INTERVAL_LABELS: Record<number, string> = {
  5: '5 мин',
  60: '1 час',
  24: '1 день',
};

export default function MobileOpenInterestPage() {
  const { isAuthenticated } = useAuth();

  // State
  const [selectedInstrument, setSelectedInstrument] = useState('SR');
  const [instrumentName, setInstrumentName] = useState('Сбербанк');
  const [period, setPeriod] = useState<Period>(getDefaultPeriod('6m', isAuthenticated) as Period);
  const [intervalValue, setIntervalValue] = useState(24);
  const [clgroup, setClgroup] = useState<'FIZ' | 'YUR'>('YUR');
  const [data, setData] = useState<ChartResponse | null>(null);
  const [loading, setLoading] = useState(true);

  // Sheets
  const [assetSearchOpen, setAssetSearchOpen] = useState(false);
  const [periodSheetOpen, setPeriodSheetOpen] = useState(false);
  const [optionsSheetOpen, setOptionsSheetOpen] = useState(false);

  // Загрузка имени при изменении тикера (если пришло из URL)
  useEffect(() => {
    let cancelled = false;
    getInstrument(selectedInstrument).then((inst) => {
      if (cancelled) return;
      if (inst?.name) setInstrumentName(inst.name);
    });
    return () => { cancelled = true; };
  }, [selectedInstrument]);

  // Загрузка chart data
  const loadData = useMemo(() => async () => {
    try {
      setLoading(true);
      const result = await getChartData(
        selectedInstrument,
        selectedInstrument,
        'futures',
        intervalValue,
        clgroup,
        true, // show_oi
        period,
      );
      setData(result);
    } catch (err) {
      console.error('Ошибка загрузки OI:', err);
    } finally {
      setLoading(false);
    }
  }, [selectedInstrument, intervalValue, clgroup, period]);

  useEffect(() => { void loadData(); }, [loadData]);
  useRealtimeData(['5min', 'hourly'], () => { void loadData(); });

  // Преобразуем data в series для MobileChart
  // Phase 2: только цена + чистая позиция (net OI)
  const chartSeries = useMemo(() => {
    if (!data || !data.candles || data.candles.length === 0) return [];

    const priceData = data.candles.map((c) => ({ time: c.time, value: c.close }));

    // Net position = pos_long + pos_short (pos_short уже отрицательный)
    // Используем pre-computed net_position если есть
    const oiData = (data.open_interest || []).map((oi) => ({
      time: oi.time,
      value: oi.net_position ?? (oi.pos_long || 0) + (oi.pos_short || 0),
    }));

    return [
      {
        data: priceData,
        color: 'var(--chart-line-1, #5DA3E9)',
        label: instrumentName,
        axis: 'left' as const,
        formatValue: (v: number) => v.toFixed(0),
      },
      ...(oiData.length > 0
        ? [{
            data: oiData,
            color: 'var(--accent)',
            label: 'Чистая позиция',
            axis: 'right' as const,
            formatValue: (v: number) =>
              Math.abs(v) >= 1000
                ? (v / 1000).toFixed(1) + 'K'
                : v.toFixed(0),
          }]
        : []),
    ];
  }, [data, instrumentName]);

  return (
    <MobileLayout>
      <MobilePageHeader
        Icon={BarChart3}
        title="Открытый интерес"
        subtitle="Позиции участников · Фьючерсы"
        helpLink="/methodology/oi"
      />

      <div className="fm-frame">
        <div style={{ margin: '0 -10px' }}>
          <MobileChart
            series={chartSeries}
            height={280}
            loading={loading}
          />
        </div>

        <MobileQuickActions
          asset={{
            name: instrumentName,
            ticker: `${selectedInstrument} · ФЬЮЧЕРС`,
          }}
          timeLabel={`${PERIOD_LABELS[period]} · ${INTERVAL_LABELS[intervalValue] ?? intervalValue + 'ч'}`}
          optionsLabel={`Чист. поз. · ${clgroup === 'FIZ' ? 'Физ' : 'Юр'}`}
          onAsset={() => setAssetSearchOpen(true)}
          onTime={() => setPeriodSheetOpen(true)}
          onOptions={() => setOptionsSheetOpen(true)}
          showFullscreen={false}
          showExport={false}
        />
      </div>

      {/* Sheet: выбор актива */}
      <MobileAssetSearch
        open={assetSearchOpen}
        onClose={() => setAssetSearchOpen(false)}
        filterType="futures"
        onSelect={(sectype, name) => {
          setSelectedInstrument(sectype);
          setInstrumentName(name);
        }}
      />

      {/* Sheet: период + интервал */}
      <MobileSheet
        open={periodSheetOpen}
        onClose={() => setPeriodSheetOpen(false)}
        title="Время"
      >
        <div style={{ padding: 16 }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
            Период
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginBottom: 16 }}>
            {(Object.keys(PERIOD_LABELS) as Period[]).map((p) => (
              <button
                key={p}
                className={`fm-chip ${period === p ? 'active' : ''}`}
                onClick={() => {
                  setPeriod(p);
                  setPeriodSheetOpen(false);
                }}
              >
                {PERIOD_LABELS[p]}
              </button>
            ))}
          </div>
          <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
            Интервал
          </div>
          <div style={{ display: 'flex', gap: 6 }}>
            {[24, 60, 5].map((int) => (
              <button
                key={int}
                className={`fm-chip ${intervalValue === int ? 'active' : ''}`}
                onClick={() => {
                  setIntervalValue(int);
                  setPeriodSheetOpen(false);
                }}
              >
                {INTERVAL_LABELS[int]}
              </button>
            ))}
          </div>
        </div>
      </MobileSheet>

      {/* Sheet: опции (категория участников) */}
      <MobileSheet
        open={optionsSheetOpen}
        onClose={() => setOptionsSheetOpen(false)}
        title="Опции"
      >
        <div style={{ padding: 16 }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
            Категория участников
          </div>
          <div style={{ display: 'flex', gap: 6, marginBottom: 16 }}>
            {(['YUR', 'FIZ'] as const).map((g) => (
              <button
                key={g}
                className={`fm-chip ${clgroup === g ? 'active' : ''}`}
                onClick={() => {
                  setClgroup(g);
                  setOptionsSheetOpen(false);
                }}
              >
                {g === 'YUR' ? 'Юридические лица' : 'Физические лица'}
              </button>
            ))}
          </div>
          <p style={{ fontSize: 12, color: 'var(--text-muted)', lineHeight: 1.5 }}>
            Phase 2 пока показывает только режим «Чистая позиция». Скоро добавим:
            режимы Позиции / Участники, варианты ОИ / Покупки / Продажи, экспирации.
          </p>
        </div>
      </MobileSheet>
    </MobileLayout>
  );
}
