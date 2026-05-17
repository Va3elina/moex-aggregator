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
import MobileSheet from '../../components/mobile/MobileSheet';
import MobileAssetSearch from '../../components/mobile/MobileAssetSearch';
import { Clock, Settings, Star } from 'lucide-react';
import { getChartData, getInstrument } from '../../services/api';
import type { ChartResponse } from '../../types';
import { useAuth } from '../../contexts/AuthContext';
import { useRealtimeData } from '../../hooks/useRealtimeData';
import { getDefaultPeriod } from '../../config/accessControl';

type Period = '1d' | '1w' | '1m' | '3m' | '6m' | '1y' | '2y' | '5y' | 'all';
type OIVariant = 'oi' | 'long' | 'short' | 'both' | 'net';

const VARIANT_LABELS: Record<OIVariant, string> = {
  oi:    'Открытый интерес',
  long:  'Покупки',
  short: 'Продажи',
  both:  'Покупки + Продажи',
  net:   'Чистая позиция',
};

const VARIANT_COLORS: Record<OIVariant, string> = {
  oi:    'var(--oi-amber)',
  long:  'var(--oi-green)',
  short: 'var(--oi-red)',
  both:  'var(--oi-purple)',
  net:   'var(--accent)',
};

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
  const [oiVariant, setOiVariant] = useState<OIVariant>('net');
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

  // Преобразуем data в series для MobileChart с учётом выбранного варианта OI
  const chartSeries = useMemo(() => {
    if (!data || !data.candles || data.candles.length === 0) return [];

    const priceData = data.candles.map((c) => ({ time: c.time, value: c.close }));
    const oiPoints = data.open_interest || [];
    const fmtOI = (v: number) =>
      Math.abs(v) >= 1000 ? (v / 1000).toFixed(1) + 'K' : v.toFixed(0);

    // Выбираем значение по varianту
    const variantValue = (oi: typeof oiPoints[number]): number => {
      switch (oiVariant) {
        case 'oi':    return (oi.pos_long || 0) + Math.abs(oi.pos_short || 0);
        case 'long':  return oi.pos_long || 0;
        case 'short': return Math.abs(oi.pos_short || 0);
        case 'net':   return oi.net_position ?? (oi.pos_long || 0) + (oi.pos_short || 0);
        case 'both':  return oi.pos_long || 0; // primary серия для both — long
      }
    };

    const oiData = oiPoints.map((oi) => ({ time: oi.time, value: variantValue(oi) }));
    // Для 'both' — добавляем дополнительную серию short
    const shortData =
      oiVariant === 'both'
        ? oiPoints.map((oi) => ({ time: oi.time, value: Math.abs(oi.pos_short || 0) }))
        : [];

    const baseSeries = [
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
            color: VARIANT_COLORS[oiVariant],
            label: oiVariant === 'both' ? 'Покупки' : VARIANT_LABELS[oiVariant],
            axis: 'right' as const,
            formatValue: fmtOI,
          }]
        : []),
      ...(shortData.length > 0
        ? [{
            data: shortData,
            color: VARIANT_COLORS.short,
            label: 'Продажи',
            axis: 'right' as const,
            formatValue: fmtOI,
          }]
        : []),
    ];

    return baseSeries;
  }, [data, instrumentName, oiVariant]);

  const timeLabel = `${PERIOD_LABELS[period]} · ${INTERVAL_LABELS[intervalValue] ?? intervalValue + 'ч'}`;
  const optionsLabel = `${VARIANT_LABELS[oiVariant]}`;

  return (
    <MobileLayout
      bottomActions={
        <>
          <button
            className="fm-page-action asset"
            onClick={() => setAssetSearchOpen(true)}
            aria-label={`Актив: ${instrumentName}`}
          >
            <Star size={14} fill="var(--accent)" strokeWidth={0} />
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start', minWidth: 0, flex: 1 }}>
              <span className="fm-asset-name">{instrumentName}</span>
              <span className="fm-asset-ticker">{selectedInstrument}</span>
            </div>
          </button>
          <button
            className="fm-page-action"
            onClick={() => setPeriodSheetOpen(true)}
            aria-label={`Время · ${timeLabel}`}
          >
            <span className="fm-rail-ico"><Clock size={16} strokeWidth={2.2} /></span>
            <span>Время</span>
          </button>
          <button
            className="fm-page-action"
            onClick={() => setOptionsSheetOpen(true)}
            aria-label={`Опции · ${optionsLabel}`}
          >
            <span className="fm-rail-ico"><Settings size={16} strokeWidth={2.2} /></span>
            <span>Опции</span>
          </button>
        </>
      }
    >
      <MobilePageHeader
        Icon={BarChart3}
        title="Открытый интерес"
        subtitle="Позиции участников · Фьючерсы"
        helpLink="/methodology/oi"
      />

      <div className="fm-frame" style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
        <div style={{ margin: '0 -10px', flex: 1, minHeight: 0 }}>
          <MobileChart series={chartSeries} loading={loading} />
        </div>
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

      {/* Sheet: опции (вариант OI + категория участников) */}
      <MobileSheet
        open={optionsSheetOpen}
        onClose={() => setOptionsSheetOpen(false)}
        title="Опции"
      >
        <div style={{ padding: 16 }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
            Что показать на графике
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginBottom: 20 }}>
            {(Object.keys(VARIANT_LABELS) as OIVariant[]).map((v) => (
              <button
                key={v}
                onClick={() => {
                  setOiVariant(v);
                  setOptionsSheetOpen(false);
                }}
                className={`fm-chip ${oiVariant === v ? 'active' : ''}`}
                style={{ gap: 6 }}
              >
                <span
                  style={{
                    width: 8,
                    height: 8,
                    borderRadius: 99,
                    background: VARIANT_COLORS[v],
                    flexShrink: 0,
                  }}
                />
                {VARIANT_LABELS[v]}
              </button>
            ))}
          </div>

          <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
            Категория участников
          </div>
          <div style={{ display: 'flex', gap: 6 }}>
            {(['YUR', 'FIZ'] as const).map((g) => (
              <button
                key={g}
                className={`fm-chip ${clgroup === g ? 'active' : ''}`}
                onClick={() => {
                  setClgroup(g);
                  setOptionsSheetOpen(false);
                }}
                style={{ flex: 1, justifyContent: 'center' }}
              >
                {g === 'YUR' ? 'Юрлица' : 'Физлица'}
              </button>
            ))}
          </div>
        </div>
      </MobileSheet>
    </MobileLayout>
  );
}
