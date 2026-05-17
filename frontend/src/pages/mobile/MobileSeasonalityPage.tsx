/**
 * MobileSeasonalityPage — мобильная версия «Сезонность».
 *
 * Phase 3 простая версия:
 *   - Asset chip (Сбербанк / другой тикер через MobileAssetSearch)
 *   - Mode chips (Дни/Месяцы/Дни месяца)
 *   - SVG histogram сезонности (зелёные/красные столбики)
 *   - Compare years, exclude dividends — Phase 4
 */
import { useEffect, useMemo, useState } from 'react';
import { CalendarDays } from 'lucide-react';
import MobileLayout from '../../components/mobile/MobileLayout';
import MobilePageHeader from '../../components/mobile/MobilePageHeader';
import MobileQuickActions from '../../components/mobile/MobileQuickActions';
import MobileAssetSearch from '../../components/mobile/MobileAssetSearch';
import MobileSheet from '../../components/mobile/MobileSheet';
import MobileSkeleton from '../../components/mobile/MobileSkeleton';
import {
  getSeasonality,
  type SeasonalityResponse,
  type SeasonalityMode,
} from '../../services/api';

const MODE_LABELS: Record<SeasonalityMode, string> = {
  weekday: 'Дни недели',
  monthly: 'Месяцы',
  monthday: 'Дни месяца',
  intraday: 'Часы',
};

const WEEKDAY_LABELS = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт'];
const MONTH_LABELS = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек'];

export default function MobileSeasonalityPage() {
  const [selectedStock, setSelectedStock] = useState('SBER');
  const [selectedName, setSelectedName] = useState('Сбербанк');
  const [mode, setMode] = useState<SeasonalityMode>('monthly');
  const [data, setData] = useState<SeasonalityResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [assetSearchOpen, setAssetSearchOpen] = useState(false);
  const [modeSheetOpen, setModeSheetOpen] = useState(false);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        setLoading(true);
        const result = await getSeasonality(selectedStock, mode);
        if (!cancelled) setData(result);
      } catch (err) {
        console.error('Ошибка seasonality:', err);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [selectedStock, mode]);

  // Преобразуем bars в формат для рендера
  const bars = useMemo(() => {
    if (!data?.bars) return [];
    return data.bars.map((b) => {
      let label = String(b.key);
      if (mode === 'weekday') label = WEEKDAY_LABELS[b.key - 1] ?? label;
      else if (mode === 'monthly') label = MONTH_LABELS[b.key - 1] ?? label;
      else if (mode === 'monthday') label = String(b.key);
      return { label, value: b.avg_change, count: b.count };
    });
  }, [data, mode]);

  // Max absolute для нормализации высоты столбцов
  const maxAbs = useMemo(() => {
    if (bars.length === 0) return 1;
    return Math.max(...bars.map((b) => Math.abs(b.value)), 0.1);
  }, [bars]);

  return (
    <MobileLayout>
      <MobilePageHeader
        Icon={CalendarDays}
        title="Сезонность"
        subtitle={`${selectedName} · ${MODE_LABELS[mode]}`}
        helpLink="/methodology/seasonality"
      />

      <div className="fm-frame">
        {/* SVG histogram */}
        <div style={{ position: 'relative', height: 240, margin: '0 -4px' }}>
          {loading ? (
            <MobileSkeleton variant="chart" height={240} />
          ) : bars.length === 0 ? (
            <div style={{ display: 'grid', placeItems: 'center', height: '100%', color: 'var(--text-muted)' }}>
              Нет данных
            </div>
          ) : (
            <SeasonalityBars bars={bars} maxAbs={maxAbs} />
          )}
        </div>

        <MobileQuickActions
          asset={{ name: selectedName, ticker: selectedStock }}
          timeLabel={MODE_LABELS[mode]}
          optionsLabel="Среднее"
          onAsset={() => setAssetSearchOpen(true)}
          onTime={() => setModeSheetOpen(true)}
          onOptions={() => setModeSheetOpen(true)}
          showFullscreen={false}
          showExport={false}
        />
      </div>

      <MobileAssetSearch
        open={assetSearchOpen}
        onClose={() => setAssetSearchOpen(false)}
        filterType="stock"
        onSelect={(sectype, name) => {
          setSelectedStock(sectype);
          setSelectedName(name);
        }}
      />

      <MobileSheet
        open={modeSheetOpen}
        onClose={() => setModeSheetOpen(false)}
        title="Режим"
      >
        <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 8 }}>
          {(['weekday', 'monthly', 'monthday'] as SeasonalityMode[]).map((m) => (
            <button
              key={m}
              className={`fm-chip ${mode === m ? 'active' : ''}`}
              onClick={() => {
                setMode(m);
                setModeSheetOpen(false);
              }}
              style={{ justifyContent: 'flex-start', padding: '14px 16px' }}
            >
              {MODE_LABELS[m]}
            </button>
          ))}
        </div>
      </MobileSheet>
    </MobileLayout>
  );
}

// ──────────────────────────────────────────────────────────
// Sub-component: SVG histogram bars
// ──────────────────────────────────────────────────────────

interface BarData {
  label: string;
  value: number;
  count: number;
}

function SeasonalityBars({ bars, maxAbs }: { bars: BarData[]; maxAbs: number }) {
  const width = 360; // logical, viewBox scaling
  const height = 240;
  const padX = 12;
  const padTop = 12;
  const padBottom = 24;
  const innerW = width - padX * 2;
  const innerH = height - padTop - padBottom;
  const zeroY = padTop + innerH / 2;
  const barW = (innerW / bars.length) * 0.7;
  const barGap = (innerW / bars.length) * 0.3;

  return (
    <svg width="100%" height={height} viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
      {/* Zero line */}
      <line
        x1={padX}
        y1={zeroY}
        x2={padX + innerW}
        y2={zeroY}
        stroke="color-mix(in srgb, var(--text-primary) 30%, transparent)"
        strokeWidth={1}
      />

      {bars.map((b, i) => {
        const x = padX + i * (barW + barGap) + barGap / 2;
        const hRel = (Math.abs(b.value) / maxAbs) * (innerH / 2);
        const isPositive = b.value >= 0;
        const y = isPositive ? zeroY - hRel : zeroY;
        const fill = isPositive ? 'var(--funds-flow-positive, #5BD49C)' : 'var(--funds-flow-negative, #FF7A5C)';

        return (
          <g key={i}>
            <rect
              x={x}
              y={y}
              width={barW}
              height={Math.max(hRel, 1)}
              fill={fill}
              rx={2}
            />
            {/* Label below x-axis */}
            <text
              x={x + barW / 2}
              y={height - 10}
              fontSize={9}
              fontWeight={600}
              fill="var(--text-secondary)"
              textAnchor="middle"
            >
              {b.label}
            </text>
            {/* Value above bar */}
            {Math.abs(b.value) >= maxAbs * 0.15 && (
              <text
                x={x + barW / 2}
                y={isPositive ? y - 3 : y + hRel + 10}
                fontSize={8}
                fontWeight={700}
                fill={isPositive ? 'var(--funds-flow-positive, #5BD49C)' : 'var(--funds-flow-negative, #FF7A5C)'}
                textAnchor="middle"
              >
                {b.value >= 0 ? '+' : ''}{b.value.toFixed(1)}%
              </text>
            )}
          </g>
        );
      })}
    </svg>
  );
}
