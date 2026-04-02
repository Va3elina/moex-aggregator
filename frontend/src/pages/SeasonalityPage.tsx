import { useEffect, useState, useMemo, useCallback, useRef } from 'react';
import { Search, ChevronDown, X, BarChart3, TrendingUp, CalendarDays } from 'lucide-react';
import { getInstruments, getSeasonality, getSeasonalityPrice } from '../services/api';
import type { SeasonalityResponse, SeasonalityMode, PriceChartResponse } from '../services/api';
import type { Instrument } from '../types';

const MODE_LABELS: Record<SeasonalityMode, string> = {
  intraday: 'Внутри дня',
  weekday: 'По дням недели',
  monthday: 'Внутри месяца',
  monthly: 'По месяцам',
};

const MODE_ITER_HINT: Record<SeasonalityMode, Record<number, string>> = {
  intraday: { 30: '30 торговых дней', 90: '90 торговых дней' },
  weekday: { 30: '30 недель', 90: '90 недель' },
  monthday: { 30: '~2.5 года', 90: '~7.5 лет' },
  monthly: { 30: '30 лет', 90: '90 лет' },
};

interface PeriodPreset {
  label: string;
  iterations: number;
  description: string;
}

const PERIOD_PRESETS: PeriodPreset[] = [
  { label: 'Актуальный', iterations: 30, description: 'Последние 30 итераций — текущий рыночный контекст' },
  { label: 'Базисный', iterations: 90, description: 'Последние 90 итераций — фундаментальная устойчивость' },
];

const PRICE_PERIODS = [
  { label: '3М', days: 90 },
  { label: '6М', days: 180 },
  { label: '1Г', days: 365 },
  { label: '3Г', days: 1095 },
  { label: '5Л', days: 1825 },
  { label: 'Всё', days: 9999 },
];

type ChartType = 'histogram' | 'price';

export default function SeasonalityPage() {
  // Stock selector
  const [stocks, setStocks] = useState<Instrument[]>([]);
  const [selectedStock, setSelectedStock] = useState<string>('SBER');
  const [selectedName, setSelectedName] = useState<string>('Сбербанк');
  const [searchQuery, setSearchQuery] = useState('');
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // Mode & params
  const [mode, setMode] = useState<SeasonalityMode>('weekday');
  const [activePeriod, setActivePeriod] = useState<PeriodPreset>(PERIOD_PRESETS[1]);
  const [chartType, setChartType] = useState<ChartType>('histogram');
  const [excludeDividends, setExcludeDividends] = useState(false);
  const [priceDays, setPriceDays] = useState(365);

  // Data
  const [dataRaw, setDataRaw] = useState<SeasonalityResponse | null>(null);
  const [dataAdj, setDataAdj] = useState<SeasonalityResponse | null>(null);
  const [priceData, setPriceData] = useState<PriceChartResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Tooltip
  const [tooltip, setTooltip] = useState<{
    x: number; y: number;
    bar?: SeasonalityResponse['bars'][0];
    barAdj?: SeasonalityResponse['bars'][0];
    priceDate?: string; priceClose?: number; priceAdj?: number;
  } | null>(null);

  // Load stocks
  useEffect(() => {
    getInstruments('stock').then(res => {
      const list = res.instruments || [];
      setStocks(list);
      const sber = list.find(s => s.sec_id === 'SBER');
      if (sber) setSelectedName(sber.name);
    }).catch(() => {});
  }, []);

  // Close dropdown on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  // Filtered stocks
  const filteredStocks = useMemo(() => {
    if (!searchQuery) return stocks.slice(0, 50);
    const q = searchQuery.toLowerCase();
    return stocks.filter(s =>
      s.sec_id.toLowerCase().includes(q) || s.name.toLowerCase().includes(q)
    ).slice(0, 50);
  }, [stocks, searchQuery]);

  // Fetch seasonality data
  const fetchSeasonality = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const raw = await getSeasonality(selectedStock, mode, activePeriod.iterations, false);
      setDataRaw(raw);
      if (mode !== 'intraday') {
        const adj = await getSeasonality(selectedStock, mode, activePeriod.iterations, true);
        setDataAdj(adj);
      } else {
        setDataAdj(null);
      }
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Ошибка загрузки');
    } finally {
      setLoading(false);
    }
  }, [selectedStock, mode, activePeriod]);

  // Fetch price data
  const fetchPrice = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await getSeasonalityPrice(selectedStock, priceDays);
      setPriceData(res);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Ошибка загрузки');
    } finally {
      setLoading(false);
    }
  }, [selectedStock, priceDays]);

  useEffect(() => {
    if (!selectedStock) return;
    if (chartType === 'histogram') {
      fetchSeasonality();
    } else {
      fetchPrice();
    }
  }, [chartType, fetchSeasonality, fetchPrice, selectedStock]);

  // Active seasonality data
  const data = excludeDividends && dataAdj ? dataAdj : dataRaw;

  // Chart dimensions
  const chartWidth = 800;
  const chartHeight = 350;
  const marginTop = 30;
  const marginBottom = 50;
  const marginLeft = 60;
  const marginRight = 20;
  const plotWidth = chartWidth - marginLeft - marginRight;
  const plotHeight = chartHeight - marginTop - marginBottom;

  // ===== HISTOGRAM CALCULATIONS =====
  const bars = data?.bars || [];
  const maxAbs = Math.max(...bars.map(b => Math.abs(b.avg_change)), 0.01);
  const barWidth = bars.length > 0 ? Math.min(plotWidth / bars.length * 0.7, 40) : 20;
  const gap = bars.length > 0 ? plotWidth / bars.length : 0;
  const rawBars = dataRaw?.bars || [];

  const getBarColor = (val: number) => {
    const intensity = Math.min(Math.abs(val) / maxAbs, 1);
    if (val >= 0) {
      const r = Math.round(20 + (1 - intensity) * 60);
      const g = Math.round(100 + intensity * 155);
      const b = Math.round(20 + (1 - intensity) * 60);
      return `rgb(${r}, ${g}, ${b})`;
    } else {
      const r = Math.round(100 + intensity * 155);
      const g = Math.round(20 + (1 - intensity) * 60);
      const b = Math.round(20 + (1 - intensity) * 60);
      return `rgb(${r}, ${g}, ${b})`;
    }
  };

  const zeroY = marginTop + plotHeight / 2;
  const scaleY = (val: number) => zeroY - (val / maxAbs) * (plotHeight / 2);

  // ===== PRICE CHART CALCULATIONS =====
  const pricePoints = priceData?.data || [];
  const priceMinMax = useMemo(() => {
    if (pricePoints.length === 0) return { min: 0, max: 1 };
    const allVals = pricePoints.flatMap(p => [p.close, p.adjusted]);
    const min = Math.min(...allVals);
    const max = Math.max(...allVals);
    const range = max - min || 1;
    return { min: min - range * 0.05, max: max + range * 0.05 };
  }, [pricePoints]);

  const scalePriceX = (i: number) => marginLeft + (i / Math.max(pricePoints.length - 1, 1)) * plotWidth;
  const scalePriceY = (v: number) =>
    marginTop + plotHeight - ((v - priceMinMax.min) / (priceMinMax.max - priceMinMax.min)) * plotHeight;

  const rawPricePath = useMemo(() => {
    if (pricePoints.length === 0) return '';
    return pricePoints.map((p, i) =>
      `${i === 0 ? 'M' : 'L'} ${scalePriceX(i)} ${scalePriceY(p.close)}`
    ).join(' ');
  }, [pricePoints, scalePriceX, scalePriceY]);

  const adjPricePath = useMemo(() => {
    if (pricePoints.length === 0) return '';
    return pricePoints.map((p, i) =>
      `${i === 0 ? 'M' : 'L'} ${scalePriceX(i)} ${scalePriceY(p.adjusted)}`
    ).join(' ');
  }, [pricePoints, scalePriceX, scalePriceY]);

  // Price Y ticks
  const priceYTicks = useMemo(() => {
    const count = 5;
    return Array.from({ length: count }, (_, i) => {
      const val = priceMinMax.min + ((priceMinMax.max - priceMinMax.min) * i) / (count - 1);
      return { value: val, y: scalePriceY(val) };
    });
  }, [priceMinMax, scalePriceY]);

  // Price X ticks
  const priceXTicks = useMemo(() => {
    if (pricePoints.length === 0) return [];
    const count = Math.min(7, pricePoints.length);
    return Array.from({ length: count }, (_, i) => {
      const idx = Math.floor((i / Math.max(count - 1, 1)) * (pricePoints.length - 1));
      const d = pricePoints[idx].date;
      return { label: d.slice(5), x: scalePriceX(idx) }; // "MM-DD"
    });
  }, [pricePoints, scalePriceX]);

  const selectStock = (secId: string, name: string) => {
    setSelectedStock(secId);
    setSelectedName(name);
    setDropdownOpen(false);
    setSearchQuery('');
  };

  const showDivToggle = chartType === 'histogram' && mode !== 'intraday';

  return (
    <div className="max-w-7xl mx-auto px-4 md:px-6 py-6 md:py-8">
      {/* Заголовок */}
      <div className="flex items-center gap-3 mb-6">
        <div className="p-3 bg-gradient-to-br from-[#06b6d4] to-[#3b82f6] rounded-xl">
          <CalendarDays className="w-6 h-6 text-white" />
        </div>
        <div>
          <h1 className="text-2xl font-bold text-theme-primary">Сезонность</h1>
          <p className="text-theme-secondary text-sm">Среднее изменение цены по временным периодам</p>
        </div>
      </div>

      {/* Controls Row 1 */}
      <div className="flex flex-wrap items-start gap-4 mb-4">
        {/* Stock selector */}
        <div className="relative" ref={dropdownRef}>
          <button
            onClick={() => setDropdownOpen(!dropdownOpen)}
            className="flex items-center gap-2 px-4 py-2.5 rounded-xl border transition-all"
            style={{
              backgroundColor: 'var(--bg-secondary)',
              borderColor: dropdownOpen ? 'var(--accent)' : 'var(--border-color)',
              color: 'var(--text-primary)',
            }}
          >
            <span className="font-semibold">{selectedStock}</span>
            <span className="text-sm" style={{ color: 'var(--text-muted)' }}>{selectedName}</span>
            <ChevronDown size={16} style={{ color: 'var(--text-muted)' }} />
          </button>

          {dropdownOpen && (
            <div
              className="absolute top-full mt-2 left-0 w-80 rounded-xl border shadow-xl z-50 overflow-hidden"
              style={{ backgroundColor: 'var(--bg-secondary)', borderColor: 'var(--border-color)' }}
            >
              <div className="p-3 border-b" style={{ borderColor: 'var(--border-color)' }}>
                <div className="relative">
                  <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2" style={{ color: 'var(--text-muted)' }} />
                  <input
                    type="text"
                    value={searchQuery}
                    onChange={e => setSearchQuery(e.target.value)}
                    placeholder="Поиск акции..."
                    autoFocus
                    className="w-full pl-9 pr-8 py-2 rounded-lg text-sm border focus:outline-none"
                    style={{
                      backgroundColor: 'var(--bg-primary)',
                      borderColor: 'var(--border-color)',
                      color: 'var(--text-primary)',
                    }}
                  />
                  {searchQuery && (
                    <button onClick={() => setSearchQuery('')} className="absolute right-2 top-1/2 -translate-y-1/2" style={{ color: 'var(--text-muted)' }}>
                      <X size={14} />
                    </button>
                  )}
                </div>
              </div>
              <div className="max-h-64 overflow-y-auto">
                {filteredStocks.map(s => (
                  <button
                    key={s.sec_id}
                    onClick={() => selectStock(s.sec_id, s.name)}
                    className="w-full flex items-center gap-3 px-4 py-2.5 text-left transition-colors hover:bg-white/5"
                    style={{ color: s.sec_id === selectedStock ? 'var(--accent)' : 'var(--text-primary)' }}
                  >
                    <span className="font-medium text-sm w-16">{s.sec_id}</span>
                    <span className="text-sm truncate" style={{ color: 'var(--text-secondary)' }}>{s.name}</span>
                  </button>
                ))}
                {filteredStocks.length === 0 && (
                  <div className="px-4 py-6 text-center text-sm" style={{ color: 'var(--text-muted)' }}>Ничего не найдено</div>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Chart type toggle */}
        <div className="flex rounded-xl border overflow-hidden" style={{ borderColor: 'var(--border-color)' }}>
          <button
            onClick={() => setChartType('histogram')}
            className="flex items-center gap-1.5 px-3 py-2.5 text-sm font-medium transition-all"
            style={{
              backgroundColor: chartType === 'histogram' ? 'var(--accent)' : 'var(--bg-secondary)',
              color: chartType === 'histogram' ? 'var(--bg-primary)' : 'var(--text-secondary)',
            }}
          >
            <BarChart3 size={14} /> Сезонность
          </button>
          <button
            onClick={() => setChartType('price')}
            className="flex items-center gap-1.5 px-3 py-2.5 text-sm font-medium transition-all"
            style={{
              backgroundColor: chartType === 'price' ? 'var(--accent)' : 'var(--bg-secondary)',
              color: chartType === 'price' ? 'var(--bg-primary)' : 'var(--text-secondary)',
            }}
          >
            <TrendingUp size={14} /> Цена
          </button>
        </div>

        {/* Seasonality-specific controls */}
        {chartType === 'histogram' && (
          <>
            {/* Mode tabs */}
            <div className="flex rounded-xl border overflow-hidden" style={{ borderColor: 'var(--border-color)' }}>
              {(Object.keys(MODE_LABELS) as SeasonalityMode[]).map(m => (
                <button
                  key={m}
                  onClick={() => setMode(m)}
                  className="px-3 py-2.5 text-sm font-medium transition-all whitespace-nowrap"
                  style={{
                    backgroundColor: m === mode ? 'var(--accent)' : 'var(--bg-secondary)',
                    color: m === mode ? 'var(--bg-primary)' : 'var(--text-secondary)',
                  }}
                >
                  {MODE_LABELS[m]}
                </button>
              ))}
            </div>

            {/* Period presets */}
            <div className="flex rounded-xl border overflow-hidden" style={{ borderColor: 'var(--border-color)' }}>
              {PERIOD_PRESETS.map(p => (
                <button
                  key={p.label}
                  onClick={() => setActivePeriod(p)}
                  className="px-4 py-2.5 text-sm font-medium transition-all whitespace-nowrap"
                  style={{
                    backgroundColor: activePeriod.label === p.label ? 'var(--accent)' : 'var(--bg-secondary)',
                    color: activePeriod.label === p.label ? 'var(--bg-primary)' : 'var(--text-secondary)',
                  }}
                  title={p.description}
                >
                  {p.label} ({p.iterations})
                </button>
              ))}
            </div>
          </>
        )}

        {/* Price-specific controls */}
        {chartType === 'price' && (
          <div className="flex rounded-xl border overflow-hidden" style={{ borderColor: 'var(--border-color)' }}>
            {PRICE_PERIODS.map(p => (
              <button
                key={p.label}
                onClick={() => setPriceDays(p.days)}
                className="px-3 py-2.5 text-sm font-medium transition-all whitespace-nowrap"
                style={{
                  backgroundColor: priceDays === p.days ? 'var(--accent)' : 'var(--bg-secondary)',
                  color: priceDays === p.days ? 'var(--bg-primary)' : 'var(--text-secondary)',
                }}
              >
                {p.label}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Controls Row 2 */}
      <div className="flex flex-wrap items-center gap-4 mb-4">
        {/* Dividend toggle - for histogram */}
        {showDivToggle && (
          <button
            onClick={() => setExcludeDividends(!excludeDividends)}
            className="flex items-center gap-2 px-4 py-2 rounded-xl border text-sm font-medium transition-all"
            style={{
              backgroundColor: excludeDividends ? 'rgba(34, 197, 94, 0.15)' : 'var(--bg-secondary)',
              borderColor: excludeDividends ? 'rgba(34, 197, 94, 0.5)' : 'var(--border-color)',
              color: excludeDividends ? '#22c55e' : 'var(--text-secondary)',
            }}
          >
            <span className={`inline-block w-3 h-3 rounded-full ${excludeDividends ? 'bg-green-500' : 'bg-gray-500'}`} />
            Без дивидендных гэпов
          </button>
        )}

        {/* Hint */}
        {chartType === 'histogram' && (
          <div className="text-xs" style={{ color: 'var(--text-muted)' }}>
            {activePeriod.iterations} итераций
            {MODE_ITER_HINT[mode]?.[activePeriod.iterations] && ` ≈ ${MODE_ITER_HINT[mode][activePeriod.iterations]}`}
          </div>
        )}

        {chartType === 'price' && priceData && (
          <div className="text-xs" style={{ color: 'var(--text-muted)' }}>
            {priceData.data.length} торговых дней • {priceData.ex_dates_count} дивидендных отсечек
          </div>
        )}
      </div>

      {/* Chart */}
      <div
        className="rounded-2xl border p-4"
        style={{ backgroundColor: 'var(--bg-secondary)', borderColor: 'var(--border-color)' }}
      >
        {loading ? (
          <div className="flex items-center justify-center" style={{ height: chartHeight }}>
            <div className="w-8 h-8 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
          </div>
        ) : error ? (
          <div className="flex items-center justify-center" style={{ height: chartHeight, color: 'var(--text-muted)' }}>
            {error}
          </div>
        ) : chartType === 'histogram' ? (
          /* ===== HISTOGRAM ===== */
          bars.length === 0 ? (
            <div className="flex items-center justify-center" style={{ height: chartHeight, color: 'var(--text-muted)' }}>Нет данных</div>
          ) : (
            <div className="relative">
              <svg viewBox={`0 0 ${chartWidth} ${chartHeight}`} className="w-full" style={{ maxHeight: '450px' }} onMouseLeave={() => setTooltip(null)}>
                {/* Grid */}
                {[-maxAbs, -maxAbs / 2, 0, maxAbs / 2, maxAbs].map((val, i) => (
                  <g key={i}>
                    <line x1={marginLeft} x2={chartWidth - marginRight} y1={scaleY(val)} y2={scaleY(val)}
                      stroke="rgba(255,255,255,0.08)" strokeWidth={val === 0 ? 1.5 : 1} />
                    <text x={marginLeft - 8} y={scaleY(val) + 5} textAnchor="end" fontSize={16} fontWeight={600} fill="#9CA3B8">
                      {val === 0 ? '0' : `${val > 0 ? '+' : ''}${val.toFixed(2)}%`}
                    </text>
                  </g>
                ))}

                {/* Bars */}
                {bars.map((bar, i) => {
                  const x = marginLeft + i * gap + (gap - barWidth) / 2;
                  const barH = Math.abs(bar.avg_change) / maxAbs * (plotHeight / 2);
                  const y = bar.avg_change >= 0 ? zeroY - barH : zeroY;
                  return (
                    <g key={bar.key}
                      onMouseEnter={(e) => {
                        const rect = (e.target as SVGElement).closest('svg')?.getBoundingClientRect();
                        if (rect) {
                          const rawBar = rawBars.find(b => b.key === bar.key);
                          setTooltip({ x: e.clientX - rect.left, y: e.clientY - rect.top, bar, barAdj: excludeDividends ? rawBar : undefined });
                        }
                      }}
                      onMouseMove={(e) => {
                        const rect = (e.target as SVGElement).closest('svg')?.getBoundingClientRect();
                        if (rect) {
                          const rawBar = rawBars.find(b => b.key === bar.key);
                          setTooltip({ x: e.clientX - rect.left, y: e.clientY - rect.top, bar, barAdj: excludeDividends ? rawBar : undefined });
                        }
                      }}
                      onMouseLeave={() => setTooltip(null)}
                      style={{ cursor: 'pointer' }}
                    >
                      <rect x={x} y={y} width={barWidth} height={Math.max(barH, 1)} rx={3}
                        fill={getBarColor(bar.avg_change)} opacity={0.9} />
                      {barH > 20 && (
                        <text x={x + barWidth / 2} y={y + barH / 2 + 4} textAnchor="middle"
                          fontSize={10} fill="white" fontWeight="600">
                          {bar.avg_change > 0 ? '+' : ''}{bar.avg_change.toFixed(3)}%
                        </text>
                      )}
                    </g>
                  );
                })}

                {/* X labels */}
                {bars.map((bar, i) => (
                  <text key={bar.key} x={marginLeft + i * gap + gap / 2} y={chartHeight - 10}
                    textAnchor="middle" fontSize={14} fill="#9CA3B8" fontWeight="600">
                    {bar.label}
                  </text>
                ))}
              </svg>

              {/* Tooltip */}
              {tooltip?.bar && (
                <div className="absolute pointer-events-none px-3 py-2 rounded-lg shadow-lg text-sm z-10"
                  style={{
                    left: Math.min(tooltip.x + 12, chartWidth - 200), top: tooltip.y - 60,
                    backgroundColor: 'var(--bg-primary)', border: '1px solid var(--border-color)', color: 'var(--text-primary)', minWidth: 150,
                  }}>
                  <div className="font-semibold mb-1">{tooltip.bar.label}</div>
                  <div style={{ color: tooltip.bar.avg_change >= 0 ? '#22c55e' : '#ef4444' }}>
                    Среднее: {tooltip.bar.avg_change > 0 ? '+' : ''}{tooltip.bar.avg_change.toFixed(4)}%
                  </div>
                  {excludeDividends && tooltip.barAdj && (
                    <div className="mt-1 pt-1 border-t" style={{ borderColor: 'var(--border-color)', color: 'rgba(239,68,68,0.8)' }}>
                      С гэпами: {tooltip.barAdj.avg_change > 0 ? '+' : ''}{tooltip.barAdj.avg_change.toFixed(4)}%
                    </div>
                  )}
                  <div style={{ color: 'var(--text-muted)' }}>Наблюдений: {tooltip.bar.count}</div>
                </div>
              )}
            </div>
          )
        ) : (
          /* ===== PRICE CHART ===== */
          pricePoints.length === 0 ? (
            <div className="flex items-center justify-center" style={{ height: chartHeight, color: 'var(--text-muted)' }}>Нет данных</div>
          ) : (
            <div className="relative">
              <svg viewBox={`0 0 ${chartWidth} ${chartHeight}`} className="w-full" style={{ maxHeight: '450px' }}
                onMouseMove={(e) => {
                  const rect = (e.target as SVGElement).closest('svg')?.getBoundingClientRect();
                  if (!rect || pricePoints.length === 0) return;
                  const svgX = (e.clientX - rect.left) / rect.width * chartWidth;
                  const dataX = svgX - marginLeft;
                  if (dataX < 0 || dataX > plotWidth) { setTooltip(null); return; }
                  const idx = Math.round((dataX / plotWidth) * (pricePoints.length - 1));
                  const clampedIdx = Math.max(0, Math.min(idx, pricePoints.length - 1));
                  const p = pricePoints[clampedIdx];
                  setTooltip({
                    x: e.clientX - rect.left,
                    y: e.clientY - rect.top,
                    priceDate: p.date,
                    priceClose: p.close,
                    priceAdj: p.adjusted,
                  });
                }}
                onMouseLeave={() => setTooltip(null)}
              >
                {/* Grid */}
                {priceYTicks.map((tick, i) => (
                  <g key={i}>
                    <line x1={marginLeft} x2={chartWidth - marginRight} y1={tick.y} y2={tick.y}
                      stroke="rgba(255,255,255,0.08)" strokeWidth={1} />
                    <text x={marginLeft - 8} y={tick.y + 4} textAnchor="end" fontSize={16} fontWeight={600} fill="var(--text-muted)">
                      {tick.value.toFixed(0)}
                    </text>
                  </g>
                ))}

                {/* X labels */}
                {priceXTicks.map((tick, i) => (
                  <text key={i} x={tick.x} y={chartHeight - 10} textAnchor="middle" fontSize={14} fontWeight={600} fill="#9CA3B8">
                    {tick.label}
                  </text>
                ))}

                {/* Raw price line */}
                {rawPricePath && (
                  <path d={rawPricePath} fill="none" stroke="#C8FF2E" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                )}

                {/* Adjusted price line (if different) */}
                {adjPricePath && adjPricePath !== rawPricePath && (
                  <path d={adjPricePath} fill="none" stroke="#22c55e" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" strokeDasharray="6,3" />
                )}

                {/* Hover crosshair */}
                {tooltip?.priceDate && (() => {
                  const idx = pricePoints.findIndex(p => p.date === tooltip.priceDate);
                  if (idx < 0) return null;
                  const cx = scalePriceX(idx);
                  return (
                    <>
                      <line x1={cx} x2={cx} y1={marginTop} y2={marginTop + plotHeight}
                        stroke="rgba(255,255,255,0.2)" strokeWidth={1} strokeDasharray="4,4" />
                      <circle cx={cx} cy={scalePriceY(pricePoints[idx].close)} r={4}
                        fill="#C8FF2E" stroke="var(--bg-secondary)" strokeWidth={2} />
                      {pricePoints[idx].close !== pricePoints[idx].adjusted && (
                        <circle cx={cx} cy={scalePriceY(pricePoints[idx].adjusted)} r={4}
                          fill="#22c55e" stroke="var(--bg-secondary)" strokeWidth={2} />
                      )}
                    </>
                  );
                })()}
              </svg>

              {/* Legend */}
              <div className="flex items-center gap-4 mt-2 ml-16 text-xs" style={{ color: 'var(--text-muted)' }}>
                <span className="flex items-center gap-1.5">
                  <span className="w-4 h-0.5 inline-block" style={{ backgroundColor: '#C8FF2E' }} /> Цена
                </span>
                {priceData && priceData.ex_dates_count > 0 && (
                  <span className="flex items-center gap-1.5">
                    <span className="w-4 h-0.5 inline-block" style={{ backgroundColor: '#22c55e', borderTop: '1px dashed #22c55e' }} />
                    Без дивидендных гэпов ({priceData.ex_dates_count} отсечек)
                  </span>
                )}
              </div>

              {/* Tooltip */}
              {tooltip?.priceDate && (
                <div className="absolute pointer-events-none px-3 py-2 rounded-lg shadow-lg text-sm z-10"
                  style={{
                    left: Math.min(tooltip.x + 12, chartWidth - 200), top: tooltip.y - 70,
                    backgroundColor: 'var(--bg-primary)', border: '1px solid var(--border-color)', color: 'var(--text-primary)', minWidth: 150,
                  }}>
                  <div className="font-semibold mb-1">{tooltip.priceDate}</div>
                  <div style={{ color: '#C8FF2E' }}>
                    Цена: {tooltip.priceClose?.toFixed(2)} ₽
                  </div>
                  {tooltip.priceAdj !== tooltip.priceClose && (
                    <div style={{ color: '#22c55e' }}>
                      Без гэпов: {tooltip.priceAdj?.toFixed(2)} ₽
                    </div>
                  )}
                </div>
              )}
            </div>
          )
        )}
      </div>

      {/* Description */}
      <div className="mt-4 text-sm" style={{ color: 'var(--text-muted)' }}>
        {chartType === 'histogram'
          ? `Среднее дневное изменение (close-to-close) ${MODE_LABELS[mode].toLowerCase()} — ${activePeriod.label.toLowerCase()} (${activePeriod.iterations} итераций${MODE_ITER_HINT[mode]?.[activePeriod.iterations] ? ` ≈ ${MODE_ITER_HINT[mode][activePeriod.iterations]}` : ''})`
          : `График цены ${selectedStock} — с дивидендными гэпами и без (adjusted close)`
        }
        {chartType === 'histogram' && excludeDividends && <span className="ml-2 text-green-500">• Дивидендные гэпы убраны</span>}
        {chartType === 'histogram' && mode === 'monthday' && <span className="ml-2">• Выходные привязаны к понедельнику</span>}
      </div>
    </div>
  );
}
