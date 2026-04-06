import { useEffect, useState, useMemo, useCallback, useRef } from 'react';
import { ChevronDown, BarChart3, TrendingUp, CalendarDays } from 'lucide-react';
import { getSeasonality, getSeasonalityPrice } from '../services/api';
import InstrumentSearchModal from '../components/InstrumentSearchModal';
import type { SeasonalityResponse, SeasonalityMode, PriceChartResponse } from '../services/api';
// types removed — InstrumentSearchModal handles instrument loading

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
  const [selectedStock, setSelectedStock] = useState<string>('SBER');
  const [selectedName, setSelectedName] = useState<string>('Сбербанк');
  const [isModalOpen, setIsModalOpen] = useState(false);

  // Mode & params
  const [mode, setMode] = useState<SeasonalityMode>('weekday');
  const [activePeriod, setActivePeriod] = useState<PeriodPreset>(PERIOD_PRESETS[1]);
  const [chartType, setChartType] = useState<ChartType>('histogram');
  const [excludeDividends, setExcludeDividends] = useState(false);
  const [priceDays, setPriceDays] = useState(365);

  // Animated bars (like flows)
  const [animatedHeights, setAnimatedHeights] = useState<number[]>([]);
  const barsAnimRef = useRef<number | null>(null);
  const prevHeightsRef = useRef<number[]>([]);
  const isFirstBarRender = useRef(true);

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

  const handleSelectInstrument = (sectype: string, name: string) => {
    setSelectedStock(sectype);
    setSelectedName(name);
    setIsModalOpen(false);
  };

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
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const _rawBars = dataRaw?.bars; void _rawBars;

  // Анимация баров (как притоки-оттоки)
  const easeOutCubic = (t: number) => 1 - Math.pow(1 - t, 3);

  useEffect(() => {
    if (!bars.length) { setAnimatedHeights([]); return; }
    if (barsAnimRef.current) cancelAnimationFrame(barsAnimRef.current);

    const target = bars.map(b => b.avg_change / maxAbs);
    const isFirst = isFirstBarRender.current || prevHeightsRef.current.length === 0;
    const from = isFirst ? new Array(target.length).fill(0) : (() => {
      // Ресемплинг если кол-во баров изменилось
      const prev = prevHeightsRef.current;
      if (prev.length === target.length) return prev;
      return target.map((_, i) => {
        const si = (i / (target.length - 1)) * (prev.length - 1);
        const lo = Math.floor(si);
        const hi = Math.min(lo + 1, prev.length - 1);
        return prev[lo] + (prev[hi] - prev[lo]) * (si - lo);
      });
    })();

    isFirstBarRender.current = false;
    const totalDuration = isFirst ? 1200 : 600;
    const staggerDelay = isFirst ? 600 : 0;
    let startTime: number | null = null;

    const animate = (ts: number) => {
      if (!startTime) startTime = ts;
      const elapsed = ts - startTime;
      const heights = target.map((v, i) => {
        const barDelay = (i / target.length) * staggerDelay;
        const barElapsed = Math.max(0, elapsed - barDelay);
        const t = Math.min(barElapsed / (totalDuration - staggerDelay), 1);
        return from[i] + (v - from[i]) * easeOutCubic(t);
      });
      setAnimatedHeights(heights);
      if (elapsed < totalDuration) {
        barsAnimRef.current = requestAnimationFrame(animate);
      } else {
        prevHeightsRef.current = target;
      }
    };
    barsAnimRef.current = requestAnimationFrame(animate);
    return () => { if (barsAnimRef.current) cancelAnimationFrame(barsAnimRef.current); };
  }, [bars, maxAbs]);

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

  // adjPricePath removed — inline rendering in SVG

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
        <div className="relative">
          <button
            onClick={() => setIsModalOpen(true)}
            className="widget-flat px-3 md:px-4 py-2 md:py-2.5 text-sm font-medium transition-colors flex items-center gap-2 md:gap-3 min-w-[160px] md:min-w-[200px] hover:opacity-90"
            style={{ color: 'var(--text-primary)' }}
          >
            <div className="w-8 h-8 rounded-full flex-shrink-0 flex items-center justify-center text-[10px] font-bold text-white/70"
              style={{ backgroundColor: `hsl(${selectedStock.split('').reduce((a, c) => a + c.charCodeAt(0), 0) % 360}, 50%, 40%)` }}>
              {selectedStock.slice(0, 2)}
            </div>
            <div className="flex-1 text-left">
              <div className="font-medium">{selectedName}</div>
              <div className="text-xs text-theme-secondary">{selectedStock}</div>
            </div>
            <ChevronDown size={16} className="text-theme-secondary" />
          </button>

          {isModalOpen && (
            <InstrumentSearchModal
              onSelect={handleSelectInstrument}
              onClose={() => setIsModalOpen(false)}
            />
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
        className="rounded-2xl border p-4 relative"
        style={{ backgroundColor: 'var(--bg-secondary)', borderColor: 'var(--border-color)' }}
      >
        {/* Спиннер обновления */}
        {loading && (bars.length > 0 || priceData) && (
          <div className="absolute top-4 right-4 z-20 flex items-center gap-2 bg-theme-tertiary/90 backdrop-blur-sm px-3 py-1.5 rounded-lg border border-theme">
            <div className="w-4 h-4 border-2 border-[#C8FF2E] border-t-transparent rounded-full animate-spin" />
            <span className="text-xs text-theme-secondary">Обновление...</span>
          </div>
        )}
        {loading && bars.length === 0 && !priceData ? (
          <div className="flex items-center justify-center" style={{ aspectRatio: '16/9' }}>
            <div className="flex flex-col items-center gap-3">
              <div className="w-8 h-8 border-2 border-[#06b6d4] border-t-transparent rounded-full animate-spin" />
              <span className="text-theme-secondary">Загрузка...</span>
            </div>
          </div>
        ) : error && bars.length === 0 ? (
          <div className="flex items-center justify-center" style={{ aspectRatio: '16/9', color: 'var(--text-muted)' }}>
            {error}
          </div>
        ) : chartType === 'histogram' ? (
          /* ===== HISTOGRAM ===== */
          bars.length === 0 ? (
            <div className="flex items-center justify-center" style={{ aspectRatio: '16/9', color: 'var(--text-muted)' }}>Нет данных</div>
          ) : (
            <div className="relative overflow-hidden pb-8 cursor-crosshair" style={{ height: 450 }}
              onMouseMove={(e) => {
                const rect = e.currentTarget.getBoundingClientRect();
                const x = e.clientX - rect.left;
                const barAreaWidth = rect.width - 60;
                const idx = Math.floor(x / (barAreaWidth / bars.length));
                if (idx >= 0 && idx < bars.length) {
                  setTooltip({ x: e.clientX - rect.left, y: e.clientY - rect.top, bar: bars[idx] });
                } else {
                  setTooltip(null);
                }
              }}
              onMouseLeave={() => setTooltip(null)}>
              {/* SVG область */}
              <div className="absolute inset-0" style={{ right: 80 }}>
                <svg viewBox="0 0 1000 500" preserveAspectRatio="none" width="100%" height="100%">
                  {/* Бары — анимированные через state */}
                  {animatedHeights.length > 0 && bars.map((bar, i) => {
                    const W = 1000;
                    const H = 500;
                    const slotW = W / bars.length;
                    const bw = slotW * (bars.length > 12 ? 0.6 : 0.5);
                    const bx = i * slotW + (slotW - bw) / 2;
                    const midY = H / 2;
                    const halfH = H * 0.38;
                    const animVal = animatedHeights[i] ?? 0;
                    const h = Math.max(Math.abs(animVal) * halfH, H * 0.005);
                    return (
                      <g key={bar.key}
                        opacity={tooltip?.bar ? (tooltip.bar.key === bar.key ? 1 : 0.35) : 1}
                        className="transition-opacity duration-150">
                        {animVal >= 0 ? (
                          <rect x={bx} y={midY - h} width={bw} height={h}
                            fill="#2EE59D" rx="3" />
                        ) : (
                          <rect x={bx} y={midY} width={bw} height={h}
                            fill="#FF4D4D" rx="3" />
                        )}
                      </g>
                    );
                  })}

                  {/* Горизонтальные линии сетки */}
                  {[-maxAbs, -maxAbs / 2, 0, maxAbs / 2, maxAbs].map((val, i) => {
                    const y = 250 - (val / maxAbs) * 190;
                    return (
                      <line key={i} x1="0" y1={y} x2="1000" y2={y}
                        stroke={val === 0 ? "rgba(255,255,255,0.15)" : "rgba(255,255,255,0.08)"} strokeWidth="1"
                        vectorEffect="non-scaling-stroke" />
                    );
                  })}

                  {/* Вертикальный курсор */}
                  {tooltip?.bar && (() => {
                    const idx = bars.indexOf(tooltip.bar!);
                    if (idx === -1) return null;
                    const slotW = 1000 / bars.length;
                    const cx = idx * slotW + slotW / 2;
                    const gridTop = 250 - 190;  // верхняя линия сетки
                    const gridBot = 250 + 190;  // нижняя линия сетки
                    return (
                      <line x1={cx} y1={gridTop} x2={cx} y2={gridBot}
                        stroke="#C8FF2E" strokeWidth="1" strokeDasharray="4 3"
                        opacity="0.5" vectorEffect="non-scaling-stroke"
                        style={{ pointerEvents: 'none' }} />
                    );
                  })()}
                </svg>
              </div>

              {/* Плавающая дата + тултип как на притоках */}
              {tooltip?.bar && (() => {
                const idx = bars.indexOf(tooltip.bar!);
                if (idx === -1) return null;
                const color = tooltip.bar!.avg_change >= 0 ? '#2EE59D' : '#FF4D4D';
                const valStr = `${tooltip.bar!.avg_change > 0 ? '+' : ''}${Math.abs(tooltip.bar!.avg_change) >= 0.01 ? tooltip.bar!.avg_change.toFixed(3) : tooltip.bar!.avg_change.toFixed(4)}%`;
                return (
                  <>
                    {/* Карточка */}
                    <div className="absolute z-30 pointer-events-none"
                      style={{
                        left: tooltip.x > 300 ? tooltip.x - 168 : tooltip.x + 8,
                        top: Math.min(Math.max(tooltip.y - 20, 4), 330)
                      }}>
                      <div className="bg-theme-tertiary/95 backdrop-blur-sm rounded-lg border border-theme shadow-xl py-1.5 px-3">
                        <div className="flex items-center justify-between gap-3">
                          <div className="flex items-center gap-1.5 min-w-0">
                            <span className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: color }} />
                            <span className="text-[11px] text-theme-secondary truncate">{tooltip.bar!.avg_change >= 0 ? 'Рост' : 'Падение'}</span>
                          </div>
                          <span className="text-xs font-semibold whitespace-nowrap" style={{ color }}>
                            {valStr}
                          </span>
                        </div>
                        <div className="text-[10px] text-theme-secondary mt-0.5">{tooltip.bar!.count} наблюдений</div>
                      </div>
                    </div>
                  </>
                );
              })()}


              {/* Подписи Y справа */}
              {[-maxAbs, -maxAbs / 2, 0, maxAbs / 2, maxAbs].map((val, i) => {
                const yPct = 50 - (val / maxAbs) * 38;
                const label = val === 0 ? '0' : `${val > 0 ? '+' : ''}${val.toFixed(2)}%`;
                return (
                  <div key={i} className="absolute pointer-events-none"
                    style={{ top: `${yPct}%`, right: 4, transform: 'translateY(-50%)' }}>
                    <span className="text-[16px] font-semibold text-[#9CA3B8]">{label}</span>
                  </div>
                );
              })}

              {/* X labels — фиксированные внизу */}
              <div className="absolute bottom-0 left-0 flex justify-between text-[14px] font-semibold text-[#9CA3B8] px-2" style={{ right: 80 }}>
                {bars.map(bar => (
                  <span key={bar.key} className="text-center" style={{ width: `${100 / bars.length}%` }}>{bar.label}</span>
                ))}
              </div>

            </div>
          )
        ) : (
          /* ===== PRICE CHART ===== */
          pricePoints.length === 0 ? (
            <div className="flex items-center justify-center" style={{ height: chartHeight, color: 'var(--text-muted)' }}>Нет данных</div>
          ) : (
            <div className="relative cursor-crosshair" style={{ height: 450 }}
              onMouseMove={(e) => {
                const rect = e.currentTarget.getBoundingClientRect();
                if (pricePoints.length === 0) return;
                // Область графика: left=60px, right=80px от контейнера
                const chartLeft = 60;
                const chartRight = 80;
                const chartAreaWidth = rect.width - chartLeft - chartRight;
                const mouseX = e.clientX - rect.left - chartLeft;
                if (mouseX < 0 || mouseX > chartAreaWidth) { setTooltip(null); return; }
                const idx = Math.round((mouseX / chartAreaWidth) * (pricePoints.length - 1));
                const clampedIdx = Math.max(0, Math.min(idx, pricePoints.length - 1));
                const p = pricePoints[clampedIdx];
                // Пиксельная позиция точки
                const pixelX = chartLeft + (clampedIdx / Math.max(pricePoints.length - 1, 1)) * chartAreaWidth;
                setTooltip({
                  x: pixelX,
                  y: e.clientY - rect.top,
                  priceDate: p.date,
                  priceClose: p.close,
                  priceAdj: p.adjusted,
                });
              }}
              onMouseLeave={() => setTooltip(null)}
            >
              {/* SVG график — растягивается на полную ширину */}
              <div className="absolute" style={{ left: 60, right: 80, top: 10, bottom: 40 }}>
                <svg viewBox={`0 0 ${plotWidth} ${plotHeight}`} preserveAspectRatio="none" width="100%" height="100%">
                  {/* Grid */}
                  {priceYTicks.map((tick, i) => (
                    <line key={i} x1="0" x2={plotWidth} y1={tick.y - marginTop} y2={tick.y - marginTop}
                      stroke="rgba(255,255,255,0.08)" strokeWidth="1" vectorEffect="non-scaling-stroke" />
                  ))}

                  {/* Raw price line */}
                  {rawPricePath && (
                    <path d={pricePoints.map((p, i) =>
                      `${i === 0 ? 'M' : 'L'} ${(i / Math.max(pricePoints.length - 1, 1)) * plotWidth} ${plotHeight - ((p.close - priceMinMax.min) / (priceMinMax.max - priceMinMax.min)) * plotHeight}`
                    ).join(' ')} fill="none" stroke="#C8FF2E" strokeWidth="2" vectorEffect="non-scaling-stroke" strokeLinecap="round" strokeLinejoin="round" />
                  )}

                  {/* Adjusted price line */}
                  {pricePoints.some(p => p.close !== p.adjusted) && (
                    <path d={pricePoints.map((p, i) =>
                      `${i === 0 ? 'M' : 'L'} ${(i / Math.max(pricePoints.length - 1, 1)) * plotWidth} ${plotHeight - ((p.adjusted - priceMinMax.min) / (priceMinMax.max - priceMinMax.min)) * plotHeight}`
                    ).join(' ')} fill="none" stroke="#22c55e" strokeWidth="2" vectorEffect="non-scaling-stroke" strokeLinecap="round" strokeLinejoin="round" strokeDasharray="6,3" />
                  )}

                  {/* Hover crosshair */}
                  {tooltip?.priceDate && (() => {
                    const idx = pricePoints.findIndex(p => p.date === tooltip.priceDate);
                    if (idx < 0) return null;
                    const cx = (idx / Math.max(pricePoints.length - 1, 1)) * plotWidth;
                    const cy = plotHeight - ((pricePoints[idx].close - priceMinMax.min) / (priceMinMax.max - priceMinMax.min)) * plotHeight;
                    return (
                      <>
                        <line x1={cx} x2={cx} y1="0" y2={plotHeight}
                          stroke="#C8FF2E" strokeWidth="1" strokeDasharray="4,3" opacity="0.5" vectorEffect="non-scaling-stroke" />
                        <circle cx={cx} cy={cy} r="4"
                          fill="#C8FF2E" stroke="var(--bg-secondary)" strokeWidth="2" vectorEffect="non-scaling-stroke" />
                      </>
                    );
                  })()}
                </svg>
              </div>

              {/* Y labels — HTML */}
              {priceYTicks.map((tick, i) => (
                <div key={i} className="absolute pointer-events-none" style={{ right: 4, top: `${((tick.y - marginTop) / plotHeight) * 100 * (400/450) + 2}%`, transform: 'translateY(-50%)' }}>
                  <span className="text-[16px] font-semibold text-[#9CA3B8]">{tick.value.toFixed(0)}</span>
                </div>
              ))}

              {/* X labels — HTML */}
              <div className="absolute bottom-0 left-[60px] flex justify-between text-[14px] font-semibold text-[#9CA3B8]" style={{ right: 80 }}>
                {priceXTicks.map((tick, i) => (
                  <span key={i}>{tick.label}</span>
                ))}
              </div>

              {/* Crosshair handled in inner SVG above */}

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

              {/* Tooltip — позиция через процент от ширины */}
              {tooltip?.priceDate && (() => {
                const isRight = tooltip.x > 500;
                return (
                  <div className="absolute pointer-events-none z-30"
                    style={{
                      left: isRight ? tooltip.x - 170 : tooltip.x + 12,
                      top: Math.max(tooltip.y - 50, 4),
                    }}>
                    <div className="bg-theme-tertiary/95 backdrop-blur-sm rounded-lg border border-theme shadow-xl py-1.5 px-3">
                      <div className="text-[10px] text-theme-secondary mb-1">{tooltip.priceDate}</div>
                      <div className="flex items-center gap-2">
                        <span className="w-2 h-2 rounded-full bg-[#C8FF2E]" />
                        <span className="text-[11px] text-theme-secondary">Цена</span>
                        <span className="text-xs font-semibold text-[#C8FF2E] ml-auto">{tooltip.priceClose?.toFixed(2)} ₽</span>
                      </div>
                      {tooltip.priceAdj !== tooltip.priceClose && (
                        <div className="flex items-center gap-2 mt-0.5">
                          <span className="w-2 h-2 rounded-full bg-[#22c55e]" />
                          <span className="text-[11px] text-theme-secondary">Без гэпов</span>
                          <span className="text-xs font-semibold text-[#22c55e] ml-auto">{tooltip.priceAdj?.toFixed(2)} ₽</span>
                        </div>
                      )}
                    </div>
                  </div>
                );
              })()}
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
