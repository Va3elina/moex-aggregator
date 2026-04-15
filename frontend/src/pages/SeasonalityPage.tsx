import { useEffect, useState, useCallback, useRef, useMemo } from 'react';
import { ChevronDown, BarChart3, TrendingUp, CalendarDays, Layers } from 'lucide-react';
import { getSeasonality, getSeasonalityPrice, getSeasonalityYearly, getSeasonalityYears } from '../services/api';
import InstrumentSearchModal from '../components/InstrumentSearchModal';
import SeasonalityHistogram from '../components/seasonality/SeasonalityHistogram';
import SeasonalityPriceChart from '../components/seasonality/SeasonalityPriceChart';
import YearlySeasonalityChart from '../components/seasonality/YearlySeasonalityChart';
import type { SeasonalityResponse, SeasonalityMode, PriceChartResponse, YearlySeasonalityResponse } from '../services/api';
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
  // Для monthly iterations = лет данных, но в БД только ~25 лет доступны.
  // Поэтому «30 лет» корректно (взяли всё), а «90 лет» вводит в заблуждение.
  monthly: { 30: 'вся доступная история', 90: 'вся доступная история' },
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

type ChartType = 'histogram' | 'price' | 'yearly';


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
  // Сравнительные серии для гистограммы (опциональные):
  // - compareYear: добавляет серию «С [год] г.» (загружается полная история от этого года)
  // - showNoOutliers: добавляет серию «Без выбросов» (исключает 2008/2014/2020/2022)
  const [compareYear, setCompareYear] = useState<number | null>(null);
  const [showNoOutliers, setShowNoOutliers] = useState(false);
  // Доступные годы (для dropdown «Сравнить с»). Обновляется при смене тикера.
  const [availableYears, setAvailableYears] = useState<number[]>([]);

  // Фетч доступных лет при смене тикера (и сброс compareYear если он вышел из диапазона)
  useEffect(() => {
    if (!selectedStock) return;
    let cancelled = false;
    getSeasonalityYears(selectedStock).then((resp) => {
      if (cancelled) return;
      setAvailableYears(resp.years);
      // Если выбранный год больше не доступен для этой бумаги — сбрасываем
      if (compareYear !== null && !resp.years.includes(compareYear)) {
        setCompareYear(null);
      }
    }).catch(() => {
      if (!cancelled) setAvailableYears([]);
    });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedStock]);

  // Price navigator
  const [priceNavRange, setPriceNavRange] = useState<[number, number] | null>(null);

  // Animated bars (like flows)
  const [animatedHeights, setAnimatedHeights] = useState<number[]>([]);
  const barsAnimRef = useRef<number | null>(null);
  const prevHeightsRef = useRef<number[]>([]);
  const isFirstBarRender = useRef(true);
  // Request ID для отбрасывания stale-ответов при быстром переключении режимов/периодов.
  const seasonalityReqIdRef = useRef(0);
  // Счётчик успешных фетчей — используется как React key для histogram'а.
  // Каждый инкремент = remount компонента = свежий цикл анимации (wave from 0).
  const [histogramFetchId, setHistogramFetchId] = useState(0);

  // Data
  const [dataRaw, setDataRaw] = useState<SeasonalityResponse | null>(null);
  const [dataAdj, setDataAdj] = useState<SeasonalityResponse | null>(null);
  // Мульти-серии [all / since 2015 / no outliers]. Применяются для
  // monthly/weekday/monthday. intraday не использует (короткая история).
  // Значения уже учитывают текущий excludeDividends (перефетчим при toggle).
  const [monthlySeries, setMonthlySeries] = useState<SeasonalityResponse[] | null>(null);
  const [priceData, setPriceData] = useState<PriceChartResponse | null>(null);
  const [yearlyData, setYearlyData] = useState<YearlySeasonalityResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Tooltip
  const [tooltip, setTooltip] = useState<{
    x: number; y: number;
    bar?: SeasonalityResponse['bars'][0];
    barAdj?: SeasonalityResponse['bars'][0];
    priceDate?: string; priceClose?: number; priceAdj?: number;
    yearlyAvgPct?: number; yearlyCurPct?: number; yearlyCurDate?: string;
  } | null>(null);

  const handleSelectInstrument = (sectype: string, name: string) => {
    setSelectedStock(sectype);
    setSelectedName(name);
    setIsModalOpen(false);
  };

  // Fetch seasonality data — все серии параллельно через Promise.all.
  // Это гарантирует ОДИН re-render после загрузки всех данных (не двойной).
  const fetchSeasonality = useCallback(async () => {
    const reqId = ++seasonalityReqIdRef.current;
    setLoading(true);
    setError(null);
    try {
      const FULL_HISTORY_ITERS = 9999;
      const canCompare = mode !== 'intraday';

      type FetchPromise = ReturnType<typeof getSeasonality>;
      const promises: FetchPromise[] = [
        getSeasonality(selectedStock, mode, activePeriod.iterations, excludeDividends),
      ];
      if (canCompare && compareYear !== null) {
        promises.push(getSeasonality(
          selectedStock, mode, FULL_HISTORY_ITERS, excludeDividends,
          { sinceYear: compareYear },
        ));
      }
      if (canCompare && showNoOutliers) {
        promises.push(getSeasonality(
          selectedStock, mode, FULL_HISTORY_ITERS, excludeDividends,
          { excludeYears: [2008, 2014, 2020, 2022] },
        ));
      }

      const results = await Promise.all(promises);
      // Stale-guard: если во время await пользователь успел переключиться на
      // другой режим/период, игнорируем этот (уже устаревший) ответ.
      if (reqId !== seasonalityReqIdRef.current) return;

      const [baseSeries, ...extras] = results;
      setDataRaw(baseSeries);
      setDataAdj(baseSeries);
      setMonthlySeries(extras.length > 0 ? [baseSeries, ...extras] : null);
      // Инкрементируем счётчик — SeasonalityHistogram получит новый key → remount → анимация
      setHistogramFetchId(id => id + 1);
    } catch (e: unknown) {
      if (reqId !== seasonalityReqIdRef.current) return;
      setError(e instanceof Error ? e.message : 'Ошибка загрузки');
    } finally {
      if (reqId === seasonalityReqIdRef.current) setLoading(false);
    }
  }, [selectedStock, mode, activePeriod, excludeDividends, compareYear, showNoOutliers]);

  // Fetch price data
  const fetchPrice = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await getSeasonalityPrice(selectedStock, priceDays);
      setPriceData(res);
      setPriceNavRange(null);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Ошибка загрузки');
    } finally {
      setLoading(false);
    }
  }, [selectedStock, priceDays]);

  // Fetch yearly seasonality
  const fetchYearly = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await getSeasonalityYearly(selectedStock, excludeDividends);
      setYearlyData(res);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Ошибка загрузки');
    } finally {
      setLoading(false);
    }
  }, [selectedStock, excludeDividends]);

  useEffect(() => {
    if (!selectedStock) return;
    if (chartType === 'histogram') fetchSeasonality();
    else if (chartType === 'price') fetchPrice();
    else fetchYearly();
  }, [chartType, fetchSeasonality, fetchPrice, fetchYearly, selectedStock]);

  // Active seasonality data
  // dataRaw уже содержит значения с учётом excludeDividends (перефетч при toggle),
  // dataAdj больше не используется отдельно — старый fallback оставлен на всякий
  const data = dataRaw;
  void dataAdj; // eslint

  // Chart dimensions
  const chartHeight = 350;

  // ===== HISTOGRAM CALCULATIONS =====
  const bars = data?.bars || [];
  const maxAbs = Math.max(...bars.map(b => Math.abs(b.avg_change)), 0.01);
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const _rawBars = dataRaw?.bars; void _rawBars;

  // Meta для мульти-серий — подписи и цвета согласованы с fetchSeasonality порядком.
  // Базовая серия (всегда первая) + опционально since-year + опционально no-outliers.
  const seriesMeta = useMemo(() => {
    const meta: { key: string; label: string; color: string }[] = [
      { key: 'base', label: 'Все годы', color: '#94A3B8' },
    ];
    if (compareYear !== null) {
      meta.push({ key: 'since', label: `С ${compareYear} г.`, color: '#60A5FA' });
    }
    if (showNoOutliers) {
      meta.push({ key: 'no_outliers', label: 'Без выбросов', color: '#A78BFA' });
    }
    return meta;
  }, [compareYear, showNoOutliers]);

  // Анимация баров — 3 сценария:
  // 1. Первый рендер (нет prev): easeOutCubic + 600ms stagger + 1200ms — бары «вылетают».
  // 2. Смена периода 30↔90 (prev.length === target.length): плавный морф между
  //    значениями, easeInOutCubic + 350ms stagger + 1000ms.
  // 3. Смена режима intraday↔weekday↔monthday↔monthly (разное число баров):
  //    применяем сценарий №1 — fade-in из нуля с каскадом. Это избегает
  //    уродливого ресемплинга и бар-«перескоков» через ноль.
  const easeOutCubic = (t: number) => 1 - Math.pow(1 - t, 3);
  const easeInOutCubic = (t: number) =>
    t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;

  useEffect(() => {
    if (!bars.length) { setAnimatedHeights([]); return; }
    if (barsAnimRef.current) cancelAnimationFrame(barsAnimRef.current);

    const target = bars.map(b => b.avg_change / maxAbs);
    const prev = prevHeightsRef.current;
    const isFirst = isFirstBarRender.current || prev.length === 0;
    const isSameShape = !isFirst && prev.length === target.length;

    // Сценарии 1 и 3 → from = нули; сценарий 2 → from = предыдущие значения
    const from = isSameShape ? prev : new Array(target.length).fill(0);

    isFirstBarRender.current = false;
    // Морф (одинаковое число баров): мягкий easeInOutCubic
    // Fade-in (первый рендер / смена режима): каскадный easeOutCubic
    const totalDuration = isSameShape ? 1000 : 1200;
    const staggerDelay = isSameShape ? 350 : 600;
    const ease = isSameShape ? easeInOutCubic : easeOutCubic;
    const perBarDuration = totalDuration - staggerDelay;
    let startTime: number | null = null;

    const animate = (ts: number) => {
      if (!startTime) startTime = ts;
      const elapsed = ts - startTime;
      const heights = target.map((v, i) => {
        const barDelay = (i / Math.max(target.length - 1, 1)) * staggerDelay;
        const barElapsed = Math.max(0, elapsed - barDelay);
        const t = Math.min(barElapsed / perBarDuration, 1);
        return from[i] + (v - from[i]) * ease(t);
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
              excludeType="futures"
              onlyGroups={chartType === 'price' ? ['Акции'] : undefined}
            />
          )}
        </div>

        {/* Chart type toggle */}
        <div className="flex rounded-xl border overflow-hidden" style={{ borderColor: 'var(--border-color)' }}>
          <button
            onClick={() => setChartType('histogram')}
            className="flex items-center gap-1.5 px-2 md:px-3 py-2 md:py-2.5 text-xs md:text-sm font-medium transition-all"
            style={{
              backgroundColor: chartType === 'histogram' ? 'var(--accent)' : 'var(--bg-secondary)',
              color: chartType === 'histogram' ? 'var(--bg-primary)' : 'var(--text-secondary)',
            }}
          >
            <BarChart3 size={14} /> Сезонность
          </button>
          <button
            onClick={() => setChartType('price')}
            className="flex items-center gap-1.5 px-2 md:px-3 py-2 md:py-2.5 text-xs md:text-sm font-medium transition-all"
            style={{
              backgroundColor: chartType === 'price' ? 'var(--accent)' : 'var(--bg-secondary)',
              color: chartType === 'price' ? 'var(--bg-primary)' : 'var(--text-secondary)',
            }}
          >
            <TrendingUp size={14} /> Цена
          </button>
          <button
            onClick={() => setChartType('yearly')}
            className="flex items-center gap-1.5 px-2 md:px-3 py-2 md:py-2.5 text-xs md:text-sm font-medium transition-all"
            style={{
              backgroundColor: chartType === 'yearly' ? 'var(--accent)' : 'var(--bg-secondary)',
              color: chartType === 'yearly' ? 'var(--bg-primary)' : 'var(--text-secondary)',
            }}
          >
            <Layers size={14} /> Годовая
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
                  className="px-2 md:px-3 py-2 md:py-2.5 text-xs md:text-sm font-medium transition-all whitespace-nowrap"
                  style={{
                    backgroundColor: m === mode ? 'var(--accent)' : 'var(--bg-secondary)',
                    color: m === mode ? 'var(--bg-primary)' : 'var(--text-secondary)',
                  }}
                >
                  {MODE_LABELS[m]}
                </button>
              ))}
            </div>

            {/* Period presets — скрываем для monthly (используется вся доступная
                история, переключатель бесполезен). Показываем для остальных. */}
            {mode !== 'monthly' && (
              <div className="flex rounded-xl border overflow-hidden" style={{ borderColor: 'var(--border-color)' }}>
                {PERIOD_PRESETS.map(p => (
                  <button
                    key={p.label}
                    onClick={() => setActivePeriod(p)}
                    className="px-2 md:px-4 py-2 md:py-2.5 text-xs md:text-sm font-medium transition-all whitespace-nowrap"
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
            )}

            {/* Dividend toggle — на одной линии с пресетами.
                Не показываем в intraday (там дивиденды не применимы). */}
            {showDivToggle && (
              <button
                onClick={() => setExcludeDividends(!excludeDividends)}
                title="Пересчитать цены с учётом реинвестирования дивидендов (Yahoo/CRSP adjusted close). Ортогонально к «Без выбросов»."
                className="flex items-center gap-2 px-2 md:px-4 py-2 md:py-2.5 rounded-xl border text-xs md:text-sm font-medium transition-all whitespace-nowrap"
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

            {/* Toggle «Без выбросов» — показывается ПЕРЕД «Сравнить с» */}
            {mode !== 'intraday' && (
              <button
                onClick={() => setShowNoOutliers(!showNoOutliers)}
                title="Добавить сравнительную серию с исключёнными годами крупных кризисов: 2008 (финкризис), 2014 (санкции/рубль), 2020 (COVID), 2022 (мобилизация). Исключения жёстко зашиты и ортогональны к «Без дивидендных гэпов»."
                className="flex items-center gap-2 px-2 md:px-4 py-2 md:py-2.5 rounded-xl border text-xs md:text-sm font-medium transition-all whitespace-nowrap"
                style={{
                  backgroundColor: showNoOutliers ? 'rgba(167, 139, 250, 0.15)' : 'var(--bg-secondary)',
                  borderColor: showNoOutliers ? 'rgba(167, 139, 250, 0.5)' : 'var(--border-color)',
                  color: showNoOutliers ? '#A78BFA' : 'var(--text-secondary)',
                }}
              >
                <span className={`inline-block w-3 h-3 rounded-full ${showNoOutliers ? '' : 'bg-gray-500'}`}
                  style={showNoOutliers ? { backgroundColor: '#A78BFA' } : {}} />
                Без выбросов
              </button>
            )}

            {/* Сравнить с — button-styled: цветной кружок + текст, overlay'енный
                невидимым select'ом. Клик на весь контрол открывает dropdown. */}
            {mode !== 'intraday' && availableYears.length > 1 && (
              <div className="relative inline-block">
                <div
                  title={compareYear !== null
                    ? `Сравниваем с ${compareYear}–${new Date().getFullYear()}`
                    : 'Добавить сравнительную серию: средние значения по годам от выбранного и до сегодня'}
                  className="flex items-center gap-2 px-2 md:px-4 py-2 md:py-2.5 rounded-xl border text-xs md:text-sm font-medium transition-all whitespace-nowrap cursor-pointer"
                  style={{
                    backgroundColor: compareYear !== null ? 'rgba(96, 165, 250, 0.15)' : 'var(--bg-secondary)',
                    borderColor: compareYear !== null ? 'rgba(96, 165, 250, 0.5)' : 'var(--border-color)',
                    color: compareYear !== null ? '#60A5FA' : 'var(--text-secondary)',
                  }}
                >
                  <span className={`inline-block w-3 h-3 rounded-full ${compareYear === null ? 'bg-gray-500' : ''}`}
                    style={compareYear !== null ? { backgroundColor: '#60A5FA' } : {}} />
                  {compareYear !== null ? `Сравнение: с ${compareYear} г.` : 'Сравнить с годом'}
                  <ChevronDown size={14} className="opacity-60" />
                </div>
                <select
                  value={compareYear ?? ''}
                  onChange={(e) => setCompareYear(e.target.value ? Number(e.target.value) : null)}
                  className="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
                  aria-label="Год сравнения"
                >
                  <option value="">— не сравнивать —</option>
                  {availableYears.slice(1).map(y => (
                    <option key={y} value={y}>С {y} г.</option>
                  ))}
                </select>
              </div>
            )}
          </>
        )}

        {/* Price-specific controls */}
        {chartType === 'price' && (
          <div className="flex rounded-xl border overflow-hidden" style={{ borderColor: 'var(--border-color)' }}>
            {PRICE_PERIODS.map(p => (
              <button
                key={p.label}
                onClick={() => setPriceDays(p.days)}
                className="px-2 md:px-3 py-2 md:py-2.5 text-xs md:text-sm font-medium transition-all whitespace-nowrap"
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

      {/* Controls Row 2 — только подсказки и yearly-контролы.
          Dividend-toggle для histogram переехал в Row 1 рядом с пресетами. */}
      <div className="flex flex-wrap items-center gap-4 mb-4">
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

        {/* Yearly-specific controls */}
        {chartType === 'yearly' && (
          <>
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
            {yearlyData && (
              <div className="text-xs" style={{ color: 'var(--text-muted)' }}>
                Среднее за {yearlyData.years_range} • текущий {yearlyData.current_year} год
              </div>
            )}
          </>
        )}
      </div>

      {/* Chart */}
      <div
        className="rounded-2xl border p-4 relative"
        style={{ backgroundColor: 'var(--bg-secondary)', borderColor: 'var(--border-color)' }}
      >
        {/* Спиннер обновления */}
        {loading && (bars.length > 0 || priceData || yearlyData) && (
          <div className="absolute top-4 right-4 z-20 flex items-center gap-2 bg-theme-tertiary/90 backdrop-blur-sm px-3 py-1.5 rounded-lg border border-theme">
            <div className="w-4 h-4 border-2 border-[#C8FF2E] border-t-transparent rounded-full animate-spin" />
            <span className="text-xs text-theme-secondary">Обновление...</span>
          </div>
        )}
        {loading && bars.length === 0 && !priceData && !yearlyData ? (
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
          <SeasonalityHistogram
            // key меняется после каждого успешного fetch'а → remount → новая анимация
            key={histogramFetchId}
            bars={bars}
            // Если длина animatedHeights не совпадает с bars (переходный кадр
            // после смены режима: новые bars пришли, а rAF ещё не запустился),
            // передаём нули, чтобы не рендерить старые значения на новых барах.
            animatedHeights={animatedHeights.length === bars.length ? animatedHeights : new Array(bars.length).fill(0)}
            maxAbs={maxAbs}
            tooltip={tooltip}
            setTooltip={setTooltip}
            // Мульти-серии активны только если включены compareYear или noOutliers
            monthlySeries={mode !== 'intraday' ? monthlySeries : null}
            seriesMeta={seriesMeta}
          />
        ) : chartType === 'price' ? (
          priceData ? (
            <SeasonalityPriceChart
              priceData={priceData}
              priceNavRange={priceNavRange}
              setPriceNavRange={setPriceNavRange}
              tooltip={tooltip}
              setTooltip={setTooltip}
              chartHeight={chartHeight}
            />
          ) : (
            <div className="flex items-center justify-center" style={{ height: chartHeight, color: 'var(--text-muted)' }}>Нет данных</div>
          )
        ) : (
          yearlyData ? (
            <YearlySeasonalityChart
              yearlyData={yearlyData}
              tooltip={tooltip}
              setTooltip={setTooltip}
              chartHeight={chartHeight}
            />
          ) : (
            <div className="flex items-center justify-center" style={{ height: chartHeight, color: 'var(--text-muted)' }}>Нет данных</div>
          )
        )}
      </div>

      {/* Description */}
      <div className="mt-4 text-sm" style={{ color: 'var(--text-muted)' }}>
        {chartType === 'histogram'
          ? `Среднее дневное изменение (close-to-close) ${MODE_LABELS[mode].toLowerCase()} — ${activePeriod.label.toLowerCase()} (${activePeriod.iterations} итераций${MODE_ITER_HINT[mode]?.[activePeriod.iterations] ? ` ≈ ${MODE_ITER_HINT[mode][activePeriod.iterations]}` : ''})`
          : chartType === 'price'
          ? `График цены ${selectedStock} — с дивидендными гэпами и без (adjusted close)`
          : `Кумулятивное изменение ${selectedStock} с начала года — среднее (${yearlyData?.years_range ?? '...'}) vs ${yearlyData?.current_year ?? ''}`
        }
        {chartType === 'histogram' && excludeDividends && <span className="ml-2 text-green-500">• Дивидендные гэпы убраны</span>}
        {chartType === 'histogram' && mode === 'monthday' && <span className="ml-2">• Выходные привязаны к понедельнику</span>}
      </div>
    </div>
  );
}
