import { useEffect, useState, useCallback, useMemo, useRef } from 'react';
import { ChevronDown, BarChart3, TrendingUp, CalendarDays, Layers, X, Plus } from 'lucide-react';
import { getSeasonality, getSeasonalityPrice, getSeasonalityYearly, getSeasonalityYears } from '../services/api';
import InstrumentSearchModal from '../components/InstrumentSearchModal';
import SeasonalityHistogram from '../components/seasonality/SeasonalityHistogram';
import SeasonalityPriceChart from '../components/seasonality/SeasonalityPriceChart';
import YearlySeasonalityChart from '../components/seasonality/YearlySeasonalityChart';
import type { SeasonalityResponse, SeasonalityMode, PriceChartResponse, YearlySeasonalityResponse } from '../services/api';
import { FUND_PALETTE } from '../config/chartTheme';

const MODE_LABELS: Record<SeasonalityMode, string> = {
  intraday: 'Внутри дня',
  weekday: 'По дням недели',
  monthday: 'Внутри месяца',
  monthly: 'По месяцам',
};

const PRICE_PERIODS = [
  { label: '3М', days: 90 },
  { label: '6М', days: 180 },
  { label: '1Г', days: 365 },
  { label: '3Г', days: 1095 },
  { label: '5Л', days: 1825 },
  { label: 'Всё', days: 9999 },
];

type ChartType = 'histogram' | 'price' | 'yearly';

// Всегда запрашиваем полную историю — логика "Актуальный/Базисный" (30/90 итераций)
// убрана по просьбе пользователя. Бэкенд принимает iterations до 9999.
const FULL_HISTORY_ITERS = 9999;

// Цвета для спец-серий (noOutliers, exactYear). Серии "Период с" используют FUND_PALETTE.
const COLOR_NO_OUTLIERS = '#A78BFA';
const COLOR_EXACT_YEAR = '#F97316';


export default function SeasonalityPage() {
  // Stock selector
  const [selectedStock, setSelectedStock] = useState<string>('SBER');
  const [selectedName, setSelectedName] = useState<string>('Сбербанк');
  const [isModalOpen, setIsModalOpen] = useState(false);

  // Mode & params
  const [mode, setMode] = useState<SeasonalityMode>('weekday');
  const [chartType, setChartType] = useState<ChartType>('histogram');
  const [excludeDividends, setExcludeDividends] = useState(false);
  const [priceDays, setPriceDays] = useState(365);

  // Серии:
  // - compareYears: массив годов для серий "Период с YYYY". По умолчанию [min_year] — это и есть
  //   аналог старой "средней за все годы", только теперь одна из равноправных серий.
  //   Пользователь может добавить ещё годов через "+" или убрать через ×.
  // - showNoOutliers: отдельная серия "Без выбросов" (исключает 2008/2014/2020/2022).
  // - showExactYear: траектория конкретного года (одна линия).
  const [compareYears, setCompareYears] = useState<number[]>([]);
  const [showNoOutliers, setShowNoOutliers] = useState(false);
  const [showExactYear, setShowExactYear] = useState<number | null>(null);
  // Доступные годы (для dropdown). Обновляется при смене тикера.
  const [availableYears, setAvailableYears] = useState<number[]>([]);

  // Фетч доступных лет при смене тикера.
  // При смене актива полностью СБРАСЫВАЕМ compareYears и showExactYear:
  // - compareYears → [min_year нового актива] (наибольший доступный период)
  // - showExactYear → null
  // Сохранение выбора "от прошлого актива" сбивает с толку — у каждого инструмента
  // своя история данных, начало периодов разное. Пользователь ожидает чистый старт.
  useEffect(() => {
    if (!selectedStock) return;
    let cancelled = false;
    getSeasonalityYears(selectedStock).then((resp) => {
      if (cancelled) return;
      setAvailableYears(resp.years);
      // Всегда ставим compareYears = [min_year] нового актива
      if (resp.years.length > 0) {
        setCompareYears([resp.years[0]]);
      } else {
        setCompareYears([]);
      }
      // showExactYear всегда сбрасываем при смене актива
      setShowExactYear(null);
    }).catch(() => {
      if (!cancelled) {
        setAvailableYears([]);
        setCompareYears([]);
        setShowExactYear(null);
      }
    });
    return () => { cancelled = true; };
  }, [selectedStock]);

  // Price navigator
  const [priceNavRange, setPriceNavRange] = useState<[number, number] | null>(null);

  // Request ID для отбрасывания stale-ответов при быстром переключении
  const seasonalityReqIdRef = useRef(0);
  // Счётчик успешных фетчей — React key для histogram'а (remount = новая анимация волны)
  const [histogramFetchId, setHistogramFetchId] = useState(0);

  // Data
  const [dataRaw, setDataRaw] = useState<SeasonalityResponse | null>(null);
  // Мульти-серии: первая = первая compareYear, остальные — доп. серии (noOutliers, exactYear).
  // Если серий нет или одна — null (single-bar mode в histogram).
  const [monthlySeries, setMonthlySeries] = useState<SeasonalityResponse[] | null>(null);
  const [priceData, setPriceData] = useState<PriceChartResponse | null>(null);
  const [yearlyData, setYearlyData] = useState<YearlySeasonalityResponse | null>(null);
  // Мульти-серии для годовой — тот же паттерн, что monthlySeries.
  const [yearlySeries, setYearlySeries] = useState<YearlySeasonalityResponse[] | null>(null);
  const yearlyReqIdRef = useRef(0);
  const [yearlyFetchId, setYearlyFetchId] = useState(0);
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

  // Fetch seasonality (histogram) — все серии параллельно.
  // Порядок промисов = порядок compareYears, затем noOutliers, затем exactYear.
  // Должен совпадать с seriesMeta.
  const fetchSeasonality = useCallback(async () => {
    const reqId = ++seasonalityReqIdRef.current;
    setLoading(true);
    setError(null);
    try {
      type FP = ReturnType<typeof getSeasonality>;
      const promises: FP[] = [];

      // Серии "Период с YYYY"
      compareYears.forEach(yr => {
        promises.push(getSeasonality(
          selectedStock, mode, FULL_HISTORY_ITERS, excludeDividends,
          { sinceYear: yr },
        ));
      });
      // "Без выбросов"
      if (showNoOutliers) {
        promises.push(getSeasonality(
          selectedStock, mode, FULL_HISTORY_ITERS, excludeDividends,
          { excludeYears: [2008, 2014, 2020, 2022] },
        ));
      }
      // "Показать год" (exact)
      if (showExactYear !== null) {
        const currentYear = new Date().getFullYear();
        const allYearsExceptExact = availableYears.filter(
          y => y !== showExactYear && y < currentYear
        );
        promises.push(getSeasonality(
          selectedStock, mode, FULL_HISTORY_ITERS, excludeDividends,
          { sinceYear: showExactYear, excludeYears: allYearsExceptExact },
        ));
      }

      if (promises.length === 0) {
        // Ни одной серии не выбрано — empty state
        setDataRaw(null);
        setMonthlySeries(null);
        setHistogramFetchId(id => id + 1);
        return;
      }

      const results = await Promise.all(promises);
      // Stale-guard
      if (reqId !== seasonalityReqIdRef.current) return;

      // Первая серия — база для maxAbs и single-bar отображения (если серия одна).
      setDataRaw(results[0]);
      setMonthlySeries(results.length > 1 ? results : null);
      setHistogramFetchId(id => id + 1);
    } catch (e: unknown) {
      if (reqId !== seasonalityReqIdRef.current) return;
      setError(e instanceof Error ? e.message : 'Ошибка загрузки');
    } finally {
      if (reqId === seasonalityReqIdRef.current) setLoading(false);
    }
  }, [selectedStock, mode, excludeDividends, compareYears, showNoOutliers, showExactYear, availableYears]);

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

  // Fetch yearly seasonality — тот же паттерн что histogram.
  // Порядок: compareYears[], noOutliers?, exactYear? — должен совпадать с seriesMeta.
  const fetchYearly = useCallback(async () => {
    const reqId = ++yearlyReqIdRef.current;
    setLoading(true);
    setError(null);
    try {
      type FP = ReturnType<typeof getSeasonalityYearly>;
      const promises: FP[] = [];

      compareYears.forEach(yr => {
        promises.push(getSeasonalityYearly(selectedStock, excludeDividends,
          { sinceYear: yr }));
      });
      if (showNoOutliers) {
        promises.push(getSeasonalityYearly(selectedStock, excludeDividends,
          { excludeYears: [2008, 2014, 2020, 2022] }));
      }
      if (showExactYear !== null) {
        const currentYear = new Date().getFullYear();
        const allYearsExceptExact = availableYears.filter(
          y => y !== showExactYear && y < currentYear
        );
        promises.push(getSeasonalityYearly(selectedStock, excludeDividends,
          { sinceYear: showExactYear, excludeYears: allYearsExceptExact }));
      }

      if (promises.length === 0) {
        // Хотя бы одну серию надо загрузить, чтобы получить current_year + years_range.
        // В этом случае берём с sinceYear=min_year (де-факто "все годы"), но не показываем.
        // Проще — empty state.
        setYearlyData(null);
        setYearlySeries(null);
        setYearlyFetchId(id => id + 1);
        return;
      }

      const results = await Promise.all(promises);
      if (reqId !== yearlyReqIdRef.current) return;
      setYearlyData(results[0]);
      setYearlySeries(results.length > 1 ? results : null);
      setYearlyFetchId(id => id + 1);
    } catch (e: unknown) {
      if (reqId !== yearlyReqIdRef.current) return;
      setError(e instanceof Error ? e.message : 'Ошибка загрузки');
    } finally {
      if (reqId === yearlyReqIdRef.current) setLoading(false);
    }
  }, [selectedStock, excludeDividends, compareYears, showNoOutliers, showExactYear, availableYears]);

  useEffect(() => {
    if (!selectedStock) return;
    if (chartType === 'histogram') fetchSeasonality();
    else if (chartType === 'price') fetchPrice();
    else fetchYearly();
  }, [chartType, fetchSeasonality, fetchPrice, fetchYearly, selectedStock]);

  const data = dataRaw;

  // Chart dimensions
  const chartHeight = 350;

  // ===== HISTOGRAM CALCULATIONS =====
  const bars = data?.bars || [];
  const maxAbs = Math.max(...bars.map(b => Math.abs(b.avg_change)), 0.01);

  // Meta для мульти-серий — подписи и цвета. Порядок ДОЛЖЕН совпадать
  // с порядком promises в fetchSeasonality и fetchYearly.
  const seriesMeta = useMemo(() => {
    const meta: { key: string; label: string; color: string }[] = [];
    compareYears.forEach((yr, idx) => {
      meta.push({
        key: `since-${yr}`,
        label: `С ${yr} г.`,
        color: FUND_PALETTE[idx % FUND_PALETTE.length],
      });
    });
    if (showNoOutliers) {
      meta.push({ key: 'no_outliers', label: 'Без выбросов', color: COLOR_NO_OUTLIERS });
    }
    if (showExactYear !== null) {
      meta.push({ key: 'exact', label: `${showExactYear} год`, color: COLOR_EXACT_YEAR });
    }
    return meta;
  }, [compareYears, showNoOutliers, showExactYear]);

  // Инструменты без дивидендов: индексы, валюты, сырьё.
  // Кнопка «Без дивидендных гэпов» бесполезна для них — прячем.
  const NON_DIVIDEND_TICKERS = new Set([
    'IMOEX', 'RTSI', 'RGBI', 'RVI', 'MCFTR', 'RGBITR', 'RUSFAR3M',
    'GLDRUB_TOM', 'USD000UTSTOM', 'EUR_RUB__TOM', 'CNYRUB_TOM',
    // Вечные фьючерсы
    'USDRUBF', 'EURRUBF', 'CNYRUBF', 'IMOEXF',
  ]);
  const hasDividends = !NON_DIVIDEND_TICKERS.has(selectedStock);
  const showDivToggle = (chartType === 'histogram' || chartType === 'yearly') && hasDividends;

  // Годы, которые ещё можно добавить как compareYear (не в списке + не текущий)
  const currentYearNum = new Date().getFullYear();
  const addableYears = availableYears.filter(
    y => !compareYears.includes(y) && y < currentYearNum
  );

  // Рендерер блока "Период с N / + / ×" — используется в histogram и yearly row2
  const renderCompareYearsControls = () => {
    if (availableYears.length === 0) return null;
    return (
      <>
        {compareYears.map((yr, idx) => {
          const color = FUND_PALETTE[idx % FUND_PALETTE.length];
          return (
            <div
              key={yr}
              className="flex items-center gap-2 px-2 md:px-3 py-2 md:py-2.5 rounded-xl border text-xs md:text-sm font-medium whitespace-nowrap"
              style={{
                backgroundColor: `${color}26`, // ~15% opacity
                borderColor: `${color}80`,
                color: color,
              }}
              title={`Серия "Период с ${yr} г." — средние значения по годам от ${yr} до сегодня`}
            >
              <span className="inline-block w-3 h-3 rounded-full" style={{ backgroundColor: color }} />
              Период с {yr}
              <button
                onClick={() => setCompareYears(compareYears.filter(y => y !== yr))}
                className="ml-1 opacity-60 hover:opacity-100 transition-opacity"
                title="Убрать"
                aria-label={`Убрать серию с ${yr} г.`}
              >
                <X size={14} />
              </button>
            </div>
          );
        })}
        {addableYears.length > 0 && (
          <div className="relative inline-block">
            <div
              className="flex items-center gap-1.5 px-2 md:px-3 py-2 md:py-2.5 rounded-xl border text-xs md:text-sm font-medium transition-all whitespace-nowrap cursor-pointer hover:opacity-80"
              style={{
                backgroundColor: 'var(--bg-secondary)',
                borderColor: 'var(--border-color)',
                color: 'var(--text-secondary)',
              }}
              title="Добавить ещё одну серию 'Период с' для сравнения"
            >
              <Plus size={14} />
              Период с
              <ChevronDown size={14} className="opacity-60" />
            </div>
            <select
              value=""
              onChange={(e) => {
                const yr = Number(e.target.value);
                if (yr && !compareYears.includes(yr)) {
                  setCompareYears([...compareYears, yr].sort((a, b) => a - b));
                }
              }}
              className="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
              aria-label="Добавить период"
            >
              <option value="">— выберите год —</option>
              {addableYears.map(y => (
                <option key={y} value={y}>С {y} г.</option>
              ))}
            </select>
          </div>
        )}
      </>
    );
  };

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

        {/* Histogram-specific: mode tabs */}
        {chartType === 'histogram' && (
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

      {/* Controls Row 2 — общие для histogram и yearly:
          "Без дивгэпов", "Без выбросов", compareYears pills + "+", "Показать год" (yearly only).
          Теперь работают и в intraday (для intraday див-гэпы = исключение ex-div дней целиком).
          Для price — отдельная строка с информацией */}
      <div className="flex flex-wrap items-center gap-2 md:gap-4 mb-4">
        {chartType === 'price' && priceData && (
          <div className="text-xs" style={{ color: 'var(--text-muted)' }}>
            {priceData.data.length} торговых дней • {priceData.ex_dates_count} дивидендных отсечек
          </div>
        )}

        {(chartType === 'histogram' || chartType === 'yearly') && (
          <>
            {/* Без дивидендных гэпов */}
            {showDivToggle && (
              <button
                onClick={() => setExcludeDividends(!excludeDividends)}
                title={mode === 'intraday' && chartType === 'histogram'
                  ? 'Исключить торговые дни, являющиеся экс-дивидендными (в день отсечки intraday-return искажён сдвигом open)'
                  : 'Пересчитать цены с учётом реинвестирования дивидендов (Yahoo/CRSP adjusted close)'}
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

            {/* Без выбросов */}
            <button
              onClick={() => setShowNoOutliers(!showNoOutliers)}
              title="Добавить серию с исключёнными годами крупных кризисов: 2008, 2014, 2020, 2022"
              className="flex items-center gap-2 px-2 md:px-4 py-2 md:py-2.5 rounded-xl border text-xs md:text-sm font-medium transition-all whitespace-nowrap"
              style={{
                backgroundColor: showNoOutliers ? 'rgba(167, 139, 250, 0.15)' : 'var(--bg-secondary)',
                borderColor: showNoOutliers ? 'rgba(167, 139, 250, 0.5)' : 'var(--border-color)',
                color: showNoOutliers ? COLOR_NO_OUTLIERS : 'var(--text-secondary)',
              }}
            >
              <span className={`inline-block w-3 h-3 rounded-full ${showNoOutliers ? '' : 'bg-gray-500'}`}
                style={showNoOutliers ? { backgroundColor: COLOR_NO_OUTLIERS } : {}} />
              Без выбросов
            </button>

            {/* Период с — множественные pills + кнопка "+" */}
            {renderCompareYearsControls()}

            {/* "Показать год" — только yearly */}
            {chartType === 'yearly' && availableYears.length > 1 && (
              <div className="relative inline-block">
                <div
                  title={showExactYear !== null ? `Траектория ${showExactYear} года` : 'Наложить траекторию конкретного года'}
                  className="flex items-center gap-2 px-2 md:px-4 py-2 md:py-2.5 rounded-xl border text-xs md:text-sm font-medium transition-all whitespace-nowrap cursor-pointer"
                  style={{
                    backgroundColor: showExactYear !== null ? 'rgba(249, 115, 22, 0.15)' : 'var(--bg-secondary)',
                    borderColor: showExactYear !== null ? 'rgba(249, 115, 22, 0.5)' : 'var(--border-color)',
                    color: showExactYear !== null ? COLOR_EXACT_YEAR : 'var(--text-secondary)',
                  }}
                >
                  <span className={`inline-block w-3 h-3 rounded-full ${showExactYear === null ? 'bg-gray-500' : ''}`}
                    style={showExactYear !== null ? { backgroundColor: COLOR_EXACT_YEAR } : {}} />
                  {showExactYear !== null ? `Год: ${showExactYear}` : 'Показать год'}
                  <ChevronDown size={14} className="opacity-60" />
                </div>
                <select
                  value={showExactYear ?? ''}
                  onChange={(e) => setShowExactYear(e.target.value ? Number(e.target.value) : null)}
                  className="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
                >
                  <option value="">— не показывать —</option>
                  {availableYears.filter(y => y < currentYearNum).map(y => (
                    <option key={y} value={y}>{y}</option>
                  ))}
                </select>
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
          bars.length === 0 && compareYears.length === 0 ? (
            <div className="flex items-center justify-center text-center px-4" style={{ aspectRatio: '16/9', color: 'var(--text-muted)' }}>
              Выберите хотя бы один "Период с" в меню выше
            </div>
          ) : (
            <SeasonalityHistogram
              key={histogramFetchId}
              bars={bars}
              maxAbs={maxAbs}
              tooltip={tooltip}
              setTooltip={setTooltip}
              monthlySeries={monthlySeries}
              seriesMeta={seriesMeta}
            />
          )
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
        ) : yearlyData ? (
          <YearlySeasonalityChart
            key={yearlyFetchId}
            yearlyData={yearlyData}
            seriesData={yearlySeries}
            seriesMeta={seriesMeta}
            tooltip={tooltip}
            setTooltip={setTooltip}
            chartHeight={chartHeight}
          />
        ) : (
          <div className="flex items-center justify-center text-center px-4" style={{ height: chartHeight, color: 'var(--text-muted)' }}>
            {compareYears.length === 0 ? 'Выберите хотя бы один "Период с" в меню выше' : 'Нет данных'}
          </div>
        )}
      </div>

      {/* Description */}
      <div className="mt-4 text-sm" style={{ color: 'var(--text-muted)' }}>
        {chartType === 'histogram'
          ? `Среднее изменение (${mode === 'intraday' ? 'open-to-close per hour' : 'close-to-close'}) ${MODE_LABELS[mode].toLowerCase()}`
          : chartType === 'price'
          ? `График цены ${selectedStock} — с дивидендными гэпами и без (adjusted close)`
          : `Кумулятивное изменение ${selectedStock} с начала года • ${yearlyData?.current_year ?? ''}`
        }
        {(chartType === 'histogram' || chartType === 'yearly') && excludeDividends && (
          <span className="ml-2 text-green-500">
            • {mode === 'intraday' && chartType === 'histogram' ? 'Экс-дивидендные дни исключены' : 'Дивидендные гэпы убраны'}
          </span>
        )}
        {chartType === 'histogram' && mode === 'monthday' && <span className="ml-2">• Выходные привязаны к понедельнику</span>}
      </div>
    </div>
  );
}
