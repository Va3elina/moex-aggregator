import { useEffect, useState, useCallback, useMemo, useRef } from 'react';
import { ChevronDown, CalendarDays, X } from 'lucide-react';
import PageHeader from '../components/PageHeader';
import InstrumentIcon from '../components/InstrumentIcon';
import Dropdown, { type DropdownOption } from '../components/Dropdown';
import { usePrefetchLogos } from '../hooks/usePrefetchLogos';
import { METHODOLOGY } from '../data/methodology';
import { getSeasonality, getSeasonalityPrice, getSeasonalityYearly, getSeasonalityYears } from '../services/api';
import InstrumentSearchModal from '../components/InstrumentSearchModal';
import SeasonalityHistogram from '../components/seasonality/SeasonalityHistogram';
import SeasonalityPriceChart from '../components/seasonality/SeasonalityPriceChart';
import YearlySeasonalityChart from '../components/seasonality/YearlySeasonalityChart';
import TestDashboard from '../components/seasonality/TestDashboard';
import ChartCaptureButton from '../components/export/ChartCaptureButton';
import type { SeasonalityResponse, SeasonalityMode, PriceChartResponse, YearlySeasonalityResponse } from '../services/api';
import { useFirstVisit } from '../hooks/useFirstVisit';
import OnboardingTour from '../components/onboarding/OnboardingTour';
import { seasonalityTourSteps } from '../data/tours/seasonality';
import { FUND_PALETTE } from '../config/chartTheme';
import { useAnalytics } from '../contexts/AnalyticsContext';

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

type ChartType = 'histogram' | 'price' | 'yearly' | 'test';

// Для Test-режима: все 4 гистограммы одновременно (Seasonax-style dashboard)
const TEST_MODES: SeasonalityMode[] = ['intraday', 'weekday', 'monthday', 'monthly'];

// Всегда запрашиваем полную историю — логика "Актуальный/Базисный" (30/90 итераций)
// убрана по просьбе пользователя. Бэкенд принимает iterations до 9999.
const FULL_HISTORY_ITERS = 9999;

// Цвета для спец-серий — theme-aware через CSS vars.
// "Без выбросов" — forest green (secondary line), "Exact year" — accent (pumpkin).
// FUND_PALETTE используется для серий "Период с" (множественные годы).
const COLOR_NO_OUTLIERS = 'var(--funds-flow-positive)';
const COLOR_EXACT_YEAR = 'var(--accent)';


export default function SeasonalityPage() {
  // Фоновая предзагрузка лого один раз — модалка выбора актива потом
  // открывается мгновенно из SW cache, без 100 запросов.
  usePrefetchLogos();

  // Stock selector
  const [selectedStock, setSelectedStock] = useState<string>('SBER');
  const [selectedName, setSelectedName] = useState<string>('Сбербанк');
  const [isModalOpen, setIsModalOpen] = useState(false);

  // Mode & params
  const [mode, setMode] = useState<SeasonalityMode>('weekday');

  // Onboarding tour
  const { isFirstVisit: isFirstVisitSeasonality, markAsSeen: markSeasonalityAsSeen } = useFirstVisit('seasonality');
  const [tourOpen, setTourOpen] = useState(false);
  useEffect(() => {
    if (isFirstVisitSeasonality) {
      const t = setTimeout(() => setTourOpen(true), 800);
      return () => clearTimeout(t);
    }
  }, [isFirstVisitSeasonality]);
  const [chartType, setChartType] = useState<ChartType>('histogram');
  const [excludeDividends, setExcludeDividends] = useState(false);
  const [priceDays, setPriceDays] = useState(365);

  // Analytics
  const { track } = useAnalytics();
  const handleModeChange = useCallback((m: SeasonalityMode) => {
    setMode(m);
    track('seasonality_mode', { mode: m, secid: selectedStock });
  }, [track, selectedStock]);

  // Серии:
  // - compareYears: массив годов для серий "Период с YYYY". По умолчанию [min_year] — это и есть
  //   аналог старой "средней за все годы", только теперь одна из равноправных серий.
  //   Пользователь может добавить ещё годов через "+" или убрать через ×.
  // - showNoOutliers: отдельная серия "Без выбросов" — теперь медиана вместо
  //   среднего арифметического. Median устойчива к выбросам автоматически
  //   (не нужно hardcode'ить кризисные годы).
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
  // Ref на chart card для capture (передаётся ChartCaptureButton)
  const chartCardRef = useRef<HTMLDivElement>(null);
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

  // Test-режим: yearly сверху + 4 гистограммы 2×2. Общие фильтры,
  // каждая гистограмма имеет свой tooltip (чтобы hover в одной не мигал другие).
  const [testHistData, setTestHistData] = useState<Record<SeasonalityMode, SeasonalityResponse | null>>({
    intraday: null, weekday: null, monthday: null, monthly: null,
  });
  const [testHistSeries, setTestHistSeries] = useState<Record<SeasonalityMode, SeasonalityResponse[] | null>>({
    intraday: null, weekday: null, monthday: null, monthly: null,
  });
  const [testHistTooltips, setTestHistTooltips] = useState<Record<SeasonalityMode, { x: number; y: number; bar?: SeasonalityResponse['bars'][0] } | null>>({
    intraday: null, weekday: null, monthday: null, monthly: null,
  });
  const testReqIdRef = useRef(0);
  // Per-chart fetch IDs — для независимой анимации при прогрессивной загрузке.
  // Yearly приходит первым, histograms — по очереди. Бампается только у того
  // чарта, данные которого только что обновились. Остальные чарты не ре-mount'ятся.
  const [testChartFetchIds, setTestChartFetchIds] = useState({
    yearly: 0, intraday: 0, weekday: 0, monthday: 0, monthly: 0,
  });

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
      // "Без выбросов" — теперь = медиана вместо среднего арифметического.
      // Median устойчива к выбросам автоматически — не нужен hardcoded список годов.
      if (showNoOutliers) {
        promises.push(getSeasonality(
          selectedStock, mode, FULL_HISTORY_ITERS, excludeDividends,
          { aggType: 'median' },
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
          { aggType: 'median' }));
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

  // Fetch для Test-режима — ПРОГРЕССИВНЫЙ:
  //   1) Yearly (пришёл первым → пользователь видит топ-чарт ~300ms)
  //   2) intraday → weekday → monthday → monthly (по очереди, по мере готовности)
  // Это снижает peak-concurrency на API с 15 до ~3 одновременных запросов
  // (серии одного mode'а) — thread-pool API не забивается. Плюс UX: каждый
  // чарт появляется independently, не ждём пока все загрузятся.
  const fetchTest = useCallback(async () => {
    const reqId = ++testReqIdRef.current;
    setLoading(true);
    setError(null);
    try {
      // Helper: собрать все promises для одного "набора" (yearly или один mode).
      const buildModePromises = (mode: SeasonalityMode) => {
        const arr: ReturnType<typeof getSeasonality>[] = [];
        compareYears.forEach(yr => {
          arr.push(getSeasonality(selectedStock, mode, FULL_HISTORY_ITERS, excludeDividends, { sinceYear: yr }));
        });
        if (showNoOutliers) {
          arr.push(getSeasonality(selectedStock, mode, FULL_HISTORY_ITERS, excludeDividends,
            { aggType: 'median' }));
        }
        if (showExactYear !== null) {
          const currentYear = new Date().getFullYear();
          const allYearsExceptExact = availableYears.filter(y => y !== showExactYear && y < currentYear);
          arr.push(getSeasonality(selectedStock, mode, FULL_HISTORY_ITERS, excludeDividends,
            { sinceYear: showExactYear, excludeYears: allYearsExceptExact }));
        }
        return arr;
      };

      const buildYearlyPromises = () => {
        const arr: ReturnType<typeof getSeasonalityYearly>[] = [];
        compareYears.forEach(yr => {
          arr.push(getSeasonalityYearly(selectedStock, excludeDividends, { sinceYear: yr }));
        });
        if (showNoOutliers) {
          arr.push(getSeasonalityYearly(selectedStock, excludeDividends,
            { aggType: 'median' }));
        }
        if (showExactYear !== null) {
          const currentYear = new Date().getFullYear();
          const allYearsExceptExact = availableYears.filter(y => y !== showExactYear && y < currentYear);
          arr.push(getSeasonalityYearly(selectedStock, excludeDividends,
            { sinceYear: showExactYear, excludeYears: allYearsExceptExact }));
        }
        return arr;
      };

      // Пустое состояние — ни одного "Период с"
      if (compareYears.length === 0) {
        setYearlyData(null);
        setYearlySeries(null);
        setTestHistData({ intraday: null, weekday: null, monthday: null, monthly: null });
        setTestHistSeries({ intraday: null, weekday: null, monthday: null, monthly: null });
        return;
      }

      // 1) YEARLY — первым (top of page, наибольший визуальный impact)
      const yearlyPromises = buildYearlyPromises();
      const yRes = await Promise.all(yearlyPromises);
      if (reqId !== testReqIdRef.current) return;
      setYearlyData(yRes[0]);
      setYearlySeries(yRes.length > 1 ? yRes : null);
      setTestChartFetchIds(prev => ({ ...prev, yearly: prev.yearly + 1 }));

      // 2) HISTOGRAMS — по очереди, каждый mode сам по себе (3 req макс за раз)
      for (const m of TEST_MODES) {
        const modePromises = buildModePromises(m);
        const res = await Promise.all(modePromises);
        if (reqId !== testReqIdRef.current) return;
        setTestHistData(prev => ({ ...prev, [m]: res[0] ?? null }));
        setTestHistSeries(prev => ({ ...prev, [m]: res.length > 1 ? res : null }));
        setTestChartFetchIds(prev => ({ ...prev, [m]: prev[m] + 1 }));
      }
    } catch (e: unknown) {
      if (reqId !== testReqIdRef.current) return;
      setError(e instanceof Error ? e.message : 'Ошибка загрузки');
    } finally {
      if (reqId === testReqIdRef.current) setLoading(false);
    }
  }, [selectedStock, excludeDividends, compareYears, showNoOutliers, showExactYear, availableYears]);

  useEffect(() => {
    if (!selectedStock) return;
    if (chartType === 'histogram') { fetchSeasonality(); return; }
    if (chartType === 'price') { fetchPrice(); return; }
    if (chartType === 'yearly') { fetchYearly(); return; }
    // Test-режим: дебаунс 350ms. Test стреляет 5-15 параллельными запросами
    // (yearly + 4 histogram modes × серии). Без дебаунса быстрые клики по
    // фильтрам (compareYears, noOutliers, excludeDividends) кладут thread
    // pool на API — наблюдали 60-сек slow requests и unhealthy state.
    if (chartType === 'test') {
      const t = setTimeout(() => fetchTest(), 350);
      return () => clearTimeout(t);
    }
  }, [chartType, fetchSeasonality, fetchPrice, fetchYearly, fetchTest, selectedStock]);

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
  const showDivToggle = (chartType === 'histogram' || chartType === 'yearly' || chartType === 'test') && hasDividends;

  // Годы, которые ещё можно добавить как compareYear (не в списке + не текущий)
  const currentYearNum = new Date().getFullYear();
  const addableYears = availableYears.filter(
    y => !compareYears.includes(y) && y < currentYearNum
  );

  // Блок фильтров (Без дивгэпов / Без выбросов / Период с / Показать год) —
  // вынесен в callback, чтобы можно было рендерить и на главной row2, и внутри
  // test-модалки. Дублирования 0, вся логика тут.
  const renderFilters = (): React.ReactNode => (
    <>
      {showDivToggle && (
        <button
          onClick={() => setExcludeDividends(!excludeDividends)}
          title="Пересчитать цены с учётом реинвестирования дивидендов + исключение ex-div дней в intraday"
          className="editorial-press flex items-center font-semibold rounded-full whitespace-nowrap"
          style={{
            backgroundColor: excludeDividends ? 'var(--accent)' : 'var(--bg-secondary)',
            color: excludeDividends ? 'var(--text-inverse)' : 'var(--text-primary)',
            border: '1.5px solid var(--text-primary)',
            boxShadow: excludeDividends ? 'var(--shadow-hard-chip)' : undefined,
            fontSize: 'var(--fs-sm)',
            padding: 'var(--sp-2) var(--sp-3)',
            gap: 'var(--sp-2)',
          }}
        >
          <span className="inline-block rounded-full" style={{ width: 'var(--ico-xs)', height: 'var(--ico-xs)', backgroundColor: excludeDividends ? 'var(--funds-flow-positive)' : 'var(--text-muted)' }} />
          Без дивидендных гэпов
        </button>
      )}
      <button
        onClick={() => setShowNoOutliers(!showNoOutliers)}
        title="Медиана вместо среднего — устойчива к выбросам, кризисные годы не сдвигают значение"
        className="editorial-press flex items-center font-semibold rounded-full whitespace-nowrap"
        style={{
          backgroundColor: showNoOutliers ? 'var(--accent)' : 'var(--bg-secondary)',
          color: showNoOutliers ? 'var(--text-inverse)' : 'var(--text-primary)',
          border: '1.5px solid var(--text-primary)',
          boxShadow: showNoOutliers ? 'var(--shadow-hard-chip)' : undefined,
          fontSize: 'var(--fs-sm)',
          padding: 'var(--sp-2) var(--sp-3)',
          gap: 'var(--sp-2)',
        }}
      >
        <span className="inline-block rounded-full" style={{ width: 'var(--ico-xs)', height: 'var(--ico-xs)', backgroundColor: showNoOutliers ? COLOR_NO_OUTLIERS : 'var(--text-muted)' }} />
        Без выбросов
      </button>
      {renderCompareYearsControls()}
      {availableYears.length > 1 && (
        <Dropdown<string>
          options={[
            { key: '', label: 'Показать год' },
            ...availableYears.filter(y => y < currentYearNum).map((y): DropdownOption<string> => ({
              key: String(y),
              label: String(y),
              color: COLOR_EXACT_YEAR,
            })),
          ]}
          value={showExactYear !== null ? String(showExactYear) : ''}
          onChange={(k) => setShowExactYear(k ? Number(k) : null)}
        />
      )}
    </>
  );

  // Рендерер блока "Период с N / + / ×" — используется в histogram и yearly row2
  const renderCompareYearsControls = () => {
    if (availableYears.length === 0) return null;
    return (
      <>
        {compareYears.map((yr, idx) => {
          const color = FUND_PALETTE[idx % FUND_PALETTE.length];
          const isOnly = compareYears.length === 1;
          // Editorial chip: paper bg + theme-primary outline + theme-primary text.
          // Цветная точка слева indicates series — единственный остаток FUND_PALETTE color
          // (нужен для синхронизации с линией на графике).
          return (
            <div
              key={yr}
              className="editorial-press flex items-center font-semibold rounded-full whitespace-nowrap"
              style={{
                background: 'var(--bg-secondary)',
                color: 'var(--text-primary)',
                border: '1.5px solid var(--text-primary)',
                fontSize: 'var(--fs-sm)',
                padding: 'var(--sp-2) var(--sp-3)',
                gap: 'var(--sp-2)',
              }}
              title={isOnly
                ? `Период с ${yr} г. — единственный активный период, его нельзя отключить. Сначала добавьте ещё один период через «+».`
                : `Серия "Период с ${yr} г." — средние значения по годам от ${yr} до сегодня`}
            >
              <span className="inline-block rounded-full" style={{ width: 'var(--ico-xs)', height: 'var(--ico-xs)', backgroundColor: color }} />
              Период с {yr}
              {!isOnly && (
                <button
                  onClick={() => setCompareYears(compareYears.filter(y => y !== yr))}
                  className="opacity-60 hover:opacity-100 transition-opacity"
                  title="Убрать"
                  aria-label={`Убрать серию с ${yr} г.`}
                >
                  <X size={14} />
                </button>
              )}
            </div>
          );
        })}
        {addableYears.length > 0 && (
          <Dropdown<string>
            options={[
              { key: '', label: '+ Период с' },
              ...addableYears.map((y): DropdownOption<string> => ({
                key: String(y),
                label: `С ${y} г.`,
              })),
            ]}
            value=""
            onChange={(k) => {
              const yr = Number(k);
              if (yr && !compareYears.includes(yr)) {
                setCompareYears([...compareYears, yr].sort((a, b) => a - b));
              }
            }}
          />
        )}
      </>
    );
  };

  // Стандартная editorial-ширина 1408px (как Strength/Buffett/etc).
  // В test-режиме расширяем до 1800px чтобы вместить 2×2 grid гистограмм.
  const containerClass = chartType === 'test'
    ? 'max-w-[1800px] mx-auto px-4 md:px-6 py-6 md:py-8'
    : 'max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8';

  return (
    <div className={containerClass}>
      <PageHeader
        icon={CalendarDays}
        title="Сезонность"
        subtitle="Среднее изменение цены по временным периодам"
        help={METHODOLOGY.seasonality}
        helpLink="/methodology/seasonality"
      />

      {/* Editorial frame — обнимает controls + chart в один контейнер */}
      <div className="editorial-frame">

      {/* Controls Row 1 — селектор актива + тип графика. Камеру в этой
          строке НЕ держим: на мобиле при wrap'е она попадала в первую строку
          сама и ломала layout (виден на скрине от 13.05). Камеру переехала
          в Row 2 — там она всегда «после фильтров, справа» через ml-auto. */}
      <div className="flex flex-wrap items-center mb-4" style={{ gap: 'var(--sp-2)' }}>
        {/* Stock selector — остаётся widget-flat (icon + multiline label) */}
        <div data-tour="seasonality-instrument" className="relative">
          <button
            onClick={() => setIsModalOpen(true)}
            className="widget-flat font-medium transition-colors flex items-center hover:opacity-90"
            style={{
              color: 'var(--text-primary)',
              fontSize: 'var(--fs-sm)',
              padding: 'var(--sp-2) var(--sp-4)',
              gap: 'var(--sp-3)',
              minWidth: 'clamp(160px, 30vw, 200px)',
            }}
          >
            <InstrumentIcon sectype={selectedStock} size={28} rounded="full" eager />
            <div className="flex-1 text-left">
              <div className="font-medium">{selectedName}</div>
              <div className="text-theme-secondary" style={{ fontSize: 'var(--fs-2xs)' }}>{selectedStock}</div>
            </div>
            <ChevronDown size={14} className="text-theme-secondary" />
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
        <div data-tour="seasonality-mode" className="flex" style={{ gap: 'var(--sp-2)' }}>
        <Dropdown<ChartType>
          options={[
            { key: 'histogram', label: 'Сезонность' },
            { key: 'yearly',    label: 'Годовая' },
          ]}
          value={chartType === 'price' || chartType === 'test' ? 'histogram' : chartType}
          onChange={setChartType}
        />

        {/* Histogram-specific: mode */}
        {chartType === 'histogram' && (
          <Dropdown<SeasonalityMode>
            options={(Object.keys(MODE_LABELS) as SeasonalityMode[]).map((m): DropdownOption<SeasonalityMode> => ({
              key: m,
              label: MODE_LABELS[m],
            }))}
            value={mode}
            onChange={handleModeChange}
          />
        )}
        </div>

        {/* Price-specific period */}
        {chartType === 'price' && (
          <Dropdown<string>
            options={PRICE_PERIODS.map((p): DropdownOption<string> => ({
              key: String(p.days),
              label: p.label,
            }))}
            value={String(priceDays)}
            onChange={(k) => setPriceDays(Number(k))}
          />
        )}

      </div>

      {/* Controls Row 2 — общие для histogram и yearly:
          "Без дивгэпов", "Без выбросов", compareYears pills + "+", "Показать год" (yearly only).
          Теперь работают и в intraday (для intraday див-гэпы = исключение ex-div дней целиком).
          Для price — отдельная строка с информацией.
          В конце через ml-auto — кнопка экспорта графика (📷). Раньше она
          была в Row 1 и ломала layout при wrap'е на мобиле. */}
      <div className="flex flex-wrap items-center gap-2 md:gap-4 mb-4">
        {chartType === 'price' && priceData && (
          <div className="text-xs" style={{ color: 'var(--text-muted)' }}>
            {priceData.data.length} торговых дней • {priceData.ex_dates_count} дивидендных отсечек
          </div>
        )}

        {(chartType === 'histogram' || chartType === 'yearly' || chartType === 'test') && renderFilters()}

        {/* Camera button — справа после всех фильтров, скрыт в test mode. */}
        {chartType !== 'test' && (
          <ChartCaptureButton
            getTargetElement={() => chartCardRef.current}
            filename={`frame-seasonality-${selectedStock.toLowerCase()}-${chartType}-${mode}`}
            metadata={{
              title: 'Сезонность',
              asset: selectedName,
              ticker: selectedStock,
              details: [
                chartType === 'histogram' ? MODE_LABELS[mode] :
                chartType === 'price' ? `${priceDays === 9999 ? 'Всё' : priceDays + ' дн'}` :
                chartType === 'yearly' ? 'Годовая' : '',
                excludeDividends ? 'Без дивгэпов' : null,
                showNoOutliers ? 'Без выбросов' : null,
              ].filter(Boolean) as string[],
            }}
            className="ml-auto"
          />
        )}
      </div>

      {/* TEST MODE — Seasonax-style dashboard (yearly + 2×2 histograms) */}
      {chartType === 'test' && (
        <TestDashboard
          compareYears={compareYears}
          selectedStock={selectedStock}
          selectedName={selectedName}
          onSelectInstrument={handleSelectInstrument}
          renderFilters={renderFilters}
          yearlyData={yearlyData}
          yearlySeries={yearlySeries}
          seriesMeta={seriesMeta}
          testHistData={testHistData}
          testHistSeries={testHistSeries}
          testHistTooltips={testHistTooltips}
          setTestHistTooltips={setTestHistTooltips}
          chartTooltip={tooltip}
          setChartTooltip={setTooltip}
          testChartFetchIds={testChartFetchIds}
          loading={loading}
          error={error}
        />
      )}

      {/* Existing Chart block — hidden in test mode.
          Paper-card как в OI/Funds-Money: rounded-2xl + p-5 + bg-theme-primary
          + 2px inkstroke. Editorial-стандарт: chart cards = 2px (тяжёлая рамка),
          chips/buttons = 1.5px. Иерархия по толщине обводки. */}
      {chartType !== 'test' && (
      <div ref={chartCardRef} data-tour="seasonality-chart" className="relative rounded-2xl bg-theme-primary p-2 md:p-5" style={{ border: '2px solid var(--text-primary)' }}>
        {/* Спиннер обновления — paper-style без glass */}
        {loading && (bars.length > 0 || priceData || yearlyData) && (
          <div
            className="absolute top-4 right-4 z-20 flex items-center rounded-lg border border-theme shadow-md"
            style={{
              background: 'var(--bg-primary)',
              padding: 'var(--sp-2) var(--sp-3)',
              gap: 'var(--sp-2)',
              fontSize: 'var(--fs-xs)',
            }}
          >
            <div className="w-4 h-4 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
            <span className="text-theme-secondary">Обновление...</span>
          </div>
        )}
        {loading && bars.length === 0 && !priceData && !yearlyData ? (
          <div className="flex items-center justify-center" style={{ aspectRatio: '16/9' }}>
            <div className="flex flex-col items-center gap-3">
              <div className="w-8 h-8 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
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
              assetLabel={selectedStock}
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
      )}

      </div>{/* /editorial-frame */}

      {/* Description */}
      <div className="mt-4 text-sm" style={{ color: 'var(--text-muted)' }}>
        {chartType === 'histogram'
          ? `Среднее изменение (${mode === 'intraday' ? 'open-to-close per hour' : 'close-to-close'}) ${MODE_LABELS[mode].toLowerCase()}`
          : chartType === 'price'
          ? `График цены ${selectedStock} — с дивидендными гэпами и без (adjusted close)`
          : chartType === 'test'
          ? `Экспериментальный режим: годовая траектория + 4 среза сезонности одновременно`
          : `Кумулятивное изменение ${selectedStock} с начала года • ${yearlyData?.current_year ?? ''}`
        }
        {(chartType === 'histogram' || chartType === 'yearly' || chartType === 'test') && excludeDividends && (
          <span className="ml-2 text-green-500">
            • {mode === 'intraday' && chartType === 'histogram' ? 'Экс-дивидендные дни исключены' : 'Дивидендные гэпы убраны'}
          </span>
        )}
        {chartType === 'histogram' && mode === 'monthday' && <span className="ml-2">• Выходные привязаны к понедельнику</span>}
      </div>

      <OnboardingTour
        steps={seasonalityTourSteps}
        open={tourOpen}
        onClose={(markSeen) => {
          setTourOpen(false);
          if (markSeen) markSeasonalityAsSeen();
        }}
      />
    </div>
  );
}
