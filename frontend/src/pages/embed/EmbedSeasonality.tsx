/**
 * EmbedSeasonality — виджет сезонности (per-ticker), паритет с полной страницей в
 * рамках компактного формата. Контролы живут в drawer'е настроек:
 *   - Тип графика: «Сезонность» (гистограмма по срезу) / «Годовая» (кумулятивная
 *     траектория года);
 *   - Разрез (только для гистограммы): Внутри дня / По дням недели / Внутри месяца /
 *     По месяцам;
 *   - Без дивидендных гэпов (скрыт для индексов/валют/сырья);
 *   - Без выбросов (добавляет вторую серию — медиану);
 *   - Текущий год (только для годовой) — линия текущего года.
 *
 * Виджет целиком под PRO-токеном → тир-гейтинга/онбординга/экспорта нет.
 * Множественные «Период с {год}» и «Показать год» намеренно НЕ перенесены —
 * для компактного виджета берём всю историю (база) + опциональную медиану.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import SeasonalityHistogram from '../../components/seasonality/SeasonalityHistogram';
import YearlySeasonalityChart from '../../components/seasonality/YearlySeasonalityChart';
import {
  getSeasonality,
  getSeasonalityYearly,
  getInstrument,
  type SeasonalityResponse,
  type SeasonalityMode,
  type YearlySeasonalityResponse,
} from '../../services/api';
import { displayTicker } from '../../utils/displayTicker';
import { EmbedMsg } from './embedUi';
import {
  useEmbedSettings,
  EmbedShell,
  DrawerSection,
  SegGroup,
  ToggleRow,
  AssetPickerInline,
} from './EmbedSettings';

type ChartType = 'histogram' | 'yearly';
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';

// Тултипы у гистограммы и годовой имеют разную форму — держим раздельно.
type HistTip = { x: number; y: number; bar?: SeasonalityResponse['bars'][0] } | null;
type YearTip = {
  x: number;
  y: number;
  yearlyAvgPct?: number;
  yearlyCurPct?: number;
  yearlyCurDate?: string;
  yearlyTd?: number;
  yearlySigma?: number;
} | null;

interface SeriesMeta {
  key: string;
  label: string;
  color: string;
}

const CHART_TYPES: { id: ChartType; label: string }[] = [
  { id: 'histogram', label: 'Сезонность' },
  { id: 'yearly', label: 'Годовая' },
];

// Точные строки MODE_LABELS со страницы Сезонности (SeasonalityPage.tsx).
const MODES: { id: SeasonalityMode; label: string }[] = [
  { id: 'intraday', label: 'Внутри дня' },
  { id: 'weekday', label: 'По дням недели' },
  { id: 'monthday', label: 'Внутри месяца' },
  { id: 'monthly', label: 'По месяцам' },
];

// Инструменты без дивидендов: индексы, валюты, сырьё, вечные фьючерсы.
// Тоггл «Без дивидендных гэпов» для них бесполезен → прячем (копия со страницы).
const NON_DIVIDEND_TICKERS = new Set([
  'IMOEX', 'RTSI', 'RGBI', 'RVI', 'MCFTR', 'RGBITR', 'RUSFAR3M',
  'GLDRUB_TOM', 'USD000UTSTOM', 'EUR_RUB__TOM', 'CNYRUB_TOM',
  'USDRUBF', 'EURRUBF', 'CNYRUBF', 'IMOEXF',
]);

// Полная история — как FULL_HISTORY_ITERS на странице.
const FULL_HISTORY_ITERS = 9999;

// Meta двух серий «Среднее» + «Без выбросов» (медиана). Цвета зафиксированы
// заданием: база = accent, медиана = positive (forest green).
const TWO_SERIES_META: SeriesMeta[] = [
  { key: 'base', label: 'Среднее', color: 'var(--accent)' },
  { key: 'no_outliers', label: 'Без выбросов', color: 'var(--funds-flow-positive)' },
];

function readLS(key: string, fallback: string): string {
  try { return localStorage.getItem(key) || fallback; } catch { return fallback; }
}
function readLSBool(key: string, fallback: boolean): boolean {
  try {
    const v = localStorage.getItem(key);
    return v === null ? fallback : v === 'true';
  } catch { return fallback; }
}

export default function EmbedSeasonality() {
  const [params] = useSearchParams();
  const settings = useEmbedSettings();

  const [stock, setStock] = useState<string>(() => params.get('instrument') || readLS('frame:embed:seasonality:stock', 'SBER'));
  const [stockName, setStockName] = useState<string>(params.get('name') || '');
  const [chartType, setChartType] = useState<ChartType>(() => readLS('frame:embed:seasonality:chartType', 'histogram') as ChartType);
  const [mode, setMode] = useState<SeasonalityMode>(() => readLS('frame:embed:seasonality:mode', 'weekday') as SeasonalityMode);
  const [excludeDividends, setExcludeDividends] = useState<boolean>(() => readLSBool('frame:embed:seasonality:excludeDividends', false));
  const [showNoOutliers, setShowNoOutliers] = useState<boolean>(() => readLSBool('frame:embed:seasonality:showNoOutliers', false));
  const [showCurrentYear, setShowCurrentYear] = useState<boolean>(() => readLSBool('frame:embed:seasonality:showCurrentYear', true));

  // Данные: база + опциональная медиана, отдельно для histogram и yearly.
  const [histBase, setHistBase] = useState<SeasonalityResponse | null>(null);
  const [histMedian, setHistMedian] = useState<SeasonalityResponse | null>(null);
  const [yearBase, setYearBase] = useState<YearlySeasonalityResponse | null>(null);
  const [yearMedian, setYearMedian] = useState<YearlySeasonalityResponse | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');

  const [histTip, setHistTip] = useState<HistTip>(null);
  const [yearTip, setYearTip] = useState<YearTip>(null);

  // Стале-гард для отбрасывания устаревших ответов при быстром переключении.
  const reqIdRef = useRef(0);

  useEffect(() => { try { localStorage.setItem('frame:embed:seasonality:stock', stock); } catch { /* quota */ } }, [stock]);
  useEffect(() => { try { localStorage.setItem('frame:embed:seasonality:chartType', chartType); } catch { /* quota */ } }, [chartType]);
  useEffect(() => { try { localStorage.setItem('frame:embed:seasonality:mode', mode); } catch { /* quota */ } }, [mode]);
  useEffect(() => { try { localStorage.setItem('frame:embed:seasonality:excludeDividends', String(excludeDividends)); } catch { /* quota */ } }, [excludeDividends]);
  useEffect(() => { try { localStorage.setItem('frame:embed:seasonality:showNoOutliers', String(showNoOutliers)); } catch { /* quota */ } }, [showNoOutliers]);
  useEffect(() => { try { localStorage.setItem('frame:embed:seasonality:showCurrentYear', String(showCurrentYear)); } catch { /* quota */ } }, [showCurrentYear]);

  useEffect(() => {
    if (stockName) return;
    let cancelled = false;
    getInstrument(stock)
      .then((i) => { if (!cancelled && i?.name) setStockName(i.name); })
      .catch(() => { /* имя не критично */ });
    return () => { cancelled = true; };
  }, [stock, stockName]);

  // Инструменты без дивидендов: тоггл прячем И всегда шлём excludeDividends=false.
  const hasDividends = !NON_DIVIDEND_TICKERS.has(stock);
  const effExcludeDividends = hasDividends && excludeDividends;

  // Фетч: база + (опционально) медиана параллельно. По chartType — гистограмма
  // или годовая. Стале-ответы отбрасываем по reqId.
  useEffect(() => {
    if (!stock) { setStatus('empty'); return; }
    const reqId = ++reqIdRef.current;
    let cancelled = false;
    setStatus('loading');

    if (chartType === 'histogram') {
      const basePromise = getSeasonality(stock, mode, FULL_HISTORY_ITERS, effExcludeDividends);
      const medianPromise = showNoOutliers
        ? getSeasonality(stock, mode, FULL_HISTORY_ITERS, effExcludeDividends, { aggType: 'median' })
        : Promise.resolve<SeasonalityResponse | null>(null);
      Promise.all([basePromise, medianPromise])
        .then(([base, median]) => {
          if (cancelled || reqId !== reqIdRef.current) return;
          setHistBase(base);
          setHistMedian(median);
          setStatus((base?.bars?.length ?? 0) > 0 ? 'ok' : 'empty');
        })
        .catch((err) => {
          if (cancelled || reqId !== reqIdRef.current) return;
          console.error('embed/seasonality histogram load failed:', err);
          setStatus('error');
        });
    } else {
      const basePromise = getSeasonalityYearly(stock, effExcludeDividends);
      const medianPromise = showNoOutliers
        ? getSeasonalityYearly(stock, effExcludeDividends, { aggType: 'median' })
        : Promise.resolve<YearlySeasonalityResponse | null>(null);
      Promise.all([basePromise, medianPromise])
        .then(([base, median]) => {
          if (cancelled || reqId !== reqIdRef.current) return;
          setYearBase(base);
          setYearMedian(median);
          setStatus((base?.average?.length ?? 0) > 0 ? 'ok' : 'empty');
        })
        .catch((err) => {
          if (cancelled || reqId !== reqIdRef.current) return;
          console.error('embed/seasonality yearly load failed:', err);
          setStatus('error');
        });
    }
    return () => { cancelled = true; };
  }, [stock, chartType, mode, effExcludeDividends, showNoOutliers]);

  // Две серии есть только когда медиана включена И реально загрузилась.
  const histTwoSeries: SeasonalityResponse[] | null = useMemo(
    () => (showNoOutliers && histBase && histMedian ? [histBase, histMedian] : null),
    [showNoOutliers, histBase, histMedian],
  );
  const yearTwoSeries: YearlySeasonalityResponse[] | null = useMemo(
    () => (showNoOutliers && yearBase && yearMedian ? [yearBase, yearMedian] : null),
    [showNoOutliers, yearBase, yearMedian],
  );

  const bars = histBase?.bars ?? [];
  const maxAbs = useMemo(
    () => Math.max(...bars.map((b) => Math.abs(b.avg_change)), 0.01),
    [bars],
  );

  // Измеряем контейнер — YearlySeasonalityChart требует числовой chartHeight.
  const boxRef = useRef<HTMLDivElement>(null);
  const [boxH, setBoxH] = useState(360);
  useEffect(() => {
    const el = boxRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const h = entries[0]?.contentRect.height;
      if (h && h > 0) setBoxH(Math.round(h));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const displayName = stockName || displayTicker(stock);

  const isHist = chartType === 'histogram';

  return (
    <EmbedShell
      settings={settings}
      title={displayName}
      subtitle="Сезонность"
      drawer={
        <>
          <DrawerSection label="Актив">
            <AssetPickerInline
              filterType="stock"
              current={stock}
              active={settings.open}
              onSelect={(secid, name) => { setStock(secid); setStockName(name); }}
            />
          </DrawerSection>
          <DrawerSection label="Тип графика">
            <SegGroup<ChartType> value={chartType} options={CHART_TYPES} onChange={setChartType} />
          </DrawerSection>
          {isHist && (
            <DrawerSection label="Разрез">
              <SegGroup<SeasonalityMode> value={mode} options={MODES} onChange={setMode} />
            </DrawerSection>
          )}
          {hasDividends && (
            <DrawerSection label="Дивиденды">
              <ToggleRow
                label="Без дивидендных гэпов"
                checked={excludeDividends}
                onChange={setExcludeDividends}
              />
            </DrawerSection>
          )}
          <DrawerSection label="Серии">
            <ToggleRow
              label="Без выбросов"
              checked={showNoOutliers}
              onChange={setShowNoOutliers}
            />
          </DrawerSection>
          {!isHist && (
            <DrawerSection label="Текущий год">
              <ToggleRow
                label="Текущий год"
                checked={showCurrentYear}
                onChange={setShowCurrentYear}
              />
            </DrawerSection>
          )}
        </>
      }
    >
      <div ref={boxRef} style={{ flex: 1, position: 'relative', minHeight: 0, overflow: 'hidden' }}>
        {status === 'ok' && isHist && bars.length > 0 && (
          <SeasonalityHistogram
            bars={bars}
            maxAbs={maxAbs}
            tooltip={histTip}
            setTooltip={setHistTip}
            monthlySeries={histTwoSeries}
            seriesMeta={histTwoSeries ? TWO_SERIES_META : undefined}
            niceXLabels={mode === 'monthday'}
          />
        )}
        {status === 'ok' && !isHist && yearBase && (
          <YearlySeasonalityChart
            yearlyData={yearBase}
            seriesData={yearTwoSeries}
            seriesMeta={yearTwoSeries ? TWO_SERIES_META : undefined}
            tooltip={yearTip}
            setTooltip={setYearTip}
            chartHeight={boxH}
            showCurrentYear={showCurrentYear}
          />
        )}
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && <EmbedMsg text={stock ? 'Нет данных' : 'Акция не выбрана'} />}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
      </div>
    </EmbedShell>
  );
}
