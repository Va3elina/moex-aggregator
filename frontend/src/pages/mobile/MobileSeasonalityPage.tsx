/**
 * MobileSeasonalityPage — мобильная версия «Сезонность».
 *
 * Phase 3 простая версия:
 *   - Asset chip (Сбербанк / другой тикер через MobileAssetSearch)
 *   - Mode chips (Дни/Месяцы/Дни месяца)
 *   - SVG histogram сезонности (зелёные/красные столбики)
 *   - Compare years, exclude dividends — Phase 4
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { CalendarDays, Lock } from 'lucide-react';
import { useTierAccess } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../../components/tier/UpgradeModal';
import MobileLayout from '../../components/mobile/MobileLayout';
import MobilePageHeader from '../../components/mobile/MobilePageHeader';
import MobileAssetSearch from '../../components/mobile/MobileAssetSearch';
import MobileSheet from '../../components/mobile/MobileSheet';
import MobileSkeleton from '../../components/mobile/MobileSkeleton';
import MobileChart from '../../components/mobile/MobileChart';
import {
  getSeasonality,
  getSeasonalityYears,
  getSeasonalityYearly,
  type SeasonalityResponse,
  type SeasonalityMode,
  type YearlySeasonalityResponse,
} from '../../services/api';
import { useOnboardingTour } from '../../hooks/useFirstVisit';
import OnboardingTour from '../../components/onboarding/OnboardingTour';
import type { TourStep } from '../../components/onboarding/OnboardingTour';

// Mobile mode: 4 API modes + локальный 'yearly' который дёргает отдельный endpoint
// (getSeasonalityYearly) и рендерится как line chart, не bars.
type MobileMode = SeasonalityMode | 'yearly';

const MODE_LABELS: Record<MobileMode, string> = {
  weekday: 'Дни недели',
  monthly: 'Месяцы',
  monthday: 'Дни месяца',
  intraday: 'Часы',
  yearly: 'Годовая',
};

const WEEKDAY_LABELS = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт'];
const MONTH_LABELS = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек'];

// Палитра цветов для compare-series. Accent neutral hues, без конфликта
// с base accent (orange) и зелёный/красный (sentiment).
const COMPARE_COLORS = [
  'var(--chart-line-1, #5DA3E9)',
  '#F4A261',
  '#9B6BD9',
  '#4ECDC4',
  '#E76F51',
  '#A8DADC',
];

export default function MobileSeasonalityPage() {
  const [selectedStock, setSelectedStock] = useState('SBER');
  const [selectedName, setSelectedName] = useState('Сбербанк');
  const [mode, setMode] = useState<MobileMode>('monthly');
  // Smart default: для Free histogram-режимы недоступны → 'yearly'
  const defaultSwitchedRef = useRef(false);
  const seasonAccess = useTierAccess('seasonality');
  const { showUpgrade } = useUpgradePrompt();
  // Для Free yearly — единственный доступный режим
  const histogramLocked = !seasonAccess.isLoading && !seasonAccess.canUseMode('histogram');
  // Phase-4 фильтры:
  const [excludeDividends, setExcludeDividends] = useState(false);
  const [aggType, setAggType] = useState<'avg' | 'median'>('avg');

  const [data, setData] = useState<SeasonalityResponse | null>(null);
  const [yearlyData, setYearlyData] = useState<YearlySeasonalityResponse | null>(null);
  // compareYearlyData[i] — yearly response для compareYears[i] (с sinceYear=yr)
  const [compareYearlyData, setCompareYearlyData] = useState<YearlySeasonalityResponse[]>([]);
  const [availableYears, setAvailableYears] = useState<number[]>([]);
  // Сравнение: rolling window (с года X и позже) + single year (только X).
  const [compareYears, setCompareYears] = useState<number[]>([]);
  const [exactYears, setExactYears] = useState<number[]>([]);
  // Histogram extra series (для каждого compareYear + exactYear).
  const [compareHistData, setCompareHistData] = useState<SeasonalityResponse[]>([]);
  const [exactHistData, setExactHistData] = useState<SeasonalityResponse[]>([]);
  const [loading, setLoading] = useState(true);

  // localStorage persist для compareYears + exactYears, per-actor.
  // Один JSON-объект `{c: [...], e: [...]}` чтобы не плодить ключи.
  useEffect(() => {
    if (!selectedStock) return;
    try {
      const saved = localStorage.getItem(`seasonality_compare_${selectedStock}`);
      if (saved) {
        const parsed = JSON.parse(saved);
        setCompareYears(Array.isArray(parsed) ? parsed : parsed.c ?? []);
        setExactYears(Array.isArray(parsed) ? [] : parsed.e ?? []);
      } else {
        setCompareYears([]);
        setExactYears([]);
      }
    } catch {
      setCompareYears([]);
      setExactYears([]);
    }
  }, [selectedStock]);

  useEffect(() => {
    if (!selectedStock) return;
    try {
      localStorage.setItem(
        `seasonality_compare_${selectedStock}`,
        JSON.stringify({ c: compareYears, e: exactYears }),
      );
    } catch {
      // ignore quota errors
    }
  }, [selectedStock, compareYears, exactYears]);

  // Fetch availableYears на смену actor.
  useEffect(() => {
    if (!selectedStock) return;
    let cancelled = false;
    getSeasonalityYears(selectedStock)
      .then((resp) => {
        if (cancelled) return;
        setAvailableYears(resp.years);
      })
      .catch(() => {
        if (!cancelled) setAvailableYears([]);
      });
    return () => {
      cancelled = true;
    };
  }, [selectedStock]);
  const [assetSearchOpen, setAssetSearchOpen] = useState(false);
  const [modeSheetOpen, setModeSheetOpen] = useState(false);

  const tour = useOnboardingTour('seasonality');
  // Единый паттерн: Intro → Кнопки → Опции (с демо режимов и compare) → Чтение → End
  const tourSteps: TourStep[] = [
    {
      selector: null,
      title: 'Сезонность',
      body: (
        <>
          <p style={{ marginBottom: 8 }}>
            Как часто и насколько актив растёт/падает в определённые
            периоды (день недели, месяц года). Поиск повторяющихся
            паттернов в истории.
          </p>
          <p>Разберём управление и научимся читать график.</p>
        </>
      ),
      onEnter: () => { setModeSheetOpen(false); setAssetSearchOpen(false); },
    },
    {
      selector: '.fm-page-actions',
      title: 'Кнопки управления',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>Снизу — 2 кнопки:</p>
          <p style={{ marginBottom: 4 }}>
            <strong>Актив</strong> — выбор тикера: акции, валюты, индексы, сырьё
          </p>
          <p>
            <strong>Опции</strong> — режим (Гистограмма / Годовая),
            сравнение с другими годами, фильтры
          </p>
        </>
      ),
      position: 'top',
      spotlightPadding: 4,
    },
    {
      selector: null,
      title: 'Режимы, годы, фильтры',
      align: 'top',
      body: (
        <>
          <p style={{ marginBottom: 4 }}>
            Открыл кнопку <strong>«Опции»</strong>. Внутри:
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Гистограмма</strong> — средние % по выбранной шкале
            (Дни недели / Месяцы / Дни месяца). Видны традиционно сильные
            и слабые периоды.
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Годовая</strong> — накопленная динамика года, можно
            сравнить текущий год с прошлыми.
          </p>
          <p>
            <strong>Сравнение с годами:</strong> «Период с года» и «Конкретный год».
            <strong> Тогглы:</strong> Без дивидендов, Медиана.
          </p>
        </>
      ),
      onEnter: () => { setModeSheetOpen(true); setAssetSearchOpen(false); },
    },
    {
      selector: '[data-tour="seasonality-chart"]',
      title: 'Чтение графика',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>
            Сейчас переключу в <strong>Годовой</strong> режим: оранжевая линия —
            этот год, серая — медиана всех годов истории.
          </p>
          <p>
            Зажми палец — появится перекрестье с реальной датой и
            накопленным процентом.
          </p>
        </>
      ),
      position: 'top',
      spotlightPadding: 4,
      onEnter: () => { setMode('yearly'); setModeSheetOpen(false); setAssetSearchOpen(false); },
    },
    {
      selector: null,
      title: 'Готово!',
      body: (
        <p>
          Нажми <strong>?</strong> рядом с заголовком — методология и
          интерпретация сезонных эффектов.
        </p>
      ),
    },
  ];

  const loadData = useMemo(
    () => async () => {
      try {
        setLoading(true);
        // base filter без sinceYear — главная серия = вся доступная история.
        // Сужение по периоду делается через compareYears / exactYears.
        const filters = { aggType };
        const currentYear = new Date().getFullYear();
        if (mode === 'yearly') {
          const basePromise = getSeasonalityYearly(selectedStock, excludeDividends, filters);
          const comparePromises = compareYears.map((yr) =>
            getSeasonalityYearly(selectedStock, excludeDividends, { ...filters, sinceYear: yr }),
          );
          // Для exact years на yearly endpoint — используем sinceYear=yr.
          // (yearly не имеет excludeYears support). Approximation: yearly данные
          // с year X = "период с X", не "только X". Это limitation API.
          const exactYearlyPromises = exactYears.map((yr) =>
            getSeasonalityYearly(selectedStock, excludeDividends, { ...filters, sinceYear: yr }),
          );
          const all = await Promise.all([basePromise, ...comparePromises, ...exactYearlyPromises]);
          setYearlyData(all[0]);
          setCompareYearlyData(all.slice(1, 1 + compareYears.length));
        } else {
          const basePromise = getSeasonality(
            selectedStock,
            mode,
            90,
            excludeDividends,
            filters,
          );
          // Compare series: rolling sinceYear для каждого compareYear
          const comparePromises = compareYears.map((yr) =>
            getSeasonality(selectedStock, mode, 90, excludeDividends, { ...filters, sinceYear: yr }),
          );
          // Exact series: excludeYears trick — исключаем все годы кроме target
          const exactPromises = exactYears.map((yr) => {
            const allOtherYears = availableYears.filter((y) => y !== yr && y < currentYear);
            return getSeasonality(selectedStock, mode, 90, excludeDividends, {
              ...filters,
              sinceYear: yr,
              excludeYears: allOtherYears,
            });
          });
          const [baseResult, ...rest] = await Promise.all([
            basePromise,
            ...comparePromises,
            ...exactPromises,
          ]);
          setData(baseResult);
          setCompareHistData(rest.slice(0, compareYears.length));
          setExactHistData(rest.slice(compareYears.length));
        }
      } catch (err) {
        console.error('Ошибка seasonality:', err);
        const msg = err instanceof Error ? err.message : String(err);
        if (msg.includes('тарифе') || msg.includes('недоступ')) {
          const requiredTier: 'basic' | 'pro' = msg.includes('Pro') ? 'pro' : 'basic';
          showUpgrade({
            tier: requiredTier,
            featureName: mode === 'intraday' ? 'режим «Внутри дня»' :
              histogramLocked ? 'режим «Сезонность»' : `актив ${selectedStock}`,
            indicator: 'seasonality',
          });
        }
      } finally {
        setLoading(false);
      }
    },
    [selectedStock, mode, excludeDividends, aggType, compareYears, exactYears, availableYears, showUpgrade, histogramLocked],
  );

  useEffect(() => {
    void loadData();
  }, [loadData]);

  // Smart default — переключение на yearly если histogram-режимы недоступны.
  useEffect(() => {
    if (seasonAccess.isLoading || defaultSwitchedRef.current) return;
    defaultSwitchedRef.current = true;
    if (histogramLocked && mode !== 'yearly') {
      setMode('yearly');
    }
  }, [seasonAccess.isLoading, histogramLocked, mode]);

  // Helper: маппинг b.key → label по mode
  const labelFor = (key: number): string => {
    if (mode === 'weekday') return WEEKDAY_LABELS[key - 1] ?? String(key);
    if (mode === 'monthly') return MONTH_LABELS[key - 1] ?? String(key);
    return String(key);
  };

  // Преобразуем bars в массив series. Base — first, compare — rolling,
  // exact — single-year-only. Каждая series имеет {label, color, bars[]}.
  const seriesGroups = useMemo(() => {
    if (!data?.bars) return [];
    const base = {
      label: 'Вся история',
      color: 'var(--accent)',
      bars: data.bars.map((b) => ({ label: labelFor(b.key), value: b.avg_change, count: b.count })),
    };
    const compare = compareYears.map((yr, i) => {
      const resp = compareHistData[i];
      return {
        label: `с ${yr}`,
        color: COMPARE_COLORS[i % COMPARE_COLORS.length],
        bars: resp?.bars?.map((b) => ({ label: labelFor(b.key), value: b.avg_change, count: b.count })) ?? [],
      };
    });
    const exact = exactYears.map((yr, i) => {
      const resp = exactHistData[i];
      return {
        label: `${yr}`,
        color: COMPARE_COLORS[(compareYears.length + i) % COMPARE_COLORS.length],
        bars: resp?.bars?.map((b) => ({ label: labelFor(b.key), value: b.avg_change, count: b.count })) ?? [],
      };
    });
    return [base, ...compare, ...exact];
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, mode, compareYears, exactYears, compareHistData, exactHistData]);

  // Backwards-compat: bars/maxAbs для текущего render-path (single series)
  const bars = seriesGroups[0]?.bars ?? [];
  const maxAbs = useMemo(() => {
    const allValues = seriesGroups.flatMap((g) => g.bars.map((b) => Math.abs(b.value)));
    if (allValues.length === 0) return 1;
    return Math.max(...allValues, 0.1);
  }, [seriesGroups]);

  // Subtitle: показываем текущее значение фильтров под заголовком
  const filtersDesc: string[] = [MODE_LABELS[mode]];
  if (excludeDividends) filtersDesc.push('без дивидендов');
  if (aggType === 'median') filtersDesc.push('медиана');

  return (
    <MobileLayout
      onAssetClick={() => setAssetSearchOpen(true)}
      assetLabel={selectedName}
      assetTicker={selectedStock}
      assetTourId="seasonality-asset"
      onSettingsClick={() => setModeSheetOpen(true)}
      settingsSummary={filtersDesc.join(' · ')}
      settingsTourId="seasonality-mode"
      onRefresh={loadData}
      loading={loading}
    >
      <MobilePageHeader
        Icon={CalendarDays}
        title="Сезонность"
        subtitle={`${selectedName} · ${MODE_LABELS[mode]}`}
        helpLink="/methodology/seasonality"
      />

      {/* Full-bleed histogram, как Heatmap. SeasonalityBars сам растягивается
          через width/height 100% и больше не использует preserveAspectRatio
          (см. компонент ниже — pixel coords через ResizeObserver). */}
      <div
        data-tour="seasonality-chart"
        style={{ flex: 1, minHeight: 0, position: 'relative' }}
      >
        <div style={{ position: 'absolute', inset: 0 }}>
          {loading ? (
            <MobileSkeleton variant="chart" height="100%" />
          ) : mode === 'yearly' ? (
            yearlyData ? <YearlySeasonalityChart data={yearlyData} compareData={compareYearlyData} compareYears={compareYears} /> : (
              <div style={{ display: 'grid', placeItems: 'center', height: '100%', color: 'var(--text-muted)' }}>
                Нет данных
              </div>
            )
          ) : bars.length === 0 ? (
            <div style={{ display: 'grid', placeItems: 'center', height: '100%', color: 'var(--text-muted)' }}>
              Нет данных
            </div>
          ) : (
            // SeasonalityBars сам разруливает single-series (одна bar на slot,
            // зелёный/красный по знаку) vs multi-series (grouped sub-bars
            // side-by-side, цвет = series.color) — как на десктопе.
            <SeasonalityBars seriesGroups={seriesGroups} maxAbs={maxAbs} />
          )}
        </div>
      </div>

      <MobileAssetSearch
        open={assetSearchOpen}
        onClose={() => setAssetSearchOpen(false)}
        filterType="no-futures"
        onSelect={(sectype, name) => {
          setSelectedStock(sectype);
          setSelectedName(name);
        }}
        indicator="seasonality"
      />

      <MobileSheet
        open={modeSheetOpen}
        onClose={() => setModeSheetOpen(false)}
        title="Опции"
      >
        <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 20 }}>
          {/* Top-level toggle: Гистограмма (bars) vs Годовая (line chart).
              Это разные chart-типы — не объединяем в один список. */}
          <div>
            <div style={{ fontSize: 11, fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
              Тип графика
            </div>
            <div style={{ display: 'flex', gap: 6 }}>
              <button
                className={`fm-chip ${mode !== 'yearly' ? 'active' : ''}`}
                onClick={() => {
                  if (histogramLocked) {
                    const tier = seasonAccess.requiredTierFor({ mode: 'histogram' });
                    if (tier) {
                      showUpgrade({
                        tier,
                        featureName: 'режим «Сезонность»',
                        indicator: 'seasonality',
                      });
                    }
                    return;
                  }
                  if (mode === 'yearly') setMode('monthly');
                }}
                style={{
                  flex: 1,
                  justifyContent: 'center',
                  padding: '12px 8px',
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 4,
                  opacity: histogramLocked ? 0.5 : 1,
                  cursor: histogramLocked ? 'not-allowed' : 'pointer',
                }}
                aria-disabled={histogramLocked}
              >
                Гистограмма
                {histogramLocked && <Lock size={11} strokeWidth={2.2} />}
              </button>
              <button
                className={`fm-chip ${mode === 'yearly' ? 'active' : ''}`}
                onClick={() => setMode('yearly')}
                style={{ flex: 1, justifyContent: 'center', padding: '12px 8px' }}
              >
                Годовая
              </button>
            </div>
          </div>

          {/* Группировка показывается только в гистограммном режиме */}
          {mode !== 'yearly' && (
            <div>
              <div style={{ fontSize: 11, fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
                Группировка
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                {(['weekday', 'monthly', 'monthday'] as MobileMode[]).map((m) => (
                  <button
                    key={m}
                    className={`fm-chip ${mode === m ? 'active' : ''}`}
                    onClick={() => setMode(m)}
                    style={{ justifyContent: 'flex-start', padding: '12px 16px' }}
                  >
                    {MODE_LABELS[m]}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Сравнить с — multi-select для обоих режимов (histogram + yearly).
              Два независимых типа: rolling window + конкретный год. */}
          {availableYears.length > 0 && (
            <>
              <div>
                <div style={{ fontSize: 11, fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 6 }}>
                  Период с года
                </div>
                <div style={{ fontSize: 10, color: 'var(--text-muted)', marginBottom: 10 }}>
                  Средняя за период от выбранного года до сегодня.
                </div>
                <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                  {availableYears.map((yr) => {
                    const selected = compareYears.includes(yr);
                    return (
                      <button
                        key={yr}
                        className={`fm-chip ${selected ? 'active' : ''}`}
                        onClick={() => {
                          setCompareYears((prev) =>
                            prev.includes(yr) ? prev.filter((y) => y !== yr) : [...prev, yr],
                          );
                        }}
                        style={{ flex: '0 0 auto', minWidth: 56, justifyContent: 'center' }}
                      >
                        {yr}
                      </button>
                    );
                  })}
                </div>
              </div>

              <div>
                <div style={{ fontSize: 11, fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 6 }}>
                  Конкретный год
                </div>
                <div style={{ fontSize: 10, color: 'var(--text-muted)', marginBottom: 10 }}>
                  Только этот год, без последующих.
                </div>
                <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                  {availableYears.map((yr) => {
                    const selected = exactYears.includes(yr);
                    return (
                      <button
                        key={yr}
                        className={`fm-chip ${selected ? 'active' : ''}`}
                        onClick={() => {
                          setExactYears((prev) =>
                            prev.includes(yr) ? prev.filter((y) => y !== yr) : [...prev, yr],
                          );
                        }}
                        style={{ flex: '0 0 auto', minWidth: 56, justifyContent: 'center' }}
                      >
                        {yr}
                      </button>
                    );
                  })}
                </div>
                {(compareYears.length > 0 || exactYears.length > 0) && (
                  <button
                    onClick={() => { setCompareYears([]); setExactYears([]); }}
                    style={{
                      marginTop: 12,
                      padding: '6px 12px',
                      fontSize: 10,
                      fontWeight: 600,
                      background: 'transparent',
                      border: '1px solid color-mix(in srgb, var(--text-primary) 30%, transparent)',
                      borderRadius: 999,
                      color: 'var(--text-secondary)',
                      cursor: 'pointer',
                    }}
                  >
                    Очистить все сравнения
                  </button>
                )}
              </div>
            </>
          )}

          {/* Toggles row — без hints, простые лейблы */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            <ToggleRow
              label="Без дивидендов"
              checked={excludeDividends}
              onChange={setExcludeDividends}
            />
            <ToggleRow
              label="Медиана"
              checked={aggType === 'median'}
              onChange={(v) => setAggType(v ? 'median' : 'avg')}
            />
          </div>
        </div>
      </MobileSheet>

      <OnboardingTour
        steps={tourSteps}
        open={tour.open}
        onClose={tour.close}
      />
    </MobileLayout>
  );
}

// ──────────────────────────────────────────────────────────
// Sub-component: ToggleRow — переключатель «лейбл слева + подсказка + switch»
// ──────────────────────────────────────────────────────────

function ToggleRow({
  label,
  hint,
  checked,
  onChange,
}: {
  label: string;
  hint?: string;
  checked: boolean;
  onChange: (v: boolean) => void;
}) {
  return (
    <button
      type="button"
      onClick={() => onChange(!checked)}
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: 10,
        padding: '10px 14px',
        background: 'var(--bg-secondary)',
        border: '1.5px solid var(--text-primary)',
        borderRadius: 10,
        boxShadow: '2px 2px 0 var(--text-primary)',
        cursor: 'pointer',
        font: 'inherit',
        color: 'var(--text-primary)',
        textAlign: 'left',
      }}
    >
      <div style={{ display: 'flex', flexDirection: 'column', gap: 2, minWidth: 0, flex: 1 }}>
        <span style={{ fontWeight: 700, fontSize: 13 }}>{label}</span>
        {hint && (
          <span style={{ fontSize: 10, color: 'var(--text-muted)', lineHeight: 1.3 }}>
            {hint}
          </span>
        )}
      </div>
      <span
        aria-hidden
        style={{
          width: 34,
          height: 20,
          borderRadius: 999,
          background: checked ? 'var(--accent)' : 'color-mix(in srgb, var(--text-primary) 15%, transparent)',
          border: '1.5px solid var(--text-primary)',
          position: 'relative',
          transition: 'background 150ms ease',
          flexShrink: 0,
        }}
      >
        <span
          style={{
            position: 'absolute',
            top: 1,
            left: checked ? 15 : 1,
            width: 14,
            height: 14,
            borderRadius: 999,
            background: '#fff',
            border: '1px solid var(--text-primary)',
            transition: 'left 150ms ease',
          }}
        />
      </span>
    </button>
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

// CompareGroup — series для grouped bar histogram (compareYears + exactYears).
interface CompareGroup {
  label: string;
  color: string;
  bars: BarData[];
}

const COLOR_POS = 'var(--funds-flow-positive, #5BD49C)';
const COLOR_NEG = 'var(--funds-flow-negative, #FF7A5C)';

function SeasonalityBars({
  seriesGroups,
  maxAbs,
}: {
  seriesGroups: CompareGroup[];
  maxAbs: number;
}) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState({ w: 360, h: 240 });
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const [touchX, setTouchX] = useState<number | null>(null);
  const [animated, setAnimated] = useState(false);

  const base = seriesGroups[0];
  const bars = base?.bars ?? [];
  const isMulti = seriesGroups.length >= 2;
  const nSeries = seriesGroups.length;

  useEffect(() => {
    if (!wrapRef.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) {
        const r = e.contentRect;
        if (r.width > 0 && r.height > 0) {
          setSize((prev) => {
            if (Math.abs(prev.w - r.width) < 8 && Math.abs(prev.h - r.height) < 8) return prev;
            return { w: Math.max(200, r.width), h: Math.max(160, r.height) };
          });
        }
      }
    });
    ro.observe(wrapRef.current);
    return () => ro.disconnect();
  }, []);

  useEffect(() => {
    setAnimated(false);
    const t = setTimeout(() => setAnimated(true), 30);
    return () => clearTimeout(t);
  }, [seriesGroups]);

  const { w: width, h: height } = size;
  const padX = 8;
  const padTop = 12;
  const padBottom = 22;
  const innerW = width - padX * 2;
  const innerH = height - padTop - padBottom;
  const zeroY = padTop + innerH / 2;
  const halfH = innerH / 2;

  // Slot-based geometry как на ПК: slotW = innerW / bars.length,
  // внутри slot — N узких sub-bars (если multi-series).
  const slotW = bars.length > 0 ? innerW / bars.length : 0;
  const slotPadding = slotW * 0.1;
  const groupW = slotW - slotPadding * 2;
  const subBarW = isMulti ? groupW / nSeries : slotW * (bars.length > 12 ? 0.6 : 0.5);

  const labelStep = Math.max(1, Math.ceil(bars.length / 6));

  const updateHover = (clientX: number) => {
    const rect = wrapRef.current?.getBoundingClientRect();
    if (!rect) return;
    const relX = clientX - rect.left;
    setTouchX(relX);
    const idx = Math.floor((relX - padX) / slotW);
    if (idx < 0 || idx >= bars.length) {
      setHoverIdx(null);
      return;
    }
    setHoverIdx((prev) => {
      if (prev === idx) return prev;
      if (typeof navigator !== 'undefined' && typeof navigator.vibrate === 'function') {
        navigator.vibrate(8);
      }
      return idx;
    });
  };

  return (
    <div ref={wrapRef} style={{ position: 'relative', width: '100%', height: '100%', minHeight: 160 }}>
      <svg
        width={width}
        height={height}
        style={{ display: 'block', touchAction: 'none' }}
        onTouchStart={(e) => updateHover(e.touches[0].clientX)}
        onTouchMove={(e) => updateHover(e.touches[0].clientX)}
        onTouchEnd={() => { setHoverIdx(null); setTouchX(null); }}
      >
        {/* Zero line */}
        <line
          x1={padX}
          y1={zeroY}
          x2={padX + innerW}
          y2={zeroY}
          stroke="color-mix(in srgb, var(--text-primary) 25%, transparent)"
          strokeWidth={1}
        />

        {/* Dashed crosshair (по центру выбранного slot) */}
        {hoverIdx !== null && (() => {
          const cx = padX + hoverIdx * slotW + slotW / 2;
          return (
            <line
              x1={cx}
              y1={padTop}
              x2={cx}
              y2={padTop + innerH}
              stroke="var(--text-primary)"
              strokeWidth={1}
              strokeDasharray="3,3"
              opacity={0.6}
              pointerEvents="none"
            />
          );
        })()}

        {/* Bars: single-series — green/red по знаку. Multi-series — grouped
            sub-bars side-by-side, цвет = series.color. */}
        {bars.map((_b, i) => {
          const delay = Math.min(i * 4, 200);
          const transition = `y 500ms cubic-bezier(0.34, 1.56, 0.64, 1) ${delay}ms, height 500ms cubic-bezier(0.34, 1.56, 0.64, 1) ${delay}ms, opacity 150ms ease`;

          if (isMulti) {
            return (
              <g key={i}>
                {seriesGroups.map((group, s) => {
                  const seriesBar = group.bars[i];
                  if (!seriesBar) return null;
                  const val = seriesBar.value;
                  const finalH = (Math.abs(val) / maxAbs) * halfH;
                  if (finalH < 0.5) return null;
                  const h = animated ? finalH : 0;
                  const bx = padX + i * slotW + slotPadding + s * subBarW;
                  const y = val >= 0 ? zeroY - h : zeroY;
                  const isHovered = hoverIdx === i;
                  return (
                    <rect
                      key={`s${s}`}
                      x={bx + 1}
                      y={y}
                      width={Math.max(1, subBarW - 2)}
                      height={h}
                      fill={group.color}
                      opacity={isHovered ? 1 : 0.85}
                      rx={1.5}
                      style={{ transition }}
                    />
                  );
                })}
              </g>
            );
          }

          // Single series fallback (как раньше)
          const b = bars[i];
          const val = b.value;
          const isPositive = val >= 0;
          const finalH = (Math.abs(val) / maxAbs) * halfH;
          if (finalH < 0.5) return null;
          const h = animated ? finalH : 0;
          const bx = padX + i * slotW + (slotW - subBarW) / 2;
          const y = isPositive ? zeroY - h : zeroY;
          const fill = isPositive ? COLOR_POS : COLOR_NEG;
          const isHovered = hoverIdx === i;
          return (
            <rect
              key={i}
              x={bx}
              y={y}
              width={subBarW}
              height={h}
              fill={fill}
              opacity={isHovered ? 1 : 0.85}
              rx={2}
              style={{ transition }}
            />
          );
        })}

        {/* X-axis labels */}
        {bars.map((b, i) => {
          if (i % labelStep !== 0 && i !== bars.length - 1) return null;
          const isFirst = i === 0;
          const isLast = i === bars.length - 1;
          const anchor: 'start' | 'middle' | 'end' = isFirst ? 'start' : isLast ? 'end' : 'middle';
          const x = isFirst
            ? padX
            : isLast
              ? padX + innerW
              : padX + i * slotW + slotW / 2;
          return (
            <text
              key={`xl-${i}`}
              x={x}
              y={height - 6}
              fontSize={9.5}
              fontWeight={600}
              fill="var(--text-secondary)"
              textAnchor={anchor}
              dominantBaseline="alphabetic"
            >
              {b.label}
            </text>
          );
        })}
      </svg>

      {/* Tooltip: на multi-series показываем все values по hoveredIdx. */}
      {hoverIdx !== null && touchX !== null && (() => {
        const tooltipW = isMulti ? 220 : 180;
        const margin = 8;
        const left = Math.max(margin, Math.min(touchX - tooltipW / 2, width - tooltipW - margin));
        const labelHeader = bars[hoverIdx]?.label ?? '';
        return (
          <div
            style={{
              position: 'absolute',
              top: 4,
              left,
              width: tooltipW,
              padding: '6px 10px',
              background: 'var(--bg-primary)',
              border: '1.5px solid var(--text-primary)',
              borderRadius: 8,
              fontSize: 11,
              pointerEvents: 'none',
              display: 'flex',
              flexDirection: 'column',
              gap: 3,
            }}
          >
            <div style={{ fontSize: 10, color: 'var(--text-secondary)', fontWeight: 700 }}>
              {labelHeader}
            </div>
            {seriesGroups.map((group, s) => {
              const sb = group.bars[hoverIdx];
              if (!sb) return null;
              const valColor = sb.value >= 0 ? COLOR_POS : COLOR_NEG;
              return (
                <div key={s} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8 }}>
                  <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, minWidth: 0 }}>
                    <span style={{ width: 7, height: 7, borderRadius: 99, background: group.color, flexShrink: 0 }} />
                    <span style={{ fontSize: 10, color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {group.label}
                    </span>
                  </span>
                  <span className="mono" style={{ color: valColor, fontWeight: 800, fontSize: 12 }}>
                    {sb.value >= 0 ? '+' : ''}{sb.value.toFixed(2)}%
                  </span>
                </div>
              );
            })}
          </div>
        );
      })()}
    </div>
  );
}

// ──────────────────────────────────────────────────────────
// YearlySeasonalityChart — 2 линии: средняя сезонность (накопленный % от
// начала года, усреднённый по всем годам в выборке) + текущий год.
// Использует MobileChart с двумя сериями на одной оси (%).
// ──────────────────────────────────────────────────────────

const MONTH_BOUNDARIES = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек'];

function YearlySeasonalityChart({
  data,
  compareData,
  compareYears,
}: {
  data: YearlySeasonalityResponse;
  compareData: YearlySeasonalityResponse[];
  compareYears: number[];
}) {
  // Преобразуем td (trading day) в синтетические даты С УЧЁТОМ ФАКТИЧЕСКИХ
  // month-границ из API (поле p.month у average). Без этого td=123 у SBER
  // (реальная дата 19 мая) рендерился как «конец июня», потому что
  // линейное масштабирование (td * 365/252) усредняет неравномерность
  // торговых дней в календаре (зимой 1.08 days/td, летом-осенью 1.77 days/td).
  //
  // Алгоритм:
  //   1. Из avg собираем monthStartTd[1..12] — первый td каждого месяца
  //   2. tdToTime(td) ищет месяц для td и интерполирует день внутри месяца
  //      пропорционально позиции td в диапазоне [start, nextStart)
  //   3. Тот же tdToTime применяется и к cur, и к avg → x-positions
  //      совпадают, x-labels (Янв/Фев/.../Дек) рендерятся в правильных
  //      местах через formatXLabel + d.getMonth().
  const series = useMemo(() => {
    const yr = data.current_year;

    // Собираем границы месяцев из avg (даже если cur короче — avg всегда
    // покрывает полный год = 252 td).
    const monthStartTd: number[] = []; // [0]=Jan firstTd, [1]=Feb firstTd, ...
    const seen = new Set<number>();
    for (const p of data.average) {
      if (!seen.has(p.month)) {
        seen.add(p.month);
        monthStartTd[p.month - 1] = p.td;
      }
    }
    const lastTd = data.average.length > 0 ? data.average[data.average.length - 1].td : 251;

    const daysInMonth = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    const tdToTime = (td: number) => {
      // Находим месяц для td: ищем монотонно с конца, чтобы получить
      // самый позднии month с monthStartTd <= td.
      let m = 1; // 1..12
      for (let i = 11; i >= 0; i--) {
        if (monthStartTd[i] !== undefined && td >= monthStartTd[i]) {
          m = i + 1;
          break;
        }
      }
      const startTd = monthStartTd[m - 1] ?? 0;
      const nextStartTd = m < 12 ? (monthStartTd[m] ?? lastTd + 1) : lastTd + 1;
      const fraction = nextStartTd > startTd
        ? (td - startTd) / (nextStartTd - startTd)
        : 0;
      const dayOfMonth = Math.max(1, Math.min(
        daysInMonth[m - 1],
        Math.round(1 + fraction * (daysInMonth[m - 1] - 1)),
      ));
      // Конструируем напрямую через String — Date constructor с (yr, 0, dayN)
      // может drift'ить через timezones. ISO 'YYYY-MM-DD' стабилен.
      const mm = String(m).padStart(2, '0');
      const dd = String(dayOfMonth).padStart(2, '0');
      return `${yr}-${mm}-${dd}`;
    };

    const avgData = data.average.map((p) => ({
      time: tdToTime(p.td),
      value: p.avg_pct,
    }));
    // CUR: time = синтетический (по td) для x-позиционирования и alignment;
    // displayTime = реальная дата трейдинг-дня из API (используется в tooltip).
    // Это даёт визуальное согласование с avg по td-shape, но юзер видит
    // настоящие даты "19.05.26" в подсказках, а не Jun-X от td-маппинга.
    const curData = data.current.map((p) => ({
      time: tdToTime(p.td),
      value: p.pct,
      displayTime: p.date,
    }));

    // Order: current first (pill), затем compareYears (свои цвета),
    // затем base average (last, hidePill). Shared left axis, % units.
    const compareSeries = compareData.map((cd, i) => ({
      data: cd.average.map((p) => ({ time: tdToTime(p.td), value: p.avg_pct })),
      color: COMPARE_COLORS[i % COMPARE_COLORS.length],
      label: `с ${compareYears[i]}`,
      axis: 'left' as const,
      formatValue: (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`,
      hidePill: true,
    }));

    return [
      ...(curData.length > 0
        ? [{
            data: curData,
            color: 'var(--accent)',
            label: `${yr}`,
            axis: 'left' as const,
            formatValue: (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`,
          }]
        : []),
      ...compareSeries,
      {
        data: avgData,
        color: 'var(--text-secondary)',
        label: 'Средняя',
        axis: 'left' as const,
        formatValue: (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`,
        hidePill: true,
      },
    ];
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, compareData, compareYears]);

  return (
    <MobileChart
      series={series}
      loading={false}
      formatXLabel={(t) => {
        const d = new Date(t);
        if (isNaN(d.getTime())) return t;
        return MONTH_BOUNDARIES[d.getMonth()];
      }}
    />
  );
}


