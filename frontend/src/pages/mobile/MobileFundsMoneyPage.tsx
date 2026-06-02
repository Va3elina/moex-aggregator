/**
 * MobileFundsMoneyPage — мобильная версия «Деньги в фондах».
 *
 * Phase 3 упрощённая версия:
 *   - Category chips (Денежный рынок / Акции / Облигации / Золото)
 *   - Period chips (1М/6М/2Г/Всё)
 *   - Chart с суммарной СЧА категории + индексом-эталоном
 *   - Притоки-Оттоки и таблица фондов — Phase 4
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { Wallet, Lock } from 'lucide-react';
import { useTierAccess } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../../components/tier/UpgradeModal';
import MobileLayout from '../../components/mobile/MobileLayout';
import MobilePageHeader from '../../components/mobile/MobilePageHeader';
import MobileChart from '../../components/mobile/MobileChart';
import MobileSkeleton from '../../components/mobile/MobileSkeleton';
import MobileFlowsHistogram from '../../components/mobile/MobileFlowsHistogram';
import {
  getFundsChartData,
  getFundsFlows,
  type FundsChartResponse,
  type FundsFlowsResponse,
  type FundCategory,
  type FundPeriod,
  type FlowTimeframe,
} from '../../services/api';
import MobileSheet from '../../components/mobile/MobileSheet';
import { useOnboardingTour } from '../../hooks/useFirstVisit';
import OnboardingTour, { type TourStep } from '../../components/onboarding/OnboardingTour';

type ViewMode = 'aum' | 'flows';

const CATEGORIES: Array<{ key: FundCategory; label: string; comingSoon?: boolean }> = [
  { key: 'money_market', label: 'Деньги' },
  { key: 'stocks', label: 'Акции' },
  { key: 'bonds', label: 'Облигации' },
  { key: 'gold', label: 'Золото' },
  // Раздел «Юань» — пока «Скоро» (NAV юаневых фондов наливается).
  { key: 'yuan', label: 'Юань', comingSoon: true },
];

const PERIODS: Array<{ key: FundPeriod; label: string }> = [
  { key: '1m', label: '1М' },
  { key: '6m', label: '6М' },
  { key: '2y', label: '2Г' },
  { key: 'all', label: 'Всё' },
];

export default function MobileFundsMoneyPage() {
  const [category, setCategory] = useState<FundCategory>('money_market');
  const [period, setPeriod] = useState<FundPeriod>('6m');
  // По умолчанию — Притоки-Оттоки: даёт юзеру сразу actionable signal
  // (приток/отток денег за день/неделю/месяц), а не статичный график СЧА.
  const [viewMode, setViewMode] = useState<ViewMode>('flows');
  const [flowTimeframe, setFlowTimeframe] = useState<FlowTimeframe>('1w');
  const fundsAccess = useTierAccess('funds_money');
  const { showUpgrade } = useUpgradePrompt();
  const [data, setData] = useState<FundsChartResponse | null>(null);
  const [flowsData, setFlowsData] = useState<FundsFlowsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [timeSheetOpen, setTimeSheetOpen] = useState(false);
  const [optionsSheetOpen, setOptionsSheetOpen] = useState(false);
  const [fundsSheetOpen, setFundsSheetOpen] = useState(false);
  // Hidden funds — Set fund_id'ов которые юзер скрыл из суммарной СЧА.
  // Сбрасывается при смене категории (фонды разные).
  const [hiddenFunds, setHiddenFunds] = useState<Set<number>>(new Set());
  // При смене категории — показываем все фонды заново
  useEffect(() => { setHiddenFunds(new Set()); }, [category]);
  const toggleFundVisibility = (fundId: number) =>
    setHiddenFunds((prev) => {
      const next = new Set(prev);
      if (next.has(fundId)) next.delete(fundId);
      else next.add(fundId);
      return next;
    });

  const tour = useOnboardingTour('funds-money');
  // Единый паттерн: Intro → Кнопки → Время → Опции (с демо режима + фонды) → Чтение → End
  const tourSteps: TourStep[] = [
    {
      selector: null,
      title: 'Деньги в фондах',
      body: (
        <>
          <p style={{ marginBottom: 8 }}>
            Куда инвесторы заносят деньги — в фонды денежного рынка, акций,
            облигаций или золота. Притоки и оттоки — индикатор настроения.
          </p>
          <p>Разберём управление и научимся читать график.</p>
        </>
      ),
      onEnter: () => {
        setOptionsSheetOpen(false);
        setTimeSheetOpen(false);
        setFundsSheetOpen(false);
      },
    },
    {
      selector: '.fm-page-actions',
      title: 'Кнопки управления',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>Снизу — 2 кнопки:</p>
          <p style={{ marginBottom: 4 }}>
            <strong>Время</strong> — глубина истории
          </p>
          <p>
            <strong>Опции</strong> — категория, режим, таймфрейм притоков,
            выбор конкретных фондов
          </p>
        </>
      ),
      position: 'top',
      spotlightPadding: 4,
    },
    {
      selector: null,
      title: 'Период',
      align: 'top',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>
            Открыл для тебя кнопку <strong>«Время»</strong>:
          </p>
          <p>
            Глубина истории: <strong>1 месяц / 6 месяцев / 2 года / Вся</strong>.
            Для коротких сроков — Притоки информативнее, для длинных — СЧА.
          </p>
        </>
      ),
      onEnter: () => { setTimeSheetOpen(true); setOptionsSheetOpen(false); setFundsSheetOpen(false); },
    },
    {
      selector: null,
      title: 'Категория, режимы, фонды',
      align: 'top',
      body: (
        <>
          <p style={{ marginBottom: 4 }}>
            Открыл кнопку <strong>«Опции»</strong>. Внутри:
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Категория:</strong> Деньги / Акции / Облигации / Золото
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Режим:</strong> Притоки (зелёный = приток, красный = отток)
            или СЧА (линия активов категории). Сейчас переключил на СЧА.
          </p>
          <p>
            <strong>Фонды:</strong> ниже есть строчка <em>Фонды (N/N)</em> —
            можно скрыть отдельные фонды, сумма пересчитается на лету.
          </p>
        </>
      ),
      onEnter: () => { setViewMode('aum'); setOptionsSheetOpen(true); setTimeSheetOpen(false); setFundsSheetOpen(false); },
    },
    {
      selector: '[data-tour="funds-chart"]',
      title: 'Чтение графика',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>
            На СЧА — оранжевая линия общей стоимости активов категории.
            Синяя — индекс-эталон (для денежного рынка RUSFAR-ставка ЦБ).
          </p>
          <p>
            Зажми палец — появится перекрестье с точными значениями.
            Возвращаю режим Притоков по умолчанию.
          </p>
        </>
      ),
      position: 'top',
      spotlightPadding: 4,
      onEnter: () => { setViewMode('flows'); setOptionsSheetOpen(false); setFundsSheetOpen(false); setTimeSheetOpen(false); },
    },
    {
      selector: null,
      title: 'Готово!',
      body: (
        <p>
          Нажми <strong>?</strong> рядом с заголовком — методология и
          источники данных по фондам.
        </p>
      ),
    },
  ];

  // Доступные (не tier-locked) фонды — только они дают данные на графики.
  // Locked-фонды приходят с tier_locked=true (тизер для гостя/Free на тарифных
  // фондах) — отображаем в funds-sheet с замочком, в расчётах не участвуют.
  // Зеркало desktop FundsMoneyPage.accessibleFunds.
  const accessibleFunds = useMemo(
    () => data?.funds.filter((f) => !f.tier_locked) ?? [],
    [data?.funds],
  );

  // visibleFundIds: список fund_id'ов для фильтрации API-запроса flows.
  // undefined = все доступные видимы (без фильтра, fastpath на бэке).
  // Если есть hidden — отправляем явный список видимых (только из accessible).
  const visibleFundIds = useMemo(() => {
    if (!data?.funds) return undefined;
    const visibleAccessible = accessibleFunds.filter((f) => !hiddenFunds.has(f.fund_id));
    if (visibleAccessible.length === accessibleFunds.length) return undefined;
    return visibleAccessible.map((f) => f.fund_id);
  }, [data?.funds, accessibleFunds, hiddenFunds]);

  // loadData — для refresh / pull-to-refresh. Зависит ТОЛЬКО от выбора
  // категории/периода/режима — не от hiddenFunds, чтобы toggle фонда
  // не триггерил refetch + loading-skeleton.
  // Для flows visibleFundIds читаем через ref внутри callback'а.
  const visibleFundIdsRef = useRef(visibleFundIds);
  useEffect(() => { visibleFundIdsRef.current = visibleFundIds; }, [visibleFundIds]);

  const loadData = useMemo(
    () => async () => {
      try {
        setLoading(true);
        // data (FundsChartResponse) грузим ВСЕГДА — он несёт список фондов
        // и tier_locked-флаги, нужные для кнопки «Фонды» и lock-индикаторов
        // в funds-sheet. Раньше data грузился только в AUM-режиме → в flows
        // кнопка управления фондами оставалась скрытой пока не переключишься.
        const chartResult = await getFundsChartData(category, period);
        setData(chartResult);
        // flowsData — только в flows-режиме (для гистограммы притоков-оттоков).
        if (viewMode === 'flows') {
          const flowsResult = await getFundsFlows(category, flowTimeframe, period, visibleFundIdsRef.current);
          setFlowsData(flowsResult);
        }
      } catch (err) {
        console.error('Ошибка funds:', err);
        const msg = err instanceof Error ? err.message : String(err);
        if (msg.includes('тарифе') || msg.includes('недоступ')) {
          const requiredTier: 'basic' | 'pro' = msg.includes('Pro') ? 'pro' : 'basic';
          showUpgrade({
            tier: requiredTier,
            featureName: flowTimeframe === '1d' ? 'дневной таймфрейм' : 'индикатор «Деньги в фондах»',
            indicator: 'funds_money',
          });
        }
      } finally {
        setLoading(false);
      }
    },
    [viewMode, category, period, flowTimeframe, showUpgrade],
  );

  useEffect(() => {
    void loadData();
  }, [loadData]);

  // Дополнительный refetch для flows-режима при toggle фондов.
  // Не зависит от loadData identity → не триггерит мигание в AUM режиме.
  useEffect(() => {
    if (viewMode !== 'flows') return;
    void (async () => {
      try {
        const result = await getFundsFlows(category, flowTimeframe, period, visibleFundIds);
        setFlowsData(result);
      } catch (err) {
        console.error('Flows refetch error:', err);
      }
    })();
    // Зависимость от visibleFundIds — единственный триггер этого effect'а
    // в режиме flows. В режиме AUM этот effect возвращается early.
  }, [visibleFundIds, viewMode, category, flowTimeframe, period]);

  // Series: суммарная СЧА (млрд ₽) + индекс.
  //
  // ВАЖНО: НЕ используем data.total_nav из API напрямую. Backend суммирует
  // через SQL `SUM(nav) GROUP BY date` без forward-fill. Это даёт ложные
  // «провалы» за последние дни — фонды публикуют NAV в разное время
  // (Альфа утром, Сбер вечером), и SUM на «утренней» дате не включает
  // ещё неопубликованные фонды.
  //
  // Решение: локально, как в desktop FundsMoneyPage:
  //   1. Forward-fill per fund — для каждой даты используем last known NAV
  //      этого конкретного фонда
  //   2. Суммируем по ВИДИМЫМ фондам (с учётом hiddenFunds toggle)
  const chartSeries = useMemo(() => {
    if (!data) return [];

    // Только accessible (не tier_locked) и не скрытые юзером.
    const visibleFunds = accessibleFunds.filter((f) => !hiddenFunds.has(f.fund_id));

    // Все уникальные даты по всем видимым фондам
    const allDates = new Set<string>();
    for (const f of visibleFunds) for (const p of f.data) allDates.add(p.date);
    const sortedDates = Array.from(allDates).sort();

    // Forward-fill per fund → Map<date, nav>
    const fundNavMaps = visibleFunds.map((fund) => {
      const map = new Map<string, number>();
      let lastNav = 0;
      const sorted = [...fund.data].sort((a, b) => a.date.localeCompare(b.date));
      for (const d of sortedDates) {
        const pt = sorted.find((p) => p.date === d);
        if (pt?.nav) lastNav = pt.nav;
        if (lastNav > 0) map.set(d, lastNav);
      }
      return map;
    });

    // Sum across visible funds for each date (млрд ₽)
    const navData = sortedDates
      .map((date) => {
        let sum = 0;
        for (const m of fundNavMaps) sum += m.get(date) ?? 0;
        return { time: date, value: sum / 1e9 };
      })
      .filter((d) => d.value > 0);

    const indexData = data.index?.data
      ?.filter((d) => d.close !== null)
      .map((d) => ({ time: d.date, value: d.close as number })) ?? [];

    return [
      {
        data: navData,
        color: 'var(--accent)',
        label: 'СЧА',
        axis: 'left' as const,
        formatValue: (v: number) => `${v.toFixed(0)}`,
      },
      ...(indexData.length > 0
        ? [{
            data: indexData,
            color: 'var(--chart-line-1, #5DA3E9)',
            label: data.index?.secid ?? 'Индекс',
            axis: 'right' as const,
            formatValue: (v: number) => v >= 1000 ? (v / 1000).toFixed(1) + 'K' : v.toFixed(0),
          }]
        : []),
    ];
  }, [data, hiddenFunds]);

  const categoryLabel = CATEGORIES.find((c) => c.key === category)?.label ?? '';
  const periodLabel = PERIODS.find((p) => p.key === period)?.label ?? '';
  const optionsSummary = `${categoryLabel} · ${viewMode === 'aum' ? 'СЧА' : 'Притоки'}`;

  return (
    <MobileLayout
      onTimeClick={() => setTimeSheetOpen(true)}
      timeSummary={periodLabel}
      timeTourId="funds-time"
      onSettingsClick={() => setOptionsSheetOpen(true)}
      settingsSummary={optionsSummary}
      settingsTourId="funds-options"
      onRefresh={loadData}
      loading={loading}
    >
      <MobilePageHeader
        Icon={Wallet}
        title="Деньги в фондах"
        helpLink="/methodology/funds-money"
        sourceNote="Индексы (IMOEX, RGBI, IMOEX2, GLDRUB): ПАО Московская Биржа"
      />

      {/* Full-bleed chart area. Histogram (Притоки) и MobileChart (СЧА)
          оба растягиваются на весь parent через absolute inset:0. */}
      <div
        data-tour="funds-chart"
        style={{ flex: 1, minHeight: 0, position: 'relative' }}
      >
        <div style={{ position: 'absolute', inset: 0 }}>
          {viewMode === 'aum' ? (
            <MobileChart
              series={chartSeries}
              loading={loading}
              formatXLabel={(t) => {
                // FundsMoney AUM — всегда дневные точки. Default formatter
                // ошибочно считает intraday timestamp типа T07:00:00 (MOEX-open),
                // выводит часы. Принудительно DD.MM.YY.
                const d = new Date(t);
                if (isNaN(d.getTime())) return t;
                return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' });
              }}
            />
          ) : loading ? (
            <MobileSkeleton variant="chart" height="100%" />
          ) : flowsData && flowsData.flows.length > 0 ? (
            <MobileFlowsHistogram flows={flowsData.flows} />
          ) : (
            <div style={{ display: 'grid', placeItems: 'center', height: '100%', color: 'var(--text-muted)' }}>
              Нет данных
            </div>
          )}
        </div>
      </div>

      {/* Time sheet: период */}
      <MobileSheet
        open={timeSheetOpen}
        onClose={() => setTimeSheetOpen(false)}
        title="Период"
      >
        <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 8 }}>
          {PERIODS.map((p) => {
            // Глубокие периоды (2Г/Всё) для гостя/Free под замком: backend
            // ответит 403. canUsePeriod читает ту же матрицу тарифов.
            const allowed = fundsAccess.isLoading || fundsAccess.canUsePeriod(p.key);
            return (
              <button
                key={p.key}
                className={`fm-chip ${period === p.key ? 'active' : ''}`}
                onClick={() => {
                  if (!allowed) {
                    const tier = fundsAccess.requiredTierFor({ period: p.key });
                    if (tier) {
                      showUpgrade({
                        tier,
                        featureName: `период «${p.label}»`,
                        indicator: 'funds_money',
                      });
                    }
                    setTimeSheetOpen(false);
                    return;
                  }
                  setPeriod(p.key);
                  setTimeSheetOpen(false);
                }}
                style={{
                  justifyContent: 'flex-start',
                  padding: '14px 16px',
                  opacity: allowed ? 1 : 0.5,
                }}
                aria-disabled={!allowed}
              >
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                  {p.label}
                  {!allowed && <Lock size={12} strokeWidth={2.2} />}
                </span>
              </button>
            );
          })}
        </div>
      </MobileSheet>

      {/* Settings sheet: категория + viewMode + flow timeframe */}
      <MobileSheet
        open={optionsSheetOpen}
        onClose={() => setOptionsSheetOpen(false)}
        title="Опции"
      >
        <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 20 }}>
          {/* Категория */}
          <div>
            <div style={{ fontSize: 11, fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
              Категория
            </div>
            <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
              {CATEGORIES.map((cat) => {
                const soon = !!cat.comingSoon;
                return (
                <button
                  key={cat.key}
                  className={`fm-chip ${category === cat.key ? 'active' : ''}`}
                  onClick={() => {
                    if (!soon) setCategory(cat.key);
                  }}
                  disabled={soon}
                  style={{ flex: 1, minWidth: 'calc(50% - 4px)', justifyContent: 'center', opacity: soon ? 0.5 : undefined, cursor: soon ? 'not-allowed' : undefined }}
                >
                  {soon ? `${cat.label} · Скоро` : cat.label}
                </button>
                );
              })}
            </div>
          </div>

          {/* Режим */}
          <div>
            <div style={{ fontSize: 11, fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
              Что показать
            </div>
            <div style={{ display: 'flex', gap: 6 }}>
              {(['aum', 'flows'] as const).map((m) => (
                <button
                  key={m}
                  className={`fm-chip ${viewMode === m ? 'active' : ''}`}
                  onClick={() => setViewMode(m)}
                  style={{ flex: 1, justifyContent: 'center' }}
                >
                  {m === 'aum' ? 'СЧА' : 'Притоки'}
                </button>
              ))}
            </div>
          </div>

          {/* Таймфрейм для притоков */}
          {viewMode === 'flows' && (
            <div>
              <div style={{ fontSize: 11, fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
                Период притоков
              </div>
              <div style={{ display: 'flex', gap: 6 }}>
                {(['1d', '1w', '1m'] as FlowTimeframe[]).map((tf) => {
                  const allowed = fundsAccess.isLoading || fundsAccess.canUseTimeframe(tf);
                  const label = tf === '1d' ? 'День' : tf === '1w' ? 'Неделя' : 'Месяц';
                  return (
                    <button
                      key={tf}
                      className={`fm-chip ${flowTimeframe === tf ? 'active' : ''}`}
                      onClick={() => {
                        if (!allowed) {
                          const tier = fundsAccess.requiredTierFor({ timeframe: tf });
                          if (tier) {
                            showUpgrade({
                              tier,
                              featureName: `таймфрейм «${label}»`,
                              indicator: 'funds_money',
                            });
                          }
                          return;
                        }
                        setFlowTimeframe(tf);
                      }}
                      style={{
                        flex: 1,
                        justifyContent: 'center',
                        opacity: allowed ? 1 : 0.5,
                        cursor: allowed ? 'pointer' : 'not-allowed',
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: 4,
                      }}
                      aria-disabled={!allowed}
                    >
                      {label}
                      {!allowed && <Lock size={11} strokeWidth={2.2} />}
                    </button>
                  );
                })}
              </div>
            </div>
          )}

          {/* Выбор видимых фондов — открывает отдельный sheet чтоб не
              перегружать основной options. Counter показывает только accessible
              (не locked) — locked-фонды для гостя/Free не идут в расчёт. */}
          {data && data.funds.length > 1 && (
            <button
              className="fm-chip"
              onClick={() => {
                setOptionsSheetOpen(false);
                setFundsSheetOpen(true);
              }}
              style={{ justifyContent: 'space-between', padding: '14px 16px' }}
            >
              <span>Фонды</span>
              <span style={{ color: 'var(--text-muted)', fontSize: 13 }}>
                {accessibleFunds.length - hiddenFunds.size}/{accessibleFunds.length}
              </span>
            </button>
          )}
        </div>
      </MobileSheet>

      {/* Funds sheet — список фондов с тогглом видимости.
          Скрытие фонда исключает его из суммарной СЧА и притоков-оттоков. */}
      <MobileSheet
        open={fundsSheetOpen}
        onClose={() => setFundsSheetOpen(false)}
        title="Фонды в категории"
      >
        <div style={{ padding: '8px 16px 16px', display: 'flex', flexDirection: 'column', gap: 4 }}>
          {/* Quick-actions: все / никто */}
          <div style={{ display: 'flex', gap: 6, marginBottom: 8 }}>
            <button
              className="fm-chip"
              onClick={() => setHiddenFunds(new Set())}
              style={{ flex: 1, justifyContent: 'center' }}
            >
              Показать все
            </button>
            <button
              className="fm-chip"
              onClick={() => {
                // Скрыть только accessible — locked фонды и так не в расчётах,
                // включать их в hiddenFunds бессмысленно.
                setHiddenFunds(new Set(accessibleFunds.map((f) => f.fund_id)));
              }}
              style={{ flex: 1, justifyContent: 'center' }}
            >
              Скрыть все
            </button>
          </div>
          {data?.funds.map((f) => {
            const locked = !!f.tier_locked;
            const hidden = hiddenFunds.has(f.fund_id);
            const lastNav = f.data.length > 0 ? f.data[f.data.length - 1].nav : null;
            // На locked-фондах tap → upgrade-modal, как на десктопе в FundsTable.
            // requiredTier берём через access-матрицу, как везде. Без actual
            // тира backend всё равно отдаст подходящий 403, но мы предотвращаем
            // запрос — показываем modal заранее.
            const requiredTier: 'basic' | 'pro' = fundsAccess.requiredTierFor({ asset: f.ticker }) ?? 'basic';
            return (
              <button
                key={f.fund_id}
                onClick={() => {
                  if (locked) {
                    showUpgrade({
                      tier: requiredTier,
                      featureName: `фонд ${f.ticker}`,
                      indicator: 'funds_money',
                    });
                    return;
                  }
                  toggleFundVisibility(f.fund_id);
                }}
                className="fm-chip"
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  padding: '12px 14px',
                  opacity: locked ? 0.55 : hidden ? 0.5 : 1,
                  transition: 'opacity 0.15s',
                  cursor: 'pointer',
                }}
                aria-disabled={locked}
              >
                <span style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start', gap: 2 }}>
                  <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6, fontWeight: 700, fontSize: 13 }}>
                    {f.ticker}
                    {locked && <Lock size={11} strokeWidth={2.2} />}
                  </span>
                  <span style={{ fontSize: 11, color: 'var(--text-muted)', textAlign: 'left' }}>
                    {f.name}
                  </span>
                </span>
                <span style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 2 }}>
                  <span style={{ fontSize: 12, fontWeight: 600 }}>
                    {locked
                      ? '—'
                      : lastNav ? (lastNav / 1e9).toFixed(1) + ' млрд' : '—'}
                  </span>
                  {!locked && (
                    <span
                      style={{
                        width: 36,
                        height: 18,
                        borderRadius: 999,
                        background: hidden ? 'var(--bg-tertiary)' : 'var(--accent)',
                        border: `1.5px solid ${hidden ? 'var(--border-color)' : 'var(--accent)'}`,
                        position: 'relative',
                        transition: 'background 0.18s ease',
                      }}
                    >
                      <span
                        style={{
                          position: 'absolute',
                          top: 1,
                          left: hidden ? 1 : 17,
                          width: 12,
                          height: 12,
                          borderRadius: '50%',
                          background: 'var(--bg-primary)',
                          transition: 'left 0.18s ease',
                        }}
                      />
                    </span>
                  )}
                </span>
              </button>
            );
          })}
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
