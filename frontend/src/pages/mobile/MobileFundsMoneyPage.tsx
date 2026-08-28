/**
 * MobileFundsMoneyPage — мобильная версия «Деньги в фондах».
 *
 * Phase 3 упрощённая версия:
 *   - Category chips (Денежный рынок / Акции / Облигации / Золото)
 *   - Period chips (1М/1Г/3Г/Всё)
 *   - Chart с суммарной СЧА категории + индексом-эталоном
 *   - Притоки-Оттоки и таблица фондов — Phase 4
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Wallet, Lock, AlarmClock } from 'lucide-react';
import { useTierAccess, useCommonFeatures } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../../components/tier/UpgradeModal';
import CreateFundAlertModal from '../../components/alerts/CreateFundAlertModal';
import { handleTierError } from '../../utils/tierError';
import { usePersistedState, usePersistedSet } from '../../hooks/usePersistedState';
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
import { resolveFundLogo, stripUkName } from '../../config/fundConfig';
import { useOnboardingTour } from '../../hooks/useFirstVisit';
import OnboardingTour, { type TourStep } from '../../components/onboarding/OnboardingTour';

type ViewMode = 'aum' | 'flows';

const CATEGORIES: Array<{ key: FundCategory; label: string; comingSoon?: boolean }> = [
  { key: 'money_market', label: 'Деньги' },
  { key: 'stocks', label: 'Акции' },
  { key: 'bonds', label: 'Облигации' },
  { key: 'gold', label: 'Золото' },
  // Раздел «Юань» — пока «Скоро» (NAV юаневых фондов наливается).
  { key: 'yuan', label: 'Юань' },
];

const PERIODS: Array<{ key: FundPeriod; label: string }> = [
  { key: '1m', label: '1М' },
  { key: '1y', label: '1Г' },
  { key: '3y', label: '3Г' },
  { key: 'all', label: 'Всё' },
];

export default function MobileFundsMoneyPage() {
  // Шарим desktop-ключи category/viewMode/flowTimeframe (записываемые наборы
  // совпадают). period → отдельный mobilePeriod: desktop frame:funds:period
  // реально пишет 3m/1y/3y (clamp FLOW_MIN_PERIODS), которых нет в mobile-чипах.
  const [searchParams, setSearchParams] = useSearchParams();
  const [category, setCategory] = usePersistedState<FundCategory>('frame:funds:category', 'money_market');
  // Выбор раздела пишем в ?category= (replace) — адресная строка отражает фонд,
  // URL можно сохранить в избранное, как ?instrument= на /oi. Чтение — ниже.
  const selectCategory = (c: FundCategory) => {
    setCategory(c);
    const next = new URLSearchParams(searchParams);
    next.set('category', c);
    setSearchParams(next, { replace: true });
  };
  // Диплинк из сигнала/аномалии: ?category= преселектит раздел. Применяем при
  // КАЖДОЙ навигации (не только на маунте) — иначе клик по второй fund-аномалии
  // другой категории не переключал бы раздел (SPA не перемонтирует страницу).
  const appliedFundsUrlRef = useRef('');
  useEffect(() => {
    const urlKey = searchParams.toString();
    if (urlKey === appliedFundsUrlRef.current) return;
    appliedFundsUrlRef.current = urlKey;
    const c = searchParams.get('category');
    if (c && ['money_market', 'stocks', 'bonds', 'gold', 'yuan'].includes(c)) setCategory(c as FundCategory);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams]);
  const [period, setPeriod] = usePersistedState<FundPeriod>('frame:funds:mobilePeriod', '1m');
  // По умолчанию — Притоки-Оттоки: даёт юзеру сразу actionable signal
  // (приток/отток денег за день/неделю/месяц), а не статичный график СЧА.
  const [viewMode, setViewMode] = usePersistedState<ViewMode>('frame:funds:viewMode', 'flows');
  const [flowTimeframe, setFlowTimeframe] = usePersistedState<FlowTimeframe>('frame:funds:flowTimeframe', '1w');
  const fundsAccess = useTierAccess('funds_money');
  const { showUpgrade } = useUpgradePrompt();

  // Tier-коррекция периода (только AUM — во flows вся фича целиком гейтится
  // тарифом, там нет валидного "отката", см. handleTierError в loadData ниже):
  // персистентный period мог остаться от прошлой авторизованной/Pro-сессии и
  // стать невалидным для текущего тарифа (гость/free/логаут). loadData сам НЕ
  // откатывает period при tier-403 — без этой коррекции график завис бы с
  // ошибкой навсегда. Тот же паттерн, что в FundsMoneyPage.tsx (desktop, AUM).
  const AUM_PERIODS: FundPeriod[] = ['1m', '1y', '3y', 'all'];
  useEffect(() => {
    if (fundsAccess.isLoading || viewMode !== 'aum') return;
    if (fundsAccess.canUsePeriod(period)) return;
    const allowed = AUM_PERIODS.filter(p => fundsAccess.canUsePeriod(p));
    if (allowed.length) setPeriod(allowed[allowed.length - 1]);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fundsAccess.isLoading, viewMode, period]);
  // Алерты в мессенджере — квота по тарифу (0=Free/гость → апселл, как у OI-колокола).
  const alertsQuota = useCommonFeatures().telegram_alerts_quota;
  const alertsLocked = alertsQuota === 0;
  const [fundAlertOpen, setFundAlertOpen] = useState(false);
  const [data, setData] = useState<FundsChartResponse | null>(null);
  const [flowsData, setFlowsData] = useState<FundsFlowsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [timeSheetOpen, setTimeSheetOpen] = useState(false);
  const [optionsSheetOpen, setOptionsSheetOpen] = useState(false);
  const [fundsSheetOpen, setFundsSheetOpen] = useState(false);
  // Hidden funds — Set fund_id'ов которые юзер скрыл из суммарной СЧА. Персистим
  // по категории (frame:funds:hidden:<category>, общий ключ с десктопом) — выбор
  // не сбрасывается на новой сессии и хранится отдельно для каждой категории;
  // смена категории перечитывает набор под новый ключ (заменяет прежний reset).
  const [hiddenFunds, setHiddenFunds] = usePersistedSet<number>(`frame:funds:hidden:${category}`);
  // Выбор ПОДМНОЖЕСТВА фондов — с Basic (funds_money.fund_picker, 2026-08-09).
  // Пока тариф не резолвнут — не запираем (как везде).
  const canPickFunds = fundsAccess.isLoading || fundsAccess.canUseFlag('fund_picker');
  // Запертый тариф: попытка изменить подвыборку (тоггл/массовые действия) —
  // апселл вместо изменения. true = действие перехвачено.
  const pickGuard = () => {
    if (canPickFunds) return false;
    showUpgrade({ tier: 'basic', featureName: 'выбор фондов', indicator: 'funds_money' });
    return true;
  };
  // Санитайз слетевшего с тарифа: ключ hiddenFunds общий с десктопом и
  // переживает окончание подписки — иначе free-юзер остался бы с подвыборкой,
  // которую бэкенд уже игнорирует.
  useEffect(() => {
    if (fundsAccess.isLoading || canPickFunds) return;
    if (hiddenFunds.size > 0) setHiddenFunds(new Set());
  }, [fundsAccess.isLoading, canPickFunds, hiddenFunds, setHiddenFunds]);
  // ?funds= из диплинка применяется один раз (после загрузки funds); флаг от повторов.
  const fundsFilterAppliedRef = useRef(false);
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
            Зажми палец, и появится перекрестье с точными значениями.
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
  // Диплинк ?funds= из Telegram-сигнала: показать ТОЛЬКО сигнальные фонды внутри
  // категории (остальные прячем через hiddenFunds). Один раз — после загрузки funds.
  useEffect(() => {
    if (fundsFilterAppliedRef.current || !data?.funds) return;
    const raw = searchParams.get('funds');
    if (!raw) return;
    fundsFilterAppliedRef.current = true;
    const want = new Set(raw.split(',').map((s) => parseInt(s, 10)).filter((n) => !isNaN(n)));
    if (want.size === 0) return;
    setHiddenFunds(new Set(accessibleFunds.filter((f) => !want.has(f.fund_id)).map((f) => f.fund_id)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data?.funds, accessibleFunds]);

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
  // Для flows фильтр внутри callback'а считаем из СВЕЖЕГО chartResult +
  // hiddenFundsRef: ref на visibleFundIds на первом заходе ещё undefined
  // (data не отрендерена) — flows уходил за ВСЕМИ фондами, бары анимировались
  // полной категорией и через секунду морфили на персистнутую подвыборку.
  const hiddenFundsRef = useRef(hiddenFunds);
  useEffect(() => { hiddenFundsRef.current = hiddenFunds; }, [hiddenFunds]);
  // Ключ последнего выданного flows-запроса — дедуп между loadData и
  // flows-refetch effect'ом (иначе после загрузки data effect повторил бы
  // тот же запрос из-за новой identity visibleFundIds).
  const lastFlowsKeyRef = useRef('');

  // Stale-guard от out-of-order гонки при быстром переключении категории/периода/
  // режима. ДВА счётчика (как desktop FundsMoneyPage):
  //  • dataReqIdRef — для setData (единственный писатель — loadData);
  //  • flowsReqIdRef — для setFlowsData (ДВА писателя: loadData в flows-режиме И
  //    flows-refetch effect ниже) → общий, чтобы они сериализовались между собой.
  // Разделять обязательно: один общий счётчик дал бы регрессию — bump flows-эффекта
  // помечал бы свежий setData loadData как stale → data (список фондов/AUM) не
  // обновлялся бы при смене категории (оба эффекта срабатывают на одну смену).
  const dataReqIdRef = useRef(0);
  const flowsReqIdRef = useRef(0);
  const loadData = useMemo(
    () => async () => {
      const dataId = ++dataReqIdRef.current;
      const flowsId = ++flowsReqIdRef.current;
      const dataStale = () => dataId !== dataReqIdRef.current;
      const flowsStale = () => flowsId !== flowsReqIdRef.current;
      try {
        setLoading(true);
        // data (FundsChartResponse) грузим ВСЕГДА — он несёт список фондов
        // и tier_locked-флаги, нужные для кнопки «Фонды» и lock-индикаторов
        // в funds-sheet. Раньше data грузился только в AUM-режиме → в flows
        // кнопка управления фондами оставалась скрытой пока не переключишься.
        const chartResult = await getFundsChartData(category, period);
        if (dataStale()) return;
        setData(chartResult);
        // flowsData — только в flows-режиме (для гистограммы притоков-оттоков).
        // flowsStale() до фетча: если refetch-effect уже перебил счётчик (маунт
        // с пустым hiddenFunds), наш запрос всё равно выкинут — не дублируем.
        if (viewMode === 'flows' && !flowsStale()) {
          const accessible = chartResult.funds.filter((f) => !f.tier_locked);
          const hidden = hiddenFundsRef.current;
          const visible = accessible.filter((f) => !hidden.has(f.fund_id));
          const ids = visible.length === accessible.length ? undefined : visible.map((f) => f.fund_id);
          lastFlowsKeyRef.current = `${category}|${flowTimeframe}|${period}|${ids?.join(',') ?? 'all'}`;
          const flowsResult = await getFundsFlows(category, flowTimeframe, period, ids);
          if (flowsStale()) return;
          setFlowsData(flowsResult);
        }
      } catch (err) {
        if (dataStale()) return;
        console.error('Ошибка funds:', err);
        handleTierError(err, {
          showUpgrade,
          indicator: 'funds_money',
          featureName: flowTimeframe === '1d' ? 'дневной таймфрейм' : 'индикатор «Деньги в фондах»',
        });
      } finally {
        if (!dataStale()) setLoading(false);
      }
    },
    [viewMode, category, period, flowTimeframe, showUpgrade],
  );

  useEffect(() => {
    void loadData();
  }, [loadData]);

  // Дополнительный refetch для flows-режима при toggle фондов.
  // Не зависит от loadData identity → не триггерит мигание в AUM режиме.
  // Есть персистнутые скрытые фонды, а список фондов ещё не загружен →
  // visibleFundIds не вычислить; не пускаем refetch-effect фетчить без фильтра
  // (флеш «всех фондов»). Первый запрос сделает loadData из свежего chartResult.
  const awaitingFundsList = hiddenFunds.size > 0 && !data?.funds;
  useEffect(() => {
    if (viewMode !== 'flows') return;
    if (awaitingFundsList) return;
    // Дедуп: тот же запрос уже выдан (loadData из chartResult, либо новая
    // identity visibleFundIds при том же составе) — не повторяем.
    const key = `${category}|${flowTimeframe}|${period}|${visibleFundIds?.join(',') ?? 'all'}`;
    if (key === lastFlowsKeyRef.current) return;
    lastFlowsKeyRef.current = key;
    // flowsReqIdRef ОБЩИЙ с loadData: оба пишут flowsData → сериализуем, иначе
    // медленный ранний запрос (старый visibleFundIds/категория) мог перезаписать
    // свежий результат. data НЕ трогаем — у неё свой dataReqIdRef.
    const reqId = ++flowsReqIdRef.current;
    const isStale = () => reqId !== flowsReqIdRef.current;
    void (async () => {
      try {
        const result = await getFundsFlows(category, flowTimeframe, period, visibleFundIds);
        if (isStale()) return;
        setFlowsData(result);
      } catch (err) {
        if (isStale()) return;
        // Tier-ошибка (403) → upgrade-модалка, как в primary loadData. Раньше
        // глоталось молча.
        if (!handleTierError(err, { showUpgrade, indicator: 'funds_money', featureName: 'притоки-оттоки фондов' })) {
          console.error('Flows refetch error:', err);
        }
      }
    })();
    // Зависимость от visibleFundIds — единственный триггер этого effect'а
    // в режиме flows. В режиме AUM этот effect возвращается early.
  }, [visibleFundIds, viewMode, category, flowTimeframe, period, showUpgrade, awaitingFundsList]);

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
        // СЧА — на ПРАВОЙ оси (свопнуто с индексом, как на десктопе/в эмбеде).
        axis: 'right' as const,
        formatValue: (v: number) => `${v.toFixed(0)}`,
      },
      ...(indexData.length > 0
        ? [{
            data: indexData,
            color: 'var(--chart-line-1, #5DA3E9)',
            label: data.index?.secid ?? 'Индекс',
            axis: 'left' as const,
            formatValue: (v: number) => v >= 1000 ? (v / 1000).toFixed(1) + 'K' : v.toFixed(0),
            formatAxis: (v: number) => v.toLocaleString('ru-RU', { maximumFractionDigits: 0 }),
          }]
        : []),
    ];
  }, [data, hiddenFunds]);

  const categoryLabel = CATEGORIES.find((c) => c.key === category)?.label ?? '';
  const periodLabel = PERIODS.find((p) => p.key === period)?.label ?? '';
  const optionsSummary = `${categoryLabel} · ${viewMode === 'aum' ? 'СЧА' : 'Притоки-Оттоки'}`;

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
        subtitle={`${categoryLabel} · ${viewMode === 'aum' ? 'СЧА' : 'Притоки-Оттоки'} · ${periodLabel}`}
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
              niceTicksLeft={true}
              niceTicksRight={true}
              gridAxis="right"
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
            // Глубокие периоды (3Г/Всё) для гостя/Free под замком: backend
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
            <div style={{ fontSize: 'var(--fs-xs)', fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
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
                    if (!soon) selectCategory(cat.key);
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
            <div style={{ fontSize: 'var(--fs-xs)', fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
              Что показать
            </div>
            <div style={{ display: 'flex', gap: 6 }}>
              {(['flows', 'aum'] as const).map((m) => (
                <button
                  key={m}
                  className={`fm-chip ${viewMode === m ? 'active' : ''}`}
                  onClick={() => setViewMode(m)}
                  style={{ flex: 1, justifyContent: 'center' }}
                >
                  {m === 'aum' ? 'СЧА' : 'Притоки-Оттоки'}
                </button>
              ))}
            </div>
          </div>

          {/* Таймфрейм для притоков */}
          {viewMode === 'flows' && (
            <div>
              <div style={{ fontSize: 'var(--fs-xs)', fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
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

          {/* Сигналы по фондам — рабочая кнопка (только в режиме притоков).
              Открывает CreateFundAlertModal БЕЗ привязки к текущей категории —
              фонды/категории выбираются внутри (дефолт — все фонды).
              Tier-гейт как у OI: quota=0 (Free/гость) → upgrade-промпт + замочек;
              иначе закрываем sheet и открываем модалку создания. */}
          {viewMode === 'flows' && (
            <div>
              <div style={{ fontSize: 'var(--fs-xs)', fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
                Сигналы
              </div>
              <button
                className="fm-chip"
                onClick={() => {
                  if (alertsLocked) {
                    showUpgrade({ tier: 'basic', featureName: 'Сигналы по фондам', indicator: 'alerts' });
                    return;
                  }
                  setOptionsSheetOpen(false);
                  setFundAlertOpen(true);
                }}
                aria-label={alertsLocked ? 'Сигналы по фондам — доступно на тарифе Basic и Pro' : 'Создать сигнал по фондам'}
                style={{
                  width: '100%',
                  justifyContent: 'center',
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 6,
                  opacity: alertsLocked ? 0.78 : 1,
                }}
              >
                <AlarmClock size={14} strokeWidth={2.2} />
                Сигнал по фондам
                {alertsLocked && <Lock size={11} strokeWidth={2.2} />}
              </button>
            </div>
          )}

          {/* Выбор видимых фондов — открывает отдельный sheet чтоб не
              перегружать основной options. Counter показывает только accessible
              (не locked) — locked-фонды для гостя/Free не идут в расчёт. */}
          {data && data.funds.length > 1 && (
            <button
              className="fm-chip"
              onClick={() => {
                // Гость/free: sheet открывается всегда (2026-08-10) — запертому
                // тиру список слегка заблюрен, поверх апселл на Basic.
                setOptionsSheetOpen(false);
                setFundsSheetOpen(true);
              }}
              style={{ justifyContent: 'space-between', padding: '14px 16px' }}
            >
              <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                Фонды
                {!canPickFunds && <Lock size={11} strokeWidth={2.2} />}
              </span>
              <span style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-base)' }}>
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
        {/* Locked-тир (funds_money.fund_picker): список открыт и читаем, но
            любая попытка изменить подвыборку открывает апселл на Basic
            (2026-08-10; раньше sheet не открывался вовсе). Гейт — pickGuard
            в обработчиках, без блюра. */}
        <div style={{ padding: '8px 16px 16px', display: 'flex', flexDirection: 'column', gap: 4 }}>
          {/* Quick-actions: все / никто */}
          <div style={{ display: 'flex', gap: 6, marginBottom: 8 }}>
            <button
              className="fm-chip"
              onClick={() => { if (pickGuard()) return; setHiddenFunds(new Set()); }}
              style={{ flex: 1, justifyContent: 'center' }}
            >
              Показать все
            </button>
            <button
              className="fm-chip"
              onClick={() => {
                if (pickGuard()) return;
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
            const logo = resolveFundLogo(f.ticker, f.uk_id);
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
                  if (pickGuard()) return;
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
                <span style={{ display: 'flex', alignItems: 'center', gap: 10, minWidth: 0 }}>
                  {/* Лого фонда (УК/автор) — как в десктоп FundsTable */}
                  <span
                    aria-hidden="true"
                    style={{
                      width: 28, height: 28, flexShrink: 0, borderRadius: '50%', overflow: 'hidden',
                      display: 'flex', alignItems: 'center', justifyContent: 'center',
                      fontWeight: 900, fontSize: 'var(--fs-sm)', lineHeight: 1,
                      backgroundColor: logo ? (logo.img ? undefined : logo.bg) : 'var(--bg-secondary)',
                      color: logo ? logo.color : 'var(--text-secondary)',
                    }}
                  >
                    {logo
                      ? (logo.img
                          ? <img src={logo.img} alt={logo.name} style={{ width: '100%', height: '100%', objectFit: 'cover' }} />
                          : logo.letter)
                      : (f.ticker.charAt(0) || '?')}
                  </span>
                  <span style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start', gap: 2, minWidth: 0 }}>
                    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6, fontWeight: 700, fontSize: 'var(--fs-base)' }}>
                      {f.ticker}
                      {locked && <Lock size={11} strokeWidth={2.2} />}
                    </span>
                    <span title={f.name} style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-muted)', textAlign: 'left', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 190 }}>
                      {stripUkName(f.name, f.uk_id)}
                    </span>
                  </span>
                </span>
                <span style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 2 }}>
                  <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 600 }}>
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

      {/* Конструктор сигнала по фондам — категория/фонды выбираются ВНУТРИ
          модалки (дефолт — все фонды), без привязки к текущей категории. */}
      {fundAlertOpen && (
        <CreateFundAlertModal
          onClose={() => setFundAlertOpen(false)}
        />
      )}

      <OnboardingTour
        steps={tourSteps}
        open={tour.open}
        onClose={tour.close}
      />
    </MobileLayout>
  );
}
