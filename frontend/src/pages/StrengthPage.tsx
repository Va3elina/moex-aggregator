import { useEffect, useLayoutEffect, useState, useMemo, useRef, useCallback } from 'react';
import { Activity } from 'lucide-react';
import ChartNavigator from '../components/ChartNavigator';
import PageHeader from '../components/PageHeader';
import { METHODOLOGY } from '../data/methodology';
import {
    getBreadthCurrent,
    getBreadthHistory,
    listAlerts,
    type BreadthCurrentResponse,
    type BreadthHistoryResponse,
    type BreadthUniverse,
    type AlertInfo,
} from '../services/api';
import AlertBellButton from '../components/alerts/AlertBellButton';
import CreateAlertModal, { type AlertMetricOption } from '../components/alerts/CreateAlertModal';
import { ALERTS_ENABLED } from '../config/alertsConfig';
import { useCommonFeatures, useTierAccess } from '../contexts/TierFeaturesContext';
import { useAuth } from '../contexts/AuthContext';
import { getDefaultPeriod } from '../config/accessControl';
import { useIndicatorData } from '../hooks/useIndicatorData';
import { usePersistedState } from '../hooks/usePersistedState';
import type { SyncedDataPoint, ChartPadding } from '../components/strength/chartUtils';
import IndexChart from '../components/strength/IndexChart';
import BreadthChart from '../components/strength/BreadthChart';
import ChartLegend from '../components/chart/ChartLegend';
import { TOOLTIP } from '../config/chartTheme';
import ChartCaptureButton from '../components/export/ChartCaptureButton';
import CsvExportButton from '../components/export/CsvExportButton';
import LayersButton from '../components/LayersButton';
import ChartActionsMenu from '../components/ChartActionsMenu';
import SandboxEntryButton from '../components/SandboxEntryButton';
import ChartSettings from '../components/chart/ChartSettings';
import { buildStrengthExportConfig } from '../components/export/exportConfigs';
import StrengthControls from '../components/strength/StrengthControls';
import DollarStaleHint from '../components/strength/DollarStaleHint';
import { computeChartTopLineY } from '../components/chart/datePillLayout';
import ChartDatePill from '../components/chart/ChartDatePill';
import { useOnboardingTour } from '../hooks/useFirstVisit';
import OnboardingTour from '../components/onboarding/OnboardingTour';
import { strengthTourSteps } from '../data/tours/strength';
import { useUpgradePrompt } from '../components/tier/UpgradeModal';

type Period = '1y' | '5y' | '10y' | '20y' | 'all';
type ChartMode = 'line' | 'histogram';

const PERIOD_DAYS: Record<Period, number> = {
    '1y': 365,
    '5y': 1825,
    '10y': 3650,
    '20y': 7300,
    'all': 9000
};

const EMA_PERIOD = 200; // Fixed EMA period

// Секторы строятся динамически из ответа API (поле sector)


// Дефолтные значения — используются на первом рендере до useLayoutEffect.
// Должны совпадать с --strength-pad-* в index.css :root (desktop breakpoint).
// Главное правило: ссылка на padding должна быть стабильной между рендерами —
// IndexChart/BreadthChart используют padding в useMemo-deps, пересоздание объекта
// сбрасывает chartData и прерывает морфинг-анимацию.
const DEFAULT_PADDING: ChartPadding = { left: 70, right: 70, top: 10, bottom: 30 };
const DEFAULT_HEIGHTS = { top: 300, bottomDual: 150, bottomSolo: 450 };

export default function StrengthPage() {
    const { isAuthenticated } = useAuth();
    // Настройки отображения персистятся в localStorage — не сбрасываются на новой сессии.
    const [period, setPeriod] = usePersistedState<Period>('frame:strength:period', getDefaultPeriod('1y', isAuthenticated) as Period);
    // EMA-период: 20 (очень краткосрок), 50 (краткосрок), 100 (среднесрок),
    // 200 (долгосрок, по умолчанию). Все доступны в pre-compute (breadth_history таблица).
    const [emaPeriod, setEmaPeriod] = usePersistedState<20 | 50 | 100 | 200>('frame:strength:emaPeriod', EMA_PERIOD);
    const [chartMode, setChartMode] = usePersistedState<ChartMode>('frame:strength:chartMode', 'histogram');
    const [showPrice, setShowPrice] = usePersistedState('frame:strength:showPrice', true);

    // Onboarding tour
    const tour = useOnboardingTour('strength');
    const [currency, setCurrency] = usePersistedState<'rub' | 'usd'>('frame:strength:currency', 'rub');
    const [universeBase, setUniverseBase] = usePersistedState<'all' | 'imoex'>('frame:strength:universeBase', 'imoex');
    // Итоговый universe: добавляем _usd при долларовом режиме
    const universe: BreadthUniverse = currency === 'usd'
        ? `${universeBase}_usd` as BreadthUniverse
        : universeBase;

    const { showUpgrade } = useUpgradePrompt();
    const strengthAccess = useTierAccess('strength');

    // Tier-коррекция периода: персистентный period мог остаться от прошлой
    // авторизованной/Pro-сессии (напр. '20y', добавлен вместе с '10y' 2026-07-22)
    // и стать невалидным для текущего тарифа (гость/free/логаут). useIndicatorData
    // сам НЕ откатывает period при tier-403 (только показывает upgrade-модалку
    // через handleTierError) — без этой коррекции график завис бы с ошибкой
    // навсегда. Тот же паттерн, что уже есть в OpenInterestPage.tsx и
    // FundsMoneyPage.tsx (AUM); баг без него — см. BuffettPage.tsx история #718/#810.
    const PERIOD_ORDER: Period[] = ['1y', '5y', '10y', '20y', 'all'];
    useEffect(() => {
        if (strengthAccess.isLoading) return;
        if (strengthAccess.canUsePeriod(period)) return;
        const allowed = PERIOD_ORDER.filter(p => strengthAccess.canUsePeriod(p));
        if (allowed.length) setPeriod(allowed[allowed.length - 1]);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [strengthAccess.isLoading, period]);

    // Данные через useIndicatorData: 2 параллельных fetch → единый объект
    // {current, history}; ниже разворачиваем в прежние имена (consumers не трогаем).
    // SSE ['daily','breadth'], tier-403 как было. loading=true на каждом старте
    // (хук всегда ставит) → даёт «Обновление…» при смене периода/EMA/валюты.
    const { data, loading, error } = useIndicatorData<{ current: BreadthCurrentResponse; history: BreadthHistoryResponse }>({
        fetcher: async () => {
            const [c, h] = await Promise.all([
                getBreadthCurrent(emaPeriod, universe),
                getBreadthHistory(emaPeriod, PERIOD_DAYS[period], universe),
            ]);
            return { current: c, history: h };
        },
        deps: [emaPeriod, period, universe, universeBase, currency],
        channels: ['daily', 'breadth'],
        errorMessage: 'Не удалось загрузить данные',
        tier: {
            showUpgrade,
            indicator: 'strength',
            featureName: universeBase === 'all' ? 'вселенная «100 акций»' :
                currency === 'usd' ? 'долларовый режим' : 'индикатор «Сила рынка»',
        },
    });
    const current = data?.current ?? null;
    const history = data?.history ?? null;

    // Долларовый ряд отстаёт от рублёвого (РТС и курс не торгуются на выходных и в
    // нерабочие дни) → маркеры «доллар не обновляется». Только в USD-режиме.
    const dollarStale = currency === 'usd' && !!history?.dollar_stale;

    // Синхронизированный hover между графиками
    const [hoverIndex, setHoverIndex] = useState<number | null>(null);
    const [hoverY, setHoverY] = useState<number>(0);

    const containerRef = useRef<HTMLDivElement>(null);
    const mouseMoveRaf = useRef<number | null>(null);
    const isNavDragRef = useRef(false);

    // Читаем размеры из CSS-токенов один раз на маунт + на resize при смене breakpoint.
    // setState с equality-check: новая ссылка создаётся ТОЛЬКО при реальном изменении
    // значений → morph-анимация в IndexChart/BreadthChart не сбрасывается.
    const [padding, setPadding] = useState<ChartPadding>(DEFAULT_PADDING);
    const [heights, setHeights] = useState(DEFAULT_HEIGHTS);
    useLayoutEffect(() => {
        const readTokens = () => {
            const cs = getComputedStyle(document.documentElement);
            const num = (name: string, fb: number) =>
                parseFloat(cs.getPropertyValue(name)) || fb;
            const nextPad: ChartPadding = {
                left: num('--strength-pad-left', DEFAULT_PADDING.left),
                // +место в правом жёлобе под «+» алерт-пилюлю (значение+кружок) —
                // иначе она не влезает и уезжает в график.
                right: num('--strength-pad-right', DEFAULT_PADDING.right) + (ALERTS_ENABLED ? 28 : 0),
                top: num('--strength-pad-top', DEFAULT_PADDING.top),
                bottom: num('--strength-pad-bottom', DEFAULT_PADDING.bottom),
            };
            const nextH = {
                top: num('--strength-chart-top-height', DEFAULT_HEIGHTS.top),
                bottomDual: num('--strength-chart-bottom-height', DEFAULT_HEIGHTS.bottomDual),
                bottomSolo: num('--strength-chart-solo-height', DEFAULT_HEIGHTS.bottomSolo),
            };
            setPadding(prev =>
                prev.left === nextPad.left && prev.right === nextPad.right &&
                prev.top === nextPad.top && prev.bottom === nextPad.bottom
                    ? prev : nextPad
            );
            setHeights(prev =>
                prev.top === nextH.top && prev.bottomDual === nextH.bottomDual &&
                prev.bottomSolo === nextH.bottomSolo
                    ? prev : nextH
            );
        };
        readTokens();
        window.addEventListener('resize', readTokens);
        return () => window.removeEventListener('resize', readTokens);
    }, []);


    // Лейбл верхнего графика (price chart). Всегда показывает индекс, который
    // фактически отрисован: IMOEX в рублях или RTS в долларах. От universeBase
    // (выбора набора акций для breadth-метрики на нижнем графике) НЕ зависит —
    // верхний график одинаков во всех режимах, и label должен это отражать.
    const priceChartLabel = currency === 'usd' ? 'Индекс RTS' : 'Индекс IMOEX';
    // Короткий лейбл для hover tooltip (где места меньше)
    const priceChartShort = currency === 'usd' ? 'RTS' : 'IMOEX';

    // Данные для графиков
    const breadthData = useMemo(() => {
        if (!history?.data) return [];
        return history.data.map(point => ({
            time: point.date,
            value: point.percent_above
        }));
    }, [history]);

    const imoexData = useMemo(() => {
        if (!history?.imoex) return [];
        return history.imoex.map(point => ({
            time: point.date,
            value: point.close
        }));
    }, [history]);

    // Синхронизированные данные — breadth + IMOEX по датам
    const syncedData = useMemo(() => {
        if (!breadthData.length || !imoexData.length) return [];

        const imoexMap = new Map(imoexData.map(d => [d.time, d.value]));
        const full = breadthData
            .filter(d => imoexMap.has(d.time))
            .map(d => ({
                time: d.time,
                breadth: d.value,
                imoex: imoexMap.get(d.time)!,
            }));

        return full;
    }, [breadthData, imoexData]);

    // Навигатор временного диапазона.
    // useLayoutEffect (не useEffect) чтобы успеть обновить range ДО первого paint —
    // иначе первый кадр рендерится с [0,0] и ChartData пустой (видимый "flash").
    // Та же pitfall что исправлена в FundsMoneyPage при рефакторинге FlowsHistogram.
    const [navRange, setNavRange] = useState<[number, number]>([0, 0]);
    useLayoutEffect(() => {
        setNavRange([0, Math.max(0, syncedData.length - 1)]);
    }, [syncedData.length, syncedData[0]?.time]);

    // Блокировка hover во время начальной анимации (500–600мс) чтобы не прерывать fade-in
    const [isAnimating, setIsAnimating] = useState(false);
    const animTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    useEffect(() => {
        if (syncedData.length > 0) {
            setIsAnimating(true);
            if (animTimerRef.current) clearTimeout(animTimerRef.current);
            animTimerRef.current = setTimeout(() => setIsAnimating(false), 700);
        }
        return () => { if (animTimerRef.current) clearTimeout(animTimerRef.current); };
    }, [syncedData[0]?.time, syncedData.length]);

    const displaySyncedData: SyncedDataPoint[] = useMemo(() => {
        if (!syncedData.length) return syncedData;
        return syncedData.slice(navRange[0], navRange[1] + 1);
    }, [syncedData, navRange]);

    // Данные для мини-графика навигатора (мемоизированы — стабильная ссылка)
    const navigatorData = useMemo(
        () => syncedData.map(d => ({ time: d.time, value: d.breadth })),
        [syncedData]
    );

    // ── Алерты на графике Силы рынка: level-алерт на % акций выше EMA ───────────
    const alertQuota = useCommonFeatures().telegram_alerts_quota;
    const alertsLocked = alertQuota === 0;
    const [chartAlertPrefill, setChartAlertPrefill] = useState<{ metricKey: string; threshold: number; currentLabel?: string } | null>(null);
    const [myAlerts, setMyAlerts] = useState<AlertInfo[]>([]);
    const reloadMyAlerts = useCallback(() => {
        if (!ALERTS_ENABLED || alertsLocked) { setMyAlerts([]); return; }
        listAlerts({ limit: 200 }).then((p) => setMyAlerts(p.items)).catch(() => setMyAlerts([]));
    }, [alertsLocked]);
    useEffect(() => { reloadMyAlerts(); }, [reloadMyAlerts]);

    // Метрика зависит от текущего вида (период EMA + вселенная). asset = вселенная,
    // metric = период EMA. Общий реестр для кнопки-будильника и «+».
    const strengthMetrics = useMemo<AlertMetricOption[]>(() => [{
        key: 'strength_level',
        label: `Сила рынка — % акций выше EMA${emaPeriod}`,
        indicator: 'strength_level', metric: String(emaPeriod), unit: '%',
        ops: [
            { value: 'cross', label: 'Пересечение (в любую сторону)' },
            { value: 'cross_up', label: '↑ Пересечение (снизу вверх)' },
            { value: 'cross_down', label: '↓ Пересечение (сверху вниз)' },
        ],
        hint: `Сработает, когда доля акций выше EMA${emaPeriod} пересечёт заданный уровень в %.`,
    }], [emaPeriod]);

    // Уровни активных strength-алертов текущего вида (EMA + вселенная) → пунктир.
    const alertLevels = useMemo(() => {
        if (!ALERTS_ENABLED) return undefined;
        const lines = myAlerts
            .filter((a) => a.status === 'active' && a.indicator === 'strength_level'
                && a.metric === String(emaPeriod) && a.asset === universe)
            .map((a) => a.threshold);
        return lines.length ? lines : undefined;
    }, [myAlerts, emaPeriod, universe]);

    // Клик «+» на оси % → модалка (уровень + текущее значение).
    const handleCreateAlertFromChart = useCallback((levelPct: number, currentPct: number) => {
        if (alertsLocked) { showUpgrade({ tier: 'basic', featureName: 'Уведомления', indicator: 'alerts' }); return; }
        setChartAlertPrefill({ metricKey: 'strength_level', threshold: Math.round(levelPct * 10) / 10, currentLabel: `${currentPct.toFixed(1)}%` });
    }, [alertsLocked, showUpgrade]);

    // Extracted pointer handler — общая логика для mouse + touch.
    // Раньше вся логика была inline в handleMouseMove → touch не работал.
    // Теперь mouse и touch handlers вызывают этот общий путь с координатами.
    const updateHover = useCallback((clientX: number, clientY: number) => {
        if (!containerRef.current || !displaySyncedData.length) return;
        if (mouseMoveRaf.current !== null) return; // throttle до одного RAF

        mouseMoveRaf.current = requestAnimationFrame(() => {
            mouseMoveRaf.current = null;
            if (!containerRef.current) return;
            const rect = containerRef.current.getBoundingClientRect();
            const px4 = 16; // px-4 на внутренних div-обёртках графиков
            const x = clientX - rect.left - px4 - padding.left;
            const y = clientY - rect.top;
            const chartWidth = rect.width - 2 * px4 - padding.left - padding.right;
            // В жёлобе оси (курсор вне plot по X) прячем вертикаль + карточку — как
            // на ОИ. Горизонталь + «+» пилюлю рисует BreadthChart по своему cursorY.
            if (x < 0 || x > chartWidth) {
                setHoverIndex(null);
                return;
            }
            const idx = Math.round((x / chartWidth) * (displaySyncedData.length - 1));
            if (idx >= 0 && idx < displaySyncedData.length) {
                setHoverIndex(idx);
                setHoverY(y);
            }
        });
    }, [displaySyncedData.length, padding.left, padding.right]);

    const handleMouseMove = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
        updateHover(e.clientX, e.clientY);
    }, [updateHover]);

    const handleTouchStart = useCallback((e: React.TouchEvent<HTMLDivElement>) => {
        if (e.touches[0]) updateHover(e.touches[0].clientX, e.touches[0].clientY);
    }, [updateHover]);

    const handleTouchMove = useCallback((e: React.TouchEvent<HTMLDivElement>) => {
        if (e.touches[0]) updateHover(e.touches[0].clientX, e.touches[0].clientY);
    }, [updateHover]);

    const handleTouchEnd = useCallback(() => {
        setHoverIndex(null);
    }, []);

    const handleMouseLeave = useCallback(() => {
        if (mouseMoveRaf.current !== null) {
            cancelAnimationFrame(mouseMoveRaf.current);
            mouseMoveRaf.current = null;
        }
        setHoverIndex(null);
    }, []);

    // Текущие значения для тултипа
    const hoverData = hoverIndex !== null && displaySyncedData[hoverIndex] ? displaySyncedData[hoverIndex] : null;

    return (
        <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8 min-h-screen">
            <PageHeader
                icon={Activity}
                title="Сила рынка"
                subtitle={`% ${universe === 'imoex' ? 'акций индекса MOEX' : 'акций'} выше EMA${emaPeriod}`}
                help={METHODOLOGY.strength}
                helpLink="/methodology/strength"
                sourceNote="Индекс IMOEX/RTSI: ПАО Московская Биржа"
            />

            {/* Editorial frame — обнимает controls + chart в один контейнер */}
            <div className="editorial-frame">

            {/* Контролы — одна строка. Camera button передаётся как trailingSlot
                чтобы стоять inline с classification chip (не накладываться). */}
            <div data-tour="strength-controls">
            <StrengthControls
                period={period}
                onPeriodChange={setPeriod}
                universeBase={universeBase}
                onUniverseBaseChange={setUniverseBase}
                currency={currency}
                onCurrencyChange={setCurrency}
                dollarStale={dollarStale}
                emaPeriod={emaPeriod}
                onEmaPeriodChange={setEmaPeriod}
                trailingSlot={
                    <>
                    <ChartActionsMenu containerRef={containerRef} tourId="strength-layers">
                    <LayersButton
                        tourId="strength-layers"
                        layers={[
                            { key: 'price', label: 'Индекс', hint: priceChartLabel, checked: showPrice, onChange: setShowPrice },
                            { key: 'histogram', label: 'Гистограмма', hint: 'Столбики вместо линии', checked: chartMode === 'histogram', onChange: (v: boolean) => setChartMode(v ? 'histogram' : 'line') },
                        ]}
                    />
                    <CsvExportButton
                        indicator="strength"
                        config={() => buildStrengthExportConfig({
                            emaPeriod,
                            universe,
                            period,
                            periodDays: PERIOD_DAYS[period],
                        })}
                    />
                    <ChartCaptureButton
                        getTargetElement={() => containerRef.current}
                        filename={`frame-strength-${universe}-ema${emaPeriod}-${period}`}
                        metadata={{
                            title: 'Сила рынка',
                            asset: priceChartLabel,
                            details: [
                                `EMA${emaPeriod}`,
                                period === '1y' ? '1 год' :
                                period === '5y' ? '5 лет' :
                                period === '10y' ? '10 лет' :
                                period === '20y' ? '20 лет' : 'Всё',
                                currency === 'usd' ? 'USD' : 'RUB',
                                chartMode === 'histogram' ? 'Гистограмма' : 'Линия',
                            ].filter(Boolean),
                        }}
                    />
                    <ChartSettings showType={false} />
                    {ALERTS_ENABLED && (
                        <AlertBellButton
                            indicator="strength"
                            asset={universe}
                            assetName="Сила рынка"
                            metrics={strengthMetrics}
                        />
                    )}
                    </ChartActionsMenu>

                    {/* Вход в песочницу — крайняя справа в строке контролов
                        (единая позиция на всех индикаторах). ChartActionsMenu
                        уходит порталом в угол графика и место здесь не занимает. */}
                    <div style={{ marginLeft: 'auto', order: 99 }}>
                        <SandboxEntryButton />
                    </div>
                    </>
                }
            />
            </div>{/* /strength-controls */}

            {/* Синхронизированные графики — оба в одном paper-контейнере, 1.5px outline */}
            <div
                ref={containerRef}
                data-tour="strength-chart"
                className="relative cursor-crosshair overflow-hidden rounded-2xl bg-theme-primary"
                style={{
                    minHeight: 'var(--chart-height, 500px)',
                    // Editorial inkstroke: 2px на chart cards (как в /oi, /funds-money).
                    // 1.5px зарезервирован для chips/buttons — иерархия по толщине рамки.
                    border: '2px solid var(--text-primary)',
                    // touchAction: none — отключает scroll/zoom при tap-and-drag по
                    // chart, иначе мобильный браузер ловит touch как scroll-gesture
                    // и crosshair не follows finger.
                    touchAction: 'none',
                }}
                onMouseMove={isAnimating ? undefined : handleMouseMove}
                onMouseLeave={handleMouseLeave}
                onTouchStart={isAnimating ? undefined : handleTouchStart}
                onTouchMove={isAnimating ? undefined : handleTouchMove}
                onTouchEnd={handleTouchEnd}
            >
                {dollarStale && (
                    <DollarStaleHint
                        style={{ position: 'absolute', top: 12, left: 16, zIndex: 30 }}
                    />
                )}
                {/* Полный loading / error на месте графика */}
                {loading && !current ? (
                    <div className="flex items-center justify-center" style={{ height: (showPrice ? heights.top + 16 : 0) + (showPrice ? heights.bottomDual : heights.bottomSolo) + 16 + 68 }}>
                        <div className="flex flex-col items-center gap-3">
                            <div className="w-8 h-8 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
                            <span className="text-theme-secondary">Загрузка...</span>
                        </div>
                    </div>
                ) : error && !current ? (
                    <div className="flex items-center justify-center" style={{ height: (showPrice ? heights.top + 16 : 0) + (showPrice ? heights.bottomDual : heights.bottomSolo) + 16 + 68 }}>
                        <div className="text-center">
                            <Activity className="w-12 h-12 text-red-400 mx-auto mb-3" />
                            <p className="text-red-400">{error}</p>
                        </div>
                    </div>
                ) : (<>
                    {/* Индикатор обновления данных — paper-style без glass */}
                    {loading && syncedData.length > 0 && (
                        <div
                            className="absolute top-3 left-4 z-20 flex items-center rounded-lg border border-theme shadow-md"
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

                    {/* Тултип-карточка при hover — следует за мышью между графиками */}
                    {hoverData && hoverIndex !== null && containerRef.current && (() => {
                        const rect = containerRef.current!.getBoundingClientRect();
                        const px4 = 16; // px-4 на внутренних div-обёртках графиков
                        const chartWidth = rect.width - 2 * px4 - padding.left - padding.right;
                        const hoverX = px4 + padding.left + (hoverIndex / Math.max(displaySyncedData.length - 1, 1)) * chartWidth;
                        const isRightHalf = hoverX > px4 + padding.left + chartWidth / 2;
                        // Fluid tooltip card width: 37% от viewport, clamped в [140, 190].
                        // На 375vw → 140 (clamped to floor), 768vw → 142, 1280vw → 190.
                        // Раньше был binary jump 140↔190 на 768px breakpoint без
                        // интерполяции — discrete UX hop при resize окна.
                        const vw = typeof window !== 'undefined' ? window.innerWidth : 1280;
                        const cardWidth = Math.max(140, Math.min(190, Math.round(vw * 0.37)));
                        const cardLeft = isRightHalf ? hoverX - cardWidth - 12 : hoverX + 12;

                        const dateStr = new Date(hoverData.time).toLocaleDateString('ru-RU', {
                            day: 'numeric', month: 'short', year: 'numeric'
                        });

                        const bv = hoverData.breadth;
                        const bt = Math.max(0, Math.min(bv / 100, 1));
                        const breadthColor = bt <= 0.35
                            ? `rgb(${Math.round(239-bt/0.35*30)},${Math.round(68+bt/0.35*60)},${Math.round(68-bt/0.35*40)})`
                            : bt <= 0.65
                            ? `rgb(${Math.round(209-(bt-0.35)/0.3*170)},${Math.round(128+(bt-0.35)/0.3*69)},${Math.round(28+(bt-0.35)/0.3*66)})`
                            : `rgb(${Math.round(39-(bt-0.65)/0.35*5)},${Math.round(197+(bt-0.65)/0.35*3)},94)`;

                        // Карточка ~60px высотой, ограничиваем в пределах контейнера
                        const cardHeight = showPrice ? 60 : 34;
                        const containerHeight = rect.height;
                        const clampedCardTop = Math.min(Math.max(hoverY - cardHeight / 2, 4), containerHeight - cardHeight - 4);

                        return (
                            <>
                                {/* Дата — по середине вертикальной пунктирной линии.
                                    Линия идёт через два chart-wrapper'а (top + bottom), не считая навигатора.
                                    Считаем по minHeight каждого wrapper'а:
                                      top wrapper:    heights.top + 34
                                      bottom wrapper: (showPrice ? bottomDual : bottomSolo) + 24
                                    Middle = (top + bottom) / 2 от верха containerRef. */}
                                {(() => {
                                    // Единая логика date-pill через computeChartTopLineY.
                                    // Helper находит первый .chart-plot внутри containerRef и считает y верхней линии.
                                    const topLineY = computeChartTopLineY({
                                        container: containerRef.current,
                                        paddingTop: padding.top,
                                    });
                                    return (
                                <ChartDatePill
                                    date={dateStr}
                                    x={hoverX}
                                    topLineY={topLineY}
                                    minX={px4 + padding.left}
                                    maxX={px4 + padding.left + chartWidth}
                                />
                                    );
                                })()}

                                {/* Карточка значений — следует за курсором */}
                                <div
                                    className="absolute z-30 pointer-events-none"
                                    style={{
                                        left: cardLeft,
                                        top: clampedCardTop,
                                    }}
                                >
                                    <div className={TOOLTIP.containerClass} style={TOOLTIP.containerStyle}>
                                        {showPrice && (
                                            <div className="flex items-center justify-between gap-3 py-0.5">
                                                <div className="flex items-center gap-1.5">
                                                    <span className={TOOLTIP.dotClass} style={{ ...TOOLTIP.dotStyle, backgroundColor: 'var(--accent)' }} />
                                                    <span className={TOOLTIP.labelClass} style={TOOLTIP.labelStyle}>{priceChartShort}</span>
                                                </div>
                                                <span className={`${TOOLTIP.valueClass} text-theme-primary`} style={TOOLTIP.valueStyle}>
                                                    {hoverData.imoex.toLocaleString('ru-RU', { maximumFractionDigits: 0 })}
                                                </span>
                                            </div>
                                        )}
                                        <div className="flex items-center justify-between gap-3 py-0.5">
                                            <div className="flex items-center gap-1.5">
                                                <span className={TOOLTIP.dotClass} style={{ ...TOOLTIP.dotStyle, backgroundColor: breadthColor }} />
                                                <span className={TOOLTIP.labelClass} style={TOOLTIP.labelStyle}>% выше EMA</span>
                                            </div>
                                            <span className={`${TOOLTIP.valueClass} text-theme-primary`} style={TOOLTIP.valueStyle}>
                                                {hoverData.breadth.toFixed(1)}%
                                            </span>
                                        </div>
                                    </div>
                                </div>
                            </>
                        );
                    })()}

                    {/* График IMOEX (верхний).
                        minHeight = height SVG + высота блока заголовка (legend ~14 + mb 2) ≈ 16px.
                        paddingTop/marginBottom легенды — единые токены геометрии
                        (--chart-legend-top-gap / --chart-legend-mb), как в SimpleChart.
                        Mobile: px-1 (4px) вместо px-4 (16px) — освобождаем 24px для графика. */}
                    {showPrice && (
                        <div className="px-1 md:px-4 pb-1 border-b border-theme relative overflow-hidden"
                             style={{ minHeight: heights.top + 16, paddingTop: 'var(--chart-legend-top-gap, 8px)' }}>
                            <div className="flex items-center justify-center relative z-10" style={{ marginBottom: 'var(--chart-legend-mb, 2px)' }}>
                                <ChartLegend
                                    items={[{ color: 'var(--accent)', label: priceChartLabel }]}
                                    fontWeight={600}
                                    style={{ color: 'var(--text-primary)' }}
                                />
                            </div>
                            <IndexChart
                                syncedData={displaySyncedData}
                                hoverIndex={hoverIndex}
                                height={heights.top}
                                padding={padding}
                                isNavDragRef={isNavDragRef}
                            />
                        </div>
                    )}

                    {/* График Breadth (нижний) — расширяется когда IMOEX скрыт.
                        minHeight = height SVG + высота блока заголовка (legend ~14 + mb 2) ≈ 16px.
                        Mobile: px-1 (4px) — стрейчим график влево. */}
                    <div className="px-1 md:px-4 pt-2 pb-1 relative overflow-hidden"
                         style={{ minHeight: (showPrice ? heights.bottomDual : heights.bottomSolo) + 16 }}>
                        <div className="flex items-center justify-center relative z-10" style={{ marginBottom: 'var(--chart-legend-mb, 2px)' }}>
                            <ChartLegend
                                items={[{ color: 'var(--accent)', label: `% акций выше EMA${emaPeriod}` }]}
                                fontWeight={600}
                                style={{ color: 'var(--text-primary)' }}
                            />
                        </div>
                        {displaySyncedData.length > 0 ? (
                            <BreadthChart
                                syncedData={displaySyncedData}
                                hoverIndex={hoverIndex}
                                height={showPrice ? heights.bottomDual : heights.bottomSolo}
                                mode={chartMode}
                                padding={padding}
                                isNavDragRef={isNavDragRef}
                                onCreateAlert={ALERTS_ENABLED ? handleCreateAlertFromChart : undefined}
                                alertLevels={alertLevels}
                                showWatermark={!showPrice}
                            />
                        ) : (
                            <div className="h-48 flex items-center justify-center text-theme-muted">
                                Нет данных для отображения
                            </div>
                        )}
                    </div>

                    {/* Навигатор временного диапазона */}
                    {syncedData.length > 0 && (
                        <div
                            data-export-ignore="true"
                            className="px-4 pb-3 strength-nav"
                            onMouseMove={e => e.stopPropagation()}
                            onMouseEnter={() => setHoverIndex(null)}
                        >
                            <ChartNavigator
                                data={navigatorData}
                                onChange={(s, e, isDrag) => { isNavDragRef.current = isDrag; setNavRange([s, e]); }}
                                color="var(--accent)"
                                insetLeft={padding.left}
                                insetRight={padding.right}
                            />
                        </div>
                    )}
                </>)}
            </div>

            </div>{/* /editorial-frame */}

            <OnboardingTour
                steps={strengthTourSteps}
                open={tour.open}
                onClose={tour.close}
            />

            {/* Модалка алерта из «+» на оси % (префилл: уровень + текущее). */}
            {ALERTS_ENABLED && chartAlertPrefill && (
                <CreateAlertModal
                    indicator="strength"
                    asset={universe}
                    assetName="Сила рынка"
                    metrics={strengthMetrics}
                    prefill={{ metricKey: chartAlertPrefill.metricKey, threshold: chartAlertPrefill.threshold, currentLabel: chartAlertPrefill.currentLabel }}
                    onClose={() => { setChartAlertPrefill(null); reloadMyAlerts(); }}
                />
            )}
        </div>
    );
}
