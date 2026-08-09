import { useEffect, useState, useMemo, useCallback, useRef } from 'react';
import { Scale } from 'lucide-react';
import PageHeader from '../components/PageHeader';
import { METHODOLOGY } from '../data/methodology';
import {
    getBuffettCapGdp,
    getBuffettCapM2,
    type BuffettCapGdpResponse,
    type BuffettRatioResponse,
    type BuffettPeriod,
} from '../services/api';
import SimpleChart from '../components/SimpleChart';
import ChartCaptureButton from '../components/export/ChartCaptureButton';
import CsvExportButton from '../components/export/CsvExportButton';
import { periodToQuery } from '../utils/csvPeriod';
import Dropdown, { type DropdownOption } from '../components/Dropdown';
import SegmentedControl from '../components/SegmentedControl';
import HelpTooltip from '../components/HelpTooltip';
import LayersButton from '../components/LayersButton';
import ChartActionsMenu from '../components/ChartActionsMenu';
import ChartSettings from '../components/chart/ChartSettings';
import { useRealtimeData } from '../hooks/useRealtimeData';
import { usePersistedState } from '../hooks/usePersistedState';
import { useFitToViewport } from '../hooks/useFitToViewport';
import { useIsMobile } from '../hooks/useIsMobile';
import { useOnboardingTour } from '../hooks/useFirstVisit';
import OnboardingTour from '../components/onboarding/OnboardingTour';
import { buffettTourSteps } from '../data/tours/buffett';
import { useTierAccess, useCommonFeatures } from '../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../components/tier/UpgradeModal';
import { handleTierError } from '../utils/tierError';
import AlertBellButton from '../components/alerts/AlertBellButton';
import CreateAlertModal, { type AlertMetricOption } from '../components/alerts/CreateAlertModal';
import { ALERTS_ENABLED } from '../config/alertsConfig';
import { listAlerts, type AlertInfo } from '../services/api';

type ViewMode = 'cap-gdp' | 'cap-m2';

/** Прогноз Кап/ВВП на сайте: временно скрыт (см. блок с Dropdown ниже). */
const SHOW_FORECAST = false;

const PERIOD_LABELS: Partial<Record<BuffettPeriod, string>> = {
    '5y': '5Л',
    '10y': '10Л',
    '20y': '20Л',
    'all': 'Всё',
};

export default function BuffettPage() {
    const isMobile = useIsMobile();
    // Настройки отображения персистятся в localStorage — не сбрасываются на новой сессии.
    const [viewMode, setViewMode] = usePersistedState<ViewMode>('frame:buffett:viewMode', 'cap-gdp');
    const buffAccess = useTierAccess('buffett');
    const { showUpgrade } = useUpgradePrompt();
    const [period, setPeriod] = usePersistedState<BuffettPeriod>('frame:buffett:period', '10y');
    // Показывать капитализацию (secondary axis) — toggle для пользователя
    // если хочет видеть только ratio Кап/ВВП или Кап/M2 без контекста размера.
    const [showCap, setShowCap] = usePersistedState('frame:buffett:showCap', true);
    const smooth = false;
    const [timeframe, setTimeframe] = usePersistedState<'1d' | '1w' | '1m'>('frame:buffett:timeframe', '1m');
    const [forecastTarget, setForecastTarget] = usePersistedState<number | null>('frame:buffett:forecastTarget', null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [capGdpData, setCapGdpData] = useState<BuffettCapGdpResponse | null>(null);
    const [capM2Data, setCapM2Data] = useState<BuffettRatioResponse | null>(null);

    // Onboarding tour
    const tour = useOnboardingTour('buffett');

    // Динамическая высота графика — chartAnchorRef как в OI/Funds-Money
    const chartAnchorRef = useRef<HTMLDivElement>(null);
    const chartHeight = useFitToViewport(chartAnchorRef, {
        min: 360,
        max: 720,
        bottomBuffer: 64,
    });

    // Stale-guard: при быстром переключении period/mode медленный ранний ответ
    // мог перезаписать свежий. reqId фиксирует «последний» запрос — устаревший
    // не применяет setState (тот же паттерн, что в эталонном useIndicatorData).
    const reqIdRef = useRef(0);
    // Последний период, по которому данные успешно загрузились — тот же паттерн,
    // что в MobileBuffettPage.tsx (lastGoodPeriod). Персистентный дефолт '10y'
    // невалиден для guest/free (max_history_days=5y) — без отката period навсегда
    // остаётся невалидным: каждый повтор падает 403, апсейл всплывает заново,
    // график остаётся пустым («Нет данных для отображения»).
    const lastGoodPeriod = useRef<BuffettPeriod>('1y');
    // true пока страница смонтирована. ResponsiveRoute может размонтировать эту
    // страницу в пользу мобильной (флип определения телефона на старте) ПОКА
    // fetch ещё в полёте — showUpgrade живёт в App-level контексте и не гасится
    // unmount'ом конкретной страницы, поэтому без guard'а модалка всплыла бы
    // поверх уже другой (мобильной) страницы. См. тот же паттерн в MobileBuffettPage.
    const isMountedRef = useRef(true);
    useEffect(() => {
        isMountedRef.current = true;
        return () => { isMountedRef.current = false; };
    }, []);

    // Загрузка данных
    const loadData = useCallback(async () => {
        const reqId = ++reqIdRef.current;
        const isStale = () => reqId !== reqIdRef.current;
        try {
            setLoading(true);
            setError(null);
            if (viewMode === 'cap-gdp') {
                const result = await getBuffettCapGdp(period, smooth, timeframe);
                if (isStale()) return;
                setCapGdpData(result);
            } else {
                const result = await getBuffettCapM2(period, smooth, timeframe);
                if (isStale()) return;
                setCapM2Data(result);
            }
            lastGoodPeriod.current = period;
        } catch (err: unknown) {
            if (isStale()) return;
            if (!isMountedRef.current) return;
            const msg = err instanceof Error ? err.message : '';
            // При 403 (гость или протухший токен) — фолбэк на 1y
            if (msg.includes('авторизац') && period !== '1y') {
                setPeriod('1y');
                return;
            }
            // Ограничение глубины истории (напр. персистентный '10y' невалиден
            // для guest/free) — откатываемся на последний рабочий период, иначе
            // график завис бы пустым, а период — «10Л»/«20Л»/«Всё» навсегда.
            if (msg.includes('Период') && msg.includes('недоступ') && period !== lastGoodPeriod.current) {
                setPeriod(lastGoodPeriod.current);
                showUpgrade({
                    tier: 'pro',
                    featureName: `период «${PERIOD_LABELS[period] ?? period}»`,
                    indicator: 'buffett',
                });
                return;
            }
            // Tier-related 403 → upgrade modal, не destructive error
            if (!handleTierError(err, {
                showUpgrade,
                indicator: 'buffett',
                featureName: viewMode === 'cap-m2' ? 'режим «Кап / M2»' : 'индикатор Баффетта',
                onTier: () => setError(null),
            })) {
                setError('Ошибка загрузки данных');
            }
            console.error(err);
        } finally {
            if (!isStale()) setLoading(false);
        }
    }, [period, smooth, viewMode, timeframe, showUpgrade]);

    useEffect(() => { loadData(); }, [loadData]);

    // SSE: автоматическое обновление
    useRealtimeData(['daily', 'buffett'], loadData);

    // Подготовка данных для SimpleChart: Cap/GDP (с прогнозом)
    const capGdpChartData = useMemo(() => {
        if (!capGdpData?.data?.length) return { primary: [], secondary: [] };

        const primary = capGdpData.data.map(d => ({ time: d.date, value: d.buffett }));
        const secondary = capGdpData.data.map(d => ({ time: d.date, value: d.cap }));

        if (forecastTarget !== null) {
            const last = capGdpData.data[capGdpData.data.length - 1];
            if (last.gdp_ttm > 0) {
                const targetCap = last.gdp_ttm * forecastTarget / 100;
                const currentRatio = last.buffett;
                const currentCap = last.cap;
                const lastDate = new Date(last.date);
                // 12 промежуточных точек за 12 месяцев с хаотичным шумом
                const steps = 12;
                const ratioDiff = forecastTarget - currentRatio;
                const capDiff = targetCap - currentCap;
                // Разный шум для ratio и cap (разные seed)
                const seedR = forecastTarget * 7 + 13;
                const seedC = forecastTarget * 11 + 37;
                for (let i = 1; i <= steps; i++) {
                    const t = i / steps;
                    const noiseR = Math.sin(seedR * i * 0.7) * 0.2 * (1 - t * 0.5);
                    const noiseC = Math.sin(seedC * i * 0.9 + 2) * 0.18 * (1 - t * 0.5);
                    const stepDate = new Date(lastDate);
                    stepDate.setMonth(stepDate.getMonth() + i);
                    const stepDateStr = stepDate.toISOString().slice(0, 10);
                    primary.push({ time: stepDateStr, value: currentRatio + ratioDiff * Math.max(0, t + noiseR) });
                    secondary.push({ time: stepDateStr, value: currentCap + capDiff * Math.max(0, t + noiseC) });
                }
            }
        }

        return { primary, secondary };
    }, [capGdpData, forecastTarget]);

    // Подготовка данных для SimpleChart: Cap/M2
    const capM2ChartData = useMemo(() => {
        if (!capM2Data?.data?.length) return { primary: [], secondary: [] };
        return {
            primary: capM2Data.data.map(d => ({
                time: d.date,
                value: d.ratio,
            })),
            secondary: capM2Data.data.map(d => ({
                time: d.date,
                value: d.cap ?? 0,
            })),
        };
    }, [capM2Data]);

    // ── Алерты на графике Баффета: level-алерт на КОЭФФИЦИЕНТ (правая ось) ──────
    const alertQuota = useCommonFeatures().telegram_alerts_quota;
    const alertsLocked = alertQuota === 0;
    const [chartAlertPrefill, setChartAlertPrefill] = useState<{ metricKey: string; threshold: number; currentLabel?: string } | null>(null);
    const [myAlerts, setMyAlerts] = useState<AlertInfo[]>([]);
    const reloadMyAlerts = () => {
        if (!ALERTS_ENABLED || alertsLocked) { setMyAlerts([]); return; }
        listAlerts({ limit: 200 }).then((p) => setMyAlerts(p.items)).catch(() => setMyAlerts([]));
    };
    useEffect(() => { reloadMyAlerts(); /* eslint-disable-next-line react-hooks/exhaustive-deps */ }, [alertsLocked]);

    // Режим коэффициента. cap-m2 хранит коэффициент как ДОЛЮ (график ×100 в %),
    // cap-gdp — уже в %. Порог алерта нормализуем в % (бэк считает 100·Cap/знам).
    const isM2 = viewMode === 'cap-m2';
    const modeKey = isM2 ? 'cap_m2' : 'cap_gdp';
    const modeLabel = isM2 ? 'Cap / M2' : 'Cap / ВВП';

    const buffettMetrics = useMemo<AlertMetricOption[]>(() => [{
        key: 'buffett_ratio', label: `Коэффициент Баффета (${modeLabel})`,
        indicator: 'buffett_ratio', metric: modeKey, unit: '%',
        ops: [
            { value: 'cross', label: 'Пересечение (в любую сторону)' },
            { value: 'cross_up', label: '↑ Пересечение (снизу вверх)' },
            { value: 'cross_down', label: '↓ Пересечение (сверху вниз)' },
        ],
        hint: `Сработает, когда индикатор Баффета (${modeLabel}) пересечёт заданный уровень в %. Порог — в тех же %, что на правой оси графика.`,
    }], [modeKey, modeLabel]);

    // Уровни активных buffett-алертов текущего режима → пунктир на ПРАВОЙ оси.
    // Порог в %; для cap-m2 домен оси = доля → делим на 100.
    const alertLevels = useMemo(() => {
        if (!ALERTS_ENABLED) return undefined;
        const lines = myAlerts
            .filter((a) => a.status === 'active' && a.indicator === 'buffett_ratio' && a.metric === modeKey)
            .map((a) => ({ value: isM2 ? a.threshold / 100 : a.threshold, color: 'var(--accent)', axis: 'secondary' as const }));
        return lines.length ? lines : undefined;
    }, [myAlerts, modeKey, isM2]);

    // Клик «+» на правой оси (коэффициент) → модалка. cap-m2: домен=доля → в %.
    const handleCreateAlertFromChart = (p: { axis: 'primary' | 'secondary'; level: number; currentValue: number }) => {
        if (p.axis !== 'secondary') return;   // «+» только на коэффициенте (правая ось)
        if (alertsLocked) { showUpgrade({ tier: 'basic', featureName: 'Алерты', indicator: 'alerts' }); return; }
        const toPct = (v: number) => (isM2 ? v * 100 : v);
        setChartAlertPrefill({
            metricKey: 'buffett_ratio',
            threshold: toPct(p.level),
            currentLabel: `${toPct(p.currentValue).toFixed(1)}%`,
        });
    };

    return (
        <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8 text-theme-primary min-h-screen">
            <PageHeader
                icon={Scale}
                title="Индикатор Баффетта"
                subtitle="Оценка рынка относительно экономики"
                help={METHODOLOGY.buffett}
                helpLink="/methodology/buffett"
            />

            {/* Editorial frame — обнимает controls + chart в один контейнер */}
            <div className="editorial-frame">

            {/* Контролы. Camera button — в конце строки через ml-auto. */}
            <div className="flex flex-wrap mb-4 md:mb-6 items-center" style={{ gap: 'var(--sp-2)' }}>
                {/* Переключатель режимов — горизонтальный + «?» с пояснением режимов */}
                <div data-tour="buffett-view-mode" style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <SegmentedControl<ViewMode>
                    options={[
                        { key: 'cap-gdp', label: 'Капитализация / ВВП' },
                        {
                            key: 'cap-m2',
                            label: 'Капитализация / M2',
                            locked: !buffAccess.isLoading && !buffAccess.canUseMode('cap-m2'),
                        },
                    ]}
                    value={viewMode}
                    onChange={setViewMode}
                    onLockedClick={() => {
                        const tier = buffAccess.requiredTierFor({ mode: 'cap-m2' });
                        if (tier) {
                            showUpgrade({
                                tier,
                                featureName: 'режим «Капитализация / M2»',
                                indicator: 'buffett',
                            });
                        }
                    }}
                    trailing={
                        <HelpTooltip
                            sections={[
                                { heading: 'Капитализация / ВВП', body: 'Классический индикатор Баффетта: капитализация рынка к ВВП. Показывает, дорог ли рынок относительно того, что реально производит экономика. Что считать недооценкой или переоценкой, смотрите в контексте конкретного периода: ориентируйтесь на недавние исторические примеры, где контекст ещё актуален. ВВП меняется медленно, поэтому это про долгосрочную картину.' },
                                { heading: 'Капитализация / M2', body: 'Капитализация к денежной массе M2 (наличные и депозиты): сколько в стране денег относительно рынка акций. Низкие значения значат, что денег много, но они не идут в акции, а сидят в депозитах и ОФЗ. M2 чувствительна к действиям ЦБ и бюджета, поэтому быстрее реагирует на монетарные условия.' },
                            ]}
                            size={18}
                        />
                    }
                />
                </div>

                {/* Период — горизонтальный ряд, в конце (после таймфрейма) */}
                <div data-tour="buffett-period" style={{ order: 3 }}>
                <SegmentedControl<BuffettPeriod>
                    options={(Object.keys(PERIOD_LABELS) as BuffettPeriod[]).map((p) => ({
                        key: p,
                        label: PERIOD_LABELS[p] ?? p,
                        // tier-замок по ПЕР-ИНДИКАТОРНОМУ canUsePeriod (бэковый
                        // max_history_days buffett), а не глобальному GUEST_MAX='1y'
                        // — иначе период за лимитом кликабелен → 403.
                        locked: !buffAccess.isLoading && !buffAccess.canUsePeriod(p),
                    }))}
                    value={period}
                    onChange={setPeriod}
                    onLockedClick={(p) => {
                        // Только tier-блокировка по матрице. Легаси-гейт гостя
                        // (isPeriodAllowed → /login) убран 2026-08: индикатор
                        // бесплатен целиком, и гость должен видеть все периоды,
                        // а не упираться в общий guest-лимит '1y' из
                        // config/accessControl (он остался у OI и фондов).
                        const tier = buffAccess.requiredTierFor({ period: p });
                        if (tier) {
                            showUpgrade({ tier, featureName: `период «${PERIOD_LABELS[p] ?? p}»`, indicator: 'buffett' });
                        }
                    }}
                />
                </div>

                {/* Таймфрейм — плитки (как на OI). cap-gdp и cap-m2 оба используют. */}
                {(viewMode === 'cap-gdp' || viewMode === 'cap-m2') && (
                    <div data-tour="buffett-timeframe" style={{ order: 2 }}>
                    <SegmentedControl<'1d' | '1w' | '1m'>
                        options={[
                            { key: '1d', label: '1Д' },
                            { key: '1w', label: '1Н' },
                            { key: '1m', label: '1М' },
                        ]}
                        value={timeframe}
                        onChange={setTimeframe}
                    />
                    </div>
                )}

                {/* Прогноз ВРЕМЕННО ОТКЛЮЧЁН на сайте (Вадим, 04.08.2026).
                    Флагом, а не удалением: расчёт, персист и отрисовка целы,
                    вернуть = поставить true. В песочнице прогноз остаётся. */}
                {SHOW_FORECAST && viewMode === 'cap-gdp' && (
                    <div data-tour="buffett-forecast">
                    <Dropdown<string>
                        options={[
                            { key: '', label: 'Прогноз: выкл' },
                            ...[10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110].map((v): DropdownOption<string> => ({
                                key: String(v),
                                label: `Прогноз: ${v}%`,
                            })),
                        ]}
                        value={forecastTarget !== null ? String(forecastTarget) : ''}
                        onChange={(k) => setForecastTarget(k ? Number(k) : null)}
                    />
                    </div>
                )}

                {/* Действия (Слои/Скриншот/CSV) свёрнуты в kebab «⋮» в углу графика
                    (паттерн OI). JSX тут, но через portal монтируется в обёртку
                    графика (containerRef=chartAnchorRef). */}
                <ChartActionsMenu containerRef={chartAnchorRef} tourId="buffett-export">
                <LayersButton
                    tourId="buffett-layers"
                    layers={[
                        { key: 'cap', label: 'Капитализация', hint: 'Линия капитализации на второй оси', checked: showCap, onChange: setShowCap },
                    ]}
                />
                <CsvExportButton
                    indicator="buffett"
                    config={() => ({
                        indicator: 'buffett',
                        title: 'Экспорт: Индикатор Баффета',
                        layers: [{
                            id: 'main',
                            label: 'Данные индикатора',
                            description: 'date, market_cap, knex (gdp_ttm или m2), ratio',
                            defaultSelected: true,
                        }],
                        // Unified порядок: режимы → период → таймфрейм.
                        selectors: [
                            {
                                kind: 'multiselect',
                                id: 'modes',
                                label: 'Режим расчёта',
                                default: [viewMode],
                                hint: 'Несколько → ZIP с CSV per режим',
                                options: [
                                    { value: 'cap-gdp', label: 'Кап / ВВП' },
                                    { value: 'cap-m2', label: 'Кап / M2' },
                                ],
                            },
                            {
                                kind: 'period',
                                id: 'period',
                                label: 'Период',
                                default: { type: 'preset', value: period },
                                presets: [
                                    { value: '1y', label: '1Г', days: 365 },
                                    { value: '5y', label: '5Л', days: 1825 },
                                    { value: '10y', label: '10Л', days: 3650 },
                                    { value: '20y', label: '20Л', days: 7300 },
                                    { value: 'all', label: 'Всё', days: 15000 },
                                ],
                            },
                            {
                                kind: 'select',
                                id: 'timeframe',
                                label: 'Таймфрейм',
                                default: timeframe,
                                options: [
                                    { value: '1d', label: 'Дневной' },
                                    { value: '1w', label: 'Недельный' },
                                    { value: '1m', label: 'Месячный' },
                                ],
                            },
                        ],
                        params: [],
                        buildUrl: (_layers, vals) => {
                            const modes = (vals.modes as string[] ?? [viewMode]).join(',');
                            const tf = (vals.timeframe as string) ?? timeframe;
                            const periodParam = periodToQuery(vals.period, 3650);
                            return `/api/export/buffett.csv?mode=${modes}&timeframe=${tf}&${periodParam}`;
                        },
                        buildFilename: (_, vals) => {
                            const modes = (vals.modes as string[] ?? [viewMode]);
                            const tf = (vals.timeframe as string) ?? timeframe;
                            return modes.length === 1
                                ? `buffett_${modes[0]}_${tf}.csv`
                                : `buffett_${Date.now()}.zip`;
                        },
                    })}
                />
                <ChartCaptureButton
                    getTargetElement={() => chartAnchorRef.current}
                    filename={`frame-buffett-${viewMode}-${period}-${timeframe}`}
                    metadata={{
                        title: 'Индикатор Баффетта',
                        asset: viewMode === 'cap-gdp' ? 'Капитализация / ВВП' : 'Капитализация / M2',
                        details: [
                            PERIOD_LABELS[period] ?? period,
                            timeframe === '1d' ? '1 день' : timeframe === '1w' ? '1 неделя' : '1 месяц',
                        ].filter(Boolean),
                    }}
                />
                <ChartSettings scopeLabels={{ primary: 'Капитализация', secondary: 'Отношение' }} />
                {ALERTS_ENABLED && (
                    <AlertBellButton
                        indicator="buffett"
                        asset="buffett"
                        assetName="Индикатор Баффета"
                        metrics={buffettMetrics}
                    />
                )}
                </ChartActionsMenu>

            </div>

            {/* График — стабильная обёртка: ref+position:relative не пересоздаются
                при смене режима (cap-gdp↔cap-m2), иначе portal kebab'а терял бы host. */}
            <div ref={chartAnchorRef} data-tour="buffett-chart" style={{ position: 'relative' }}>
            {error ? (
                <div className="flex items-center justify-center" style={{ height: chartHeight }}>
                    <div className="text-theme-danger text-center">
                        <p className="text-lg font-medium">{error}</p>
                        <p className="text-sm text-theme-secondary mt-2">Попробуйте обновить страницу</p>
                    </div>
                </div>
            ) : viewMode === 'cap-gdp' ? (
                <div>
                {/* Swap осей: главное значение (Кап/ВВП) на ПРАВОЙ оси —
                    TradingView-style. Cap на ЛЕВОЙ. Цвет accent-orange сохранён
                    за ratio через swap primaryColor/secondaryColor.
                    reverseLegend=true чтобы в легенде Кап/ВВП шёл первым.
                    formatPrimaryAxis даёт короткий формат "123" без " трлн ₽"
                    суффикса — иначе axis labels "123 трлн ₽" не влезают. */}
                <SimpleChart
                    data={capGdpChartData.secondary}
                    secondaryData={capGdpChartData.primary}
                    height={chartHeight}
                    primaryColor="var(--accent-secondary)"
                    secondaryColor="var(--accent)"
                    showPrimary={showCap}
                    showSecondary={true}
                    reverseLegend={true}
                    formatValue={(v) => `${v.toFixed(2)} трлн ₽`}
                    formatPrimaryAxis={(v) => String(Math.round(v))}
                    niceTicks={true}
                    // Сетка привязана к ПРАВОЙ оси (Кап/ВВП) — главное значение.
                    // niceTicksSecondary даёт круглые деления ratio, gridAxis их
                    // кладёт линиями. Левая ось (капитализация) — visual reference.
                    niceTicksSecondary={true}
                    gridAxis="secondary"
                    formatSecondaryValue={(v) => `${v.toFixed(2)}%`}
                    formatSecondaryAxis={(v) => `${v.toFixed(1)}%`}
                    // «+» алерт на КОЭФФИЦИЕНТ (правая ось). Левая (капитализация) — без «+».
                    onCreateAlert={ALERTS_ENABLED ? handleCreateAlertFromChart : undefined}
                    alertAxes={ALERTS_ENABLED ? ['secondary'] : undefined}
                    horizontalLines={alertLevels}
                    primaryLabel={isMobile ? 'Кап. (₽)' : 'Капитализация (трлн ₽)'}
                    secondaryLabel={isMobile ? 'Кап / ВВП' : 'Капитализация / ВВП'}
                    loading={loading}
                    forecastCount={forecastTarget !== null ? 12 : 0}
                    showValueHeader={false}
                    legendPosition="top"
                    showDownloadButton={false}
                    showNavigator={true}
                    hideTime={true}
                    // padding 100/100 — симметрия chart area. Unit "трлн ₽" в
                    // axis labels рендерится меньшим шрифтом (70%) через split
                    // в SimpleChart, поэтому общая ширина label умещается в 100px.
                    chartPadding={{ left: 100, right: 100 }}
                />
                </div>
            ) : (
                <div>
                <SimpleChart
                    data={capM2ChartData.secondary}
                    secondaryData={capM2ChartData.primary}
                    height={chartHeight}
                    primaryColor="var(--accent-secondary)"
                    secondaryColor="var(--accent)"
                    showPrimary={showCap}
                    showSecondary={true}
                    reverseLegend={true}
                    formatValue={(v) => `${v.toFixed(2)} трлн ₽`}
                    formatPrimaryAxis={(v) => String(Math.round(v))}
                    niceTicks={true}
                    // Сетка по ПРАВОЙ оси (Кап/M2), как в cap-gdp выше.
                    niceTicksSecondary={true}
                    gridAxis="secondary"
                    formatSecondaryValue={(v) => `${(v * 100).toFixed(2)}%`}
                    formatSecondaryAxis={(v) => `${(v * 100).toFixed(1)}%`}
                    onCreateAlert={ALERTS_ENABLED ? handleCreateAlertFromChart : undefined}
                    alertAxes={ALERTS_ENABLED ? ['secondary'] : undefined}
                    horizontalLines={alertLevels}
                    primaryLabel={isMobile ? 'Кап. (₽)' : 'Капитализация (трлн ₽)'}
                    secondaryLabel={isMobile ? 'Кап / M2' : 'Капитализация / M2'}
                    loading={loading}
                    showValueHeader={false}
                    legendPosition="top"
                    showDownloadButton={false}
                    showNavigator={true}
                    hideTime={true}
                    chartPadding={{ left: 100, right: 100 }}
                />
                </div>
            )}
            </div>{/* /buffett-chart — стабильная обёртка */}

            </div>{/* /editorial-frame */}

            {/* Onboarding tour */}
            <OnboardingTour
                steps={buffettTourSteps}
                open={tour.open}
                onClose={tour.close}
            />

            {/* Модалка алерта из «+» на коэффициенте (префилл: уровень % + текущее). */}
            {ALERTS_ENABLED && chartAlertPrefill && (
                <CreateAlertModal
                    indicator="buffett"
                    asset="buffett"
                    assetName="Индикатор Баффета"
                    metrics={buffettMetrics}
                    prefill={{ metricKey: chartAlertPrefill.metricKey, threshold: chartAlertPrefill.threshold, currentLabel: chartAlertPrefill.currentLabel }}
                    onClose={() => { setChartAlertPrefill(null); reloadMyAlerts(); }}
                />
            )}
        </div>
    );
}
