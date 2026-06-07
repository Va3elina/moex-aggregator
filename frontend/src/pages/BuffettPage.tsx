import { useEffect, useState, useMemo, useCallback, useRef } from 'react';
import { Scale } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
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
import { useAuth } from '../contexts/AuthContext';
import { isPeriodAllowed } from '../config/accessControl';
import { useRealtimeData } from '../hooks/useRealtimeData';
import { usePersistedState } from '../hooks/usePersistedState';
import { useFitToViewport } from '../hooks/useFitToViewport';
import { useIsMobile } from '../hooks/useIsMobile';
import { useOnboardingTour } from '../hooks/useFirstVisit';
import OnboardingTour from '../components/onboarding/OnboardingTour';
import { buffettTourSteps } from '../data/tours/buffett';
import { useTierAccess } from '../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../components/tier/UpgradeModal';
import { handleTierError } from '../utils/tierError';

type ViewMode = 'cap-gdp' | 'cap-m2';

const PERIOD_LABELS: Partial<Record<BuffettPeriod, string>> = {
    '5y': '5Л',
    '10y': '10Л',
    '20y': '20Л',
    'all': 'Всё',
};

export default function BuffettPage() {
    const { isAuthenticated } = useAuth();
    const navigate = useNavigate();
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
        bottomBuffer: 96,
    });

    // Загрузка данных
    const loadData = useCallback(async () => {
        try {
            setLoading(true);
            setError(null);
            if (viewMode === 'cap-gdp') {
                const result = await getBuffettCapGdp(period, smooth, timeframe);
                setCapGdpData(result);
            } else {
                const result = await getBuffettCapM2(period, smooth, timeframe);
                setCapM2Data(result);
            }
        } catch (err: unknown) {
            const msg = err instanceof Error ? err.message : '';
            // При 403 (гость или протухший токен) — фолбэк на 1y
            if (msg.includes('авторизац') && period !== '1y') {
                setPeriod('1y');
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
            setLoading(false);
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
                {/* Переключатель режимов */}
                <div data-tour="buffett-view-mode">
                <Dropdown<ViewMode>
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
                />
                </div>

                {/* Период */}
                <div data-tour="buffett-period">
                <Dropdown<BuffettPeriod>
                    options={(Object.keys(PERIOD_LABELS) as BuffettPeriod[]).map((p): DropdownOption<BuffettPeriod> => ({
                        key: p,
                        label: PERIOD_LABELS[p] ?? p,
                        locked: !isPeriodAllowed(p, isAuthenticated),
                    }))}
                    value={period}
                    onChange={setPeriod}
                    onLockedClick={(p) => {
                        // Tier-блокировка → upgrade modal; иначе legacy guest gate → /login.
                        if (!buffAccess.canUsePeriod(p)) {
                            const tier = buffAccess.requiredTierFor({ period: p });
                            if (tier) {
                                showUpgrade({ tier, featureName: `период «${PERIOD_LABELS[p] ?? p}»`, indicator: 'buffett' });
                                return;
                            }
                        }
                        if (!isPeriodAllowed(p, isAuthenticated)) navigate('/login');
                    }}
                />
                </div>

                {/* Таймфрейм — для cap-gdp и cap-m2 */}
                {(viewMode === 'cap-gdp' || viewMode === 'cap-m2') && (
                    <div data-tour="buffett-timeframe">
                    <Dropdown<'1d' | '1w' | '1m'>
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

                {/* Прогноз — только для cap-gdp */}
                {viewMode === 'cap-gdp' && (
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

                {/* Toggle капитализации (secondary axis) — chip pattern из Strength */}
                <button
                    data-tour="buffett-cap-toggle"
                    onClick={() => setShowCap(!showCap)}
                    className="editorial-press font-semibold rounded-full"
                    style={{
                        backgroundColor: showCap ? 'var(--accent)' : 'var(--bg-secondary)',
                        color: showCap ? 'var(--text-inverse)' : 'var(--text-primary)',
                        border: '2px solid var(--text-primary)',
                        boxShadow: showCap ? 'var(--shadow-hard-chip)' : undefined,
                        fontSize: 'var(--fs-sm)',
                        padding: 'var(--sp-2) var(--sp-3)',
                    }}
                >
                    Капитализация
                </button>

                {/* Camera + CSV buttons inline, прижаты к правому краю */}
                <div data-tour="buffett-export" className="ml-auto flex items-center" style={{ gap: 'var(--sp-2)' }}>
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
                </div>

            </div>

            {/* График */}
            {error ? (
                <div className="flex items-center justify-center" style={{ height: chartHeight }}>
                    <div className="text-theme-danger text-center">
                        <p className="text-lg font-medium">{error}</p>
                        <p className="text-sm text-theme-secondary mt-2">Попробуйте обновить страницу</p>
                    </div>
                </div>
            ) : viewMode === 'cap-gdp' ? (
                <div ref={chartAnchorRef} data-tour="buffett-chart">
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
                    formatPrimaryAxis={(v) => `${v.toFixed(2)} трлн ₽`}
                    formatSecondaryValue={(v) => `${v.toFixed(2)}%`}
                    formatSecondaryAxis={(v) => `${v.toFixed(1)}%`}
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
                <div ref={chartAnchorRef} data-tour="buffett-chart">
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
                    formatPrimaryAxis={(v) => `${v.toFixed(2)} трлн ₽`}
                    formatSecondaryValue={(v) => `${(v * 100).toFixed(2)}%`}
                    formatSecondaryAxis={(v) => `${(v * 100).toFixed(1)}%`}
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

            </div>{/* /editorial-frame */}

            {/* Описание */}
            <div className="mt-6 grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="bg-theme-secondary rounded-2xl p-5 border border-theme">
                    <h3 className="font-semibold mb-2 text-theme-warning">Капитализация / ВВП — оценка vs экономика</h3>
                    <p className="text-sm text-theme-secondary leading-relaxed">
                        Классический индикатор Баффетта: рыночная капитализация к ВВП.
                        Отвечает на вопрос «дорог ли рынок относительно того, что реально производит экономика».
                        Для России: ниже 40% — недооценка, 40–70% — норма, выше 70% — возможная переоценка.
                        Знаменатель (ВВП) движется медленно, поэтому индикатор отражает долгосрочную картину.
                    </p>
                </div>
                <div className="bg-theme-secondary rounded-2xl p-5 border border-theme">
                    <h3 className="font-semibold mb-2 text-theme-warning">Капитализация / M2 — оценка vs ликвидность</h3>
                    <p className="text-sm text-theme-secondary leading-relaxed">
                        Рыночная капитализация к денежной массе M2 (наличные + депозиты).
                        Отвечает на другой вопрос: «сколько в стране денег относительно рынка акций».
                        Низкие значения = денег много, но они не идут в акции (сидят в депозитах/ОФЗ).
                        Знаменатель чувствителен к действиям ЦБ и бюджета — индикатор быстрее реагирует
                        на монетарные условия, чем Cap/ВВП.
                    </p>
                </div>
            </div>

            {/* Onboarding tour */}
            <OnboardingTour
                steps={buffettTourSteps}
                open={tour.open}
                onClose={tour.close}
            />
        </div>
    );
}
