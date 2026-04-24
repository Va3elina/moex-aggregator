import React, { useEffect, useLayoutEffect, useState, useMemo, useRef, useCallback } from 'react';
import { TrendingUp, DollarSign, Banknote, LineChart, BarChart2, Gem, Wallet, Lock } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import PageHeader from '../components/PageHeader';
import {
    getFundsChartData,
    getFundsFlows,
    getFundHoldings,
    type FundsChartResponse,
    type FundsFlowsResponse,
    type FundHoldingsResponse,
    type FundInfo,
    type FundCategory,
    type FundPeriod,
    type FlowTimeframe
} from '../services/api';
import SimpleChart from '../components/SimpleChart';
import { useAuth } from '../contexts/AuthContext';
import { isPeriodAllowed, getDefaultPeriod } from '../config/accessControl';
import { useRealtimeData } from '../hooks/useRealtimeData';
import FundCardModal from '../components/funds/FundCardModal';
import FundsTable from '../components/funds/FundsTable';
import FlowsHistogram from '../components/funds/FlowsHistogram';

// Режимы отображения
type ViewMode = 'aum' | 'flows';

// Периоды
type Period = '1m' | '3m' | '6m' | '1y' | '2y' | '3y' | 'all';

const PERIOD_LABELS: Record<Period, string> = {
    '1m': '1М',
    '3m': '3М',
    '6m': '6М',
    '1y': '1Г',
    '2y': '2Г',
    '3y': '3Г',
    'all': 'Всё'
};

// Периоды для режима СЧА (ограниченный набор)
const AUM_PERIODS: Period[] = ['1m', '6m', '2y', 'all'];

// Категории
const CATEGORIES: { key: FundCategory; name: string; icon: React.ElementType; index: string }[] = [
    { key: 'money_market', name: 'Денежный рынок', icon: Banknote, index: 'RUSFAR3M' },
    { key: 'stocks', name: 'Акции', icon: TrendingUp, index: 'IMOEX' },
    { key: 'bonds', name: 'Облигации', icon: DollarSign, index: 'RGBITR' },
    { key: 'gold', name: 'Золото', icon: Gem, index: 'GLDRUB_TOM' },
];

const INDEX_COLOR = '#C8FF2E';

// Easing для анимации гистограммы
import { ANIMATION } from '../config/chartTheme';
const easeOutCubic = ANIMATION.easing;


export default function FundsMoneyPage() {
    const { isAuthenticated } = useAuth();
    const navigate = useNavigate();
    const [category, setCategory] = useState<FundCategory>('money_market');
    const [period, setPeriod] = useState<Period>(getDefaultPeriod('6m', isAuthenticated) as Period);
    const [viewMode, setViewMode] = useState<ViewMode>('aum');
    const [flowTimeframe, setFlowTimeframeRaw] = useState<FlowTimeframe>('1d');

    // Ограничения периодов для flow таймфреймов (как на ОИ)
    const FLOW_MIN_PERIODS: Record<FlowTimeframe, Period[]> = {
        '1d': ['1m', '3m', '6m', '1y', '2y', '3y', 'all'],
        '1w': ['6m', '1y', '2y', '3y', 'all'],
        '1m': ['2y', '3y', 'all'],
        '3m': ['2y', '3y', 'all'],
        '1y': ['3y', 'all'],
    };

    const isFlowPeriodAvailable = (p: Period): boolean => {
        if (viewMode !== 'flows') return true;
        const allowed = FLOW_MIN_PERIODS[flowTimeframe] || FLOW_MIN_PERIODS['1w'];
        return allowed.includes(p);
    };

    const setFlowTimeframe = (tf: FlowTimeframe) => {
        setFlowTimeframeRaw(tf);
        const allowed = FLOW_MIN_PERIODS[tf];
        if (!allowed.includes(period)) {
            setPeriod(allowed[0]);
        }
    };
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [data, setData] = useState<FundsChartResponse | null>(null);
    const [flowsData, setFlowsData] = useState<FundsFlowsResponse | null>(null);
    const [selectedFund, setSelectedFund] = useState<FundInfo | null>(null);
    const [fundHoldings, setFundHoldings] = useState<FundHoldingsResponse | null>(null);
    const [holdingsLoading, setHoldingsLoading] = useState(false);
    const [hiddenFunds, setHiddenFunds] = useState<Set<number>>(new Set());
    const [collapsedSubcats, setCollapsedSubcats] = useState<Set<string>>(new Set());
    const [navSortDir, setNavSortDir] = useState<'desc' | 'asc'>('desc');
    const [hoveredFlowIndex, setHoveredFlowIndex] = useState<number | null>(null);
    const [hoveredAnnotation, setHoveredAnnotation] = useState<string | null>(null); // date key
    const [showEvents, setShowEvents] = useState(false);
    const flowTooltipRef = useRef<HTMLDivElement>(null);
    const [flowNavRange, setFlowNavRange] = useState<[number, number]>([0, 0]);
    const flowChartRef = useRef<SVGSVGElement>(null);
    const flowContainerRef = useRef<HTMLDivElement>(null);

    // Анимация баров гистограммы (морфинг при смене данных)
    const [animatedBarsIn, setAnimatedBarsIn] = useState<number[]>([]);
    const [animatedBarsOut, setAnimatedBarsOut] = useState<number[]>([]);
    const prevBarsInRef = useRef<number[]>([]);
    const prevBarsOutRef = useRef<number[]>([]);
    const barsAnimRef = useRef<number | null>(null);
    const isFirstBarsRender = useRef(true);

    // Сброс скрытых фондов только при смене категории (не периода/таймфрейма)
    useEffect(() => { setHiddenFunds(new Set()); }, [category]);

    // Загрузка данных
    const loadData = useCallback(async () => {
        try {
            setLoading(true);
            setError(null);
            const result = await getFundsChartData(category, period as FundPeriod);
            setData(result);
        } catch (err) {
            setError('Ошибка загрузки данных');
            console.error(err);
        } finally {
            setLoading(false);
        }
    }, [category, period]);

    useEffect(() => { loadData(); }, [loadData]);

    // SSE: автоматическое обновление при новых данных
    useRealtimeData(['funds'], loadData);

    // Видимые fund_ids (для фильтрации flows)
    const visibleFundIds = useMemo(() => {
        if (!data?.funds) return undefined;
        const visible = data.funds.filter(f => !hiddenFunds.has(f.fund_id));
        // Если все видимы — не передаём фильтр (все фонды)
        if (visible.length === data.funds.length) return undefined;
        return visible.map(f => f.fund_id);
    }, [data?.funds, hiddenFunds]);

    // Тикеры скрытых фондов (для фильтрации событий на графике)
    const hiddenTickers = useMemo(() => {
        if (!data?.funds || hiddenFunds.size === 0) return new Set<string>();
        return new Set(
            data.funds.filter(f => hiddenFunds.has(f.fund_id)).map(f => f.ticker)
        );
    }, [data?.funds, hiddenFunds]);

    // Все тикеры в текущей категории (для определения «наших» фондов)
    const allTickers = useMemo(() => {
        if (!data?.funds) return new Set<string>();
        return new Set(data.funds.map(f => f.ticker));
    }, [data?.funds]);

    // Загрузка данных притоков/оттоков
    useEffect(() => {
        if (viewMode !== 'flows') return;

        async function loadFlowsData() {
            try {
                setLoading(true);
                const result = await getFundsFlows(category, flowTimeframe, period as FundPeriod, visibleFundIds);
                setFlowsData(result);
            } catch (err) {
                console.error('Flows error:', err);
            } finally {
                setLoading(false);
            }
        }
        loadFlowsData();
    }, [viewMode, category, flowTimeframe, period, visibleFundIds]);

    // Агрегация данных на основе видимых фондов
    const aggregatedData = useMemo(() => {
        if (!data?.funds) return { chartData: [], totalCurrentNav: 0 };

        const visibleFunds = data.funds.filter(f => !hiddenFunds.has(f.fund_id));

        // Собираем все уникальные даты
        const allDates = new Set<string>();
        visibleFunds.forEach(fund => {
            fund.data.forEach(p => allDates.add(p.date));
        });
        const responsiveDates = Array.from(allDates).sort();

        // Forward-fill: для каждого фонда строим карту date→nav с протяжкой
        // (ПИФы и БПИФы публикуют данные в разные дни — без ffill будут провалы)
        const fundNavMaps = visibleFunds.map(fund => {
            const map = new Map<string, number>();
            let lastNav = 0;
            const sorted = [...fund.data].sort((a, b) => a.date.localeCompare(b.date));
            for (const d of responsiveDates) {
                const point = sorted.find(p => p.date === d);
                if (point?.nav) lastNav = point.nav;
                if (lastNav > 0) map.set(d, lastNav);
            }
            return map;
        });

        // Суммируем NAV по датам (с forward-fill)
        const chartData = responsiveDates.map(date => {
            let totalNav = 0;
            fundNavMaps.forEach(navMap => {
                totalNav += navMap.get(date) || 0;
            });
            return {
                time: date,
                value: totalNav / 1e9 // млрд руб
            };
        });

        // Текущая суммарная СЧА (последняя точка)
        const totalCurrentNav = chartData.length > 0 ? chartData[chartData.length - 1].value : 0;

        return { chartData, totalCurrentNav };
    }, [data, hiddenFunds]);

    // Данные индекса (на вторичной оси)
    const indexData = useMemo(() => {
        if (!data?.index?.data) return undefined;
        return data.index.data.map(d => ({
            time: d.date,
            value: d.close || 0
        }));
    }, [data]);

    // Форматирование значений СЧА:
    // Золото и акции — 2 знака после точки (сотые млрд = десятки млн)
    // Остальные (облигации, денежный рынок) — без дробной части (крупные суммы)
    const formatNav = (value: number) => {
        if (category === 'gold' || category === 'stocks') {
            return value.toFixed(2);
        }
        return value.toFixed(0);
    };


    const toggleFundVisibility = (fundId: number) => {
        setHiddenFunds(prev => {
            const next = new Set(prev);
            if (next.has(fundId)) {
                next.delete(fundId);
            } else {
                next.add(fundId);
            }
            return next;
        });
    };

    const openFundCard = async (fund: FundInfo) => {
        setSelectedFund(fund);
        setHoldingsLoading(true);
        setFundHoldings(null);
        try {
            const data = await getFundHoldings(fund.fund_id);
            setFundHoldings(data);
        } catch {
            setFundHoldings({ fund_id: fund.fund_id, holdings: [] });
        } finally {
            setHoldingsLoading(false);
        }
    };

    const handleFlowMouseMove = (e: React.MouseEvent<HTMLDivElement>) => {
        if (!flowsData?.flows?.length || !flowContainerRef.current) return;
        const rect = flowContainerRef.current.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        const cs = getComputedStyle(document.documentElement);
        const padLeft = parseFloat(cs.getPropertyValue('--chart-pad-left')) || 100;
        const padRight = parseFloat(cs.getPropertyValue('--chart-pad-right-single')) || 95;
        const chartWidth = rect.width - padLeft - padRight;
        const xInChart = x - padLeft;
        if (xInChart < 0 || xInChart > chartWidth) return;
        const visibleCount = flowNavRange[1] - flowNavRange[0] + 1;
        const barWidth = chartWidth / visibleCount;
        const idx = Math.floor(xInChart / barWidth);
        if (idx >= 0 && idx < visibleCount) {
            setHoveredFlowIndex(idx);
            if (flowTooltipRef.current) {
                const hoverX = padLeft + idx * barWidth + barWidth / 2;
                const isRightHalf = hoverX > rect.width / 2;
                const rawLeft = isRightHalf ? hoverX - 188 : hoverX + 8;
                const cardLeft = Math.max(4, Math.min(rawLeft, rect.width - 192));
                const containerH = rect.height;
                flowTooltipRef.current.style.left = `${cardLeft}px`;
                flowTooltipRef.current.style.top = `${Math.min(Math.max(y - 20, 4), containerH - 60)}px`;
            }
        }
    };

    const handleFlowMouseLeave = useCallback(() => {
        setHoveredFlowIndex(null);
    }, []);

    // Сброс анимации при выходе из режима flows — следующее появление будет fade-in из нуля
    useEffect(() => {
        if (viewMode !== 'flows') {
            if (barsAnimRef.current) cancelAnimationFrame(barsAnimRef.current);
            setAnimatedBarsIn([]);
            setAnimatedBarsOut([]);
            prevBarsInRef.current = [];
            prevBarsOutRef.current = [];
            isFirstBarsRender.current = true;
        }
    }, [viewMode]);

    // Сброс навигатора при смене данных — useLayoutEffect (а не useEffect) чтобы
    // обновление срабатывало ДО первого paint'a после прихода данных, иначе rect
    // селект-окна моментально мелькает с width=0 → full width.
    useLayoutEffect(() => {
        if (flowsData?.flows?.length) {
            setFlowNavRange([0, flowsData.flows.length - 1]);
        }
    }, [flowsData]);

    // Анимация гистограммы при смене flowsData.
    // Всегда начинаем с нуля + каскад слева направо (волна),
    // а не морфим из предыдущих значений — при переключении
    // день/неделя/месяц данные полностью разные, морфинг
    // показывал хаотичную перестановку баров.
    useEffect(() => {
        if (!flowsData?.flows?.length) return;

        if (barsAnimRef.current) cancelAnimationFrame(barsAnimRef.current);

        const targetFlows = flowsData.flows.map(f => f.flow);
        const fromFlows = new Array(targetFlows.length).fill(0);

        isFirstBarsRender.current = false;

        // Каскадная анимация: бары появляются слева направо (волна).
        // Параметры из единого конфига chartTheme.ANIMATION.
        const totalDuration = ANIMATION.waveDuration;
        const staggerDelay = ANIMATION.waveStagger;
        let startTime: number | null = null;

        const animate = (timestamp: number) => {
            if (!startTime) startTime = timestamp;
            const elapsed = timestamp - startTime;

            const flows = targetFlows.map((v, i) => {
                const barDelay = (i / targetFlows.length) * staggerDelay;
                const barElapsed = Math.max(0, elapsed - barDelay);
                const t = Math.min(barElapsed / (totalDuration - staggerDelay), 1);
                return fromFlows[i] + (v - fromFlows[i]) * easeOutCubic(t);
            });

            // Разделяем на in/out по знаку текущего анимированного значения
            setAnimatedBarsIn(flows.map(v => Math.max(0, v)));
            setAnimatedBarsOut(flows.map(v => Math.min(0, v)));

            if (elapsed < totalDuration) {
                barsAnimRef.current = requestAnimationFrame(animate);
            } else {
                prevBarsInRef.current = targetFlows;
                prevBarsOutRef.current = [];
            }
        };

        barsAnimRef.current = requestAnimationFrame(animate);

        return () => {
            if (barsAnimRef.current) cancelAnimationFrame(barsAnimRef.current);
        };
    }, [flowsData]);

    const currentCategory = CATEGORIES.find(c => c.key === category);

    return (
        <div className="max-w-7xl mx-auto px-4 md:px-6 py-6 md:py-8 text-theme-primary min-h-screen">
            <PageHeader
                icon={Wallet}
                title="Деньги в фондах"
                subtitle="Динамика СЧА фондов и индексов"
            />

            {/* Вкладки категорий */}
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-6">
                {CATEGORIES.map(cat => {
                    const Icon = cat.icon;
                    const isActive = category === cat.key;
                    return (
                        <button
                            key={cat.key}
                            onClick={() => setCategory(cat.key)}
                            className={`flex items-center gap-2 px-3 py-3 rounded-xl font-medium transition-colors duration-200 min-w-0 ${isActive
                                ? 'bg-[#6366f1] text-white shadow-lg shadow-[#6366f1]/25'
                                : 'bg-theme-secondary text-theme-secondary hover:text-theme-primary border border-theme'
                                }`}
                        >
                            <Icon className="w-5 h-5 shrink-0" />
                            <span className="truncate text-sm">{cat.name}</span>
                            <span className={`text-xs px-1.5 py-0.5 rounded-full shrink-0 ${isActive ? 'bg-white/20' : 'bg-white/5'
                                }`}>
                                {cat.index}
                            </span>
                        </button>
                    );
                })}
            </div>

            {/* Периоды */}
            <div className="flex items-center gap-4 mb-6 flex-wrap">
                <div className="btn-group-scroll gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                    {(Object.keys(PERIOD_LABELS) as Period[]).filter(p => AUM_PERIODS.includes(p)).map((p) => {
                        const available = isFlowPeriodAvailable(p);
                        const allowed = isPeriodAllowed(p, isAuthenticated);
                        return (
                            <button
                                key={p}
                                onClick={() => {
                                    if (!allowed) { navigate('/login'); return; }
                                    if (available) setPeriod(p);
                                }}
                                disabled={!available && allowed}
                                title={!allowed ? 'Войдите для доступа' : !available ? 'Недоступно для этого таймфрейма' : undefined}
                                className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors duration-200 ${
                                    !allowed
                                        ? 'text-theme-muted cursor-not-allowed opacity-50'
                                        : period === p
                                            ? 'btn-control active'
                                            : available
                                                ? 'text-theme-secondary hover:text-theme-primary'
                                                : 'text-theme-muted cursor-not-allowed'
                                }`}
                            >
                                {PERIOD_LABELS[p]}
                                {!allowed && <Lock className="inline-block ml-0.5 w-3 h-3" />}
                            </button>
                        );
                    })}
                </div>

                {/* Режим: СЧА / Притоки-оттоки */}
                <div className="btn-group-scroll gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                    <button
                        onClick={() => { setViewMode('aum'); if (!AUM_PERIODS.includes(period)) setPeriod('6m'); }}
                        title="СЧА (объём активов)"
                        className={`flex items-center gap-2 px-3 py-1.5 rounded-lg transition-colors duration-200 ${viewMode === 'aum'
                            ? 'btn-control active'
                            : 'text-theme-secondary hover:text-theme-primary'
                            }`}
                    >
                        <LineChart size={16} />
                        <span className="text-sm font-medium">СЧА</span>
                    </button>
                    <button
                        onClick={() => setViewMode('flows')}
                        title="Притоки и оттоки"
                        className={`flex items-center gap-2 px-3 py-1.5 rounded-lg transition-colors duration-200 ${viewMode === 'flows'
                            ? 'btn-control active'
                            : 'text-theme-secondary hover:text-theme-primary'
                            }`}
                    >
                        <BarChart2 size={16} />
                        <span className="text-sm font-medium">Притоки-Оттоки</span>
                    </button>
                </div>

                {/* Таймфрейм для flows */}
                {viewMode === 'flows' && (
                    <div className="btn-group-scroll gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                        {(['1d', '1w', '1m'] as FlowTimeframe[]).map((tf) => (
                            <button
                                key={tf}
                                onClick={() => setFlowTimeframe(tf)}
                                className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors duration-200 ${flowTimeframe === tf
                                    ? 'btn-control active'
                                    : 'text-theme-secondary hover:text-theme-primary'
                                    }`}
                            >
                                {tf === '1d' ? 'День' : tf === '1w' ? 'Неделя' : 'Месяц'}
                            </button>
                        ))}
                    </div>
                )}

                {/* Тоггл событий */}
                {viewMode === 'flows' && (
                <div className="btn-group-scroll bg-theme-secondary rounded-xl border border-theme p-1">
                  <button
                    onClick={() => setShowEvents(!showEvents)}
                    className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors duration-200 ${showEvents
                      ? 'btn-control active'
                      : 'text-theme-secondary hover:text-theme-primary'
                    }`}
                  >
                    События
                  </button>
                </div>
                )}
            </div>

            {/* График — общий контейнер с фоном чтобы не мигал при переключении */}
            <div className="bg-theme-secondary rounded-2xl border border-theme overflow-hidden mb-6">
            {error ? (
                <div className="flex items-center justify-center h-[450px]">
                    <div className="text-[#FF4D4D] text-center">
                        <p className="text-lg font-medium">{error}</p>
                        <p className="text-sm text-theme-secondary mt-2">Попробуйте обновить страницу</p>
                    </div>
                </div>
            ) : viewMode === 'aum' ? (
                <div>
                    <SimpleChart
                        data={aggregatedData.chartData}
                        secondaryData={indexData}
                        height={450}
                        primaryColor="#6366f1"
                        secondaryColor={INDEX_COLOR}
                        showSecondary={true}
                        formatValue={formatNav}
                        formatSecondaryValue={(v) => v.toLocaleString('ru-RU', { maximumFractionDigits: 2 })}
                        primaryLabel="Суммарная СЧА (млрд руб)"
                        secondaryLabel={currentCategory?.index || 'Индекс'}
                        loading={loading}
                        showValueHeader={false}
                        legendPosition="top"
                        showDownloadButton={false}
                        showNavigator={true}
                        chartPadding={{ left: 120 }}
                        hideTime={true}
                    />
                </div>
            ) : (
            <FlowsHistogram
                        flowsData={flowsData}
                        animatedBarsIn={animatedBarsIn}
                        animatedBarsOut={animatedBarsOut}
                        flowNavRange={flowNavRange}
                        hoveredFlowIndex={hoveredFlowIndex}
                        hoveredAnnotation={hoveredAnnotation}
                        showEvents={showEvents}
                        hiddenTickers={hiddenTickers}
                        allTickers={allTickers}
                        category={category}
                        loading={loading}
                        flowContainerRef={flowContainerRef}
                        flowChartRef={flowChartRef}
                        flowTooltipRef={flowTooltipRef}
                        onMouseMove={handleFlowMouseMove}
                        onMouseLeave={handleFlowMouseLeave}
                        onSetHoveredAnnotation={setHoveredAnnotation}
                        onSetFlowNavRange={setFlowNavRange}
                    />
            )}
            </div>

            {/* Таблица фондов */}
            <FundsTable
                data={data}
                hiddenFunds={hiddenFunds}
                collapsedSubcats={collapsedSubcats}
                navSortDir={navSortDir}
                aggregatedData={aggregatedData}
                onToggleFundVisibility={toggleFundVisibility}
                onSetHiddenFunds={setHiddenFunds}
                onSetCollapsedSubcats={setCollapsedSubcats}
                onSetNavSortDir={setNavSortDir}
                onOpenFundCard={openFundCard}
            />

            {/* Модальная карточка фонда */}
            {selectedFund && (
                <FundCardModal
                    selectedFund={selectedFund}
                    fundHoldings={fundHoldings}
                    holdingsLoading={holdingsLoading}
                    onClose={() => setSelectedFund(null)}
                />
            )}
        </div>
    );
}
