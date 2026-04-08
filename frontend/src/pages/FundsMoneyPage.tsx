import React, { useEffect, useState, useMemo, useRef, useCallback } from 'react';
import { TrendingUp, DollarSign, Banknote, LineChart, BarChart2, Gem, Wallet, Lock } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
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
import { X } from 'lucide-react';
import SimpleChart from '../components/SimpleChart';
import { useAuth } from '../contexts/AuthContext';
import { isPeriodAllowed, getDefaultPeriod } from '../config/accessControl';
import { useRealtimeData } from '../hooks/useRealtimeData';

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

// Цветовая палитра для фондов
const FUND_COLORS = [
    '#2EE59D', '#4DA3FF', '#9D4DFF', '#FF4D4D', '#FFB020',
    '#00D9FF', '#FF6B9D', '#FCD34D', '#14B8A6', '#F97316'
];

import { UK_LOGOS } from '../config/fundConfig';
import { FUND_ANNOTATIONS } from '../config/fundAnnotations';

const INDEX_COLOR = '#C8FF2E';

// Easing для анимации гистограммы
const easeOutCubic = (t: number) => 1 - Math.pow(1 - t, 3);

// Ресемплинг массива значений до нужной длины (для морфинга при смене кол-ва баров)
const resampleFlows = (vals: number[], len: number): number[] => {
    if (len === 0) return [];
    if (vals.length === 0) return new Array(len).fill(0);
    if (vals.length === len) return vals;
    return Array.from({ length: len }, (_, i) => {
        const t = len === 1 ? 0 : i / (len - 1);
        const si = t * (vals.length - 1);
        const lo = Math.floor(si);
        const hi = Math.min(lo + 1, vals.length - 1);
        return vals[lo] + (vals[hi] - vals[lo]) * (si - lo);
    });
};

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

    // Форматирование значений
    const formatNav = (value: number) => `${value.toFixed(0)}`;


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
        const chartWidth = rect.width - 60;
        const visibleCount = flowNavRange[1] - flowNavRange[0] + 1;
        const barWidth = chartWidth / visibleCount;
        const idx = Math.floor(x / barWidth);
        if (idx >= 0 && idx < visibleCount) {
            setHoveredFlowIndex(idx);
            // Двигаем тултип напрямую через ref (без ре-рендера)
            if (flowTooltipRef.current) {
                const hoverX = idx * barWidth + barWidth / 2;
                const isRightHalf = hoverX > chartWidth / 2;
                const cardLeft = isRightHalf ? hoverX - 188 : hoverX + 8;
                flowTooltipRef.current.style.left = `${cardLeft}px`;
                flowTooltipRef.current.style.top = `${Math.min(Math.max(y - 20, 4), 330)}px`;
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

    // Сброс навигатора при смене данных
    useEffect(() => {
        if (flowsData?.flows?.length) {
            setFlowNavRange([0, flowsData.flows.length - 1]);
        }
    }, [flowsData]);

    // Морфинг гистограммы при смене flowsData
    useEffect(() => {
        if (!flowsData?.flows?.length) return;

        if (barsAnimRef.current) cancelAnimationFrame(barsAnimRef.current);

        const targetFlows = flowsData.flows.map(f => f.flow);
        const isFirst = isFirstBarsRender.current || prevBarsInRef.current.length === 0;
        const fromFlows = isFirst ? new Array(targetFlows.length).fill(0) : resampleFlows(prevBarsInRef.current, targetFlows.length);

        isFirstBarsRender.current = false;

        // Каскадная анимация: бары появляются слева направо
        const totalDuration = isFirst ? 1200 : 600;
        const staggerDelay = isFirst ? 600 : 0;
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
            {/* Заголовок */}
            <div className="flex items-center gap-3 mb-6">
                <div className="p-3 bg-gradient-to-br from-[#6366f1] to-[#9D4DFF] rounded-xl">
                    <Wallet className="w-6 h-6 text-white" />
                </div>
                <div>
                    <h1 className="text-2xl font-bold text-theme-primary">Деньги в фондах</h1>
                    <p className="text-theme-secondary text-sm">Динамика СЧА фондов и индексов</p>
                </div>
            </div>

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
                <div className="flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
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
                <div className="flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
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
                    <div className="flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
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
                <div className="flex items-center bg-theme-secondary rounded-xl border border-theme p-1">
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
            <div className="bg-theme-secondary rounded-2xl border border-theme overflow-hidden mb-6" style={{ minHeight: 500 }}>
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
                        primaryLabel="Суммарная СЧА"
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
            <div className="p-6 relative" style={{ minHeight: 850 }}>
                    {/* Спиннер загрузки — в углу если есть старые данные, в центре если первая загрузка */}
                    {loading && !flowsData?.flows?.length && animatedBarsIn.length === 0 ? (
                        <div className="flex items-center justify-center" style={{ height: 450 }}>
                            <div className="flex flex-col items-center gap-3">
                                <div className="w-8 h-8 border-2 border-[#6366f1] border-t-transparent rounded-full animate-spin" />
                                <span className="text-theme-secondary">Загрузка...</span>
                            </div>
                        </div>
                    ) : (<>
                    {loading && (
                        <div className="absolute top-4 right-4 z-20 flex items-center gap-2 bg-theme-tertiary/90 backdrop-blur-sm px-3 py-1.5 rounded-lg border border-theme">
                            <div className="w-4 h-4 border-2 border-[#C8FF2E] border-t-transparent rounded-full animate-spin" />
                            <span className="text-xs text-theme-secondary">Обновление...</span>
                        </div>
                    )}
                    {/* Гистограмма притоков/оттоков */}
                    <div className="pb-6">
                        {/* Легенда */}
                        <div className="flex items-center justify-center gap-5 mb-2 text-sm">
                            <span className="flex items-center gap-2">
                                <span className="w-3 h-3 rounded-full bg-[#2EE59D]" />
                                <span className="text-theme-primary font-medium">Приток</span>
                            </span>
                            <span className="flex items-center gap-2">
                                <span className="w-3 h-3 rounded-full bg-[#FF4D4D]" />
                                <span className="text-theme-primary font-medium">Отток</span>
                            </span>
                        </div>

                        {/* Плавающая дата — между легендой и графиком */}
                        <div className="relative h-6 mb-1">
                            {hoveredFlowIndex !== null && flowsData?.flows && (() => {
                                const visibleFlowsList = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                                const f = visibleFlowsList[hoveredFlowIndex];
                                if (!f) return null;
                                const visibleCount = flowNavRange[1] - flowNavRange[0] + 1;
                                const chartWidth = flowContainerRef.current ? flowContainerRef.current.getBoundingClientRect().width - 60 : 0;
                                const barWidth = chartWidth / visibleCount;
                                const dateX = hoveredFlowIndex * barWidth + barWidth / 2;
                                const dateStr = new Date(f.period_end).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', year: 'numeric' });
                                return (
                                    <div
                                        className="absolute z-30 pointer-events-none"
                                        style={{
                                            left: dateX,
                                            top: 0,
                                            transform: 'translateX(-50%)',
                                        }}
                                    >
                                        <span className="text-[11px] text-theme-secondary bg-theme-tertiary/90 backdrop-blur-sm px-2 py-0.5 rounded border border-theme whitespace-nowrap">
                                            {dateStr}
                                        </span>
                                    </div>
                                );
                            })()}
                        </div>

                        {/* График с тултипом */}
                        <div
                            ref={flowContainerRef}
                            className="relative cursor-crosshair"
                            style={{ aspectRatio: '16 / 9' }}
                            onMouseMove={handleFlowMouseMove}
                            onMouseLeave={handleFlowMouseLeave}
                        >
                            {/* Область графика: отступ справа для подписей */}
                            <div className="absolute inset-0" style={{ right: 60 }}>
                            <svg
                                ref={flowChartRef}
                                width="100%"
                                height="100%"
                                preserveAspectRatio="none"
                            >
                                {animatedBarsIn.length > 0 && (() => {
                                    const visibleIn = animatedBarsIn.slice(flowNavRange[0], flowNavRange[1] + 1);
                                    const visibleOut = animatedBarsOut.slice(flowNavRange[0], flowNavRange[1] + 1);
                                    const maxScale = Math.max(
                                        ...visibleIn.map(v => Math.abs(v)),
                                        ...visibleOut.map(v => Math.abs(v)),
                                        0.001
                                    );
                                    const barWidth = 100 / (visibleIn.length || 1);
                                    const midY = 50;
                                    const halfH = 47;
                                    const minBarH = 1.2;

                                    return visibleIn.map((inVal, i) => {
                                        const outVal = visibleOut[i] ?? 0;
                                        const isHovered = hoveredFlowIndex === i;
                                        const opacity = hoveredFlowIndex === null ? 1 : isHovered ? 1 : 0.35;
                                        const x = `${i * barWidth + barWidth * 0.15}%`;
                                        const w = `${barWidth * 0.7}%`;

                                        const hIn = inVal > 0 ? Math.max((inVal / maxScale) * halfH, minBarH) : 0;
                                        const hOut = outVal < 0 ? Math.max((Math.abs(outVal) / maxScale) * halfH, minBarH) : 0;

                                        return (
                                            <g key={i} opacity={opacity}>
                                                {hIn > 0 && (
                                                    <rect x={x} y={`${midY - hIn}%`} width={w} height={`${hIn}%`}
                                                        fill="#2EE59D" rx="2" />
                                                )}
                                                {hOut > 0 && (
                                                    <rect x={x} y={`${midY}%`} width={w} height={`${hOut}%`}
                                                        fill="#FF4D4D" rx="2" />
                                                )}
                                            </g>
                                        );
                                    });
                                })()}
                                {/* Горизонтальные линии сетки */}
                                {flowsData?.flows?.length && (() => {
                                    const visibleF = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                                    const maxAbsF = Math.max(
                                        ...visibleF.map(f => Math.abs(f.flow)),
                                        0.001
                                    );
                                    const ticks = [-maxAbsF, -maxAbsF / 2, 0, maxAbsF / 2, maxAbsF];
                                    return ticks.map((val, i) => {
                                        const yPct = 50 - (val / maxAbsF) * 47;
                                        return (
                                            <line key={`grid-${i}`}
                                                x1="0" y1={`${yPct}%`} x2="100%" y2={`${yPct}%`}
                                                stroke={val === 0 ? "rgba(255,255,255,0.15)" : "rgba(255,255,255,0.08)"} strokeWidth="1"
                                            />
                                        );
                                    });
                                })()}
                                {/* Вертикальный курсор */}
                                {hoveredFlowIndex !== null && flowsData?.flows && (() => {
                                    const visibleCount = flowNavRange[1] - flowNavRange[0] + 1;
                                    const barWidth = 100 / visibleCount;
                                    const cx = hoveredFlowIndex * barWidth + barWidth / 2;
                                    return (
                                        <line
                                            x1={`${cx}%`}
                                            y1="0"
                                            x2={`${cx}%`}
                                            y2="100%"
                                            stroke="#C8FF2E"
                                            strokeWidth="1"
                                            strokeDasharray="4 3"
                                            opacity="0.5"
                                            style={{ pointerEvents: 'none' }}
                                        />
                                    );
                                })()}
                                {/* Вертикальная линия аномалии — только при hover на кружок */}
                                {hoveredAnnotation && flowsData?.flows && (() => {
                                    const visibleFlows = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                                    const idx = visibleFlows.findIndex(f => f.period_end.slice(0, 10) === hoveredAnnotation);
                                    if (idx === -1) return null;
                                    const barW = 100 / visibleFlows.length;
                                    const cx = idx * barW + barW / 2;
                                    return (
                                        <line
                                            x1={`${cx}%`} y1="0" x2={`${cx}%`} y2="100%"
                                            stroke="#9CA3B8" strokeWidth="1" strokeDasharray="3 4"
                                            opacity="0.4" style={{ pointerEvents: 'none' }}
                                        />
                                    );
                                })()}
                            </svg>
                            </div>

                            {/* Тултип-карточка со значением — позиция через ref */}
                            {hoveredFlowIndex !== null && flowsData?.flows && (() => {
                                const visibleFlowsList = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                                const f = visibleFlowsList[hoveredFlowIndex];
                                if (!f) return null;
                                const flowStr = `${f.flow > 0 ? '+' : ''}${Math.abs(f.flow) >= 0.01 ? f.flow.toFixed(2) : f.flow.toFixed(3)} млрд ₽`;
                                const pctStr = `${f.flow_pct > 0 ? '+' : ''}${f.flow_pct.toFixed(2)}%`;
                                const color = f.flow >= 0 ? '#2EE59D' : '#FF4D4D';
                                return (
                                    <div
                                        ref={flowTooltipRef}
                                        className="absolute z-30 pointer-events-none"
                                    >
                                        <div className="bg-theme-tertiary/95 backdrop-blur-sm rounded-lg border border-theme shadow-xl pointer-events-none py-1.5 px-3">
                                            <div className="flex items-center justify-between gap-3">
                                                <div className="flex items-center gap-1.5 min-w-0">
                                                    <span className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: color }} />
                                                    <span className="text-[11px] text-theme-secondary truncate">{f.flow >= 0 ? 'Приток' : 'Отток'}</span>
                                                </div>
                                                <span className="text-xs font-semibold whitespace-nowrap" style={{ color }}>
                                                    {flowStr}
                                                </span>
                                            </div>
                                            <div className="flex items-center justify-between gap-3 mt-0.5">
                                                <div className="flex items-center gap-1.5 min-w-0">
                                                    <span className="w-2 h-2 rounded-full flex-shrink-0 bg-[#6366f1]" />
                                                    <span className="text-[11px] text-theme-secondary">Изменение</span>
                                                </div>
                                                <span className="text-xs font-semibold whitespace-nowrap" style={{ color }}>
                                                    {pctStr}
                                                </span>
                                            </div>
                                        </div>
                                    </div>
                                );
                            })()}

                            {/* Подписи значений справа */}
                            {flowsData?.flows?.length && (() => {
                                const visibleF = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                                const maxAbs = Math.max(
                                    ...visibleF.map(f => Math.abs(f.flow)),
                                    0.001
                                );
                                const ticks = [maxAbs, maxAbs / 2, 0, -maxAbs / 2, -maxAbs];
                                return ticks.map((val, i) => {
                                    const yPct = 50 - (val / maxAbs) * 47;
                                    const label = val === 0 ? '0' : `${val > 0 ? '+' : ''}${Math.abs(val) >= 0.1 ? val.toFixed(1) : val.toFixed(2)}`;
                                    return (
                                        <div key={`label-${i}`}
                                            className="absolute pointer-events-none"
                                            style={{ top: `${yPct}%`, right: 4, transform: 'translateY(-50%)' }}
                                        >
                                            <span className="text-[16px] font-semibold text-[#9CA3B8]">
                                                {label}
                                            </span>
                                        </div>
                                    );
                                });
                            })()}



                            {/* Даты оси X — равномерно по всей ширине */}
                            <div className="absolute -bottom-6 left-0 flex justify-between text-[14px] font-semibold text-[#9CA3B8] px-2" style={{ right: 60 }}>
                                {flowsData?.flows && flowsData.flows.length > 0 && (() => {
                                    const flows = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                                    if (!flows.length) return null;
                                    const tickCount = Math.min(6, flows.length);
                                    return Array.from({ length: tickCount }, (_, i) => {
                                        const idx = Math.min(Math.round(i * (flows.length - 1) / Math.max(tickCount - 1, 1)), flows.length - 1);
                                        if (!flows[idx]) return null;
                                        const date = new Date(flows[idx].period_end);
                                        return (
                                            <span key={i}>{date.toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', year: '2-digit' })}</span>
                                        );
                                    });
                                })()}
                            </div>
                        </div>

                        {/* Маркеры аномальных событий — зарезервированное место */}
                        <div className="relative" style={{ height: 28, marginTop: -34 }}>
                        {showEvents && flowsData?.flows && flowsData.flows.length > 0 && (() => {
                            const visibleFlows = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                            const chartWidth = flowContainerRef.current ? flowContainerRef.current.getBoundingClientRect().width - 60 : 0;
                            const barWidth = chartWidth / visibleFlows.length;

                            // Находим аннотации — нечёткое совпадение дат (ближайший бар к дате аннотации)
                            const markers = FUND_ANNOTATIONS
                                .filter(a => a.category === category)
                                .map(annotation => {
                                    const annDate = new Date(annotation.date).getTime();
                                    // Ищем ближайший бар по дате (не строгое совпадение)
                                    let bestIdx = -1;
                                    let bestDist = Infinity;
                                    for (let i = 0; i < visibleFlows.length; i++) {
                                        const barDate = new Date(visibleFlows[i].period_end).getTime();
                                        const dist = Math.abs(barDate - annDate);
                                        // Допуск: до 7 дней (для недельного ТФ)
                                        if (dist < bestDist && dist <= 7 * 86400000) {
                                            bestDist = dist;
                                            bestIdx = i;
                                        }
                                    }
                                    if (bestIdx === -1) return null;
                                    const logo = UK_LOGOS[annotation.ukId];
                                    if (!logo) return null;
                                    return { ...annotation, idx: bestIdx, logo, x: bestIdx * barWidth + barWidth / 2 };
                                })
                                .filter(Boolean) as (typeof FUND_ANNOTATIONS[0] & { idx: number; logo: typeof UK_LOGOS[string]; x: number })[];

                            if (!markers.length) return null;

                            return (<>
                                    {markers.map((m, i) => (
                                        <div
                                            key={i}
                                            className="absolute -translate-x-1/2 group"
                                            style={{ left: m.x }}
                                            onMouseEnter={() => setHoveredAnnotation(m.date)}
                                            onMouseLeave={() => setHoveredAnnotation(null)}
                                        >
                                            {/* Кружок — серый, мелкий */}
                                            <div
                                                className="w-7 h-7 rounded-full flex items-center justify-center cursor-pointer transition-opacity opacity-50 hover:opacity-100"
                                                style={{ backgroundColor: '#3a3f4f', color: '#9CA3B8', fontSize: 11, fontWeight: 600 }}
                                            >
                                                {m.logo.letter}
                                            </div>

                                            {/* Тултип при hover */}
                                            <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 hidden group-hover:block z-50 pointer-events-none">
                                                <div className="bg-theme-tertiary/95 backdrop-blur-sm rounded-lg border border-theme shadow-xl py-1.5 px-2.5 whitespace-nowrap max-w-[320px]">
                                                    <div className="flex items-center gap-2 mb-0.5">
                                                        <span className="text-[10px] font-medium text-[#9CA3B8]">
                                                            {m.type === 'merger' ? 'Слияние' : m.type === 'liquidation' ? 'Ликвидация' : 'Реорганизация'}
                                                        </span>
                                                        <span className="text-[10px] text-theme-secondary">
                                                            {new Date(m.date).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', year: 'numeric' })}
                                                        </span>
                                                    </div>
                                                    <p className="text-[11px] text-theme-primary whitespace-normal leading-tight">
                                                        {m.description}
                                                    </p>
                                                </div>
                                            </div>
                                        </div>
                                    ))}
                            </>);
                        })()}
                        </div>

                        {/* Скользящее окно — минималистичный ползунок */}
                        {flowsData?.flows && flowsData.flows.length > 1 && (() => {
                            const n = flowsData.flows.length;
                            const [s0, s1] = flowNavRange;
                            const handleNavMouse = (e: React.MouseEvent<HTMLDivElement>, type: 'left' | 'right' | 'window') => {
                                e.preventDefault();
                                const container = e.currentTarget.closest('[data-flow-nav]') as HTMLDivElement;
                                if (!container) return;
                                const rect = container.getBoundingClientRect();
                                const startX = e.clientX;
                                const startS0 = s0, startS1 = s1;
                                const onMove = (ev: MouseEvent) => {
                                    const dx = ev.clientX - startX;
                                    const di = Math.round((dx / rect.width) * (n - 1));
                                    if (type === 'left') {
                                        setFlowNavRange([Math.max(0, Math.min(startS0 + di, s1 - 1)), s1]);
                                    } else if (type === 'right') {
                                        setFlowNavRange([s0, Math.max(s0 + 1, Math.min(startS1 + di, n - 1))]);
                                    } else {
                                        const range = startS1 - startS0;
                                        let ns = Math.max(0, Math.min(startS0 + di, n - 1 - range));
                                        setFlowNavRange([ns, ns + range]);
                                    }
                                };
                                const onUp = () => { document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp); };
                                document.addEventListener('mousemove', onMove);
                                document.addEventListener('mouseup', onUp);
                            };

                            const selLeftPct = (s0 / Math.max(n - 1, 1)) * 100;
                            const selRightPct = (s1 / Math.max(n - 1, 1)) * 100;

                            return (
                                <div className="mt-10 relative select-none" style={{ height: 56 }} data-flow-nav>
                                    {/* Неактивный трек */}
                                    <div className="absolute inset-x-0 inset-y-0 bg-white/[0.03] rounded-lg" />
                                    {/* Маска слева */}
                                    <div className="absolute inset-y-0 left-0 bg-black/50 rounded-l-lg"
                                        style={{ width: `${selLeftPct}%` }} />
                                    {/* Маска справа */}
                                    <div className="absolute inset-y-0 right-0 bg-black/50 rounded-r-lg"
                                        style={{ width: `${100 - selRightPct}%` }} />
                                    {/* Активная зона */}
                                    <div className="absolute inset-y-0 cursor-grab"
                                        style={{
                                            left: `${selLeftPct}%`,
                                            width: `${selRightPct - selLeftPct}%`,
                                            background: 'rgba(56,98,251,0.08)',
                                            borderTop: '1px solid rgba(56,98,251,0.45)',
                                            borderBottom: '1px solid rgba(56,98,251,0.45)',
                                        }}
                                        onMouseDown={(e) => handleNavMouse(e, 'window')}
                                    />
                                    {/* Левый хэндл */}
                                    <div className="absolute top-1/2 -translate-y-1/2 -translate-x-1/2 flex items-center justify-center cursor-ew-resize"
                                        style={{
                                            left: `${selLeftPct}%`,
                                            width: 14, height: 56 * 0.7,
                                            borderRadius: 3,
                                            background: 'rgba(56,98,251,0.9)',
                                        }}
                                        onMouseDown={(e) => handleNavMouse(e, 'left')}
                                    >
                                        <span className="text-white text-[9px] font-bold pointer-events-none">◀</span>
                                    </div>
                                    {/* Правый хэндл */}
                                    <div className="absolute top-1/2 -translate-y-1/2 -translate-x-1/2 flex items-center justify-center cursor-ew-resize"
                                        style={{
                                            left: `${selRightPct}%`,
                                            width: 14, height: 56 * 0.7,
                                            borderRadius: 3,
                                            background: 'rgba(56,98,251,0.9)',
                                        }}
                                        onMouseDown={(e) => handleNavMouse(e, 'right')}
                                    >
                                        <span className="text-white text-[9px] font-bold pointer-events-none">▶</span>
                                    </div>
                                </div>
                            );
                        })()}
                    </div>
                    </>)}
            </div>
            )}
            </div>

            {/* Таблица фондов */}
            <div className="mt-6 bg-theme-secondary rounded-2xl border border-theme overflow-hidden">
                <div className="p-4 border-b border-theme flex items-center justify-between">
                    <h3 className="font-semibold">Фонды категории</h3>
                    <div className="flex items-center gap-2">
                        <span className="text-theme-secondary text-sm">Суммарная СЧА выбранных:</span>
                        <span className="text-[#2EE59D] font-mono font-bold">
                            {aggregatedData.totalCurrentNav.toFixed(2)} млрд ₽
                        </span>
                    </div>
                </div>
                <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                        <thead>
                            <tr className="text-theme-secondary text-left">
                                <th className="px-4 py-3 font-medium w-10"></th>
                                <th className="px-4 py-3 font-medium">Тикер</th>
                                <th className="px-4 py-3 font-medium">Название</th>
                                <th className="px-4 py-3 font-medium text-right">
                                    <button
                                        onClick={() => setNavSortDir(d => d === 'desc' ? 'asc' : 'desc')}
                                        className="inline-flex items-center gap-1 hover:text-theme-primary transition-colors"
                                    >
                                        СЧА
                                        <span className="text-xs">{navSortDir === 'desc' ? '↓' : '↑'}</span>
                                    </button>
                                </th>
                                <th className="px-4 py-3 font-medium text-right">Дата</th>
                            </tr>
                        </thead>
                        <tbody>
                            {(() => {
                                if (!data?.funds) return null;
                                // Группируем фонды по подкатегории
                                const groups: { subcat: string | null; funds: typeof data.funds }[] = [];
                                const subcatMap = new Map<string | null, typeof data.funds>();
                                for (const fund of data.funds) {
                                    const key = fund.subcategory || null;
                                    if (!subcatMap.has(key)) subcatMap.set(key, []);
                                    subcatMap.get(key)!.push(fund);
                                }
                                subcatMap.forEach((funds, subcat) => groups.push({
                                    subcat,
                                    funds: [...funds].sort((a, b) => {
                                        const navA = a.data[a.data.length - 1]?.nav ?? 0;
                                        const navB = b.data[b.data.length - 1]?.nav ?? 0;
                                        return navSortDir === 'desc' ? navB - navA : navA - navB;
                                    }),
                                }));

                                let globalIdx = 0;
                                return groups.map(({ subcat, funds: groupFunds }) => {
                                    const groupIds = groupFunds.map(f => f.fund_id);
                                    const allHidden = groupIds.every(id => hiddenFunds.has(id));
                                    const someHidden = groupIds.some(id => hiddenFunds.has(id));

                                    const toggleSubcat = () => {
                                        setHiddenFunds(prev => {
                                            const next = new Set(prev);
                                            if (allHidden) {
                                                groupIds.forEach(id => next.delete(id));
                                            } else {
                                                groupIds.forEach(id => next.add(id));
                                            }
                                            return next;
                                        });
                                    };

                                    const isCollapsed = subcat ? collapsedSubcats.has(subcat) : false;
                                    const toggleCollapse = () => {
                                        if (!subcat) return;
                                        setCollapsedSubcats(prev => {
                                            const next = new Set(prev);
                                            next.has(subcat) ? next.delete(subcat) : next.add(subcat);
                                            return next;
                                        });
                                    };

                                    return (
                                        <React.Fragment key={subcat || '__none__'}>
                                            {subcat && (
                                                <tr className="bg-white/[0.04] border-t-2 border-white/10">
                                                    <td className="px-4 py-3">
                                                        <div
                                                            className="flex items-center justify-center w-8 h-8 rounded-lg hover:bg-white/5 cursor-pointer transition-colors"
                                                            onClick={toggleSubcat}
                                                        >
                                                            <input
                                                                type="checkbox"
                                                                checked={!allHidden}
                                                                ref={el => { if (el) el.indeterminate = someHidden && !allHidden; }}
                                                                onChange={() => {}}
                                                                className="w-4 h-4 rounded border-white/20 bg-white/5 text-[#6366f1] focus:ring-[#6366f1] focus:ring-offset-[#121523] cursor-pointer"
                                                            />
                                                        </div>
                                                    </td>
                                                    <td colSpan={4} className="px-4 py-3 cursor-pointer select-none" onClick={toggleCollapse}>
                                                        <div className="flex items-center gap-2">
                                                            <span className="text-[10px] text-theme-secondary transition-transform duration-200" style={{ transform: isCollapsed ? 'rotate(-90deg)' : 'rotate(0deg)' }}>▼</span>
                                                            <span className="text-sm font-bold text-theme-primary">
                                                                {subcat}
                                                            </span>
                                                            <span className="text-xs text-theme-secondary">({groupFunds.length})</span>
                                                        </div>
                                                    </td>
                                                </tr>
                                            )}
                                            {isCollapsed && (() => { globalIdx += groupFunds.length; return null; })()}
                                            {!isCollapsed && groupFunds.map((fund) => {
                                                const colorIdx = globalIdx++;
                                                const lastData = fund.data[fund.data.length - 1];
                                                const isHidden = hiddenFunds.has(fund.fund_id);
                                                return (
                                                    <tr
                                                        key={fund.fund_id}
                                                        className={`border-t border-theme transition-colors ${isHidden ? 'opacity-50 grayscale' : 'hover:bg-white/5'}`}
                                                    >
                                                        <td className="px-4 py-3">
                                                            <div
                                                                className="flex items-center justify-center w-8 h-8 rounded-lg hover:bg-white/5 cursor-pointer transition-colors"
                                                                onClick={() => toggleFundVisibility(fund.fund_id)}
                                                            >
                                                                <input
                                                                    type="checkbox"
                                                                    checked={!isHidden}
                                                                    onChange={() => {}}
                                                                    className="w-4 h-4 rounded border-white/20 bg-white/5 text-[#6366f1] focus:ring-[#6366f1] focus:ring-offset-[#121523] cursor-pointer"
                                                                />
                                                            </div>
                                                        </td>
                                                        <td className="px-4 py-3 cursor-pointer" onClick={() => openFundCard(fund)}>
                                                            <div className="flex items-center gap-2">
                                                                {(() => {
                                                                    const uk = fund.uk_id ? UK_LOGOS[fund.uk_id] : null;
                                                                    if (uk) {
                                                                        return (
                                                                            <div className="w-7 h-7 rounded-full flex items-center justify-center flex-shrink-0 font-black text-sm"
                                                                                style={{ backgroundColor: uk.bg, color: uk.color }}>
                                                                                {uk.letter}
                                                                            </div>
                                                                        );
                                                                    }
                                                                    return (
                                                                        <div className="w-3 h-3 rounded-full flex-shrink-0"
                                                                            style={{ backgroundColor: FUND_COLORS[colorIdx % FUND_COLORS.length] }} />
                                                                    );
                                                                })()}
                                                                <span className="font-medium">{fund.ticker}</span>
                                                            </div>
                                                        </td>
                                                        <td className="px-4 py-3 text-theme-secondary cursor-pointer" onClick={() => openFundCard(fund)}>{fund.name}</td>
                                                        <td className="px-4 py-3 text-right font-mono">
                                                            {lastData?.nav ? `${(lastData.nav / 1e9).toFixed(2)} млрд ₽` : '—'}
                                                        </td>
                                                        <td className="px-4 py-3 text-right text-theme-secondary">
                                                            {lastData?.date || '—'}
                                                        </td>
                                                    </tr>
                                                );
                                            })}
                                        </React.Fragment>
                                    );
                                });
                            })()}
                        </tbody>
                    </table>
                </div>
            </div>

            {/* Модальная карточка фонда */}
            {selectedFund && (
                <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-md" onClick={() => setSelectedFund(null)}>
                    <div className="bg-white/[0.08] backdrop-blur-xl rounded-2xl border border-white/15 shadow-2xl w-full max-w-md mx-4 max-h-[80vh] overflow-hidden" style={{ boxShadow: '0 8px 32px rgba(0,0,0,0.4), inset 0 1px 0 rgba(255,255,255,0.1)' }} onClick={e => e.stopPropagation()}>
                        {/* Header */}
                        <div className="flex items-center justify-between p-5 border-b border-white/10">
                            <div className="flex items-center gap-3">
                                {selectedFund.uk_id && UK_LOGOS[selectedFund.uk_id] && (
                                    <div className="w-12 h-12 rounded-xl flex items-center justify-center flex-shrink-0 font-black text-xl"
                                        style={{ backgroundColor: UK_LOGOS[selectedFund.uk_id].bg, color: UK_LOGOS[selectedFund.uk_id].color }}>
                                        {UK_LOGOS[selectedFund.uk_id].letter}
                                    </div>
                                )}
                                <div>
                                    <div className="font-bold text-lg">{selectedFund.ticker}</div>
                                    <div className="text-sm text-theme-secondary">{selectedFund.name}</div>
                                </div>
                            </div>
                            <button onClick={() => setSelectedFund(null)} className="p-1 rounded-lg hover:bg-white/10 transition-colors">
                                <X size={20} />
                            </button>
                        </div>

                        {/* Info */}
                        <div className="px-5 py-3 border-b border-white/10 bg-white/[0.03] grid grid-cols-2 gap-3 text-sm">
                            {selectedFund.subcategory && (
                                <div>
                                    <span className="text-theme-secondary">Тип: </span>
                                    <span className="font-medium">{selectedFund.subcategory}</span>
                                </div>
                            )}
                            <div>
                                <span className="text-theme-secondary">СЧА: </span>
                                <span className="font-mono font-bold text-[#2EE59D]">
                                    {(() => {
                                        const last = selectedFund.data[selectedFund.data.length - 1];
                                        return last?.nav ? `${(last.nav / 1e9).toFixed(2)} млрд ₽` : '—';
                                    })()}
                                </span>
                            </div>
                        </div>

                        {/* Holdings — donut chart + list */}
                        <div className="px-5 py-3 overflow-y-auto" style={{ maxHeight: 'calc(80vh - 180px)' }}>
                            <div className="text-sm font-semibold text-theme-secondary mb-3">Состав фонда</div>
                            {holdingsLoading ? (
                                <div className="flex justify-center py-8">
                                    <div className="w-6 h-6 border-2 border-[#6366f1] border-t-transparent rounded-full animate-spin" />
                                </div>
                            ) : fundHoldings?.holdings?.length ? (
                                <div>
                                    {/* Donut Chart */}
                                    <div className="flex justify-center mb-4">
                                        <svg viewBox="0 0 200 200" width="180" height="180">
                                            {(() => {
                                                const DONUT_COLORS = ['#6366f1','#2EE59D','#4DA3FF','#FF4D4D','#FFB020','#00D9FF','#9D4DFF','#FF6B9D','#FCD34D','#14B8A6','#F97316','#818CF8'];
                                                const top = fundHoldings.holdings.slice(0, 10);
                                                const otherWeight = fundHoldings.holdings.slice(10).reduce((s, h) => s + h.weight, 0);
                                                const items = otherWeight > 0 ? [...top, { name: 'Прочее', weight: otherWeight }] : top;
                                                const total = items.reduce((s, h) => s + h.weight, 0);
                                                let cumAngle = -90;
                                                const cx = 100, cy = 100, r = 70, ir = 45;
                                                return items.map((h, i) => {
                                                    const angle = (h.weight / total) * 360;
                                                    const startRad = (cumAngle * Math.PI) / 180;
                                                    const endRad = ((cumAngle + angle) * Math.PI) / 180;
                                                    cumAngle += angle;
                                                    const x1 = cx + r * Math.cos(startRad), y1 = cy + r * Math.sin(startRad);
                                                    const x2 = cx + r * Math.cos(endRad), y2 = cy + r * Math.sin(endRad);
                                                    const ix1 = cx + ir * Math.cos(endRad), iy1 = cy + ir * Math.sin(endRad);
                                                    const ix2 = cx + ir * Math.cos(startRad), iy2 = cy + ir * Math.sin(startRad);
                                                    const largeArc = angle > 180 ? 1 : 0;
                                                    const d = `M ${x1} ${y1} A ${r} ${r} 0 ${largeArc} 1 ${x2} ${y2} L ${ix1} ${iy1} A ${ir} ${ir} 0 ${largeArc} 0 ${ix2} ${iy2} Z`;
                                                    return <path key={i} d={d} fill={DONUT_COLORS[i % DONUT_COLORS.length]} stroke="#1A1F2E" strokeWidth="1.5" />;
                                                });
                                            })()}
                                            <text x="100" y="96" textAnchor="middle" fill="white" fontSize="14" fontWeight="bold">
                                                {fundHoldings.holdings.length}
                                            </text>
                                            <text x="100" y="112" textAnchor="middle" fill="#9CA3B8" fontSize="10">
                                                позиций
                                            </text>
                                        </svg>
                                    </div>
                                    {/* Legend */}
                                    <div className="space-y-1.5">
                                        {(() => {
                                            const DONUT_COLORS = ['#6366f1','#2EE59D','#4DA3FF','#FF4D4D','#FFB020','#00D9FF','#9D4DFF','#FF6B9D','#FCD34D','#14B8A6','#F97316','#818CF8'];
                                            const top = fundHoldings.holdings.slice(0, 10);
                                            const otherWeight = fundHoldings.holdings.slice(10).reduce((s, h) => s + h.weight, 0);
                                            const items = otherWeight > 0 ? [...top, { name: `Прочее (${fundHoldings.holdings.length - 10})`, weight: otherWeight }] : top;
                                            return items.map((h, i) => (
                                                <div key={i} className="flex items-center gap-2 text-sm">
                                                    <div className="w-2.5 h-2.5 rounded-sm flex-shrink-0" style={{ backgroundColor: DONUT_COLORS[i % DONUT_COLORS.length] }} />
                                                    <span className="truncate flex-1">{h.name}</span>
                                                    <span className="font-mono text-theme-secondary flex-shrink-0">{(h.weight * 100).toFixed(1)}%</span>
                                                </div>
                                            ));
                                        })()}
                                    </div>
                                </div>
                            ) : (
                                <div className="text-center text-theme-secondary py-8 text-sm">
                                    Состав не публикуется
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
