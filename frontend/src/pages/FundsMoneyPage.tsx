import React, { useEffect, useLayoutEffect, useState, useMemo, useRef, useCallback } from 'react';
import { TrendingUp, DollarSign, Banknote, Gem, Wallet } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import PageHeader from '../components/PageHeader';
import Dropdown, { type DropdownOption } from '../components/Dropdown';
import { METHODOLOGY } from '../data/methodology';
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
import { useFitToViewport } from '../hooks/useFitToViewport';
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

// Цвета СЧА графика — theme-aware. Primary (СЧА) = accent (рыжий), secondary
// (индекс) = forest-green из funds-flow палитры — единый visual-язык с flows.
const INDEX_COLOR = 'var(--funds-flow-positive)';
const NAV_COLOR   = 'var(--accent)';

// Easing для анимации гистограммы
import { ANIMATION } from '../config/chartTheme';
const easeOutCubic = ANIMATION.easing;


export default function FundsMoneyPage() {
    const { isAuthenticated } = useAuth();
    const navigate = useNavigate();
    const [category, setCategory] = useState<FundCategory>('money_market');
    const [period, setPeriod] = useState<Period>(getDefaultPeriod('6m', isAuthenticated) as Period);
    // Default режим — Притоки-Оттоки (более информативно для нового пользователя)
    const [viewMode, setViewMode] = useState<ViewMode>('flows');
    const [flowTimeframe, setFlowTimeframeRaw] = useState<FlowTimeframe>('1d');

    // Динамическая высота графика — chartAnchorRef как в OI page
    const chartAnchorRef = useRef<HTMLDivElement>(null);
    const chartHeight = useFitToViewport(chartAnchorRef, {
        min: 360,
        max: 720,
        bottomBuffer: 96,
    });

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
    // Tooltip position через STATE а не DOM-мутацию — иначе после React re-render
    // позиция сбрасывается до следующего mousemove → визуальный "коэффициент".
    // Паттерн как в SeasonalityHistogram (handlePointerMove → setTooltip({x,y})).
    const [flowTooltipPos, setFlowTooltipPos] = useState<{ x: number; y: number } | null>(null);
    const [hoveredAnnotation, setHoveredAnnotation] = useState<string | null>(null); // date key
    const [showEvents, setShowEvents] = useState(false);
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
        if (!flowsData?.flows?.length || !flowContainerRef.current || !flowChartRef.current) return;
        // ВАЖНО: измеряем РЕАЛЬНУЮ геометрию через SVG-элемент, а не через
        // parseFloat(getComputedStyle(--chart-pad-left)). На mobile чарт-pad
        // задан через clamp() — getPropertyValue вернёт сырую строку
        // "clamp(34px, 11vw, 58px)", parseFloat = NaN → fallback 100. Реальный
        // pad на mobile ~41px → mismatch создаёт "коэффициент 2x" между
        // движением пальца и cursor'ом. SVG getBoundingClientRect даёт точные
        // computed pixels.
        const containerRect = flowContainerRef.current.getBoundingClientRect();
        const svgRect = flowChartRef.current.getBoundingClientRect();
        const xInChart = e.clientX - svgRect.left;
        if (xInChart < 0 || xInChart > svgRect.width) return;
        const visibleCount = flowNavRange[1] - flowNavRange[0] + 1;
        const barWidth = svgRect.width / visibleCount;
        const idx = Math.floor(xInChart / barWidth);
        if (idx >= 0 && idx < visibleCount) {
            setHoveredFlowIndex(idx);
            // x в координатах outer-container'а: offset до SVG + позиция в SVG
            const slotCenterInContainer = (svgRect.left - containerRect.left) + idx * barWidth + barWidth / 2;
            const y = e.clientY - containerRect.top;
            setFlowTooltipPos({ x: slotCenterInContainer, y });
        }
    };

    const handleFlowMouseLeave = useCallback(() => {
        setHoveredFlowIndex(null);
        setFlowTooltipPos(null);
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
        <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8 text-theme-primary min-h-screen">
            <PageHeader
                icon={Wallet}
                title="Деньги в фондах"
                subtitle="Динамика СЧА фондов и индексов"
                help={METHODOLOGY.fundsMoney}
                helpLink="/methodology/funds-money"
            />

            {/* Editorial frame — обнимает категории + controls + chart в один контейнер.
                Категории получают editorial-press effect (translate + 4×4 hard shadow). */}
            <div className="editorial-frame">

            {/* Вкладки категорий — pill-стиль с press-effect, fluid font/padding */}
            <div className="grid grid-cols-2 sm:grid-cols-4 mb-4 md:mb-6" style={{ gap: 'var(--sp-2)' }}>
                {CATEGORIES.map(cat => {
                    const Icon = cat.icon;
                    const isActive = category === cat.key;
                    return (
                        <button
                            key={cat.key}
                            onClick={() => setCategory(cat.key)}
                            className="editorial-press flex items-center font-semibold rounded-full min-w-0"
                            style={{
                                gap: 'var(--sp-2)',
                                padding: 'var(--sp-2) var(--sp-3)',
                                fontSize: 'var(--fs-sm)',
                                backgroundColor: isActive ? 'var(--accent)' : 'var(--bg-secondary)',
                                color: isActive ? 'var(--text-inverse)' : 'var(--text-primary)',
                                border: '2px solid var(--text-primary)',
                                boxShadow: isActive ? 'var(--shadow-hard-chip)' : undefined,
                            }}
                        >
                            <Icon className="shrink-0" style={{ width: 'var(--ico-sm)', height: 'var(--ico-sm)' }} />
                            <span className="truncate flex-1 text-left">{cat.name}</span>
                            <span
                                className="rounded-full shrink-0"
                                style={{
                                    fontSize: 'var(--fs-2xs)',
                                    padding: 'calc(var(--sp-1)) var(--sp-2)',
                                    background: isActive ? 'rgba(255,255,255,0.2)' : 'rgba(255,255,255,0.05)',
                                }}
                            >
                                {cat.index}
                            </span>
                        </button>
                    );
                })}
            </div>

            {/* Контролы */}
            <div className="flex flex-wrap mb-4 md:mb-6" style={{ gap: 'var(--sp-2)' }}>
                <Dropdown<Period>
                    options={(Object.keys(PERIOD_LABELS) as Period[])
                        .filter(p => AUM_PERIODS.includes(p))
                        .map((p): DropdownOption<Period> => ({
                            key: p,
                            label: PERIOD_LABELS[p],
                            locked: !isPeriodAllowed(p, isAuthenticated) || !isFlowPeriodAvailable(p),
                        }))}
                    value={period}
                    onChange={(p) => {
                        const allowed = isPeriodAllowed(p, isAuthenticated);
                        if (!allowed) { navigate('/login'); return; }
                        if (isFlowPeriodAvailable(p)) setPeriod(p);
                    }}
                />

                {/* Режим: СЧА / Притоки-оттоки */}
                <Dropdown<ViewMode>
                    options={[
                        { key: 'aum',   label: 'СЧА' },
                        { key: 'flows', label: 'Притоки-Оттоки' },
                    ]}
                    value={viewMode}
                    onChange={(m) => {
                        setViewMode(m);
                        if (m === 'aum' && !AUM_PERIODS.includes(period)) setPeriod('6m');
                    }}
                />

                {/* Таймфрейм для flows */}
                {viewMode === 'flows' && (
                    <Dropdown<FlowTimeframe>
                        options={[
                            { key: '1d', label: 'День' },
                            { key: '1w', label: 'Неделя' },
                            { key: '1m', label: 'Месяц' },
                        ]}
                        value={flowTimeframe}
                        onChange={setFlowTimeframe}
                    />
                )}

                {/* Тоггл событий */}
                {viewMode === 'flows' && (
                    <button
                        onClick={() => setShowEvents(!showEvents)}
                        className="editorial-press font-semibold rounded-full"
                        style={{
                            backgroundColor: showEvents ? 'var(--accent)' : 'var(--bg-secondary)',
                            color: showEvents ? 'var(--text-inverse)' : 'var(--text-primary)',
                            border: '2px solid var(--text-primary)',
                            boxShadow: showEvents ? 'var(--shadow-hard-chip)' : undefined,
                            fontSize: 'var(--fs-sm)',
                            padding: 'var(--sp-2) var(--sp-4)',
                        }}
                    >
                        События
                    </button>
                )}
            </div>

            {/* График */}
            {error ? (
                <div className="flex items-center justify-center" style={{ height: chartHeight }}>
                    <div className="text-theme-danger text-center">
                        <p className="text-lg font-medium">{error}</p>
                        <p className="text-sm text-theme-secondary mt-2">Попробуйте обновить страницу</p>
                    </div>
                </div>
            ) : viewMode === 'aum' ? (
                <div ref={chartAnchorRef}>
                    <SimpleChart
                        data={aggregatedData.chartData}
                        secondaryData={indexData}
                        height={chartHeight}
                        primaryColor={NAV_COLOR}
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
            <div ref={chartAnchorRef} style={{ ['--chart-height' as string]: `${chartHeight}px` }}>
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
                        flowTooltipPos={flowTooltipPos}
                        onMouseMove={handleFlowMouseMove}
                        onMouseLeave={handleFlowMouseLeave}
                        onSetHoveredAnnotation={setHoveredAnnotation}
                        onSetFlowNavRange={setFlowNavRange}
                    />
            </div>
            )}

            </div>{/* /editorial-frame */}

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
