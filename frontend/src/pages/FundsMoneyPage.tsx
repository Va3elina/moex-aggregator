import { useEffect, useState, useMemo, useRef, useCallback } from 'react';
import { TrendingUp, DollarSign, Banknote, LineChart, BarChart2, Gem, Wallet } from 'lucide-react';
import {
    getFundsChartData,
    getFundsFlows,
    type FundsChartResponse,
    type FundsFlowsResponse,
    type FundCategory,
    type FundPeriod,
    type FlowTimeframe
} from '../services/api';
import SimpleChart from '../components/SimpleChart';

// Режимы отображения
type ViewMode = 'aum' | 'flows';

// Периоды
type Period = '1w' | '1m' | '3m' | '6m' | '1y' | '2y' | '3y' | 'all';

const PERIOD_LABELS: Record<Period, string> = {
    '1w': '1Н',
    '1m': '1М',
    '3m': '3М',
    '6m': '6М',
    '1y': '1Г',
    '2y': '2Г',
    '3y': '3Г',
    'all': 'Всё'
};

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

const INDEX_COLOR = '#C8FF2E';

export default function FundsMoneyPage() {
    const [category, setCategory] = useState<FundCategory>('money_market');
    const [period, setPeriod] = useState<Period>('6m');
    const [viewMode, setViewMode] = useState<ViewMode>('aum');
    const [flowTimeframe, setFlowTimeframe] = useState<FlowTimeframe>('1w');
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [data, setData] = useState<FundsChartResponse | null>(null);
    const [flowsData, setFlowsData] = useState<FundsFlowsResponse | null>(null);
    const [hiddenFunds, setHiddenFunds] = useState<Set<number>>(new Set());
    const [hoveredFlowIndex, setHoveredFlowIndex] = useState<number | null>(null);
    const [flowHoverPos, setFlowHoverPos] = useState<{ x: number; y: number }>({ x: 0, y: 0 });
    const flowChartRef = useRef<SVGSVGElement>(null);
    const flowContainerRef = useRef<HTMLDivElement>(null);

    // Загрузка данных
    useEffect(() => {
        async function loadData() {
            try {
                setLoading(true);
                setError(null);
                const result = await getFundsChartData(category, period as FundPeriod);
                setData(result);
                // Сброс скрытых фондов при смене категории/периода
                setHiddenFunds(new Set());
            } catch (err) {
                setError('Ошибка загрузки данных');
                console.error(err);
            } finally {
                setLoading(false);
            }
        }
        loadData();
    }, [category, period]);

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

        // Суммируем NAV по датам
        const chartData = responsiveDates.map(date => {
            let totalNav = 0;
            visibleFunds.forEach(fund => {
                const point = fund.data.find(d => d.date === date);
                if (point?.nav) totalNav += point.nav;
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
    const formatNav = (value: number) => `${value.toFixed(1)} млрд ₽`;


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

    const handleFlowMouseMove = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
        if (!flowsData?.flows?.length || !flowContainerRef.current) return;
        const rect = flowContainerRef.current.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        const barWidth = rect.width / flowsData.flows.length;
        const idx = Math.floor(x / barWidth);
        if (idx >= 0 && idx < flowsData.flows.length) {
            setHoveredFlowIndex(idx);
            setFlowHoverPos({ x, y });
        }
    }, [flowsData]);

    const handleFlowMouseLeave = useCallback(() => {
        setHoveredFlowIndex(null);
    }, []);

    const currentCategory = CATEGORIES.find(c => c.key === category);

    return (
        <div className="max-w-7xl mx-auto px-4 md:px-6 py-6 md:py-8 text-theme-primary">
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
            <div className="flex gap-2 mb-6">
                {CATEGORIES.map(cat => {
                    const Icon = cat.icon;
                    const isActive = category === cat.key;
                    return (
                        <button
                            key={cat.key}
                            onClick={() => setCategory(cat.key)}
                            className={`flex items-center gap-2 px-4 py-3 rounded-xl font-medium transition-all duration-300 ${isActive
                                ? 'bg-[#6366f1] text-white shadow-lg shadow-[#6366f1]/25'
                                : 'bg-theme-secondary text-theme-secondary hover:text-theme-primary border border-theme'
                                }`}
                        >
                            <Icon className="w-5 h-5" />
                            <span>{cat.name}</span>
                            <span className={`text-xs px-2 py-0.5 rounded-full ${isActive ? 'bg-white/20' : 'bg-white/5'
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
                    {(Object.keys(PERIOD_LABELS) as Period[]).map((p) => (
                        <button
                            key={p}
                            onClick={() => setPeriod(p)}
                            className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-all duration-300 ${period === p
                                ? 'btn-control active'
                                : 'text-theme-secondary hover:text-theme-primary'
                                }`}
                        >
                            {PERIOD_LABELS[p]}
                        </button>
                    ))}
                </div>

                {/* Режим: СЧА / Притоки-оттоки */}
                <div className="flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                    <button
                        onClick={() => setViewMode('aum')}
                        title="СЧА (объём активов)"
                        className={`flex items-center gap-2 px-3 py-1.5 rounded-lg transition-all duration-300 ${viewMode === 'aum'
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
                        className={`flex items-center gap-2 px-3 py-1.5 rounded-lg transition-all duration-300 ${viewMode === 'flows'
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
                                className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-all duration-300 ${flowTimeframe === tf
                                    ? 'btn-control active'
                                    : 'text-theme-secondary hover:text-theme-primary'
                                    }`}
                            >
                                {tf === '1d' ? 'День' : tf === '1w' ? 'Неделя' : 'Месяц'}
                            </button>
                        ))}
                    </div>
                )}
            </div>

            {/* График */}
            {error ? (
                <div className="flex items-center justify-center h-[450px] bg-theme-secondary rounded-2xl border border-theme mb-6">
                    <div className="text-[#FF4D4D] text-center">
                        <p className="text-lg font-medium">{error}</p>
                        <p className="text-sm text-theme-secondary mt-2">Попробуйте обновить страницу</p>
                    </div>
                </div>
            ) : viewMode === 'aum' ? (
                <div className="mb-6">
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
                    />
                </div>
            ) : (
            <div className="bg-theme-secondary rounded-2xl p-6 border border-theme mb-6">
                {loading && !flowsData ? (
                    <div className="flex items-center justify-center h-[450px]">
                        <div className="flex flex-col items-center gap-3">
                            <div className="w-8 h-8 border-2 border-[#6366f1] border-t-transparent rounded-full animate-spin" />
                            <span className="text-theme-secondary">Загрузка...</span>
                        </div>
                    </div>
                ) : (
                    /* Гистограмма притоков/оттоков */
                    <div className="h-[450px]">
                        {/* Заголовок — всегда показывает суммарное значение за период */}
                        <div className="mb-2 flex items-baseline gap-4">
                            <span className="text-4xl font-bold text-theme-primary tracking-tight">
                                {(() => {
                                    const total = flowsData?.flows.reduce((sum, f) => sum + f.flow, 0) ?? 0;
                                    return `${total > 0 ? '+' : ''}${total.toFixed(1)} млрд ₽`;
                                })()}
                            </span>
                            <span className="text-sm text-theme-secondary">за период</span>
                        </div>

                        {/* График с тултипом */}
                        <div
                            ref={flowContainerRef}
                            className="h-[370px] relative cursor-crosshair"
                            onMouseMove={handleFlowMouseMove}
                            onMouseLeave={handleFlowMouseLeave}
                        >
                            <svg
                                ref={flowChartRef}
                                width="100%"
                                height="100%"
                                preserveAspectRatio="none"
                            >
                                {flowsData?.flows && (() => {
                                    const flows = flowsData.flows;
                                    const maxAbsFlow = Math.max(...flows.map(f => Math.abs(f.flow)), 1);
                                    const barWidth = 100 / (flows.length || 1);
                                    const midY = 50;

                                    return flows.map((f, i) => {
                                        const h = (Math.abs(f.flow) / maxAbsFlow) * 45;
                                        const isPositive = f.flow >= 0;
                                        const y = isPositive ? midY - h : midY;
                                        const baseColor = isPositive ? '#2EE59D' : '#FF4D4D';
                                        const isHovered = hoveredFlowIndex === i;

                                        return (
                                            <rect
                                                key={i}
                                                x={`${i * barWidth + barWidth * 0.1}%`}
                                                y={`${y}%`}
                                                width={`${barWidth * 0.8}%`}
                                                height={`${h}%`}
                                                fill={baseColor}
                                                opacity={hoveredFlowIndex === null ? 1 : isHovered ? 1 : 0.35}
                                                rx="2"
                                                className="transition-opacity duration-150"
                                            />
                                        );
                                    });
                                })()}
                                {/* Центральная линия */}
                                <line x1="0" y1="50%" x2="100%" y2="50%" stroke="#2A2F3E" strokeWidth="1" />
                                {/* Вертикальный курсор */}
                                {hoveredFlowIndex !== null && flowsData?.flows && (() => {
                                    const barWidth = 100 / flowsData.flows.length;
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
                            </svg>

                            {/* Тултип-карточка */}
                            {hoveredFlowIndex !== null && flowsData?.flows[hoveredFlowIndex] && flowContainerRef.current && (() => {
                                const f = flowsData.flows[hoveredFlowIndex];
                                const containerW = flowContainerRef.current!.getBoundingClientRect().width;
                                const barWidth = containerW / flowsData.flows.length;
                                const hoverX = hoveredFlowIndex * barWidth + barWidth / 2;
                                const isRightHalf = hoverX > containerW / 2;
                                const cardLeft = isRightHalf ? hoverX - 180 - 12 : hoverX + 12;

                                const dateStr = new Date(f.period_end).toLocaleDateString('ru-RU', {
                                    day: 'numeric', month: 'short', year: 'numeric'
                                });

                                const flowColor = f.flow >= 0 ? '#2EE59D' : '#FF4D4D';

                                return (
                                    <>
                                        {/* Дата над crosshair */}
                                        <div
                                            className="absolute z-30 pointer-events-none"
                                            style={{
                                                left: Math.min(Math.max(hoverX - 50, 4), containerW - 104),
                                                top: 4,
                                            }}
                                        >
                                            <span className="text-[11px] text-theme-secondary bg-theme-tertiary/90 backdrop-blur-sm px-2 py-0.5 rounded border border-theme whitespace-nowrap">
                                                {dateStr}
                                            </span>
                                        </div>

                                        {/* Карточка значений */}
                                        <div
                                            className="absolute z-30 pointer-events-none"
                                            style={{
                                                left: cardLeft,
                                                top: Math.min(Math.max(flowHoverPos.y - 25, 28), 320),
                                            }}
                                        >
                                            <div className="bg-theme-tertiary/95 backdrop-blur-sm rounded-lg border border-theme shadow-xl py-1.5 px-3">
                                                <div className="flex items-center justify-between gap-3 py-0.5">
                                                    <div className="flex items-center gap-1.5">
                                                        <span className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: flowColor }} />
                                                        <span className="text-[11px] text-theme-secondary">{f.flow >= 0 ? 'Приток' : 'Отток'}</span>
                                                    </div>
                                                    <span className="text-xs font-semibold text-theme-primary whitespace-nowrap">
                                                        {f.flow > 0 ? '+' : ''}{f.flow.toFixed(2)} млрд ₽
                                                    </span>
                                                </div>
                                                <div className="flex items-center justify-between gap-3 py-0.5">
                                                    <div className="flex items-center gap-1.5">
                                                        <span className="w-2 h-2 rounded-full flex-shrink-0 bg-[#6366f1]" />
                                                        <span className="text-[11px] text-theme-secondary">Изменение</span>
                                                    </div>
                                                    <span className="text-xs font-semibold text-theme-primary whitespace-nowrap">
                                                        {f.flow_pct > 0 ? '+' : ''}{f.flow_pct.toFixed(2)}%
                                                    </span>
                                                </div>
                                            </div>
                                        </div>
                                    </>
                                );
                            })()}

                            {/* Даты оси X */}
                            <div className="absolute bottom-0 left-0 right-0 flex justify-between text-xs text-theme-secondary px-2">
                                {flowsData?.flows && flowsData.flows.length > 0 && (
                                    <>
                                        <span>{new Date(flowsData.flows[0].period_start).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' })}</span>
                                        <span>{new Date(flowsData.flows[flowsData.flows.length - 1].period_end).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' })}</span>
                                    </>
                                )}
                            </div>
                        </div>
                        <div className="flex items-center justify-center gap-6 mt-4 text-sm">
                            <span className="flex items-center gap-2">
                                <span className="w-3 h-3 rounded-full bg-[#2EE59D]" />
                                <span className="text-theme-secondary">Приток</span>
                            </span>
                            <span className="flex items-center gap-2">
                                <span className="w-3 h-3 rounded-full bg-[#FF4D4D]" />
                                <span className="text-theme-secondary">Отток</span>
                            </span>
                        </div>
                    </div>
                )}
            </div>
            )}

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
                                <th className="px-4 py-3 font-medium text-right">Последняя СЧА</th>
                                <th className="px-4 py-3 font-medium text-right">Дата</th>
                            </tr>
                        </thead>
                        <tbody>
                            {data?.funds?.map((fund, i) => {
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
                                                    onChange={() => { }} // Обработка клика на div
                                                    className="w-4 h-4 rounded border-white/20 bg-white/5 text-[#6366f1] focus:ring-[#6366f1] focus:ring-offset-[#121523] cursor-pointer"
                                                />
                                            </div>
                                        </td>
                                        <td className="px-4 py-3">
                                            <div className="flex items-center gap-2">
                                                <div
                                                    className="w-3 h-3 rounded-full"
                                                    style={{ backgroundColor: FUND_COLORS[i % FUND_COLORS.length] }}
                                                />
                                                <span className="font-medium">{fund.ticker}</span>
                                            </div>
                                        </td>
                                        <td className="px-4 py-3 text-theme-secondary">{fund.name}</td>
                                        <td className="px-4 py-3 text-right font-mono">
                                            {lastData?.nav ? `${(lastData.nav / 1e9).toFixed(2)} млрд ₽` : '—'}
                                        </td>
                                        <td className="px-4 py-3 text-right text-theme-secondary">
                                            {lastData?.date || '—'}
                                        </td>
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    );
}
