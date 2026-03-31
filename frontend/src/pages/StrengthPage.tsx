import { memo, useEffect, useLayoutEffect, useState, useMemo, useRef, useCallback } from 'react';
import { TrendingUp, Activity, ArrowUp, ArrowDown, BarChart2, LineChart, Filter, Lock } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import ChartNavigator from '../components/ChartNavigator';
import {
    getBreadthCurrent,
    getBreadthHistory,
    type BreadthCurrentResponse,
    type BreadthHistoryResponse,
    type BreadthUniverse,
} from '../services/api';
import { useAuth } from '../contexts/AuthContext';
import { isPeriodAllowed, getDefaultPeriod } from '../config/accessControl';
import { useRealtimeData } from '../hooks/useRealtimeData';

type Period = '6m' | '1y' | '2y' | 'all';
type ChartMode = 'line' | 'histogram';

const PERIOD_LABELS: Record<Period, string> = {
    '6m': '6М',
    '1y': '1Г',
    '2y': '2Г',
    'all': 'Всё'
};

const PERIOD_DAYS: Record<Period, number> = {
    '6m': 180,
    '1y': 365,
    '2y': 730,
    'all': 7000
};

const EMA_PERIOD = 200; // Fixed EMA period

const CLASSIFICATION_LABELS: Record<string, { label: string; color: string; bg: string }> = {
    overbought: { label: 'Перекупленность', color: 'text-emerald-400', bg: 'bg-emerald-500/20' },
    bullish: { label: 'Бычий тренд', color: 'text-emerald-300', bg: 'bg-emerald-500/10' },
    neutral: { label: 'Нейтрально', color: 'text-amber-400', bg: 'bg-amber-500/20' },
    oversold: { label: 'Перепроданность', color: 'text-red-400', bg: 'bg-red-500/20' },
};

// Секторы строятся динамически из ответа API (поле sector)


// Константа уровня модуля — стабильная ссылка, не пересоздаётся при рендерах.
// Это критично: SyncedPriceChart/SyncedBreadthChart используют padding в useMemo-deps,
// пересоздание объекта каждый рендер сбрасывает chartData и прерывает морфинг-анимацию.
const EDGE_MARGIN = 8;
const CHART_PADDING = { left: 45, right: 65, top: 10, bottom: 30 } as const;

export default function StrengthPage() {
    const { isAuthenticated } = useAuth();
    const navigate = useNavigate();
    const [period, setPeriod] = useState<Period>(getDefaultPeriod('1y', isAuthenticated) as Period);
    const emaPeriod = EMA_PERIOD; // Fixed EMA200
    const [chartMode, setChartMode] = useState<ChartMode>('histogram');
    const [showPrice, setShowPrice] = useState(true);
    const [selectedSector, setSelectedSector] = useState('Все');
    const [universe, setUniverse] = useState<BreadthUniverse>('all');

    const [current, setCurrent] = useState<BreadthCurrentResponse | null>(null);
    const [history, setHistory] = useState<BreadthHistoryResponse | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    // Синхронизированный hover между графиками
    const [hoverIndex, setHoverIndex] = useState<number | null>(null);
    const [hoverY, setHoverY] = useState<number>(0);

    const containerRef = useRef<HTMLDivElement>(null);
    const mouseMoveRaf = useRef<number | null>(null);
    const isNavDragRef = useRef(false);

    const loadData = useCallback(async () => {
        // Показываем loading только при первой загрузке (нет данных)
        if (!current && !history) setLoading(true);
        setError(null);
        try {
            const [currentData, historyData] = await Promise.all([
                getBreadthCurrent(emaPeriod, universe),
                getBreadthHistory(emaPeriod, PERIOD_DAYS[period], universe)
            ]);
            setCurrent(currentData);
            setHistory(historyData);
        } catch (err) {
            setError('Не удалось загрузить данные');
            console.error(err);
        } finally {
            setLoading(false);
        }
    }, [emaPeriod, period, universe]);

    useEffect(() => { loadData(); }, [loadData]);

    // SSE: автоматическое обновление при новых данных
    useRealtimeData(['daily', 'breadth'], loadData);

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

    // Навигатор временного диапазона
    const [navRange, setNavRange] = useState<[number, number]>([0, 0]);
    useEffect(() => {
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

    const displaySyncedData = useMemo(() => {
        if (!syncedData.length) return syncedData;
        return syncedData.slice(navRange[0], navRange[1] + 1);
    }, [syncedData, navRange]);

    // Данные для мини-графика навигатора (мемоизированы — стабильная ссылка)
    const navigatorData = useMemo(
        () => syncedData.map(d => ({ time: d.time, value: d.breadth })),
        [syncedData]
    );

    // Динамические секторы из ответа API
    const sectorNames = useMemo(() => {
        if (!current?.stocks) return ['Все'];
        const names = new Set(current.stocks.map(s => s.sector).filter(Boolean));
        return ['Все', ...Array.from(names).sort()];
    }, [current?.stocks]);

    // Реальное количество акций в каждом секторе
    const sectorCounts = useMemo(() => {
        if (!current?.stocks) return {};
        const counts: Record<string, number> = { 'Все': current.stocks.length };
        for (const stock of current.stocks) {
            const sec = stock.sector || 'Другое';
            counts[sec] = (counts[sec] || 0) + 1;
        }
        return counts;
    }, [current?.stocks]);

    // Фильтрованные акции по сектору
    const filteredStocks = useMemo(() => {
        if (!current?.stocks) return [];
        if (selectedSector === 'Все') return current.stocks;

        return current.stocks.filter(s => s.sector === selectedSector);
    }, [current?.stocks, selectedSector]);

    // Сбросить сектор если он стал пустым после обновления данных
    useEffect(() => {
        if (selectedSector !== 'Все' && (sectorCounts[selectedSector] ?? 0) === 0) {
            setSelectedSector('Все');
        }
    }, [sectorCounts, selectedSector]);

    const classInfo = current?.classification
        ? CLASSIFICATION_LABELS[current.classification]
        : CLASSIFICATION_LABELS.neutral;

    // Общие размеры графиков
    const padding = CHART_PADDING;

    const handleMouseMove = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
        if (!containerRef.current || !displaySyncedData.length) return;
        if (mouseMoveRaf.current !== null) return; // throttle до одного RAF

        const clientX = e.clientX;
        const clientY = e.clientY;

        mouseMoveRaf.current = requestAnimationFrame(() => {
            mouseMoveRaf.current = null;
            if (!containerRef.current) return;
            const rect = containerRef.current.getBoundingClientRect();
            const px4 = 16; // px-4 на внутренних div-обёртках графиков
            const x = clientX - rect.left - px4 - padding.left;
            const y = clientY - rect.top;
            const chartWidth = rect.width - 2 * px4 - padding.left - padding.right;
            const idx = Math.round((x / chartWidth) * (displaySyncedData.length - 1));
            if (idx >= 0 && idx < displaySyncedData.length) {
                setHoverIndex(idx);
                setHoverY(y);
            }
        });
    }, [displaySyncedData.length, padding.left, padding.right]);

    const handleMouseLeave = useCallback(() => {
        if (mouseMoveRaf.current !== null) {
            cancelAnimationFrame(mouseMoveRaf.current);
            mouseMoveRaf.current = null;
        }
        setHoverIndex(null);
    }, []);

    // Текущие значения для тултипа
    const hoverData = hoverIndex !== null && displaySyncedData[hoverIndex] ? displaySyncedData[hoverIndex] : null;

    const stocksAbove = current?.stocks ? current.stocks.filter(s => s.is_above).length : current?.count_above ?? 0;
    const stocksTotal = current?.stocks?.length ?? current?.count_total ?? 0;

    return (
        <div className="max-w-7xl mx-auto px-4 md:px-6 py-6 md:py-8 min-h-screen">
            {/* Заголовок */}
            <div className="flex items-center gap-3 mb-6">
                <div className="p-3 bg-gradient-to-br from-[#8b5cf6] to-[#ec4899] rounded-xl">
                    <TrendingUp className="w-6 h-6 text-white" />
                </div>
                <div>
                    <h1 className="text-2xl font-bold text-theme-primary">Сила рынка</h1>
                    <p className="text-theme-secondary text-sm">
                        % {universe === 'imoex' ? 'акций индекса MOEX' : 'акций'} выше EMA{emaPeriod}
                    </p>
                </div>
            </div>

            {/* Контролы — одна строка */}
            <div className="flex items-center gap-4 mb-6 flex-wrap">
                {/* Период */}
                <div className="flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                    {(Object.entries(PERIOD_LABELS) as [Period, string][]).map(([key, label]) => {
                        const allowed = isPeriodAllowed(key, isAuthenticated);
                        return (
                            <button
                                key={key}
                                onClick={() => {
                                    if (!allowed) { navigate('/login'); return; }
                                    setPeriod(key);
                                }}
                                title={!allowed ? 'Войдите для доступа' : undefined}
                                className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors ${
                                    !allowed
                                        ? 'text-theme-muted cursor-not-allowed opacity-50'
                                        : period === key
                                            ? 'btn-control active'
                                            : 'text-theme-secondary hover:text-theme-primary'
                                }`}
                            >
                                {label}
                                {!allowed && <Lock className="inline-block ml-0.5 w-3 h-3" />}
                            </button>
                        );
                    })}
                </div>

                {/* Тип графика breadth */}
                <div className="flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                    <button
                        onClick={() => setChartMode('line')}
                        className={`p-2 rounded-lg transition-colors ${chartMode === 'line' ? 'bg-white/10' : ''}`}
                        title="Линия"
                    >
                        <LineChart size={18} className={chartMode === 'line' ? 'text-theme-accent' : 'text-theme-secondary'} />
                    </button>
                    <button
                        onClick={() => setChartMode('histogram')}
                        className={`p-2 rounded-lg transition-colors ${chartMode === 'histogram' ? 'bg-white/10' : ''}`}
                        title="Гистограмма"
                    >
                        <BarChart2 size={18} className={chartMode === 'histogram' ? 'text-theme-accent' : 'text-theme-secondary'} />
                    </button>
                </div>

                {/* Вселенная: все акции / IMOEX */}
                <div className="flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                    <button
                        onClick={() => setUniverse('all')}
                        className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors ${
                            universe === 'all' ? 'btn-control active' : 'text-theme-secondary hover:text-theme-primary'
                        }`}
                    >
                        Все акции
                    </button>
                    <button
                        onClick={() => setUniverse('imoex')}
                        className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors ${
                            universe === 'imoex' ? 'btn-control active' : 'text-theme-secondary hover:text-theme-primary'
                        }`}
                    >
                        Индекс MOEX
                    </button>
                </div>

                {/* Показать IMOEX */}
                <button
                    onClick={() => setShowPrice(!showPrice)}
                    className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors border ${showPrice ? 'bg-indigo-500/20 border-indigo-500/50 text-indigo-400' : 'border-theme text-theme-secondary'
                        }`}
                >
                    IMOEX
                </button>

                {/* Статус + счётчик — справа */}
                {current && (
                    <div className="flex items-center gap-3 sm:ml-auto">
                        <span className="text-sm text-theme-secondary">
                            <span className="font-bold text-theme-primary">{stocksAbove}</span>/{stocksTotal} выше EMA
                        </span>
                        <div className={`px-3 py-1 rounded-full ${classInfo.bg}`}>
                            <span className={`text-xs font-medium ${classInfo.color}`}>
                                {classInfo.label}
                            </span>
                        </div>
                    </div>
                )}
            </div>

            {/* Синхронизированные графики */}
            <div
                ref={containerRef}
                className="bg-theme-secondary rounded-2xl border border-theme mb-6 relative cursor-crosshair min-h-[500px]"
                onMouseMove={isAnimating ? undefined : handleMouseMove}
                onMouseLeave={handleMouseLeave}
            >
                {/* Полный loading / error на месте графика */}
                {loading && !current ? (
                    <div className="flex items-center justify-center h-[400px]">
                        <div className="flex flex-col items-center gap-3">
                            <div className="w-8 h-8 border-2 border-[#8b5cf6] border-t-transparent rounded-full animate-spin" />
                            <span className="text-theme-secondary">Загрузка...</span>
                        </div>
                    </div>
                ) : error && !current ? (
                    <div className="flex items-center justify-center h-[400px]">
                        <div className="text-center">
                            <Activity className="w-12 h-12 text-red-400 mx-auto mb-3" />
                            <p className="text-red-400">{error}</p>
                        </div>
                    </div>
                ) : (<>
                    {/* Индикатор обновления данных */}
                    {loading && syncedData.length > 0 && (
                        <div className="absolute top-3 right-4 z-20 flex items-center gap-2 bg-theme-tertiary/90 backdrop-blur-sm px-3 py-1.5 rounded-lg border border-theme">
                            <div className="w-4 h-4 border-2 border-[#C8FF2E] border-t-transparent rounded-full animate-spin" />
                            <span className="text-xs text-theme-secondary">Обновление...</span>
                        </div>
                    )}

                    {/* Тултип-карточка при hover — следует за мышью между графиками */}
                    {hoverData && hoverIndex !== null && containerRef.current && (() => {
                        const rect = containerRef.current!.getBoundingClientRect();
                        const px4 = 16; // px-4 на внутренних div-обёртках графиков
                        const chartWidth = rect.width - 2 * px4 - padding.left - padding.right;
                        const hoverX = px4 + padding.left + (hoverIndex / Math.max(displaySyncedData.length - 1, 1)) * chartWidth;
                        const isRightHalf = hoverX > px4 + padding.left + chartWidth / 2;
                        const cardLeft = isRightHalf ? hoverX - 190 - 12 : hoverX + 12;

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
                                {/* Дата — закреплена наверху, привязана к вертикальной линии */}
                                <div
                                    className="absolute z-20 pointer-events-none"
                                    style={{
                                        left: Math.min(Math.max(hoverX - 60, 4), rect.width - 128),
                                        top: 38,
                                    }}
                                >
                                    <span className="text-[11px] text-theme-secondary bg-theme-tertiary/90 backdrop-blur-sm px-2 py-0.5 rounded border border-theme whitespace-nowrap">
                                        {dateStr}
                                    </span>
                                </div>

                                {/* Карточка значений — следует за курсором */}
                                <div
                                    className="absolute z-30 pointer-events-none"
                                    style={{
                                        left: cardLeft,
                                        top: clampedCardTop,
                                    }}
                                >
                                    <div className="bg-theme-tertiary/95 backdrop-blur-sm rounded-lg border border-theme shadow-xl py-1.5 px-3">
                                        {showPrice && (
                                            <div className="flex items-center justify-between gap-3 py-0.5">
                                                <div className="flex items-center gap-1.5">
                                                    <span className="w-2 h-2 rounded-full flex-shrink-0 bg-[#6366f1]" />
                                                    <span className="text-[11px] text-theme-secondary">IMOEX</span>
                                                </div>
                                                <span className="text-xs font-semibold text-theme-primary whitespace-nowrap">
                                                    {hoverData.imoex.toLocaleString('ru-RU', { maximumFractionDigits: 0 })}
                                                </span>
                                            </div>
                                        )}
                                        <div className="flex items-center justify-between gap-3 py-0.5">
                                            <div className="flex items-center gap-1.5">
                                                <span className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: breadthColor }} />
                                                <span className="text-[11px] text-theme-secondary">% выше EMA</span>
                                            </div>
                                            <span className="text-xs font-semibold text-theme-primary whitespace-nowrap">
                                                {hoverData.breadth.toFixed(1)}%
                                            </span>
                                        </div>
                                    </div>
                                </div>
                            </>
                        );
                    })()}

                    {/* График IMOEX (верхний) */}
                    {showPrice && (
                        <div className="px-4 pt-4 pb-2 border-b border-theme relative bg-theme-secondary" style={{ minHeight: 334 }}>
                            <div className="flex items-center justify-center gap-2 mb-5 relative z-10">
                                <span className="w-3 h-3 rounded-full bg-[#6366f1]" />
                                <span className="text-sm font-semibold text-theme-primary">Индекс МосБиржи</span>
                            </div>
                            <SyncedPriceChart
                                syncedData={displaySyncedData}
                                hoverIndex={hoverIndex}
                                height={300}
                                padding={padding}
                                isNavDragRef={isNavDragRef}
                            />
                        </div>
                    )}

                    {/* График Breadth (нижний) — расширяется когда IMOEX скрыт */}
                    <div className="px-4 pt-3 pb-2 relative bg-theme-secondary" style={{ minHeight: showPrice ? 174 : 474 }}>
                        <div className="flex items-center justify-center gap-2 mb-2 relative z-10">
                            <span className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: 'var(--accent)' }} />
                            <span className="text-sm font-semibold text-theme-primary">% акций выше EMA{emaPeriod}</span>
                        </div>
                        {displaySyncedData.length > 0 ? (
                            <SyncedBreadthChart
                                syncedData={displaySyncedData}
                                hoverIndex={hoverIndex}
                                height={showPrice ? 150 : 450}
                                mode={chartMode}
                                padding={padding}
                                isNavDragRef={isNavDragRef}
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
                            className="px-4 pb-3"
                            onMouseMove={e => e.stopPropagation()}
                            onMouseEnter={() => setHoverIndex(null)}
                        >
                            <ChartNavigator
                                data={navigatorData}
                                onChange={(s, e, isDrag) => { isNavDragRef.current = isDrag; setNavRange([s, e]); }}
                                color="#8b5cf6"
                            />
                        </div>
                    )}
                </>)}
            </div>

            {/* Таблица акций с фильтром по секторам */}
            {current?.stocks && (
                <div className="widget p-4 md:p-6">
                    <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-4">
                        <h3 className="text-lg font-semibold text-theme-primary">
                            Детализация по акциям
                        </h3>

                        {/* Фильтр по секторам */}
                        <div className="flex flex-wrap items-center gap-2">
                            <Filter size={16} className="text-theme-muted" />
                            {sectorNames
                                .filter(sector => (sectorCounts[sector] ?? 0) > 0)
                                .map((sector) => (
                                    <button
                                        key={sector}
                                        onClick={() => setSelectedSector(sector)}
                                        className={`px-3 py-1 text-xs font-medium rounded-full transition-colors ${selectedSector === sector
                                            ? 'bg-white/10 text-theme-primary border border-theme'
                                            : 'text-theme-secondary hover:text-theme-primary'
                                            }`}
                                    >
                                        {sector}
                                        {sector !== 'Все' && (
                                            <span className="ml-1 opacity-50">({sectorCounts[sector]})</span>
                                        )}
                                    </button>
                                ))}
                        </div>
                    </div>

                    <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                            <thead>
                                <tr className="text-theme-muted border-b border-theme">
                                    <th className="text-left py-3 px-2">Тикер</th>
                                    <th className="text-right py-3 px-2">Цена</th>
                                    <th className="text-right py-3 px-2">EMA{emaPeriod}</th>
                                    <th className="text-right py-3 px-2">Отклонение</th>
                                    <th className="text-center py-3 px-2">Статус</th>
                                </tr>
                            </thead>
                            <tbody>
                                {filteredStocks.map((stock) => (
                                    <tr key={stock.ticker} className="border-b border-theme/50 hover:bg-white/5">
                                        <td className="py-3 px-2 font-medium text-theme-primary">
                                            {stock.ticker}
                                        </td>
                                        <td className="py-3 px-2 text-right text-theme-primary">
                                            {stock.price.toLocaleString('ru-RU', { maximumFractionDigits: 2 })}
                                        </td>
                                        <td className="py-3 px-2 text-right text-theme-secondary">
                                            {stock.ema.toLocaleString('ru-RU', { maximumFractionDigits: 2 })}
                                        </td>
                                        <td className={`py-3 px-2 text-right font-medium ${stock.diff_percent >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                                            <span className="flex items-center justify-end gap-1">
                                                {stock.diff_percent >= 0 ? <ArrowUp size={14} /> : <ArrowDown size={14} />}
                                                {Math.abs(stock.diff_percent).toFixed(2)}%
                                            </span>
                                        </td>
                                        <td className="py-3 px-2 text-center">
                                            <span className={`inline-block px-2 py-1 rounded-full text-xs font-medium ${stock.is_above
                                                ? 'bg-emerald-500/20 text-emerald-400'
                                                : 'bg-red-500/20 text-red-400'
                                                }`}>
                                                {stock.is_above ? 'Выше' : 'Ниже'}
                                            </span>
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>

                        {filteredStocks.length === 0 && (
                            <div className="py-8 text-center text-theme-muted">
                                Нет акций в выбранном секторе
                            </div>
                        )}
                    </div>
                </div>
            )}
        </div>
    );
}

// Утилиты анимации
const lerp = (a: number, b: number, t: number) => a + (b - a) * t;
const easeOutCubic = (t: number) => 1 - Math.pow(1 - t, 3);

const resamplePts = (pts: { x: number; y: number }[], len: number) => {
    if (pts.length === 0) return [];
    if (pts.length === len) return pts;
    const out: { x: number; y: number }[] = [];
    for (let i = 0; i < len; i++) {
        const t = i / (len - 1);
        const si = t * (pts.length - 1);
        const lo = Math.floor(si);
        const hi = Math.min(lo + 1, pts.length - 1);
        const lt = si - lo;
        out.push({ x: lerp(pts[lo].x, pts[hi].x, lt), y: lerp(pts[lo].y, pts[hi].y, lt) });
    }
    return out;
};

const morphPts = (from: { x: number; y: number }[], to: { x: number; y: number }[], t: number) => {
    const n = Math.max(from.length, to.length);
    const a = resamplePts(from, n);
    const b = resamplePts(to, n);
    return a.map((p, i) => ({ x: lerp(p.x, b[i].x, t), y: lerp(p.y, b[i].y, t) }));
};

const ptsToPath = (pts: { x: number; y: number }[]) =>
    pts.length ? pts.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ') : '';

const ptsToArea = (pts: { x: number; y: number }[], bottom: number) => {
    if (!pts.length) return '';
    return ptsToPath(pts) + ` L ${pts[pts.length - 1].x} ${bottom} L ${pts[0].x} ${bottom} Z`;
};

// Синхронизированный график цены с морфинг-анимацией
function SyncedPriceChart({
    syncedData,
    hoverIndex,
    height,
    padding,
    isNavDragRef,
}: {
    syncedData: { time: string; breadth: number; imoex: number }[];
    hoverIndex: number | null;
    height: number;
    padding: { left: number; right: number; top: number; bottom: number };
    isNavDragRef?: { current: boolean };
}) {
    const svgRef = useRef<SVGSVGElement>(null);
    const chartWrapRef = useRef<HTMLDivElement>(null);
    const containerRef = useRef<HTMLDivElement>(null);
    const [width, setWidth] = useState(0);
    const [animLinePath, setAnimLinePath] = useState('');
    const [animAreaPath, setAnimAreaPath] = useState('');
    const prevPtsRef = useRef<{ x: number; y: number }[]>([]);
    const currPtsRef = useRef<{ x: number; y: number }[]>([]); // текущая визуальная позиция
    const animRef = useRef<number | null>(null);
    const isFirstRef = useRef(true);
    const prevWidthRef = useRef(0);
    const [revealed, setRevealed] = useState(false);

    useEffect(() => {
        const el = containerRef.current;
        if (!el) return;
        const ro = new ResizeObserver(entries => {
            const w = entries[0]?.contentRect.width;
            if (w && w > 0) setWidth(w);
        });
        ro.observe(el);
        return () => ro.disconnect();
    }, []);

    const chartWidth = width - padding.left - padding.right;
    const chartHeight = height - padding.top - padding.bottom;

    const chartData = useMemo(() => {
        if (!syncedData.length || chartWidth <= 0) return null;

        const values = syncedData.map(d => d.imoex);
        const minVal = Math.min(...values);
        const maxVal = Math.max(...values);
        const range = maxVal - minVal || 1;
        const yMin = minVal - range * 0.01;
        const yMax = maxVal + range * 0.01;

        const scaleX = (i: number) => padding.left + (i / Math.max(syncedData.length - 1, 1)) * chartWidth;
        const scaleY = (v: number) => padding.top + chartHeight - ((v - yMin) / (yMax - yMin)) * chartHeight;

        const points = syncedData.map((d, i) => ({ x: scaleX(i), y: scaleY(d.imoex), value: d.imoex }));

        const yTicks = Array.from({ length: 4 }, (_, i) => {
            const v = yMin + ((yMax - yMin) * i) / 3;
            return { value: v, y: scaleY(v) };
        });

        const xTickCount = Math.min(7, syncedData.length);
        const xTicks = Array.from({ length: xTickCount }, (_, i) => {
            const idx = Math.floor((i / Math.max(xTickCount - 1, 1)) * (syncedData.length - 1));
            return { x: scaleX(idx) };
        });

        return { points, yTicks, xTicks, scaleX };
    }, [syncedData, chartWidth, chartHeight, padding]);

    // Анимация морфинга
    useLayoutEffect(() => {
        if (!chartData) return;
        const target = chartData.points.map(p => ({ x: p.x, y: p.y }));
        const bottom = padding.top + chartHeight;

        if (animRef.current) cancelAnimationFrame(animRef.current);

        // Resize — мгновенное обновление без анимации
        if (prevWidthRef.current !== 0 && prevWidthRef.current !== chartWidth) {
            prevWidthRef.current = chartWidth;
            prevPtsRef.current = target;
            currPtsRef.current = [];
            setAnimLinePath(ptsToPath(target));
            setAnimAreaPath(ptsToArea(target, bottom));
            if (!revealed) setRevealed(true);
            return;
        }
        prevWidthRef.current = chartWidth;

        // Во время перетаскивания навигатора — мгновенное обновление без анимации
        if (isNavDragRef?.current) {
            prevPtsRef.current = target;
            currPtsRef.current = [];
            setAnimLinePath(ptsToPath(target));
            setAnimAreaPath(ptsToArea(target, bottom));
            return;
        }

        if (isFirstRef.current || prevPtsRef.current.length === 0) {
            isFirstRef.current = false;
            prevPtsRef.current = target;
            currPtsRef.current = [];
            // CSS reveal слева направо
            setAnimLinePath(ptsToPath(target));
            setAnimAreaPath(ptsToArea(target, bottom));
            setRevealed(true);
            return;
        }

        // Стартуем с текущей визуальной позиции (если анимация была прервана)
        const from = currPtsRef.current.length > 0 ? currPtsRef.current : prevPtsRef.current;
        let start: number | null = null;
        const animate = (ts: number) => {
            if (!start) start = ts;
            const t = easeOutCubic(Math.min((ts - start) / 600, 1));
            const interp = morphPts(from, target, t);
            currPtsRef.current = interp;
            setAnimLinePath(ptsToPath(interp));
            setAnimAreaPath(ptsToArea(interp, bottom));
            if (t < 1) {
                animRef.current = requestAnimationFrame(animate);
            } else {
                prevPtsRef.current = target;
                currPtsRef.current = [];
            }
        };
        animRef.current = requestAnimationFrame(animate);

        return () => { if (animRef.current) cancelAnimationFrame(animRef.current); };
    }, [chartData, chartHeight, padding.top]);

    const crosshairX = chartData && hoverIndex !== null && hoverIndex < syncedData.length
        ? chartData.scaleX(hoverIndex) : null;

    return (
        <div ref={containerRef}>
            <div ref={chartWrapRef}
                style={{ opacity: revealed ? 1 : 0, transition: 'opacity 0.3s ease' }}>
                {width > 0 && chartData && (
                    <svg ref={svgRef} width={width} height={height} className="overflow-visible" style={{ backgroundColor: 'var(--bg-secondary)' }}>
                        <defs>
                            <linearGradient id="priceGradient" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="0%" stopColor="#6366f1" stopOpacity="0.3" />
                                <stop offset="100%" stopColor="#6366f1" stopOpacity="0" />
                            </linearGradient>
                        </defs>

                        <path d={animAreaPath} fill="url(#priceGradient)" />
                        <path d={animLinePath} fill="none" stroke="#6366f1" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />

                        {crosshairX !== null && (
                            <line x1={crosshairX} y1={padding.top} x2={crosshairX} y2={padding.top + chartHeight}
                                stroke="var(--text-muted)" strokeWidth="1" strokeDasharray="3,3" />
                        )}
                        {crosshairX !== null && hoverIndex !== null && hoverIndex < chartData.points.length && (
                            <circle cx={chartData.points[hoverIndex].x} cy={chartData.points[hoverIndex].y}
                                r={5} fill="#6366f1" stroke="white" strokeWidth="2" />
                        )}

                        {chartData.xTicks.map((tick, i) => (
                            i > 0 && i < chartData.xTicks.length - 1 ? (
                                <line key={`vg-${i}`}
                                    x1={tick.x} x2={tick.x}
                                    y1={padding.top} y2={padding.top + chartHeight}
                                    stroke="rgba(255,255,255,0.08)" strokeWidth="1"
                                />
                            ) : null
                        ))}
                        {chartData.yTicks.map((tick, i) => (
                            <g key={i}>
                                <line
                                    x1={padding.left} x2={width - padding.right}
                                    y1={tick.y} y2={tick.y}
                                    stroke="rgba(255,255,255,0.08)" strokeWidth="1"
                                />
                                <text
                                    x={width - EDGE_MARGIN}
                                    y={Math.max(padding.top + 6, Math.min(tick.y, padding.top + chartHeight - 6))}
                                    textAnchor="end" dominantBaseline="middle" fill="var(--text-muted)" fontSize="16" fontWeight="600">
                                    {tick.value.toLocaleString('ru-RU', { maximumFractionDigits: 0 })}
                                </text>
                            </g>
                        ))}
                    </svg>
                )}
            </div>
        </div>
    );
}

// Мемоизированные рендереры — не перерисовываются при смене hoverIndex

type BarDef = { x: number; y: number; width: number; height: number; color: string };

const HistogramBars = memo(({ bars }: { bars: BarDef[] }) => {
    // Группируем бары по цвету и строим один <path> на цвет — вместо тысяч <rect>
    const byColor = new Map<string, string[]>();
    for (const bar of bars) {
        if (bar.height <= 0) continue;
        let arr = byColor.get(bar.color);
        if (!arr) { arr = []; byColor.set(bar.color, arr); }
        arr.push(`M${bar.x - 0.5},${bar.y + bar.height}V${bar.y}H${bar.x + bar.width + 0.5}V${bar.y + bar.height}Z`);
    }
    return (
        <g shapeRendering="crispEdges">
            {Array.from(byColor.entries()).map(([color, parts]) => (
                <path key={color} d={parts.join('')} fill={color} />
            ))}
        </g>
    );
});

const BreadthLineRenderer = memo(({ animLinePath, dataLength, breadthValues, getColor }: {
    animLinePath: string;
    dataLength: number;
    breadthValues: number[];
    getColor: (v: number) => string;
}) => {
    if (!animLinePath || dataLength < 2) return null;
    const pathParts = animLinePath.match(/[\d.]+/g);
    if (!pathParts || pathParts.length < 4) return null;
    const aPts: { x: number; y: number }[] = [];
    for (let i = 0; i < pathParts.length; i += 2) {
        aPts.push({ x: parseFloat(pathParts[i]), y: parseFloat(pathParts[i + 1]) });
    }
    // Группируем сегменты по цвету — один <path> на цвет вместо тысяч отдельных
    const byColor = new Map<string, string[]>();
    for (let i = 0; i < aPts.length - 1; i++) {
        const origIdx = (i / Math.max(aPts.length - 1, 1)) * (dataLength - 1);
        const lo = Math.floor(origIdx);
        const hi = Math.min(lo + 1, dataLength - 1);
        const val = (breadthValues[lo] + breadthValues[hi]) / 2;
        const color = getColor(val);
        let arr = byColor.get(color);
        if (!arr) { arr = []; byColor.set(color, arr); }
        arr.push(`M${aPts[i].x},${aPts[i].y}L${aPts[i + 1].x},${aPts[i + 1].y}`);
    }
    return (
        <>
            {Array.from(byColor.entries()).map(([color, parts]) => (
                <path key={color} d={parts.join('')}
                    fill="none" stroke={color} strokeWidth="2.5" strokeLinecap="round" />
            ))}
        </>
    );
});

// Синхронизированный график Breadth с морфинг-анимацией
function SyncedBreadthChart({
    syncedData,
    hoverIndex,
    height,
    mode,
    padding,
    isNavDragRef,
}: {
    syncedData: { time: string; breadth: number; imoex: number }[];
    hoverIndex: number | null;
    height: number;
    mode: ChartMode;
    padding: { left: number; right: number; top: number; bottom: number };
    isNavDragRef?: { current: boolean };
}) {
    const svgRef = useRef<SVGSVGElement>(null);
    const chartWrapRef = useRef<HTMLDivElement>(null);
    const containerRef = useRef<HTMLDivElement>(null);
    const [width, setWidth] = useState(0);

    // Анимация для line mode
    const [animLinePath, setAnimLinePath] = useState('');
    const prevPtsRef = useRef<{ x: number; y: number }[]>([]);
    const currPtsRef = useRef<{ x: number; y: number }[]>([]); // текущая визуальная позиция
    const animRef = useRef<number | null>(null);
    const isFirstRef = useRef(true);

    // Анимация для histogram mode
    const [animBars, setAnimBars] = useState<{ x: number; y: number; width: number; height: number; color: string }[]>([]);
    const prevBarsRef = useRef<{ y: number; height: number }[]>([]);
    const currBarsRef = useRef<{ y: number; height: number }[]>([]);
    const prevWidthRef = useRef(0);
    const [revealed, setRevealed] = useState(false);

    useEffect(() => {
        const el = containerRef.current;
        if (!el) return;
        const ro = new ResizeObserver(entries => {
            const w = entries[0]?.contentRect.width;
            if (w && w > 0) setWidth(w);
        });
        ro.observe(el);
        return () => ro.disconnect();
    }, []);

    const chartWidth = width - padding.left - padding.right;
    const chartHeight = height - padding.top - padding.bottom;

    const getColor = useCallback((value: number) => {
        // Плавный градиент: яркий красный (0%) → зелёный (100%), минимум оранжевого
        const t = Math.max(0, Math.min(value / 100, 1));
        if (t <= 0.35) {
            // 0..0.35 → яркий красный → тёмно-оранжевый
            const s = t / 0.35;
            const r = Math.round(239 - s * 30);
            const g = Math.round(68 + s * 60);
            const b = Math.round(68 - s * 40);
            return `rgb(${r},${g},${b})`;
        }
        if (t <= 0.65) {
            // 0.35..0.65 → быстрый переход через нейтральную зону
            const s = (t - 0.35) / 0.3;
            const r = Math.round(209 - s * 170);
            const g = Math.round(128 + s * 69);
            const b = Math.round(28 + s * 66);
            return `rgb(${r},${g},${b})`;
        }
        // 0.65..1 → зелёный всё ярче
        const s = (t - 0.65) / 0.35;
        const r = Math.round(39 - s * 5);
        const g = Math.round(197 + s * 3);
        const b = Math.round(94 - s * 0);
        return `rgb(${r},${g},${b})`;
    }, []);

    // Мемоизированный массив значений для BreadthLineRenderer — стабильная ссылка
    const breadthValues = useMemo(() => syncedData.map(d => d.breadth), [syncedData]);

    const chartData = useMemo(() => {
        if (!syncedData.length || chartWidth <= 0) return null;

        const yMin = 0;
        const yMax = 100;

        const scaleX = (i: number) => padding.left + (i / Math.max(syncedData.length - 1, 1)) * chartWidth;
        const scaleY = (v: number) => padding.top + chartHeight - ((v - yMin) / (yMax - yMin)) * chartHeight;

        const levels = [20, 40, 60, 80].map(value => ({
            value,
            color: 'rgba(255,255,255,0.08)',
            dash: '0',
            y: scaleY(value),
        }));

        const points = syncedData.map((d, i) => ({ x: scaleX(i), y: scaleY(d.breadth), value: d.breadth }));

        const lineSegments: { path: string; color: string }[] = [];
        for (let i = 0; i < points.length - 1; i++) {
            const avgValue = (points[i].value + points[i + 1].value) / 2;
            lineSegments.push({
                path: `M ${points[i].x} ${points[i].y} L ${points[i + 1].x} ${points[i + 1].y}`,
                color: getColor(avgValue)
            });
        }

        const bottom = padding.top + chartHeight;
        // Ширина бара = шаг между точками (sub-pixel для плотных данных — без наложений)
        const stepPx = chartWidth / Math.max(syncedData.length - 1, 1);
        const barWidth = Math.max(stepPx, 0.5);
        const bars = syncedData.map((d, i) => {
            const cx = scaleX(i);
            const x = cx - barWidth / 2;
            const y = scaleY(d.breadth);
            const h = bottom - y;
            return { x, y, width: barWidth, height: h, color: getColor(d.breadth) };
        });

        const xTickCount = Math.min(8, syncedData.length);
        const xTicks = Array.from({ length: xTickCount }, (_, i) => {
            const idx = Math.floor(i * (syncedData.length - 1) / Math.max(xTickCount - 1, 1));
            return {
                x: scaleX(idx),
                label: new Date(syncedData[idx].time).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', year: '2-digit' })
            };
        });

        const yTicks = [20, 40, 60, 80].map(v => ({
            value: v,
            y: scaleY(v),
            color: 'var(--text-muted)',
        }));

        return { points, lineSegments, bars, levels, xTicks, yTicks, chartWidth, chartHeight, scaleX };
    }, [syncedData, chartWidth, chartHeight, padding, getColor]);

    // Морфинг-анимация
    useLayoutEffect(() => {
        if (!chartData) return;
        if (animRef.current) cancelAnimationFrame(animRef.current);

        const bottom = padding.top + chartHeight;

        // Resize — мгновенное обновление без анимации
        if (prevWidthRef.current !== 0 && prevWidthRef.current !== chartWidth) {
            prevWidthRef.current = chartWidth;
            const targetPts = chartData.points.map(p => ({ x: p.x, y: p.y }));
            prevPtsRef.current = targetPts;
            currPtsRef.current = [];
            prevBarsRef.current = chartData.bars.map(b => ({ y: b.y, height: b.height }));
            currBarsRef.current = [];
            setAnimLinePath(ptsToPath(targetPts));
            setAnimBars(chartData.bars);
            if (!revealed) setRevealed(true);
            return;
        }
        prevWidthRef.current = chartWidth;

        // Во время перетаскивания навигатора — мгновенное обновление без анимации
        if (isNavDragRef?.current) {
            const targetPts = chartData.points.map(p => ({ x: p.x, y: p.y }));
            const targetBars = chartData.bars;
            prevPtsRef.current = targetPts;
            currPtsRef.current = [];
            prevBarsRef.current = targetBars.map(b => ({ y: b.y, height: b.height }));
            currBarsRef.current = [];
            setAnimLinePath(ptsToPath(targetPts));
            setAnimBars(targetBars);
            return;
        }

        if (mode === 'line' || mode === 'histogram') {
            const targetPts = chartData.points.map(p => ({ x: p.x, y: p.y }));
            const targetBars = chartData.bars;

            if (isFirstRef.current || prevPtsRef.current.length === 0) {
                isFirstRef.current = false;
                prevPtsRef.current = targetPts;
                currPtsRef.current = [];
                prevBarsRef.current = targetBars.map(b => ({ y: b.y, height: b.height }));
                currBarsRef.current = [];

                // CSS reveal слева направо
                setAnimLinePath(ptsToPath(targetPts));
                setAnimBars(targetBars);
                setRevealed(true);
                return;
            }

            // Стартуем с текущей визуальной позиции (если анимация была прервана)
            const fromPts = currPtsRef.current.length > 0 ? currPtsRef.current : prevPtsRef.current;
            const fromBars = currBarsRef.current.length > 0 ? currBarsRef.current : prevBarsRef.current;
            let start: number | null = null;

            const isFirstAnim = fromBars.length === 0;
            const totalDuration = isFirstAnim ? 1200 : 600;
            const staggerDelay = isFirstAnim ? 600 : 0;

            const animate = (ts: number) => {
                if (!start) start = ts;
                const elapsed = ts - start;
                const lineT = easeOutCubic(Math.min(elapsed / totalDuration, 1));

                const interp = morphPts(fromPts, targetPts, lineT);
                currPtsRef.current = interp;
                setAnimLinePath(ptsToPath(interp));

                const n = Math.max(fromBars.length, targetBars.length);
                const morphedBars: typeof targetBars = [];
                for (let i = 0; i < n; i++) {
                    const barDelay = (i / Math.max(n - 1, 1)) * staggerDelay;
                    const barElapsed = Math.max(0, elapsed - barDelay);
                    const t = easeOutCubic(Math.min(barElapsed / (totalDuration - staggerDelay), 1));
                    const fb = fromBars[Math.min(i, fromBars.length - 1)] || { y: bottom, height: 0 };
                    const tb = targetBars[Math.min(i, targetBars.length - 1)];
                    morphedBars.push({ ...tb, y: lerp(fb.y, tb.y, t), height: lerp(fb.height, tb.height, t) });
                }
                currBarsRef.current = morphedBars.map(b => ({ y: b.y, height: b.height }));
                setAnimBars(morphedBars);

                if (elapsed < totalDuration) {
                    animRef.current = requestAnimationFrame(animate);
                } else {
                    prevPtsRef.current = targetPts;
                    currPtsRef.current = [];
                    prevBarsRef.current = targetBars.map(b => ({ y: b.y, height: b.height }));
                    currBarsRef.current = [];
                }
            };
            animRef.current = requestAnimationFrame(animate);
        }

        return () => { if (animRef.current) cancelAnimationFrame(animRef.current); };
    }, [chartData, chartHeight, padding.top, mode]);

    const crosshairX = chartData && hoverIndex !== null && hoverIndex < syncedData.length ? chartData.scaleX(hoverIndex) : null;

    return (
        <div ref={containerRef}>
            <div ref={chartWrapRef}
                style={{ opacity: revealed ? 1 : 0, transition: 'opacity 0.3s ease' }}>
                {width > 0 && chartData && (
                    <svg ref={svgRef} width={width} height={height} className="overflow-visible" style={{ backgroundColor: 'var(--bg-secondary)' }}>
                        <defs>
                            <linearGradient id="breadthGradient" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="0%" stopColor="var(--accent)" stopOpacity="0.3" />
                                <stop offset="100%" stopColor="var(--accent)" stopOpacity="0" />
                            </linearGradient>
                            {/* Клип: бары не выходят за пределы области графика */}
                            <clipPath id="breadthChartClip">
                                <rect x={padding.left} y={padding.top} width={chartWidth} height={chartHeight} />
                            </clipPath>
                        </defs>

                        <g>
                            {/* Reference levels */}
                            {chartData.levels.map((level, i) => (
                                <line key={i} x1={padding.left} y1={level.y} x2={width - padding.right} y2={level.y}
                                    stroke={level.color} strokeWidth="1" strokeDasharray={level.dash} opacity="0.5" />
                            ))}

                            {/* Animated histogram bars — clipPath не даёт барам заходить на Y-метки и за левый край */}
                            {mode === 'histogram' && (
                                <g clipPath="url(#breadthChartClip)">
                                    <HistogramBars bars={animBars} />
                                    {/* Highlight активного бара — перерисовывается только при смене hoverIndex (1 элемент) */}
                                    {hoverIndex !== null && animBars[hoverIndex] && (() => {
                                        const bar = animBars[hoverIndex];
                                        return <rect x={bar.x} y={bar.y} width={bar.width} height={bar.height}
                                            fill={bar.color} opacity={1} />;
                                    })()}
                                </g>
                            )}

                            {/* Animated line with colored segments — в memo-компоненте, не перерисовывается при смене hoverIndex */}
                            {mode === 'line' && (
                                <BreadthLineRenderer
                                    animLinePath={animLinePath}
                                    dataLength={syncedData.length}
                                    breadthValues={breadthValues}
                                    getColor={getColor}
                                />
                            )}

                            {/* Crosshair */}
                            {crosshairX !== null && (
                                <line x1={crosshairX} y1={padding.top} x2={crosshairX} y2={padding.top + chartData.chartHeight}
                                    stroke="var(--text-muted)" strokeWidth="1" strokeDasharray="3,3" />
                            )}
                            {crosshairX !== null && hoverIndex !== null && hoverIndex < chartData.points.length && (
                                <circle cx={chartData.points[hoverIndex].x} cy={chartData.points[hoverIndex].y}
                                    r={5} fill="var(--accent)" stroke="white" strokeWidth="2" />
                            )}

                        </g>

                        {/* Y axis */}
                        {chartData.yTicks.map((tick, i) => (
                            <text key={i} x={width - EDGE_MARGIN} y={tick.y}
                                textAnchor="end" dominantBaseline="middle"
                                fill={tick.color || 'var(--text-muted)'} fontSize="16" fontWeight="600">
                                {tick.value}%
                            </text>
                        ))}

                        {/* X axis — первая метка прижата влево, последняя вправо */}
                        {chartData.xTicks.map((tick, i) => (
                            <text key={i} x={tick.x} y={padding.top + chartData.chartHeight + 18}
                                textAnchor={i === 0 ? 'start' : i === chartData.xTicks.length - 1 ? 'end' : 'middle'}
                                fill="var(--text-muted)" fontSize="16" fontWeight="600">
                                {tick.label}
                            </text>
                        ))}
                    </svg>
                )}
            </div>
        </div>
    );
}
