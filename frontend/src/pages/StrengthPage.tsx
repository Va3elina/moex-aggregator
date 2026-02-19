import { useEffect, useState, useMemo, useRef, useCallback } from 'react';
import { TrendingUp, Activity, ArrowUp, ArrowDown, BarChart2, LineChart, Filter } from 'lucide-react';
import {
    getBreadthCurrent,
    getBreadthHistory,
    type BreadthCurrentResponse,
    type BreadthHistoryResponse,
} from '../services/api';

type Period = '3m' | '6m' | '1y' | '2y' | 'all';
type ChartMode = 'line' | 'histogram';

const PERIOD_LABELS: Record<Period, string> = {
    '3m': '3М',
    '6m': '6М',
    '1y': '1Г',
    '2y': '2Г',
    'all': 'Всё'
};

const PERIOD_DAYS: Record<Period, number> = {
    '3m': 90,
    '6m': 180,
    '1y': 365,
    '2y': 730,
    'all': 7000
};

const EMA_PERIOD = 200; // Fixed EMA period
const MAX_CHART_POINTS = 800; // Максимум точек на графике — выше начинается лаг

const CLASSIFICATION_LABELS: Record<string, { label: string; color: string; bg: string }> = {
    overbought: { label: 'Перекупленность', color: 'text-emerald-400', bg: 'bg-emerald-500/20' },
    bullish: { label: 'Бычий тренд', color: 'text-emerald-300', bg: 'bg-emerald-500/10' },
    neutral: { label: 'Нейтрально', color: 'text-amber-400', bg: 'bg-amber-500/20' },
    oversold: { label: 'Перепроданность', color: 'text-red-400', bg: 'bg-red-500/20' },
};

// Секторы для фильтрации — соответствуют реальным тикерам в БД
const SECTORS: Record<string, string[]> = {
    'Все': [],
    'Финансы': ['SBER', 'SBERP', 'VTBR', 'MOEX', 'T', 'BSPB', 'CBOM', 'SVCB', 'RENI'],
    'Нефть и газ': ['ROSN', 'LKOH', 'GAZP', 'NVTK', 'TATN', 'TATNP', 'SNGS', 'SNGSP', 'TRNFP'],
    'Металлургия': ['GMKN', 'NLMK', 'CHMF', 'MAGN', 'RUAL', 'ALRS', 'PLZL'],
    'Энергетика': ['IRAO', 'MSNG', 'OGKB', 'UPRO', 'ENPG'],
    'Телеком и IT': ['YDEX', 'MTSS', 'RTKM', 'POSI', 'VKCO', 'HEAD'],
    'Транспорт': ['AFLT', 'FLOT'],
    'Другое': ['PHOR', 'PIKK', 'AFKS', 'MDMG'],
};


export default function StrengthPage() {
    const [period, setPeriod] = useState<Period>('1y');
    const emaPeriod = EMA_PERIOD; // Fixed EMA200
    const [chartMode, setChartMode] = useState<ChartMode>('histogram');
    const [showPrice, setShowPrice] = useState(true);
    const [selectedSector, setSelectedSector] = useState('Все');

    const [current, setCurrent] = useState<BreadthCurrentResponse | null>(null);
    const [history, setHistory] = useState<BreadthHistoryResponse | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    // Синхронизированный hover между графиками
    const [hoverIndex, setHoverIndex] = useState<number | null>(null);
    const [hoverY, setHoverY] = useState<number>(0);

    const containerRef = useRef<HTMLDivElement>(null);
    const mouseMoveRaf = useRef<number | null>(null);

    useEffect(() => {
        const fetchData = async () => {
            // Показываем loading только при первой загрузке (нет данных)
            if (!current && !history) setLoading(true);
            setError(null);
            try {
                const [currentData, historyData] = await Promise.all([
                    getBreadthCurrent(emaPeriod),
                    getBreadthHistory(emaPeriod, PERIOD_DAYS[period])
                ]);
                setCurrent(currentData);
                setHistory(historyData);
            } catch (err) {
                setError('Не удалось загрузить данные');
                console.error(err);
            } finally {
                setLoading(false);
            }
        };
        fetchData();
    }, [emaPeriod, period]);

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

        // Downsample для больших периодов — иначе 4600+ точек лагают SVG
        if (full.length <= MAX_CHART_POINTS) return full;
        const step = full.length / MAX_CHART_POINTS;
        return Array.from({ length: MAX_CHART_POINTS }, (_, i) => {
            const lo = Math.floor(i * step);
            const hi = Math.min(Math.floor((i + 1) * step), full.length);
            const bucket = full.slice(lo, hi);
            return {
                time: bucket[Math.floor(bucket.length / 2)].time,
                breadth: bucket.reduce((s, d) => s + d.breadth, 0) / bucket.length,
                imoex: bucket.reduce((s, d) => s + d.imoex, 0) / bucket.length,
            };
        });
    }, [breadthData, imoexData]);

    // Реальное количество акций в каждом секторе (только те, что вернул API)
    const sectorCounts = useMemo(() => {
        if (!current?.stocks) return {};
        const availableTickers = new Set(current.stocks.map(s => s.ticker));
        const counts: Record<string, number> = {};
        for (const [sector, tickers] of Object.entries(SECTORS)) {
            if (sector === 'Все') {
                counts[sector] = current.stocks.length;
            } else {
                counts[sector] = tickers.filter(t => availableTickers.has(t)).length;
            }
        }
        return counts;
    }, [current?.stocks]);

    // Фильтрованные акции по сектору
    const filteredStocks = useMemo(() => {
        if (!current?.stocks) return [];
        if (selectedSector === 'Все') return current.stocks;

        const sectorTickers = SECTORS[selectedSector] || [];
        return current.stocks.filter(s => sectorTickers.includes(s.ticker));
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
    const padding = { left: 10, right: 70, top: 20, bottom: 30 };

    const handleMouseMove = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
        if (!containerRef.current || !syncedData.length) return;
        if (mouseMoveRaf.current !== null) return; // throttle до одного RAF

        const clientX = e.clientX;
        const clientY = e.clientY;

        mouseMoveRaf.current = requestAnimationFrame(() => {
            mouseMoveRaf.current = null;
            if (!containerRef.current) return;
            const rect = containerRef.current.getBoundingClientRect();
            const x = clientX - rect.left - padding.left;
            const y = clientY - rect.top;
            const chartWidth = rect.width - padding.left - padding.right;
            const idx = Math.round((x / chartWidth) * (syncedData.length - 1));
            if (idx >= 0 && idx < syncedData.length) {
                setHoverIndex(idx);
                setHoverY(y);
            }
        });
    }, [syncedData.length, padding.left, padding.right]);

    const handleMouseLeave = useCallback(() => {
        if (mouseMoveRaf.current !== null) {
            cancelAnimationFrame(mouseMoveRaf.current);
            mouseMoveRaf.current = null;
        }
        setHoverIndex(null);
    }, []);

    // Текущие значения для тултипа
    const hoverData = hoverIndex !== null && syncedData[hoverIndex] ? syncedData[hoverIndex] : null;

    const stocksAbove = current?.stocks ? current.stocks.filter(s => s.is_above).length : current?.count_above ?? 0;
    const stocksTotal = current?.stocks?.length ?? current?.count_total ?? 0;

    return (
        <div className="p-4 md:p-6 max-w-7xl mx-auto">
            {/* Заголовок */}
            <div className="flex items-center gap-3 mb-6">
                <div className="p-3 bg-gradient-to-br from-[#8b5cf6] to-[#ec4899] rounded-xl">
                    <TrendingUp className="w-6 h-6 text-white" />
                </div>
                <div>
                    <h1 className="text-2xl font-bold text-theme-primary">Сила рынка</h1>
                    <p className="text-theme-secondary text-sm">% акций выше EMA{emaPeriod}</p>
                </div>
            </div>

            {/* Контролы — одна строка */}
            <div className="flex items-center gap-4 mb-6 flex-wrap">
                {/* Период */}
                <div className="flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                    {(Object.entries(PERIOD_LABELS) as [Period, string][]).map(([key, label]) => (
                        <button
                            key={key}
                            onClick={() => setPeriod(key)}
                            className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-all ${period === key ? 'btn-control active' : 'text-theme-secondary hover:text-theme-primary'}`}
                        >
                            {label}
                        </button>
                    ))}
                </div>

                {/* Тип графика breadth */}
                <div className="flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                    <button
                        onClick={() => setChartMode('line')}
                        className={`p-2 rounded-lg transition-all ${chartMode === 'line' ? 'bg-white/10' : ''}`}
                        title="Линия"
                    >
                        <LineChart size={18} className={chartMode === 'line' ? 'text-theme-accent' : 'text-theme-secondary'} />
                    </button>
                    <button
                        onClick={() => setChartMode('histogram')}
                        className={`p-2 rounded-lg transition-all ${chartMode === 'histogram' ? 'bg-white/10' : ''}`}
                        title="Гистограмма"
                    >
                        <BarChart2 size={18} className={chartMode === 'histogram' ? 'text-theme-accent' : 'text-theme-secondary'} />
                    </button>
                </div>

                {/* Показать IMOEX */}
                <button
                    onClick={() => setShowPrice(!showPrice)}
                    className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-all border ${showPrice ? 'bg-indigo-500/20 border-indigo-500/50 text-indigo-400' : 'border-theme text-theme-secondary'
                        }`}
                >
                    IMOEX
                </button>

                {/* Статус + счётчик — справа */}
                {current && (
                    <div className="flex items-center gap-3 ml-auto">
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
                className="widget overflow-hidden mb-6 relative cursor-crosshair"
                onMouseMove={handleMouseMove}
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
                    const chartWidth = rect.width - padding.left - padding.right;
                    const hoverX = padding.left + (hoverIndex / Math.max(syncedData.length - 1, 1)) * chartWidth;
                    const isRightHalf = hoverX > padding.left + chartWidth / 2;
                    const cardLeft = isRightHalf ? hoverX - 190 - 12 : hoverX + 12;

                    const dateStr = new Date(hoverData.time).toLocaleDateString('ru-RU', {
                        day: 'numeric', month: 'short', year: 'numeric'
                    });

                    const breadthColor = hoverData.breadth >= 70 ? '#22c55e'
                        : hoverData.breadth >= 50 ? '#4ade80'
                        : hoverData.breadth < 30 ? '#ef4444'
                        : '#fbbf24';

                    // Карточка ~60px высотой, ограничиваем в пределах контейнера
                    const cardHeight = showPrice ? 60 : 34;
                    const containerHeight = rect.height;
                    const clampedCardTop = Math.min(Math.max(hoverY - cardHeight / 2, 4), containerHeight - cardHeight - 4);

                    return (
                        <>
                            {/* Дата — закреплена наверху, привязана к вертикальной линии */}
                            <div
                                className="absolute z-30 pointer-events-none"
                                style={{
                                    left: Math.min(Math.max(hoverX - 50, padding.left), rect.width - padding.right - 100),
                                    top: 4,
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
                {showPrice && syncedData.length > 0 && (
                    <div className="px-4 pt-3 pb-2 border-b border-theme relative">
                        <div className="flex items-center gap-2 mb-1">
                            <span className="w-2.5 h-2.5 rounded-full bg-[#6366f1]" />
                            <span className="text-xs text-theme-secondary">Индекс МосБиржи</span>
                        </div>
                        <SyncedPriceChart
                            syncedData={syncedData}
                            hoverIndex={hoverIndex}
                            height={300}
                            padding={padding}
                        />
                    </div>
                )}

                {/* График Breadth (нижний) */}
                <div className="px-4 pt-3 pb-2 relative">
                    <div className="flex items-center gap-2 mb-1">
                        <span className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: 'var(--accent)' }} />
                        <span className="text-xs text-theme-secondary">% акций выше EMA{emaPeriod}</span>
                        <div className="flex items-center gap-2 ml-auto text-[10px]">
                            <span className="flex items-center gap-1">
                                <span className="w-3 border-t border-dashed border-emerald-500" />
                                <span className="text-emerald-400">70</span>
                            </span>
                            <span className="flex items-center gap-1">
                                <span className="w-3 border-t border-amber-500" />
                                <span className="text-amber-400">50</span>
                            </span>
                            <span className="flex items-center gap-1">
                                <span className="w-3 border-t border-dashed border-red-500" />
                                <span className="text-red-400">30</span>
                            </span>
                        </div>
                    </div>
                    {syncedData.length > 0 ? (
                        <SyncedBreadthChart
                            syncedData={syncedData}
                            hoverIndex={hoverIndex}
                            height={150}
                            mode={chartMode}
                            padding={padding}
                        />
                    ) : (
                        <div className="h-48 flex items-center justify-center text-theme-muted">
                            Нет данных для отображения
                        </div>
                    )}
                </div>
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
                            {Object.keys(SECTORS)
                                .filter(sector => (sectorCounts[sector] ?? 0) > 0)
                                .map((sector) => (
                                <button
                                    key={sector}
                                    onClick={() => setSelectedSector(sector)}
                                    className={`px-3 py-1 text-xs font-medium rounded-full transition-all ${selectedSector === sector
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
    padding
}: {
    syncedData: { time: string; breadth: number; imoex: number }[];
    hoverIndex: number | null;
    height: number;
    padding: { left: number; right: number; top: number; bottom: number };
}) {
    const svgRef = useRef<SVGSVGElement>(null);
    const [width, setWidth] = useState(800);
    const [animLinePath, setAnimLinePath] = useState('');
    const [animAreaPath, setAnimAreaPath] = useState('');
    const prevPtsRef = useRef<{ x: number; y: number }[]>([]);
    const animRef = useRef<number | null>(null);
    const isFirstRef = useRef(true);

    useEffect(() => {
        const updateWidth = () => {
            if (svgRef.current?.parentElement) {
                setWidth(svgRef.current.parentElement.clientWidth);
            }
        };
        updateWidth();
        window.addEventListener('resize', updateWidth);
        return () => window.removeEventListener('resize', updateWidth);
    }, []);

    const chartWidth = width - padding.left - padding.right;
    const chartHeight = height - padding.top - padding.bottom;

    const chartData = useMemo(() => {
        if (!syncedData.length) return null;

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

        return { points, yTicks, scaleX };
    }, [syncedData, chartWidth, chartHeight, padding]);

    // Анимация морфинга
    useEffect(() => {
        if (!chartData) return;
        const target = chartData.points.map(p => ({ x: p.x, y: p.y }));
        const bottom = padding.top + chartHeight;

        if (animRef.current) cancelAnimationFrame(animRef.current);

        if (isFirstRef.current || prevPtsRef.current.length === 0) {
            isFirstRef.current = false;
            prevPtsRef.current = target;
            // Fade-in снизу
            let start: number | null = null;
            const fadeIn = (ts: number) => {
                if (!start) start = ts;
                const t = easeOutCubic(Math.min((ts - start) / 500, 1));
                const anim = target.map(p => ({ x: p.x, y: bottom - (bottom - p.y) * t }));
                setAnimLinePath(ptsToPath(anim));
                setAnimAreaPath(ptsToArea(anim, bottom));
                if (t < 1) animRef.current = requestAnimationFrame(fadeIn);
            };
            animRef.current = requestAnimationFrame(fadeIn);
            return;
        }

        const from = prevPtsRef.current;
        let start: number | null = null;
        const animate = (ts: number) => {
            if (!start) start = ts;
            const t = easeOutCubic(Math.min((ts - start) / 600, 1));
            const interp = morphPts(from, target, t);
            setAnimLinePath(ptsToPath(interp));
            setAnimAreaPath(ptsToArea(interp, bottom));
            if (t < 1) {
                animRef.current = requestAnimationFrame(animate);
            } else {
                prevPtsRef.current = target;
            }
        };
        animRef.current = requestAnimationFrame(animate);

        return () => { if (animRef.current) cancelAnimationFrame(animRef.current); };
    }, [chartData, chartHeight, padding.top]);

    if (!chartData) return null;

    const crosshairX = hoverIndex !== null && hoverIndex < syncedData.length
        ? chartData.scaleX(hoverIndex) : null;

    return (
        <svg ref={svgRef} width="100%" height={height} className="overflow-visible">
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

            {chartData.yTicks.map((tick, i) => (
                <text key={i} x={width - padding.right + 8} y={tick.y}
                    textAnchor="start" dominantBaseline="middle" fill="var(--text-muted)" fontSize="11">
                    {tick.value.toLocaleString('ru-RU', { maximumFractionDigits: 0 })}
                </text>
            ))}
        </svg>
    );
}

// Синхронизированный график Breadth с морфинг-анимацией
function SyncedBreadthChart({
    syncedData,
    hoverIndex,
    height,
    mode,
    padding
}: {
    syncedData: { time: string; breadth: number; imoex: number }[];
    hoverIndex: number | null;
    height: number;
    mode: ChartMode;
    padding: { left: number; right: number; top: number; bottom: number };
}) {
    const svgRef = useRef<SVGSVGElement>(null);
    const [width, setWidth] = useState(800);

    // Анимация для line mode
    const [animLinePath, setAnimLinePath] = useState('');
    const prevPtsRef = useRef<{ x: number; y: number }[]>([]);
    const animRef = useRef<number | null>(null);
    const isFirstRef = useRef(true);

    // Анимация для histogram mode
    const [animBars, setAnimBars] = useState<{ x: number; y: number; width: number; height: number; color: string }[]>([]);
    const prevBarsRef = useRef<{ y: number; height: number }[]>([]);

    useEffect(() => {
        const updateWidth = () => {
            if (svgRef.current?.parentElement) {
                setWidth(svgRef.current.parentElement.clientWidth);
            }
        };
        updateWidth();
        window.addEventListener('resize', updateWidth);
        return () => window.removeEventListener('resize', updateWidth);
    }, []);

    const chartWidth = width - padding.left - padding.right;
    const chartHeight = height - padding.top - padding.bottom;

    const getColor = useCallback((value: number) => {
        if (value >= 70) return '#22c55e';
        if (value >= 50) return '#4ade80';
        if (value < 30) return '#ef4444';
        return '#fbbf24';
    }, []);

    const chartData = useMemo(() => {
        if (!syncedData.length) return null;

        const yMin = 0;
        const yMax = 100;

        const scaleX = (i: number) => padding.left + (i / Math.max(syncedData.length - 1, 1)) * chartWidth;
        const scaleY = (v: number) => padding.top + chartHeight - ((v - yMin) / (yMax - yMin)) * chartHeight;

        const levels = [
            { value: 70, color: '#22c55e', dash: '4,4' },
            { value: 50, color: '#f59e0b', dash: '0' },
            { value: 30, color: '#ef4444', dash: '4,4' },
        ].map(l => ({ ...l, y: scaleY(l.value) }));

        const points = syncedData.map((d, i) => ({ x: scaleX(i), y: scaleY(d.breadth), value: d.breadth }));

        const lineSegments: { path: string; color: string }[] = [];
        for (let i = 0; i < points.length - 1; i++) {
            const avgValue = (points[i].value + points[i + 1].value) / 2;
            lineSegments.push({
                path: `M ${points[i].x} ${points[i].y} L ${points[i + 1].x} ${points[i + 1].y}`,
                color: getColor(avgValue)
            });
        }

        const barWidth = Math.max(chartWidth / syncedData.length - 1, 2);
        const bottom = padding.top + chartHeight;
        const bars = syncedData.map((d, i) => {
            const x = scaleX(i) - barWidth / 2;
            const y = scaleY(d.breadth);
            const h = bottom - y;
            return { x, y, width: barWidth, height: h, color: getColor(d.breadth) };
        });

        const xTickCount = Math.min(8, syncedData.length);
        const xTicks = Array.from({ length: xTickCount }, (_, i) => {
            const idx = Math.floor(i * (syncedData.length - 1) / Math.max(xTickCount - 1, 1));
            return {
                x: scaleX(idx),
                label: new Date(syncedData[idx].time).toLocaleDateString('ru-RU', { day: '2-digit', month: 'short' })
            };
        });

        const yTicks = [0, 25, 50, 75, 100].map(v => ({ value: v, y: scaleY(v) }));

        return { points, lineSegments, bars, levels, xTicks, yTicks, chartWidth, chartHeight, scaleX };
    }, [syncedData, chartWidth, chartHeight, padding, getColor]);

    // Морфинг-анимация
    useEffect(() => {
        if (!chartData) return;
        if (animRef.current) cancelAnimationFrame(animRef.current);

        const bottom = padding.top + chartHeight;

        if (mode === 'line' || mode === 'histogram') {
            const targetPts = chartData.points.map(p => ({ x: p.x, y: p.y }));
            const targetBars = chartData.bars;

            if (isFirstRef.current || prevPtsRef.current.length === 0) {
                isFirstRef.current = false;
                prevPtsRef.current = targetPts;
                prevBarsRef.current = targetBars.map(b => ({ y: b.y, height: b.height }));

                let start: number | null = null;
                const fadeIn = (ts: number) => {
                    if (!start) start = ts;
                    const t = easeOutCubic(Math.min((ts - start) / 500, 1));

                    // Animate line from bottom
                    const animPts = targetPts.map(p => ({ x: p.x, y: bottom - (bottom - p.y) * t }));
                    setAnimLinePath(ptsToPath(animPts));

                    // Animate bars growing from bottom
                    setAnimBars(targetBars.map(b => ({
                        ...b,
                        y: bottom - b.height * t,
                        height: b.height * t,
                    })));

                    if (t < 1) animRef.current = requestAnimationFrame(fadeIn);
                };
                animRef.current = requestAnimationFrame(fadeIn);
                return;
            }

            const fromPts = prevPtsRef.current;
            const fromBars = prevBarsRef.current;
            let start: number | null = null;

            const animate = (ts: number) => {
                if (!start) start = ts;
                const t = easeOutCubic(Math.min((ts - start) / 600, 1));

                // Morph line
                const interp = morphPts(fromPts, targetPts, t);
                setAnimLinePath(ptsToPath(interp));

                // Morph bars
                const n = Math.max(fromBars.length, targetBars.length);
                const morphedBars: typeof targetBars = [];
                for (let i = 0; i < n; i++) {
                    const fb = fromBars[Math.min(i, fromBars.length - 1)] || { y: bottom, height: 0 };
                    const tb = targetBars[Math.min(i, targetBars.length - 1)];
                    morphedBars.push({
                        ...tb,
                        y: lerp(fb.y, tb.y, t),
                        height: lerp(fb.height, tb.height, t),
                    });
                }
                setAnimBars(morphedBars);

                if (t < 1) {
                    animRef.current = requestAnimationFrame(animate);
                } else {
                    prevPtsRef.current = targetPts;
                    prevBarsRef.current = targetBars.map(b => ({ y: b.y, height: b.height }));
                }
            };
            animRef.current = requestAnimationFrame(animate);
        }

        return () => { if (animRef.current) cancelAnimationFrame(animRef.current); };
    }, [chartData, chartHeight, padding.top, mode]);

    if (!chartData) return null;

    const crosshairX = hoverIndex !== null && hoverIndex < syncedData.length ? chartData.scaleX(hoverIndex) : null;

    return (
        <svg ref={svgRef} width="100%" height={height} className="overflow-visible">
            <defs>
                <linearGradient id="breadthGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="var(--accent)" stopOpacity="0.3" />
                    <stop offset="100%" stopColor="var(--accent)" stopOpacity="0" />
                </linearGradient>
            </defs>

            {/* Reference levels */}
            {chartData.levels.map((level, i) => (
                <line key={i} x1={padding.left} y1={level.y} x2={width - padding.right} y2={level.y}
                    stroke={level.color} strokeWidth="1" strokeDasharray={level.dash} opacity="0.5" />
            ))}

            {/* Animated histogram bars */}
            {mode === 'histogram' && (
                <>
                    {animBars.map((bar, i) => (
                        <rect key={i} x={bar.x} y={bar.y} width={bar.width} height={bar.height}
                            fill={bar.color} opacity={0.75} />
                    ))}
                    {/* Highlight активного бара отдельно — не перерисовываем все 800 */}
                    {hoverIndex !== null && animBars[hoverIndex] && (() => {
                        const bar = animBars[hoverIndex];
                        return <rect x={bar.x} y={bar.y} width={bar.width} height={bar.height}
                            fill={bar.color} opacity={1} />;
                    })()}
                </>
            )}

            {/* Animated line with colored segments */}
            {mode === 'line' && chartData.points.length > 1 && (() => {
                // Parse animated path to get interpolated points for coloring
                const pathParts = animLinePath.match(/[\d.]+/g);
                if (!pathParts || pathParts.length < 4) return null;
                const aPts: { x: number; y: number }[] = [];
                for (let i = 0; i < pathParts.length; i += 2) {
                    aPts.push({ x: parseFloat(pathParts[i]), y: parseFloat(pathParts[i + 1]) });
                }
                // Use original values for coloring, resampled to animated points count
                const segments: { path: string; color: string }[] = [];
                for (let i = 0; i < aPts.length - 1; i++) {
                    const origIdx = (i / Math.max(aPts.length - 1, 1)) * (syncedData.length - 1);
                    const lo = Math.floor(origIdx);
                    const hi = Math.min(lo + 1, syncedData.length - 1);
                    const val = (syncedData[lo].breadth + syncedData[hi].breadth) / 2;
                    segments.push({
                        path: `M ${aPts[i].x} ${aPts[i].y} L ${aPts[i + 1].x} ${aPts[i + 1].y}`,
                        color: getColor(val),
                    });
                }
                return segments.map((seg, i) => (
                    <path key={i} d={seg.path} fill="none" stroke={seg.color} strokeWidth="2.5" strokeLinecap="round" />
                ));
            })()}

            {/* Crosshair */}
            {crosshairX !== null && (
                <line x1={crosshairX} y1={padding.top} x2={crosshairX} y2={padding.top + chartData.chartHeight}
                    stroke="var(--text-muted)" strokeWidth="1" strokeDasharray="3,3" />
            )}
            {crosshairX !== null && hoverIndex !== null && hoverIndex < chartData.points.length && (
                <circle cx={chartData.points[hoverIndex].x} cy={chartData.points[hoverIndex].y}
                    r={5} fill="var(--accent)" stroke="white" strokeWidth="2" />
            )}

            {/* Y axis */}
            {chartData.yTicks.map((tick, i) => (
                <text key={i} x={width - padding.right + 8} y={tick.y}
                    textAnchor="start" dominantBaseline="middle" fill="var(--text-muted)" fontSize="11">
                    {tick.value}%
                </text>
            ))}

            {/* X axis */}
            {chartData.xTicks.map((tick, i) => (
                <text key={i} x={tick.x} y={padding.top + chartData.chartHeight + 18}
                    textAnchor="middle" fill="var(--text-muted)" fontSize="11">
                    {tick.label}
                </text>
            ))}
        </svg>
    );
}
