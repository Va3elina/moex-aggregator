import { useEffect, useLayoutEffect, useState, useMemo, useRef, useCallback } from 'react';
import { TrendingUp, Activity } from 'lucide-react';
import ChartNavigator from '../components/ChartNavigator';
import {
    getBreadthCurrent,
    getBreadthHistory,
    type BreadthCurrentResponse,
    type BreadthHistoryResponse,
    type BreadthUniverse,
} from '../services/api';
import { useAuth } from '../contexts/AuthContext';
import { getDefaultPeriod } from '../config/accessControl';
import { useRealtimeData } from '../hooks/useRealtimeData';
import type { SyncedDataPoint, ChartPadding } from '../components/strength/chartUtils';
import IndexChart from '../components/strength/IndexChart';
import BreadthChart from '../components/strength/BreadthChart';
import SectorDetail from '../components/strength/SectorDetail';
import StrengthControls from '../components/strength/StrengthControls';

type Period = '6m' | '1y' | '2y' | '5y' | 'all';
type ChartMode = 'line' | 'histogram';

const PERIOD_DAYS: Record<Period, number> = {
    '6m': 180,
    '1y': 365,
    '2y': 730,
    '5y': 1825,
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


// Дефолтные значения — используются на первом рендере до useLayoutEffect.
// Должны совпадать с --strength-pad-* в index.css :root (desktop breakpoint).
// Главное правило: ссылка на padding должна быть стабильной между рендерами —
// IndexChart/BreadthChart используют padding в useMemo-deps, пересоздание объекта
// сбрасывает chartData и прерывает морфинг-анимацию.
const DEFAULT_PADDING: ChartPadding = { left: 70, right: 70, top: 10, bottom: 30 };
const DEFAULT_HEIGHTS = { top: 300, bottomDual: 150, bottomSolo: 450 };

export default function StrengthPage() {
    const { isAuthenticated } = useAuth();
    const [period, setPeriod] = useState<Period>(getDefaultPeriod('1y', isAuthenticated) as Period);
    // EMA-период: 50 (краткосрок), 100 (среднесрок), 200 (долгосрок, по умолчанию).
    // Все три доступны в pre-compute (breadth_history таблица).
    const [emaPeriod, setEmaPeriod] = useState<50 | 100 | 200>(EMA_PERIOD);
    const [chartMode, setChartMode] = useState<ChartMode>('histogram');
    const [showPrice, setShowPrice] = useState(true);
    const [selectedSector, setSelectedSector] = useState('Все');
    const [currency, setCurrency] = useState<'rub' | 'usd'>('rub');
    const [universeBase, setUniverseBase] = useState<'all' | 'imoex'>('imoex');
    // Итоговый universe: добавляем _usd при долларовом режиме
    const universe: BreadthUniverse = currency === 'usd'
        ? `${universeBase}_usd` as BreadthUniverse
        : universeBase;

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
                right: num('--strength-pad-right', DEFAULT_PADDING.right),
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
            <StrengthControls
                period={period}
                onPeriodChange={setPeriod}
                chartMode={chartMode}
                onChartModeChange={setChartMode}
                universeBase={universeBase}
                onUniverseBaseChange={setUniverseBase}
                currency={currency}
                onCurrencyChange={setCurrency}
                emaPeriod={emaPeriod}
                onEmaPeriodChange={setEmaPeriod}
                showPrice={showPrice}
                onShowPriceChange={setShowPrice}
                stocksAbove={stocksAbove}
                stocksTotal={stocksTotal}
                classInfo={classInfo}
                hasCurrent={!!current}
            />

            {/* Синхронизированные графики */}
            <div
                ref={containerRef}
                className="bg-theme-secondary rounded-2xl border border-theme mb-6 relative cursor-crosshair overflow-hidden" style={{ minHeight: 'var(--chart-height, 500px)' }}
                onMouseMove={isAnimating ? undefined : handleMouseMove}
                onMouseLeave={handleMouseLeave}
            >
                {/* Полный loading / error на месте графика */}
                {loading && !current ? (
                    <div className="flex items-center justify-center" style={{ height: 'var(--chart-height, 450px)' }}>
                        <div className="flex flex-col items-center gap-3">
                            <div className="w-8 h-8 border-2 border-[#8b5cf6] border-t-transparent rounded-full animate-spin" />
                            <span className="text-theme-secondary">Загрузка...</span>
                        </div>
                    </div>
                ) : error && !current ? (
                    <div className="flex items-center justify-center" style={{ height: 'var(--chart-height, 450px)' }}>
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
                                {/* Дата — закреплена наверху, привязана к вертикальной линии.
                                    top использует общий токен --date-top-legend-top (38px), как в SimpleChart. */}
                                <div
                                    className="absolute z-20 pointer-events-none"
                                    style={{
                                        left: Math.min(Math.max(hoverX - 60, 4), rect.width - 128),
                                        top: 'var(--date-top-legend-top, 38px)',
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

                    {/* График IMOEX (верхний).
                        minHeight = height SVG + высота блока заголовка (mb-5=20 + text-sm=14) ≈ 34px. */}
                    {showPrice && (
                        <div className="px-4 pt-4 pb-1 border-b border-theme relative overflow-hidden"
                             style={{ minHeight: heights.top + 34 }}>
                            <div className="flex items-center justify-center gap-2 mb-5 relative z-10">
                                <span className="w-3 h-3 rounded-full bg-[#6366f1]" />
                                <span className="text-sm font-semibold text-theme-primary">{currency === 'usd' ? 'Индекс RTS' : 'Индекс IMOEX'}</span>
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
                        minHeight = height SVG + высота блока заголовка (mb-2=8 + text-sm=14 + margin) ≈ 24px. */}
                    <div className="px-4 pt-2 pb-1 relative overflow-hidden"
                         style={{ minHeight: (showPrice ? heights.bottomDual : heights.bottomSolo) + 24 }}>
                        <div className="flex items-center justify-center gap-2 mb-2 relative z-10">
                            <span className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: 'var(--accent)' }} />
                            <span className="text-sm font-semibold text-theme-primary">% акций выше EMA{emaPeriod}</span>
                        </div>
                        {displaySyncedData.length > 0 ? (
                            <BreadthChart
                                syncedData={displaySyncedData}
                                hoverIndex={hoverIndex}
                                height={showPrice ? heights.bottomDual : heights.bottomSolo}
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
                <SectorDetail
                    sectorNames={sectorNames}
                    sectorCounts={sectorCounts}
                    selectedSector={selectedSector}
                    onSelectSector={setSelectedSector}
                    filteredStocks={filteredStocks}
                    emaPeriod={emaPeriod}
                />
            )}
        </div>
    );
}
