import { useEffect, useLayoutEffect, useState, useMemo, useRef, useCallback } from 'react';
import { Activity } from 'lucide-react';
import ChartNavigator from '../components/ChartNavigator';
import PageHeader from '../components/PageHeader';
import { METHODOLOGY } from '../data/methodology';
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
import { computeChartTopLineY, getDatePillStyle } from '../components/chart/datePillLayout';

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

// Theme-aware (color = CSS var, bg = inline style через color-mix).
// classInfo.color и .bg теперь не классы а CSS-значения — используются inline.
const CLASSIFICATION_LABELS: Record<string, { label: string; color: string; bg: string }> = {
    overbought: { label: 'Перекупленность', color: 'var(--funds-flow-positive)', bg: 'color-mix(in srgb, var(--funds-flow-positive) 18%, transparent)' },
    bullish:    { label: 'Бычий тренд',     color: 'var(--funds-flow-positive)', bg: 'color-mix(in srgb, var(--funds-flow-positive) 12%, transparent)' },
    neutral:    { label: 'Нейтрально',       color: 'var(--text-muted)',          bg: 'color-mix(in srgb, var(--text-muted) 18%, transparent)' },
    oversold:   { label: 'Перепроданность',  color: 'var(--funds-flow-negative)', bg: 'color-mix(in srgb, var(--funds-flow-negative) 18%, transparent)' },
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
        // ВСЕГДА выставляем loading=true при старте fetch'а — даёт визуальный
        // индикатор «Обновление...» при смене периода/EMA/валюты. Full-loader
        // (loading && !current) не сработает потому что данные ещё не сброшены.
        setLoading(true);
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
        <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8 min-h-screen">
            <PageHeader
                icon={Activity}
                title="Сила рынка"
                subtitle={`% ${universe === 'imoex' ? 'акций индекса MOEX' : 'акций'} выше EMA${emaPeriod}`}
                help={METHODOLOGY.strength}
                helpLink="/methodology/strength"
            />

            {/* Editorial frame — обнимает controls + chart в один контейнер */}
            <div className="editorial-frame">

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

            {/* Синхронизированные графики — оба в одном paper-контейнере, 1.5px outline */}
            <div
                ref={containerRef}
                className="relative cursor-crosshair overflow-hidden rounded-2xl bg-theme-primary"
                style={{
                    minHeight: 'var(--chart-height, 500px)',
                    border: '1.5px solid var(--text-primary)',
                }}
                onMouseMove={isAnimating ? undefined : handleMouseMove}
                onMouseLeave={handleMouseLeave}
            >
                {/* Полный loading / error на месте графика */}
                {loading && !current ? (
                    <div className="flex items-center justify-center" style={{ height: (showPrice ? heights.top + 34 : 0) + (showPrice ? heights.bottomDual : heights.bottomSolo) + 24 + 68 }}>
                        <div className="flex flex-col items-center gap-3">
                            <div className="w-8 h-8 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
                            <span className="text-theme-secondary">Загрузка...</span>
                        </div>
                    </div>
                ) : error && !current ? (
                    <div className="flex items-center justify-center" style={{ height: (showPrice ? heights.top + 34 : 0) + (showPrice ? heights.bottomDual : heights.bottomSolo) + 24 + 68 }}>
                        <div className="text-center">
                            <Activity className="w-12 h-12 text-red-400 mx-auto mb-3" />
                            <p className="text-red-400">{error}</p>
                        </div>
                    </div>
                ) : (<>
                    {/* Индикатор обновления данных — paper-style без glass */}
                    {loading && syncedData.length > 0 && (
                        <div
                            className="absolute top-3 right-4 z-20 flex items-center rounded-lg border border-theme shadow-md"
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
                                {/* Дата — по середине вертикальной пунктирной линии.
                                    Линия идёт через два chart-wrapper'а (top + bottom), не считая навигатора.
                                    Считаем по minHeight каждого wrapper'а:
                                      top wrapper:    heights.top + 34
                                      bottom wrapper: (showPrice ? bottomDual : bottomSolo) + 24
                                    Middle = (top + bottom) / 2 от верха containerRef. */}
                                {(() => {
                                    // Единая логика date-pill через computeChartTopLineY.
                                    // Helper находит первый .chart-reveal внутри containerRef и считает y верхней линии.
                                    const topLineY = computeChartTopLineY({
                                        container: containerRef.current,
                                        paddingTop: padding.top,
                                    });
                                    return (
                                <div
                                    className="absolute z-20 pointer-events-none"
                                    style={getDatePillStyle(hoverX, topLineY)}
                                >
                                    <span
                                        className="text-theme-secondary rounded border border-theme whitespace-nowrap shadow-sm"
                                        style={{
                                            background: 'var(--bg-primary)',
                                            padding: '2px var(--sp-2)',
                                            fontSize: 'var(--fs-2xs)',
                                        }}
                                    >
                                        {dateStr}
                                    </span>
                                </div>
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
                                    <div
                                        className="rounded-lg border border-theme shadow-md"
                                        style={{
                                            background: 'var(--bg-primary)',
                                            padding: 'var(--sp-2) var(--sp-3)',
                                            fontSize: 'var(--fs-xs)',
                                        }}
                                    >
                                        {showPrice && (
                                            <div className="flex items-center justify-between gap-3 py-0.5">
                                                <div className="flex items-center gap-1.5">
                                                    <span className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: 'var(--accent)' }} />
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
                            <div className="flex items-center justify-center mb-5 relative z-10" style={{ gap: 'var(--sp-2)' }}>
                                <span className="rounded-full" style={{ width: 'var(--ico-xs)', height: 'var(--ico-xs)', backgroundColor: 'var(--accent)' }} />
                                <span className="font-semibold text-theme-primary" style={{ fontSize: 'var(--fs-sm)' }}>{currency === 'usd' ? 'Индекс RTS' : 'Индекс IMOEX'}</span>
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
                        <div className="flex items-center justify-center mb-2 relative z-10" style={{ gap: 'var(--sp-2)' }}>
                            <span className="rounded-full" style={{ width: 'var(--ico-xs)', height: 'var(--ico-xs)', backgroundColor: 'var(--accent)' }} />
                            <span className="font-semibold text-theme-primary" style={{ fontSize: 'var(--fs-sm)' }}>% акций выше EMA{emaPeriod}</span>
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
                                color="var(--accent)"
                            />
                        </div>
                    )}
                </>)}
            </div>

            </div>{/* /editorial-frame */}

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
