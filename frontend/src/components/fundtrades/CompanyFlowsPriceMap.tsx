/**
 * CompanyFlowsPriceMap — режим «Карта сделок» в «Потоках по компании».
 *
 * Недельная линия цены акции (закрытия), на которую посажены кругляши
 * месячных ЧИСТЫХ сделок фондов: зелёный = нетто-покупка, красный =
 * нетто-продажа, площадь ∝ |нетто ₽| (нормировка по максимуму видимого
 * окна навигатора). Идея режима: увидеть, НА КАКИХ уровнях цены фонды
 * покупали и продавали — а не только величину потока, как в гистограмме.
 *
 * Оформление и механика 1-в-1 с CompanyFlowsHistogram: та же карточка,
 * ChartLegend-заголовок, навигатор-брашер, watermark, пилюля даты и ТОТ ЖЕ
 * тултип (чистый поток + разбивка по фондам). Обрезка пустого левого хвоста
 * — тем же порогом MIN_VISIBLE_FLOW_MLN, чтобы оба режима стартовали с
 * одного месяца.
 *
 * Геометрия: линия и сетка — SVG в нормированных координатах (viewBox
 * 0..1000, non-scaling-stroke), кругляши — HTML-дивы поверх (left/top в %,
 * размер в px) — так радиус не искажается stretch'ем SVG и не нужен
 * ResizeObserver. Hover цепляется к БЛИЖАЙШЕМУ кругляшу по X (слоты месяцев
 * разрежены — примерно каждая 4-я неделя), курсор-линия снапится к нему.
 */
import {
    useEffect,
    useMemo,
    useLayoutEffect,
    useRef,
    useState,
} from 'react';
import { LineChart } from 'lucide-react';
import { GRID, CROSSHAIR, ANIMATION, FUND_PALETTE, cssVar } from '../../config/chartTheme';
import { useIsMobile } from '../../hooks/useIsMobile';
import ChartWatermark from '../ChartWatermark';
import ChartNavigator from '../ChartNavigator';
import ChartLegend from '../chart/ChartLegend';
import { ChartTooltip, TooltipRow, ChartDatePill } from '../chart';
import { computeChartTopLineY } from '../chart/datePillLayout';
import type { CompanyFlowsSeries } from './CompanyFlowsHistogram';

const easeOutCubic = ANIMATION.easing;

// Тот же порог, что в CompanyFlowsHistogram: оба режима обрезают пустой
// левый хвост истории по первому месяцу с |нетто| ≥ 1 млн ₽.
const MIN_VISIBLE_FLOW_MLN = 1;

// Радиусы кругляшей, px. Площадь ∝ |нетто| → r ∝ sqrt(|нетто|/max по окну).
const R_MIN = 4;
const R_MAX = 16;

// Цвет линии цены — deep indigo, первый цвет FUND_PALETTE: им же красится первый
// период на «Сезонности». Раньше линия шла акцентным рыжим, но он спорил с
// красными кругляшами продаж (оба тёплые) — на плотных участках было не понять,
// где линия, а где сделка. Индиго холодный и от зелёного с красным далеко.
const PRICE_LINE_COLOR = FUND_PALETTE[0];

interface CompanyFlowsPriceMapProps {
    /** "YYYY-MM" — месячная ось потоков (уже обрезана периодом в родителе). */
    months: string[];
    /** Серии по фондам (₽, выровнено с months) — сумма даёт нетто месяца. */
    series: CompanyFlowsSeries[];
    /** ISO-даты понедельников недель, ASC — вся история цены. */
    weeks: string[];
    /** Недельные закрытия, выровнено с weeks. */
    closes: number[];
    /** Имя бумаги для легенды («Полюс») — подпись линии цены. */
    assetName?: string;
    height?: number;
    loading?: boolean;
    /** Все фонды сняты пользователем — empty-state (как в гистограмме). */
    noFundsSelected?: boolean;
    /** Нет истории цены (не акция / нет candles) — свой empty-state. */
    priceMissing?: boolean;
    /** Перезапуск анимации: меняй при смене бумаги / фондов / периода. */
    animTrigger?: string;
}

// ── Форматтеры (fmtFlow/fmtMln — 1-в-1 из CompanyFlowsHistogram) ──
function fmtMlnNumber(abs: number): string {
    return abs >= 10 ? Math.round(abs).toLocaleString('ru-RU') : abs.toFixed(1);
}

function fmtFlow(v: number): string {
    const sign = v > 0 ? '+' : v < 0 ? '−' : '';
    return `${sign}${fmtMlnNumber(Math.abs(v))} млн ₽`;
}

// Подпись оси Y — цена, ₽: адаптивная точность (5 300 / 512.4 / 84.12 / 0.5613).
function fmtPrice(v: number): string {
    if (v >= 1000) return Math.round(v).toLocaleString('ru-RU');
    if (v >= 100) return v.toFixed(1);
    if (v >= 1) return v.toFixed(2);
    return v.toFixed(4);
}

function monthLabel(m: string): string {
    return new Date(`${m}-01T00:00:00`).toLocaleDateString('ru-RU', { month: 'long', year: 'numeric' });
}

export default function CompanyFlowsPriceMap({
    months: monthsAll,
    series: seriesAll,
    weeks: weeksAll,
    closes: closesAll,
    assetName,
    height = 420,
    loading = false,
    noFundsSelected = false,
    priceMissing = false,
    animTrigger,
}: CompanyFlowsPriceMapProps) {
    const isMobile = useIsMobile();
    const containerRef = useRef<HTMLDivElement>(null);
    const svgRef = useRef<SVGSVGElement>(null);

    // ── Обрезка пустого левого хвоста — тем же порогом, что в гистограмме. ──
    const trimStart = useMemo(() => {
        const n = monthsAll.length;
        for (let i = 0; i < n; i++) {
            let net = 0;
            for (const s of seriesAll) {
                const v = s.values[i];
                if (v == null || Number.isNaN(v)) continue;
                net += v / 1e6;
            }
            if (Math.abs(net) >= MIN_VISIBLE_FLOW_MLN) return i;
        }
        return 0;
    }, [monthsAll, seriesAll]);

    const months = useMemo(() => monthsAll.slice(trimStart), [monthsAll, trimStart]);
    const series = useMemo<CompanyFlowsSeries[]>(
        () => seriesAll.map(s => ({ ...s, values: s.values.slice(trimStart) })),
        [seriesAll, trimStart],
    );

    // Нетто месяца (млн ₽) — сумма по переданным (уже отфильтрованным) фондам.
    const netMln = useMemo(() => {
        const n = months.length;
        const out = new Array<number | null>(n).fill(null);
        for (const s of series) {
            for (let i = 0; i < n; i++) {
                const v = s.values[i];
                if (v == null || Number.isNaN(v)) continue;
                out[i] = (out[i] ?? 0) + v / 1e6;
            }
        }
        return out;
    }, [months, series]);

    // ── Недели цены, обрезанные до окна месяцев потоков. ──
    // Сравнение "YYYY-MM-DD".slice(0,7) с "YYYY-MM" — лексикографика ISO.
    const { weeks, closes } = useMemo(() => {
        if (!months.length) return { weeks: [] as string[], closes: [] as number[] };
        const lo = months[0];
        const hi = months[months.length - 1];
        const w: string[] = [];
        const c: number[] = [];
        for (let i = 0; i < weeksAll.length; i++) {
            const m = weeksAll[i].slice(0, 7);
            if (m < lo || m > hi) continue;
            const v = closesAll[i];
            if (v == null || !(v > 0)) continue;
            w.push(weeksAll[i]);
            c.push(v);
        }
        return { weeks: w, closes: c };
    }, [months, weeksAll, closesAll]);

    const hasData = weeks.length > 1 && months.length > 0;

    // ── Кругляши: месяц с нетто ≠ 0 → якорь на ПОСЛЕДНЕЙ неделе месяца. ──
    // Месяцы без недель цены (история цены короче истории потоков) — скипаем.
    const markers = useMemo(() => {
        const lastWeekOfMonth = new Map<string, number>();
        for (let i = 0; i < weeks.length; i++) lastWeekOfMonth.set(weeks[i].slice(0, 7), i);
        const out: { wi: number; mi: number; net: number }[] = [];
        for (let mi = 0; mi < months.length; mi++) {
            const net = netMln[mi];
            if (net == null || net === 0) continue;
            const wi = lastWeekOfMonth.get(months[mi]);
            if (wi == null) continue;
            out.push({ wi, mi, net });
        }
        return out;
    }, [weeks, months, netMln]);

    // ── Навигатор: [start, end] по индексам НЕДЕЛЬ. ──
    const [navRange, setNavRange] = useState<[number, number]>([0, 0]);
    useLayoutEffect(() => {
        if (weeks.length > 0) setNavRange([0, weeks.length - 1]);
    }, [weeks.length, animTrigger]);

    const navigatorData = useMemo(
        () => weeks.map((w, i) => ({ time: w, value: closes[i] ?? 0 })),
        [weeks, closes],
    );

    // ── Видимое окно ──
    const visStart = navRange[0];
    const visCount = Math.max(navRange[1] - navRange[0] + 1, 1);
    const visCloses = useMemo(() => closes.slice(navRange[0], navRange[1] + 1), [closes, navRange]);
    const visMarkers = useMemo(
        () => markers.filter(m => m.wi >= navRange[0] && m.wi <= navRange[1]),
        [markers, navRange],
    );

    // Шкала цены видимого окна, с полями 6% сверху/снизу.
    const [priceLo, priceHi] = useMemo(() => {
        if (!visCloses.length) return [0, 1];
        let lo = Math.min(...visCloses);
        let hi = Math.max(...visCloses);
        if (lo === hi) { lo *= 0.97; hi *= 1.03; }
        const pad = (hi - lo) * 0.06;
        return [lo - pad, hi + pad];
    }, [visCloses]);

    // Нормировка размера: максимум |нетто| среди ВИДИМЫХ кругляшей.
    const maxAbsNet = useMemo(
        () => Math.max(...visMarkers.map(m => Math.abs(m.net)), 0.001),
        [visMarkers],
    );

    // Координаты в долях области графика: x — центр слота недели (как бары
    // гистограммы), y — положение цены между priceLo..priceHi (3..97%).
    const xFrac = (wi: number) => (wi - visStart + 0.5) / visCount;
    const yFrac = (close: number) => 0.03 + (1 - (close - priceLo) / (priceHi - priceLo)) * 0.94;

    const rFor = (net: number) => R_MIN + (R_MAX - R_MIN) * Math.sqrt(Math.abs(net) / maxAbsNet);

    // ── Каскадное появление кругляшей (масштаб 0→1, слева направо). ──
    const [elapsed, setElapsed] = useState<number>(ANIMATION.waveDuration);
    const rafRef = useRef<number | null>(null);
    const markersSig = useMemo(() => markers.map(m => m.mi).join(','), [markers]);
    useEffect(() => {
        if (rafRef.current) cancelAnimationFrame(rafRef.current);
        let start: number | null = null;
        const tick = (ts: number) => {
            if (start == null) start = ts;
            const e = ts - start;
            setElapsed(e);
            if (e < ANIMATION.waveDuration) rafRef.current = requestAnimationFrame(tick);
        };
        rafRef.current = requestAnimationFrame(tick);
        return () => {
            if (rafRef.current) cancelAnimationFrame(rafRef.current);
        };
    }, [markersSig, animTrigger]);
    const popFor = (orderIdx: number, total: number) => {
        const delay = (orderIdx / Math.max(total, 1)) * ANIMATION.waveStagger;
        const t = Math.min(Math.max(elapsed - delay, 0) / (ANIMATION.waveDuration - ANIMATION.waveStagger), 1);
        return easeOutCubic(t);
    };

    // ── Hover: ближайший видимый кругляш по X. ──
    const [hoveredIdx, setHoveredIdx] = useState<number | null>(null); // индекс в visMarkers
    const [tooltipPos, setTooltipPos] = useState<{ x: number; y: number } | null>(null);

    const handleMouseMove = (e: React.MouseEvent<HTMLDivElement>) => {
        if (!visMarkers.length || !containerRef.current || !svgRef.current) return;
        const containerRect = containerRef.current.getBoundingClientRect();
        const svgRect = svgRef.current.getBoundingClientRect();
        const xInChart = e.clientX - svgRect.left;
        if (xInChart < 0 || xInChart > svgRect.width) return;
        let best = 0;
        let bestDist = Infinity;
        for (let i = 0; i < visMarkers.length; i++) {
            const mx = xFrac(visMarkers[i].wi) * svgRect.width;
            const d = Math.abs(mx - xInChart);
            if (d < bestDist) { bestDist = d; best = i; }
        }
        setHoveredIdx(best);
        const snapX = (svgRect.left - containerRect.left) + xFrac(visMarkers[best].wi) * svgRect.width;
        setTooltipPos({ x: snapX, y: e.clientY - containerRect.top });
    };
    const handleMouseLeave = () => {
        setHoveredIdx(null);
        setTooltipPos(null);
    };

    // Разбивка по фондам для hovered месяца — 1-в-1 логика гистограммы.
    const hoverBreakdown = useMemo(() => {
        if (hoveredIdx == null || !visMarkers[hoveredIdx]) return null;
        const mi = visMarkers[hoveredIdx].mi;
        const rows = series
            .map(s => {
                const raw = s.values[mi];
                return raw == null || Number.isNaN(raw)
                    ? null
                    : { label: s.label, color: s.color, mln: raw / 1e6 };
            })
            .filter((r): r is { label: string; color: string; mln: number } => r != null && r.mln !== 0)
            .sort((a, b) => Math.abs(b.mln) - Math.abs(a.mln));
        const net = rows.reduce((acc, r) => acc + r.mln, 0);
        return { rows, net };
    }, [hoveredIdx, visMarkers, series]);

    // Линия цены: path в нормированных координатах 0..1000 (non-scaling-stroke).
    const linePath = useMemo(() => {
        if (visCloses.length < 2) return '';
        const pts: string[] = [];
        for (let i = 0; i < visCloses.length; i++) {
            const x = ((i + 0.5) / visCount) * 1000;
            const y = yFrac(visCloses[i]) * 1000;
            pts.push(`${i === 0 ? 'M' : 'L'}${x.toFixed(2)},${y.toFixed(2)}`);
        }
        return pts.join(' ');
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [visCloses, visCount, priceLo, priceHi]);

    // Тики сетки/оси Y — 5 равных шагов цены.
    const priceTicks = useMemo(
        () => Array.from({ length: 5 }, (_, i) => priceLo + ((4 - i) / 4) * (priceHi - priceLo)),
        [priceLo, priceHi],
    );

    // ─────────────────────────────────────────────────────────────────────
    return (
        <div className="rounded-2xl p-5 bg-theme-primary border border-theme relative" style={{ ['--chart-height' as string]: `${height}px` }}>
            {noFundsSelected ? (
                <div
                    className="flex flex-col items-center justify-center text-center"
                    style={{ height: 'calc(var(--chart-height, 420px) + 100px)', gap: 'var(--sp-3)', padding: 'var(--sp-6)' }}
                >
                    <div
                        style={{
                            width: 56, height: 56, borderRadius: 12,
                            background: 'var(--accent)', border: '2px solid var(--text-primary)',
                            boxShadow: 'var(--shadow-hard-chip, 3px 3px 0 var(--text-primary))',
                            display: 'flex', alignItems: 'center', justifyContent: 'center',
                            marginBottom: 'var(--sp-2)',
                        }}
                    >
                        <LineChart size={28} strokeWidth={2.4} color="#FFFFFF" />
                    </div>
                    <div className="font-semibold text-theme-primary" style={{ fontSize: 'var(--fs-lg)' }}>
                        Не выбрано ни одного фонда
                    </div>
                    <div className="text-theme-secondary" style={{ fontSize: 'var(--fs-sm)', maxWidth: 360 }}>
                        Отметьте фонды в фильтре выше, чтобы увидеть сделки на графике цены.
                    </div>
                </div>
            ) : priceMissing ? (
                <div
                    className="flex flex-col items-center justify-center text-center"
                    style={{ height: 'calc(var(--chart-height, 420px) + 100px)', gap: 'var(--sp-3)', padding: 'var(--sp-6)' }}
                >
                    <div
                        style={{
                            width: 56, height: 56, borderRadius: 12,
                            background: 'var(--accent)', border: '2px solid var(--text-primary)',
                            boxShadow: 'var(--shadow-hard-chip, 3px 3px 0 var(--text-primary))',
                            display: 'flex', alignItems: 'center', justifyContent: 'center',
                            marginBottom: 'var(--sp-2)',
                        }}
                    >
                        <LineChart size={28} strokeWidth={2.4} color="#FFFFFF" />
                    </div>
                    <div className="font-semibold text-theme-primary" style={{ fontSize: 'var(--fs-lg)' }}>
                        Нет истории цены по этой бумаге
                    </div>
                    <div className="text-theme-secondary" style={{ fontSize: 'var(--fs-sm)', maxWidth: 380 }}>
                        Карта сделок доступна для акций, торгующихся на МосБирже.
                        Потоки по этой бумаге смотрите в режиме «Столбцы».
                    </div>
                </div>
            ) : loading && !hasData ? (
                <div className="flex items-center justify-center" style={{ height: 'calc(var(--chart-height, 420px) + 100px)' }}>
                    <div className="flex flex-col items-center" style={{ gap: 'var(--sp-3)' }}>
                        <div className="w-8 h-8 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
                        <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-base)' }}>Загрузка...</span>
                    </div>
                </div>
            ) : !hasData ? (
                <div className="flex items-center justify-center text-center" style={{ height: 'calc(var(--chart-height, 420px) + 100px)', color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)' }}>
                    Нет данных за период
                </div>
            ) : (<>
                {loading && (
                    <div className="absolute top-4 left-4 z-20 flex items-center gap-2 rounded-lg border border-theme shadow-md" style={{ background: 'var(--bg-primary)', padding: 'var(--sp-2) var(--sp-3)' }}>
                        <div className="w-4 h-4 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
                        <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-xs)' }}>Обновление...</span>
                    </div>
                )}

                <div>
                    {/* Легенда: линия цены (имя бумаги) + кругляши сделок. Знак
                        сделки несёт ОДИН двухцветный маркер, а не две отдельные
                        записи «покупки»/«продажи»: кругляши это одна серия с двумя
                        знаками, и раздельные записи читались как две разные. */}
                    <div style={{ marginTop: 'calc(var(--chart-legend-top-gap, 8px) - 20px)', marginBottom: 'var(--chart-legend-mb, 16px)' }}>
                        <ChartLegend
                            items={[
                                { color: PRICE_LINE_COLOR, label: assetName || 'Цена', marker: 'dot' },
                                {
                                    color: 'var(--funds-flow-negative)',
                                    colorRight: 'var(--funds-flow-positive)',
                                    label: 'Чистые покупки и продажи (млн ₽)',
                                    marker: 'split',
                                },
                            ]}
                            fontWeight={600}
                            itemGap={6}
                            gap="clamp(6px, 1vw, 16px)"
                            style={{ color: 'var(--text-primary)' }}
                        />
                    </div>

                    {/* График с тултипом */}
                    <div
                        ref={containerRef}
                        className="relative cursor-crosshair chart-reveal"
                        style={{ height: 'var(--chart-height, 420px)', display: 'flow-root', touchAction: 'none' }}
                        onMouseMove={handleMouseMove}
                        onMouseLeave={handleMouseLeave}
                        onTouchStart={(e) => {
                            if (!e.touches[0]) return;
                            const t = e.touches[0];
                            handleMouseMove({ clientX: t.clientX, clientY: t.clientY, currentTarget: e.currentTarget } as React.MouseEvent<HTMLDivElement>);
                        }}
                        onTouchMove={(e) => {
                            if (!e.touches[0]) return;
                            const t = e.touches[0];
                            handleMouseMove({ clientX: t.clientX, clientY: t.clientY, currentTarget: e.currentTarget } as React.MouseEvent<HTMLDivElement>);
                        }}
                        onTouchEnd={handleMouseLeave}
                    >
                        {/* Область графика — те же CSS-отступы, что в гистограмме. */}
                        <div className="absolute" style={{ top: 'var(--chart-pad-top, 19px)', bottom: 'var(--chart-pad-bottom, 50px)', left: 'var(--chart-pad-left, 100px)', right: 'var(--chart-pad-right-single, 95px)' }}>
                            <svg ref={svgRef} width="100%" height="100%" viewBox="0 0 1000 1000" preserveAspectRatio="none">
                                {/* Горизонтальная сетка по тикам цены */}
                                {priceTicks.map((p, i) => {
                                    const y = yFrac(p) * 1000;
                                    return (
                                        <line key={`grid-${i}`} x1="0" y1={y} x2="1000" y2={y} stroke={GRID.major} strokeWidth="1" vectorEffect="non-scaling-stroke" />
                                    );
                                })}

                                {/* Линия цены — акцентный цвет, толщина как primary-линии. */}
                                {linePath && (
                                    <path
                                        d={linePath}
                                        fill="none"
                                        stroke={PRICE_LINE_COLOR}
                                        strokeWidth="2"
                                        vectorEffect="non-scaling-stroke"
                                        strokeLinecap="round"
                                        strokeLinejoin="round"
                                    />
                                )}

                                {/* Вертикальный курсор — снап к hovered кругляшу. */}
                                {hoveredIdx !== null && visMarkers[hoveredIdx] && (
                                    <line
                                        x1={xFrac(visMarkers[hoveredIdx].wi) * 1000}
                                        y1="30"
                                        x2={xFrac(visMarkers[hoveredIdx].wi) * 1000}
                                        y2="970"
                                        stroke={CROSSHAIR.accentColor}
                                        strokeWidth="1"
                                        vectorEffect="non-scaling-stroke"
                                        strokeDasharray={CROSSHAIR.accentDashArray}
                                        opacity={CROSSHAIR.accentOpacity}
                                        style={{ pointerEvents: 'none' }}
                                    />
                                )}
                            </svg>

                            {/* Кругляши сделок — HTML-дивы поверх SVG: размер в px не
                                искажается stretch'ем viewBox. Сидят на линии цены. */}
                            {visMarkers.map((m, i) => {
                                const close = closes[m.wi];
                                const r = rFor(m.net) * popFor(i, visMarkers.length);
                                if (r <= 0.2) return null;
                                const dim = hoveredIdx !== null && hoveredIdx !== i;
                                return (
                                    <div
                                        key={m.mi}
                                        style={{
                                            position: 'absolute',
                                            left: `${xFrac(m.wi) * 100}%`,
                                            top: `${yFrac(close) * 100}%`,
                                            width: r * 2,
                                            height: r * 2,
                                            transform: 'translate(-50%, -50%)',
                                            borderRadius: '50%',
                                            background: m.net > 0 ? 'var(--funds-flow-positive)' : 'var(--funds-flow-negative)',
                                            border: '1.5px solid var(--bg-primary)',
                                            boxSizing: 'border-box',
                                            opacity: dim ? 0.45 : 1,
                                            pointerEvents: 'none',
                                        }}
                                    />
                                );
                            })}
                        </div>

                        {/* Watermark — как в гистограмме. */}
                        <ChartWatermark
                            left="calc(var(--chart-pad-left, 100px) + 5px)"
                            bottom="calc(var(--chart-pad-bottom, 50px) + 0.03 * (var(--chart-height, 420px) - var(--chart-pad-top, 19px) - var(--chart-pad-bottom, 50px)) + 5px)"
                        />

                        {/* Тултип — 1-в-1 контент гистограммы: нетто + разбивка. */}
                        {hoveredIdx !== null && tooltipPos && hoverBreakdown && (() => {
                            const net = hoverBreakdown.net;
                            const netColor = net >= 0 ? 'var(--funds-flow-positive)' : 'var(--funds-flow-negative)';
                            const MAX_ROWS = 6;
                            const shown = hoverBreakdown.rows.slice(0, MAX_ROWS);
                            const extra = hoverBreakdown.rows.length - shown.length;
                            return (
                                <ChartTooltip x={tooltipPos.x} y={tooltipPos.y} clampTop={cssVar('--chart-pad-top', 14)} clampBottom={cssVar('--chart-pad-bottom', 50)}>
                                    <TooltipRow
                                        hideDot
                                        color={netColor}
                                        label={net >= 0 ? 'Чистая покупка' : 'Чистая продажа'}
                                        value={fmtFlow(net)}
                                        labelClass="font-bold"
                                        labelColor="var(--text-primary)"
                                    />
                                    {shown.length > 0 && (
                                        <div style={{ marginTop: 'var(--sp-1)', paddingTop: 'var(--sp-1)', borderTop: '1px solid var(--border-color)' }}>
                                            {shown.map((r, i) => (
                                                <TooltipRow
                                                    key={i}
                                                    hideDot
                                                    color={r.color}
                                                    label={r.label}
                                                    value={fmtFlow(r.mln)}
                                                    labelClass="font-semibold"
                                                    labelColor="var(--axis-color, #9CA3B8)"
                                                    valueColor="var(--axis-color, #9CA3B8)"
                                                />
                                            ))}
                                            {extra > 0 && (
                                                <div className="text-theme-secondary" style={{ fontSize: 'var(--fs-2xs)', marginTop: 'var(--sp-1)' }}>
                                                    и ещё {extra} {extra === 1 ? 'фонд' : extra >= 2 && extra <= 4 ? 'фонда' : 'фондов'}
                                                </div>
                                            )}
                                        </div>
                                    )}
                                </ChartTooltip>
                            );
                        })()}

                        {/* Подписи цены справа (ось Y) */}
                        <div className="absolute pointer-events-none" style={{ top: 'var(--chart-pad-top, 19px)', bottom: 'var(--chart-pad-bottom, 50px)', right: 0, width: 'var(--chart-pad-right-single, 95px)' }}>
                            {priceTicks.map((p, i) => (
                                <div key={`label-${i}`} className="absolute" style={{ top: `${yFrac(p) * 100}%`, left: 12, transform: 'translateY(-50%)' }}>
                                    <span className="font-semibold" style={{ fontSize: 'var(--chart-font-y, 16px)', color: 'var(--axis-color, #9CA3B8)' }}>
                                        {fmtPrice(p)}
                                    </span>
                                </div>
                            ))}
                        </div>

                        {/* Подписи оси X — месяц/год по видимым неделям. */}
                        <div className="absolute flex justify-between font-semibold px-2" style={{ bottom: 'var(--chart-xlabel-bottom, 20px)', left: 'var(--chart-pad-left, 100px)', right: 'var(--chart-pad-right-single, 95px)', fontSize: 'var(--chart-font-x, 14px)', color: 'var(--axis-color, #9CA3B8)' }}>
                            {(() => {
                                const vis = weeks.slice(navRange[0], navRange[1] + 1);
                                if (!vis.length) return null;
                                const tickCount = Math.min(isMobile ? 3 : 6, vis.length);
                                return Array.from({ length: tickCount }, (_, i) => {
                                    const idx = Math.min(Math.round(i * (vis.length - 1) / Math.max(tickCount - 1, 1)), vis.length - 1);
                                    if (!vis[idx]) return null;
                                    return (
                                        <span key={i}>{new Date(`${vis[idx]}T00:00:00`).toLocaleDateString('ru-RU', { month: 'short', year: '2-digit' })}</span>
                                    );
                                });
                            })()}
                        </div>
                    </div>

                    {/* Навигатор — та же линия цены в миниатюре, поэтому и цвет тот
                        же (в остальных графиках рельс акцентный, но здесь рыжий
                        рельс под синей линией читался бы как другая серия). */}
                    {navigatorData.length > 1 && (
                        <div data-export-ignore="true">
                            <ChartNavigator
                                data={navigatorData}
                                color={PRICE_LINE_COLOR}
                                previewMode="line"
                                onChange={(s, e) => setNavRange([s, e])}
                                insetLeft="var(--chart-pad-left)"
                                insetRight="var(--chart-pad-right-single)"
                            />
                        </div>
                    )}
                </div>

                {/* Плавающая пилюля даты — месяц hovered кругляша. */}
                {hoveredIdx !== null && visMarkers[hoveredIdx] && (() => {
                    const m = months[visMarkers[hoveredIdx].mi];
                    if (!m) return null;
                    const cont = containerRef.current;
                    const svg = svgRef.current;
                    if (!cont || !svg) return null;
                    const containerRect = cont.getBoundingClientRect();
                    const svgRect = svg.getBoundingClientRect();
                    const padTop = svgRect.top - containerRect.top;
                    const chartW = svgRect.width;
                    const chartAreaH = svgRect.height;
                    const centerX = cont.offsetLeft + (svgRect.left - containerRect.left) + xFrac(visMarkers[hoveredIdx].wi) * chartW;
                    const topLineY = computeChartTopLineY({
                        wrapper: cont,
                        paddingTop: padTop,
                        gridOffsetFrac: 0.03,
                        chartAreaHeight: chartAreaH,
                    });
                    const plotLeft = cont.offsetLeft + (svgRect.left - containerRect.left);
                    return (
                        <ChartDatePill
                            date={monthLabel(m)}
                            x={centerX}
                            topLineY={topLineY}
                            minX={plotLeft}
                            maxX={plotLeft + chartW}
                        />
                    );
                })()}
            </>)}
        </div>
    );
}
