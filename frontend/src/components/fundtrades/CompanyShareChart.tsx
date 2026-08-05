/**
 * CompanyShareChart — режим «Доля» в «Потоках по компании».
 *
 * Две синхронные панели (по образцу «Силы рынка»: сверху цена, снизу
 * гистограмма): верх — недельная линия цены акции (контекст), низ —
 * помесячная гистограмма ДОЛИ бумаги в портфелях выбранных фондов.
 * Видно, как менялась ставка фондов на бумагу против движения цены.
 *
 * Доля месяца — два веса (тумблер «По капиталу / По доле», как в «Общем
 * портфеле»):
 *   rub   — Σ(СЧА_фонда × доля) / Σ(СЧА_фонда): доля бумаги в общем котле,
 *           крупные фонды весомее;
 *   share — простое среднее долей по фондам: консенсус управляющих без
 *           перекоса на гигантов.
 * В расчёт месяца входят только фонды с ПОЛНЫМ снапшотом (weights[i] != null);
 * 0 — честный ноль (фонд отчитался без бумаги), null — дыра данных → месяц
 * без бара (разрыв), а не ложный ноль.
 *
 * Ось X — МЕСЯЦЫ (слоты). Недели цены раскладываются внутри слота своего
 * месяца дробно ((k+0.5)/K), поэтому линия остаётся гладкой, а бар месяца
 * геометрически совпадает со своим участком линии. Нет истории цены
 * (облигация/ОФЗ) → гистограмма занимает всю высоту, режим живёт без линии.
 *
 * Оформление и механика — 1-в-1 с CompanyFlowsPriceMap: та же карточка,
 * ChartLegend, навигатор-брашер, watermark, пилюля даты, тултип. В тултипе —
 * доля + «Сделки за месяц» (нетто из /company-flows: подсказка, двигали долю
 * сделки или переоценка) + разбивка долей по фондам.
 */
import {
    useEffect,
    useMemo,
    useLayoutEffect,
    useRef,
    useState,
} from 'react';
import { Percent } from 'lucide-react';
import { GRID, CROSSHAIR, ANIMATION, FUND_PALETTE, cssVar } from '../../config/chartTheme';
import { useIsMobile } from '../../hooks/useIsMobile';
import ChartWatermark from '../ChartWatermark';
import ChartNavigator from '../ChartNavigator';
import ChartLegend from '../chart/ChartLegend';
import { ChartTooltip, TooltipRow, ChartDatePill } from '../chart';
import { computeChartTopLineY } from '../chart/datePillLayout';

const easeOutCubic = ANIMATION.easing;

// Цвет линии цены — как в «Карте сделок» (deep indigo, холодный, не спорит
// с акцентными барами доли).
const PRICE_LINE_COLOR = FUND_PALETTE[0];
// Бары доли — акцентный: доля не несёт знака, зелёный/красный тут были бы ложью.
const BAR_COLOR = 'var(--accent)';

// Раскладка панелей в viewBox 0..1000: верх — цена, низ — доля.
const PRICE_BOT = 560;   // низ ценовой панели
const SHARE_TOP = 640;   // верх панели доли (между ними — визуальный зазор)
const SHARE_BOT = 1000;  // базовая линия баров

export type ShareMode = 'rub' | 'share';

export interface CompanyShareFundSeries {
    label: string;
    color: string;
    /** Доля бумаги в % от СЧА фонда, по месяцам (null = нет полного снапшота). */
    weights: (number | null)[];
    /** Полная СЧА фонда на тот же снапшот (для веса «по капиталу»). */
    navs: (number | null)[];
}

interface CompanyShareChartProps {
    /** "YYYY-MM" — месячная ось (уже обрезана периодом в родителе). */
    months: string[];
    /** Серии по фондам (уже отфильтрованные пикером), выровнено с months. */
    funds: CompanyShareFundSeries[];
    /** Нетто-сделки месяца по тем же фондам, млн ₽ (для строки тултипа). */
    netFlowMln: (number | null)[];
    /** ISO-даты понедельников недель, ASC — вся история цены. */
    weeks: string[];
    /** Недельные закрытия, выровнено с weeks. */
    closes: number[];
    shareMode: ShareMode;
    /** Имя бумаги для легенды — подпись линии цены. */
    assetName?: string;
    height?: number;
    loading?: boolean;
    /** Все фонды сняты пользователем — empty-state. */
    noFundsSelected?: boolean;
    /** Нет истории цены (не акция) → гистограмма во всю высоту. */
    priceMissing?: boolean;
    /** Перезапуск анимации: меняй при смене бумаги / фондов / периода / веса. */
    animTrigger?: string;
}

// ── Форматтеры ──
function fmtPct(v: number): string {
    const d = v >= 10 ? 1 : v >= 0.1 ? 2 : 3;
    return `${v.toLocaleString('ru-RU', { maximumFractionDigits: d })}%`;
}

function fmtMlnNumber(abs: number): string {
    return abs >= 10 ? Math.round(abs).toLocaleString('ru-RU') : abs.toFixed(1);
}

function fmtFlow(v: number): string {
    const sign = v > 0 ? '+' : v < 0 ? '−' : '';
    return `${sign}${fmtMlnNumber(Math.abs(v))} млн ₽`;
}

function fmtPrice(v: number): string {
    if (v >= 1000) return Math.round(v).toLocaleString('ru-RU');
    if (v >= 100) return v.toFixed(1);
    if (v >= 1) return v.toFixed(2);
    return v.toFixed(4);
}

function monthLabel(m: string): string {
    return new Date(`${m}-01T00:00:00`).toLocaleDateString('ru-RU', { month: 'long', year: 'numeric' });
}

export default function CompanyShareChart({
    months: monthsAll,
    funds: fundsAll,
    netFlowMln: netAll,
    weeks: weeksAll,
    closes: closesAll,
    shareMode,
    assetName,
    height = 420,
    loading = false,
    noFundsSelected = false,
    priceMissing = false,
    animTrigger,
}: CompanyShareChartProps) {
    const isMobile = useIsMobile();
    const containerRef = useRef<HTMLDivElement>(null);
    const svgRef = useRef<SVGSVGElement>(null);

    // ── Доля месяца по выбранному весу (по ПОЛНОМУ набору месяцев). ──
    const shareValsAll = useMemo(() => {
        return monthsAll.map((_, i) => {
            let num = 0, den = 0, sum = 0, cnt = 0;
            for (const f of fundsAll) {
                const w = f.weights[i];
                if (w == null) continue;
                cnt++;
                sum += w;
                const nav = f.navs[i];
                if (nav != null && nav > 0) { num += nav * w; den += nav; }
            }
            if (cnt === 0) return null;
            // «По капиталу» без единой СЧА в месяце — честнее среднее, чем дыра.
            return shareMode === 'rub' ? (den > 0 ? num / den : sum / cnt) : sum / cnt;
        });
    }, [monthsAll, fundsAll, shareMode]);

    // ── Обрезка пустого левого хвоста: до первого месяца с долей > 0. ──
    const trimStart = useMemo(() => {
        for (let i = 0; i < monthsAll.length; i++) {
            const v = shareValsAll[i];
            if (v != null && v > 0) return i;
        }
        return 0;
    }, [monthsAll, shareValsAll]);

    const months = useMemo(() => monthsAll.slice(trimStart), [monthsAll, trimStart]);
    const shareVals = useMemo(() => shareValsAll.slice(trimStart), [shareValsAll, trimStart]);
    const netFlow = useMemo(() => netAll.slice(trimStart), [netAll, trimStart]);
    const funds = useMemo(
        () => fundsAll.map(f => ({
            ...f,
            weights: f.weights.slice(trimStart),
            navs: f.navs.slice(trimStart),
        })),
        [fundsAll, trimStart],
    );

    const hasPrice = !priceMissing && weeksAll.length > 1;

    // ── Недели цены, разложенные по слотам месяцев. ──
    // Для каждого месяца — список [weekIdx...]; неделя k из K сидит на дробной
    // позиции (k+0.5)/K внутри слота. Сравнение "YYYY-MM-DD".slice(0,7) —
    // лексикографика ISO.
    const weeksByMonth = useMemo(() => {
        const map = new Map<string, number[]>();
        if (!months.length) return map;
        const lo = months[0];
        const hi = months[months.length - 1];
        for (let i = 0; i < weeksAll.length; i++) {
            const m = weeksAll[i].slice(0, 7);
            if (m < lo || m > hi) continue;
            const v = closesAll[i];
            if (v == null || !(v > 0)) continue;
            let arr = map.get(m);
            if (!arr) { arr = []; map.set(m, arr); }
            arr.push(i);
        }
        return map;
    }, [months, weeksAll, closesAll]);

    const hasData = months.length > 0 && shareVals.some(v => v != null);

    // ── Навигатор: [start, end] по индексам МЕСЯЦЕВ. ──
    const [navRange, setNavRange] = useState<[number, number]>([0, 0]);
    useLayoutEffect(() => {
        if (months.length > 0) setNavRange([0, months.length - 1]);
    }, [months.length, animTrigger]);

    // Превью навигатора: линия цены (последнее закрытие месяца); дыры тянем
    // предыдущим значением. Нет цены → сама доля.
    const navigatorData = useMemo(() => {
        let prev = 0;
        return months.map((m, i) => {
            let v: number | null = null;
            if (hasPrice) {
                const wk = weeksByMonth.get(m);
                if (wk && wk.length) v = closesAll[wk[wk.length - 1]];
            } else {
                v = shareVals[i] ?? null;
            }
            if (v == null) v = prev;
            prev = v;
            return { time: `${m}-01`, value: v };
        });
    }, [months, hasPrice, weeksByMonth, closesAll, shareVals]);

    // ── Видимое окно ──
    const visStart = navRange[0];
    const visCount = Math.max(navRange[1] - navRange[0] + 1, 1);
    const visMonthIdx = useMemo(
        () => Array.from({ length: visCount }, (_, i) => visStart + i).filter(i => i < months.length),
        [visStart, visCount, months.length],
    );

    // Шкала цены видимого окна (по неделям видимых месяцев), поля 6%.
    const [priceLo, priceHi] = useMemo(() => {
        if (!hasPrice) return [0, 1];
        const vals: number[] = [];
        for (const mi of visMonthIdx) {
            const wk = weeksByMonth.get(months[mi]);
            if (wk) for (const wi of wk) vals.push(closesAll[wi]);
        }
        if (!vals.length) return [0, 1];
        let lo = Math.min(...vals);
        let hi = Math.max(...vals);
        if (lo === hi) { lo *= 0.97; hi *= 1.03; }
        const pad = (hi - lo) * 0.06;
        return [lo - pad, hi + pad];
    }, [hasPrice, visMonthIdx, months, weeksByMonth, closesAll]);

    // Шкала доли: 0..max видимого окна ×1.12 (бар максимума не упирается в верх).
    const shareMax = useMemo(() => {
        let mx = 0;
        for (const mi of visMonthIdx) {
            const v = shareVals[mi];
            if (v != null && v > mx) mx = v;
        }
        return (mx || 0.0001) * 1.12;
    }, [visMonthIdx, shareVals]);

    // Границы панелей: без цены гистограмма забирает всю высоту.
    const shareTop = hasPrice ? SHARE_TOP : 40;

    // Координаты (viewBox 0..1000).
    const slotX = (mi: number, frac = 0.5) => ((mi - visStart + frac) / visCount) * 1000;
    const priceY = (close: number) =>
        PRICE_BOT * (0.05 + (1 - (close - priceLo) / (priceHi - priceLo)) * 0.9);
    const shareY = (v: number) => shareTop + (1 - v / shareMax) * (SHARE_BOT - shareTop);

    // Линия цены: недели видимых месяцев, дробно внутри слотов.
    const linePath = useMemo(() => {
        if (!hasPrice) return '';
        const pts: string[] = [];
        for (const mi of visMonthIdx) {
            const wk = weeksByMonth.get(months[mi]);
            if (!wk) continue;
            for (let k = 0; k < wk.length; k++) {
                const x = slotX(mi, (k + 0.5) / wk.length);
                const y = priceY(closesAll[wk[k]]);
                pts.push(`${pts.length === 0 ? 'M' : 'L'}${x.toFixed(2)},${y.toFixed(2)}`);
            }
        }
        return pts.length >= 2 ? pts.join(' ') : '';
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [hasPrice, visMonthIdx, months, weeksByMonth, closesAll, priceLo, priceHi, visStart, visCount]);

    // Тики цены (4) и доли (0 / середина / максимум).
    const priceTicks = useMemo(
        () => (hasPrice
            ? Array.from({ length: 4 }, (_, i) => priceLo + ((3 - i) / 3) * (priceHi - priceLo))
            : []),
        [hasPrice, priceLo, priceHi],
    );
    const shareTicks = useMemo(
        () => [shareMax / 1.12, shareMax / 2.24, 0],
        [shareMax],
    );

    // ── Каскадное появление баров (высота 0→1, слева направо). ──
    const [elapsed, setElapsed] = useState<number>(ANIMATION.waveDuration);
    const rafRef = useRef<number | null>(null);
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
    }, [animTrigger, shareMode]);
    const popFor = (orderIdx: number, total: number) => {
        const delay = (orderIdx / Math.max(total, 1)) * ANIMATION.waveStagger;
        const t = Math.min(Math.max(elapsed - delay, 0) / (ANIMATION.waveDuration - ANIMATION.waveStagger), 1);
        return easeOutCubic(t);
    };

    // ── Hover: ближайший месяц с данными по X. ──
    const [hoveredMi, setHoveredMi] = useState<number | null>(null); // индекс в months
    const [tooltipPos, setTooltipPos] = useState<{ x: number; y: number } | null>(null);

    const hoverableIdx = useMemo(
        () => visMonthIdx.filter(mi => shareVals[mi] != null),
        [visMonthIdx, shareVals],
    );

    const handleMouseMove = (e: React.MouseEvent<HTMLDivElement>) => {
        if (!hoverableIdx.length || !containerRef.current || !svgRef.current) return;
        const containerRect = containerRef.current.getBoundingClientRect();
        const svgRect = svgRef.current.getBoundingClientRect();
        const xInChart = e.clientX - svgRect.left;
        if (xInChart < 0 || xInChart > svgRect.width) return;
        let best = hoverableIdx[0];
        let bestDist = Infinity;
        for (const mi of hoverableIdx) {
            const mx = (slotX(mi) / 1000) * svgRect.width;
            const d = Math.abs(mx - xInChart);
            if (d < bestDist) { bestDist = d; best = mi; }
        }
        setHoveredMi(best);
        const snapX = (svgRect.left - containerRect.left) + (slotX(best) / 1000) * svgRect.width;
        setTooltipPos({ x: snapX, y: e.clientY - containerRect.top });
    };
    const handleMouseLeave = () => {
        setHoveredMi(null);
        setTooltipPos(null);
    };

    // Разбивка тултипа: доля по каждому фонду (top-6 по убыванию).
    const hoverBreakdown = useMemo(() => {
        if (hoveredMi == null) return null;
        const rows = funds
            .map(f => {
                const w = f.weights[hoveredMi];
                return w == null || w === 0 ? null : { label: f.label, color: f.color, w };
            })
            .filter((r): r is { label: string; color: string; w: number } => r != null)
            .sort((a, b) => b.w - a.w);
        return { rows, share: shareVals[hoveredMi], net: netFlow[hoveredMi] };
    }, [hoveredMi, funds, shareVals, netFlow]);

    const shareModeLabel = shareMode === 'rub' ? 'Доля в общем портфеле' : 'Средняя доля в фондах';

    // Ширина бара: 66% слота, но не тоньше 2px-эквивалента (узкое окно → шире).
    const barHalf = Math.min((0.33 / visCount) * 1000, 22);

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
                        <Percent size={28} strokeWidth={2.4} color="#FFFFFF" />
                    </div>
                    <div className="font-semibold text-theme-primary" style={{ fontSize: 'var(--fs-lg)' }}>
                        Не выбрано ни одного фонда
                    </div>
                    <div className="text-theme-secondary" style={{ fontSize: 'var(--fs-sm)', maxWidth: 360 }}>
                        Отметьте фонды в фильтре выше, чтобы увидеть долю бумаги в их портфелях.
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
                    {/* Легенда: линия цены (если есть) + бары доли. */}
                    <div style={{ marginTop: 'calc(var(--chart-legend-top-gap, 8px) - 20px)', marginBottom: 'var(--chart-legend-mb, 16px)' }}>
                        <ChartLegend
                            items={[
                                ...(hasPrice
                                    ? [{ color: PRICE_LINE_COLOR, label: assetName || 'Цена', marker: 'dot' as const }]
                                    : []),
                                { color: BAR_COLOR, label: `${shareModeLabel} (%)`, marker: 'dot' as const },
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
                                {/* Сетка панели цены */}
                                {priceTicks.map((p, i) => (
                                    <line key={`pg-${i}`} x1="0" y1={priceY(p)} x2="1000" y2={priceY(p)} stroke={GRID.major} strokeWidth="1" vectorEffect="non-scaling-stroke" />
                                ))}
                                {/* Сетка панели доли */}
                                {shareTicks.map((v, i) => (
                                    <line key={`sg-${i}`} x1="0" y1={shareY(v)} x2="1000" y2={shareY(v)} stroke={GRID.major} strokeWidth="1" vectorEffect="non-scaling-stroke" />
                                ))}

                                {/* Бары доли: null → пропуск (разрыв данных), 0 → пустой слот. */}
                                {visMonthIdx.map((mi, order) => {
                                    const v = shareVals[mi];
                                    if (v == null || v <= 0) return null;
                                    const grow = popFor(order, visMonthIdx.length);
                                    if (grow <= 0.01) return null;
                                    const x = slotX(mi);
                                    const yTop = SHARE_BOT - (SHARE_BOT - shareY(v)) * grow;
                                    const dim = hoveredMi !== null && hoveredMi !== mi;
                                    return (
                                        <rect
                                            key={mi}
                                            x={x - barHalf}
                                            y={yTop}
                                            width={barHalf * 2}
                                            height={SHARE_BOT - yTop}
                                            fill={BAR_COLOR}
                                            opacity={dim ? 0.45 : 0.92}
                                        />
                                    );
                                })}

                                {/* Линия цены поверх сетки. */}
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

                                {/* Вертикальный курсор — снап к hovered месяцу. */}
                                {hoveredMi !== null && (
                                    <line
                                        x1={slotX(hoveredMi)}
                                        y1="30"
                                        x2={slotX(hoveredMi)}
                                        y2="985"
                                        stroke={CROSSHAIR.accentColor}
                                        strokeWidth="1"
                                        vectorEffect="non-scaling-stroke"
                                        strokeDasharray={CROSSHAIR.accentDashArray}
                                        opacity={CROSSHAIR.accentOpacity}
                                        style={{ pointerEvents: 'none' }}
                                    />
                                )}
                            </svg>
                        </div>

                        {/* Watermark — как в гистограмме. */}
                        <ChartWatermark
                            left="calc(var(--chart-pad-left, 100px) + 5px)"
                            bottom="calc(var(--chart-pad-bottom, 50px) + 0.03 * (var(--chart-height, 420px) - var(--chart-pad-top, 19px) - var(--chart-pad-bottom, 50px)) + 5px)"
                        />

                        {/* Тултип: доля + сделки месяца + разбивка по фондам. */}
                        {hoveredMi !== null && tooltipPos && hoverBreakdown && hoverBreakdown.share != null && (() => {
                            const MAX_ROWS = 6;
                            const shown = hoverBreakdown.rows.slice(0, MAX_ROWS);
                            const extra = hoverBreakdown.rows.length - shown.length;
                            const net = hoverBreakdown.net;
                            return (
                                <ChartTooltip x={tooltipPos.x} y={tooltipPos.y} clampTop={cssVar('--chart-pad-top', 14)} clampBottom={cssVar('--chart-pad-bottom', 50)}>
                                    <TooltipRow
                                        hideDot
                                        color={BAR_COLOR}
                                        label={shareModeLabel}
                                        value={fmtPct(hoverBreakdown.share)}
                                        labelClass="font-bold"
                                        labelColor="var(--text-primary)"
                                    />
                                    {net != null && Math.abs(net) >= 0.05 && (
                                        <TooltipRow
                                            hideDot
                                            color={net >= 0 ? 'var(--funds-flow-positive)' : 'var(--funds-flow-negative)'}
                                            label="Сделки за месяц"
                                            value={fmtFlow(net)}
                                            labelClass="font-semibold"
                                            labelColor="var(--axis-color, #9CA3B8)"
                                            valueColor={net >= 0 ? 'var(--funds-flow-positive)' : 'var(--funds-flow-negative)'}
                                        />
                                    )}
                                    {shown.length > 0 && (
                                        <div style={{ marginTop: 'var(--sp-1)', paddingTop: 'var(--sp-1)', borderTop: '1px solid var(--border-color)' }}>
                                            {shown.map((r, i) => (
                                                <TooltipRow
                                                    key={i}
                                                    hideDot
                                                    color={r.color}
                                                    label={r.label}
                                                    value={fmtPct(r.w)}
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

                        {/* Ось Y справа: цена (верхняя панель) + доля (нижняя). */}
                        <div className="absolute pointer-events-none" style={{ top: 'var(--chart-pad-top, 19px)', bottom: 'var(--chart-pad-bottom, 50px)', right: 0, width: 'var(--chart-pad-right-single, 95px)' }}>
                            {priceTicks.map((p, i) => (
                                <div key={`pl-${i}`} className="absolute" style={{ top: `${priceY(p) / 10}%`, left: 12, transform: 'translateY(-50%)' }}>
                                    <span className="font-semibold" style={{ fontSize: 'var(--chart-font-y, 16px)', color: 'var(--axis-color, #9CA3B8)' }}>
                                        {fmtPrice(p)}
                                    </span>
                                </div>
                            ))}
                            {shareTicks.map((v, i) => (
                                <div key={`sl-${i}`} className="absolute" style={{ top: `${shareY(v) / 10}%`, left: 12, transform: 'translateY(-50%)' }}>
                                    <span className="font-semibold" style={{ fontSize: 'var(--chart-font-y, 16px)', color: 'var(--axis-color, #9CA3B8)' }}>
                                        {fmtPct(v)}
                                    </span>
                                </div>
                            ))}
                        </div>

                        {/* Подписи оси X — по видимым месяцам. */}
                        <div className="absolute flex justify-between font-semibold px-2" style={{ bottom: 'var(--chart-xlabel-bottom, 20px)', left: 'var(--chart-pad-left, 100px)', right: 'var(--chart-pad-right-single, 95px)', fontSize: 'var(--chart-font-x, 14px)', color: 'var(--axis-color, #9CA3B8)' }}>
                            {(() => {
                                const vis = visMonthIdx.map(mi => months[mi]);
                                if (!vis.length) return null;
                                const tickCount = Math.min(isMobile ? 3 : 6, vis.length);
                                return Array.from({ length: tickCount }, (_, i) => {
                                    const idx = Math.min(Math.round(i * (vis.length - 1) / Math.max(tickCount - 1, 1)), vis.length - 1);
                                    if (!vis[idx]) return null;
                                    return (
                                        <span key={i}>{new Date(`${vis[idx]}-01T00:00:00`).toLocaleDateString('ru-RU', { month: 'short', year: '2-digit' })}</span>
                                    );
                                });
                            })()}
                        </div>
                    </div>

                    {/* Навигатор: превью — линия цены (или доли, если цены нет). */}
                    {navigatorData.length > 1 && (
                        <div data-export-ignore="true">
                            <ChartNavigator
                                data={navigatorData}
                                color={hasPrice ? PRICE_LINE_COLOR : BAR_COLOR}
                                previewMode="line"
                                onChange={(s, e) => setNavRange([s, e])}
                                insetLeft="var(--chart-pad-left)"
                                insetRight="var(--chart-pad-right-single)"
                            />
                        </div>
                    )}
                </div>

                {/* Плавающая пилюля даты — hovered месяц. */}
                {hoveredMi !== null && (() => {
                    const m = months[hoveredMi];
                    if (!m) return null;
                    const cont = containerRef.current;
                    const svg = svgRef.current;
                    if (!cont || !svg) return null;
                    const containerRect = cont.getBoundingClientRect();
                    const svgRect = svg.getBoundingClientRect();
                    const padTop = svgRect.top - containerRect.top;
                    const chartW = svgRect.width;
                    const chartAreaH = svgRect.height;
                    const centerX = cont.offsetLeft + (svgRect.left - containerRect.left) + (slotX(hoveredMi) / 1000) * chartW;
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
