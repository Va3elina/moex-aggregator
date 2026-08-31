/**
 * FlowsHistogram — режим «Притоки-Оттоки» в «Деньгах в фондах».
 *
 * Оформление — по макету «Позиции/% в обращении/Навеса» в «Потоках по
 * компании» (CompanyShareChart): ДВЕ визуально разделённые секции в одной
 * карточке, у каждой своя центрированная легенда, между ними разделитель
 * border-b. Верхняя — линия бенчмарка категории (IMOEX/RGBITR/…, контекст),
 * нижняя — биполярная гистограмма чистых притоков/оттоков. Курсор общий:
 * вертикаль идёт сквозь обе панели, тултип один на обе.
 *
 * Ось X — СЛОТЫ ПЕРИОДОВ потоков (день/неделя/месяц по flowTimeframe).
 * Дневные закрытия индекса раскладываются внутри слота своего периода дробно
 * ((k+0.5)/K): линия остаётся гладкой, а бар периода геометрически совпадает
 * со своим участком линии — таймфреймы разные, сетка одна. Нет данных индекса
 * (или слой выключен) → верхней секции нет, гистограмма забирает всю высоту.
 *
 * Механика (навигатор, морф линии и баров, reveal, пилюля даты, axis-пилюли,
 * watermark) — 1-в-1 с CompanyShareChart.
 */
import {
    useEffect,
    useMemo,
    useLayoutEffect,
    useRef,
    useState,
} from 'react';
import { BarChart3 } from 'lucide-react';
import type { FundsFlowsResponse, IndexDataPoint } from '../../services/api';
import { GRID, CROSSHAIR, ANIMATION, cssVar } from '../../config/chartTheme';
import { useIsMobile } from '../../hooks/useIsMobile';
import ChartWatermark from '../ChartWatermark';
import ChartNavigator from '../ChartNavigator';
import ChartLegend from '../chart/ChartLegend';
import { ChartTooltip, TooltipRow, ChartDatePill, ChartAxisPill } from '../chart';
import { useChartReveal } from '../chart/useChartReveal';
import { resampleVals, morphPts } from '../../utils/chartAnimation';
import { useNoChartAnim } from '../chart/chartAnim';

const easeOutCubic = ANIMATION.easing;

// Цвет линии бенчмарка — акцентный оранжевый, как линия цены в «Потоках по
// компании»: контекст, который тянет взгляд. Бары остаются в знаковой палитре
// притоков/оттоков (зелёный/красный) — у них цвет несёт смысл.
const INDEX_LINE_COLOR = 'var(--accent)';
const FLOW_POS = 'var(--funds-flow-positive)';
const FLOW_NEG = 'var(--funds-flow-negative)';

// Полоса подписей оси X внутри нижней секции, px.
const XLABEL_H = 40;

interface FlowsHistogramProps {
    flowsData: FundsFlowsResponse | null;
    /** Все фонды категории выключены пользователем — empty-state. */
    noFundsSelected?: boolean;
    loading: boolean;
    /** Обобщающий заголовок гистограммы («Чистые притоки и оттоки … (млрд ₽)»). */
    flowTitle?: string;
    /** Дневные закрытия бенчмарка категории (index из getFundsChartData). */
    indexData?: IndexDataPoint[];
    /** Имя бенчмарка для легенды (IMOEX, RGBITR, …). */
    indexLabel?: string;
    /** Слой «Индекс» выключен — верхней панели нет. */
    showIndex?: boolean;
    /** Общий бюджет высоты карточки (как chartHeight в SimpleChart). */
    height?: number;
    /** Смена категории/ТФ/периода: сброс навигатора. */
    animTrigger?: string;
}

function fmtPrice(v: number): string {
    if (v >= 1000) return Math.round(v).toLocaleString('ru-RU');
    if (v >= 100) return v.toFixed(1);
    if (v >= 1) return v.toFixed(2);
    return v.toFixed(4);
}

// Ось потоков: короткий знак-значение («+1,2»), млрд ₽ подразумевается заголовком.
function fmtFlowAxis(v: number): string {
    if (v === 0) return '0';
    const abs = Math.abs(v);
    const s = abs >= 0.1 ? v.toFixed(1) : v.toFixed(2);
    return `${v > 0 ? '+' : ''}${s}`;
}

function fmtFlowVal(v: number): string {
    const s = Math.abs(v) >= 0.01 ? v.toFixed(2) : v.toFixed(3);
    return `${v > 0 ? '+' : ''}${s} млрд ₽`;
}

export default function FlowsHistogram({
    flowsData,
    noFundsSelected = false,
    loading,
    flowTitle = 'Чистые притоки и оттоки (млрд ₽)',
    indexData,
    indexLabel,
    showIndex = true,
    height = 450,
    animTrigger,
}: FlowsHistogramProps) {
    const isMobile = useIsMobile();
    // Обёртка обеих панелей — общая система координат для курсора и тултипа.
    const wrapRef = useRef<HTMLDivElement>(null);
    const priceSvgRef = useRef<SVGSVGElement>(null);
    const flowSvgRef = useRef<SVGSVGElement>(null);

    const flows = useMemo(() => flowsData?.flows ?? [], [flowsData?.flows]);
    const hasData = flows.length > 0;

    // ── Закрытия индекса, разложенные по слотам периодов потоков. ──
    // Слот i покрывает [period_start_i, period_end_i]; ISO-строки сравниваются
    // лексикографически. Точки вне диапазона потоков отбрасываются.
    const idxBySlot = useMemo(() => {
        const bySlot: number[][] = flows.map(() => []);
        if (!indexData?.length || !flows.length) return bySlot;
        let si = 0;
        for (const p of indexData) {
            const v = p.close;
            if (v == null || !(v > 0)) continue;
            while (si < flows.length && flows[si].period_end < p.date) si++;
            if (si >= flows.length) break;
            if (p.date >= flows[si].period_start) bySlot[si].push(v);
        }
        return bySlot;
    }, [flows, indexData]);

    const hasPrice = useMemo(
        () => showIndex && idxBySlot.some(a => a.length > 0),
        [showIndex, idxBySlot],
    );

    // ── Резерв панели индекса на время загрузки: при смене категории/периода
    // новые потоки приходят раньше/позже индекса — пока грузимся, держим место
    // панели, если у ПРЕДЫДУЩЕГО состояния она была (без вспышки-схлопывания).
    const prevHadPriceRef = useRef(false);
    useEffect(() => {
        if (!loading) prevHadPriceRef.current = hasPrice;
    }, [loading, hasPrice]);
    const showPricePanel = hasPrice || (loading && showIndex && prevHadPriceRef.current);

    // Высоты секций — формула CompanyShareChart: из бюджета вычитаем хром второй
    // секции и полосу X-подписей, остальное 2:1 в пользу бенчмарка.
    const CHROME_H = 45;
    const inner = Math.max(height - CHROME_H - XLABEL_H, 240);
    const topH = showPricePanel ? Math.round(inner * (2 / 3)) : 0;
    const botH = inner - topH;

    // ── Навигатор: [start, end] по индексам слотов. ──
    const [navRange, setNavRange] = useState<[number, number]>([0, 0]);
    useLayoutEffect(() => {
        if (flows.length > 0) setNavRange([0, flows.length - 1]);
    }, [flows.length, animTrigger]);

    // Превью навигатора: линия индекса (последнее закрытие слота, дыры тянем
    // предыдущим значением). Нет индекса → сами потоки.
    const navigatorData = useMemo(() => {
        let prev = 0;
        return flows.map((f, i) => {
            let v: number | null = null;
            if (hasPrice) {
                const arr = idxBySlot[i];
                if (arr && arr.length) v = arr[arr.length - 1];
            } else {
                v = f.flow;
            }
            if (v == null) v = prev;
            prev = v;
            return { time: f.period_end, value: v };
        });
    }, [flows, hasPrice, idxBySlot]);

    // ── Видимое окно ──
    const visStart = navRange[0];
    const visCount = Math.max(navRange[1] - navRange[0] + 1, 1);
    const visIdx = useMemo(
        () => Array.from({ length: visCount }, (_, i) => visStart + i).filter(i => i < flows.length),
        [visStart, visCount, flows.length],
    );

    // Шкала индекса видимого окна, поля 6%.
    const [priceLo, priceHi] = useMemo(() => {
        if (!hasPrice) return [0, 1];
        const vals: number[] = [];
        for (const i of visIdx) {
            const arr = idxBySlot[i];
            if (arr) for (const v of arr) vals.push(v);
        }
        if (!vals.length) return [0, 1];
        let lo = Math.min(...vals);
        let hi = Math.max(...vals);
        if (lo === hi) { lo *= 0.97; hi *= 1.03; }
        const pad = (hi - lo) * 0.06;
        return [lo - pad, hi + pad];
    }, [hasPrice, visIdx, idxBySlot]);

    // Шкала потоков: симметричная ±max видимого окна (биполярная гистограмма).
    const flowMax = useMemo(() => {
        let mx = 0;
        for (const i of visIdx) {
            const v = Math.abs(flows[i]?.flow ?? 0);
            if (v > mx) mx = v;
        }
        return mx || 0.001;
    }, [visIdx, flows]);

    // Координаты в СВОЁМ viewBox 0..1000 каждой панели.
    const slotX = (i: number, frac = 0.5) => ((i - visStart + frac) / visCount) * 1000;
    const priceY = (close: number) =>
        ((1 - (close - priceLo) / (priceHi - priceLo)) * 0.95) * 1000;
    // Середина 500, амплитуда 470 (запас 3% сверху/снизу — как было).
    const flowY = (v: number) => 500 - (v / flowMax) * 470;

    // Линия индекса: дневные точки видимых слотов, дробно внутри слота.
    const targetLinePts = useMemo(() => {
        if (!hasPrice) return [] as { x: number; y: number }[];
        const pts: { x: number; y: number }[] = [];
        for (const i of visIdx) {
            const arr = idxBySlot[i];
            if (!arr || !arr.length) continue;
            for (let k = 0; k < arr.length; k++) {
                pts.push({ x: slotX(i, (k + 0.5) / arr.length), y: priceY(arr[k]) });
            }
        }
        return pts;
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [hasPrice, visIdx, idxBySlot, priceLo, priceHi, visStart, visCount]);

    // ── Морф линии при смене данных — как в CompanyShareChart: из текущих
    // отображаемых точек в новые (morphPts ресемплирует). Драг — мгновенно.
    const [linePts, setLinePts] = useState<{ x: number; y: number }[]>([]);
    const linePtsRef = useRef<{ x: number; y: number }[]>([]);
    const lineRafRef = useRef<number | null>(null);
    const lineMorphActiveRef = useRef(false);
    const prevLineDataRef = useRef<unknown[] | null>(null);
    useLayoutEffect(() => {
        const dataKey = [flows, indexData];
        const prev = prevLineDataRef.current;
        const dataChanged = !prev || prev.some((v, i) => v !== dataKey[i]);
        prevLineDataRef.current = dataKey;
        const morphInFlight = lineMorphActiveRef.current;
        if (lineRafRef.current) cancelAnimationFrame(lineRafRef.current);
        lineRafRef.current = null;
        const from = linePtsRef.current;
        const needMorph = (dataChanged || morphInFlight)
            && from.length >= 2 && targetLinePts.length >= 2;
        if (!needMorph) {
            lineMorphActiveRef.current = false;
            linePtsRef.current = targetLinePts;
            setLinePts(targetLinePts);
            return;
        }
        lineMorphActiveRef.current = true;
        let start: number | null = null;
        const tick = (ts: number) => {
            if (start == null) start = ts;
            const t = Math.min((ts - start) / ANIMATION.morphDuration, 1);
            const pts = t >= 1 ? targetLinePts : morphPts(from, targetLinePts, easeOutCubic(t));
            linePtsRef.current = pts;
            setLinePts(pts);
            if (t < 1) {
                lineRafRef.current = requestAnimationFrame(tick);
            } else {
                lineRafRef.current = null;
                lineMorphActiveRef.current = false;
            }
        };
        lineRafRef.current = requestAnimationFrame(tick);
        return () => {
            if (lineRafRef.current) {
                cancelAnimationFrame(lineRafRef.current);
                lineRafRef.current = null;
            }
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [targetLinePts]);
    const linePath = useMemo(
        () => (linePts.length >= 2
            ? linePts.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(2)},${p.y.toFixed(2)}`).join(' ')
            : ''),
        [linePts],
    );

    // Тики индекса (4) и потоков (±max / ±max/2 / 0).
    const priceTicks = useMemo(
        () => (hasPrice
            ? Array.from({ length: 4 }, (_, i) => priceLo + ((3 - i) / 3) * (priceHi - priceLo))
            : []),
        [hasPrice, priceLo, priceHi],
    );
    const flowTicks = useMemo(
        () => [flowMax, flowMax / 2, 0, -flowMax / 2, -flowMax],
        [flowMax],
    );

    // ── Анимация баров — волна на первом рендере, морф дальше (в визуальном
    // пространстве: старые значения нормализуются со старой шкалы на новую).
    const [dispVals, setDispVals] = useState<number[]>([]);
    const dispRef = useRef<number[]>([]);
    const wavedRef = useRef(false);
    const rafRef = useRef<number | null>(null);
    const targetVals = useMemo(() => flows.map(f => f.flow), [flows]);
    // Песочница: волна появления выключена (см. chart/chartAnim.ts).
    const noAnim = useNoChartAnim();
    useLayoutEffect(() => {
        if (!hasData) {
            dispRef.current = [];
            setDispVals([]);
            return;
        }
        if (rafRef.current) cancelAnimationFrame(rafRef.current);
        const target = targetVals;
        const wave = !wavedRef.current || dispRef.current.length === 0;
        wavedRef.current = true;
        if (wave && noAnim) {
            dispRef.current = target;
            setDispVals(target);
            return;
        }
        const newMax = Math.max(...target.map(Math.abs), 0.001);
        const oldMax = Math.max(...dispRef.current.map(Math.abs), 0.001);
        const from = wave
            ? new Array<number>(target.length).fill(0)
            : resampleVals(dispRef.current, target.length).map(v => (v / oldMax) * newMax);
        // Стартовый кадр — до первого paint, чтобы не мигнуть старыми барами.
        dispRef.current = from;
        setDispVals(from);
        const totalDuration = wave ? ANIMATION.waveDuration : ANIMATION.morphDuration;
        const staggerDelay = wave ? ANIMATION.waveStagger : 0;
        let start: number | null = null;
        const tick = (ts: number) => {
            if (start == null) start = ts;
            const elapsed = ts - start;
            const next = target.map((v, i) => {
                const barDelay = (i / target.length) * staggerDelay;
                const barElapsed = Math.max(0, elapsed - barDelay);
                const t = Math.min(barElapsed / (totalDuration - staggerDelay), 1);
                return from[i] + (v - from[i]) * easeOutCubic(t);
            });
            dispRef.current = next;
            setDispVals(next);
            if (elapsed < totalDuration) rafRef.current = requestAnimationFrame(tick);
        };
        rafRef.current = requestAnimationFrame(tick);
        return () => {
            if (rafRef.current) cancelAnimationFrame(rafRef.current);
        };
    }, [targetVals, hasData, noAnim]);

    // ── Reveal линии индекса слева направо — один раз, когда линия готова.
    const revealW = useChartReveal(hasPrice && linePath !== '', 1000);

    // ── Hover: ближайший слот по X (общий на обе панели). ──
    const [hoveredI, setHoveredI] = useState<number | null>(null);
    const [tooltipPos, setTooltipPos] = useState<{ x: number; y: number } | null>(null);

    const handleMouseMove = (e: React.MouseEvent<HTMLDivElement>) => {
        const geomSvg = flowSvgRef.current;
        if (!visIdx.length || !wrapRef.current || !geomSvg) return;
        const wrapRect = wrapRef.current.getBoundingClientRect();
        const svgRect = geomSvg.getBoundingClientRect();
        const xInChart = e.clientX - svgRect.left;
        if (xInChart < 0 || xInChart > svgRect.width) return;
        const slotW = svgRect.width / visCount;
        const raw = visStart + Math.floor(xInChart / slotW);
        const i = Math.min(Math.max(raw, visStart), visStart + visIdx.length - 1);
        setHoveredI(i);
        const snapX = (svgRect.left - wrapRect.left) + (slotX(i) / 1000) * svgRect.width;
        setTooltipPos({ x: snapX, y: e.clientY - wrapRect.top });
    };
    const handleMouseLeave = () => {
        setHoveredI(null);
        setTooltipPos(null);
    };

    // ── Геометрия оверлеев (пилюля даты + тултип) — от верхней панели. ──
    const overlayGeom = (() => {
        const wrap = wrapRef.current;
        const topSvg = priceSvgRef.current ?? flowSvgRef.current;
        if (!wrap || !topSvg) return null;
        const wrapRect = wrap.getBoundingClientRect();
        const svgRect = topSvg.getBoundingClientRect();
        return {
            plotLeft: svgRect.left - wrapRect.left,
            plotWidth: svgRect.width,
            topLineY: svgRect.top - wrapRect.top,
        };
    })();

    // Индекс hovered слота — последнее закрытие слота (то же, что в превью).
    const hoverPrice = useMemo(() => {
        if (!hasPrice || hoveredI == null) return null;
        const arr = idxBySlot[hoveredI];
        if (!arr || !arr.length) return null;
        return arr[arr.length - 1];
    }, [hasPrice, hoveredI, idxBySlot]);

    // Последние значения видимого окна — статичные таблетки на правой оси.
    const lastVisPrice = useMemo(() => {
        if (!hasPrice) return null;
        for (let i = visIdx.length - 1; i >= 0; i--) {
            const arr = idxBySlot[visIdx[i]];
            if (arr && arr.length) return arr[arr.length - 1];
        }
        return null;
    }, [hasPrice, visIdx, idxBySlot]);

    const lastVisFlow = useMemo(() => {
        for (let i = visIdx.length - 1; i >= 0; i--) {
            const f = flows[visIdx[i]];
            if (f) return f.flow;
        }
        return null;
    }, [visIdx, flows]);

    // Ширина бара: 66% слота, но не шире 22 юнитов (узкое окно → не распухают).
    const barHalf = Math.min((0.33 / visCount) * 1000, 22);
    // Минимальная видимая высота ненулевого бара (юниты viewBox) — как прежние 1.2%.
    const MIN_BAR_H = 12;

    const padArea = {
        left: 'var(--chart-pad-left, 100px)',
        right: 'var(--chart-pad-right-single, 95px)',
    } as const;

    // Вертикальный курсор — нейтральный серый: у баров цвет несёт знак потока.
    const crosshair = (i: number) => (
        <line
            x1={slotX(i)}
            y1="0"
            x2={slotX(i)}
            y2="1000"
            stroke={CROSSHAIR.color}
            strokeWidth={CROSSHAIR.strokeWidth}
            vectorEffect="non-scaling-stroke"
            strokeDasharray={CROSSHAIR.dashArray}
            style={{ pointerEvents: 'none' }}
        />
    );

    // ─────────────────────────────────────────────────────────────────────
    return (
        <div
            className="rounded-2xl p-5 bg-theme-primary border border-theme relative"
            style={{ ['--chart-height' as string]: `${height}px` }}
        >
            {noFundsSelected ? (
                <div
                    className="flex flex-col items-center justify-center text-center"
                    style={{ height: 'calc(var(--chart-height, 450px) + 100px)', gap: 'var(--sp-3)', padding: 'var(--sp-6)' }}
                >
                    {/* Editorial-эмблема: accent-square с outline + hard-shadow. */}
                    <div
                        style={{
                            width: 56, height: 56, borderRadius: 12,
                            background: 'var(--accent)', border: '2px solid var(--text-primary)',
                            boxShadow: 'var(--shadow-hard-chip, 3px 3px 0 var(--text-primary))',
                            display: 'flex', alignItems: 'center', justifyContent: 'center',
                            marginBottom: 'var(--sp-2)',
                        }}
                    >
                        <BarChart3 size={28} strokeWidth={2.4} color="#FFFFFF" />
                    </div>
                    <div className="font-semibold text-theme-primary" style={{ fontSize: 'var(--fs-lg)' }}>
                        Не выбрано ни одного фонда
                    </div>
                    <div className="text-theme-secondary" style={{ fontSize: 'var(--fs-sm)', maxWidth: 360 }}>
                        Отметьте фонды в списке «Фонды категории» ниже, чтобы увидеть гистограмму притоков и оттоков.
                    </div>
                </div>
            ) : loading && !hasData ? (
                <div className="flex items-center justify-center" style={{ height: 'calc(var(--chart-height, 450px) + 100px)' }}>
                    <div className="flex flex-col items-center" style={{ gap: 'var(--sp-3)' }}>
                        <div className="w-8 h-8 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
                        <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-base)' }}>Загрузка...</span>
                    </div>
                </div>
            ) : !hasData ? (
                <div className="flex items-center justify-center text-center" style={{ height: 'calc(var(--chart-height, 450px) + 100px)', color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)' }}>
                    Нет данных за период
                </div>
            ) : (<>
                {loading && (
                    <div className="absolute top-4 left-4 z-20 flex items-center gap-2 rounded-lg border border-theme shadow-md" style={{ background: 'var(--bg-primary)', padding: 'var(--sp-2) var(--sp-3)' }}>
                        <div className="w-4 h-4 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
                        <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-xs)' }}>Обновление...</span>
                    </div>
                )}

                {/* Обёртка обеих секций — общий курсор, тултип и пилюля даты. */}
                <div
                    ref={wrapRef}
                    className="relative cursor-crosshair"
                    style={{ touchAction: 'none' }}
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
                    {/* ── Верхняя секция: бенчмарк категории. marginTop с вычетом
                        20px — компенсация паддинга карточки (p-5), как в
                        CompanyShareChart. */}
                    {showPricePanel && (
                        <div className="pb-1 border-b border-theme relative overflow-hidden" style={{ marginTop: 'calc(var(--chart-legend-top-gap, 8px) - 20px)' }}>
                            <div className="flex items-center justify-center relative z-10" style={{ marginBottom: 'var(--chart-legend-mb, 2px)' }}>
                                <ChartLegend
                                    items={[{ color: INDEX_LINE_COLOR, label: indexLabel || 'Индекс' }]}
                                    fontWeight={600}
                                    style={{ color: 'var(--text-primary)' }}
                                />
                            </div>
                            <div className="relative" style={{ height: topH }}>
                                <div className="absolute" style={{ top: 'var(--chart-pad-top, 14px)', bottom: 0, left: padArea.left, right: padArea.right }}>
                                    <svg ref={priceSvgRef} width="100%" height="100%" viewBox="0 0 1000 1000" preserveAspectRatio="none">
                                        <defs>
                                            <clipPath id="fundsFlowsIndexRevealClip">
                                                <rect x={0} y={0} width={Math.max(revealW ?? 1000, 0)} height={1000} />
                                            </clipPath>
                                        </defs>
                                        {priceTicks.map((p, i) => (
                                            <line key={`pg-${i}`} x1="0" y1={priceY(p)} x2="1000" y2={priceY(p)} stroke={GRID.major} strokeWidth="1" vectorEffect="non-scaling-stroke" />
                                        ))}
                                        {linePath && (
                                            <g clipPath="url(#fundsFlowsIndexRevealClip)">
                                                <path
                                                    d={linePath}
                                                    fill="none"
                                                    stroke={INDEX_LINE_COLOR}
                                                    strokeWidth="2"
                                                    vectorEffect="non-scaling-stroke"
                                                    strokeLinecap="round"
                                                    strokeLinejoin="round"
                                                />
                                            </g>
                                        )}
                                        {hoveredI !== null && crosshair(hoveredI)}
                                    </svg>
                                </div>
                                {/* Ось Y индекса — справа. */}
                                <div className="absolute pointer-events-none" style={{ top: 'var(--chart-pad-top, 14px)', bottom: 0, right: 0, width: padArea.right }}>
                                    {priceTicks.map((p, i) => (
                                        <div key={`pl-${i}`} className="absolute" style={{ top: `${priceY(p) / 10}%`, left: 12, transform: 'translateY(-50%)' }}>
                                            <span className="font-semibold" style={{ fontSize: 'var(--chart-font-y, 16px)', color: 'var(--axis-color, #9CA3B8)' }}>
                                                {fmtPrice(p)}
                                            </span>
                                        </div>
                                    ))}
                                    {(() => {
                                        const v = lastVisPrice;
                                        if (v == null || !(v > 0)) return null;
                                        return (
                                            <ChartAxisPill
                                                value={fmtPrice(v)}
                                                color={INDEX_LINE_COLOR}
                                                topFrac={priceY(v) / 1000}
                                            />
                                        );
                                    })()}
                                </div>
                            </div>
                        </div>
                    )}

                    {/* ── Нижняя секция: биполярная гистограмма притоков/оттоков. ── */}
                    <div className="relative overflow-hidden" style={showPricePanel ? { paddingTop: 'var(--sp-2)' } : { marginTop: 'calc(var(--chart-legend-top-gap, 8px) - 20px)' }}>
                        <div className="flex items-center justify-center relative z-10" style={{ marginBottom: showPricePanel ? 'var(--sp-2)' : 'var(--chart-legend-mb, 2px)' }}>
                            <ChartLegend
                                items={[{ color: 'transparent', label: flowTitle, marker: 'none' }]}
                                fontWeight={600}
                                itemGap={6}
                                style={{ color: 'var(--text-primary)' }}
                            />
                        </div>
                        <div className="relative" style={{ height: botH + XLABEL_H }}>
                            <div className="absolute" style={{ top: 0, bottom: XLABEL_H, left: padArea.left, right: padArea.right }}>
                                <svg ref={flowSvgRef} width="100%" height="100%" viewBox="0 0 1000 1000" preserveAspectRatio="none">
                                    {flowTicks.map((v, i) => (
                                        <line key={`fg-${i}`} x1="0" y1={flowY(v)} x2="1000" y2={flowY(v)} stroke={v === 0 ? GRID.zero : GRID.major} strokeWidth="1" vectorEffect="non-scaling-stroke" />
                                    ))}
                                    {/* Бары: высоты из dispVals (анимированные значения). */}
                                    {visIdx.map((i) => {
                                        const v = dispVals[i] ?? 0;
                                        if (v === 0) return null;
                                        const x = slotX(i);
                                        const h = Math.max(Math.abs(v) / flowMax * 470, MIN_BAR_H);
                                        const dim = hoveredI !== null && hoveredI !== i;
                                        return (
                                            <rect
                                                key={i}
                                                x={x - barHalf}
                                                y={v > 0 ? 500 - h : 500}
                                                width={barHalf * 2}
                                                height={h}
                                                fill={v > 0 ? FLOW_POS : FLOW_NEG}
                                                opacity={dim ? 0.45 : 0.92}
                                            />
                                        );
                                    })}
                                    {hoveredI !== null && crosshair(hoveredI)}
                                </svg>
                            </div>
                            {/* Ось Y потоков — справа. */}
                            <div className="absolute pointer-events-none" style={{ top: 0, bottom: XLABEL_H, right: 0, width: padArea.right }}>
                                {flowTicks.map((v, i) => (
                                    <div key={`fl-${i}`} className="absolute" style={{ top: `${flowY(v) / 10}%`, left: 12, transform: 'translateY(-50%)' }}>
                                        <span className="font-semibold" style={{ fontSize: 'var(--chart-font-y, 16px)', color: 'var(--axis-color, #9CA3B8)' }}>
                                            {fmtFlowAxis(v)}
                                        </span>
                                    </div>
                                ))}
                                {(() => {
                                    const v = lastVisFlow;
                                    if (v == null) return null;
                                    return (
                                        <ChartAxisPill
                                            value={fmtFlowAxis(v)}
                                            color={v >= 0 ? FLOW_POS : FLOW_NEG}
                                            topFrac={flowY(v) / 1000}
                                        />
                                    );
                                })()}
                            </div>
                            {/* Подписи оси X — по видимым слотам, DD.MM.YY. */}
                            <div className="absolute flex justify-between font-semibold px-2" style={{ bottom: 8, left: padArea.left, right: padArea.right, fontSize: 'var(--chart-font-x, 14px)', color: 'var(--axis-color, #9CA3B8)' }}>
                                {(() => {
                                    if (!visIdx.length) return null;
                                    const tickCount = Math.min(isMobile ? 3 : 6, visIdx.length);
                                    return Array.from({ length: tickCount }, (_, i) => {
                                        const idx = Math.min(Math.round(i * (visIdx.length - 1) / Math.max(tickCount - 1, 1)), visIdx.length - 1);
                                        const f = flows[visIdx[idx]];
                                        if (!f) return null;
                                        const date = new Date(f.period_end);
                                        return (
                                            <span key={i}>{date.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' })}</span>
                                        );
                                    });
                                })()}
                            </div>
                            <ChartWatermark
                                left={`calc(${padArea.left} + 5px)`}
                                bottom={`${XLABEL_H + 5}px`}
                            />
                        </div>
                    </div>

                    {/* Тултип: бенчмарк + приток/отток + изменение %. */}
                    {hoveredI !== null && tooltipPos && flows[hoveredI] && (() => {
                        const f = flows[hoveredI];
                        const color = f.flow >= 0 ? FLOW_POS : FLOW_NEG;
                        return (
                            <ChartTooltip
                                x={tooltipPos.x}
                                y={tooltipPos.y}
                                clampTop={overlayGeom?.topLineY ?? cssVar('--chart-pad-top', 14)}
                                clampBottom={XLABEL_H}
                                cardStyle={{ padding: 'var(--sp-2)' }}
                            >
                                {hoverPrice != null && (
                                    <div style={{ marginBottom: 'var(--sp-1)', paddingBottom: 'var(--sp-1)', borderBottom: '1px solid var(--border-color)' }}>
                                        <TooltipRow
                                            color={INDEX_LINE_COLOR}
                                            label={indexLabel || 'Индекс'}
                                            value={fmtPrice(hoverPrice)}
                                            labelClass="font-bold"
                                            labelColor="var(--text-primary)"
                                            valueColor="var(--text-primary)"
                                        />
                                    </div>
                                )}
                                <TooltipRow color={color} label={f.flow >= 0 ? 'Приток' : 'Отток'} value={fmtFlowVal(f.flow)} />
                            </ChartTooltip>
                        );
                    })()}

                    {/* Плавающая пилюля даты — hovered слот, над верхней панелью. */}
                    {hoveredI !== null && overlayGeom && flows[hoveredI] && (
                        <ChartDatePill
                            date={new Date(flows[hoveredI].period_end).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', year: 'numeric' })}
                            x={overlayGeom.plotLeft + (slotX(hoveredI) / 1000) * overlayGeom.plotWidth}
                            topLineY={overlayGeom.topLineY}
                            minX={overlayGeom.plotLeft}
                            maxX={overlayGeom.plotLeft + overlayGeom.plotWidth}
                        />
                    )}
                </div>

                {/* Навигатор: превью — линия индекса (или потоков, если индекса нет). */}
                {navigatorData.length > 1 && (
                    <div data-export-ignore="true">
                        <ChartNavigator
                            data={navigatorData}
                            color="var(--accent)"
                            previewMode="line"
                            onChange={(s, e) => setNavRange([s, e])}
                            insetLeft="var(--chart-pad-left)"
                            insetRight="var(--chart-pad-right-single)"
                        />
                    </div>
                )}
            </>)}
        </div>
    );
}
