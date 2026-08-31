/**
 * CompanyFlowsHistogram — гистограмма «Потоки по компании», оформленная
 * 1-в-1 как funds/FlowsHistogram («Деньги в фондах»): одна серия чистого
 * потока (зелёный приток / красный отток) от нулевой линии, навигатор-брашер,
 * watermark, плавающая пилюля даты, каскадная волна баров.
 *
 * Отличие от FlowsHistogram: входные данные многосерийные (поток по каждому
 * фонду). Бары рисуют ЧИСТЫЙ поток (сумму по выбранным фондам), а тултип
 * дополнительно раскрывает разбивку — какой фонд внёс наибольший вклад в
 * движение этого месяца. Компонент самодостаточен (сам держит анимацию,
 * навигатор, hover/tooltip), поэтому вызывающему достаточно передать данные.
 *
 * Значения приходят в ₽ → внутри переводим в млн (÷1e6): потоки по компании
 * мельче, чем в «Деньги в фондах», поэтому отдельная единица — только здесь.
 */
import {
    useLayoutEffect,
    useMemo,
    useRef,
    useState,
} from 'react';
import { BarChart3 } from 'lucide-react';
import { GRID, CROSSHAIR, ANIMATION, cssVar } from '../../config/chartTheme';
import { resampleVals } from '../../utils/chartAnimation';
import { useNoChartAnim } from '../chart/chartAnim';
import { useIsMobile } from '../../hooks/useIsMobile';
import ChartWatermark from '../ChartWatermark';
import ChartNavigator from '../ChartNavigator';
import ChartLegend from '../chart/ChartLegend';
import { ChartTooltip, TooltipRow, ChartDatePill } from '../chart';
import { computeChartTopLineY } from '../chart/datePillLayout';

const easeOutCubic = ANIMATION.easing;

// Порог «заметного» месяца для обрезки пустого левого хвоста истории: первая
// покупка или продажа крупнее 1 млн ₽ (netMln уже в млн). Всё, что левее —
// копеечные/нулевые потоки (фонды ещё не держали бумагу) — прячем.
const MIN_VISIBLE_FLOW_MLN = 1;

export interface CompanyFlowsSeries {
    label: string;
    color: string;
    /** ₽ по месяцам, выровнено с `months` по индексу. */
    values: (number | null)[];
}

interface CompanyFlowsHistogramProps {
    /** "YYYY-MM" — ось X. */
    months: string[];
    /** Серии по фондам (для баров суммируются, для тултипа — разбивка). ₽. */
    series: CompanyFlowsSeries[];
    /** Заголовок графика (рендерится тем же ChartLegend, что и в макете). */
    title?: string;
    /** Высота области графика, px. */
    height?: number;
    loading?: boolean;
    /** Все фонды сняты пользователем — empty-state вместо пустой гистограммы. */
    noFundsSelected?: boolean;
    /** Смена бумаги / набора фондов: сброс навигатора + морф баров
     *  (волна с нуля играет только на первом рендере с данными). */
    animTrigger?: string;
    /** Подписи чистого потока в тултипе. Дефолт — «Чистая покупка/продажа»
     *  (потоки по компании); карточка фонда передаёт «Приток/Отток». */
    tooltipLabels?: { pos: string; neg: string };
}

// Число потока в млн без знака/единиц: целое с разделителем тысяч при ≥10 млн,
// иначе 1 знак после запятой — мелкие потоки (<10 млн) не схлопываются в «0».
function fmtMlnNumber(abs: number): string {
    return abs >= 10 ? Math.round(abs).toLocaleString('ru-RU') : abs.toFixed(1);
}

// Формат значения потока — млн ₽ (раньше млрд). Типичные месячные потоки тут
// десятки–сотни млн: в млрд это «0.07» и плохо читается. Знак + / −.
function fmtFlow(v: number): string {
    const sign = v > 0 ? '+' : v < 0 ? '−' : '';
    return `${sign}${fmtMlnNumber(Math.abs(v))} млн ₽`;
}

// Подпись оси Y — короткая (без «млн ₽»), как в макете.
function fmtAxis(v: number): string {
    if (v === 0) return '0';
    const sign = v > 0 ? '+' : '−';
    return `${sign}${fmtMlnNumber(Math.abs(v))}`;
}

// "YYYY-MM" → Date (первое число месяца). Локальное время — для подписей дат.
function monthToDate(m: string): Date {
    return new Date(`${m}-01T00:00:00`);
}

export default function CompanyFlowsHistogram({
    months: monthsAll,
    series: seriesAll,
    title = 'Чистые покупки и продажи (млн ₽)',
    height = 420,
    loading = false,
    noFundsSelected = false,
    animTrigger,
    tooltipLabels,
}: CompanyFlowsHistogramProps) {
    const isMobile = useIsMobile();
    const containerRef = useRef<HTMLDivElement>(null);
    const svgRef = useRef<SVGSVGElement>(null);

    // ── Обрезка пустого левого хвоста истории ──────────────────────────────
    // Слева гистограмма часто зияет: месяцами тянутся копеечные/нулевые потоки
    // (фонды ещё не держали бумагу). Ищем первый месяц, где ЧИСТЫЙ поток (сумма
    // по выбранным фондам) даёт заметный бар — первую покупку/продажу крупнее
    // 1 млн ₽ — и режем всё до него. Заметного месяца нет вовсе → не режем.
    // Считаем по seriesAll напрямую (netMln ниже строится уже от обрезки).
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

    // Видимые данные — с обрезанным левым хвостом. Всё ниже (netMln, навигатор,
    // анимация, сетка, подписи осей) строится уже от этих обрезанных массивов.
    const months = useMemo(() => monthsAll.slice(trimStart), [monthsAll, trimStart]);
    const series = useMemo<CompanyFlowsSeries[]>(
        () => seriesAll.map(s => ({ ...s, values: s.values.slice(trimStart) })),
        [seriesAll, trimStart],
    );

    // ── Чистый поток по месяцам (млн ₽): сумма по всем переданным сериям. ──
    const netMln = useMemo(() => {
        const n = months.length;
        const out = new Array<number>(n).fill(0);
        for (const s of series) {
            for (let i = 0; i < n; i++) {
                const v = s.values[i];
                if (v == null || Number.isNaN(v)) continue;
                out[i] += v / 1e6;
            }
        }
        return out;
    }, [months, series]);

    // Есть ли что рисовать — по НАЛИЧИЮ данных (как макет, который смотрит на
    // flows.length), а не по «есть ненулевой нетто». Иначе месяц, где покупки
    // одних фондов гасят продажи других (нетто = 0), ошибочно даёт «нет данных».
    const hasData = useMemo(
        () => months.length > 0 && series.some(s => s.values.some(v => v != null && !Number.isNaN(v))),
        [months, series],
    );

    // ── Навигатор: [start, end] видимого диапазона. ──
    const [navRange, setNavRange] = useState<[number, number]>([0, 0]);
    useLayoutEffect(() => {
        if (months.length > 0) setNavRange([0, months.length - 1]);
    }, [months.length, animTrigger]);

    // ── Анимация баров — схема как в OI (SimpleChart): каскадная волна
    // grow-from-zero только на ПЕРВОМ рендере с данными, дальше любые смены
    // данных (бумага, набор фондов) морфят из текущих отображаемых значений,
    // ресемплированных к новой длине. ──
    const [animated, setAnimated] = useState<number[]>([]);
    const dispRef = useRef<number[]>([]);
    const wavedRef = useRef(false);
    const rafRef = useRef<number | null>(null);
    // Песочница: волна появления выключена (см. chart/chartAnim.ts).
    const noAnim = useNoChartAnim();
    // useLayoutEffect + синхронный сет стартового кадра: с useEffect между
    // приходом данных и первым rAF-кадром успевал отрисоваться кадр со
    // старыми значениями на новой шкале — вспышка гигантских баров.
    useLayoutEffect(() => {
        if (!netMln.length) {
            setAnimated([]);
            dispRef.current = [];
            return;
        }
        if (rafRef.current) cancelAnimationFrame(rafRef.current);
        const target = netMln;
        const wave = !wavedRef.current || dispRef.current.length === 0;
        wavedRef.current = true;
        if (wave && noAnim) {
            dispRef.current = target;
            setAnimated(target);
            return;
        }
        // Морф в визуальном пространстве: стартовые значения нормализуются со
        // старой шкалы на новую (v / oldMax * newMax) — иначе при смене бумаги
        // с крупными потоками на мелкую бары стартовали бы выше холста.
        const newMax = Math.max(...target.map(Math.abs), 0.001);
        const oldMax = Math.max(...dispRef.current.map(Math.abs), 0.001);
        const from = wave
            ? new Array<number>(target.length).fill(0)
            : resampleVals(dispRef.current, target.length).map(v => (v / oldMax) * newMax);
        // Стартовый кадр — до первого paint, чтобы не мигнуть старыми барами.
        dispRef.current = from;
        setAnimated(from);
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
            setAnimated(next);
            if (elapsed < totalDuration) rafRef.current = requestAnimationFrame(tick);
        };
        rafRef.current = requestAnimationFrame(tick);
        return () => {
            if (rafRef.current) cancelAnimationFrame(rafRef.current);
        };
        // netMln (стабилен через useMemo) + animTrigger — старт морфа/волны.
    }, [netMln, animTrigger, noAnim]);

    // ── Hover ──
    const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);
    const [tooltipPos, setTooltipPos] = useState<{ x: number; y: number } | null>(null);

    const navigatorData = useMemo(
        () => months.map((m, i) => ({ time: m, value: netMln[i] ?? 0 })),
        [months, netMln],
    );

    const handleMouseMove = (e: React.MouseEvent<HTMLDivElement>) => {
        if (!months.length || !containerRef.current || !svgRef.current) return;
        const containerRect = containerRef.current.getBoundingClientRect();
        const svgRect = svgRef.current.getBoundingClientRect();
        const xInChart = e.clientX - svgRect.left;
        if (xInChart < 0 || xInChart > svgRect.width) return;
        const visibleCount = navRange[1] - navRange[0] + 1;
        const barWidth = svgRect.width / visibleCount;
        const idx = Math.floor(xInChart / barWidth);
        if (idx >= 0 && idx < visibleCount) {
            setHoveredIndex(idx);
            const slotCenter = (svgRect.left - containerRect.left) + idx * barWidth + barWidth / 2;
            setTooltipPos({ x: slotCenter, y: e.clientY - containerRect.top });
        }
    };
    const handleMouseLeave = () => {
        setHoveredIndex(null);
        setTooltipPos(null);
    };

    // ── Разбивка по фондам для hovered месяца (вклад в чистый поток). ──
    const hoverBreakdown = useMemo(() => {
        if (hoveredIndex == null) return null;
        const absIdx = navRange[0] + hoveredIndex;
        const rows = series
            .map(s => {
                const raw = s.values[absIdx];
                return raw == null || Number.isNaN(raw)
                    ? null
                    : { label: s.label, color: s.color, mln: raw / 1e6 };
            })
            .filter((r): r is { label: string; color: string; mln: number } => r != null && r.mln !== 0)
            .sort((a, b) => Math.abs(b.mln) - Math.abs(a.mln));
        const net = rows.reduce((acc, r) => acc + r.mln, 0);
        return { rows, net };
    }, [hoveredIndex, navRange, series]);

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
                        <BarChart3 size={28} strokeWidth={2.4} color="#FFFFFF" />
                    </div>
                    <div className="font-semibold text-theme-primary" style={{ fontSize: 'var(--fs-lg)' }}>
                        Не выбрано ни одного фонда
                    </div>
                    <div className="text-theme-secondary" style={{ fontSize: 'var(--fs-sm)', maxWidth: 360 }}>
                        Отметьте фонды в фильтре выше, чтобы увидеть чистый поток по бумаге.
                    </div>
                </div>
            ) : loading && !hasData && animated.length === 0 ? (
                <div className="flex items-center justify-center" style={{ height: 'calc(var(--chart-height, 420px) + 100px)' }}>
                    <div className="flex flex-col items-center" style={{ gap: 'var(--sp-3)' }}>
                        <div className="w-8 h-8 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
                        <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-base)' }}>Загрузка...</span>
                    </div>
                </div>
            ) : !hasData ? (
                <div className="flex items-center justify-center text-center" style={{ height: 'calc(var(--chart-height, 420px) + 100px)', color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)' }}>
                    Нет данных по потокам за период
                </div>
            ) : (<>
                {loading && (
                    <div className="absolute top-4 left-4 z-20 flex items-center gap-2 rounded-lg border border-theme shadow-md" style={{ background: 'var(--bg-primary)', padding: 'var(--sp-2) var(--sp-3)' }}>
                        <div className="w-4 h-4 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
                        <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-xs)' }}>Обновление...</span>
                    </div>
                )}

                <div>
                    {/* Заголовок графика — тем же ChartLegend (SVG-text), что и в макете.
                        Negative margin-top компенсирует p-5 (20px) карточки — легенда
                        на --chart-legend-top-gap от верхней границы, как в SimpleChart. */}
                    <div style={{ marginTop: 'calc(var(--chart-legend-top-gap, 8px) - 20px)', marginBottom: 'var(--chart-legend-mb, 16px)' }}>
                        <ChartLegend
                            items={[{ color: 'transparent', label: title, marker: 'none' }]}
                            fontWeight={600}
                            itemGap={6}
                            gap="clamp(6px, 1vw, 16px)"
                            style={{ color: 'var(--text-primary)' }}
                        />
                    </div>

                    {/* График с тултипом */}
                    {/* Без chart-reveal на обёртке: бары и так растут «волной» слева
                        направо, а обёртка уводила вместе с ними оси и даты —
                        обрамление видно с первого кадра. */}
                    <div
                        ref={containerRef}
                        className="relative cursor-crosshair"
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
                        {/* Область графика — отступы из тех же CSS-переменных, что и макет. */}
                        <div className="absolute" style={{ top: 'var(--chart-pad-top, 19px)', bottom: 'var(--chart-pad-bottom, 50px)', left: 'var(--chart-pad-left, 100px)', right: 'var(--chart-pad-right-single, 95px)' }}>
                            <svg ref={svgRef} width="100%" height="100%" preserveAspectRatio="none">
                                {animated.length > 0 && (() => {
                                    const visible = animated.slice(navRange[0], navRange[1] + 1);
                                    const visibleTarget = netMln.slice(navRange[0], navRange[1] + 1);
                                    const maxScale = Math.max(...visibleTarget.map(v => Math.abs(v)), 0.001);
                                    const barWidth = 100 / (visible.length || 1);
                                    const midY = 50;
                                    const halfH = 47;
                                    const minBarH = 1.2;
                                    const showOutline = visible.length <= 50;
                                    const outlineWidth = visible.length <= 20 ? 1 : 0.7;
                                    const strokeProps: React.SVGProps<SVGRectElement> = showOutline
                                        ? { stroke: 'var(--bar-outline)', strokeWidth: outlineWidth, vectorEffect: 'non-scaling-stroke' }
                                        : {};
                                    return visible.map((val, i) => {
                                        const isHovered = hoveredIndex === i;
                                        const opacity = hoveredIndex === null ? 1 : isHovered ? 1 : 0.35;
                                        const x = `${i * barWidth + barWidth * 0.15}%`;
                                        const w = `${barWidth * 0.7}%`;
                                        const hIn = val > 0 ? Math.max((val / maxScale) * halfH, minBarH) : 0;
                                        const hOut = val < 0 ? Math.max((Math.abs(val) / maxScale) * halfH, minBarH) : 0;
                                        return (
                                            <g key={i} opacity={opacity}>
                                                {hIn > 0 && (
                                                    <rect x={x} y={`${midY - hIn}%`} width={w} height={`${hIn}%`} fill={'var(--funds-flow-positive)'} {...strokeProps} rx="2" />
                                                )}
                                                {hOut > 0 && (
                                                    <rect x={x} y={`${midY}%`} width={w} height={`${hOut}%`} fill={'var(--funds-flow-negative)'} {...strokeProps} rx="2" />
                                                )}
                                            </g>
                                        );
                                    });
                                })()}

                                {/* Горизонтальная сетка + нулевая линия */}
                                {(() => {
                                    const visibleTarget = netMln.slice(navRange[0], navRange[1] + 1);
                                    const maxAbs = Math.max(...visibleTarget.map(v => Math.abs(v)), 0.001);
                                    const ticks = [-maxAbs, -maxAbs / 2, 0, maxAbs / 2, maxAbs];
                                    return ticks.map((val, i) => {
                                        const yPct = 50 - (val / maxAbs) * 47;
                                        return (
                                            <line key={`grid-${i}`} x1="0" y1={`${yPct}%`} x2="100%" y2={`${yPct}%`} stroke={val === 0 ? GRID.zero : GRID.major} strokeWidth="1" />
                                        );
                                    });
                                })()}

                                {/* Вертикальный курсор */}
                                {hoveredIndex !== null && (() => {
                                    const visibleCount = navRange[1] - navRange[0] + 1;
                                    const barWidth = 100 / visibleCount;
                                    const cx = hoveredIndex * barWidth + barWidth / 2;
                                    return (
                                        <line x1={`${cx}%`} y1="3%" x2={`${cx}%`} y2="97%" stroke={CROSSHAIR.accentColor} strokeWidth="1" strokeDasharray={CROSSHAIR.accentDashArray} opacity={CROSSHAIR.accentOpacity} style={{ pointerEvents: 'none' }} />
                                    );
                                })()}
                            </svg>
                        </div>

                        {/* Watermark — привязан к нижней gridline (3% запас), как в макете. */}
                        <ChartWatermark
                            left="calc(var(--chart-pad-left, 100px) + 5px)"
                            bottom="calc(var(--chart-pad-bottom, 50px) + 0.03 * (var(--chart-height, 420px) - var(--chart-pad-top, 19px) - var(--chart-pad-bottom, 50px)) + 5px)"
                        />

                        {/* Тултип: чистый поток + разбивка по фондам (вклад в движение). */}
                        {hoveredIndex !== null && tooltipPos && hoverBreakdown && (() => {
                            const net = hoverBreakdown.net;
                            const netColor = net >= 0 ? 'var(--funds-flow-positive)' : 'var(--funds-flow-negative)';
                            const MAX_ROWS = 6;
                            const shown = hoverBreakdown.rows.slice(0, MAX_ROWS);
                            const extra = hoverBreakdown.rows.length - shown.length;
                            return (
                                <ChartTooltip x={tooltipPos.x} y={tooltipPos.y} clampTop={cssVar('--chart-pad-top', 14)} clampBottom={cssVar('--chart-pad-bottom', 50)}>
                                    {/* Без точек: единственное цветное число — чистый поток
                                        (зелёный приток / красный отток). Итог — жирный тёмный
                                        лейбл; разбивка по фондам идёт тем же серым, что цифры
                                        оси Y справа от графика, чтобы не спорить с итогом. */}
                                    <TooltipRow
                                        hideDot
                                        color={netColor}
                                        label={net >= 0 ? (tooltipLabels?.pos ?? 'Чистая покупка') : (tooltipLabels?.neg ?? 'Чистая продажа')}
                                        value={fmtFlow(net)}
                                        labelClass="font-bold"
                                        labelColor="var(--text-primary)"
                                    />
                                    {/* Разбивка по сериям — только когда серий > 1: для
                                        единственной серии (карточка фонда) она дублировала бы
                                        итоговое число строкой ниже. */}
                                    {series.length > 1 && shown.length > 0 && (
                                        <div style={{ marginTop: 'var(--sp-1)', paddingTop: 'var(--sp-1)', borderTop: '1px solid var(--border-color)' }}>
                                            {/* Имя фонда — тем же начертанием и цветом, что и
                                                число справа (font-semibold + серый оси Y):
                                                строка читается как одно целое. */}
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

                        {/* Подписи значений справа (ось Y) */}
                        {(() => {
                            const visibleTarget = netMln.slice(navRange[0], navRange[1] + 1);
                            const maxAbs = Math.max(...visibleTarget.map(v => Math.abs(v)), 0.001);
                            const ticks = [maxAbs, maxAbs / 2, 0, -maxAbs / 2, -maxAbs];
                            return (
                                <div className="absolute pointer-events-none" style={{ top: 'var(--chart-pad-top, 19px)', bottom: 'var(--chart-pad-bottom, 50px)', right: 0, width: 'var(--chart-pad-right-single, 95px)' }}>
                                    {ticks.map((val, i) => {
                                        const yPct = 50 - (val / maxAbs) * 47;
                                        return (
                                            <div key={`label-${i}`} className="absolute" style={{ top: `${yPct}%`, left: 12, transform: 'translateY(-50%)' }}>
                                                <span className="font-semibold" style={{ fontSize: 'var(--chart-font-y, 16px)', color: 'var(--axis-color, #9CA3B8)' }}>
                                                    {fmtAxis(val)}
                                                </span>
                                            </div>
                                        );
                                    })}
                                </div>
                            );
                        })()}

                        {/* Подписи оси X — месяц/год (прорежены, на мобиле 3 шт.) */}
                        <div className="absolute flex justify-between font-semibold px-2" style={{ bottom: 'var(--chart-xlabel-bottom, 20px)', left: 'var(--chart-pad-left, 100px)', right: 'var(--chart-pad-right-single, 95px)', fontSize: 'var(--chart-font-x, 14px)', color: 'var(--axis-color, #9CA3B8)' }}>
                            {(() => {
                                const visibleMonths = months.slice(navRange[0], navRange[1] + 1);
                                if (!visibleMonths.length) return null;
                                const tickCount = Math.min(isMobile ? 3 : 6, visibleMonths.length);
                                return Array.from({ length: tickCount }, (_, i) => {
                                    const idx = Math.min(Math.round(i * (visibleMonths.length - 1) / Math.max(tickCount - 1, 1)), visibleMonths.length - 1);
                                    if (!visibleMonths[idx]) return null;
                                    return (
                                        <span key={i}>{monthToDate(visibleMonths[idx]).toLocaleDateString('ru-RU', { month: 'short', year: '2-digit' })}</span>
                                    );
                                });
                            })()}
                        </div>
                    </div>

                    {/* Навигатор — тот же ChartNavigator (rail-таймлайн, как во всех графиках). */}
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
                </div>

                {/* Плавающая пилюля даты — на конце пунктирной линии курсора. */}
                {hoveredIndex !== null && (() => {
                    const visibleMonths = months.slice(navRange[0], navRange[1] + 1);
                    const m = visibleMonths[hoveredIndex];
                    if (!m) return null;
                    const cont = containerRef.current;
                    const svg = svgRef.current;
                    if (!cont || !svg) return null;
                    const visibleCount = navRange[1] - navRange[0] + 1;
                    const containerRect = cont.getBoundingClientRect();
                    const svgRect = svg.getBoundingClientRect();
                    const padTop = svgRect.top - containerRect.top;
                    const chartW = svgRect.width;
                    const chartAreaH = svgRect.height;
                    const barWidth = chartW / visibleCount;
                    const centerX = cont.offsetLeft + (svgRect.left - containerRect.left) + hoveredIndex * barWidth + barWidth / 2;
                    const topLineY = computeChartTopLineY({
                        wrapper: cont,
                        paddingTop: padTop,
                        gridOffsetFrac: 0.03,
                        chartAreaHeight: chartAreaH,
                    });
                    const dateStr = monthToDate(m).toLocaleDateString('ru-RU', { month: 'long', year: 'numeric' });
                    const plotLeft = cont.offsetLeft + (svgRect.left - containerRect.left);
                    return (
                        <ChartDatePill
                            date={dateStr}
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
