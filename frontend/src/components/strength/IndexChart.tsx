import { useEffect, useLayoutEffect, useState, useMemo, useRef } from 'react';
import { easeOutCubic, morphPts, ptsToPath, ptsToArea, type SyncedDataPoint, type ChartPadding } from './chartUtils';
import { CHART_COLORS, GRID, CROSSHAIR, ANIMATION } from '../../config/chartTheme';
import { useIsMobile } from '../../hooks/useIsMobile';
import { useViewportWidth } from '../../hooks/useViewportWidth';
import { axisFontSize, xAxisTickCount } from '../chart/chartTypography';
import { measureText } from '../chart/measureText';
import { useChartReveal } from '../chart/useChartReveal';
import ChartWatermark from '../ChartWatermark';

interface IndexChartProps {
    syncedData: SyncedDataPoint[];
    hoverIndex: number | null;
    height: number;
    padding: ChartPadding;
    isNavDragRef?: { current: boolean };
}

export default function IndexChart({
    syncedData,
    hoverIndex,
    height,
    padding,
    isNavDragRef,
}: IndexChartProps) {
    const svgRef = useRef<SVGSVGElement>(null);
    const chartWrapRef = useRef<HTMLDivElement>(null);
    const containerRef = useRef<HTMLDivElement>(null);
    const [width, setWidth] = useState(0);
    // Меньше vertical gridlines на мобиле — синхронно с BreadthChart X-labels
    const isMobile = useIsMobile();
    const vw = useViewportWidth();
    const axisFs = axisFontSize(vw);
    const [animLinePath, setAnimLinePath] = useState('');
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [animAreaPath, setAnimAreaPath] = useState(''); void animAreaPath;
    const prevPtsRef = useRef<{ x: number; y: number }[]>([]);
    const currPtsRef = useRef<{ x: number; y: number }[]>([]);
    const animRef = useRef<number | null>(null);
    const isFirstRef = useRef(true);
    const prevWidthRef = useRef(0);
    // Reveal только линии на первом рендере: rAF-анимация ширины clip-rect
    // (useChartReveal, как в SimpleChart). Оси, сетка и пилюля вне клипа —
    // видны с первого кадра.
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
    // X-подписи у верхнего графика не рисуются (они под BreadthChart),
    // поэтому padding.bottom не резервируем — линия тянется до низа SVG.
    const chartHeight = height - padding.top;
    const revealW = useChartReveal(revealed, chartWidth);

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

        // Adaptive — синхронизирован с BreadthChart через ту же формулу
        const xTickCount = Math.min(xAxisTickCount(chartWidth, axisFs), syncedData.length);
        const xTicks = Array.from({ length: xTickCount }, (_, i) => {
            const idx = Math.floor((i / Math.max(xTickCount - 1, 1)) * (syncedData.length - 1));
            return { x: scaleX(idx) };
        });

        return { points, yTicks, xTicks, scaleX };
    }, [syncedData, chartWidth, chartHeight, padding, isMobile, axisFs]);

    // Morph animation
    useLayoutEffect(() => {
        if (!chartData) return;
        const target = chartData.points.map(p => ({ x: p.x, y: p.y }));
        const bottom = padding.top + chartHeight;

        if (animRef.current) cancelAnimationFrame(animRef.current);

        // Resize -- instant update without animation
        if (prevWidthRef.current !== 0 && prevWidthRef.current !== chartWidth) {
            prevWidthRef.current = chartWidth;
            prevPtsRef.current = target;
            currPtsRef.current = [];
            setAnimLinePath(ptsToPath(target));
            setAnimAreaPath(ptsToArea(target, bottom));
            return;
        }
        prevWidthRef.current = chartWidth;

        // During navigator drag -- instant update
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
            setAnimLinePath(ptsToPath(target));
            setAnimAreaPath(ptsToArea(target, bottom));
            // Запускаем reveal линии слева направо (rAF clip-rect, как в SimpleChart).
            if (!revealed) setRevealed(true);
            return;
        }

        // Start from current visual position (if animation was interrupted)
        const from = currPtsRef.current.length > 0 ? currPtsRef.current : prevPtsRef.current;
        let start: number | null = null;
        const animate = (ts: number) => {
            if (!start) start = ts;
            const t = easeOutCubic(Math.min((ts - start) / ANIMATION.morphDuration, 1));
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
            {/* chart-plot — маркер для computeChartTopLineY (StrengthPage ищет
                обёртку графика query-селектором; раньше это был .chart-reveal). */}
            <div ref={chartWrapRef} className="relative chart-plot">
                {width > 0 && chartData && (
                    <svg ref={svgRef} width={width} height={height} className="block" style={{ backgroundColor: 'var(--bg-primary)', contain: 'paint' }}>
                        <defs>
                            <linearGradient id="priceGradient" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="0%" stopColor={CHART_COLORS.primary} stopOpacity="0.3" />
                                <stop offset="100%" stopColor={CHART_COLORS.primary} stopOpacity="0" />
                            </linearGradient>
                            {/* Reveal-клип: ширина rect анимируется useChartReveal
                                (rAF), финал revealW=null — rect следует за resize. */}
                            <clipPath id="indexRevealClip">
                                <rect x={padding.left} y={0} width={Math.max(revealW ?? chartWidth, 0)} height={height} />
                            </clipPath>
                        </defs>

                        <g clipPath="url(#indexRevealClip)">
                            <path d={animLinePath} fill="none" stroke={CHART_COLORS.primary} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                        </g>

                        {crosshairX !== null && (
                            <line x1={crosshairX} y1={padding.top} x2={crosshairX} y2={padding.top + chartHeight}
                                stroke={CROSSHAIR.color} strokeWidth={CROSSHAIR.strokeWidth} strokeDasharray={CROSSHAIR.dashArray} />
                        )}
                        {crosshairX !== null && hoverIndex !== null && hoverIndex < chartData.points.length && (
                            <circle cx={chartData.points[hoverIndex].x} cy={chartData.points[hoverIndex].y}
                                r={5} fill={CHART_COLORS.primary} />
                        )}

                        {chartData.xTicks.map((tick, i) => (
                            i > 0 && i < chartData.xTicks.length - 1 ? (
                                <line key={`vg-${i}`}
                                    x1={tick.x} x2={tick.x}
                                    y1={padding.top} y2={padding.top + chartHeight}
                                    stroke={GRID.major} strokeWidth="1"
                                />
                            ) : null
                        ))}
                        {chartData.yTicks.map((tick, i) => (
                            <g key={i}>
                                <line
                                    x1={padding.left} x2={width - padding.right}
                                    y1={tick.y} y2={tick.y}
                                    stroke={GRID.major} strokeWidth="1"
                                />
                                <text
                                    x={width - padding.right + 12}
                                    y={Math.max(padding.top + 6, Math.min(tick.y, padding.top + chartHeight - 6))}
                                    textAnchor="start" dominantBaseline="middle" fill="var(--axis-color, #9CA3B8)" fontSize="var(--chart-font-y, 16)" fontWeight="600"
                                    paintOrder="stroke" stroke="var(--bg-primary)" strokeWidth="4" strokeLinejoin="round">
                                    {tick.value.toLocaleString('ru-RU', { maximumFractionDigits: 0 })}
                                </text>
                            </g>
                        ))}

                        {/* Current value pill — TV-style filled label на правой оси.
                            + dashed connector от последней точки до pill. */}
                        {(() => {
                            const lastP = chartData.points[chartData.points.length - 1];
                            if (!lastP) return null;
                            const value = lastP.value.toLocaleString('ru-RU', { maximumFractionDigits: 0 });
                            const fontY = axisFs;
                            const fontWeight = 700;
                            const padX = 8; // как в SimpleChart — единый размер заливки pill'а
                            const padY = 2;
                            const pillH = fontY + padY * 2;
                            const textW = measureText(value, fontY, fontWeight);
                            const pillW = Math.ceil(textW) + padX * 2 + 1;
                            // Pill выровнен с подписями оси (textAnchor=start у chartEnd+12),
                            // НО с клампом по обоим краям жёлоба:
                            //  1) не вылезать за правый край виджета — pill шире подписи оси на
                            //     2·padX, и на узком --strength-pad-right (55/45px) правый край
                            //     уезжал за width и обрезался (жалоба);
                            //  2) не наезжать на конец линии слева (зазор от графика).
                            const plotRight = width - padding.right;
                            let pillLeft = (plotRight + 12) - padX;
                            pillLeft = Math.min(pillLeft, width - 4 - pillW);
                            pillLeft = Math.max(pillLeft, plotRight + 2);
                            const textX = pillLeft + padX;
                            // Вертикальный кламп: линия тянется до самого низа SVG
                            // (padding.bottom здесь не резервируется), и на цене у
                            // минимума окна центр pill'а = низ SVG → нижняя половина
                            // обрезалась краем холста. Симметрично для максимума.
                            const pillY = Math.min(Math.max(lastP.y, pillH / 2 + 1), height - pillH / 2 - 1);
                            return (
                                <g pointerEvents="none">
                                    <rect
                                        x={pillLeft}
                                        y={pillY - pillH / 2}
                                        width={pillW}
                                        height={pillH}
                                        rx={4} ry={4}
                                        fill="#FF5C2B"
                                        style={{ fill: '#FF5C2B' }}
                                    />
                                    <text
                                        x={textX}
                                        y={pillY}
                                        textAnchor="start"
                                        dominantBaseline="central"
                                        fill="#FFFFFF"
                                        fontSize={fontY}
                                        fontWeight={fontWeight}
                                        style={{ fontVariantNumeric: 'tabular-nums' }}
                                    >
                                        {value}
                                    </text>
                                </g>
                            );
                        })()}
                    </svg>
                )}
                {/* Watermark — привязан к data area, как в SimpleChart.
                    Нижний отступ у графика убран (X-подписи под BreadthChart),
                    нижняя граница data area = низ SVG → bottom=5 даёт 5px зазор. */}
                <ChartWatermark left={padding.left + 5} bottom={5} />

                {/* Legacy HTML pill убран — новый SVG pill рисуется внутри
                    chart svg (см. "Current value pill" блок выше). HTML версия
                    с bg=bg-primary + border перекрывала SVG версию белым. */}
            </div>
        </div>
    );
}
