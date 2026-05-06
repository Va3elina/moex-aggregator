import { useEffect, useLayoutEffect, useState, useMemo, useRef } from 'react';
import { easeOutCubic, morphPts, ptsToPath, ptsToArea, type SyncedDataPoint, type ChartPadding } from './chartUtils';
import { CHART_COLORS, GRID, CROSSHAIR, ANIMATION } from '../../config/chartTheme';
import { useIsMobile } from '../../hooks/useIsMobile';
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
    const [animLinePath, setAnimLinePath] = useState('');
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [animAreaPath, setAnimAreaPath] = useState(''); void animAreaPath;
    const prevPtsRef = useRef<{ x: number; y: number }[]>([]);
    const currPtsRef = useRef<{ x: number; y: number }[]>([]);
    const animRef = useRef<number | null>(null);
    const isFirstRef = useRef(true);
    const prevWidthRef = useRef(0);
    // CSS-driven reveal на первом рендере (clip-path анимация слева направо).
    // Match SimpleChart pattern: state триггерит class .chart-reveal на wrapper.
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

        const xTickCount = Math.min(isMobile ? 4 : 7, syncedData.length);
        const xTicks = Array.from({ length: xTickCount }, (_, i) => {
            const idx = Math.floor((i / Math.max(xTickCount - 1, 1)) * (syncedData.length - 1));
            return { x: scaleX(idx) };
        });

        return { points, yTicks, xTicks, scaleX };
    }, [syncedData, chartWidth, chartHeight, padding, isMobile]);

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
            // Запускаем CSS-reveal слева направо (как в SimpleChart).
            // setState внутри useLayoutEffect → синхронный re-render до paint,
            // первый видимый кадр уже с class и clip-path в стартовом состоянии.
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
            <div ref={chartWrapRef} className={`relative ${revealed ? 'chart-reveal' : ''}`}>
                {width > 0 && chartData && (
                    <svg ref={svgRef} width={width} height={height} className="block" style={{ backgroundColor: 'var(--bg-primary)', contain: 'paint' }}>
                        <defs>
                            <linearGradient id="priceGradient" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="0%" stopColor={CHART_COLORS.primary} stopOpacity="0.3" />
                                <stop offset="100%" stopColor={CHART_COLORS.primary} stopOpacity="0" />
                            </linearGradient>
                        </defs>

                        <path d={animLinePath} fill="none" stroke={CHART_COLORS.primary} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />

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
                    </svg>
                )}
                {/* Watermark — привязан к data area, как в SimpleChart.
                    Gridlines IndexChart как у SimpleChart: нижняя на padding.bottom
                    от низа SVG → bottom=padding.bottom+5 даёт 5px зазор.
                    padding обновляется в StrengthPage через resize listener,
                    значит watermark адаптивен к mobile/tablet/desktop. */}
                <ChartWatermark left={padding.left + 5} bottom={padding.bottom + 5} />

                {/* Current value label — TradingView-style на правой оси.
                    Pill с цветом primary line, на y координате последней точки.
                    Виден всегда (включая при hover). */}
                {chartData && chartData.points.length > 0 && (() => {
                    const last = chartData.points[chartData.points.length - 1];
                    return (
                        <div
                            className="absolute pointer-events-none"
                            style={{
                                // padding x=3, label-text-LEFT = axis-text-LEFT (line 189).
                                left: width - padding.right + 12 - 3,
                                top: last.y,
                                transform: 'translateY(-50%)',
                                background: 'var(--bg-primary)',
                                border: `1.5px solid ${CHART_COLORS.primary}`,
                                borderRadius: 4,
                                padding: '2px 3px',
                                fontSize: 'var(--chart-font-y, 16px)',
                                fontWeight: 700,
                                color: 'var(--text-primary)',
                                whiteSpace: 'nowrap',
                                zIndex: 2,
                                lineHeight: 1.2,
                                fontVariantNumeric: 'tabular-nums',
                            }}
                        >
                            {last.value.toLocaleString('ru-RU', { maximumFractionDigits: 0 })}
                        </div>
                    );
                })()}
            </div>
        </div>
    );
}
