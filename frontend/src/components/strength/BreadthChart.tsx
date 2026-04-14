import { memo, useEffect, useLayoutEffect, useState, useMemo, useRef, useCallback } from 'react';
import { lerp, easeOutCubic, morphPts, ptsToPath, type SyncedDataPoint, type ChartPadding } from './chartUtils';
import { GRID, CROSSHAIR } from '../../config/chartTheme';

type ChartMode = 'line' | 'histogram';

type BarDef = { x: number; y: number; width: number; height: number; color: string };

const HistogramBars = memo(({ bars }: { bars: BarDef[] }) => {
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

interface BreadthChartProps {
    syncedData: SyncedDataPoint[];
    hoverIndex: number | null;
    height: number;
    mode: ChartMode;
    padding: ChartPadding;
    isNavDragRef?: { current: boolean };
}

export default function BreadthChart({
    syncedData,
    hoverIndex,
    height,
    mode,
    padding,
    isNavDragRef,
}: BreadthChartProps) {
    const svgRef = useRef<SVGSVGElement>(null);
    const chartWrapRef = useRef<HTMLDivElement>(null);
    const containerRef = useRef<HTMLDivElement>(null);
    const [width, setWidth] = useState(0);

    // Animation for line mode
    const [animLinePath, setAnimLinePath] = useState('');
    const prevPtsRef = useRef<{ x: number; y: number }[]>([]);
    const currPtsRef = useRef<{ x: number; y: number }[]>([]);
    const animRef = useRef<number | null>(null);
    const isFirstRef = useRef(true);

    // Animation for histogram mode
    const [animBars, setAnimBars] = useState<BarDef[]>([]);
    const prevBarsRef = useRef<{ y: number; height: number }[]>([]);
    const currBarsRef = useRef<{ y: number; height: number }[]>([]);
    const prevWidthRef = useRef(0);

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
        const t = Math.max(0, Math.min(value / 100, 1));
        if (t <= 0.35) {
            const s = t / 0.35;
            const r = Math.round(239 - s * 30);
            const g = Math.round(68 + s * 60);
            const b = Math.round(68 - s * 40);
            return `rgb(${r},${g},${b})`;
        }
        if (t <= 0.65) {
            const s = (t - 0.35) / 0.3;
            const r = Math.round(209 - s * 170);
            const g = Math.round(128 + s * 69);
            const b = Math.round(28 + s * 66);
            return `rgb(${r},${g},${b})`;
        }
        const s = (t - 0.65) / 0.35;
        const r = Math.round(39 - s * 5);
        const g = Math.round(197 + s * 3);
        const b = Math.round(94 - s * 0);
        return `rgb(${r},${g},${b})`;
    }, []);

    const breadthValues = useMemo(() => syncedData.map(d => d.breadth), [syncedData]);

    const chartData = useMemo(() => {
        if (!syncedData.length || chartWidth <= 0) return null;

        const yMin = 0;
        const yMax = 100;

        const scaleX = (i: number) => padding.left + (i / Math.max(syncedData.length - 1, 1)) * chartWidth;
        const scaleY = (v: number) => padding.top + chartHeight - ((v - yMin) / (yMax - yMin)) * chartHeight;

        const levels = [20, 40, 60, 80].map(value => ({
            value,
            color: GRID.major,
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
            color: 'var(--axis-color, #9CA3B8)',
        }));

        return { points, lineSegments, bars, levels, xTicks, yTicks, chartWidth, chartHeight, scaleX };
    }, [syncedData, chartWidth, chartHeight, padding, getColor]);

    // Morph animation
    useLayoutEffect(() => {
        if (!chartData) return;
        if (animRef.current) cancelAnimationFrame(animRef.current);

        const bottom = padding.top + chartHeight;

        // Resize -- instant update
        if (prevWidthRef.current !== 0 && prevWidthRef.current !== chartWidth) {
            prevWidthRef.current = chartWidth;
            const targetPts = chartData.points.map(p => ({ x: p.x, y: p.y }));
            prevPtsRef.current = targetPts;
            currPtsRef.current = [];
            prevBarsRef.current = chartData.bars.map(b => ({ y: b.y, height: b.height }));
            currBarsRef.current = [];
            setAnimLinePath(ptsToPath(targetPts));
            setAnimBars(chartData.bars);
            return;
        }
        prevWidthRef.current = chartWidth;

        // During navigator drag -- instant update
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

                setAnimLinePath(ptsToPath(targetPts));
                setAnimBars(targetBars);
                return;
            }

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
            <div ref={chartWrapRef}>
                {width > 0 && chartData && (
                    <svg ref={svgRef} width={width} height={height} className="block" style={{ backgroundColor: 'var(--bg-secondary)', contain: 'paint' }}>
                        <defs>
                            <linearGradient id="breadthGradient" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="0%" stopColor="var(--accent)" stopOpacity="0.3" />
                                <stop offset="100%" stopColor="var(--accent)" stopOpacity="0" />
                            </linearGradient>
                            {/* Clip: bars don't overflow chart area */}
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

                            {/* Animated histogram bars */}
                            {mode === 'histogram' && (
                                <g clipPath="url(#breadthChartClip)">
                                    <HistogramBars bars={animBars} />
                                    {hoverIndex !== null && animBars[hoverIndex] && (() => {
                                        const bar = animBars[hoverIndex];
                                        return <rect x={bar.x} y={bar.y} width={bar.width} height={bar.height}
                                            fill={bar.color} opacity={1} />;
                                    })()}
                                </g>
                            )}

                            {/* Animated line with colored segments */}
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
                                    stroke={CROSSHAIR.color} strokeWidth={CROSSHAIR.strokeWidth} strokeDasharray={CROSSHAIR.dashArray} />
                            )}
                            {crosshairX !== null && hoverIndex !== null && hoverIndex < chartData.points.length && (
                                <circle cx={chartData.points[hoverIndex].x} cy={chartData.points[hoverIndex].y}
                                    r={5} fill="var(--accent)" />
                            )}

                        </g>

                        {/* Y axis */}
                        {chartData.yTicks.map((tick, i) => (
                            <text key={i} x={width - padding.right + 12} y={tick.y}
                                textAnchor="start" dominantBaseline="middle"
                                fill={tick.color || 'var(--axis-color, #9CA3B8)'} fontSize="var(--chart-font-y, 16)" fontWeight="600"
                                paintOrder="stroke" stroke="var(--bg-secondary)" strokeWidth="4" strokeLinejoin="round">
                                {tick.value}%
                            </text>
                        ))}

                        {/* X axis */}
                        {chartData.xTicks.map((tick, i) => (
                            <text key={i} x={tick.x} y={padding.top + chartData.chartHeight + 18}
                                textAnchor={i === 0 ? 'start' : i === chartData.xTicks.length - 1 ? 'end' : 'middle'}
                                fill="var(--axis-color, #9CA3B8)" fontSize="var(--chart-font-x, 14)" fontWeight="600">
                                {tick.label}
                            </text>
                        ))}
                    </svg>
                )}
            </div>
        </div>
    );
}
