import { memo, useEffect, useLayoutEffect, useState, useMemo, useRef, useCallback } from 'react';
import { lerp, easeOutCubic, morphPts, ptsToPath, type SyncedDataPoint, type ChartPadding } from './chartUtils';
import { GRID, CROSSHAIR, ANIMATION } from '../../config/chartTheme';
import { useIsMobile } from '../../hooks/useIsMobile';
import { useViewportWidth } from '../../hooks/useViewportWidth';
import { axisFontSize, xAxisTickCount } from '../chart/chartTypography';
import { measureText } from '../chart/measureText';
import ChartWatermark from '../ChartWatermark';

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
    /** Клик по «+» на оси % → создать алерт на уровень силы рынка (levelPct) +
     *  currentValuePct (текущее значение для «Сейчас: …»). Задан → на десктопе при
     *  наведении рисуется горизонталь + «+» пилюля в жёлобе правой оси. */
    onCreateAlert?: (levelPct: number, currentValuePct: number) => void;
    /** Уровни активных алертов (%) → пунктир на графике. */
    alertLevels?: number[];
    /** Показать водяной знак «Фрейм». Задаётся `!showPrice` — когда индекс скрыт,
     *  этот график становится соло, и марка переезжает сюда с IndexChart
     *  (при двух графиках знак остаётся только на индексе → без дубля). */
    showWatermark?: boolean;
}

export default function BreadthChart({
    syncedData,
    hoverIndex,
    height,
    mode,
    padding,
    isNavDragRef,
    onCreateAlert,
    alertLevels,
    showWatermark = false,
}: BreadthChartProps) {
    const svgRef = useRef<SVGSVGElement>(null);
    const chartWrapRef = useRef<HTMLDivElement>(null);
    // На мобиле X-tick'ов меньше — иначе формат "29 окт. 25 г." накладывается
    const isMobile = useIsMobile();
    const vw = useViewportWidth();
    const axisFs = axisFontSize(vw);
    const containerRef = useRef<HTMLDivElement>(null);
    const [width, setWidth] = useState(0);

    // Animation for line mode
    const [animLinePath, setAnimLinePath] = useState('');
    const prevPtsRef = useRef<{ x: number; y: number }[]>([]);
    const currPtsRef = useRef<{ x: number; y: number }[]>([]);
    const animRef = useRef<number | null>(null);
    const isFirstRef = useRef(true);
    // CSS-driven reveal на первом рендере (clip-path анимация слева направо)
    const [revealed, setRevealed] = useState(false);

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

    // «+» алерт на ось % (TradingView-стиль). cursorY трекаем локально в этом SVG.
    const alertsOn = !!onCreateAlert && !isMobile;
    const [cursorY, setCursorY] = useState<number | null>(null);
    const handleAlertMove = useCallback((e: React.MouseEvent<SVGSVGElement>) => {
        if (!alertsOn) return;
        const rect = e.currentTarget.getBoundingClientRect();
        const y = e.clientY - rect.top;
        setCursorY(Math.min(Math.max(y, padding.top), padding.top + chartHeight));
    }, [alertsOn, padding.top, chartHeight]);
    // Инверсия scaleY (домен фиксирован 0..100): уровень % под курсором.
    const levelAt = (y: number) => 100 * (padding.top + chartHeight - y) / chartHeight;
    const scaleYPct = (v: number) => padding.top + chartHeight - (Math.max(0, Math.min(v, 100)) / 100) * chartHeight;

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

        // Adaptive count — chartWidth/labelWidth, формула в chartTypography.
        // Auto-corrects под текущий axis font size (6 breakpoints).
        const xTickCount = Math.min(xAxisTickCount(chartWidth, axisFs), syncedData.length);
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
    }, [syncedData, chartWidth, chartHeight, padding, getColor, isMobile, axisFs]);

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
                // CSS-reveal на первом рендере (как в SimpleChart/IndexChart)
                if (!revealed) setRevealed(true);
                return;
            }

            const fromPts = currPtsRef.current.length > 0 ? currPtsRef.current : prevPtsRef.current;
            const fromBars = currBarsRef.current.length > 0 ? currBarsRef.current : prevBarsRef.current;
            let start: number | null = null;

            const isFirstAnim = fromBars.length === 0;
            const totalDuration = isFirstAnim ? ANIMATION.waveDuration : ANIMATION.morphDuration;
            const staggerDelay = isFirstAnim ? ANIMATION.waveStagger : 0;

            // Ресемплим старые bars до длины новых — устраняет рывок при смене
            // количества bars (например период 6m→1y: 180→365). Без ресемпла все
            // "лишние" бары (i > fromBars.length-1) стартовали бы из позиции
            // одного и того же последнего старого бара → визуально "выпадали из одной точки".
            // Теперь каждый новый бар стартует с интерполированного значения между
            // двумя соседними старыми барами (тот же приём что resamplePts для линий).
            const resampleBarHeights = (bars: { y: number; height: number }[], len: number) => {
                if (bars.length === 0 || bars.length === len) return bars;
                const out: { y: number; height: number }[] = [];
                for (let i = 0; i < len; i++) {
                    const srcT = i / Math.max(len - 1, 1) * (bars.length - 1);
                    const lo = Math.floor(srcT);
                    const hi = Math.min(lo + 1, bars.length - 1);
                    const lt = srcT - lo;
                    out.push({
                        y: lerp(bars[lo].y, bars[hi].y, lt),
                        height: lerp(bars[lo].height, bars[hi].height, lt),
                    });
                }
                return out;
            };
            const fromBarsResampled = resampleBarHeights(fromBars, targetBars.length);

            const animate = (ts: number) => {
                if (!start) start = ts;
                const elapsed = ts - start;
                const lineT = easeOutCubic(Math.min(elapsed / totalDuration, 1));

                const interp = morphPts(fromPts, targetPts, lineT);
                currPtsRef.current = interp;
                setAnimLinePath(ptsToPath(interp));

                const n = targetBars.length;
                const morphedBars: typeof targetBars = [];
                for (let i = 0; i < n; i++) {
                    const barDelay = (i / Math.max(n - 1, 1)) * staggerDelay;
                    const barElapsed = Math.max(0, elapsed - barDelay);
                    const t = easeOutCubic(Math.min(barElapsed / (totalDuration - staggerDelay), 1));
                    const fb = fromBarsResampled[i] || { y: bottom, height: 0 };
                    const tb = targetBars[i];
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
            <div ref={chartWrapRef} className={`relative ${revealed ? 'chart-reveal' : ''}`}>
                {width > 0 && chartData && (
                    <svg ref={svgRef} width={width} height={height} className="block"
                        style={{ backgroundColor: 'var(--bg-primary)', contain: 'paint' }}
                        onMouseMove={alertsOn ? handleAlertMove : undefined}
                        onMouseLeave={alertsOn ? () => setCursorY(null) : undefined}>
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
                                paintOrder="stroke" stroke="var(--bg-primary)" strokeWidth="4" strokeLinejoin="round">
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

                        {/* Current value pill — TV-style filled + dashed connector
                            от последней точки до pill. Цвет = breadth color
                            (зелёный/красный по %). Pixel-perfect alignment через SVG. */}
                        {(() => {
                            const lastIdx = chartData.points.length - 1;
                            if (lastIdx < 0) return null;
                            const lastP = chartData.points[lastIdx];
                            const lastBreadth = syncedData[lastIdx]?.breadth ?? 0;
                            const value = `${lastBreadth.toFixed(1)}%`;
                            const color = getColor(lastBreadth);
                            const fontY = axisFs;
                            const fontWeight = 700;
                            const padX = 8; // как в SimpleChart — единый размер заливки pill'а
                            const padY = 2;
                            const pillH = fontY + padY * 2;
                            const textW = measureText(value, fontY, fontWeight);
                            const pillW = Math.ceil(textW) + padX * 2 + 1;
                            // Pill в жёлобе оси с клампом по обоим краям (как IndexChart):
                            // не за правый край виджета (раньше острый срезанный угол справа)
                            // и не на конец линии слева.
                            const plotRight = width - padding.right;
                            let pillLeft = (plotRight + 12) - padX;
                            pillLeft = Math.min(pillLeft, width - 4 - pillW);
                            pillLeft = Math.max(pillLeft, plotRight + 2);
                            const textX = pillLeft + padX;
                            return (
                                <g pointerEvents="none">
                                    <rect
                                        x={pillLeft}
                                        y={lastP.y - pillH / 2}
                                        width={pillW}
                                        height={pillH}
                                        rx={4} ry={4}
                                        fill={color}
                                        fillOpacity={1}
                                    />
                                    <text
                                        x={textX}
                                        y={lastP.y}
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

                        {/* Уровни активных алертов силы рынка → пунктир. */}
                        {onCreateAlert && alertLevels && alertLevels.map((lv, i) => (
                            <line key={`al-${i}`} pointerEvents="none"
                                x1={padding.left} y1={scaleYPct(lv)} x2={width - padding.right} y2={scaleYPct(lv)}
                                stroke="var(--accent)" strokeWidth={1} strokeDasharray="5,4" opacity={0.75} />
                        ))}

                        {/* Горизонтальный кросхэйр + «+» пилюля на оси % (десктоп). */}
                        {alertsOn && cursorY !== null && chartData && (() => {
                            const level = levelAt(cursorY);
                            const label = `${level.toFixed(1)}%`;
                            const lastBreadth = syncedData[syncedData.length - 1]?.breadth ?? 0;
                            const fontY = axisFs, fontWeight = 700;
                            const padX = 8, padY = 2, AIR = 4, gap = 4;
                            const pillH = fontY + padY * 2;
                            const r = fontY * 0.62;
                            const fill = 'color-mix(in srgb, var(--accent) 45%, var(--bg-primary))';
                            const valW = measureText(label, fontY, fontWeight);
                            const pillW = Math.ceil(valW) + padX * 2;
                            const plotRight = width - padding.right;
                            let valLeft = plotRight + AIR;
                            let cx = valLeft + pillW + gap + r;
                            const groupRight = cx + r;
                            if (groupRight > width - 2) { const s = (width - 2) - groupRight; valLeft += s; cx += s; }
                            return (
                                <g>
                                    <line pointerEvents="none" x1={padding.left} y1={cursorY} x2={plotRight} y2={cursorY}
                                        stroke="var(--text-primary)" strokeWidth={CROSSHAIR.strokeWidth} strokeDasharray="4,4" opacity={0.4} />
                                    <g style={{ cursor: 'pointer' }} onClick={() => onCreateAlert(level, lastBreadth)}>
                                        <rect x={valLeft - 2} y={cursorY - pillH / 2 - 4}
                                            width={(cx + r) - valLeft + 4} height={pillH + 8} fill="transparent" />
                                        <rect x={valLeft} y={cursorY - pillH / 2} width={pillW} height={pillH} rx={4} ry={4} fill={fill} />
                                        <text x={valLeft + pillW / 2} y={cursorY} textAnchor="middle" dominantBaseline="central"
                                            fill="#FFFFFF" fontSize={fontY} fontWeight={fontWeight}>{label}</text>
                                        <circle cx={cx} cy={cursorY} r={r} fill={fill} stroke="#FFFFFF" strokeWidth={1} strokeOpacity={0.6} />
                                        <line x1={cx - r * 0.5} y1={cursorY} x2={cx + r * 0.5} y2={cursorY}
                                            stroke="#FFFFFF" strokeWidth={Math.max(1.5, r * 0.24)} strokeLinecap="round" />
                                        <line x1={cx} y1={cursorY - r * 0.5} x2={cx} y2={cursorY + r * 0.5}
                                            stroke="#FFFFFF" strokeWidth={Math.max(1.5, r * 0.24)} strokeLinecap="round" />
                                    </g>
                                </g>
                            );
                        })()}
                    </svg>
                )}
                {/* Watermark — только в соло-режиме (индекс скрыт). Позиция 1:1 с
                    IndexChart, чтобы марка стояла в той же точке, что раньше на индексе. */}
                {showWatermark && (
                    <ChartWatermark left={padding.left + 5} bottom={padding.bottom + 5} />
                )}
            </div>
        </div>
    );
}
