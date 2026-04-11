import React from 'react';
import { UK_LOGOS } from '../../config/fundConfig';
import { FUND_ANNOTATIONS } from '../../config/fundAnnotations';
import type { FundsFlowsResponse, FundCategory } from '../../services/api';
import { CHART_COLORS, GRID, CROSSHAIR, TOOLTIP } from '../../config/chartTheme';

interface FlowsHistogramProps {
    flowsData: FundsFlowsResponse | null;
    animatedBarsIn: number[];
    animatedBarsOut: number[];
    flowNavRange: [number, number];
    hoveredFlowIndex: number | null;
    hoveredAnnotation: string | null;
    showEvents: boolean;
    category: FundCategory;
    loading: boolean;
    flowContainerRef: React.RefObject<HTMLDivElement | null>;
    flowChartRef: React.RefObject<SVGSVGElement | null>;
    flowTooltipRef: React.RefObject<HTMLDivElement | null>;
    onMouseMove: (e: React.MouseEvent<HTMLDivElement>) => void;
    onMouseLeave: () => void;
    onSetHoveredAnnotation: (date: string | null) => void;
    onSetFlowNavRange: React.Dispatch<React.SetStateAction<[number, number]>>;
}

export default function FlowsHistogram({
    flowsData,
    animatedBarsIn,
    animatedBarsOut,
    flowNavRange,
    hoveredFlowIndex,
    hoveredAnnotation,
    showEvents,
    category,
    loading,
    flowContainerRef,
    flowChartRef,
    flowTooltipRef,
    onMouseMove,
    onMouseLeave,
    onSetHoveredAnnotation,
    onSetFlowNavRange,
}: FlowsHistogramProps) {
    return (
        <div className="p-6 relative">
            {/* Спиннер загрузки — в углу если есть старые данные, в центре если первая загрузка */}
            {loading && !flowsData?.flows?.length && animatedBarsIn.length === 0 ? (
                <div className="flex items-center justify-center" style={{ height: 450 }}>
                    <div className="flex flex-col items-center gap-3">
                        <div className="w-8 h-8 border-2 border-[#6366f1] border-t-transparent rounded-full animate-spin" />
                        <span className="text-theme-secondary">Загрузка...</span>
                    </div>
                </div>
            ) : (<>
            {loading && (
                <div className="absolute top-4 right-4 z-20 flex items-center gap-2 bg-theme-tertiary/90 backdrop-blur-sm px-3 py-1.5 rounded-lg border border-theme">
                    <div className="w-4 h-4 border-2 border-[#C8FF2E] border-t-transparent rounded-full animate-spin" />
                    <span className="text-xs text-theme-secondary">Обновление...</span>
                </div>
            )}
            {/* Гистограмма притоков/оттоков */}
            <div className="pb-6">
                {/* Легенда */}
                <div className="flex items-center justify-center gap-5 mb-2 text-sm">
                    <span className="flex items-center gap-2">
                        <span className="w-3 h-3 rounded-full" style={{ backgroundColor: CHART_COLORS.positive }} />
                        <span className="text-theme-primary font-medium">Приток</span>
                    </span>
                    <span className="flex items-center gap-2">
                        <span className="w-3 h-3 rounded-full" style={{ backgroundColor: CHART_COLORS.negative }} />
                        <span className="text-theme-primary font-medium">Отток</span>
                    </span>
                </div>

                {/* Плавающая дата — между легендой и графиком */}
                <div className="relative h-6 mb-1">
                    {hoveredFlowIndex !== null && flowsData?.flows && (() => {
                        const visibleFlowsList = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                        const f = visibleFlowsList[hoveredFlowIndex];
                        if (!f) return null;
                        const visibleCount = flowNavRange[1] - flowNavRange[0] + 1;
                        const padRight = parseFloat(getComputedStyle(document.documentElement).getPropertyValue('--chart-pad-left')) || 60;
                        const containerW = flowContainerRef.current?.getBoundingClientRect().width ?? 800;
                        const chartW = containerW - padRight;
                        const barWidth = chartW / visibleCount;
                        const centerX = hoveredFlowIndex * barWidth + barWidth / 2;
                        const labelW = 110; // ~ширина лейбла "13 окт. 2025 г."
                        const clampedX = Math.max(labelW / 2, Math.min(centerX, chartW - labelW / 2));
                        const dateStr = new Date(f.period_end).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', year: 'numeric' });
                        return (
                            <div
                                className="absolute z-30 pointer-events-none"
                                style={{ left: clampedX, top: 0, transform: 'translateX(-50%)' }}
                            >
                                <span className="text-[11px] text-theme-secondary bg-theme-tertiary/90 backdrop-blur-sm px-2 py-0.5 rounded border border-theme whitespace-nowrap">
                                    {dateStr}
                                </span>
                            </div>
                        );
                    })()}
                </div>

                {/* График с тултипом */}
                <div
                    ref={flowContainerRef}
                    className="relative cursor-crosshair"
                    style={{ aspectRatio: '2.4', minHeight: 280, maxHeight: 550 }}
                    onMouseMove={onMouseMove}
                    onMouseLeave={onMouseLeave}
                >
                    {/* Область графика: отступ справа для подписей */}
                    <div className="absolute inset-0" style={{ right: 'var(--chart-pad-left, 60px)' }}>
                    <svg
                        ref={flowChartRef}
                        width="100%"
                        height="100%"
                        preserveAspectRatio="none"
                    >
                        {animatedBarsIn.length > 0 && (() => {
                            const visibleIn = animatedBarsIn.slice(flowNavRange[0], flowNavRange[1] + 1);
                            const visibleOut = animatedBarsOut.slice(flowNavRange[0], flowNavRange[1] + 1);
                            const maxScale = Math.max(
                                ...visibleIn.map(v => Math.abs(v)),
                                ...visibleOut.map(v => Math.abs(v)),
                                0.001
                            );
                            const barWidth = 100 / (visibleIn.length || 1);
                            const midY = 50;
                            const halfH = 47;
                            const minBarH = 1.2;

                            return visibleIn.map((inVal, i) => {
                                const outVal = visibleOut[i] ?? 0;
                                const isHovered = hoveredFlowIndex === i;
                                const opacity = hoveredFlowIndex === null ? 1 : isHovered ? 1 : 0.35;
                                const x = `${i * barWidth + barWidth * 0.15}%`;
                                const w = `${barWidth * 0.7}%`;

                                const hIn = inVal > 0 ? Math.max((inVal / maxScale) * halfH, minBarH) : 0;
                                const hOut = outVal < 0 ? Math.max((Math.abs(outVal) / maxScale) * halfH, minBarH) : 0;

                                return (
                                    <g key={i} opacity={opacity}>
                                        {hIn > 0 && (
                                            <rect x={x} y={`${midY - hIn}%`} width={w} height={`${hIn}%`}
                                                fill={CHART_COLORS.positive} rx="2" />
                                        )}
                                        {hOut > 0 && (
                                            <rect x={x} y={`${midY}%`} width={w} height={`${hOut}%`}
                                                fill={CHART_COLORS.negative} rx="2" />
                                        )}
                                    </g>
                                );
                            });
                        })()}
                        {/* Горизонтальные линии сетки */}
                        {flowsData?.flows?.length && (() => {
                            const visibleF = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                            const maxAbsF = Math.max(
                                ...visibleF.map(f => Math.abs(f.flow)),
                                0.001
                            );
                            const ticks = [-maxAbsF, -maxAbsF / 2, 0, maxAbsF / 2, maxAbsF];
                            return ticks.map((val, i) => {
                                const yPct = 50 - (val / maxAbsF) * 47;
                                return (
                                    <line key={`grid-${i}`}
                                        x1="0" y1={`${yPct}%`} x2="100%" y2={`${yPct}%`}
                                        stroke={val === 0 ? GRID.zero : GRID.major} strokeWidth="1"
                                    />
                                );
                            });
                        })()}
                        {/* Вертикальный курсор */}
                        {hoveredFlowIndex !== null && flowsData?.flows && (() => {
                            const visibleCount = flowNavRange[1] - flowNavRange[0] + 1;
                            const barWidth = 100 / visibleCount;
                            const cx = hoveredFlowIndex * barWidth + barWidth / 2;
                            return (
                                <line
                                    x1={`${cx}%`}
                                    y1="0"
                                    x2={`${cx}%`}
                                    y2="100%"
                                    stroke={CROSSHAIR.accentColor}
                                    strokeWidth="1"
                                    strokeDasharray={CROSSHAIR.accentDashArray}
                                    opacity={CROSSHAIR.accentOpacity}
                                    style={{ pointerEvents: 'none' }}
                                />
                            );
                        })()}
                        {/* Вертикальная линия аномалии — только при hover на кружок */}
                        {hoveredAnnotation && flowsData?.flows && (() => {
                            const visibleFlows = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                            // Нечёткий поиск — как для кружков (допуск 7 дней)
                            const annDate = new Date(hoveredAnnotation).getTime();
                            let idx = -1;
                            let bestDist = Infinity;
                            for (let i = 0; i < visibleFlows.length; i++) {
                                const dist = Math.abs(new Date(visibleFlows[i].period_end).getTime() - annDate);
                                if (dist < bestDist && dist <= 7 * 86400000) { bestDist = dist; idx = i; }
                            }
                            if (idx === -1) return null;
                            const barW = 100 / visibleFlows.length;
                            const cx = idx * barW + barW / 2;
                            return (
                                <line
                                    x1={`${cx}%`} y1="0" x2={`${cx}%`} y2="100%"
                                    stroke={CHART_COLORS.muted} strokeWidth="1" strokeDasharray="3 4"
                                    opacity="0.4" style={{ pointerEvents: 'none' }}
                                />
                            );
                        })()}
                    </svg>
                    </div>

                    {/* Тултип-карточка со значением — позиция через ref */}
                    {hoveredFlowIndex !== null && flowsData?.flows && (() => {
                        const visibleFlowsList = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                        const f = visibleFlowsList[hoveredFlowIndex];
                        if (!f) return null;
                        const flowStr = `${f.flow > 0 ? '+' : ''}${Math.abs(f.flow) >= 0.01 ? f.flow.toFixed(2) : f.flow.toFixed(3)} млрд ₽`;
                        const pctStr = `${f.flow_pct > 0 ? '+' : ''}${f.flow_pct.toFixed(2)}%`;
                        const color = f.flow >= 0 ? CHART_COLORS.positive : CHART_COLORS.negative;
                        return (
                            <div
                                ref={flowTooltipRef}
                                className="absolute z-30 pointer-events-none"
                            >
                                <div className={`${TOOLTIP.containerClass} pointer-events-none`}>
                                    <div className="flex items-center justify-between gap-3">
                                        <div className="flex items-center gap-1.5 min-w-0">
                                            <span className={`${TOOLTIP.dotSize} flex-shrink-0`} style={{ backgroundColor: color }} />
                                            <span className={`${TOOLTIP.labelClass} truncate`}>{f.flow >= 0 ? 'Приток' : 'Отток'}</span>
                                        </div>
                                        <span className={`${TOOLTIP.valueClass} whitespace-nowrap`} style={{ color }}>
                                            {flowStr}
                                        </span>
                                    </div>
                                    <div className="flex items-center justify-between gap-3 mt-0.5">
                                        <div className="flex items-center gap-1.5 min-w-0">
                                            <span className={`${TOOLTIP.dotSize} flex-shrink-0`} style={{ backgroundColor: CHART_COLORS.primary }} />
                                            <span className={TOOLTIP.labelClass}>Изменение</span>
                                        </div>
                                        <span className={`${TOOLTIP.valueClass} whitespace-nowrap`} style={{ color }}>
                                            {pctStr}
                                        </span>
                                    </div>
                                </div>
                            </div>
                        );
                    })()}

                    {/* Подписи значений справа */}
                    {flowsData?.flows?.length && (() => {
                        const visibleF = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                        const maxAbs = Math.max(
                            ...visibleF.map(f => Math.abs(f.flow)),
                            0.001
                        );
                        const ticks = [maxAbs, maxAbs / 2, 0, -maxAbs / 2, -maxAbs];
                        return ticks.map((val, i) => {
                            const yPct = 50 - (val / maxAbs) * 47;
                            const label = val === 0 ? '0' : `${val > 0 ? '+' : ''}${Math.abs(val) >= 0.1 ? val.toFixed(1) : val.toFixed(2)}`;
                            return (
                                <div key={`label-${i}`}
                                    className="absolute pointer-events-none"
                                    style={{ top: `${yPct}%`, right: 4, transform: 'translateY(-50%)' }}
                                >
                                    <span className="font-semibold" style={{ fontSize: 'var(--chart-font-y, 16px)', color: CHART_COLORS.muted }}>
                                        {label}
                                    </span>
                                </div>
                            );
                        });
                    })()}



                    {/* Даты оси X — равномерно по всей ширине */}
                    <div className="absolute -bottom-6 left-0 flex justify-between font-semibold px-2" style={{ right: 'var(--chart-pad-left, 60px)', fontSize: 'var(--chart-font-x, 14px)', color: 'var(--axis-color, #9CA3B8)' }}>
                        {flowsData?.flows && flowsData.flows.length > 0 && (() => {
                            const flows = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                            if (!flows.length) return null;
                            const tickCount = Math.min(6, flows.length);
                            return Array.from({ length: tickCount }, (_, i) => {
                                const idx = Math.min(Math.round(i * (flows.length - 1) / Math.max(tickCount - 1, 1)), flows.length - 1);
                                if (!flows[idx]) return null;
                                const date = new Date(flows[idx].period_end);
                                return (
                                    <span key={i}>{date.toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', year: '2-digit' })}</span>
                                );
                            });
                        })()}
                    </div>
                </div>

                {/* Маркеры аномальных событий — зарезервированное место */}
                <div className="relative" style={{ height: 28, marginTop: -34, right: 0 }}>
                <div style={{ position: 'absolute', left: 0, right: 'var(--chart-pad-left, 60px)', top: 0, bottom: 0 }}>
                {showEvents && flowsData?.flows && flowsData.flows.length > 0 && (() => {
                    const visibleFlows = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                    const barW = 100 / visibleFlows.length; // в процентах — как пунктирная линия

                    // Находим аннотации — нечёткое совпадение дат (ближайший бар к дате аннотации)
                    const markers = FUND_ANNOTATIONS
                        .filter(a => a.category === category)
                        .map(annotation => {
                            const annDate = new Date(annotation.date).getTime();
                            // Ищем ближайший бар по дате (не строгое совпадение)
                            let bestIdx = -1;
                            let bestDist = Infinity;
                            for (let i = 0; i < visibleFlows.length; i++) {
                                const barDate = new Date(visibleFlows[i].period_end).getTime();
                                const dist = Math.abs(barDate - annDate);
                                // Допуск: до 7 дней (для недельного ТФ)
                                if (dist < bestDist && dist <= 7 * 86400000) {
                                    bestDist = dist;
                                    bestIdx = i;
                                }
                            }
                            if (bestIdx === -1) return null;
                            const logo = UK_LOGOS[annotation.ukId];
                            if (!logo) return null;
                            const xPct = bestIdx * barW + barW / 2; // % — совпадает с пунктиром
                            return { ...annotation, idx: bestIdx, logo, xPct };
                        })
                        .filter(Boolean) as (typeof FUND_ANNOTATIONS[0] & { idx: number; logo: typeof UK_LOGOS[string]; xPct: number })[];

                    if (!markers.length) return null;

                    return (<>
                            {markers.map((m, i) => (
                                <div
                                    key={i}
                                    className="absolute -translate-x-1/2 group"
                                    style={{ left: `${m.xPct}%` }}
                                    onMouseEnter={() => onSetHoveredAnnotation(m.date)}
                                    onMouseLeave={() => onSetHoveredAnnotation(null)}
                                >
                                    {/* Кружок — серый, мелкий */}
                                    <div
                                        className="w-7 h-7 rounded-full flex items-center justify-center cursor-pointer transition-opacity opacity-50 hover:opacity-100"
                                        style={{ backgroundColor: '#3a3f4f', color: CHART_COLORS.muted, fontSize: 11, fontWeight: 600 }}
                                    >
                                        {m.logo.letter}
                                    </div>

                                    {/* Тултип при hover */}
                                    <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 hidden group-hover:block z-50 pointer-events-none">
                                        <div className="bg-theme-tertiary/95 backdrop-blur-sm rounded-lg border border-theme shadow-xl py-1.5 px-2.5 whitespace-nowrap max-w-[320px]">
                                            <div className="flex items-center gap-2 mb-0.5">
                                                <span className="text-[10px] font-medium" style={{ color: CHART_COLORS.muted }}>
                                                    {m.type === 'merger' ? 'Слияние' : m.type === 'liquidation' ? 'Ликвидация' : 'Реорганизация'}
                                                </span>
                                                <span className="text-[10px] text-theme-secondary">
                                                    {new Date(m.date).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', year: 'numeric' })}
                                                </span>
                                            </div>
                                            <p className="text-[11px] text-theme-primary whitespace-normal leading-tight">
                                                {m.description}
                                            </p>
                                        </div>
                                    </div>
                                </div>
                            ))}
                    </>);
                })()}
                </div>
                </div>

                {/* Скользящее окно — минималистичный ползунок */}
                {flowsData?.flows && flowsData.flows.length > 1 && (() => {
                    const n = flowsData.flows.length;
                    const [s0, s1] = flowNavRange;
                    const handleNavMouse = (e: React.MouseEvent<HTMLDivElement>, type: 'left' | 'right' | 'window') => {
                        e.preventDefault();
                        const container = e.currentTarget.closest('[data-flow-nav]') as HTMLDivElement;
                        if (!container) return;
                        const rect = container.getBoundingClientRect();
                        const startX = e.clientX;
                        const startS0 = s0, startS1 = s1;
                        const onMove = (ev: MouseEvent) => {
                            const dx = ev.clientX - startX;
                            const di = Math.round((dx / rect.width) * (n - 1));
                            if (type === 'left') {
                                onSetFlowNavRange([Math.max(0, Math.min(startS0 + di, s1 - 1)), s1]);
                            } else if (type === 'right') {
                                onSetFlowNavRange([s0, Math.max(s0 + 1, Math.min(startS1 + di, n - 1))]);
                            } else {
                                const range = startS1 - startS0;
                                let ns = Math.max(0, Math.min(startS0 + di, n - 1 - range));
                                onSetFlowNavRange([ns, ns + range]);
                            }
                        };
                        const onUp = () => { document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp); };
                        document.addEventListener('mousemove', onMove);
                        document.addEventListener('mouseup', onUp);
                    };

                    const selLeftPct = (s0 / Math.max(n - 1, 1)) * 100;
                    const selRightPct = (s1 / Math.max(n - 1, 1)) * 100;

                    return (
                        <div className="mt-10 relative select-none" style={{ height: 56 }} data-flow-nav>
                            {/* Неактивный трек */}
                            <div className="absolute inset-x-0 inset-y-0 bg-white/[0.03] rounded-lg" />
                            {/* Маска слева */}
                            <div className="absolute inset-y-0 left-0 bg-black/50 rounded-l-lg"
                                style={{ width: `${selLeftPct}%` }} />
                            {/* Маска справа */}
                            <div className="absolute inset-y-0 right-0 bg-black/50 rounded-r-lg"
                                style={{ width: `${100 - selRightPct}%` }} />
                            {/* Активная зона */}
                            <div className="absolute inset-y-0 cursor-grab"
                                style={{
                                    left: `${selLeftPct}%`,
                                    width: `${selRightPct - selLeftPct}%`,
                                    background: 'rgba(56,98,251,0.08)',
                                    borderTop: '1px solid rgba(56,98,251,0.45)',
                                    borderBottom: '1px solid rgba(56,98,251,0.45)',
                                }}
                                onMouseDown={(e) => handleNavMouse(e, 'window')}
                            />
                            {/* Левый хэндл */}
                            <div className="absolute top-1/2 -translate-y-1/2 -translate-x-1/2 flex items-center justify-center cursor-ew-resize"
                                style={{
                                    left: `${selLeftPct}%`,
                                    width: 14, height: 56 * 0.7,
                                    borderRadius: 3,
                                    background: 'rgba(56,98,251,0.9)',
                                }}
                                onMouseDown={(e) => handleNavMouse(e, 'left')}
                            >
                                <svg width="6" height="10" viewBox="0 0 6 10" className="pointer-events-none">
                                  <path d="M4 1 L1 5 L4 9" fill="none" stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                                </svg>
                            </div>
                            {/* Правый хэндл */}
                            <div className="absolute top-1/2 -translate-y-1/2 -translate-x-1/2 flex items-center justify-center cursor-ew-resize"
                                style={{
                                    left: `${selRightPct}%`,
                                    width: 14, height: 56 * 0.7,
                                    borderRadius: 3,
                                    background: 'rgba(56,98,251,0.9)',
                                }}
                                onMouseDown={(e) => handleNavMouse(e, 'right')}
                            >
                                <svg width="6" height="10" viewBox="0 0 6 10" className="pointer-events-none">
                                  <path d="M2 1 L5 5 L2 9" fill="none" stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                                </svg>
                            </div>
                        </div>
                    );
                })()}
            </div>
            </>)}
        </div>
    );
}
