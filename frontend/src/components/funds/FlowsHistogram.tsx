import React, { useMemo } from 'react';
import { BarChart3 } from 'lucide-react';
import { UK_LOGOS } from '../../config/fundConfig';
import { FUND_ANNOTATIONS } from '../../config/fundAnnotations';
import type { FundsFlowsResponse, FundCategory } from '../../services/api';
import { CHART_COLORS, GRID, CROSSHAIR } from '../../config/chartTheme';
import { useIsMobile } from '../../hooks/useIsMobile';
import ChartWatermark from '../ChartWatermark';
import ChartLegend from '../chart/ChartLegend';
import ChartNavigator from '../ChartNavigator';
import { ChartTooltip, TooltipRow } from '../chart';
import { computeChartTopLineY, getDatePillStyle } from '../chart/datePillLayout';

interface FlowsHistogramProps {
    flowsData: FundsFlowsResponse | null;
    /** Все фонды категории выключены пользователем — показываем empty-state
     *  «Выберите фонды» вместо пустой/сломанной гистограммы. */
    noFundsSelected?: boolean;
    animatedBarsIn: number[];
    animatedBarsOut: number[];
    flowNavRange: [number, number];
    hoveredFlowIndex: number | null;
    hoveredAnnotation: string | null;
    showEvents: boolean;
    category: FundCategory;
    hiddenTickers?: Set<string>;
    allTickers?: Set<string>;
    loading: boolean;
    flowContainerRef: React.RefObject<HTMLDivElement | null>;
    flowChartRef: React.RefObject<SVGSVGElement | null>;
    /** Pos через state — рендерится атомарно с tooltip'ом через ChartTooltip. */
    flowTooltipPos: { x: number; y: number } | null;
    onMouseMove: (e: React.MouseEvent<HTMLDivElement>) => void;
    onMouseLeave: () => void;
    onSetHoveredAnnotation: (date: string | null) => void;
    onSetFlowNavRange: React.Dispatch<React.SetStateAction<[number, number]>>;
    /** Текст legend'а для положительных баров (например «Приток в фонды денежного рынка, млрд руб»). Если не задан — default «Приток (млрд руб)». */
    inflowLabel?: string;
    /** Текст legend'а для отрицательных баров (например «Отток из фондов денежного рынка»). Если не задан — default «Отток (млрд руб)». */
    outflowLabel?: string;
}

export default function FlowsHistogram({
    flowsData,
    noFundsSelected = false,
    animatedBarsIn,
    animatedBarsOut,
    flowNavRange,
    hoveredFlowIndex,
    hoveredAnnotation,
    showEvents,
    category,
    hiddenTickers,
    allTickers,
    loading,
    flowContainerRef,
    flowChartRef,
    flowTooltipPos,
    onMouseMove,
    onMouseLeave,
    onSetHoveredAnnotation,
    onSetFlowNavRange,
    inflowLabel = 'Приток (млрд руб)',
    outflowLabel = 'Отток (млрд руб)',
}: FlowsHistogramProps) {
    // На мобиле уменьшаем количество X-tick'ов чтобы даты не накладывались
    // (формат "29 окт. 25 г." ≈ 70px на 10px шрифте → 6 шт. = 420px > 343px viewport).
    const isMobile = useIsMobile();

    // Стабильная ссылка на data для ChartNavigator — иначе на каждый render
    // .map() создаёт новый массив → внутренний useEffect([data]) сбрасывает selFrac,
    // вызывает onChange, родитель setState → новый рендер → ∞ цикл.
    const navigatorData = useMemo(
        () => flowsData?.flows?.map(f => ({ time: f.period_end, value: f.flow })) ?? [],
        [flowsData?.flows]
    );

    return (
        <div className="rounded-2xl p-5 bg-theme-primary border border-theme relative">
            {/* Empty-state: все фонды выключены. Показываем подсказку вместо
                сломанной/пустой гистограммы. Приоритетнее loading и data. */}
            {noFundsSelected ? (
                <div
                    className="flex flex-col items-center justify-center text-center"
                    style={{ height: 'calc(var(--chart-height, 450px) + 100px)', gap: 'var(--sp-3)', padding: 'var(--sp-6)' }}
                >
                    {/* Editorial-эмблема: accent-square с outline + hard-shadow
                        и белой иконкой — тот же стиль что page-header-icon
                        и иконка в UpgradeModal. */}
                    <div
                        style={{
                            width: 56,
                            height: 56,
                            borderRadius: 12,
                            background: 'var(--accent)',
                            border: '2px solid var(--text-primary)',
                            boxShadow: 'var(--shadow-hard-chip, 3px 3px 0 var(--text-primary))',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
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
            ) : (<>
            {/* Спиннер загрузки — height МАТЧИТ полный размер loaded-версии:
                chart-height + legend (~36) + navigator (~64). Иначе CLS-jump когда данные приедут. */}
            {loading && !flowsData?.flows?.length && animatedBarsIn.length === 0 ? (
                <div className="flex items-center justify-center" style={{ height: 'calc(var(--chart-height, 450px) + 100px)' }}>
                    <div className="flex flex-col items-center" style={{ gap: 'var(--sp-3)' }}>
                        <div className="w-8 h-8 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
                        <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-base)' }}>Загрузка...</span>
                    </div>
                </div>
            ) : (<>
            {loading && (
                <div className="absolute top-4 right-4 z-20 flex items-center gap-2 rounded-lg border border-theme shadow-md" style={{ background: 'var(--bg-primary)', padding: 'var(--sp-2) var(--sp-3)' }}>
                    <div className="w-4 h-4 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
                    <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-xs)' }}>Обновление...</span>
                </div>
            )}
            {/* Гистограмма притоков/оттоков */}
            <div>
                {/* Легенда — SVG-based для pixel-perfect dot↔text alignment */}
                <div style={{ marginBottom: 'var(--chart-legend-mb, 16px)' }}>
                    <ChartLegend
                        items={[
                            { color: 'var(--funds-flow-positive)', label: inflowLabel },
                            { color: 'var(--funds-flow-negative)', label: outflowLabel },
                        ]}
                        fontWeight={600}
                        gap={20}
                        style={{ color: 'var(--text-primary)' }}
                    />
                </div>

                {/* График с тултипом */}
                <div
                    ref={flowContainerRef}
                    className="relative cursor-crosshair chart-reveal"
                    style={{
                        height: 'var(--chart-height, 450px)',
                        display: 'flow-root',
                        touchAction: 'none', // mobile: водение пальцем по чарту не скроллит страницу
                    }}
                    onMouseMove={onMouseMove}
                    onMouseLeave={onMouseLeave}
                    onTouchStart={(e) => {
                        if (!e.touches[0]) return;
                        // Синтезируем mouse-like event: clientX/Y + currentTarget — всё что нужно
                        // от event'а в этом handler'е (см. FundsMoneyPage).
                        const t = e.touches[0];
                        onMouseMove({ clientX: t.clientX, clientY: t.clientY, currentTarget: e.currentTarget } as React.MouseEvent<HTMLDivElement>);
                    }}
                    onTouchMove={(e) => {
                        if (!e.touches[0]) return;
                        const t = e.touches[0];
                        onMouseMove({ clientX: t.clientX, clientY: t.clientY, currentTarget: e.currentTarget } as React.MouseEvent<HTMLDivElement>);
                    }}
                    onTouchEnd={onMouseLeave}
                >
                    {/* Область графика — все отступы из CSS-переменных (унифицировано с SimpleChart) */}
                    <div className="absolute" style={{ top: 'var(--chart-pad-top, 19px)', bottom: 'var(--chart-pad-bottom, 50px)', left: 'var(--chart-pad-left, 100px)', right: 'var(--chart-pad-right-single, 95px)' }}>
                    <svg
                        ref={flowChartRef}
                        width="100%"
                        height="100%"
                        preserveAspectRatio="none"
                    >
                        {animatedBarsIn.length > 0 && flowsData?.flows && (() => {
                            const visibleIn = animatedBarsIn.slice(flowNavRange[0], flowNavRange[1] + 1);
                            const visibleOut = animatedBarsOut.slice(flowNavRange[0], flowNavRange[1] + 1);
                            // maxScale из ЦЕЛЕВЫХ данных (flowsData.flows), а не из
                            // анимированных (animatedBarsIn/Out). Иначе при волне:
                            // первые бары дорастают до 100%, потом при появлении более
                            // крупного бара масштаб скачет и первые бары сжимаются.
                            const visibleFlows = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                            const maxScale = Math.max(
                                ...visibleFlows.map(f => Math.abs(f.flow)),
                                0.001
                            );
                            const barWidth = 100 / (visibleIn.length || 1);
                            const midY = 50;
                            const halfH = 47;
                            const minBarH = 1.2;
                            // Динамическая толщина окантовки. Раньше был жёсткий
                            // cutoff (>50 баров → нет окантовки совсем): тонкие
                            // бары + stroke=1px = 80% чёрный, поэтому полностью
                            // отключали. Теперь strokeWidth плавно уменьшается:
                            // мало баров — полная окантовка 1px (читаемые границы);
                            // много баров — тонкая 0.25-0.4px (hint, не дoминирует).
                            // vectorEffect=non-scaling-stroke фиксирует px независимо
                            // от SVG ширины — окантовка не «прыгает» при resize.
                            const outlineWidth = visibleIn.length <= 20 ? 1
                                              : visibleIn.length <= 50 ? 0.7
                                              : visibleIn.length <= 100 ? 0.4
                                              : 0.25;

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
                                                fill={'var(--funds-flow-positive)'}
                                                stroke="var(--bar-outline)" strokeWidth={outlineWidth}
                                                vectorEffect="non-scaling-stroke"
                                                rx="2" />
                                        )}
                                        {hOut > 0 && (
                                            <rect x={x} y={`${midY}%`} width={w} height={`${hOut}%`}
                                                fill={'var(--funds-flow-negative)'}
                                                stroke="var(--bar-outline)" strokeWidth={outlineWidth}
                                                vectorEffect="non-scaling-stroke"
                                                rx="2" />
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
                        {/* Вертикальный курсор — bounded верхней/нижней grid-линией.
                            Grid-линии рассчитаны как yPct = 50 ± 47 (см. блок Горизонтальные
                            линии сетки выше). Crosshair должна совпадать с этим диапазоном,
                            а не идти от 0% до 100% (иначе выпирает на 3% с каждого края). */}
                        {hoveredFlowIndex !== null && flowsData?.flows && (() => {
                            const visibleCount = flowNavRange[1] - flowNavRange[0] + 1;
                            const barWidth = 100 / visibleCount;
                            const cx = hoveredFlowIndex * barWidth + barWidth / 2;
                            return (
                                <line
                                    x1={`${cx}%`}
                                    y1="3%"
                                    x2={`${cx}%`}
                                    y2="97%"
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
                                    x1={`${cx}%`} y1="3%" x2={`${cx}%`} y2="97%"
                                    stroke={CHART_COLORS.muted} strokeWidth="1" strokeDasharray="3 4"
                                    opacity="0.4" style={{ pointerEvents: 'none' }}
                                />
                            );
                        })()}
                    </svg>
                    </div>

                    {/* Watermark — sibling inner chart-area, в flowContainerRef.
                        Привязан к нижней gridline + 5px зазор (как у SimpleChart).
                        FlowsHistogram отличается: gridlines идут от 3% до 97%
                        высоты SVG (запас 3% по краям), а не 0%-100%. Поэтому
                        нижняя gridline = padding.bottom + 3% * SVG_h от низа.

                        Формула bottom:
                          padding.bottom              ← x-axis area
                        + 0.03 * SVG_h                ← gap до gridline (3% запас)
                        + 5px                         ← зазор как у СЧА
                        где SVG_h = chart-height - chart-pad-top - chart-pad-bottom

                        Desktop: 50 + 0.03*(450-19-50) + 5 ≈ 66px
                        Tablet:  50 + 0.03*(380-19-50) + 5 ≈ 64px
                        Mobile:  40 + 0.03*(300-16-40) + 5 ≈ 52px

                        left: padding.left + 5px (симметрично с bottom-gap). */}
                    <ChartWatermark
                        left="calc(var(--chart-pad-left, 100px) + 5px)"
                        bottom="calc(var(--chart-pad-bottom, 50px) + 0.03 * (var(--chart-height, 450px) - var(--chart-pad-top, 19px) - var(--chart-pad-bottom, 50px)) + 5px)"
                    />

                    {/* Тултип через shared ChartTooltip — тот же компонент что в
                        Seasonality. Auto-flip через измерение parentW + cardW
                        (ResizeObserver), позиционирование через style — атомарно
                        с React render. Никаких DOM-мутаций, никаких offsetWidth
                        в hot-path → следует за пальцем 1-в-1. */}
                    {hoveredFlowIndex !== null && flowsData?.flows && flowTooltipPos && (() => {
                        const visibleFlowsList = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                        const f = visibleFlowsList[hoveredFlowIndex];
                        if (!f) return null;
                        const flowStr = `${f.flow > 0 ? '+' : ''}${Math.abs(f.flow) >= 0.01 ? f.flow.toFixed(2) : f.flow.toFixed(3)} млрд ₽`;
                        const pctStr = `${f.flow_pct > 0 ? '+' : ''}${f.flow_pct.toFixed(2)}%`;
                        const color = f.flow >= 0 ? 'var(--funds-flow-positive)' : 'var(--funds-flow-negative)';
                        return (
                            <ChartTooltip x={flowTooltipPos.x} y={flowTooltipPos.y}>
                                <TooltipRow color={color} label={f.flow >= 0 ? 'Приток' : 'Отток'} value={flowStr} />
                                <TooltipRow color={CHART_COLORS.primary} label="Изменение" value={pctStr} valueClass={`${f.flow >= 0 ? '' : ''}`} />
                            </ChartTooltip>
                        );
                    })()}

                    {/* Подписи значений справа — в правой axis-зоне flowContainerRef */}
                    {flowsData?.flows?.length && (() => {
                        const visibleF = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                        const maxAbs = Math.max(
                            ...visibleF.map(f => Math.abs(f.flow)),
                            0.001
                        );
                        const ticks = [maxAbs, maxAbs / 2, 0, -maxAbs / 2, -maxAbs];
                        return (
                            <div className="absolute pointer-events-none" style={{ top: 'var(--chart-pad-top, 19px)', bottom: 'var(--chart-pad-bottom, 50px)', right: 0, width: 'var(--chart-pad-right-single, 95px)' }}>
                                {ticks.map((val, i) => {
                                    const yPct = 50 - (val / maxAbs) * 47;
                                    const label = val === 0 ? '0' : `${val > 0 ? '+' : ''}${Math.abs(val) >= 0.1 ? val.toFixed(1) : val.toFixed(2)}`;
                                    return (
                                        <div key={`label-${i}`}
                                            className="absolute"
                                            style={{ top: `${yPct}%`, left: 12, transform: 'translateY(-50%)' }}
                                        >
                                            <span className="font-semibold" style={{ fontSize: 'var(--chart-font-y, 16px)', color: 'var(--axis-color, #9CA3B8)' }}>
                                                {label}
                                            </span>
                                        </div>
                                    );
                                })}
                            </div>
                        );
                    })()}

                    {/* Даты оси X — формат DD.MM.YY (как в SimpleChart/OI), без
                        месячного слова. На мобиле 3 тика чтобы не накладывались. */}
                    <div className="absolute flex justify-between font-semibold px-2" style={{ bottom: 'var(--chart-xlabel-bottom, 20px)', left: 'var(--chart-pad-left, 100px)', right: 'var(--chart-pad-right-single, 95px)', fontSize: 'var(--chart-font-x, 14px)', color: 'var(--axis-color, #9CA3B8)' }}>
                        {flowsData?.flows && flowsData.flows.length > 0 && (() => {
                            const flows = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                            if (!flows.length) return null;
                            const tickCount = Math.min(isMobile ? 3 : 6, flows.length);
                            return Array.from({ length: tickCount }, (_, i) => {
                                const idx = Math.min(Math.round(i * (flows.length - 1) / Math.max(tickCount - 1, 1)), flows.length - 1);
                                if (!flows[idx]) return null;
                                const date = new Date(flows[idx].period_end);
                                return (
                                    <span key={i}>{date.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' })}</span>
                                );
                            });
                        })()}
                    </div>

                {/* Маркеры аномальных событий — зарезервированное место */}
                <div className="relative" style={{ height: 'var(--chart-annotation-height, 28px)', marginTop: 'var(--chart-annotation-offset, -34px)', right: 0 }}>
                <div style={{ position: 'absolute', left: 'var(--chart-pad-left, 100px)', right: 'var(--chart-pad-right-single, 95px)', top: 0, bottom: 0 }}>
                {showEvents && flowsData?.flows && flowsData.flows.length > 0 && (() => {
                    const visibleFlows = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                    const barW = 100 / visibleFlows.length; // в процентах — как пунктирная линия

                    // Находим аннотации — нечёткое совпадение дат (ближайший бар к дате аннотации)
                    const markers = FUND_ANNOTATIONS
                        .filter(a => a.category === category)
                        // Привязка к чекбоксам: если тикер фонда в нашем списке
                        // и он скрыт — скрываем событие. Если тикер не из нашего
                        // списка (внешний фонд) — показываем всегда.
                        .filter(a => {
                            if (!hiddenTickers || !allTickers) return true;
                            if (!allTickers.has(a.ticker)) return true; // не наш фонд → показываем
                            return !hiddenTickers.has(a.ticker); // наш → проверяем чекбокс
                        })
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
                                        <div className="rounded-lg border border-theme shadow-md whitespace-nowrap max-w-[320px]" style={{ background: 'var(--bg-primary)', padding: 'var(--sp-2) var(--sp-3)' }}>
                                            <div className="flex items-center gap-2 mb-0.5">
                                                <span className="font-medium" style={{ color: CHART_COLORS.muted, fontSize: 'var(--fs-2xs)' }}>
                                                    {m.type === 'merger' ? 'Слияние' : m.type === 'liquidation' ? 'Ликвидация' : 'Реорганизация'}
                                                </span>
                                                <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-2xs)' }}>
                                                    {new Date(m.date).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', year: 'numeric' })}
                                                </span>
                                            </div>
                                            <p className="text-theme-primary whitespace-normal leading-tight" style={{ fontSize: 'var(--fs-xs)' }}>
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
                </div>

                {/* Navigator — ОДИН и тот же ChartNavigator что в СЧА/OI/всех SimpleChart.
                    showPreview=false — у нас гистограмма, line preview не подходит.
                    Скрыт в html2canvas snapshot через data-export-ignore. */}
                {navigatorData.length > 1 && (
                    <div data-export-ignore="true">
                        <ChartNavigator
                            data={navigatorData}
                            color="var(--accent)"
                            showPreview={false}
                            onChange={(s, e) => onSetFlowNavRange([s, e])}
                        />
                    </div>
                )}
            </div>
            </>)}

            {/* Плавающая дата — позиционируется по реальному offsetTop/offsetLeft
                flowContainerRef'а. top = низ chart-area (за вычетом chart-pad-bottom)
                + небольшой подъём чтобы pill сидел на конце пунктирной линии. */}
            {hoveredFlowIndex !== null && flowsData?.flows && (() => {
                const visibleFlowsList = flowsData.flows.slice(flowNavRange[0], flowNavRange[1] + 1);
                const f = visibleFlowsList[hoveredFlowIndex];
                if (!f) return null;
                const cont = flowContainerRef.current;
                const svg = flowChartRef.current;
                if (!cont || !svg) return null;
                const visibleCount = flowNavRange[1] - flowNavRange[0] + 1;
                // Реальная геометрия SVG — не parseFloat clamp() из CSS-vars
                // (см. handleFlowMouseMove fix). chart-pad-top/bottom тоже через
                // svg.getBoundingClientRect для согласованности.
                const containerRect = cont.getBoundingClientRect();
                const svgRect = svg.getBoundingClientRect();
                const padTop = svgRect.top - containerRect.top;
                const containerH = cont.clientHeight;
                const chartW = svgRect.width;
                const chartAreaH = svgRect.height;
                const barWidth = chartW / visibleCount;
                void containerH; // not needed with new approach

                // Единая логика date-pill через computeChartTopLineY.
                const centerX = cont.offsetLeft + (svgRect.left - containerRect.left) + hoveredFlowIndex * barWidth + barWidth / 2;
                // computeChartTopLineY возвращает в outer-coords (т.к. wrapper.offsetTop
                // уже относительно outer'а). Не добавляем cont.offsetTop отдельно.
                const topLineY = computeChartTopLineY({
                    wrapper: cont,
                    paddingTop: padTop,
                    gridOffsetFrac: 0.03,
                    chartAreaHeight: chartAreaH,
                });
                const dateStr = new Date(f.period_end).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', year: 'numeric' });
                return (
                    <div
                        className="absolute z-20 pointer-events-none"
                        style={getDatePillStyle(centerX, topLineY)}
                    >
                        <span className="text-theme-secondary rounded border border-theme whitespace-nowrap" style={{ background: 'var(--bg-primary)', padding: 'calc(var(--sp-1)) var(--sp-2)', fontSize: 'var(--fs-2xs)' }}>
                            {dateStr}
                        </span>
                    </div>
                );
            })()}
            </>)}
        </div>
    );
}
