import React, { useMemo } from 'react';
import { BarChart3 } from 'lucide-react';
import type { FundsFlowsResponse } from '../../services/api';
import { CHART_COLORS, GRID, CROSSHAIR, cssVar } from '../../config/chartTheme';
import { useIsMobile } from '../../hooks/useIsMobile';
import ChartWatermark from '../ChartWatermark';
import ChartNavigator from '../ChartNavigator';
import ChartLegend from '../chart/ChartLegend';
import { ChartTooltip, TooltipRow, ChartDatePill } from '../chart';
import { computeChartTopLineY } from '../chart/datePillLayout';

interface FlowsHistogramProps {
    flowsData: FundsFlowsResponse | null;
    /** Все фонды категории выключены пользователем — показываем empty-state
     *  «Выберите фонды» вместо пустой/сломанной гистограммы. */
    noFundsSelected?: boolean;
    animatedBarsIn: number[];
    animatedBarsOut: number[];
    flowNavRange: [number, number];
    hoveredFlowIndex: number | null;
    loading: boolean;
    flowContainerRef: React.RefObject<HTMLDivElement | null>;
    flowChartRef: React.RefObject<SVGSVGElement | null>;
    /** Pos через state — рендерится атомарно с tooltip'ом через ChartTooltip. */
    flowTooltipPos: { x: number; y: number } | null;
    onMouseMove: (e: React.MouseEvent<HTMLDivElement>) => void;
    onMouseLeave: () => void;
    onSetFlowNavRange: React.Dispatch<React.SetStateAction<[number, number]>>;
    /** Обобщающий заголовок графика (например «Чистые притоки и оттоки из фондов
     *  облигаций (млрд ₽)»). Заменяет прежнюю двухпунктовую легенду приток/отток.
     *  Если не задан — default «Чистые притоки и оттоки (млрд ₽)». */
    flowTitle?: string;
}

export default function FlowsHistogram({
    flowsData,
    noFundsSelected = false,
    animatedBarsIn,
    animatedBarsOut,
    flowNavRange,
    hoveredFlowIndex,
    loading,
    flowContainerRef,
    flowChartRef,
    flowTooltipPos,
    onMouseMove,
    onMouseLeave,
    onSetFlowNavRange,
    flowTitle = 'Чистые притоки и оттоки (млрд ₽)',
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
                <div className="absolute top-4 left-4 z-20 flex items-center gap-2 rounded-lg border border-theme shadow-md" style={{ background: 'var(--bg-primary)', padding: 'var(--sp-2) var(--sp-3)' }}>
                    <div className="w-4 h-4 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
                    <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-xs)' }}>Обновление...</span>
                </div>
            )}
            {/* Гистограмма притоков/оттоков */}
            <div>
                {/* Заголовок графика — одно обобщающее название вместо двухпунктовой
                    легенды. Рендерим тем же ChartLegend (SVG-text), что и легенда СЧА,
                    чтобы шрифт и жирность совпадали 1-в-1; marker 'none' — без кружка.
                    Negative margin-top компенсирует p-5 (20px) карточки — легенда
                    на --chart-legend-top-gap от верхней границы, как в SimpleChart. */}
                <div style={{ marginTop: 'calc(var(--chart-legend-top-gap, 8px) - 20px)', marginBottom: 'var(--chart-legend-mb, 16px)' }}>
                    <ChartLegend
                        items={[{ color: 'transparent', label: flowTitle, marker: 'none' }]}
                        fontWeight={600}
                        itemGap={6}
                        gap="clamp(6px, 1vw, 16px)"
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
                            // Окантовка баров. На длинных таймфреймах (2г/«всё»)
                            // видимых баров — сотни. SVG-stroke + vectorEffect=
                            // non-scaling-stroke на каждом баре делает repaint
                            // дорогим, и при hover (re-render всех баров с новой
                            // opacity) график «виснет». Поэтому при >50 баров
                            // окантовку выключаем полностью (она там и так была
                            // лишь 0.25-0.4px «хинтом») — это возвращает прежний
                            // быстрый рендер. ≤50 баров — полноценная окантовка,
                            // non-scaling-stroke фиксирует px при resize.
                            const showOutline = visibleIn.length <= 50;
                            const outlineWidth = visibleIn.length <= 20 ? 1 : 0.7;
                            const strokeProps: React.SVGProps<SVGRectElement> = showOutline
                                ? {
                                      stroke: 'var(--bar-outline)',
                                      strokeWidth: outlineWidth,
                                      vectorEffect: 'non-scaling-stroke',
                                  }
                                : {};

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
                                                {...strokeProps}
                                                rx="2" />
                                        )}
                                        {hOut > 0 && (
                                            <rect x={x} y={`${midY}%`} width={w} height={`${hOut}%`}
                                                fill={'var(--funds-flow-negative)'}
                                                {...strokeProps}
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
                            <ChartTooltip x={flowTooltipPos.x} y={flowTooltipPos.y} clampTop={cssVar('--chart-pad-top', 14)} clampBottom={cssVar('--chart-pad-bottom', 50)}>
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

                </div>

                {/* Navigator — тот же ChartNavigator, что во всех SimpleChart-графиках.
                    previewMode=line → rail-таймлайн (тонкая линия + круглые ручки),
                    единый вид со всеми остальными графиками.
                    Скрыт в html2canvas snapshot через data-export-ignore. */}
                {navigatorData.length > 1 && (
                    <div data-export-ignore="true">
                        <ChartNavigator
                            data={navigatorData}
                            color="var(--accent)"
                            previewMode="line"
                            onChange={(s, e) => onSetFlowNavRange([s, e])}
                            insetLeft="var(--chart-pad-left)"
                            insetRight="var(--chart-pad-right-single)"
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
