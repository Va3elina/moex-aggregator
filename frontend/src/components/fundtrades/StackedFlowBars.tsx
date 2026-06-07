/**
 * StackedFlowBars — лёгкий SVG divergent-stacked bar chart во времени.
 *
 * Generic: знает только `months` (ось X) + `series` (стек значений). НЕ зависит
 * от FundsFlowsResponse / fund-trades типов — вызывающий передаёт уже готовые
 * серии (по фондам, или одну серию для summary).
 *
 * Divergent stacking: для каждого месяца положительные значения серий стекаются
 * ВВЕРХ от нуля, отрицательные — ВНИЗ. Линия нуля по центру вертикальной шкалы.
 *
 * Editorial-стиль осей/сетки повторяет funds/FlowsHistogram.tsx: горизонтальная
 * сетка через GRID (chartTheme), фон bg-theme-primary, border var(--border-color),
 * текст var(--text-muted), шрифты var(--fs-2xs)/var(--fs-xs). Тултип — shared
 * ChartTooltip + TooltipRow (paper-style).
 */
import { useMemo, useRef, useState } from 'react';
import { GRID } from '../../config/chartTheme';
import { useIsMobile } from '../../hooks/useIsMobile';
import { useGrowReveal } from '../../hooks/useGrowReveal';
import { ChartTooltip, TooltipRow } from '../chart';

export interface StackedSeries {
    label: string;
    color: string;
    values: (number | null)[];
}

interface StackedFlowBarsProps {
    /** "YYYY-MM" — ось X, выровнена с series[].values по индексу. */
    months: string[];
    /** Стек по фондам (или одна серия для summary). values выровнены по months. */
    series: StackedSeries[];
    /** Высота области графика в px. default 320. */
    height?: number;
    /** Форматер значений для тултипа/оси Y. default → "X.XX млрд ₽" (v / 1e9). */
    formatValue?: (v: number) => string;
    /** Форматер подписи месяца на оси X. default "MM.YYYY". */
    formatMonth?: (m: string) => string;
    /** Если true и серия одна — красить сегмент по знаку (зелёный + / красный −).
     *  Для «Суммарный Flow». */
    signColorForSingle?: boolean;
    /** Перезапуск entrance-волны (grow-from-zero): меняй при смене бумаги /
     *  набора фондов. См. useGrowReveal — ключ ОБЯЗАН ловить и смену count. */
    animTrigger?: string;
}

const POSITIVE_COLOR = 'var(--funds-flow-positive)';
const NEGATIVE_COLOR = 'var(--funds-flow-negative)';

function defaultFormatValue(v: number): string {
    const bln = v / 1e9;
    const abs = Math.abs(bln);
    const digits = abs >= 100 ? 0 : abs >= 1 ? 2 : 3;
    const sign = bln > 0 ? '+' : bln < 0 ? '−' : '';
    return `${sign}${Math.abs(bln).toFixed(digits)} млрд ₽`;
}

function defaultFormatMonth(m: string): string {
    // "YYYY-MM" → "MM.YYYY"
    const [y, mo] = m.split('-');
    return mo && y ? `${mo}.${y}` : m;
}

export default function StackedFlowBars({
    months,
    series,
    height = 320,
    formatValue = defaultFormatValue,
    formatMonth = defaultFormatMonth,
    signColorForSingle = false,
    animTrigger,
}: StackedFlowBarsProps) {
    const isMobile = useIsMobile();
    const containerRef = useRef<HTMLDivElement>(null);
    const plotRef = useRef<HTMLDivElement>(null);
    const [hovered, setHovered] = useState<number | null>(null);
    const [tooltipPos, setTooltipPos] = useState<{ x: number; y: number } | null>(null);

    const isSingleSigned = signColorForSingle && series.length === 1;

    // ── Entrance-волна «grow-from-zero» слева-направо (как в гистограмме ЦБ).
    //    animKey ловит и явный animTrigger, и первую загрузку ([]→данные) через
    //    длину months + крайние даты — иначе волна не сыграет при первом открытии. ──
    const animKey = `${animTrigger ?? ''}|${months.length}|${months[0] ?? ''}|${months[months.length - 1] ?? ''}`;
    const reveal = useGrowReveal(months.length, animKey);

    // ── Геометрия: для каждого месяца считаем сумму положительных и отрицательных
    //    сегментов (divergent), плюс глобальные max(pos) / max(|neg|) для шкалы. ──
    const layout = useMemo(() => {
        const n = months.length;
        // posStacks[i] / negStacks[i] = массив сегментов {value, color, label} в порядке стека
        const posStacks: { value: number; color: string; label: string }[][] = [];
        const negStacks: { value: number; color: string; label: string }[][] = [];
        let maxPos = 0;
        let maxNeg = 0; // абсолютная величина

        for (let i = 0; i < n; i++) {
            const pos: { value: number; color: string; label: string }[] = [];
            const neg: { value: number; color: string; label: string }[] = [];
            let posSum = 0;
            let negSum = 0;
            for (const s of series) {
                const raw = s.values[i];
                if (raw == null || raw === 0 || Number.isNaN(raw)) continue;
                // Цвет сегмента: для single-signed summary — по знаку, иначе series.color.
                const color = isSingleSigned
                    ? raw >= 0
                        ? POSITIVE_COLOR
                        : NEGATIVE_COLOR
                    : s.color;
                if (raw > 0) {
                    pos.push({ value: raw, color, label: s.label });
                    posSum += raw;
                } else {
                    neg.push({ value: raw, color, label: s.label });
                    negSum += Math.abs(raw);
                }
            }
            posStacks.push(pos);
            negStacks.push(neg);
            if (posSum > maxPos) maxPos = posSum;
            if (negSum > maxNeg) maxNeg = negSum;
        }

        const maxAbs = Math.max(maxPos, maxNeg, 1e-9);
        return { posStacks, negStacks, maxAbs };
    }, [months, series, isSingleSigned]);

    const hasData = useMemo(
        () => series.some(s => s.values.some(v => v != null && v !== 0 && !Number.isNaN(v))),
        [series],
    );

    // ── Геометрия SVG (проценты). 3% запас сверху/снизу — как в FlowsHistogram. ──
    const midY = 50; // нулевая линия по центру
    const halfH = 47; // полу-высота шкалы (3% запас)
    const minSegH = 0.4; // минимальная видимая высота крошечного сегмента (%)

    // Y-тики: симметрично вокруг нуля.
    const yTicks = useMemo(() => {
        const m = layout.maxAbs;
        return [m, m / 2, 0, -m / 2, -m];
    }, [layout.maxAbs]);

    const barWidthPct = 100 / Math.max(months.length, 1);

    // Прорежение подписей X: чтобы даты не накладывались.
    const xTickEvery = useMemo(() => {
        const maxTicks = isMobile ? 4 : 8;
        return Math.max(1, Math.ceil(months.length / maxTicks));
    }, [months.length, isMobile]);

    // ── Hover: определить индекс месяца по X, сохранить позицию для тултипа. ──
    const handleMove = (clientX: number, _clientY: number) => {
        const plot = plotRef.current;
        const cont = containerRef.current;
        if (!plot || !cont) return;
        const plotRect = plot.getBoundingClientRect();
        const contRect = cont.getBoundingClientRect();
        const rel = clientX - plotRect.left;
        if (rel < 0 || rel > plotRect.width || months.length === 0) {
            setHovered(null);
            setTooltipPos(null);
            return;
        }
        const idx = Math.min(months.length - 1, Math.max(0, Math.floor((rel / plotRect.width) * months.length)));
        setHovered(idx);
        // Тултип — ПО ВЕРХУ графика (не под пальцем): x совмещаем с центром столбца
        // (как пунктирный курсор), y=0 → ChartTooltip пинит карточку к верху (top≈4).
        const barCenterX = (plotRect.left - contRect.left) + ((idx + 0.5) / months.length) * plotRect.width;
        setTooltipPos({ x: barCenterX, y: 0 });
    };

    const clearHover = () => {
        setHovered(null);
        setTooltipPos(null);
    };

    // Подготовка содержимого тултипа для hovered месяца.
    const tooltipRows = useMemo(() => {
        if (hovered == null) return null;
        const rows: { color: string; label: string; value: string }[] = [];
        let sum = 0;
        for (const s of series) {
            const v = s.values[hovered];
            if (v == null || v === 0 || Number.isNaN(v)) continue;
            const color = isSingleSigned ? (v >= 0 ? POSITIVE_COLOR : NEGATIVE_COLOR) : s.color;
            rows.push({ color, label: s.label, value: formatValue(v) });
            sum += v;
        }
        return { rows, sum };
    }, [hovered, series, isSingleSigned, formatValue]);

    return (
        <div className="rounded-2xl p-5 bg-theme-primary border border-theme relative">
            {!hasData ? (
                <div
                    className="flex items-center justify-center text-center text-theme-secondary"
                    style={{ height, fontSize: 'var(--fs-sm)' }}
                >
                    Нет данных за период
                </div>
            ) : (
                <div
                    ref={containerRef}
                    className="relative"
                    style={{ touchAction: 'none' }}
                    onMouseMove={e => handleMove(e.clientX, e.clientY)}
                    onMouseLeave={clearHover}
                    onTouchStart={e => {
                        const t = e.touches[0];
                        if (t) handleMove(t.clientX, t.clientY);
                    }}
                    onTouchMove={e => {
                        const t = e.touches[0];
                        if (t) handleMove(t.clientX, t.clientY);
                    }}
                    onTouchEnd={clearHover}
                >
                    {/* Область графика: слева ось Y (подписи), снизу ось X (месяцы). */}
                    <div className="relative" style={{ height }}>
                        {/* Y-axis подписи (слева, вне плота) */}
                        <div
                            className="absolute pointer-events-none"
                            style={{ top: 0, bottom: 0, left: 0, width: 'var(--chart-pad-left, 64px)' }}
                        >
                            {yTicks.map((val, i) => {
                                const yPct = midY - (val / layout.maxAbs) * halfH;
                                const label =
                                    val === 0 ? '0' : formatValue(val).replace(/\s*млрд\s*₽\s*$/u, '');
                                return (
                                    <div
                                        key={`y-${i}`}
                                        className="absolute font-semibold tabular-nums"
                                        style={{
                                            top: `${yPct}%`,
                                            right: 8,
                                            transform: 'translateY(-50%)',
                                            color: 'var(--text-muted)',
                                            fontSize: 'var(--fs-2xs)',
                                            whiteSpace: 'nowrap',
                                        }}
                                    >
                                        {label}
                                    </div>
                                );
                            })}
                        </div>

                        {/* Плот: сетка + бары. Курсор crosshair. */}
                        <div
                            ref={plotRef}
                            className="absolute cursor-crosshair"
                            style={{ top: 0, bottom: 0, left: 'var(--chart-pad-left, 64px)', right: 'var(--sp-2)' }}
                        >
                            <svg width="100%" height="100%" preserveAspectRatio="none">
                                {/* Горизонтальная сетка + нулевая линия */}
                                {yTicks.map((val, i) => {
                                    const yPct = midY - (val / layout.maxAbs) * halfH;
                                    return (
                                        <line
                                            key={`grid-${i}`}
                                            x1="0"
                                            y1={`${yPct}%`}
                                            x2="100%"
                                            y2={`${yPct}%`}
                                            stroke={val === 0 ? GRID.zero : GRID.major}
                                            strokeWidth="1"
                                        />
                                    );
                                })}

                                {/* Divergent-stacked бары */}
                                {months.map((_, i) => {
                                    const isHovered = hovered === i;
                                    const opacity = hovered == null ? 1 : isHovered ? 1 : 0.35;
                                    const x = i * barWidthPct + barWidthPct * 0.15;
                                    const w = barWidthPct * 0.7;
                                    // Entrance-прогресс этого периода (grow-from-zero у midY).
                                    // progress=0 → бар нулевой высоты; 1 → полный. И высота,
                                    // и смещение стека масштабируются — стек остаётся корректным.
                                    const progress = reveal[i] ?? 1;

                                    // Положительный стек — растёт вверх от midY.
                                    let cumPos = 0;
                                    const posRects = layout.posStacks[i].map((seg, si) => {
                                        const segH = Math.max((seg.value / layout.maxAbs) * halfH, minSegH) * progress;
                                        const yTop = midY - cumPos - segH;
                                        cumPos += segH;
                                        return (
                                            <rect
                                                key={`p-${si}`}
                                                x={`${x}%`}
                                                y={`${yTop}%`}
                                                width={`${w}%`}
                                                height={`${segH}%`}
                                                fill={seg.color}
                                                stroke="var(--bar-outline)"
                                                strokeWidth={0.5}
                                                vectorEffect="non-scaling-stroke"
                                            />
                                        );
                                    });

                                    // Отрицательный стек — растёт вниз от midY.
                                    let cumNeg = 0;
                                    const negRects = layout.negStacks[i].map((seg, si) => {
                                        const segH = Math.max((Math.abs(seg.value) / layout.maxAbs) * halfH, minSegH) * progress;
                                        const yTop = midY + cumNeg;
                                        cumNeg += segH;
                                        return (
                                            <rect
                                                key={`n-${si}`}
                                                x={`${x}%`}
                                                y={`${yTop}%`}
                                                width={`${w}%`}
                                                height={`${segH}%`}
                                                fill={seg.color}
                                                stroke="var(--bar-outline)"
                                                strokeWidth={0.5}
                                                vectorEffect="non-scaling-stroke"
                                            />
                                        );
                                    });

                                    return (
                                        <g key={`bar-${i}`} opacity={opacity}>
                                            {posRects}
                                            {negRects}
                                        </g>
                                    );
                                })}

                                {/* Вертикальный курсор по hovered столбцу */}
                                {hovered != null && (() => {
                                    const cx = hovered * barWidthPct + barWidthPct / 2;
                                    return (
                                        <line
                                            x1={`${cx}%`}
                                            y1="3%"
                                            x2={`${cx}%`}
                                            y2="97%"
                                            stroke="var(--accent)"
                                            strokeWidth="1"
                                            strokeDasharray="4,3"
                                            opacity={0.5}
                                            style={{ pointerEvents: 'none' }}
                                        />
                                    );
                                })()}
                            </svg>
                        </div>
                    </div>

                    {/* Ось X — подписи месяцев (прорежены) */}
                    <div
                        className="relative"
                        style={{
                            marginLeft: 'var(--chart-pad-left, 64px)',
                            marginRight: 'var(--sp-2)',
                            marginTop: 'var(--sp-2)',
                            height: 16,
                        }}
                    >
                        {months.map((m, i) => {
                            if (i % xTickEvery !== 0 && i !== months.length - 1) return null;
                            const cx = i * barWidthPct + barWidthPct / 2;
                            return (
                                <span
                                    key={`x-${i}`}
                                    className="absolute font-semibold tabular-nums"
                                    style={{
                                        left: `${cx}%`,
                                        transform: 'translateX(-50%)',
                                        color: 'var(--text-muted)',
                                        fontSize: 'var(--fs-2xs)',
                                        whiteSpace: 'nowrap',
                                    }}
                                >
                                    {formatMonth(m)}
                                </span>
                            );
                        })}
                    </div>

                    {/* Тултип — разбивка серий за hovered месяц + сумма. */}
                    {hovered != null && tooltipPos && tooltipRows && tooltipRows.rows.length > 0 && (
                        <ChartTooltip x={tooltipPos.x} y={tooltipPos.y}>
                            <div
                                className="font-semibold text-theme-secondary"
                                style={{ fontSize: 'var(--fs-2xs)', marginBottom: 'var(--sp-1)' }}
                            >
                                {formatMonth(months[hovered])}
                            </div>
                            {tooltipRows.rows.map((r, i) => (
                                <TooltipRow key={i} color={r.color} label={r.label} value={r.value} />
                            ))}
                            {/* Сумма — только когда серий больше одной (для summary она избыточна). */}
                            {series.length > 1 && (
                                <div
                                    className="flex items-center"
                                    style={{
                                        gap: 'var(--sp-2)',
                                        marginTop: 'var(--sp-1)',
                                        paddingTop: 'var(--sp-1)',
                                        borderTop: '1px solid var(--border-color)',
                                    }}
                                >
                                    <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-2xs)' }}>
                                        Итого
                                    </span>
                                    <span
                                        className="font-semibold tabular-nums ml-auto pl-2"
                                        style={{
                                            fontSize: 'var(--fs-2xs)',
                                            color:
                                                tooltipRows.sum >= 0 ? POSITIVE_COLOR : NEGATIVE_COLOR,
                                        }}
                                    >
                                        {formatValue(tooltipRows.sum)}
                                    </span>
                                </div>
                            )}
                        </ChartTooltip>
                    )}
                </div>
            )}
        </div>
    );
}
