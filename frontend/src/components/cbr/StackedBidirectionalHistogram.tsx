/**
 * StackedBidirectionalHistogram — стек-бар с двусторонним накоплением.
 *
 * Для каждого периода (вертикальной колонки):
 *   • Категории с value > 0 накапливаются ВВЕРХ от 0
 *   • Категории с value < 0 накапливаются ВНИЗ от 0
 *
 * Используется для ОРФР Банка России — потоки участников на бирже.
 * Источник вдохновения: график ЦБ «Среднемесячные покупки/продажи».
 *
 * Особенности:
 *   • Между годами рисуется вертикальная пунктирная линия и подпись года
 *   • Symmetric Y-axis (max = abs(max) + 10% запас, округлено до 10)
 *   • strokeWidth между сегментами dynamic (как в FlowsHistogram)
 *   • Tooltip с разбивкой по 7 категориям + total
 *   • Без анимации mount (instant render — по решению заказчика)
 *   • Дружит с html2canvas (никаких CSS transforms, всё SVG)
 */

import { useCallback, useMemo, useRef, useState } from 'react';
import type { CbrFlowsPeriod } from '../../services/api';
import { getCategoryColor } from './cbrPalette';
import { useTheme } from '../../contexts/ThemeContext';
import { useViewportWidth } from '../../hooks/useViewportWidth';
import { axisFontSize } from '../chart/chartTypography';
import ChartLegend, { type ChartLegendItem } from '../chart/ChartLegend';
import ChartWatermark from '../ChartWatermark';

interface Props {
  periods: CbrFlowsPeriod[];
  categories: string[];   // в порядке стека (снизу вверх для positive, сверху вниз для negative)
  unit: string;           // 'млрд руб.'
  height: number;
  loading?: boolean;
}

interface HoverState {
  periodIdx: number;
  category?: string;  // если hover на конкретный сегмент
  mouseX: number;
  mouseY: number;
}

interface BarSegment {
  category: string;
  value: number;
  y: number;        // верх прямоугольника
  height: number;   // абсолютная высота
  isPositive: boolean;
}

interface PeriodLayout {
  index: number;
  period: CbrFlowsPeriod;
  x: number;            // левый край bar slot'а
  centerX: number;
  barX: number;         // левый край самого бара (с padding'ом)
  barW: number;
  segments: BarSegment[];
  positiveTotal: number;
  negativeTotal: number;
}

const ROW_GAP_X = 0.35;          // 35% gap между барами (slot - 2×pad = bar)
const Y_AXIS_PAD_TOP = 14;       // px над верхом chart
const Y_AXIS_PAD_BOTTOM = 56;    // место под labels периодов + год
const X_AXIS_PAD_LEFT = 64;      // место под y-axis ticks
const X_AXIS_PAD_RIGHT = 16;
const MIN_BAR_PX = 0.5;          // минимальная высота сегмента в px

export default function StackedBidirectionalHistogram({
  periods,
  categories,
  unit,
  height,
  loading,
}: Props) {
  const { theme } = useTheme();
  const containerRef = useRef<HTMLDivElement>(null);
  const [hover, setHover] = useState<HoverState | null>(null);
  const vw = useViewportWidth();
  const axisFs = axisFontSize(vw);

  // ─── Вычисляем layout ──────────────────────────────────────────────────
  const layout = useMemo(() => {
    if (!periods.length) return null;

    // Y-axis range — симметрично, по абсолютному максимуму sum positive/negative
    let maxAbs = 0;
    for (const p of periods) {
      let pos = 0, neg = 0;
      for (const cat of categories) {
        const v = p.values[cat] ?? 0;
        if (v >= 0) pos += v;
        else neg += -v;
      }
      maxAbs = Math.max(maxAbs, pos, neg);
    }
    const yMax = Math.max(10, Math.ceil((maxAbs * 1.12) / 10) * 10);

    return { yMax };
  }, [periods, categories]);

  const periodLayouts = useMemo<PeriodLayout[]>(() => {
    if (!periods.length || !layout) return [];

    // chartWidth/Height — будут масштабироваться viewBox, считаем в условных px=1000
    const VIEW_W = 1000;
    const chartW = VIEW_W - X_AXIS_PAD_LEFT - X_AXIS_PAD_RIGHT;
    const chartH = height - Y_AXIS_PAD_TOP - Y_AXIS_PAD_BOTTOM;
    const halfH = chartH / 2;
    const zeroY = Y_AXIS_PAD_TOP + halfH;

    const slotW = chartW / periods.length;
    const barW = slotW * (1 - ROW_GAP_X);
    const padX = (slotW - barW) / 2;

    const scaleY = (val: number) => (Math.abs(val) / layout.yMax) * halfH;

    return periods.map((p, i) => {
      const x = X_AXIS_PAD_LEFT + i * slotW;
      const barX = x + padX;
      const centerX = x + slotW / 2;

      // Накапливаем segments
      let stackUp = 0, stackDown = 0;
      const segments: BarSegment[] = [];
      let positiveTotal = 0, negativeTotal = 0;
      for (const cat of categories) {
        const v = p.values[cat] ?? 0;
        if (v === 0) continue;
        const h = Math.max(scaleY(v), MIN_BAR_PX);
        if (v > 0) {
          segments.push({
            category: cat,
            value: v,
            y: zeroY - scaleY(stackUp) - h,
            height: h,
            isPositive: true,
          });
          stackUp += v;
          positiveTotal += v;
        } else {
          segments.push({
            category: cat,
            value: v,
            y: zeroY + scaleY(stackDown),
            height: h,
            isPositive: false,
          });
          stackDown += -v;
          negativeTotal += v;
        }
      }
      return { index: i, period: p, x, centerX, barX, barW, segments, positiveTotal, negativeTotal };
    });
  }, [periods, categories, layout, height]);

  // Y-axis ticks
  const yTicks = useMemo(() => {
    if (!layout) return [];
    const step = layout.yMax >= 80 ? 20 : layout.yMax >= 40 ? 10 : layout.yMax >= 20 ? 5 : 2;
    const ticks: number[] = [];
    for (let v = -layout.yMax; v <= layout.yMax; v += step) {
      ticks.push(v);
    }
    return ticks;
  }, [layout]);

  // Год-сепараторы — между периодами с разным годом
  const yearSeparators = useMemo(() => {
    const seps: { x: number; year: number; firstIdx: number; lastIdx: number }[] = [];
    if (!periodLayouts.length) return seps;
    let blockStart = 0;
    let currentYear = periodLayouts[0].period.year;
    for (let i = 1; i <= periodLayouts.length; i++) {
      const yearChanged = i === periodLayouts.length || periodLayouts[i].period.year !== currentYear;
      if (yearChanged) {
        seps.push({
          x: i < periodLayouts.length
            ? (periodLayouts[i - 1].centerX + periodLayouts[i].centerX) / 2
            : periodLayouts[periodLayouts.length - 1].centerX,
          year: currentYear,
          firstIdx: blockStart,
          lastIdx: i - 1,
        });
        blockStart = i;
        if (i < periodLayouts.length) currentYear = periodLayouts[i].period.year;
      }
    }
    return seps;
  }, [periodLayouts]);

  const legendItems = useMemo<ChartLegendItem[]>(
    () => categories.map((cat) => ({ color: getCategoryColor(cat, theme), label: cat })),
    [categories, theme],
  );

  // ─── Hover handler ───────────────────────────────────────────────────────
  const handleMove = useCallback((e: React.MouseEvent<SVGSVGElement>) => {
    const svg = e.currentTarget;
    const rect = svg.getBoundingClientRect();
    const xRatio = (e.clientX - rect.left) / rect.width;
    const xView = xRatio * 1000;
    if (!periodLayouts.length) return;
    // Найти ближайший период
    let nearestIdx = 0;
    let nearestDist = Infinity;
    periodLayouts.forEach((pl, i) => {
      const d = Math.abs(pl.centerX - xView);
      if (d < nearestDist) { nearestDist = d; nearestIdx = i; }
    });
    setHover({
      periodIdx: nearestIdx,
      mouseX: e.clientX - rect.left,
      mouseY: e.clientY - rect.top,
    });
  }, [periodLayouts, height]);

  const handleLeave = useCallback(() => setHover(null), []);

  // ─── States ──────────────────────────────────────────────────────────────
  if (loading) {
    return (
      <div
        className="rounded-2xl"
        style={{
          height: `${height}px`,
          background: 'var(--bg-secondary)',
          opacity: 0.6,
        }}
      />
    );
  }
  if (!periods.length || !layout) {
    return (
      <div
        className="flex items-center justify-center rounded-2xl"
        style={{
          height: `${height}px`,
          color: 'var(--text-muted)',
          fontSize: 'var(--fs-sm)',
        }}
      >
        Нет данных
      </div>
    );
  }

  const zeroY = Y_AXIS_PAD_TOP + (height - Y_AXIS_PAD_TOP - Y_AXIS_PAD_BOTTOM) / 2;
  const halfH = (height - Y_AXIS_PAD_TOP - Y_AXIS_PAD_BOTTOM) / 2;

  // ─── Dynamic strokeWidth (внутренние границы между сегментами) ─────────
  const outlineWidth = periodLayouts.length <= 20 ? 1
    : periodLayouts.length <= 50 ? 0.7
    : periodLayouts.length <= 100 ? 0.4
    : 0.25;

  return (
    <div ref={containerRef} className="relative" style={{ height: `${height}px` }}>
      {/* Legend сверху */}
      <div className="absolute top-0 left-0 right-0 z-10" style={{ pointerEvents: 'none' }}>
        <ChartLegend items={legendItems} />
      </div>

      <svg
        width="100%"
        height={height}
        viewBox={`0 0 1000 ${height}`}
        preserveAspectRatio="none"
        style={{ overflow: 'visible', display: 'block' }}
        onMouseMove={handleMove}
        onMouseLeave={handleLeave}
      >
        {/* Y-axis grid + labels */}
        {yTicks.map((v) => {
          const y = zeroY - (v / layout.yMax) * halfH;
          const isZero = v === 0;
          return (
            <g key={v}>
              <line
                x1={X_AXIS_PAD_LEFT} x2={1000 - X_AXIS_PAD_RIGHT}
                y1={y} y2={y}
                stroke={isZero ? 'var(--text-primary)' : 'var(--text-muted)'}
                strokeWidth={isZero ? 1.5 : 1}
                strokeDasharray={isZero ? undefined : '2 4'}
                opacity={isZero ? 0.55 : 0.3}
                vectorEffect="non-scaling-stroke"
              />
              <text
                x={X_AXIS_PAD_LEFT - 8}
                y={y}
                textAnchor="end"
                dominantBaseline="central"
                fontSize={axisFs}
                fontWeight={700}
                fill="var(--axis-color, var(--text-primary))"
                style={{ fontVariantNumeric: 'tabular-nums' }}
              >
                {v}
              </text>
            </g>
          );
        })}

        {/* Unit hint вверху слева */}
        <text
          x={X_AXIS_PAD_LEFT - 8}
          y={Y_AXIS_PAD_TOP - 4}
          textAnchor="end"
          fontSize={axisFs * 0.85}
          fontWeight={600}
          opacity={0.7}
          fill="var(--text-secondary)"
        >
          {unit}
        </text>

        {/* Год-сепараторы (вертикальные пунктиры) */}
        {yearSeparators.slice(0, -1).map((s, i) => (
          <line
            key={`sep-${i}`}
            x1={s.x} x2={s.x}
            y1={Y_AXIS_PAD_TOP} y2={height - Y_AXIS_PAD_BOTTOM}
            stroke="var(--text-muted)"
            strokeWidth={1}
            strokeDasharray="3 4"
            opacity={0.35}
            vectorEffect="non-scaling-stroke"
          />
        ))}

        {/* Bars */}
        {periodLayouts.map((pl) => {
          const isHovered = hover?.periodIdx === pl.index;
          return (
            <g key={pl.index} opacity={hover && !isHovered ? 0.5 : 1} style={{ transition: 'opacity 120ms' }}>
              {pl.segments.map((seg) => (
                <rect
                  key={`${pl.index}-${seg.category}`}
                  x={pl.barX}
                  y={seg.y}
                  width={pl.barW}
                  height={seg.height}
                  fill={getCategoryColor(seg.category, theme)}
                  stroke="var(--bar-outline)"
                  strokeWidth={outlineWidth}
                  vectorEffect="non-scaling-stroke"
                />
              ))}
            </g>
          );
        })}

        {/* X-axis labels — период (квартал/месяц) */}
        {periodLayouts.map((pl) => {
          const labelText = pl.period.kind === 'quarter'
            ? pl.period.label.replace(' квартал', 'к')  // 'I квартал' → 'Iк'
            : pl.period.label.slice(0, 3);  // 'Апрель' → 'Апр'
          return (
            <text
              key={`lab-${pl.index}`}
              x={pl.centerX}
              y={height - Y_AXIS_PAD_BOTTOM + 18}
              textAnchor="middle"
              fontSize={axisFs}
              fontWeight={hover?.periodIdx === pl.index ? 700 : 500}
              fill="var(--text-primary)"
              opacity={hover && hover.periodIdx !== pl.index ? 0.6 : 1}
              style={{ transition: 'opacity 120ms' }}
            >
              {labelText}
            </text>
          );
        })}

        {/* Год — подпись под labels, центрирована между firstIdx и lastIdx */}
        {yearSeparators.map((s) => {
          const startX = periodLayouts[s.firstIdx]?.centerX ?? 0;
          const endX = periodLayouts[s.lastIdx]?.centerX ?? 0;
          const midX = (startX + endX) / 2;
          return (
            <text
              key={`year-${s.year}`}
              x={midX}
              y={height - Y_AXIS_PAD_BOTTOM + 40}
              textAnchor="middle"
              fontSize={axisFs * 0.95}
              fontWeight={700}
              fill="var(--text-secondary)"
              opacity={0.85}
            >
              {s.year}
            </text>
          );
        })}

        {/* Crosshair при hover */}
        {hover && periodLayouts[hover.periodIdx] && (
          <line
            x1={periodLayouts[hover.periodIdx].centerX}
            x2={periodLayouts[hover.periodIdx].centerX}
            y1={Y_AXIS_PAD_TOP}
            y2={height - Y_AXIS_PAD_BOTTOM}
            stroke="var(--text-primary)"
            strokeWidth={1}
            opacity={0.25}
            strokeDasharray="2 3"
            vectorEffect="non-scaling-stroke"
            pointerEvents="none"
          />
        )}
      </svg>

      {/* Watermark inside chart-area */}
      <div
        style={{
          position: 'absolute',
          left: `${(X_AXIS_PAD_LEFT / 1000) * 100}%`,
          bottom: `${Y_AXIS_PAD_BOTTOM + 8}px`,
          pointerEvents: 'none',
          opacity: 0.3,
        }}
      >
        <ChartWatermark />
      </div>

      {/* Tooltip */}
      {hover && periodLayouts[hover.periodIdx] && (() => {
        const pl = periodLayouts[hover.periodIdx];
        const p = pl.period;
        const sortedPositive = pl.segments.filter(s => s.isPositive).sort((a, b) => b.value - a.value);
        const sortedNegative = pl.segments.filter(s => !s.isPositive).sort((a, b) => a.value - b.value);
        const total = pl.positiveTotal + pl.negativeTotal;

        // Position: справа или слева от мыши в зависимости от позиции
        const containerW = containerRef.current?.clientWidth ?? 800;
        const tooltipW = 260;
        const placeLeft = hover.mouseX > containerW - tooltipW - 24;
        const left = placeLeft ? hover.mouseX - tooltipW - 16 : hover.mouseX + 16;
        const top = Math.max(8, Math.min(hover.mouseY - 20, height - 240));

        return (
          <div
            data-export-ignore="true"
            className="absolute z-20 rounded-xl"
            style={{
              left: `${left}px`,
              top: `${top}px`,
              width: `${tooltipW}px`,
              background: 'var(--bg-theme-primary)',
              border: '1.5px solid var(--text-primary)',
              boxShadow: 'var(--shadow-hard-card)',
              padding: 'var(--sp-3)',
              pointerEvents: 'none',
              fontSize: 'var(--fs-xs)',
              color: 'var(--text-primary)',
            }}
          >
            <div className="font-bold" style={{ fontSize: 'var(--fs-sm)', marginBottom: 'var(--sp-2)' }}>
              {p.label} {p.year}
            </div>
            {sortedPositive.map((s) => (
              <div key={s.category} className="flex items-center justify-between" style={{ gap: 'var(--sp-2)', marginBottom: 2 }}>
                <span className="flex items-center min-w-0" style={{ gap: 'var(--sp-1)' }}>
                  <span style={{
                    display: 'inline-block', width: 8, height: 8, borderRadius: '50%',
                    background: getCategoryColor(s.category, theme), flexShrink: 0,
                  }} />
                  <span className="truncate">{s.category}</span>
                </span>
                <span className="font-semibold whitespace-nowrap" style={{ color: 'var(--funds-flow-positive)' }}>
                  +{s.value.toFixed(2)}
                </span>
              </div>
            ))}
            {sortedPositive.length > 0 && sortedNegative.length > 0 && (
              <div style={{ height: 1, background: 'var(--text-muted)', opacity: 0.3, margin: 'var(--sp-2) 0' }} />
            )}
            {sortedNegative.map((s) => (
              <div key={s.category} className="flex items-center justify-between" style={{ gap: 'var(--sp-2)', marginBottom: 2 }}>
                <span className="flex items-center min-w-0" style={{ gap: 'var(--sp-1)' }}>
                  <span style={{
                    display: 'inline-block', width: 8, height: 8, borderRadius: '50%',
                    background: getCategoryColor(s.category, theme), flexShrink: 0,
                  }} />
                  <span className="truncate">{s.category}</span>
                </span>
                <span className="font-semibold whitespace-nowrap" style={{ color: 'var(--funds-flow-negative)' }}>
                  {s.value.toFixed(2)}
                </span>
              </div>
            ))}
            <div style={{ height: 1.5, background: 'var(--text-primary)', opacity: 0.4, margin: 'var(--sp-2) 0' }} />
            <div className="flex items-center justify-between font-bold">
              <span>Итого нетто</span>
              <span style={{ color: total >= 0 ? 'var(--funds-flow-positive)' : 'var(--funds-flow-negative)' }}>
                {total >= 0 ? '+' : ''}{total.toFixed(2)} {unit.replace('млрд руб.', 'млрд')}
              </span>
            </div>
          </div>
        );
      })()}
    </div>
  );
}
