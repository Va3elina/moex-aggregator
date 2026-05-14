/**
 * StackedBidirectionalHistogram — стек-бар с двусторонним накоплением.
 *
 * Для каждого периода:
 *   • Категории с value > 0 накапливаются ВВЕРХ от 0
 *   • Категории с value < 0 накапливаются ВНИЗ от 0
 *
 * Архитектура — pattern FlowsHistogram (Притоки/Оттоки):
 *   • Chart-area внутри absolute div с CSS padding (top/bottom/left/right)
 *   • SVG width=100% height=100% preserveAspectRatio="none"
 *   • Bars в процентах от SVG (0-100%)
 *   • Y-axis labels — HTML absolute div справа
 *   • X-axis period labels — HTML absolute div снизу
 *
 * Это даёт chart-area реально расширяющийся до padding'ов, без issue
 * с aspect ratio viewBox vs container, которое раньше центрировало content.
 */

import { useCallback, useMemo, useRef, useState } from 'react';
import type { CbrFlowsPeriod } from '../../services/api';
import { getCategoryColor } from './cbrPalette';
import { useTheme } from '../../contexts/ThemeContext';
import { useViewportWidth } from '../../hooks/useViewportWidth';
import { axisFontSize } from '../chart/chartTypography';
import ChartLegend, { type ChartLegendItem } from '../chart/ChartLegend';
import ChartWatermark from '../ChartWatermark';
import { TOOLTIP } from '../../config/chartTheme';

interface Props {
  periods: CbrFlowsPeriod[];
  categories: string[];
  unit: string;
  height: number;
  loading?: boolean;
}

interface HoverState {
  periodIdx: number;
  mouseX: number;
  mouseY: number;
}

// CSS padding chart-area внутри outer container.
// LEFT = 16 (минимум visual breathing, axis labels справа)
// RIGHT = 64 (для axis labels "60 млрд")
// TOP = 20 (зазор от верха chart до первого tick "+60")
// BOTTOM = 64 (под labels периодов "Iк" + год "2024")
const PAD_TOP = 20;
const PAD_BOTTOM = 64;
const PAD_LEFT = 16;
const PAD_RIGHT = 84;   // 64 → 84 — для «60 млрд» (main + suffix + breath room)
const MIN_BAR_H = 0.6;  // % минимум высоты сегмента

export default function StackedBidirectionalHistogram({
  periods,
  categories,
  height,
  loading,
}: Props) {
  const { theme } = useTheme();
  const containerRef = useRef<HTMLDivElement>(null);
  const [hover, setHover] = useState<HoverState | null>(null);
  const vw = useViewportWidth();
  const axisFs = axisFontSize(vw);

  // Y-axis симметричный max
  const yMax = useMemo(() => {
    if (!periods.length) return 10;
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
    return Math.max(10, Math.ceil((maxAbs * 1.12) / 10) * 10);
  }, [periods, categories]);

  // 5 уровней Y-axis: [-max, -max/2, 0, max/2, max]
  const yTicks = useMemo(
    () => [-yMax, -yMax / 2, 0, yMax / 2, yMax],
    [yMax],
  );

  // Год-сепараторы — между периодами с разным годом.
  const yearBlocks = useMemo(() => {
    const blocks: { year: number; firstIdx: number; lastIdx: number }[] = [];
    if (!periods.length) return blocks;
    let blockStart = 0;
    let currentYear = periods[0].year;
    for (let i = 1; i <= periods.length; i++) {
      const yearChanged = i === periods.length || periods[i].year !== currentYear;
      if (yearChanged) {
        blocks.push({ year: currentYear, firstIdx: blockStart, lastIdx: i - 1 });
        blockStart = i;
        if (i < periods.length) currentYear = periods[i].year;
      }
    }
    return blocks;
  }, [periods]);

  const legendItems = useMemo<ChartLegendItem[]>(
    () => categories.map((cat) => ({ color: getCategoryColor(cat, theme), label: cat })),
    [categories, theme],
  );

  // ─── Hover handler ───────────────────────────────────────────────────────
  const handleMove = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
    const target = e.currentTarget;
    const rect = target.getBoundingClientRect();
    const xRatio = (e.clientX - rect.left) / rect.width;
    if (!periods.length || xRatio < 0 || xRatio > 1) {
      setHover(null);
      return;
    }
    const nearestIdx = Math.min(periods.length - 1, Math.max(0, Math.floor(xRatio * periods.length)));
    const outer = containerRef.current;
    const outerRect = outer ? outer.getBoundingClientRect() : rect;
    setHover({
      periodIdx: nearestIdx,
      mouseX: e.clientX - outerRect.left,
      mouseY: e.clientY - outerRect.top,
    });
  }, [periods.length]);

  const handleLeave = useCallback(() => setHover(null), []);

  // ─── States ──────────────────────────────────────────────────────────────
  if (loading) {
    return (
      <div className="rounded-2xl" style={{ height: `${height}px`, background: 'var(--bg-secondary)', opacity: 0.6 }} />
    );
  }
  if (!periods.length) {
    return (
      <div className="flex items-center justify-center"
        style={{ height: `${height}px`, color: 'var(--text-muted)', fontSize: 'var(--fs-sm)' }}>
        Нет данных
      </div>
    );
  }

  // Динамическая толщина окантовки
  const outlineWidth = periods.length <= 20 ? 1
    : periods.length <= 50 ? 0.7
    : periods.length <= 100 ? 0.4
    : 0.25;

  const barSlot = 100 / periods.length;  // % одного period-слота
  const barW = barSlot * 0.65;            // 65% слота — сам бар
  const barOffset = (barSlot - barW) / 2;

  return (
    <div
      ref={containerRef}
      className="relative flex flex-col"
      style={{ height: `${height}px` }}
    >
      {/* ═══ Зона легенды — auto height, не сжимается при wrap (7 категорий
              могут перейти на 2 строки на узком viewport). flex-shrink: 0
              чтобы legend никогда не уходила за верхний край. ═══ */}
      <div
        style={{
          flexShrink: 0,
          paddingTop: 'var(--sp-2)',
          paddingBottom: 'var(--sp-2)',
        }}
      >
        <ChartLegend items={legendItems} />
      </div>

      {/* ═══ Контейнер chart — flex: 1 (берёт оставшуюся высоту) ═══ */}
      <div
        className="relative"
        style={{ flex: 1, minHeight: 0 }}
        onMouseMove={handleMove}
        onMouseLeave={handleLeave}
      >
        {/* === Y-axis labels (HTML absolute справа) === */}
        <div
          className="absolute"
          style={{
            top: `${PAD_TOP}px`,
            bottom: `${PAD_BOTTOM}px`,
            right: 0,
            width: `${PAD_RIGHT}px`,
            pointerEvents: 'none',
          }}
        >
          {yTicks.map((v) => {
            const yPct = 50 - (v / yMax) * 50;  // 0% top → 100% bottom
            return (
              <div
                key={v}
                className="absolute"
                style={{
                  top: `${yPct}%`,
                  left: 6,
                  right: 0,
                  transform: 'translateY(-50%)',
                  fontSize: `${axisFs}px`,
                  fontWeight: 700,
                  color: 'var(--axis-color, var(--text-primary))',
                  fontVariantNumeric: 'tabular-nums',
                  whiteSpace: 'nowrap',
                }}
              >
                {Math.round(v)}
                <span style={{ fontSize: `${axisFs * 0.7}px`, fontWeight: 700, opacity: 0.85, marginLeft: 3 }}>
                  млрд
                </span>
              </div>
            );
          })}
        </div>

        {/* === Chart-area (SVG inside absolute div) === */}
        <div
          className="absolute"
          style={{
            top: `${PAD_TOP}px`,
            bottom: `${PAD_BOTTOM}px`,
            left: `${PAD_LEFT}px`,
            right: `${PAD_RIGHT}px`,
          }}
        >
          <svg
            width="100%"
            height="100%"
            preserveAspectRatio="none"
            style={{ overflow: 'visible', display: 'block' }}
          >
            {/* Горизонтальные grid линии (solid, 5 уровней) */}
            {yTicks.map((v) => {
              const yPct = 50 - (v / yMax) * 50;
              const isZero = v === 0;
              return (
                <line
                  key={v}
                  x1="0" x2="100%"
                  y1={`${yPct}%`} y2={`${yPct}%`}
                  stroke={isZero ? 'var(--text-primary)' : 'var(--text-muted)'}
                  strokeWidth={isZero ? 1.5 : 1}
                  opacity={isZero ? 0.55 : 0.3}
                  vectorEffect="non-scaling-stroke"
                />
              );
            })}

            {/* Год-сепараторы (solid тонкие линии между годами) */}
            {yearBlocks.slice(0, -1).map((block, i) => {
              // Сепаратор между текущим блоком и следующим — посередине между
              // последним baromof current и первым of next.
              const x = (block.lastIdx + 1) * barSlot;
              return (
                <line
                  key={`year-sep-${i}`}
                  x1={`${x}%`} x2={`${x}%`}
                  y1="0" y2="100%"
                  stroke="var(--text-muted)"
                  strokeWidth={1}
                  opacity={0.4}
                  vectorEffect="non-scaling-stroke"
                />
              );
            })}

            {/* Bars (накопленные стеки) */}
            {periods.map((p, i) => {
              const isHovered = hover?.periodIdx === i;
              const opacity = hover && !isHovered ? 0.5 : 1;
              let stackUp = 0;
              let stackDown = 0;
              const x = i * barSlot + barOffset;
              return (
                <g key={i} opacity={opacity} style={{ transition: 'opacity 120ms' }}>
                  {categories.map((cat) => {
                    const v = p.values[cat] ?? 0;
                    if (v === 0) return null;
                    const hPct = Math.max((Math.abs(v) / yMax) * 50, MIN_BAR_H);
                    if (v > 0) {
                      const stackUpPct = (stackUp / yMax) * 50;
                      stackUp += v;
                      return (
                        <rect
                          key={`${i}-${cat}`}
                          x={`${x}%`}
                          y={`${50 - stackUpPct - hPct}%`}
                          width={`${barW}%`}
                          height={`${hPct}%`}
                          fill={getCategoryColor(cat, theme)}
                          stroke="var(--bar-outline)" strokeWidth={outlineWidth}
                          vectorEffect="non-scaling-stroke"
                        />
                      );
                    } else {
                      const stackDownPct = (stackDown / yMax) * 50;
                      stackDown += -v;
                      return (
                        <rect
                          key={`${i}-${cat}`}
                          x={`${x}%`}
                          y={`${50 + stackDownPct}%`}
                          width={`${barW}%`}
                          height={`${hPct}%`}
                          fill={getCategoryColor(cat, theme)}
                          stroke="var(--bar-outline)" strokeWidth={outlineWidth}
                          vectorEffect="non-scaling-stroke"
                        />
                      );
                    }
                  })}
                </g>
              );
            })}

            {/* Crosshair при hover */}
            {hover && periods[hover.periodIdx] && (
              <line
                x1={`${hover.periodIdx * barSlot + barSlot / 2}%`}
                x2={`${hover.periodIdx * barSlot + barSlot / 2}%`}
                y1="0" y2="100%"
                stroke="var(--text-primary)"
                strokeWidth={1}
                opacity={0.25}
                strokeDasharray="2 3"
                vectorEffect="non-scaling-stroke"
                pointerEvents="none"
              />
            )}
          </svg>
        </div>

        {/* === X-axis labels (период) — HTML absolute снизу === */}
        <div
          className="absolute"
          style={{
            left: `${PAD_LEFT}px`,
            right: `${PAD_RIGHT}px`,
            bottom: `${PAD_BOTTOM - 22}px`,
            height: '20px',
            pointerEvents: 'none',
          }}
        >
          {periods.map((p, i) => {
            const labelText = p.kind === 'quarter'
              ? p.label.replace(' квартал', 'к')
              : p.label.slice(0, 3);
            const centerPct = i * barSlot + barSlot / 2;
            const isHovered = hover?.periodIdx === i;
            return (
              <div
                key={`xlab-${i}`}
                className="absolute"
                style={{
                  left: `${centerPct}%`,
                  transform: 'translateX(-50%)',
                  fontSize: `${axisFs}px`,
                  fontWeight: isHovered ? 700 : 500,
                  color: 'var(--text-primary)',
                  opacity: hover && !isHovered ? 0.6 : 1,
                  transition: 'opacity 120ms',
                  whiteSpace: 'nowrap',
                }}
              >
                {labelText}
              </div>
            );
          })}
        </div>

        {/* === Год labels — HTML absolute ниже labels периодов === */}
        <div
          className="absolute"
          style={{
            left: `${PAD_LEFT}px`,
            right: `${PAD_RIGHT}px`,
            bottom: `${PAD_BOTTOM - 48}px`,
            height: '20px',
            pointerEvents: 'none',
          }}
        >
          {yearBlocks.map((block) => {
            const startCenterPct = block.firstIdx * barSlot + barSlot / 2;
            const endCenterPct = block.lastIdx * barSlot + barSlot / 2;
            const midPct = (startCenterPct + endCenterPct) / 2;
            return (
              <div
                key={`year-${block.year}`}
                className="absolute"
                style={{
                  left: `${midPct}%`,
                  transform: 'translateX(-50%)',
                  fontSize: `${axisFs * 0.95}px`,
                  fontWeight: 700,
                  color: 'var(--text-primary)',
                  whiteSpace: 'nowrap',
                }}
              >
                {block.year}
              </div>
            );
          })}
        </div>

        {/* === Watermark === */}
        <div
          style={{
            position: 'absolute',
            left: `${PAD_LEFT + 4}px`,
            bottom: `${PAD_BOTTOM + 4}px`,
            pointerEvents: 'none',
          }}
        >
          <ChartWatermark />
        </div>
      </div>

      {/* === Tooltip === */}
      {hover && periods[hover.periodIdx] && (() => {
        const p = periods[hover.periodIdx];
        const entries = categories
          .map((cat) => ({ cat, val: p.values[cat] ?? 0 }))
          .filter(e => e.val !== 0);
        const sortedPositive = entries.filter(e => e.val > 0).sort((a, b) => b.val - a.val);
        const sortedNegative = entries.filter(e => e.val < 0).sort((a, b) => a.val - b.val);
        const total = entries.reduce((s, e) => s + e.val, 0);

        const containerW = containerRef.current?.clientWidth ?? 800;
        const tooltipW = 260;
        const placeLeft = hover.mouseX > containerW - tooltipW - 24;
        const left = placeLeft ? hover.mouseX - tooltipW - 16 : hover.mouseX + 16;
        const top = Math.max(8, Math.min(hover.mouseY - 20, height - 240));

        return (
          <div
            data-export-ignore="true"
            className={`${TOOLTIP.containerClass} absolute z-20`}
            style={{
              ...TOOLTIP.containerStyle,
              left: `${left}px`,
              top: `${top}px`,
              width: `${tooltipW}px`,
              pointerEvents: 'none',
              whiteSpace: 'normal',
            }}
          >
            <div className={TOOLTIP.dateClass} style={{ ...TOOLTIP.dateStyle, marginBottom: 'var(--sp-1)', display: 'inline-block' }}>
              {p.label} {p.year}
            </div>
            {sortedPositive.map((e) => (
              <div key={e.cat} className="flex items-center justify-between py-0.5" style={{ gap: 'var(--sp-2)' }}>
                <div className="flex items-center min-w-0" style={{ gap: 'var(--sp-1)' }}>
                  <span className={TOOLTIP.dotClass} style={{ ...TOOLTIP.dotStyle, backgroundColor: getCategoryColor(e.cat, theme) }} />
                  <span className={`${TOOLTIP.labelClass} truncate`} style={TOOLTIP.labelStyle}>{e.cat}</span>
                </div>
                <span className={TOOLTIP.valueClass} style={{ ...TOOLTIP.valueStyle, color: 'var(--funds-flow-positive)' }}>
                  +{e.val.toFixed(2)}
                </span>
              </div>
            ))}
            {sortedPositive.length > 0 && sortedNegative.length > 0 && (
              <div style={{ height: 1, background: 'var(--text-muted)', opacity: 0.25, margin: '4px 0' }} />
            )}
            {sortedNegative.map((e) => (
              <div key={e.cat} className="flex items-center justify-between py-0.5" style={{ gap: 'var(--sp-2)' }}>
                <div className="flex items-center min-w-0" style={{ gap: 'var(--sp-1)' }}>
                  <span className={TOOLTIP.dotClass} style={{ ...TOOLTIP.dotStyle, backgroundColor: getCategoryColor(e.cat, theme) }} />
                  <span className={`${TOOLTIP.labelClass} truncate`} style={TOOLTIP.labelStyle}>{e.cat}</span>
                </div>
                <span className={TOOLTIP.valueClass} style={{ ...TOOLTIP.valueStyle, color: 'var(--funds-flow-negative)' }}>
                  {e.val.toFixed(2)}
                </span>
              </div>
            ))}
            <div style={{ height: 1, background: 'var(--text-primary)', opacity: 0.35, margin: '4px 0' }} />
            <div className="flex items-center justify-between font-bold" style={{ fontSize: 'var(--fs-xs)' }}>
              <span>Итого нетто</span>
              <span style={{ color: total >= 0 ? 'var(--funds-flow-positive)' : 'var(--funds-flow-negative)' }}>
                {total >= 0 ? '+' : ''}{total.toFixed(2)} млрд
              </span>
            </div>
          </div>
        );
      })()}
    </div>
  );
}
