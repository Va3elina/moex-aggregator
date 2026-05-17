/**
 * MobileChart — edge-to-edge chart для мобильной версии.
 *
 * Дизайн отличается от десктопного SimpleChart:
 *   - Chart-area тянется на ВСЮ ширину frame'а
 *   - Y-axis tick labels рендерятся ВНУТРИ chart-area по краям (с полу-
 *     прозрачным фоном) вместо отдельных боковых отступов
 *   - Current value pill — на правом краю каждой линии (всегда виден)
 *   - X-axis labels снизу с минимальным паддингом
 *   - Crosshair через long-press (300ms hold) вместо hover
 *
 * Поддерживает 1 или 2 серии: primary (price) + optional secondary (OI).
 * Каждая со своей шкалой Y (dual-axis).
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import type { CSSProperties } from 'react';

export interface MobileChartSeries {
  /** Точки данных (timestamp + value). Должно быть >= 2 точек. */
  data: Array<{ time: string; value: number }>;
  /** Цвет линии (var(--*)). */
  color: string;
  /** Подпись для tooltip'а / legend'а. */
  label: string;
  /** На какой оси: 'left' (primary) или 'right' (secondary). */
  axis?: 'left' | 'right';
  /** Форматтер значений в pill / tooltip. */
  formatValue?: (v: number) => string;
}

interface MobileChartProps {
  series: MobileChartSeries[];
  /** Высота canvas в px. На мобиле обычно 240-320. */
  height?: number;
  /** Видимость X-axis date labels. */
  showXLabels?: boolean;
  /** Кастомный format для X-axis labels. По умолчанию DD.MM. */
  formatXLabel?: (time: string) => string;
  loading?: boolean;
}

const PAD_TOP = 8;
const PAD_BOTTOM = 18;
const PAD_X = 4;
const LONG_PRESS_MS = 300;

export default function MobileChart({
  series,
  height = 260,
  showXLabels = true,
  formatXLabel,
  loading = false,
}: MobileChartProps) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(360);
  const [crosshair, setCrosshair] = useState<{ idx: number; pinned: boolean } | null>(null);
  const longPressTimer = useRef<number | null>(null);

  // ResizeObserver — измеряем родительский контейнер
  useEffect(() => {
    if (!wrapRef.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) {
        const w = e.contentRect.width;
        if (w > 0) setWidth(Math.max(200, w));
      }
    });
    ro.observe(wrapRef.current);
    return () => ro.disconnect();
  }, []);

  const innerW = width - PAD_X * 2;
  const innerH = height - PAD_TOP - PAD_BOTTOM;

  // Разделяем серии по осям
  const leftSeries = useMemo(() => series.filter((s) => s.axis !== 'right'), [series]);
  const rightSeries = useMemo(() => series.filter((s) => s.axis === 'right'), [series]);
  const hasLeft = leftSeries.length > 0;
  const hasRight = rightSeries.length > 0;

  // Range fitting для каждой оси (с 5% headroom сверху-снизу)
  const fit = (data: number[]) => {
    if (data.length === 0) return { min: 0, max: 1, span: 1 };
    const min = Math.min(...data);
    const max = Math.max(...data);
    const span = max - min || 1;
    return { min: min - span * 0.05, max: max + span * 0.05, span: span * 1.1 };
  };

  const leftRange = useMemo(
    () => (hasLeft ? fit(leftSeries[0].data.map((d) => d.value)) : null),
    [hasLeft, leftSeries],
  );
  const rightRange = useMemo(
    () => (hasRight ? fit(rightSeries[0].data.map((d) => d.value)) : null),
    [hasRight, rightSeries],
  );

  // Сколько точек на оси X — берём из самой длинной серии
  const N = useMemo(() => Math.max(...series.map((s) => s.data.length), 0), [series]);

  // X-coordinate i-th точки
  const xAt = (i: number) => PAD_X + (i / Math.max(N - 1, 1)) * innerW;

  // Y-coordinate value по конкретной оси
  const yAt = (v: number, range: { min: number; span: number } | null) =>
    range ? PAD_TOP + innerH - ((v - range.min) / range.span) * innerH : 0;

  // SVG path для линии
  const pathFor = (data: { value: number }[], range: { min: number; span: number } | null) =>
    data
      .map((d, i) => {
        const x = xAt(i);
        const y = yAt(d.value, range);
        return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(' ');

  // Long-press handlers
  const handleTouchStart = (e: React.TouchEvent) => {
    if (longPressTimer.current) clearTimeout(longPressTimer.current);
    const touch = e.touches[0];
    const rect = wrapRef.current?.getBoundingClientRect();
    if (!rect) return;
    const relX = touch.clientX - rect.left - PAD_X;
    const idx = Math.round((relX / innerW) * (N - 1));
    longPressTimer.current = window.setTimeout(() => {
      setCrosshair({ idx: Math.max(0, Math.min(N - 1, idx)), pinned: true });
    }, LONG_PRESS_MS);
  };

  const handleTouchMove = (e: React.TouchEvent) => {
    // Если crosshair pinned — двигаем его за пальцем (drag mode)
    if (!crosshair?.pinned) return;
    e.preventDefault();
    const touch = e.touches[0];
    const rect = wrapRef.current?.getBoundingClientRect();
    if (!rect) return;
    const relX = touch.clientX - rect.left - PAD_X;
    const idx = Math.round((relX / innerW) * (N - 1));
    setCrosshair({ idx: Math.max(0, Math.min(N - 1, idx)), pinned: true });
  };

  const handleTouchEnd = () => {
    if (longPressTimer.current) {
      clearTimeout(longPressTimer.current);
      longPressTimer.current = null;
    }
    // Crosshair остаётся pinned — закрывается tap'ом по экрану вне chart'а
  };

  const handleClickAway = () => setCrosshair(null);

  // Mouse handlers для desktop тестирования
  const handleMouseMove = (e: React.MouseEvent) => {
    const rect = wrapRef.current?.getBoundingClientRect();
    if (!rect) return;
    const relX = e.clientX - rect.left - PAD_X;
    const idx = Math.round((relX / innerW) * (N - 1));
    setCrosshair({ idx: Math.max(0, Math.min(N - 1, idx)), pinned: false });
  };

  const handleMouseLeave = () => {
    if (!crosshair?.pinned) setCrosshair(null);
  };

  // X-axis ticks (3-4 для мобиле, чтобы не наезжали)
  const xTicks = useMemo(() => {
    const count = Math.min(4, N);
    return Array.from({ length: count }, (_, i) => Math.round((i / (count - 1)) * (N - 1)));
  }, [N]);

  // Format X label
  const fmtX = (time: string) => {
    if (formatXLabel) return formatXLabel(time);
    const d = new Date(time);
    if (isNaN(d.getTime())) return time;
    return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit' });
  };

  // Format Y (axis pill)
  const fmtY = (v: number, s: MobileChartSeries) =>
    s.formatValue ? s.formatValue(v) : v.toFixed(0);

  if (loading || series.length === 0 || N < 2) {
    return (
      <div
        ref={wrapRef}
        style={{ height, display: 'grid', placeItems: 'center', color: 'var(--text-muted)' }}
      >
        {loading ? 'Загрузка...' : 'Нет данных'}
      </div>
    );
  }

  // Last value pills (на правом краю)
  const leftLastIdx = hasLeft ? leftSeries[0].data.length - 1 : -1;
  const rightLastIdx = hasRight ? rightSeries[0].data.length - 1 : -1;

  return (
    <div ref={wrapRef} style={{ position: 'relative', width: '100%', height }}>
      <svg
        width={width}
        height={height}
        style={{ display: 'block', userSelect: 'none', touchAction: 'pan-y' }}
        onTouchStart={handleTouchStart}
        onTouchMove={handleTouchMove}
        onTouchEnd={handleTouchEnd}
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
        onClick={handleClickAway}
      >
        {/* Y-axis tick lines (3 штуки: 15%, 50%, 85% от высоты) */}
        {[0.15, 0.5, 0.85].map((t, i) => (
          <line
            key={`grid-${i}`}
            x1={PAD_X}
            y1={PAD_TOP + innerH * t}
            x2={PAD_X + innerW}
            y2={PAD_TOP + innerH * t}
            stroke="color-mix(in srgb, var(--text-primary) 8%, transparent)"
            strokeWidth={1}
          />
        ))}

        {/* Линии */}
        {series.map((s, sIdx) => {
          const range = s.axis === 'right' ? rightRange : leftRange;
          return (
            <path
              key={`line-${sIdx}`}
              d={pathFor(s.data, range)}
              fill="none"
              stroke={s.color}
              strokeWidth={2}
              strokeLinejoin="round"
              strokeLinecap="round"
            />
          );
        })}

        {/* Y-axis tick labels — INSIDE chart edges */}
        {hasLeft && leftRange && [0.15, 0.5, 0.85].map((t, i) => {
          const v = leftRange.min + leftRange.span * (1 - t);
          return (
            <text
              key={`yl-${i}`}
              x={PAD_X + 4}
              y={PAD_TOP + innerH * t + 3}
              fontSize={9}
              fontWeight={600}
              fill="color-mix(in srgb, var(--text-primary) 55%, transparent)"
              style={{ pointerEvents: 'none' }}
            >
              {fmtY(v, leftSeries[0])}
            </text>
          );
        })}
        {hasRight && rightRange && [0.15, 0.5, 0.85].map((t, i) => {
          const v = rightRange.min + rightRange.span * (1 - t);
          return (
            <text
              key={`yr-${i}`}
              x={PAD_X + innerW - 4}
              y={PAD_TOP + innerH * t + 3}
              fontSize={9}
              fontWeight={600}
              fill="color-mix(in srgb, var(--text-primary) 55%, transparent)"
              textAnchor="end"
              style={{ pointerEvents: 'none' }}
            >
              {fmtY(v, rightSeries[0])}
            </text>
          );
        })}

        {/* Current value pills (на последней точке каждой серии) */}
        {hasLeft && leftRange && leftLastIdx >= 0 && (() => {
          const lastV = leftSeries[0].data[leftLastIdx].value;
          const x = xAt(leftLastIdx);
          const y = yAt(lastV, leftRange);
          const text = fmtY(lastV, leftSeries[0]);
          return <PillLabel key="pill-left" x={x} y={y} text={text} color={leftSeries[0].color} anchor="end" />;
        })()}
        {hasRight && rightRange && rightLastIdx >= 0 && (() => {
          const lastV = rightSeries[0].data[rightLastIdx].value;
          const x = xAt(rightLastIdx);
          const y = yAt(lastV, rightRange);
          const text = fmtY(lastV, rightSeries[0]);
          return <PillLabel key="pill-right" x={x} y={y} text={text} color={rightSeries[0].color} anchor="end" />;
        })()}

        {/* Crosshair vertical line + tooltip-data dots */}
        {crosshair && (
          <g pointerEvents="none">
            <line
              x1={xAt(crosshair.idx)}
              y1={PAD_TOP}
              x2={xAt(crosshair.idx)}
              y2={PAD_TOP + innerH}
              stroke="var(--text-primary)"
              strokeWidth={1}
              strokeDasharray="3,3"
              opacity={0.6}
            />
            {series.map((s, sIdx) => {
              const range = s.axis === 'right' ? rightRange : leftRange;
              const point = s.data[Math.min(crosshair.idx, s.data.length - 1)];
              if (!point) return null;
              const x = xAt(crosshair.idx);
              const y = yAt(point.value, range);
              return (
                <circle
                  key={`cross-dot-${sIdx}`}
                  cx={x}
                  cy={y}
                  r={4}
                  fill={s.color}
                  stroke="var(--bg-secondary)"
                  strokeWidth={2}
                />
              );
            })}
          </g>
        )}

        {/* X-axis labels */}
        {showXLabels && (
          <g>
            {xTicks.map((idx, ti) => {
              const longest = series.reduce((acc, s) => (s.data.length > acc.data.length ? s : acc), series[0]);
              const point = longest.data[Math.min(idx, longest.data.length - 1)];
              if (!point) return null;
              const x = xAt(idx);
              const isFirst = ti === 0;
              const isLast = ti === xTicks.length - 1;
              const anchor = isFirst ? 'start' : isLast ? 'end' : 'middle';
              return (
                <text
                  key={`xl-${idx}`}
                  x={x}
                  y={height - 4}
                  fontSize={10}
                  fontWeight={600}
                  fill="var(--text-secondary)"
                  textAnchor={anchor}
                >
                  {fmtX(point.time)}
                </text>
              );
            })}
          </g>
        )}
      </svg>

      {/* Tooltip — над линиями. Только когда crosshair активен. */}
      {crosshair && (
        <TooltipCard
          x={xAt(crosshair.idx)}
          chartW={width}
          series={series}
          idx={crosshair.idx}
        />
      )}
    </div>
  );
}

// ──────────────────────────────────────────────────────────
// Sub-components
// ──────────────────────────────────────────────────────────

interface PillLabelProps {
  x: number;
  y: number;
  text: string;
  color: string;
  anchor?: 'start' | 'end';
}

function PillLabel({ x, y, text, color, anchor = 'end' }: PillLabelProps) {
  const charW = 5.5;
  const padX = 4;
  const padY = 2;
  const fontY = 10;
  const w = text.length * charW + padX * 2;
  const h = fontY + padY * 2;
  const pillX = anchor === 'end' ? x - w - 2 : x + 2;
  return (
    <g pointerEvents="none">
      <rect x={pillX} y={y - h / 2} width={w} height={h} rx={3} fill={color} />
      <text
        x={pillX + w / 2}
        y={y}
        textAnchor="middle"
        dominantBaseline="central"
        fontSize={fontY}
        fontWeight={700}
        fill="#fff"
        style={{ fontFamily: 'IBM Plex Mono, monospace' }}
      >
        {text}
      </text>
    </g>
  );
}

interface TooltipCardProps {
  x: number;
  chartW: number;
  series: MobileChartSeries[];
  idx: number;
}

function TooltipCard({ x, chartW, series, idx }: TooltipCardProps) {
  const W = 140;
  const leftSide = x > chartW / 2;
  const left = leftSide ? Math.max(8, x - W - 8) : Math.min(chartW - W - 8, x + 8);

  const firstPoint = series[0]?.data[Math.min(idx, series[0].data.length - 1)];
  const date = firstPoint?.time
    ? new Date(firstPoint.time).toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' })
    : '';

  const style: CSSProperties = {
    position: 'absolute',
    top: 8,
    left,
    width: W,
    background: 'var(--bg-primary)',
    border: '1.5px solid var(--text-primary)',
    borderRadius: 8,
    padding: '6px 9px',
    fontSize: 11,
    boxShadow: '3px 3px 0 var(--text-primary)',
    pointerEvents: 'none',
  };

  return (
    <div style={style}>
      <div style={{ color: 'var(--text-secondary)', fontSize: 9, marginBottom: 4, fontWeight: 600 }}>
        {date}
      </div>
      {series.map((s, i) => {
        const point = s.data[Math.min(idx, s.data.length - 1)];
        if (!point) return null;
        const value = s.formatValue ? s.formatValue(point.value) : point.value.toFixed(2);
        return (
          <div
            key={`tr-${i}`}
            style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 6, marginTop: i === 0 ? 0 : 2 }}
          >
            <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, color: s.color, fontWeight: 600 }}>
              <span style={{ width: 7, height: 7, borderRadius: 99, background: s.color }} />
              {s.label}
            </span>
            <span className="mono" style={{ color: 'var(--text-primary)', fontWeight: 700 }}>{value}</span>
          </div>
        );
      })}
    </div>
  );
}
