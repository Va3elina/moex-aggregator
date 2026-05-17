/**
 * MobileFlowsHistogram — bidirectional histogram для Притоки-Оттоки.
 *
 * SVG-based, viewBox с preserveAspectRatio="none" → tracking всю
 * доступную ширину/высоту parent контейнера.
 *
 * Каждый период — столбец:
 *   - gross_in (зелёный, вверх от zero line) — суммарные притоки
 *   - gross_out (красный, вниз от zero line) — суммарные оттоки
 *
 * Tap по столбцу → краткий tooltip с цифрами на этот период.
 */
import { useMemo, useState } from 'react';
import type { FlowDataPoint } from '../../services/api';

interface MobileFlowsHistogramProps {
  flows: FlowDataPoint[];
}

const COLOR_IN = 'var(--funds-flow-positive, #5BD49C)';
const COLOR_OUT = 'var(--funds-flow-negative, #FF7A5C)';

function fmtDate(iso: string): string {
  const d = new Date(iso);
  if (isNaN(d.getTime())) return iso;
  return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit' });
}

function fmtBillions(v: number): string {
  if (!Number.isFinite(v)) return '—';
  const abs = Math.abs(v);
  if (abs >= 1) return `${v.toFixed(1)} млрд`;
  if (abs >= 0.01) return `${(v * 1000).toFixed(0)} млн`;
  return `${(v * 1000).toFixed(1)} млн`;
}

export default function MobileFlowsHistogram({ flows }: MobileFlowsHistogramProps) {
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  // Логические координаты — viewBox растягивается на 100% контейнера
  const W = 360;
  const H = 280;
  const padX = 8;
  const padTop = 16;
  const padBottom = 24;
  const innerW = W - padX * 2;
  const innerH = H - padTop - padBottom;
  const zeroY = padTop + innerH / 2;

  // Максимум по абсолютному значению для нормализации высоты
  const maxAbs = useMemo(() => {
    if (flows.length === 0) return 1;
    return Math.max(
      ...flows.flatMap((f) => [Math.abs(f.gross_in), Math.abs(f.gross_out)]),
      0.1,
    );
  }, [flows]);

  const barW = (innerW / flows.length) * 0.7;
  const barGap = (innerW / flows.length) * 0.3;

  // X-axis labels — каждый Nth период (на мобиле 3-4 метки)
  const labelStep = Math.max(1, Math.ceil(flows.length / 4));

  // Currently hovered tooltip
  const hovered = hoverIdx !== null ? flows[hoverIdx] : null;

  return (
    <div style={{ position: 'relative', width: '100%', height: '100%', minHeight: 200 }}>
      <svg
        width="100%"
        height="100%"
        viewBox={`0 0 ${W} ${H}`}
        preserveAspectRatio="none"
        style={{ display: 'block', touchAction: 'manipulation' }}
        onClick={(e) => {
          // Hit-testing — определяем какой bar тапнули
          const rect = (e.currentTarget as SVGSVGElement).getBoundingClientRect();
          const relX = ((e.clientX - rect.left) / rect.width) * W;
          const idx = Math.floor((relX - padX) / (barW + barGap));
          if (idx >= 0 && idx < flows.length) {
            setHoverIdx(hoverIdx === idx ? null : idx);
          } else {
            setHoverIdx(null);
          }
        }}
      >
        {/* Zero line */}
        <line
          x1={padX}
          y1={zeroY}
          x2={padX + innerW}
          y2={zeroY}
          stroke="color-mix(in srgb, var(--text-primary) 25%, transparent)"
          strokeWidth={1}
        />

        {/* Bars */}
        {flows.map((f, i) => {
          const x = padX + i * (barW + barGap) + barGap / 2;
          const hIn = (Math.abs(f.gross_in) / maxAbs) * (innerH / 2);
          const hOut = (Math.abs(f.gross_out) / maxAbs) * (innerH / 2);
          const isHovered = hoverIdx === i;

          return (
            <g key={i}>
              {hIn > 0 && (
                <rect
                  x={x}
                  y={zeroY - hIn}
                  width={barW}
                  height={hIn}
                  fill={COLOR_IN}
                  opacity={isHovered ? 1 : 0.85}
                  rx={1.5}
                />
              )}
              {hOut > 0 && (
                <rect
                  x={x}
                  y={zeroY}
                  width={barW}
                  height={hOut}
                  fill={COLOR_OUT}
                  opacity={isHovered ? 1 : 0.85}
                  rx={1.5}
                />
              )}
            </g>
          );
        })}

        {/* X-axis labels */}
        {flows.map((f, i) => {
          if (i % labelStep !== 0 && i !== flows.length - 1) return null;
          const x = padX + i * (barW + barGap) + barGap / 2 + barW / 2;
          return (
            <text
              key={`xl-${i}`}
              x={x}
              y={H - 8}
              fontSize={9}
              fontWeight={600}
              fill="var(--text-secondary)"
              textAnchor="middle"
            >
              {fmtDate(f.period_start)}
            </text>
          );
        })}
      </svg>

      {/* Tooltip card */}
      {hovered && (
        <div
          style={{
            position: 'absolute',
            top: 8,
            left: 8,
            right: 8,
            padding: '10px 12px',
            background: 'var(--bg-primary)',
            border: '1.5px solid var(--text-primary)',
            borderRadius: 8,
            boxShadow: '3px 3px 0 var(--text-primary)',
            fontSize: 11,
            pointerEvents: 'none',
          }}
        >
          <div style={{ fontSize: 10, color: 'var(--text-secondary)', marginBottom: 4, fontWeight: 600 }}>
            {fmtDate(hovered.period_start)} — {fmtDate(hovered.period_end)}
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 2 }}>
            <span style={{ color: COLOR_IN, fontWeight: 600 }}>● Притоки</span>
            <span className="mono" style={{ fontWeight: 700 }}>{fmtBillions(hovered.gross_in)} ₽</span>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
            <span style={{ color: COLOR_OUT, fontWeight: 600 }}>● Оттоки</span>
            <span className="mono" style={{ fontWeight: 700 }}>{fmtBillions(hovered.gross_out)} ₽</span>
          </div>
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              paddingTop: 4,
              borderTop: '1px solid color-mix(in srgb, var(--text-primary) 15%, transparent)',
              fontWeight: 700,
            }}
          >
            <span>Нетто</span>
            <span
              className="mono"
              style={{ color: hovered.flow >= 0 ? COLOR_IN : COLOR_OUT }}
            >
              {hovered.flow >= 0 ? '+' : ''}{fmtBillions(hovered.flow)} ₽
            </span>
          </div>
        </div>
      )}
    </div>
  );
}
