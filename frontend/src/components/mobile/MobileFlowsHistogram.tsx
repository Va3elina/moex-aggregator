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
import { useEffect, useMemo, useRef, useState } from 'react';
import type { FlowDataPoint } from '../../services/api';
import { axisFontSize } from '../chart/chartTypography';

interface MobileFlowsHistogramProps {
  flows: FlowDataPoint[];
}

const COLOR_IN = 'var(--funds-flow-positive, #5BD49C)';
const COLOR_OUT = 'var(--funds-flow-negative, #FF7A5C)';

function fmtDate(iso: string): string {
  const d = new Date(iso);
  if (isNaN(d.getTime())) return iso;
  return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' });
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
  // Touch X position для floating tooltip — null когда нет активного touch.
  const [touchX, setTouchX] = useState<number | null>(null);

  // Real pixel size через ResizeObserver с threshold-фильтром.
  // Threshold 8px стабилизирует SVG при iOS Safari address bar toggle.
  const wrapRef = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState({ w: 360, h: 280 });
  useEffect(() => {
    if (!wrapRef.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) {
        const r = e.contentRect;
        if (r.width > 0 && r.height > 0) {
          setSize((prev) => {
            if (Math.abs(prev.w - r.width) < 8 && Math.abs(prev.h - r.height) < 8) {
              return prev;
            }
            return { w: Math.max(200, r.width), h: Math.max(180, r.height) };
          });
        }
      }
    });
    ro.observe(wrapRef.current);
    return () => ro.disconnect();
  }, []);

  const W = size.w;
  const H = size.h;
  // Размер шрифта осей — общий с «Открытыми позициями» (MobileChart): дискретная
  // шкала по 6 брейкпоинтам ширины (axisFontSize), а не хардкод 9px.
  const axisFs = axisFontSize(W);
  const padX = 8;
  const padTop = 16;
  const padBottom = 24;
  // Правый жёлоб под шкалу (как в «Силе рынка»): бары до W - padRight, цифры
  // нетто-потоков стоят в чистой колонке справа.
  const padRight = 44;
  const innerW = W - padX - padRight;
  const innerH = H - padTop - padBottom;
  const zeroY = padTop + innerH / 2;

  // Максимум по абсолютному значению net flow — каждый bar теперь рисуется
  // как net (один цвет на знак), gross_in/gross_out видны только в tooltip.
  // Это match с desktop histogram: clean visual signal "приходят/уходят деньги".
  const maxAbs = useMemo(() => {
    if (flows.length === 0) return 1;
    return Math.max(...flows.map((f) => Math.abs(f.flow)), 0.1);
  }, [flows]);

  const barW = (innerW / flows.length) * 0.7;
  const barGap = (innerW / flows.length) * 0.3;

  // X-axis labels — каждый Nth период (на мобиле 3-4 метки)
  const labelStep = Math.max(1, Math.ceil(flows.length / 4));

  // Currently hovered tooltip
  const hovered = hoverIdx !== null ? flows[hoverIdx] : null;

  // Bar grow-up animation: на mount/новые flows initial height = 0,
  // в следующем frame -> actual. CSS transition даёт springy spring up.
  const [animated, setAnimated] = useState(false);
  useEffect(() => {
    setAnimated(false);
    const t = setTimeout(() => setAnimated(true), 30);
    return () => clearTimeout(t);
  }, [flows]);

  // Haptic tick + setHoverIdx (только если новая bar — иначе vibration spam).
  // Также tracks touchX в SVG-координатах для floating tooltip position.
  const updateHoverFromTouch = (clientX: number) => {
    const rect = wrapRef.current?.getBoundingClientRect();
    if (!rect) return;
    const relX = clientX - rect.left;
    setTouchX(relX);
    const idx = Math.floor((relX - padX) / (barW + barGap));
    if (idx < 0 || idx >= flows.length) {
      setHoverIdx(null);
      return;
    }
    setHoverIdx((prev) => {
      if (prev === idx) return prev;
      if (typeof navigator !== 'undefined' && typeof navigator.vibrate === 'function') {
        navigator.vibrate(8);
      }
      return idx;
    });
  };

  return (
    <div ref={wrapRef} style={{ position: 'relative', width: '100%', height: '100%', minHeight: 180 }}>
      <svg
        width={W}
        height={H}
        style={{ display: 'block', touchAction: 'none' }}
        onTouchStart={(e) => updateHoverFromTouch(e.touches[0].clientX)}
        onTouchMove={(e) => updateHoverFromTouch(e.touches[0].clientX)}
        onTouchEnd={() => { setHoverIdx(null); setTouchX(null); }}
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

        {/* Y-axis шкала нетто-потоков (млрд ₽) в правом жёлобе: [+max, +max/2,
            0, -max/2, -max]. Крайние с hanging/alphabetic, чтобы не обрезались. */}
        {[maxAbs, maxAbs / 2, 0, -maxAbs / 2, -maxAbs].map((val, i, arr) => {
          const ty = zeroY - (val / maxAbs) * (innerH / 2);
          const baseline = i === 0 ? 'hanging' : i === arr.length - 1 ? 'alphabetic' : 'central';
          const label = val === 0
            ? '0'
            : `${val > 0 ? '+' : ''}${Math.abs(val) >= 10 ? val.toFixed(0) : val.toFixed(1)}`;
          return (
            <text
              key={`ys-${i}`}
              x={W - 4}
              y={ty}
              fontSize={axisFs}
              fontWeight={600}
              fill="color-mix(in srgb, var(--text-primary) 55%, transparent)"
              textAnchor="end"
              dominantBaseline={baseline}
              pointerEvents="none"
            >
              {label}
            </text>
          );
        })}

        {/* Dashed crosshair — vertical line через центр выбранной bar.
            Помогает визуально связать tooltip values с конкретным столбцом. */}
        {hoverIdx !== null && (() => {
          const cx = padX + hoverIdx * (barW + barGap) + barGap / 2 + barW / 2;
          return (
            <line
              x1={cx}
              y1={padTop}
              x2={cx}
              y2={H - padBottom}
              stroke="var(--text-primary)"
              strokeWidth={1}
              strokeDasharray="3,3"
              opacity={0.6}
              pointerEvents="none"
            />
          );
        })()}

        {/* Net-only bars: один bar на период, цвет = знак flow.
            flow >= 0 → зелёный вверх от zero-line.
            flow < 0  → красный вниз от zero-line.
            Springy grow-up animation с staggered delay. */}
        {flows.map((f, i) => {
          const x = padX + i * (barW + barGap) + barGap / 2;
          const isPositive = f.flow >= 0;
          const finalH = (Math.abs(f.flow) / maxAbs) * (innerH / 2);
          const h = animated ? finalH : 0;
          const isHovered = hoverIdx === i;
          const delay = Math.min(i * 3, 200);
          const color = isPositive ? COLOR_IN : COLOR_OUT;
          const y = isPositive ? zeroY - h : zeroY;

          if (finalH === 0) return null;

          return (
            <rect
              key={i}
              x={x}
              y={y}
              width={barW}
              height={h}
              fill={color}
              opacity={isHovered ? 1 : 0.85}
              rx={1.5}
              style={{
                transition: `y 500ms cubic-bezier(0.34, 1.56, 0.64, 1) ${delay}ms, height 500ms cubic-bezier(0.34, 1.56, 0.64, 1) ${delay}ms, opacity 150ms ease`,
              }}
            />
          );
        })}

        {/* X-axis labels: первый — anchor=start, последний — anchor=end,
            остальные — middle. Это держит labels всегда внутри SVG, не
            выпадая за edges при большом количестве bars. */}
        {flows.map((f, i) => {
          if (i % labelStep !== 0 && i !== flows.length - 1) return null;
          const isFirst = i === 0;
          const isLast = i === flows.length - 1;
          const anchor: 'start' | 'middle' | 'end' = isFirst ? 'start' : isLast ? 'end' : 'middle';
          // x: первый → padX (с edge), последний → правый край плота (padX +
          // innerW, перед жёлобом шкалы), остальные → центр bar
          const x = isFirst
            ? padX
            : isLast
              ? padX + innerW
              : padX + i * (barW + barGap) + barGap / 2 + barW / 2;
          return (
            <text
              key={`xl-${i}`}
              x={x}
              y={H - 8}
              fontSize={axisFs}
              fontWeight={600}
              fill="var(--text-secondary)"
              textAnchor={anchor}
            >
              {fmtDate(f.period_start)}
            </text>
          );
        })}
      </svg>

      {/* Floating tooltip — следует за пальцем как в TradingView. Position
          clamped по [margin, W - tooltipW - margin] чтобы не вылезал за SVG. */}
      {hovered && touchX !== null && (() => {
        const tooltipW = 200;
        const margin = 8;
        const left = Math.max(margin, Math.min(touchX - tooltipW / 2, W - tooltipW - margin));
        return (
        <div
          style={{
            position: 'absolute',
            top: 4,
            left,
            width: tooltipW,
            padding: '6px 10px',
            background: 'var(--bg-primary)',
            border: '1.5px solid var(--text-primary)',
            borderRadius: 8,
            fontSize: 'var(--fs-xs)',
            pointerEvents: 'none',
            display: 'flex',
            flexDirection: 'column',
            gap: 3,
          }}
        >
          <div style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-secondary)', fontWeight: 600, letterSpacing: '0.02em' }}>
            {fmtDate(hovered.period_start)} — {fmtDate(hovered.period_end)}
          </div>
          {/* Минималистичный: только нетто и процент изменения СЧА */}
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', gap: 8 }}>
            <span
              className="mono"
              style={{ color: hovered.flow >= 0 ? COLOR_IN : COLOR_OUT, fontWeight: 800, fontSize: 'var(--fs-base)' }}
            >
              {hovered.flow >= 0 ? '+' : ''}{fmtBillions(hovered.flow)} ₽
            </span>
            <span
              className="mono"
              style={{
                color: hovered.flow_pct >= 0 ? COLOR_IN : COLOR_OUT,
                fontWeight: 700,
                fontSize: 'var(--fs-xs)',
              }}
            >
              {hovered.flow_pct >= 0 ? '+' : ''}{hovered.flow_pct.toFixed(2)}%
            </span>
          </div>
        </div>
        );
      })()}
    </div>
  );
}
