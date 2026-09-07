/**
 * MobileFlowsHistogram — «Притоки-Оттоки» на мобилке.
 *
 * Один SVG с двумя панелями по модели «Силы рынка» (StrengthDualChart в
 * MobileStrengthPage): сверху — линия бенчмарка категории (index из
 * nav-ответа: IMOEX / RGBITR / RUSFAR3M / GLDRUB_TOM), снизу — биполярная
 * гистограмма чистых притоков (зелёный вверх = приток, красный вниз = отток).
 * Ось X общая — слоты периодов потоков; дневные закрытия индекса ложатся
 * внутри своего слота дробно ((k+0.5)/K), как в десктопном FlowsHistogram.
 * Нет индекса (не приехал / слой «Индекс» выключен) → одна гистограмма во
 * всю высоту, как было до панели бенчмарка.
 *
 * Правая шкала обеих панелей лежит ПОВЕРХ графика, а не в отдельном жёлобе —
 * как на остальных мобильных графиках (MobileChart «Открытых позиций», #329):
 * столбцы идут до самого правого края, цифры шкалы — полупрозрачный оверлей
 * с подложкой цвета фона, чтобы читались поверх столбца.
 *
 * Touch → общий crosshair сквозь обе панели + tooltip: период, индекс, нетто.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import type { FlowDataPoint, IndexDataPoint } from '../../services/api';
import { axisFontSize, xAxisTickCount } from '../chart/chartTypography';

interface MobileFlowsHistogramProps {
  flows: FlowDataPoint[];
  /** Дневные закрытия бенчмарка категории (index.data из nav-ответа) — верхняя
   *  панель. Точки вне диапазона потоков отбрасываются. */
  indexData?: IndexDataPoint[];
  /** Подпись линии индекса (secid: IMOEX, RGBITR, …). */
  indexLabel?: string;
  /** Слой «Индекс» (общий тумблер с режимом СЧА и десктопом). false →
   *  панель бенчмарка не рисуется даже при наличии данных. */
  showIndex?: boolean;
}

const COLOR_IN = 'var(--funds-flow-positive, #5BD49C)';
const COLOR_OUT = 'var(--funds-flow-negative, #FF7A5C)';
// Линия индекса — синяя, как индекс-эталон на графике СЧА этой же страницы
// (MobileChart) и линия цены в «Силе рынка».
const INDEX_COLOR = 'var(--chart-line-1, #5DA3E9)';
const AXIS_FILL = 'color-mix(in srgb, var(--text-primary) 55%, transparent)';

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

// Значение индекса на оси/в pill'е: адаптивная точность — IMOEX ~2800 без
// дробной части, RGBITR ~600 с одной, ставка RUSFAR ~15.5 с двумя.
function fmtPrice(v: number): string {
  if (!Number.isFinite(v)) return '—';
  if (v >= 1000) return Math.round(v).toString();
  if (v >= 100) return v.toFixed(1);
  if (v >= 10) return v.toFixed(2);
  return v.toFixed(3);
}

/** Pill последнего значения у правого края (правый край = x, растём влево). */
function Pill({ x, y, text, color, fontSize }: { x: number; y: number; text: string; color: string; fontSize: number }) {
  const charW = fontSize * 0.55;
  const padX = 4;
  const padY = 2;
  const w = text.length * charW + padX * 2;
  const h = fontSize + padY * 2;
  const pillX = x - w - 2;
  return (
    <g pointerEvents="none">
      <rect x={pillX} y={y - h / 2} width={w} height={h} rx={3} fill={color} />
      <text
        x={pillX + w / 2}
        y={y}
        textAnchor="middle"
        dominantBaseline="central"
        fontSize={fontSize}
        fontWeight={700}
        fill="#fff"
        style={{ fontFamily: 'IBM Plex Mono, monospace' }}
      >
        {text}
      </text>
    </g>
  );
}

export default function MobileFlowsHistogram({
  flows,
  indexData,
  indexLabel,
  showIndex = true,
}: MobileFlowsHistogramProps) {
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
  const N = flows.length;

  // ── Закрытия индекса, разложенные по слотам периодов потоков. ──
  // Слот i покрывает [period_start_i, period_end_i]; ISO-строки сравниваются
  // лексикографически. Точки вне диапазона потоков отбрасываются.
  const idxBySlot = useMemo(() => {
    const bySlot: number[][] = flows.map(() => []);
    if (!indexData?.length || !flows.length) return bySlot;
    let si = 0;
    for (const p of indexData) {
      const v = p.close;
      if (v == null || !(v > 0)) continue;
      while (si < flows.length && flows[si].period_end < p.date) si++;
      if (si >= flows.length) break;
      if (p.date >= flows[si].period_start) bySlot[si].push(v);
    }
    return bySlot;
  }, [flows, indexData]);
  const hasPrice = showIndex && idxBySlot.some((a) => a.length > 0);

  // ── Геометрия панелей (как в StrengthDualChart): верх = индекс, низ = бары.
  // Между панелями — полоса X-подписей (MID_GAP); без индекса её нет и
  // гистограмма забирает всю высоту.
  const MID_GAP = hasPrice ? 18 : 0;
  const totalInnerH = H - padTop - padBottom - MID_GAP;
  const topH = hasPrice ? totalInnerH * 0.5 : 0;
  const botH = totalInnerH - topH;
  const topY0 = padTop;
  const topY1 = topY0 + topH;
  const botY0 = topY1 + MID_GAP;
  const botY1 = botY0 + botH;
  const zeroY = botY0 + botH / 2;
  // Плот на всю ширину (симметричный padX): правая шкала — оверлей, не жёлоб.
  const innerW = W - padX * 2;
  const slotW = N > 0 ? innerW / N : 0;
  const barW = slotW * 0.7;
  const barGap = slotW * 0.3;
  const barX = (i: number) => padX + i * slotW + barGap / 2;
  const barCx = (i: number) => barX(i) + barW / 2;

  // Максимум по абсолютному значению net flow — каждый bar рисуется как net
  // (один цвет на знак), gross_in/gross_out видны только в tooltip.
  const maxAbs = useMemo(() => {
    if (flows.length === 0) return 1;
    return Math.max(...flows.map((f) => Math.abs(f.flow)), 0.1);
  }, [flows]);
  const yTicks = [maxAbs, maxAbs / 2, 0, -maxAbs / 2, -maxAbs];
  const fmtYTick = (val: number) =>
    val === 0 ? '0' : `${val > 0 ? '+' : ''}${Math.abs(val) >= 10 ? val.toFixed(0) : val.toFixed(1)}`;
  const yFlow = (v: number) => zeroY - (v / maxAbs) * (botH / 2);

  // Шкала индекса — по всем точкам в окне потоков, поля 6% сверху/снизу.
  const priceRange = useMemo(() => {
    if (!hasPrice) return { min: 0, span: 1 };
    let lo = Infinity;
    let hi = -Infinity;
    for (const arr of idxBySlot) {
      for (const v of arr) {
        if (v < lo) lo = v;
        if (v > hi) hi = v;
      }
    }
    if (!Number.isFinite(lo) || !Number.isFinite(hi)) return { min: 0, span: 1 };
    if (lo === hi) { lo *= 0.97; hi *= 1.03; }
    const pad = (hi - lo) * 0.06;
    return { min: lo - pad, span: (hi - lo) + pad * 2 };
  }, [hasPrice, idxBySlot]);
  const yPrice = (v: number) => topY1 - ((v - priceRange.min) / priceRange.span) * topH;

  // Линия индекса: дневные точки каждого слота дробно внутри слота. Y считаем
  // тут же (не через yPrice), чтобы deps мемо были честными.
  const linePts = useMemo(() => {
    if (!hasPrice) return [] as { x: number; y: number }[];
    const pts: { x: number; y: number }[] = [];
    for (let i = 0; i < N; i++) {
      const arr = idxBySlot[i];
      if (!arr || !arr.length) continue;
      const x0 = padX + i * slotW;
      for (let k = 0; k < arr.length; k++) {
        pts.push({
          x: x0 + slotW * ((k + 0.5) / arr.length),
          y: topY1 - ((arr[k] - priceRange.min) / priceRange.span) * topH,
        });
      }
    }
    return pts;
  }, [hasPrice, idxBySlot, N, slotW, topY1, topH, priceRange]);
  const linePath = useMemo(
    () => linePts.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' '),
    [linePts],
  );

  // Последнее закрытие слота — значение индекса для тултипа и точки курсора.
  const slotClose = (i: number): number | null => {
    const arr = idxBySlot[i];
    return arr && arr.length ? arr[arr.length - 1] : null;
  };
  // Точка курсора на линии: последняя точка слота.
  const slotDot = (i: number): { x: number; y: number } | null => {
    const arr = idxBySlot[i];
    if (!arr || !arr.length) return null;
    const K = arr.length;
    return { x: padX + i * slotW + slotW * ((K - 0.5) / K), y: yPrice(arr[K - 1]) };
  };
  // Последнее значение индекса в окне — pill у правого края.
  const lastIndexVal = useMemo(() => {
    for (let i = N - 1; i >= 0; i--) {
      const arr = idxBySlot[i];
      if (arr && arr.length) return arr[arr.length - 1];
    }
    return null;
  }, [idxBySlot, N]);

  // X-axis labels — равномерно распределённые индексы (как в «Потоке капитала»):
  // первая у левого края, последняя у правого. Кол-во подписей по ширине
  // ("DD.MM.YY" ~8 симв. + воздух).
  const xLabelCount = Math.min(xAxisTickCount(innerW, axisFs, 12), N);
  const xLabelIndices = xLabelCount < 2
    ? (N > 0 ? [0] : [])
    : Array.from({ length: xLabelCount }, (_, k) =>
        Math.min(Math.round((k * (N - 1)) / (xLabelCount - 1)), N - 1),
      );
  const xLabelAt = (i: number, ti: number): { x: number; anchor: 'start' | 'middle' | 'end' } => {
    const isFirst = ti === 0;
    const isLast = ti === xLabelIndices.length - 1;
    if (isFirst) return { x: padX, anchor: 'start' };
    if (isLast) return { x: padX + innerW, anchor: 'end' };
    return { x: barCx(i), anchor: 'middle' };
  };

  // Currently hovered tooltip
  const hovered = hoverIdx !== null ? flows[hoverIdx] : null;
  const hoveredClose = hoverIdx !== null && hasPrice ? slotClose(hoverIdx) : null;

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
    const idx = slotW > 0 ? Math.floor((relX - padX) / slotW) : -1;
    if (idx < 0 || idx >= N) {
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

  // Подписи шкал — оверлей поверх графика: полупрозрачный текст с подложкой
  // цвета фона (paintOrder=stroke), чтобы цифры читались поверх столбца/линии.
  const axisTextProps = {
    x: W - 4,
    fontSize: axisFs,
    fontWeight: 600,
    fill: AXIS_FILL,
    stroke: 'var(--bg-primary)',
    strokeWidth: 3,
    strokeLinejoin: 'round' as const,
    paintOrder: 'stroke' as const,
    textAnchor: 'end' as const,
    pointerEvents: 'none' as const,
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
        {/* ── Верхняя панель: бенчмарк категории ── */}
        {hasPrice && (
          <>
            {/* Сетка индекса (3 уровня) — как в «Силе рынка» */}
            {[0.2, 0.5, 0.8].map((t) => (
              <line
                key={`gt-${t}`}
                x1={padX}
                y1={topY0 + topH * t}
                x2={padX + innerW}
                y2={topY0 + topH * t}
                stroke="color-mix(in srgb, var(--text-primary) 8%, transparent)"
                strokeWidth={1}
              />
            ))}
            {/* Подпись линии — что за индекс (в шапке страницы его нет) */}
            {indexLabel && (
              <text
                x={padX + 2}
                y={topY0 + 1}
                fontSize={axisFs}
                fontWeight={700}
                fill={INDEX_COLOR}
                dominantBaseline="hanging"
                pointerEvents="none"
              >
                {indexLabel}
              </text>
            )}
            {linePath && (
              <path
                d={linePath}
                fill="none"
                stroke={INDEX_COLOR}
                strokeWidth={2}
                strokeLinejoin="round"
                strokeLinecap="round"
              />
            )}
            {/* Шкала индекса — оверлей у правого края */}
            {[0.2, 0.5, 0.8].map((t) => {
              const v = priceRange.min + priceRange.span * (1 - t);
              return (
                <text key={`yp-${t}`} {...axisTextProps} y={topY0 + topH * t} dominantBaseline="central">
                  {fmtPrice(v)}
                </text>
              );
            })}
            {lastIndexVal != null && (
              <Pill x={W} y={yPrice(lastIndexVal)} text={fmtPrice(lastIndexVal)} color={INDEX_COLOR} fontSize={axisFs} />
            )}
            {/* X-подписи между панелями (на дате), как в «Силе рынка» */}
            {xLabelIndices.map((i, ti) => {
              const f = flows[i];
              if (!f) return null;
              const { x, anchor } = xLabelAt(i, ti);
              return (
                <text
                  key={`xm-${i}`}
                  x={x}
                  y={topY1 + MID_GAP - 4}
                  fontSize={axisFs}
                  fontWeight={600}
                  fill="var(--text-secondary)"
                  textAnchor={anchor}
                >
                  {fmtDate(f.period_start)}
                </text>
              );
            })}
          </>
        )}

        {/* ── Нижняя панель: гистограмма чистых притоков ── */}
        {/* Горизонтальные полоски (0.25/0.5/0.75 от полувысоты, вверх и вниз) —
            как в «Потоке капитала». */}
        {[0.25, 0.5, 0.75].map((t) => {
          const yUp = zeroY - (botH / 2) * t;
          const yDown = zeroY + (botH / 2) * t;
          return (
            <g key={`grid-${t}`}>
              <line x1={padX} y1={yUp} x2={padX + innerW} y2={yUp}
                stroke="color-mix(in srgb, var(--text-primary) 8%, transparent)" strokeWidth={1} />
              <line x1={padX} y1={yDown} x2={padX + innerW} y2={yDown}
                stroke="color-mix(in srgb, var(--text-primary) 8%, transparent)" strokeWidth={1} />
            </g>
          );
        })}

        {/* Zero line */}
        <line
          x1={padX}
          y1={zeroY}
          x2={padX + innerW}
          y2={zeroY}
          stroke="color-mix(in srgb, var(--text-primary) 25%, transparent)"
          strokeWidth={1}
        />

        {/* Dashed crosshair — вертикаль через центр выбранной bar, сквозь обе
            панели. Помогает связать tooltip values с конкретным столбцом. */}
        {hoverIdx !== null && (
          <line
            x1={barCx(hoverIdx)}
            y1={hasPrice ? topY0 : botY0}
            x2={barCx(hoverIdx)}
            y2={botY1}
            stroke="var(--text-primary)"
            strokeWidth={1}
            strokeDasharray="3,3"
            opacity={0.6}
            pointerEvents="none"
          />
        )}

        {/* Net-only bars: один bar на период, цвет = знак flow.
            flow >= 0 → зелёный вверх от zero-line.
            flow < 0  → красный вниз от zero-line.
            Springy grow-up animation с staggered delay. */}
        {flows.map((f, i) => {
          const x = barX(i);
          const isPositive = f.flow >= 0;
          const finalH = (Math.abs(f.flow) / maxAbs) * (botH / 2);
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

        {/* Y-axis шкала нетто-потоков (млрд ₽) оверлеем у правого края:
            [+max, +max/2, 0, -max/2, -max]. Крайние с hanging/alphabetic,
            чтобы не обрезались. */}
        {yTicks.map((val, i, arr) => {
          const ty = yFlow(val);
          const baseline = i === 0 ? 'hanging' : i === arr.length - 1 ? 'alphabetic' : 'central';
          return (
            <text key={`ys-${i}`} {...axisTextProps} y={ty} dominantBaseline={baseline}>
              {fmtYTick(val)}
            </text>
          );
        })}
        {/* Pill последнего нетто — у правого края, цвет по знаку (как в ОИ) */}
        {N > 0 && (
          <Pill
            x={W}
            y={yFlow(flows[N - 1].flow)}
            text={fmtYTick(flows[N - 1].flow)}
            color={flows[N - 1].flow >= 0 ? COLOR_IN : COLOR_OUT}
            fontSize={axisFs}
          />
        )}

        {/* Точка курсора на линии индекса — поверх баров и шкалы */}
        {hoverIdx !== null && hasPrice && (() => {
          const d = slotDot(hoverIdx);
          if (!d) return null;
          return (
            <circle
              cx={d.x}
              cy={d.y}
              r={4}
              fill={INDEX_COLOR}
              stroke="var(--bg-primary)"
              strokeWidth={2}
              pointerEvents="none"
            />
          );
        })()}

        {/* X-axis labels снизу: равномерно распределены (xLabelIndices). Первый —
            anchor=start, последний — anchor=end, остальные — middle под столбцом. */}
        {xLabelIndices.map((i, ti) => {
          const f = flows[i];
          if (!f) return null;
          const { x, anchor } = xLabelAt(i, ti);
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
          {/* Индекс на конец периода — первой строкой: приток без ценового
              контекста не говорит, дорого ли заходили деньги. */}
          {hoveredClose != null && (
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', gap: 8 }}>
              <span style={{ color: INDEX_COLOR, fontWeight: 700 }}>{indexLabel ?? 'Индекс'}</span>
              <span className="mono" style={{ fontWeight: 800 }}>{fmtPrice(hoveredClose)}</span>
            </div>
          )}
          {/* Минималистичный: только нетто */}
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', gap: 8 }}>
            <span
              className="mono"
              style={{ color: hovered.flow >= 0 ? COLOR_IN : COLOR_OUT, fontWeight: 800, fontSize: 'var(--fs-base)' }}
            >
              {hovered.flow >= 0 ? '+' : ''}{fmtBillions(hovered.flow)} ₽
            </span>
          </div>
        </div>
        );
      })()}
    </div>
  );
}
