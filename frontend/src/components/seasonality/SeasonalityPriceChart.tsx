import { useRef, useMemo } from 'react';
import ChartNavigator from '../ChartNavigator';
import type { PriceChartResponse } from '../../services/api';
import { CHART_COLORS, GRID, CROSSHAIR, DOT, TOOLTIP, PADDING, cssVar } from '../../config/chartTheme';

interface TooltipState {
  x: number;
  y: number;
  priceDate?: string;
  priceClose?: number;
  priceAdj?: number;
}

interface SeasonalityPriceChartProps {
  priceData: PriceChartResponse;
  priceNavRange: [number, number] | null;
  setPriceNavRange: (range: [number, number] | null) => void;
  tooltip: TooltipState | null;
  setTooltip: (t: TooltipState | null) => void;
  chartHeight: number;
}

export default function SeasonalityPriceChart({
  priceData,
  priceNavRange,
  setPriceNavRange,
  tooltip,
  setTooltip,
  chartHeight,
}: SeasonalityPriceChartProps) {
  const divHoverRef = useRef(false);

  const allPricePoints = priceData.data;
  const priceDividends = priceData.dividends;

  // Navigator data format
  const priceNavData = useMemo(() =>
    allPricePoints.map(p => ({ time: p.date, value: p.close })),
    [allPricePoints]);

  // Display points (filtered by navigator)
  const pricePoints = useMemo(() => {
    if (!priceNavRange) return allPricePoints;
    return allPricePoints.slice(priceNavRange[0], priceNavRange[1] + 1);
  }, [allPricePoints, priceNavRange]);

  const priceMinMax = useMemo(() => {
    if (pricePoints.length === 0) return { min: 0, max: 1 };
    const allVals = pricePoints.flatMap(p => [p.close, p.adjusted]);
    const min = Math.min(...allVals);
    const max = Math.max(...allVals);
    const range = max - min || 1;
    return { min: min - range * 0.05, max: max + range * 0.05 };
  }, [pricePoints]);

  if (pricePoints.length === 0) {
    return (
      <div className="flex items-center justify-center" style={{ height: chartHeight, color: 'var(--text-muted)' }}>Нет данных</div>
    );
  }

  // Padding from CSS tokens (with fallback)
  const PL = cssVar('--chart-pad-left', PADDING.left), PR = cssVar('--chart-pad-right-dual', PADDING.rightDual), PT = PADDING.top, PB = 60;
  const hasAdj = pricePoints.some(p => p.close !== p.adjusted);
  const scX = (i: number) => (i / Math.max(pricePoints.length - 1, 1));
  const scY = (v: number) => 1 - (v - priceMinMax.min) / (priceMinMax.max - priceMinMax.min);
  const yTicks = Array.from({ length: 5 }, (_, i) => {
    const val = priceMinMax.min + ((priceMinMax.max - priceMinMax.min) * i) / 4;
    return { value: val, pct: scY(val) * 100 };
  });
  const xTicks = (() => {
    const count = Math.min(7, pricePoints.length);
    return Array.from({ length: count }, (_, i) => {
      const idx = Math.floor((i / Math.max(count - 1, 1)) * (pricePoints.length - 1));
      return { label: pricePoints[idx].date.slice(5), pct: scX(idx) * 100 };
    });
  })();
  // Visible dividends
  const visibleDivs = priceDividends.filter(d => {
    return pricePoints.some(p => p.date === d.date);
  }).map(d => {
    const idx = pricePoints.findIndex(p => p.date === d.date);
    return { ...d, idx, pct: scX(idx) * 100 };
  });

  return (
    <div>
      {/* Legend centered */}
      <div className="flex justify-center gap-5 text-sm mb-3">
        <span className="flex items-center gap-2">
          <span className="w-3 h-3 rounded-full" style={{ backgroundColor: CHART_COLORS.accent }} />
          <span className="text-theme-primary font-medium">Цена</span>
        </span>
        {hasAdj && (
          <span className="flex items-center gap-2">
            <span className="w-3 h-3 rounded-full" style={{ backgroundColor: CHART_COLORS.adjusted }} />
            <span className="text-theme-primary font-medium">Без дивидендных гэпов</span>
          </span>
        )}
      </div>

      {/* Floating date label */}
      <div className="relative" style={{ height: 22 }}>
        {tooltip?.priceDate && (
          <div className="absolute pointer-events-none" style={{ left: tooltip.x, transform: 'translateX(-50%)' }}>
            <span className="text-[11px] text-theme-secondary bg-theme-tertiary/90 backdrop-blur-sm px-2 py-0.5 rounded border border-theme whitespace-nowrap">
              {tooltip.priceDate}
            </span>
          </div>
        )}
      </div>

      {/* Chart area */}
      <div className="relative cursor-crosshair" style={{ height: 'var(--chart-height, 420px)' }}
        onMouseMove={(e) => {
          if (divHoverRef.current) return;
          const rect = e.currentTarget.getBoundingClientRect();
          if (pricePoints.length === 0) return;
          const chartAreaW = rect.width - PL - PR;
          const mx = e.clientX - rect.left - PL;
          if (mx < 0 || mx > chartAreaW) { setTooltip(null); return; }
          const idx = Math.round((mx / chartAreaW) * (pricePoints.length - 1));
          const ci = Math.max(0, Math.min(idx, pricePoints.length - 1));
          const p = pricePoints[ci];
          setTooltip({
            x: PL + (ci / Math.max(pricePoints.length - 1, 1)) * chartAreaW,
            y: e.clientY - rect.top,
            priceDate: p.date, priceClose: p.close, priceAdj: p.adjusted,
          });
        }}
        onMouseLeave={() => { divHoverRef.current = false; setTooltip(null); }}
      >
        {/* SVG */}
        <div className="absolute" style={{ left: PL, right: PR, top: PT, bottom: PB }}>
          <svg viewBox={`0 0 1000 500`} preserveAspectRatio="none" width="100%" height="100%">
            {/* Grid */}
            {yTicks.map((t, i) => (
              <line key={i} x1="0" x2="1000" y1={t.pct / 100 * 500} y2={t.pct / 100 * 500}
                stroke={GRID.major} strokeWidth="1" vectorEffect="non-scaling-stroke" />
            ))}
            {/* Raw price */}
            <path d={pricePoints.map((p, i) =>
              `${i === 0 ? 'M' : 'L'} ${scX(i) * 1000} ${scY(p.close) * 500}`
            ).join(' ')} fill="none" stroke={CHART_COLORS.accent} strokeWidth="2" vectorEffect="non-scaling-stroke" strokeLinecap="round" strokeLinejoin="round" />
            {/* Adjusted */}
            {hasAdj && (
              <path d={pricePoints.map((p, i) =>
                `${i === 0 ? 'M' : 'L'} ${scX(i) * 1000} ${scY(p.adjusted) * 500}`
              ).join(' ')} fill="none" stroke={CHART_COLORS.adjusted} strokeWidth="2" vectorEffect="non-scaling-stroke" strokeLinecap="round" strokeLinejoin="round" strokeDasharray="6,3" />
            )}
            {/* Crosshair */}
            {tooltip?.priceDate && (() => {
              const idx = pricePoints.findIndex(p => p.date === tooltip.priceDate);
              if (idx < 0) return null;
              const cx = scX(idx) * 1000;
              const cy = scY(pricePoints[idx].close) * 500;
              return (
                <>
                  <line x1={cx} x2={cx} y1="0" y2="500"
                    stroke={CROSSHAIR.accentColor} strokeWidth="1" strokeDasharray={CROSSHAIR.accentDashArray} opacity={CROSSHAIR.accentOpacity} vectorEffect="non-scaling-stroke" />
                  <circle cx={cx} cy={cy} r={DOT.radius}
                    fill={CHART_COLORS.accent} stroke={DOT.strokeColor} strokeWidth={DOT.strokeWidth} vectorEffect="non-scaling-stroke" />
                </>
              );
            })()}
          </svg>
        </div>

        {/* Y labels */}
        {yTicks.map((t, i) => (
          <div key={i} className="absolute pointer-events-none" style={{ right: 4, top: `${PT + t.pct / 100 * (420 - PT - PB)}px`, transform: 'translateY(-50%)' }}>
            <span className="font-semibold" style={{ fontSize: 'var(--chart-font-x, 14px)', color: 'var(--axis-color, #9CA3B8)' }}>{t.value.toFixed(0)}</span>
          </div>
        ))}

        {/* X labels */}
        <div className="absolute flex justify-between font-semibold" style={{ left: PL, right: PR, bottom: 4, fontSize: 'var(--chart-font-x, 13px)', color: 'var(--axis-color, #9CA3B8)' }}>
          {xTicks.map((t, i) => (
            <span key={i}>{t.label}</span>
          ))}
        </div>

        {/* Dividend circles at bottom */}
        {visibleDivs.map((d, i) => {
          const chartAreaH = 420 - PT - PB;
          return (
            <div key={i} className="absolute group" style={{
              left: PL,
              right: PR,
              top: PT + chartAreaH - 14,
            }}><div style={{ position: 'absolute', left: `${d.pct}%`, transform: 'translateX(-50%)' }}
              onMouseEnter={() => { divHoverRef.current = true; setTooltip(null); }}
              onMouseLeave={() => { divHoverRef.current = false; }}
            >
              <div className="w-7 h-7 rounded-full flex items-center justify-center opacity-50 hover:opacity-100 transition-opacity cursor-pointer"
                style={{ backgroundColor: '#3a3f4f', color: '#9CA3B8', fontSize: 11, fontWeight: 600 }}>
                Д
              </div>
              {/* Vertical dashed guide line */}
              <div className="hidden group-hover:block absolute left-1/2 pointer-events-none" style={{
                bottom: 28, height: chartAreaH - 28,
                borderLeft: '1px dashed rgba(156, 163, 184, 0.4)',
              }} />
              {/* Tooltip above circle */}
              <div className="hidden group-hover:block absolute bottom-full left-1/2 -translate-x-1/2 mb-2 whitespace-nowrap z-30">
                <div className={TOOLTIP.containerClass}>
                  <div className="text-[11px] font-medium text-theme-primary">{d.value} ₽ — {d.date}</div>
                </div>
              </div>
            </div></div>
          );
        })}

        {/* Value tooltip */}
        {tooltip?.priceDate && (() => {
          const isRight = tooltip.x > 500;
          return (
            <div className="absolute pointer-events-none z-30"
              style={{
                left: isRight ? tooltip.x - 150 : tooltip.x + 12,
                top: Math.max(tooltip.y - 40, 4),
              }}>
              <div className={TOOLTIP.containerClass}>
                <div className="flex items-center gap-2">
                  <span className={TOOLTIP.dotSize} style={{ backgroundColor: CHART_COLORS.accent }} />
                  <span className={TOOLTIP.labelClass}>Цена</span>
                  <span className={`${TOOLTIP.valueClass} ml-auto pl-2`} style={{ color: CHART_COLORS.accent }}>{tooltip.priceClose?.toFixed(2)} ₽</span>
                </div>
                {tooltip.priceAdj !== tooltip.priceClose && (
                  <div className="flex items-center gap-2 mt-0.5">
                    <span className={TOOLTIP.dotSize} style={{ backgroundColor: CHART_COLORS.adjusted }} />
                    <span className={TOOLTIP.labelClass}>Без гэпов</span>
                    <span className={`${TOOLTIP.valueClass} ml-auto pl-2`} style={{ color: CHART_COLORS.adjusted }}>{tooltip.priceAdj?.toFixed(2)} ₽</span>
                  </div>
                )}
              </div>
            </div>
          );
        })()}
      </div>

      {/* Navigator */}
      <ChartNavigator
        data={priceNavData}
        onChange={(s, e) => setPriceNavRange([s, e])}
        color={CHART_COLORS.accent}
      />
    </div>
  );
}
