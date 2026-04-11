import { useRef, useMemo } from 'react';
import ChartNavigator from '../ChartNavigator';
import type { PriceChartResponse } from '../../services/api';
import { CHART_COLORS, CROSSHAIR, PADDING, cssVar } from '../../config/chartTheme';
import { ChartGrid, ChartCrosshair, ChartDot, ChartDateLabel, ChartTooltip, TooltipRow, ChartYAxis, ChartXAxis, ChartMarker } from '../chart';

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
      {tooltip?.priceDate ? (
        <ChartDateLabel date={tooltip.priceDate} x={tooltip.x} />
      ) : (
        <div style={{ height: 22 }} />
      )}

      {/* Chart area */}
      <div className="relative cursor-crosshair" style={{ aspectRatio: '2.4', minHeight: 280, maxHeight: 550 }}
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
            <ChartGrid yTicks={yTicks} />
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
            {/* Crosshair + Dot */}
            {tooltip?.priceDate && (() => {
              const idx = pricePoints.findIndex(p => p.date === tooltip.priceDate);
              if (idx < 0) return null;
              const cx = scX(idx) * 1000;
              const cy = scY(pricePoints[idx].close) * 500;
              return (
                <>
                  <ChartCrosshair x={cx} color="rgba(200,255,46,0.5)" dashArray={CROSSHAIR.accentDashArray} />
                  <ChartDot x={cx} y={cy} color={CHART_COLORS.accent} />
                </>
              );
            })()}
          </svg>
        </div>

        {/* Y labels */}
        <ChartYAxis ticks={yTicks} side="right" format={(v) => v.toFixed(0)} padTop={PT} padBottom={PB} />

        {/* X labels */}
        <ChartXAxis labels={xTicks.map(t => t.label)} padLeft={PL} padRight={PR} />

        {/* Dividend circles at bottom */}
        <div className="absolute" style={{ left: PL, right: PR, bottom: PB - 14 }}>
          {visibleDivs.map((d, i) => (
            <ChartMarker
              key={i}
              label="Д"
              xPct={d.pct}
              guideHeight={`calc(100% - ${PB + 14}px)`}
              onHover={() => { divHoverRef.current = true; setTooltip(null); }}
              onLeave={() => { divHoverRef.current = false; }}
            >
              <div className="text-[11px] font-medium text-theme-primary">{d.value} ₽ — {d.date}</div>
            </ChartMarker>
          ))}
        </div>

        {/* Value tooltip */}
        {tooltip?.priceDate && (
          <ChartTooltip x={tooltip.x} y={tooltip.y} flipAt={500}>
            <TooltipRow color={CHART_COLORS.accent} label="Цена" value={`${tooltip.priceClose?.toFixed(2)} ₽`} />
            {tooltip.priceAdj !== tooltip.priceClose && (
              <TooltipRow color={CHART_COLORS.adjusted} label="Без гэпов" value={`${tooltip.priceAdj?.toFixed(2)} ₽`} />
            )}
          </ChartTooltip>
        )}
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
