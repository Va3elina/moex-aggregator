import type { YearlySeasonalityResponse } from '../../services/api';
import { CHART_COLORS, GRID, CROSSHAIR, TOOLTIP, PADDING, cssVar } from '../../config/chartTheme';

interface TooltipState {
  x: number;
  y: number;
  yearlyAvgPct?: number;
  yearlyCurPct?: number;
  yearlyCurDate?: string;
}

interface YearlySeasonalityChartProps {
  yearlyData: YearlySeasonalityResponse;
  tooltip: TooltipState | null;
  setTooltip: (t: TooltipState | null) => void;
  chartHeight: number;
}

export default function YearlySeasonalityChart({
  yearlyData,
  tooltip,
  setTooltip,
  chartHeight,
}: YearlySeasonalityChartProps) {
  if (!yearlyData || yearlyData.average.length === 0) {
    return (
      <div className="flex items-center justify-center" style={{ height: chartHeight, color: 'var(--text-muted)' }}>Нет данных</div>
    );
  }

  const avg = yearlyData.average;
  const cur = yearlyData.current;
  const maxTD = yearlyData.max_trading_days || Math.max(...avg.map(a => a.td), ...cur.map(c => c.td), 250);

  const allPcts = [...avg.map(a => a.avg_pct), ...cur.map(c => c.pct)];
  const rawMin = Math.min(...allPcts);
  const rawMax = Math.max(...allPcts);
  const range = rawMax - rawMin || 1;
  const yMin = rawMin - range * 0.1;
  const yMax = rawMax + range * 0.1;

  const scX = (td: number) => td / maxTD;
  const scY = (pct: number) => 1 - (pct - yMin) / (yMax - yMin);

  // Y ticks
  const yTickCount = 6;
  const yTicks = Array.from({ length: yTickCount }, (_, i) => {
    const val = yMin + ((yMax - yMin) * i) / (yTickCount - 1);
    return { value: val, pct: scY(val) * 100 };
  });

  // Month tick positions
  const monthPositions: { td: number; label: string }[] = [];
  const monthLabels = ['', 'Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек'];
  const seenMonths = new Set<number>();
  for (const p of avg) {
    if (!seenMonths.has(p.month)) {
      seenMonths.add(p.month);
      monthPositions.push({ td: p.td, label: monthLabels[p.month] || '' });
    }
  }

  // SVG paths
  const avgPath = avg.map((p, i) =>
    `${i === 0 ? 'M' : 'L'} ${scX(p.td) * 1000} ${scY(p.avg_pct) * 500}`
  ).join(' ');

  const curPath = cur.map((p, i) =>
    `${i === 0 ? 'M' : 'L'} ${scX(p.td) * 1000} ${scY(p.pct) * 500}`
  ).join(' ');

  const PL = cssVar('--chart-pad-left', PADDING.left), PR = cssVar('--chart-pad-right-dual', PADDING.rightDual), PT = PADDING.top, PB = PADDING.bottom;

  return (
    <div>
      {/* Legend */}
      <div className="flex justify-center gap-5 text-sm mb-3">
        <span className="flex items-center gap-2">
          <span className="w-6 h-0.5 rounded" style={{ backgroundColor: CHART_COLORS.muted, display: 'inline-block' }} />
          <span className="text-theme-secondary font-medium">
            Среднее ({yearlyData.years_range})
          </span>
        </span>
        <span className="flex items-center gap-2">
          <span className="w-6 h-0.5 rounded" style={{ backgroundColor: CHART_COLORS.accent, display: 'inline-block' }} />
          <span className="text-theme-primary font-medium">
            {yearlyData.current_year}
          </span>
        </span>
      </div>

      {/* Floating date on hover */}
      <div className="relative" style={{ height: 22 }}>
        {tooltip?.yearlyCurDate && (
          <div className="absolute pointer-events-none" style={{ left: tooltip.x, transform: 'translateX(-50%)' }}>
            <span className="text-[11px] text-theme-secondary bg-theme-tertiary/90 backdrop-blur-sm px-2 py-0.5 rounded-md border border-theme">
              {tooltip.yearlyCurDate}
            </span>
          </div>
        )}
      </div>

      {/* Chart */}
      <div className="relative cursor-crosshair" style={{ height: 'var(--chart-height, 420px)' }}
        onMouseMove={(e) => {
          const rect = e.currentTarget.getBoundingClientRect();
          const x = e.clientX - rect.left;
          const chartW = rect.width - PL - PR;
          const frac = (x - PL) / chartW;
          if (frac < 0 || frac > 1) { setTooltip(null); return; }
          const targetTD = Math.round(frac * maxTD);

          // Find closest average point
          let closestAvg = avg[0];
          for (const p of avg) {
            if (Math.abs(p.td - targetTD) < Math.abs(closestAvg.td - targetTD)) closestAvg = p;
          }
          // Find closest current point
          let closestCur = cur.length > 0 ? cur[0] : null;
          if (cur.length > 0) {
            for (const p of cur) {
              if (Math.abs(p.td - targetTD) < Math.abs(closestCur!.td - targetTD)) closestCur = p;
            }
            if (closestCur && Math.abs(closestCur.td - targetTD) > 5) closestCur = null;
          }

          setTooltip({
            x,
            y: e.clientY - rect.top,
            yearlyAvgPct: closestAvg.avg_pct,
            yearlyCurPct: closestCur?.pct,
            yearlyCurDate: closestCur?.date,
          });
        }}
        onMouseLeave={() => setTooltip(null)}
      >
        {/* Chart area */}
        <div className="absolute" style={{ left: PL, right: PR, top: PT, bottom: PB }}>
          <svg viewBox="0 0 1000 500" preserveAspectRatio="none" width="100%" height="100%">
            {/* Grid lines */}
            {yTicks.map((t, i) => (
              <line key={i} x1="0" x2="1000"
                y1={t.pct / 100 * 500} y2={t.pct / 100 * 500}
                stroke={GRID.minor} strokeWidth="1"
                vectorEffect="non-scaling-stroke" />
            ))}
            {/* Zero line */}
            <line x1="0" x2="1000"
              y1={scY(0) * 500} y2={scY(0) * 500}
              stroke="rgba(255,255,255,0.2)" strokeWidth="1"
              vectorEffect="non-scaling-stroke" />
            {/* Month separators */}
            {monthPositions.slice(1).map((mp, i) => (
              <line key={i} x1={scX(mp.td) * 1000} x2={scX(mp.td) * 1000}
                y1="0" y2="500"
                stroke={GRID.minor} strokeWidth="1"
                vectorEffect="non-scaling-stroke" />
            ))}
            {/* Average line - grey dashed */}
            <path d={avgPath} fill="none" stroke={CHART_COLORS.muted} strokeWidth="1.5"
              vectorEffect="non-scaling-stroke"
              strokeLinecap="round" strokeLinejoin="round" opacity="0.8" />
            {/* Current year line - accent solid */}
            {cur.length > 0 && (
              <path d={curPath} fill="none" stroke={CHART_COLORS.accent} strokeWidth="2"
                vectorEffect="non-scaling-stroke"
                strokeLinecap="round" strokeLinejoin="round" />
            )}
            {/* Crosshair */}
            {tooltip && (() => {
              const frac = (tooltip.x - PL) / (document.querySelector('.cursor-crosshair')?.getBoundingClientRect().width ?? 800);
              const svgX = Math.max(0, Math.min(1000, frac * 1000 / ((document.querySelector('.cursor-crosshair')?.getBoundingClientRect().width ?? 800) - PL - PR) * (document.querySelector('.cursor-crosshair')?.getBoundingClientRect().width ?? 800)));
              return (
                <line x1={svgX} x2={svgX}
                  y1="0" y2="500"
                  stroke={CROSSHAIR.color} strokeWidth={CROSSHAIR.strokeWidth}
                  vectorEffect="non-scaling-stroke" strokeDasharray={CROSSHAIR.dashArray} />
              );
            })()}
          </svg>
        </div>

        {/* Y labels (right side) */}
        {yTicks.map((t, i) => {
          const chartH = (typeof window !== 'undefined' ? parseFloat(getComputedStyle(document.documentElement).getPropertyValue('--chart-height')) || 420 : 420);
          const topPx = PT + (t.pct / 100) * (chartH - PT - PB);
          return (
            <div key={i} className="absolute pointer-events-none"
              style={{ right: 4, top: topPx, transform: 'translateY(-50%)' }}>
              <span className="font-semibold text-[10px]" style={{ color: 'var(--text-muted)' }}>
                {t.value > 0 ? '+' : ''}{t.value.toFixed(1)}%
              </span>
            </div>
          );
        })}

        {/* X labels - month names from data */}
        {monthPositions.map((mp, i) => (
          <div key={i} className="absolute pointer-events-none font-semibold text-[10px] md:text-xs"
            style={{
              left: `${PL + scX(mp.td) * (100 - (PL + PR) * 100 / (typeof window !== 'undefined' ? window.innerWidth : 1000))}%`,
              bottom: 4,
              color: 'var(--text-muted)',
              transform: 'translateX(-50%)',
            }}>
          </div>
        ))}
        <div className="absolute flex justify-between font-semibold text-[10px] md:text-xs"
          style={{ left: PL, right: PR, bottom: 4, color: 'var(--text-muted)' }}>
          {monthPositions.map(mp => (
            <span key={mp.td}>{mp.label}</span>
          ))}
        </div>

        {/* Tooltip */}
        {tooltip?.yearlyAvgPct !== undefined && (() => {
          const isRight = tooltip.x > 300;
          return (
            <div className="absolute pointer-events-none z-30"
              style={{ left: isRight ? tooltip.x - 150 : tooltip.x + 12, top: Math.max(tooltip.y - 40, 4) }}>
              <div className={TOOLTIP.containerClass}>
                <div className="flex items-center gap-2">
                  <span className={TOOLTIP.dotSize} style={{ backgroundColor: CHART_COLORS.muted }} />
                  <span className={TOOLTIP.labelClass}>Среднее</span>
                  <span className={`${TOOLTIP.valueClass} ml-auto pl-2`} style={{ color: CHART_COLORS.muted }}>
                    {tooltip.yearlyAvgPct > 0 ? '+' : ''}{tooltip.yearlyAvgPct.toFixed(2)}%
                  </span>
                </div>
                {tooltip.yearlyCurPct !== undefined && (
                  <div className="flex items-center gap-2 mt-0.5">
                    <span className={TOOLTIP.dotSize} style={{ backgroundColor: CHART_COLORS.accent }} />
                    <span className={TOOLTIP.labelClass}>{yearlyData.current_year}</span>
                    <span className={`${TOOLTIP.valueClass} ml-auto pl-2`} style={{ color: CHART_COLORS.accent }}>
                      {tooltip.yearlyCurPct > 0 ? '+' : ''}{tooltip.yearlyCurPct.toFixed(2)}%
                    </span>
                  </div>
                )}
              </div>
            </div>
          );
        })()}
      </div>
    </div>
  );
}
