import type { SeasonalityResponse } from '../../services/api';
import { CHART_COLORS, CROSSHAIR, TOOLTIP } from '../../config/chartTheme';
import { ChartGrid, ChartCrosshair } from '../chart';

interface TooltipState {
  x: number;
  y: number;
  bar?: SeasonalityResponse['bars'][0];
}

interface SeasonalityHistogramProps {
  bars: SeasonalityResponse['bars'];
  animatedHeights: number[];
  maxAbs: number;
  tooltip: TooltipState | null;
  setTooltip: (t: TooltipState | null) => void;
}

export default function SeasonalityHistogram({
  bars,
  animatedHeights,
  maxAbs,
  tooltip,
  setTooltip,
}: SeasonalityHistogramProps) {
  if (bars.length === 0) {
    return (
      <div className="flex items-center justify-center" style={{ aspectRatio: '16/9', color: 'var(--text-muted)' }}>Нет данных</div>
    );
  }

  return (
    <div className="relative overflow-hidden pb-8 cursor-crosshair" style={{ aspectRatio: '2.4', minHeight: 280, maxHeight: 550 }}
      onMouseMove={(e) => {
        const rect = e.currentTarget.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const barAreaWidth = rect.width - 60;
        const idx = Math.floor(x / (barAreaWidth / bars.length));
        if (idx >= 0 && idx < bars.length) {
          setTooltip({ x: e.clientX - rect.left, y: e.clientY - rect.top, bar: bars[idx] });
        } else {
          setTooltip(null);
        }
      }}
      onMouseLeave={() => setTooltip(null)}>
      {/* SVG area */}
      <div className="absolute inset-0" style={{ right: 80 }}>
        <svg viewBox="0 0 1000 500" preserveAspectRatio="none" width="100%" height="100%">
          {/* Bars */}
          {animatedHeights.length > 0 && bars.map((bar, i) => {
            const W = 1000;
            const H = 500;
            const slotW = W / bars.length;
            const bw = slotW * (bars.length > 12 ? 0.6 : 0.5);
            const bx = i * slotW + (slotW - bw) / 2;
            const midY = H / 2;
            const halfH = H * 0.38;
            const animVal = animatedHeights[i] ?? 0;
            const h = Math.max(Math.abs(animVal) * halfH, H * 0.005);
            return (
              <g key={bar.key}
                opacity={tooltip?.bar ? (tooltip.bar.key === bar.key ? 1 : 0.35) : 1}
                className="transition-opacity duration-150">
                {animVal >= 0 ? (
                  <rect x={bx} y={midY - h} width={bw} height={h}
                    fill={CHART_COLORS.positive} rx="3" />
                ) : (
                  <rect x={bx} y={midY} width={bw} height={h}
                    fill={CHART_COLORS.negative} rx="3" />
                )}
              </g>
            );
          })}

          {/* Grid lines */}
          <ChartGrid
            yTicks={[-maxAbs, -maxAbs / 2, maxAbs / 2, maxAbs].map(val => ({
              pct: (250 - (val / maxAbs) * 190) / 500 * 100,
            }))}
            zeroPct={(250 / 500) * 100}
          />

          {/* Vertical cursor */}
          {tooltip?.bar && (() => {
            const idx = bars.indexOf(tooltip.bar!);
            if (idx === -1) return null;
            const slotW = 1000 / bars.length;
            const cx = idx * slotW + slotW / 2;
            return (
              <ChartCrosshair x={cx} color={CROSSHAIR.accentColor}
                dashArray={CROSSHAIR.accentDashArray} opacity={CROSSHAIR.accentOpacity}
                y1={250 - 190} y2={250 + 190} />
            );
          })()}
        </svg>
      </div>

      {/* Floating date + tooltip */}
      {tooltip?.bar && (() => {
        const idx = bars.indexOf(tooltip.bar!);
        if (idx === -1) return null;
        const color = tooltip.bar!.avg_change >= 0 ? CHART_COLORS.positive : CHART_COLORS.negative;
        const valStr = `${tooltip.bar!.avg_change > 0 ? '+' : ''}${Math.abs(tooltip.bar!.avg_change) >= 0.01 ? tooltip.bar!.avg_change.toFixed(3) : tooltip.bar!.avg_change.toFixed(4)}%`;
        return (
          <>
            {/* Card */}
            <div className="absolute z-30 pointer-events-none"
              style={{
                left: tooltip.x > 300 ? tooltip.x - 168 : tooltip.x + 8,
                top: Math.min(Math.max(tooltip.y - 20, 4), 330)
              }}>
              <div className={TOOLTIP.containerClass}>
                <div className="flex items-center justify-between gap-3">
                  <div className="flex items-center gap-1.5 min-w-0">
                    <span className={`${TOOLTIP.dotSize} flex-shrink-0`} style={{ backgroundColor: color }} />
                    <span className={`${TOOLTIP.labelClass} truncate`}>{tooltip.bar!.avg_change >= 0 ? 'Рост' : 'Падение'}</span>
                  </div>
                  <span className={`${TOOLTIP.valueClass} whitespace-nowrap`} style={{ color }}>
                    {valStr}
                  </span>
                </div>
                <div className="text-[10px] text-theme-secondary mt-0.5">{tooltip.bar!.count} наблюдений</div>
              </div>
            </div>
          </>
        );
      })()}


      {/* Y labels right */}
      {[-maxAbs, -maxAbs / 2, 0, maxAbs / 2, maxAbs].map((val, i) => {
        const yPct = 50 - (val / maxAbs) * 38;
        const label = val === 0 ? '0' : `${val > 0 ? '+' : ''}${val.toFixed(2)}%`;
        return (
          <div key={i} className="absolute pointer-events-none"
            style={{ top: `${yPct}%`, right: 4, transform: 'translateY(-50%)' }}>
            <span className="font-semibold" style={{ fontSize: 'var(--chart-font-y, 16px)', color: 'var(--axis-color, #9CA3B8)' }}>{label}</span>
          </div>
        );
      })}

      {/* X labels */}
      <div className="absolute bottom-0 left-0 flex justify-between font-semibold px-2" style={{ right: 'var(--chart-pad-right-dual, 80px)', fontSize: 'var(--chart-font-x, 14px)', color: 'var(--axis-color, #9CA3B8)' }}>
        {bars.map((bar, i) => {
          const isMob = typeof window !== 'undefined' && window.innerWidth < 768;
          const showLabel = !isMob || bars.length <= 7 || i % 2 === 0;
          return (
            <span key={bar.key} className="text-center" style={{ width: `${100 / bars.length}%` }}>
              {showLabel ? bar.label : ''}
            </span>
          );
        })}
      </div>

    </div>
  );
}
