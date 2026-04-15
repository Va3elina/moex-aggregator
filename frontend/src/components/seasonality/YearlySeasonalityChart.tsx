import type { YearlySeasonalityResponse } from '../../services/api';
import { CHART_COLORS, PADDING, cssVar } from '../../config/chartTheme';
import { ChartGrid, ChartCrosshair, ChartDateLabel, ChartTooltip, TooltipRow, ChartYAxis, ChartXAxis } from '../chart';

interface TooltipState {
  x: number;
  y: number;
  yearlyAvgPct?: number;
  yearlyCurPct?: number;
  yearlyCurDate?: string;
  // td ближайшей точки — для снэпа крестовины к фактическим данным (а не к мыши)
  yearlyTd?: number;
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

  // Вспомогательная функция: приблизительный месяц для заданного td
  // (если текущий год ещё не дошёл до этого td, показываем ≈ месяц).
  const getApproxMonth = (td: number): string => {
    let match = monthLabels[avg[0]?.month || 1];
    for (const p of avg) {
      if (p.td <= td) match = monthLabels[p.month] || match;
      else break;
    }
    return match;
  };

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
      {tooltip?.yearlyCurDate ? (
        <ChartDateLabel date={tooltip.yearlyCurDate} x={tooltip.x} />
      ) : (
        <div style={{ height: 22 }} />
      )}

      {/* Chart */}
      <div className="relative cursor-crosshair" style={{ aspectRatio: '2.4', minHeight: 280, maxHeight: 550 }}
        onMouseMove={(e) => {
          const rect = e.currentTarget.getBoundingClientRect();
          const mouseX = e.clientX - rect.left;
          const chartW = rect.width - PL - PR;
          const frac = (mouseX - PL) / chartW;
          if (frac < 0 || frac > 1) { setTooltip(null); return; }
          const targetTD = Math.round(frac * maxTD);

          // Ближайшая точка в avg (есть всегда на любом td в диапазоне)
          let closestAvg = avg[0];
          for (const p of avg) {
            if (Math.abs(p.td - targetTD) < Math.abs(closestAvg.td - targetTD)) closestAvg = p;
          }
          // Ближайшая точка в cur. Правило: 2026 показываем ТОЛЬКО если
          // курсор не вышел за последнюю имеющуюся точку текущего года.
          // Ранее был допуск ±5 td — это показывало "последнее значение 2026"
          // даже ПРАВЕЕ конца жёлтой линии, что выглядело как экстраполяция
          // в будущее.
          let closestCur = null as (typeof cur[number] | null);
          if (cur.length > 0) {
            const lastCurTd = cur[cur.length - 1].td;
            if (targetTD <= lastCurTd) {
              let candidate = cur[0];
              for (const p of cur) {
                if (Math.abs(p.td - targetTD) < Math.abs(candidate.td - targetTD)) candidate = p;
              }
              closestCur = candidate;
            }
          }

          // Снэп x-координаты крестовины к ближайшей точке данных.
          // Это устраняет визуальный рассинхрон между положением крестовины
          // и значениями в тултипе.
          const snappedTD = closestAvg.td;
          const snappedX = PL + (snappedTD / maxTD) * chartW;

          // Дата для метки: реальная из cur, иначе ≈ месяц + год из контекста
          const displayDate = closestCur?.date
            ?? `≈ ${getApproxMonth(snappedTD)} ${yearlyData.current_year}`;

          setTooltip({
            x: snappedX,
            y: e.clientY - rect.top,
            yearlyAvgPct: closestAvg.avg_pct,
            yearlyCurPct: closestCur?.pct,
            yearlyCurDate: displayDate,
            yearlyTd: snappedTD,
          });
        }}
        onMouseLeave={() => setTooltip(null)}
      >
        {/* Chart area */}
        <div className="absolute" style={{ left: PL, right: PR, top: PT, bottom: PB }}>
          <svg viewBox="0 0 1000 500" preserveAspectRatio="none" width="100%" height="100%">
            {/* Grid + month separators (без zero line — обе серии стартуют
                от 0%, нулевая линия создаёт скученность с -4.5%/+2.8%) */}
            <ChartGrid
              yTicks={yTicks}
              xSeparators={monthPositions.slice(1).map(mp => scX(mp.td) * 100)}
            />
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
            {/* Crosshair — используем уже снэпнутый td напрямую, без DOM-измерений */}
            {tooltip?.yearlyTd !== undefined && (
              <ChartCrosshair x={scX(tooltip.yearlyTd) * 1000} />
            )}
          </svg>
        </div>

        {/* Y labels (right side) */}
        <ChartYAxis
          ticks={yTicks}
          side="right"
          format={(v) => `${v > 0 ? '+' : ''}${v.toFixed(1)}%`}
          color="var(--axis-color, #9CA3B8)"
          padTop={PT}
          padBottom={PB}
        />

        {/* X labels - month names from data */}
        <ChartXAxis
          labels={monthPositions.map(mp => mp.label)}
          padLeft={PL}
          padRight={PR}
          color="var(--axis-color, #9CA3B8)"
        />

        {/* Tooltip */}
        {tooltip?.yearlyAvgPct !== undefined && (
          <ChartTooltip x={tooltip.x} y={tooltip.y}>
            <TooltipRow
              color={CHART_COLORS.muted}
              label="Среднее"
              value={`${tooltip.yearlyAvgPct > 0 ? '+' : ''}${tooltip.yearlyAvgPct.toFixed(2)}%`}
            />
            {tooltip.yearlyCurPct !== undefined && (
              <TooltipRow
                color={CHART_COLORS.accent}
                label={String(yearlyData.current_year)}
                value={`${tooltip.yearlyCurPct > 0 ? '+' : ''}${tooltip.yearlyCurPct.toFixed(2)}%`}
              />
            )}
          </ChartTooltip>
        )}
      </div>
    </div>
  );
}
