import { useLayoutEffect, useState, useMemo } from 'react';
import type { YearlySeasonalityResponse } from '../../services/api';
import { CHART_COLORS, PADDING, cssVar } from '../../config/chartTheme';
import { ChartGrid, ChartCrosshair, ChartDateLabel, ChartTooltip, TooltipRow, ChartYAxis, ChartXAxis } from '../chart';
import ChartNavigator from '../ChartNavigator';
import ChartWatermark from '../ChartWatermark';

interface TooltipState {
  x: number;
  y: number;
  yearlyAvgPct?: number;
  yearlyCurPct?: number;
  yearlyCurDate?: string;
  yearlyTd?: number;
  /** σ-отклонение текущего года от средней */
  yearlySigma?: number;
}

interface SeriesMeta {
  key: string;
  label: string;
  color: string;
}

interface YearlySeasonalityChartProps {
  yearlyData: YearlySeasonalityResponse;
  /** Мульти-серии [base, ...extras]. Если null — рисуем 1 линию из yearlyData. */
  seriesData?: YearlySeasonalityResponse[] | null;
  /** Метки серий — цвета и лейблы. */
  seriesMeta?: SeriesMeta[];
  tooltip: TooltipState | null;
  setTooltip: (t: TooltipState | null) => void;
  chartHeight: number;
}

export default function YearlySeasonalityChart({
  yearlyData,
  seriesData,
  seriesMeta,
  tooltip,
  setTooltip,
  chartHeight,
}: YearlySeasonalityChartProps) {
  // CSS-reveal на mount (key-based remount в parent)
  const [revealed, setRevealed] = useState(false);
  // Navigator range — [startIdx, endIdx] в координатах bucket'ов baseAvg.
  // null = показываем весь год.
  const [navRange, setNavRange] = useState<[number, number] | null>(null);
  useLayoutEffect(() => {
    if (yearlyData.average.length > 0 && !revealed) setRevealed(true);
  }, [yearlyData.average.length, revealed]);

  if (!yearlyData || yearlyData.average.length === 0) {
    return (
      <div className="flex items-center justify-center" style={{ height: chartHeight, color: 'var(--text-muted)' }}>Нет данных</div>
    );
  }

  // Когда выбран ОДИН период без extras, seriesData не передаётся (SeasonalityPage
  // оптимизирует — не делает лишний промис). seriesMeta при этом ВСЕГДА содержит
  // 1 entry с цветом. Используем meta когда есть, series fallback'ится на yearlyData.
  // Это убирает баг "линия серая когда выбран только один период" — теперь цвет
  // берётся из seriesMeta[0] (обычно forest green / accent).
  const series = seriesData && seriesData.length > 0 ? seriesData : [yearlyData];
  const meta: SeriesMeta[] = seriesMeta && seriesMeta.length > 0
    ? seriesMeta
    : [{ key: 'base', label: `Период с ${yearlyData.years_range?.split('-')[0] ?? ''}`, color: CHART_COLORS.muted }];
  const safeCount = Math.min(series.length, meta.length);
  const allSeries = series.slice(0, safeCount);
  const allMeta: SeriesMeta[] = meta.slice(0, safeCount);

  const baseAvg = yearlyData.average;
  const cur = yearlyData.current;
  const fullMaxTD = yearlyData.max_trading_days || 252;

  // Navigator data (для миниатюры внизу) — базовая серия
  const navData = useMemo(() =>
    baseAvg.map(p => ({ time: String(p.td), value: p.avg_pct })),
    [baseAvg]);

  // Visible range: если навигатор активен, фильтруем по td
  const navStart = navRange ? navRange[0] : 0;
  const navEnd = navRange ? navRange[1] : baseAvg.length - 1;
  const visibleTdMin = baseAvg[navStart]?.td ?? 0;
  const visibleTdMax = baseAvg[navEnd]?.td ?? fullMaxTD;
  void visibleTdMax; // используется в scX

  // Фильтр: оставить только точки в видимом диапазоне td
  const filterByTd = <T extends { td: number }>(arr: T[]): T[] =>
    arr.filter(p => p.td >= visibleTdMin && p.td <= visibleTdMax);

  const visAllSeries = allSeries.map(s => ({
    ...s,
    average: filterByTd(s.average),
  }));
  const visCur = filterByTd(cur);

  // Y-scale по ВИДИМОЙ части
  const allPcts = [
    ...visAllSeries.flatMap(s => s.average.map(a => a.avg_pct)),
    ...visCur.map(c => c.pct),
  ];
  if (allPcts.length === 0) allPcts.push(0);
  const rawMin = Math.min(...allPcts);
  const rawMax = Math.max(...allPcts);
  const range = rawMax - rawMin || 1;
  const yMin = rawMin - range * 0.1;
  const yMax = rawMax + range * 0.1;

  // scX нормализует td в видимый диапазон [0..1]
  const scX = (td: number) => (td - visibleTdMin) / (visibleTdMax - visibleTdMin || 1);
  const scY = (pct: number) => 1 - (pct - yMin) / (yMax - yMin);

  // Y ticks
  const yTickCount = 6;
  const yTicks = Array.from({ length: yTickCount }, (_, i) => {
    const val = yMin + ((yMax - yMin) * i) / (yTickCount - 1);
    return { value: val, pct: scY(val) * 100 };
  });

  // Month separators (из base avg для вертикальных линий)
  const monthLabels = ['', 'Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек'];
  const monthPositions: { td: number; label: string }[] = [];
  const seenMonths = new Set<number>();
  for (const p of baseAvg) {
    if (!seenMonths.has(p.month)) {
      seenMonths.add(p.month);
      monthPositions.push({ td: p.td, label: monthLabels[p.month] || '' });
    }
  }

  const getApproxMonth = (td: number): string => {
    let match = monthLabels[baseAvg[0]?.month || 1];
    for (const p of baseAvg) {
      if (p.td <= td) match = monthLabels[p.month] || match;
      else break;
    }
    return match;
  };

  // SVG paths для каждой ВИДИМОЙ серии
  const seriesPaths = visAllSeries.map(s =>
    s.average.map((p, i) =>
      `${i === 0 ? 'M' : 'L'} ${scX(p.td) * 1000} ${scY(p.avg_pct) * 500}`
    ).join(' ')
  );

  const curPath = visCur.map((p, i) =>
    `${i === 0 ? 'M' : 'L'} ${scX(p.td) * 1000} ${scY(p.pct) * 500}`
  ).join(' ');

  // X-axis: месяцы (Янв-Дек) — покрывают всю ширину графика.
  // Реальные даты из cur покрывали бы только часть года (до текущей даты),
  // а средняя линия идёт до декабря — получался бы пустой участок без подписей.
  const xLabels = monthPositions.map(mp => mp.label);

  // Унифицировано с SimpleChart/FlowsHistogram: PT/PB теперь тоже читаются из CSS-vars,
  // а не из JS-константы PADDING. Это устраняет drift (PADDING.top=10 vs --chart-pad-top=19px).
  const PL = cssVar('--chart-pad-left', PADDING.left);
  const PR = cssVar('--chart-pad-right-single', PADDING.rightSingle);
  const PT = cssVar('--chart-pad-top', PADDING.top);
  const PB = cssVar('--chart-pad-bottom', PADDING.bottom);

  // Unified pointer handler — вызывается и от mouse, и от touch.
  // Извлекает clientX/Y → тот же код для обоих событий. Без этого на mobile
  // пользователь не мог пальцем "водить" по чарту, только тыкать.
  const handlePointerMove = (clientX: number, clientY: number, el: HTMLElement) => {
    const rect = el.getBoundingClientRect();
    const mouseX = clientX - rect.left;
    const chartW = rect.width - PL - PR;
    const frac = (mouseX - PL) / chartW;
    if (frac < 0 || frac > 1) { setTooltip(null); return; }
    const targetTD = visibleTdMin + Math.round(frac * (visibleTdMax - visibleTdMin));

    const visBaseAvg = visAllSeries[0]?.average ?? [];
    let closestAvg = visBaseAvg[0] ?? baseAvg[0];
    for (const p of visBaseAvg) {
      if (Math.abs(p.td - targetTD) < Math.abs(closestAvg.td - targetTD)) closestAvg = p;
    }

    let closestCur = null as (typeof cur[number] | null);
    if (visCur.length > 0) {
      const lastCurTd = visCur[visCur.length - 1].td;
      if (targetTD <= lastCurTd) {
        let candidate = visCur[0];
        for (const p of visCur) {
          if (Math.abs(p.td - targetTD) < Math.abs(candidate.td - targetTD)) candidate = p;
        }
        closestCur = candidate;
      }
    }

    const snappedTD = closestAvg.td;
    const snappedX = PL + scX(snappedTD) * chartW;
    const displayDate = closestCur?.date
      ?? `≈ ${getApproxMonth(snappedTD)} ${yearlyData.current_year}`;

    setTooltip({
      x: snappedX,
      y: clientY - rect.top,
      yearlyAvgPct: closestAvg.avg_pct,
      yearlyCurPct: closestCur?.pct,
      yearlyCurDate: displayDate,
      yearlyTd: snappedTD,
    });
  };

  return (
    <div className={revealed ? 'chart-reveal' : ''}>
      {/* Legend — все серии + current.
          Размер шрифта И margin-bottom через CSS vars — переопределяются на wrapper
          в test-dashboard'е для компактного режима (мельче шрифт + меньше отступ). */}
      <div className="flex justify-center flex-wrap gap-4"
        style={{
          fontSize: 'var(--seasonality-legend-font-size, 14px)',
          marginBottom: 'var(--seasonality-legend-mb, 12px)',
        }}>
        {allMeta.map(m => (
          <span key={m.key} className="flex items-center gap-2">
            <span className="w-6 h-0.5 rounded" style={{ backgroundColor: m.color, display: 'inline-block' }} />
            <span className="text-theme-secondary font-medium">{m.label}</span>
          </span>
        ))}
        <span className="flex items-center gap-2">
          <span className="w-6 h-0.5 rounded" style={{ backgroundColor: CHART_COLORS.accent, display: 'inline-block' }} />
          <span className="text-theme-primary font-medium">{yearlyData.current_year}</span>
        </span>
      </div>

      {/* Floating date — absolute positioning ВНУТРИ chart-container'а через
          relative wrapper. Pill bottom приземляется на 4px выше верхней
          горизонтальной линии графика (которая находится на y=PT внутри SVG).
          Match pattern SimpleChart/FlowsHistogram. NO flow-space → no CLS на hover. */}

      {/* Chart */}
      <div
        className="relative cursor-crosshair"
        style={{
          aspectRatio: 'var(--seasonality-aspect-ratio, 2.4)',
          minHeight: 'var(--seasonality-min-height, 280px)',
          maxHeight: 'var(--seasonality-max-height, 550px)',
          // max-width: 100% — защита от aspect-ratio + min-height расширения
          // за parent на узких viewport'ах. Без этого chart выезжает за card
          // на mobile (детально см. SeasonalityHistogram.tsx).
          maxWidth: '100%',
          // touchAction: none — отключает браузерные жесты (scroll/zoom) при
          // водении пальцем по чарту. Критично для mobile UX.
          touchAction: 'none',
        }}
        onMouseMove={(e) => handlePointerMove(e.clientX, e.clientY, e.currentTarget)}
        onMouseLeave={() => setTooltip(null)}
        onTouchStart={(e) => {
          if (e.touches[0]) handlePointerMove(e.touches[0].clientX, e.touches[0].clientY, e.currentTarget);
        }}
        onTouchMove={(e) => {
          if (e.touches[0]) handlePointerMove(e.touches[0].clientX, e.touches[0].clientY, e.currentTarget);
        }}
        onTouchEnd={() => setTooltip(null)}
      >
        {/* Date pill — absolute over chart, bottom 5px above top grid line (y=PT).
            Pill height ~22px → wrapper top = PT - 5 - 22 = PT - 27. */}
        {tooltip?.yearlyCurDate && (
          <div className="absolute pointer-events-none" style={{ top: PT - 27, left: 0, right: 0, zIndex: 5 }}>
            <ChartDateLabel date={tooltip.yearlyCurDate} x={tooltip.x} />
          </div>
        )}
        <div className="absolute" style={{ left: PL, right: PR, top: PT, bottom: PB }}>
          <svg viewBox="0 0 1000 500" preserveAspectRatio="none" width="100%" height="100%">
            {/* Grid + zero line + month separators */}
            <ChartGrid
              yTicks={yTicks}
              xSeparators={monthPositions.slice(1).map(mp => scX(mp.td) * 100)}
            />

            {/* Series lines — все равноправны (одна толщина/opacity).
                Раньше 0-я была "среднее за все годы" и рисовалась приглушённой;
                теперь все серии — явно выбранные "Период с YYYY", все равнозначные. */}
            {seriesPaths.map((path, s) => (
              path ? (
                <path key={allMeta[s]?.key ?? s} d={path}
                  fill="none" stroke={allMeta[s]?.color ?? CHART_COLORS.muted}
                  strokeWidth="2"
                  vectorEffect="non-scaling-stroke"
                  strokeLinecap="round" strokeLinejoin="round"
                  opacity={0.85}
                />
              ) : null
            ))}

            {/* Current year line — accent, поверх */}
            {cur.length > 0 && (
              <path d={curPath} fill="none" stroke={CHART_COLORS.accent} strokeWidth="2"
                vectorEffect="non-scaling-stroke"
                strokeLinecap="round" strokeLinejoin="round" />
            )}

            {/* Crosshair */}
            {tooltip?.yearlyTd !== undefined && (
              <ChartCrosshair x={scX(tooltip.yearlyTd) * 1000} />
            )}
          </svg>
        </div>

        {/* Y labels — padRight=PR обязателен, иначе ChartYAxis использует
            default 80, и на mobile (PR=56) labels overlap data area на 12px
            («правая ось вошла в график»). */}
        <ChartYAxis
          ticks={yTicks}
          side="right"
          format={(v) => `${v > 0 ? '+' : ''}${v.toFixed(1)}%`}
          color="var(--axis-color, #9CA3B8)"
          padTop={PT}
          padBottom={PB}
          padRight={PR}
        />

        {/* X labels — реальные даты из current year или месяцы */}
        <ChartXAxis
          labels={xLabels}
          padLeft={PL}
          padRight={PR}
          color="var(--axis-color, #9CA3B8)"
        />

        {/* Tooltip */}
        {tooltip?.yearlyAvgPct !== undefined && (
          <ChartTooltip x={tooltip.x} y={tooltip.y}>
            {/* Все серии */}
            {allMeta.map((m, s) => {
              const seriesAvg = visAllSeries[s]?.average;
              const pt = seriesAvg?.find(p => p.td === tooltip.yearlyTd);
              if (!pt) return null;
              return (
                <TooltipRow key={m.key}
                  color={m.color}
                  label={m.label.length > 20 ? m.label.slice(0, 18) + '…' : m.label}
                  value={`${pt.avg_pct > 0 ? '+' : ''}${pt.avg_pct.toFixed(2)}%`}
                />
              );
            })}
            {/* Current year */}
            {tooltip.yearlyCurPct !== undefined && (
              <TooltipRow
                color={CHART_COLORS.accent}
                label={String(yearlyData.current_year)}
                value={`${tooltip.yearlyCurPct > 0 ? '+' : ''}${tooltip.yearlyCurPct.toFixed(2)}%`}
              />
            )}
            {/* Отклонение текущего года от среднего — в процентах */}
            {tooltip.yearlyCurPct !== undefined && tooltip.yearlyAvgPct !== undefined && (
              <div className="text-2xs text-theme-secondary mt-1 pt-1 border-t border-theme">
                {(() => {
                  const diff = tooltip.yearlyCurPct! - tooltip.yearlyAvgPct!;
                  const color = diff >= 0 ? CHART_COLORS.positive : CHART_COLORS.negative;
                  // std из базовой серии
                  const basePt = visAllSeries[0]?.average?.find(p => p.td === tooltip.yearlyTd);
                  return (
                    <>
                      Отклонение: <span className="font-semibold" style={{ color }}>
                        {diff >= 0 ? '+' : ''}{diff.toFixed(1)}%
                      </span>
                      {basePt?.std_pct ? (
                        <span className="opacity-60 ml-1">(разброс ±{basePt.std_pct.toFixed(1)}%)</span>
                      ) : null}
                    </>
                  );
                })()}
              </div>
            )}
          </ChartTooltip>
        )}
        {/* Watermark — привязан к data area через CSS calc().
            Gridlines идут от 0% до 100% SVG (yTicks от yMin до yMax),
            нижняя на padding.bottom от низа → bottom = pad-bottom + 5px зазор.
            Адаптивно к media query через CSS-вары. */}
        <ChartWatermark
          left="calc(var(--chart-pad-left, 100px) + 20px)"
          bottom="calc(var(--chart-pad-bottom, 50px) + 5px)"
        />
      </div>

      {/* Navigator — скользящее окно по году */}
      <ChartNavigator
        data={navData}
        onChange={(s, e) => setNavRange([s, e])}
        color={CHART_COLORS.muted}
      />
    </div>
  );
}
