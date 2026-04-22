/**
 * MTD (Month-To-Date) — зум на один месяц.
 * Аналог годовой но в рамках месяца: все серии (base, без выбросов,
 * среднее с, показать год) + current year.
 */
import { useLayoutEffect, useState } from 'react';
import type { MtdResponse } from '../../services/api';
import { CHART_COLORS, PADDING, cssVar } from '../../config/chartTheme';
import { ChartGrid, ChartCrosshair, ChartTooltip, TooltipRow, ChartYAxis } from '../chart';

const MONTH_NAMES = ['','Январь','Февраль','Март','Апрель','Май','Июнь',
  'Июль','Август','Сентябрь','Октябрь','Ноябрь','Декабрь'];

interface SeriesMeta {
  key: string;
  label: string;
  color: string;
}

interface MtdChartProps {
  /** Базовая серия (все годы) */
  data: MtdResponse;
  /** Дополнительные серии [base, ...extras] — как в yearly */
  seriesData?: MtdResponse[] | null;
  seriesMeta?: SeriesMeta[];
  tooltip: { x: number; y: number; [k: string]: unknown } | null;
  setTooltip: (t: MtdChartProps['tooltip']) => void;
  chartHeight: number;
}

export default function MtdChart({ data, seriesData, seriesMeta, tooltip, setTooltip, chartHeight }: MtdChartProps) {
  const [revealed, setRevealed] = useState(false);
  useLayoutEffect(() => {
    if ((data.average.length > 0 || data.current.length > 0) && !revealed) setRevealed(true);
  }, [data.average.length, data.current.length, revealed]);

  const cur = data.current;

  // Собираем все серии
  const safeCount = seriesData && seriesMeta
    ? Math.min(seriesData.length, seriesMeta.length) : 0;
  const isMulti = safeCount >= 1;
  const allSeries = isMulti ? seriesData!.slice(0, safeCount) : [data];
  const allMeta: SeriesMeta[] = isMulti
    ? seriesMeta!.slice(0, safeCount)
    : [{ key: 'base', label: `Все годы (${data.average[0]?.count ?? 0})`, color: CHART_COLORS.muted }];

  // maxTD — длина БАЗОВОЙ серии (все годы). Это общий знаменатель:
  // средняя заканчивается на td где есть данные по большинству лет (~20).
  // Конкретный год или current могут быть длиннее (субботы) — обрезаем,
  // чтобы все линии были одной длины.
  const baseLastTd = data.average.length > 0 ? data.average[data.average.length - 1].td : 20;
  const maxTD = Math.max(baseLastTd, 15
  );

  if (allSeries.every(s => s.average.length === 0) && cur.length === 0) {
    return <div className="flex items-center justify-center" style={{ height: chartHeight, color: 'var(--text-muted)' }}>Нет данных</div>;
  }

  // Y-scale по всем сериям + current
  const allVals = [
    ...allSeries.flatMap(s => s.average.map(a => a.avg_pct)),
    ...cur.map(c => c.pct),
    0,
  ];
  const rawMin = Math.min(...allVals);
  const rawMax = Math.max(...allVals);
  const range = rawMax - rawMin || 1;
  const yMin = rawMin - range * 0.1;
  const yMax = rawMax + range * 0.1;

  const scX = (td: number) => td / maxTD;
  const scY = (v: number) => 1 - (v - yMin) / (yMax - yMin);

  const yTickCount = 6;
  const yTicks = Array.from({ length: yTickCount }, (_, i) => {
    const val = yMin + ((yMax - yMin) * i) / (yTickCount - 1);
    return { value: val, pct: scY(val) * 100 };
  });

  const toPath = (pts: { td: number; val: number }[]) =>
    pts.length > 0
      ? pts.map((p, i) => `${i === 0 ? 'M' : 'L'} ${scX(p.td) * 1000} ${scY(p.val) * 500}`).join(' ')
      : '';

  const curPath = toPath(cur.filter(p => p.td <= maxTD).map(p => ({ td: p.td, val: p.pct })));

  // X labels — каждый торговый день от 1 до maxTD
  const xSteps: number[] = [];
  for (let i = 1; i <= maxTD; i++) xSteps.push(i);

  // MTD не имеет Y-подписей слева — минимальный отступ для первой X-метки.
  // Справа — стандартный для Y-подписей. PT/PB унифицированы с остальными charts
  // через --chart-pad-top/bottom (было: хардкод из JS-константы → drift с CSS).
  const PL = cssVar('--mtd-pad-left', 20);
  const PR = cssVar('--chart-pad-right-single', PADDING.rightSingle);
  const PT = cssVar('--chart-pad-top', PADDING.top);
  const PB = cssVar('--chart-pad-bottom', PADDING.bottom); // X-подписи выносим ЗА контейнер графика

  // Все уникальные td для snap'а
  const allTds = [...new Set([
    ...allSeries.flatMap(s => s.average.map(a => a.td)),
    ...cur.map(c => c.td),
  ])].sort((a, b) => a - b);

  const handleMouseMove = (e: React.MouseEvent<HTMLDivElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const mouseX = e.clientX - rect.left;
    const chartW = rect.width - PL - PR;
    const frac = (mouseX - PL) / chartW;
    if (frac < 0 || frac > 1) { setTooltip(null); return; }
    const targetTD = Math.round(frac * maxTD);
    const snappedTD = allTds.length > 0
      ? allTds.reduce((best, td) => Math.abs(td - targetTD) < Math.abs(best - targetTD) ? td : best, allTds[0])
      : targetTD;
    const snappedX = PL + (snappedTD / maxTD) * chartW;
    const curPt = cur.find(p => p.td === snappedTD);

    setTooltip({
      x: snappedX, y: e.clientY - rect.top,
      mtdTd: snappedTD,
      mtdCur: curPt?.pct,
      mtdDate: curPt?.date,
    });
  };

  const t = tooltip as Record<string, unknown> | null;

  return (
    <div className={revealed ? 'chart-reveal' : ''}>
      <div className="flex justify-center text-sm mb-1">
        <span className="text-theme-primary font-semibold">{MONTH_NAMES[data.month]} — торговые дни</span>
      </div>

      {/* Legend */}
      <div className="flex justify-center flex-wrap gap-4 text-xs mb-2">
        {allMeta.map(m => (
          <span key={m.key} className="flex items-center gap-1.5">
            <span className="w-4 h-0.5 rounded" style={{ backgroundColor: m.color }} />
            <span className="text-theme-secondary">{m.label}</span>
          </span>
        ))}
        {cur.length > 0 && (
          <span className="flex items-center gap-1.5">
            <span className="w-4 h-0.5 rounded" style={{ backgroundColor: CHART_COLORS.accent }} />
            <span className="text-theme-primary">{data.current_year}</span>
          </span>
        )}
      </div>

      <div style={{ height: 'var(--chart-date-placeholder-height, 22px)' }} />

      <div className="relative cursor-crosshair" style={{
          aspectRatio: 'var(--seasonality-aspect-ratio, 2.4)',
          minHeight: 'var(--seasonality-min-height, 280px)',
          maxHeight: 'var(--seasonality-max-height, 550px)',
        }}
        onMouseMove={handleMouseMove}
        onMouseLeave={() => setTooltip(null)}
      >
        <div className="absolute" style={{ left: PL, right: PR, top: PT, bottom: PB }}>
          <svg viewBox="0 0 1000 500" preserveAspectRatio="none" width="100%" height="100%">
            <ChartGrid yTicks={yTicks} />

            {/* Series lines — пропускаем пустые серии */}
            {allMeta.map((m, s) => {
              const pts = allSeries[s]?.average;
              if (!pts || pts.length < 2) return null;
              const path = toPath(pts.map(p => ({ td: p.td, val: p.avg_pct })));
              if (!path) return null;
              return (
                <path key={m.key} d={path} fill="none" stroke={m.color}
                  strokeWidth={s === 0 ? '1.5' : '2'}
                  vectorEffect="non-scaling-stroke"
                  strokeLinecap="round" strokeLinejoin="round"
                  opacity={s === 0 ? 0.6 : 0.85} />
              );
            })}

            {/* Current year */}
            {curPath && (
              <path d={curPath} fill="none" stroke={CHART_COLORS.accent} strokeWidth="2.5"
                vectorEffect="non-scaling-stroke" strokeLinecap="round" strokeLinejoin="round" />
            )}

            {t?.mtdTd !== undefined && (
              <ChartCrosshair x={scX(t.mtdTd as number) * 1000} />
            )}
          </svg>
        </div>

        <ChartYAxis ticks={yTicks} side="right"
          format={(v) => `${v > 0 ? '+' : ''}${v.toFixed(1)}%`}
          padTop={PT} padBottom={PB} />

        {/* Tooltip */}
        {t?.mtdTd !== undefined && (
          <ChartTooltip x={t.x as number} y={t.y as number}>
            <div className="text-[10px] text-theme-secondary mb-1">День {t.mtdTd as number}</div>
            {allMeta.map((m, s) => {
              const pt = allSeries[s]?.average?.find(p => p.td === (t.mtdTd as number));
              if (!pt) return null;
              return (
                <TooltipRow key={m.key} color={m.color}
                  label={m.label.length > 16 ? m.label.slice(0, 14) + '…' : m.label}
                  value={`${pt.avg_pct > 0 ? '+' : ''}${pt.avg_pct.toFixed(2)}%`} />
              );
            })}
            {t.mtdCur !== undefined && (
              <TooltipRow color={CHART_COLORS.accent} label={String(data.current_year)}
                value={`${(t.mtdCur as number) > 0 ? '+' : ''}${(t.mtdCur as number).toFixed(2)}%`} />
            )}
            {/* Отклонение от базовой серии */}
            {t.mtdCur !== undefined && (() => {
              const basePt = allSeries[0]?.average?.find(p => p.td === (t.mtdTd as number));
              if (!basePt) return null;
              const diff = (t.mtdCur as number) - basePt.avg_pct;
              const color = diff >= 0 ? CHART_COLORS.positive : CHART_COLORS.negative;
              return (
                <div className="text-[10px] text-theme-secondary mt-1 pt-1 border-t border-white/10">
                  Отклонение: <span className="font-semibold" style={{ color }}>
                    {diff >= 0 ? '+' : ''}{diff.toFixed(1)}%
                  </span>
                  {basePt.std_pct > 0 && (
                    <span className="opacity-60 ml-1">(разброс ±{basePt.std_pct.toFixed(1)}%)</span>
                  )}
                </div>
              );
            })()}
          </ChartTooltip>
        )}
      </div>

      {/* X labels — СНАРУЖИ контейнера графика, чтобы не обрезались */}
      <div className="relative font-semibold" style={{
          height: 'var(--chart-xlabel-row-height, 24px)',
          marginLeft: PL,
          marginRight: PR,
          fontSize: 'var(--chart-font-x, 14px)',
          color: 'var(--axis-color, #9CA3B8)',
        }}>
        {xSteps.map(td => (
          <span key={td} style={{
            position: 'absolute',
            left: `${scX(td) * 100}%`,
            transform: 'translateX(-50%)',
          }}>{td}</span>
        ))}
      </div>
    </div>
  );
}
