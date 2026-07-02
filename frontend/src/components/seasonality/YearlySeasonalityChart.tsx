import { useLayoutEffect, useState, useRef } from 'react';
import type { YearlySeasonalityResponse } from '../../services/api';
import { CHART_COLORS, PADDING, cssVar } from '../../config/chartTheme';
import { ChartGrid, ChartCrosshair, ChartDateLabel, ChartTooltip, TooltipRow, ChartYAxis } from '../chart';
import ChartLegend from '../chart/ChartLegend';
import ChartWatermark from '../ChartWatermark';
import { useIsMobile } from '../../hooks/useIsMobile';
import { useViewportWidth } from '../../hooks/useViewportWidth';
import { axisFontSize } from '../chart/chartTypography';
import { measureText } from '../chart/measureText';

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
  /** Показывать линию текущего года (accent). По умолчанию true. */
  showCurrentYear?: boolean;
}

export default function YearlySeasonalityChart({
  yearlyData,
  seriesData,
  seriesMeta,
  tooltip,
  setTooltip,
  chartHeight,
  showCurrentYear = true,
}: YearlySeasonalityChartProps) {
  // На мобиле выводим квартальные подписи (Янв/Апр/Июл/Окт = 4 шт)
  // вместо 12 — иначе они накладываются на 311px viewport.
  const isMobile = useIsMobile();
  // CSS-reveal на mount (key-based remount в parent)
  const [revealed, setRevealed] = useState(false);
  useLayoutEffect(() => {
    if (yearlyData.average.length > 0 && !revealed) setRevealed(true);
  }, [yearlyData.average.length, revealed]);

  // === Pill positioning через ResizeObserver ===
  // Раньше pill был HTML-div с transform: translateY(-50%). В html2canvas
  // snapshot транформа не применялась — текст съезжал относительно заливки.
  // Calc-fix через `top: calc(% - halfHeight)` тоже неточен из-за того что
  // Inter font-box ≈ 1.29em (а не CSS line-height 1.2). Решение — переделать
  // pill на SVG <rect> + <text dominantBaseline="central"> (как в ChartLegend).
  // Pixel-perfect и в браузере, и в snapshot'е — SVG geometry spec-compliant.
  // Для этого нужны pixel-coords data-area, отслеживаем через ResizeObserver.
  const dataAreaRef = useRef<HTMLDivElement>(null);
  const [dataAreaSize, setDataAreaSize] = useState({ w: 0, h: 0 });
  useLayoutEffect(() => {
    const el = dataAreaRef.current;
    if (!el) return;
    const update = () => {
      const r = el.getBoundingClientRect();
      setDataAreaSize({ w: r.width, h: r.height });
    };
    update();
    const ro = new ResizeObserver(update);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  // Viewport-aware font size для pill (совпадает с Y-axis labels).
  const vw = useViewportWidth();
  const pillFontPx = axisFontSize(vw);

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
  // Тоггл «Текущий год»: пустой массив каскадно убирает линию, value-pill,
  // вклад в Y-шкалу и строки тултипа. Легенда гейтится отдельно (ниже).
  const cur = showCurrentYear ? yearlyData.current : [];
  const fullMaxTD = yearlyData.max_trading_days || 252;

  // Всегда показываем весь год: td нормализуется по полному диапазону торговых
  // дней [0..fullMaxTD] (scX(td) = td / fullMaxTD).
  const visibleTdMin = 0;
  const visibleTdMax = fullMaxTD;

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

  // X-axis: месяцы (Янв-Дек) — каждая подпись позиционируется absolute по
  // реальной X-координате месяца через scX(td). Раньше использовался
  // ChartXAxis с flex justify-between, который распределял 12 items равномерно
  // (0%, 9%, 18%, ..., 100%) — а реальные позиции месяцев в chart-area идут
  // 0%, 8.3%, 16.7%, ..., 91.7% (по trading-day). На широком экране разница
  // незаметна, на mobile при выводе квартальных меток (idx 0,3,6,9) "Окт"
  // оказывался на 82% labels-area вместо реальных 75% chart-data — отсюда
  // визуальный сдвиг. Теперь label положение точно совпадает с месяцем в данных.
  // Фильтр по td: если активен navigator с узким окном, рендерим только те
  // month-метки, чей td попадает в видимый диапазон. Без этого метки вне окна
  // получали scX(td) < 0 или > 1, позиционировались левее/правее dataArea и
  // визуально «уезжали» за видимые границы при перемещении navigator'а.
  const visibleMonthLabels = monthPositions
    .map((mp, idx) => ({ ...mp, idx }))
    .filter(mp => mp.td >= visibleTdMin && mp.td <= visibleTdMax)
    .filter(mp => !isMobile || mp.idx % 3 === 0);

  // PL/PR — seasonality-specific (12px/32px на mobile, 60/70 на desktop)
  // вместо общих --chart-pad-* (34/38 mobile, 100/95 desktop). Это даёт
  // yearly chart на mobile +30% ширины data-area. Y-labels помещаются
  // в 32px при компактном fluid font (8-11px).
  const PL = cssVar('--seasonality-chart-pad-left', PADDING.left);
  const PR = cssVar('--seasonality-chart-pad-right', PADDING.rightSingle);
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
      {/* Легенда — серии-периоды (среднее + сравниваемые года + текущий год),
          каждая со своим цветом линии. Название актива убрано — оно есть
          в заголовке карточки, дублирование выглядело перегружено. */}
      <div
        style={{ marginBottom: 'var(--seasonality-legend-mb, var(--chart-legend-mb, 2px))' }}
        className="flex items-center justify-center gap-3 flex-wrap"
      >
        <ChartLegend
          items={[
            ...allMeta.map(m => ({ color: m.color, label: m.label })),
            ...(cur.length > 0
              ? [{ color: CHART_COLORS.accent, label: String(yearlyData.current_year) }]
              : []),
          ]}
          fontWeight={600}
          gap={16}
          style={{ color: 'var(--text-primary)' }}
        />
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
        {/* Date pill — absolute над верхней грид-линией (y=PT): низ текста
            (~14px) на 3px ниже линии, как PILL_GAP_ABOVE_LINE в остальных
            графиках → wrapper top = PT + 3 - 14 = PT - 11. */}
        {tooltip?.yearlyCurDate && (
          <div className="absolute pointer-events-none" style={{ top: PT - 11, left: 0, right: 0, zIndex: 5 }}>
            <ChartDateLabel date={tooltip.yearlyCurDate} x={tooltip.x} />
          </div>
        )}
        <div ref={dataAreaRef} className="absolute" style={{ left: PL, right: PR, top: PT, bottom: PB }}>
          {/* Watermark — ВНУТРИ inner data-area div'а (а не chartContainer'а).
              Так absolute left/bottom считаются от границ data-area, а не от
              container'а. Раньше watermark был снаружи с offset через
              `calc(--chart-pad-bottom + 5px)`, и при ситуациях когда
              container/data-area рассинхронизировались (min-height vs
              aspect-ratio override на узком экране) — watermark «сползал»
              ниже X-labels. Теперь координаты не зависят от padding-vars. */}
          <ChartWatermark left="6px" bottom="6px" />

          <svg viewBox="0 0 1000 500" preserveAspectRatio="none" width="100%" height="100%">
            {/* Grid + zero line + month separators.
                zeroPct: позиция 0 на оси Y. emphasizeZero=true → жирная контрастная
                линия (text-primary @ 1.75px @ opacity 0.6) — yearly чарт показывает
                avg % изменения относительно "точки старта", и нулевая линия — это
                водораздел между накопленным ростом и накопленным падением. */}
            <ChartGrid
              yTicks={yTicks}
              zeroPct={yMin <= 0 && yMax >= 0 ? scY(0) * 100 : undefined}
              emphasizeZero
              xSeparators={monthPositions.slice(1).map(mp => scX(mp.td) * 100)}
            />

            {/* Series lines — все равноправны (одна толщина/opacity).
                Раньше 0-я была "среднее за все годы" и рисовалась приглушённой;
                теперь все серии — явно выбранные "Период с YYYY", все равнозначные.
                strokeWidth=3 совпадает с tokens.linePrimaryW в SimpleChart (OI/Buffett) —
                визуальная консистентность линий между индикаторами. */}
            {seriesPaths.map((path, s) => (
              path ? (
                <path key={allMeta[s]?.key ?? s} d={path}
                  fill="none" stroke={allMeta[s]?.color ?? CHART_COLORS.muted}
                  strokeWidth="3"
                  vectorEffect="non-scaling-stroke"
                  strokeLinecap="round" strokeLinejoin="round"
                  opacity={0.85}
                />
              ) : null
            ))}

            {/* Current year line — accent, поверх */}
            {cur.length > 0 && (
              <path d={curPath} fill="none" stroke={CHART_COLORS.accent} strokeWidth="3"
                vectorEffect="non-scaling-stroke"
                strokeLinecap="round" strokeLinejoin="round" />
            )}

            {/* Crosshair */}
            {tooltip?.yearlyTd !== undefined && (
              <ChartCrosshair x={scX(tooltip.yearlyTd) * 1000} />
            )}
          </svg>

          {/* Current value pill — SVG-based для pixel-perfect alignment в
              html2canvas snapshot. Раньше был HTML <div> с
              `transform: translateY(-50%)` — html2canvas терял transform и
              текст съезжал относительно заливки. Calc-fix без transform
              тоже неточен (Inter font-box ≈ 1.29em ≠ CSS line-height 1.2).
              SVG <text dominantBaseline="central"> — spec-compliant
              geometry, рендерится одинаково в браузере и snapshot'е.

              Render только когда ResizeObserver уже измерил data-area
              (dataAreaSize.h > 0) — иначе на первом тике координаты 0,
              pill рисуется в углу. */}
          {visCur.length > 0 && dataAreaSize.h > 0 && (() => {
            const last = visCur[visCur.length - 1];
            const yPx = scY(last.pct) * dataAreaSize.h;
            const sign = last.pct > 0 ? '+' : '';
            const digits = Math.abs(last.pct) >= 10 ? 0 : 1;
            const value = `${sign}${last.pct.toFixed(digits)}%`;
            const padX = 6;
            const padY = 2;
            const pillH = pillFontPx + padY * 2;
            const textW = measureText(value, pillFontPx, 700);
            const pillW = Math.ceil(textW) + padX * 2;
            // X-позиция pill: справа от data-area, +1px gap
            const pillX = dataAreaSize.w + 1;
            // SVG должен быть ШИРЕ чем data-area, иначе html2canvas
            // обрезает content по SVG-bounds даже при overflow:visible.
            // Делаем SVG width = data + pillW + 8px buffer чтобы pill
            // (расположенный за правым краем data-area) полностью попал
            // внутрь viewport SVG.
            const svgW = dataAreaSize.w + pillW + 8;
            const svgH = dataAreaSize.h;

            return (
              <svg
                width={svgW}
                height={svgH}
                viewBox={`0 0 ${svgW} ${svgH}`}
                style={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  pointerEvents: 'none',
                  overflow: 'visible',
                  zIndex: 2,
                }}
              >
                <rect
                  x={pillX}
                  y={yPx - pillH / 2}
                  width={pillW}
                  height={pillH}
                  rx={4}
                  ry={4}
                  fill={CHART_COLORS.accent}
                />
                <text
                  x={pillX + padX}
                  y={yPx}
                  textAnchor="start"
                  dominantBaseline="central"
                  fontSize={pillFontPx}
                  fontWeight={700}
                  fill="#fff"
                  style={{ fontVariantNumeric: 'tabular-nums', fontFamily: 'inherit' }}
                >
                  {value}
                </text>
              </svg>
            );
          })()}

          {/* Hover-точки на линиях (как на остальных графиках). Рисуем в
              отдельном pixel-space SVG (основной SVG растянут
              preserveAspectRatio=none → там circle стал бы эллипсом).
              Координата X совпадает с crosshair (scX(yearlyTd)). */}
          {tooltip?.yearlyTd !== undefined && dataAreaSize.h > 0 && (() => {
            const td = tooltip.yearlyTd;
            const xPx = scX(td) * dataAreaSize.w;
            const dots: { y: number; color: string }[] = [];
            visAllSeries.forEach((s, i) => {
              const pt = s.average.find(p => p.td === td);
              if (pt) dots.push({ y: scY(pt.avg_pct) * dataAreaSize.h, color: allMeta[i]?.color ?? CHART_COLORS.muted });
            });
            if (visCur.length > 0) {
              const cpt = visCur.find(p => p.td === td);
              if (cpt) dots.push({ y: scY(cpt.pct) * dataAreaSize.h, color: CHART_COLORS.accent });
            }
            if (dots.length === 0) return null;
            return (
              <svg
                width={dataAreaSize.w}
                height={dataAreaSize.h}
                viewBox={`0 0 ${dataAreaSize.w} ${dataAreaSize.h}`}
                style={{ position: 'absolute', top: 0, left: 0, pointerEvents: 'none', overflow: 'visible', zIndex: 3 }}
              >
                {dots.map((d, i) => (
                  <circle key={i} cx={xPx} cy={d.y} r={5}
                    fill={d.color} stroke="var(--bg-secondary)" strokeWidth={2} />
                ))}
              </svg>
            );
          })()}
        </div>

        {/* Y labels — padRight=PR обязателен, иначе ChartYAxis использует
            default 80, и на mobile (PR=56) labels overlap data area на 12px
            («правая ось вошла в график»).

            Format: для |v|≥10 убираем десятичную часть ("+25%" вместо "+25.0%"),
            иначе toFixed(1) ("+8.1%"). Это держит длину label'а ≤ 5 символов и
            на mobile (PR=32px, fluid font 8-11px) метки гарантированно влезают.
            Раньше "+25.0%" обрезался до "+25." при сильных годах. */}
        <ChartYAxis
          ticks={yTicks}
          side="right"
          format={(v) => {
            const sign = v > 0 ? '+' : '';
            const digits = Math.abs(v) >= 10 ? 0 : 1;
            return `${sign}${v.toFixed(digits)}%`;
          }}
          color="var(--axis-color, #9CA3B8)"
          padTop={PT}
          padBottom={PB}
          padRight={PR}
        />

        {/* X labels — каждая подпись на точной X-позиции своего месяца через
            scX(td). Бывший <ChartXAxis> с flex justify-between давал визуальный
            сдвиг подписей относительно chart data (см. visibleMonthLabels comment).
            bottom=10 (вместо 4) поднимает подписи ближе к chart-data — раньше
            был большой gap между нижней gridline и подписями на mobile. */}
        {visibleMonthLabels.map((mp, i) => {
          // Mobile (4 квартальных label'а): equal distribution across chart-area
          // (0%, 33%, 67%, 100%). Labels репрезентируют КВАРТАЛЫ, не конкретные
          // дни — поэтому visual balance важнее точного scX(td) alignment'а.
          // С scX'ом Окт оставался на 75% и справа была пустая ⅓.
          //
          // Desktop (12 monthly): остаётся scX(td) — там labels репрезентируют
          // КОНКРЕТНЫЕ месячные точки данных, alignment важен.
          const xPct = isMobile && visibleMonthLabels.length > 1
            ? (i / (visibleMonthLabels.length - 1)) * 100
            : scX(mp.td) * 100;
          return (
            <div
              key={mp.idx}
              className="absolute font-bold pointer-events-none"
              style={{
                left: `calc(${PL}px + ${xPct / 100} * (100% - ${PL}px - ${PR}px))`,
                bottom: 22,
                transform: 'translateX(-50%)',
                fontSize: 'var(--chart-font-x, 13px)',
                color: 'var(--axis-color, #9CA3B8)',
                whiteSpace: 'nowrap',
              }}
            >
              {mp.label}
            </div>
          );
        })}

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
        {/* Asset name перенесён в legend row выше (на одном уровне с легендой). */}
      </div>
    </div>
  );
}
