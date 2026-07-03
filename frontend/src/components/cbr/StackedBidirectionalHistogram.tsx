/**
 * StackedBidirectionalHistogram — стек-бар с двусторонним накоплением.
 *
 * Для каждого периода:
 *   • Категории с value > 0 накапливаются ВВЕРХ от 0
 *   • Категории с value < 0 накапливаются ВНИЗ от 0
 *
 * Архитектура — pattern FlowsHistogram (Притоки/Оттоки):
 *   • Chart-area внутри absolute div с CSS padding (top/bottom/left/right)
 *   • SVG width=100% height=100% preserveAspectRatio="none"
 *   • Bars в процентах от SVG (0-100%)
 *   • Y-axis labels — HTML absolute div справа
 *   • X-axis period labels — HTML absolute div снизу
 *
 * Это даёт chart-area реально расширяющийся до padding'ов, без issue
 * с aspect ratio viewBox vs container, которое раньше центрировало content.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { CbrFlowsPeriod } from '../../services/api';
import { getCategoryColor } from './cbrPalette';
import { getCategoryShortLabel } from './cbrCategoryInfo';
import { useTheme } from '../../contexts/ThemeContext';
import { useViewportWidth } from '../../hooks/useViewportWidth';
import ChartLegend, { type ChartLegendItem } from '../chart/ChartLegend';
import ChartDatePill from '../chart/ChartDatePill';
import ChartWatermark from '../ChartWatermark';
import { TOOLTIP } from '../../config/chartTheme';

interface Props {
  periods: CbrFlowsPeriod[];
  categories: string[];
  unit: string;
  height: number;
  loading?: boolean;
  /**
   * Явный триггер re-animation. Меняется когда parent хочет показать wave
   * заново (смена type активов / period selector). НЕ должен меняться
   * при toggle категорий (тогда animation не сбрасывается → нет «пустого
   * графика» при выключении категории по очереди).
   *
   * Если не передан — fallback на periods length+dates (старая логика).
   */
  animTrigger?: string;
  /**
   * Полный (нефильтрованный) список периодов — для расчёта м/м и г/г в
   * тултипе. При узком period-фильтре («1Г») соседи для сравнения лежат
   * за пределами видимых `periods`. Не передан → fallback на `periods`.
   */
  allPeriods?: CbrFlowsPeriod[];
}

interface HoverState {
  periodIdx: number;
  mouseX: number;
  mouseY: number;
}

// Padding chart-area + font-size через CSS variables (fluid clamp в index.css).
// Это даёт автоматическую адаптацию на mobile: --chart-pad-left/right на
// узких экранах меньше (~34-58px), на desktop ~95-100px. Font axis тоже
// scales дискретно (10→11→13→15→17px на breakpoints 320/375/425/768/1024/1440).
// Symmetric padding-left ≈ padding-right даёт chart-area по центру paper-card.
// Match с pattern FlowsHistogram/SimpleChart.
const MIN_BAR_H = 0.6;  // % минимум высоты сегмента

// Объём торгов периода = max(сумма нетто-покупок, |сумма нетто-продаж|).
// В balanced market покупки ≈ продажи, поэтому это «сколько денег
// прокрутилось». Совпадает с метрикой «Объём торгов» в тултипе.
function periodVolume(p: CbrFlowsPeriod, categories: string[]): number {
  let pos = 0, neg = 0;
  for (const cat of categories) {
    const v = p.values[cat] ?? 0;
    if (v > 0) pos += v;
    else neg += -v;
  }
  return Math.max(pos, neg);
}

export default function StackedBidirectionalHistogram({
  periods,
  categories,
  height,
  loading,
  animTrigger,
  allPeriods,
}: Props) {
  const { theme } = useTheme();
  const containerRef = useRef<HTMLDivElement>(null);
  // Chart-контейнер (зона ПОД легендой) — для клампа тултипа в plot-коридоре.
  const chartWrapRef = useRef<HTMLDivElement>(null);
  const [hover, setHover] = useState<HoverState | null>(null);
  const vw = useViewportWidth();
  const isMobile = vw < 768;

  // Entrance animation: grow-from-zero wave.
  // animProgress[i] ∈ [0, 1] — прогресс bar'а #i (height + stack scale).
  //
  // animKey — комбинируем ДВА сигнала:
  //   - animTrigger (от parent) — меняется при смене type/period, явный
  //     сигнал «перезапусти wave».
  //   - data-signature (длина + крайние даты) — меняется когда данные впервые
  //     приезжают. На первом открытии компонент рендерится в loading-состоянии
  //     с пустыми данными; эффект делает early-return, а animTrigger потом не
  //     меняется → entrance-анимация НЕ играла при первом открытии страницы.
  //     Signature ловит переход [] → загружено.
  //   Toggle категорий не меняет ни animTrigger, ни signature → wave корректно
  //   НЕ перезапускается.
  //
  // ВАЖНО: signature берём из allPeriods (ПОЛНЫЙ набор), а не из periods (срез
  // нижнего таймлайна-навигатора через navRange). Иначе драг навигатора менял
  // длину/крайние даты periods → wave переигрывалась на каждое движение окна.
  // Теперь — как в «Деньги в фондах»: волна привязана к fetched-данным, а
  // навигатор лишь слайсит уже отрисованные бары. Fallback на periods для
  // вызовов без allPeriods (мобилка без навигатора) — поведение не меняется.
  const sigPeriods = allPeriods ?? periods;
  const dataSig =
    `${sigPeriods.length}|${sigPeriods[0]?.end_date ?? ''}` +
    `|${sigPeriods[sigPeriods.length - 1]?.end_date ?? ''}`;
  const animKey = `${animTrigger ?? ''}|${dataSig}`;
  const [animProgress, setAnimProgress] = useState<number[]>(() =>
    new Array(periods.length).fill(0),
  );
  // Сброс прогресса СИНХРОННО при смене animKey (React-паттерн «adjust state
  // during render»). Без него один кадр бары рендерятся со старым animProgress:
  // при переключении на более длинный период правые бары (индексы за пределами
  // старого массива) попадают на `animProgress[i] ?? 1` = full — и видны на
  // полную высоту до того, как до них дойдёт wave.
  const [animatedKey, setAnimatedKey] = useState(animKey);
  if (animKey !== animatedKey) {
    setAnimatedKey(animKey);
    setAnimProgress(new Array(periods.length).fill(0));
  }
  useEffect(() => {
    if (periods.length === 0) return;
    setAnimProgress(new Array(periods.length).fill(0));
    const start = performance.now();
    // Slower wave (match с Притоки/Оттоки feel):
    const totalStagger = Math.min(800, periods.length * 70);
    const perBarDuration = 900;
    const totalDuration = totalStagger + perBarDuration;
    let raf = 0;
    const tick = (now: number) => {
      const elapsed = now - start;
      if (elapsed >= totalDuration) {
        setAnimProgress(new Array(periods.length).fill(1));
        return;
      }
      setAnimProgress(
        periods.map((_, i) => {
          const delay = periods.length > 1 ? (i / (periods.length - 1)) * totalStagger : 0;
          const localElapsed = Math.max(0, elapsed - delay);
          const t = Math.min(1, localElapsed / perBarDuration);
          return 1 - Math.pow(1 - t, 4);
        }),
      );
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [animKey]);
  // Y-axis симметричный max
  const yMax = useMemo(() => {
    if (!periods.length) return 10;
    let maxAbs = 0;
    for (const p of periods) {
      let pos = 0, neg = 0;
      for (const cat of categories) {
        const v = p.values[cat] ?? 0;
        if (v >= 0) pos += v;
        else neg += -v;
      }
      maxAbs = Math.max(maxAbs, pos, neg);
    }
    return Math.max(10, Math.ceil((maxAbs * 1.12) / 10) * 10);
  }, [periods, categories]);

  // 5 уровней Y-axis: [-max, -max/2, 0, max/2, max]
  const yTicks = useMemo(
    () => [-yMax, -yMax / 2, 0, yMax / 2, yMax],
    [yMax],
  );

  const legendItems = useMemo<ChartLegendItem[]>(
    () => categories.map((cat) => ({ color: getCategoryColor(cat, theme), label: getCategoryShortLabel(cat) })),
    [categories, theme],
  );

  // ─── Hover handler ───────────────────────────────────────────────────────
  // Bars лежат внутри chart-area, которая отступает от краёв container на
  // var(--chart-pad-left) и var(--chart-pad-right-single). Считаем xRatio
  // относительно chart-area, не container — иначе у краёв курсор активирует
  // wrong bar (раньше чем визуально подошёл).
  const handleMove = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
    const target = e.currentTarget;
    const rect = target.getBoundingClientRect();
    const cs = getComputedStyle(document.documentElement);
    const padLeft = parseFloat(cs.getPropertyValue('--chart-pad-left')) || 100;
    const padRight = parseFloat(cs.getPropertyValue('--chart-pad-right-single')) || 95;
    const chartAreaWidth = rect.width - padLeft - padRight;
    if (chartAreaWidth <= 0 || !periods.length) {
      setHover(null);
      return;
    }
    const xWithinChart = e.clientX - rect.left - padLeft;
    const xRatio = xWithinChart / chartAreaWidth;
    if (xRatio < 0 || xRatio > 1) {
      setHover(null);
      return;
    }
    const nearestIdx = Math.min(
      periods.length - 1,
      Math.max(0, Math.floor(xRatio * periods.length)),
    );
    const outer = containerRef.current;
    const outerRect = outer ? outer.getBoundingClientRect() : rect;
    setHover({
      periodIdx: nearestIdx,
      mouseX: e.clientX - outerRect.left,
      mouseY: e.clientY - outerRect.top,
    });
  }, [periods.length]);

  const handleLeave = useCallback(() => setHover(null), []);

  // ─── States ──────────────────────────────────────────────────────────────
  if (loading) {
    return (
      <div className="rounded-2xl" style={{ height: `${height}px`, background: 'var(--bg-secondary)', opacity: 0.6 }} />
    );
  }
  if (!periods.length) {
    return (
      <div className="flex items-center justify-center"
        style={{ height: `${height}px`, color: 'var(--text-muted)', fontSize: 'var(--fs-sm)' }}>
        Нет данных
      </div>
    );
  }

  // Динамическая толщина окантовки
  const outlineWidth = periods.length <= 20 ? 1
    : periods.length <= 50 ? 0.7
    : periods.length <= 100 ? 0.4
    : 0.25;

  const barSlot = 100 / periods.length;  // % одного period-слота
  const barW = barSlot * 0.65;            // 65% слота — сам бар
  const barOffset = (barSlot - barW) / 2;

  return (
    <div
      ref={containerRef}
      className="relative flex flex-col"
      style={{ height: `${height}px` }}
    >
      {/* ═══ Зона легенды — auto height, не сжимается при wrap (7 категорий
              могут перейти на 2 строки на узком viewport). flex-shrink: 0
              чтобы legend никогда не уходила за верхний край.
              Отступы — единые токены геометрии графиков: сверху
              --chart-legend-top-gap (зазор от верха paper-card), снизу
              --chart-legend-mb (зазор легенда → chart-area). ═══ */}
      <div
        style={{
          flexShrink: 0,
          paddingTop: 'var(--chart-legend-top-gap, 8px)',
          paddingBottom: 'var(--chart-legend-mb, 2px)',
        }}
      >
        <ChartLegend items={legendItems} />
      </div>

      {/* ═══ Контейнер chart — flex: 1 (берёт оставшуюся высоту) ═══ */}
      <div
        ref={chartWrapRef}
        className="relative"
        style={{ flex: 1, minHeight: 0 }}
        onMouseMove={handleMove}
        onMouseLeave={handleLeave}
      >
        {/* === Y-axis labels (HTML absolute справа) ===
            bottom такой же как chart-area — labels выравниваются с grid lines. */}
        <div
          className="absolute"
          style={{
            top: 'var(--chart-pad-top, 19px)',
            bottom: 'var(--chart-pad-bottom, 50px)',
            right: 0,
            width: 'var(--chart-pad-right-single, 95px)',
            pointerEvents: 'none',
          }}
        >
          {yTicks.map((v) => {
            const yPct = 50 - (v / yMax) * 50;  // 0% top → 100% bottom
            return (
              <div
                key={v}
                className="absolute font-bold"
                style={{
                  top: `${yPct}%`,
                  left: 'var(--sp-2)',
                  right: 0,
                  transform: 'translateY(-50%)',
                  fontSize: 'var(--chart-font-y, 17px)',
                  color: 'var(--axis-color, var(--text-primary))',
                  fontVariantNumeric: 'tabular-nums',
                  whiteSpace: 'nowrap',
                }}
              >
                {Math.round(v)}
                <span className="font-bold" style={{ fontSize: '0.7em', opacity: 0.85, marginLeft: 3 }}>
                  млрд
                </span>
              </div>
            );
          })}
        </div>

        {/* === Chart-area (SVG inside absolute div) ===
            bottom = pad-bottom + (font-x + 8) — оставляем место для ДВУХ строк
            подписей (период + год) под chart-area. Иначе year обрезался за
            paper-card. */}
        <div
          className="absolute"
          style={{
            top: 'var(--chart-pad-top, 19px)',
            bottom: 'var(--chart-pad-bottom, 50px)',
            left: 'var(--chart-pad-left, 100px)',
            right: 'var(--chart-pad-right-single, 95px)',
          }}
        >
          <svg
            width="100%"
            height="100%"
            preserveAspectRatio="none"
            style={{ overflow: 'visible', display: 'block' }}
          >
            {/* Горизонтальные grid линии (solid, 5 уровней) */}
            {yTicks.map((v) => {
              const yPct = 50 - (v / yMax) * 50;
              const isZero = v === 0;
              return (
                <line
                  key={v}
                  x1="0" x2="100%"
                  y1={`${yPct}%`} y2={`${yPct}%`}
                  stroke={isZero ? 'var(--text-primary)' : 'var(--text-muted)'}
                  strokeWidth={isZero ? 1.5 : 1}
                  opacity={isZero ? 0.55 : 0.3}
                  vectorEffect="non-scaling-stroke"
                />
              );
            })}

            {/* Bars с grow-from-zero wave animation:
                progress ∈ [0,1] — height и position стека масштабируются
                пропорционально. На progress=0 — bar высотой 0 у zero line.
                На progress=1 — полная высота. Stack maintained correctly во
                все промежуточные моменты (т.к. stackUp/Down тоже scale-ются). */}
            {periods.map((p, i) => {
              const isHovered = hover?.periodIdx === i;
              const hoverOpacity = hover && !isHovered ? 0.5 : 1;
              // progress: animProgress[i] либо 1 если animation завершена / state cleared
              const progress = animProgress[i] ?? 1;
              let stackUp = 0;
              let stackDown = 0;
              const x = i * barSlot + barOffset;
              return (
                <g
                  key={i}
                  opacity={hoverOpacity}
                  style={{ transition: 'opacity 150ms' }}
                >
                  {categories.map((cat) => {
                    const v = p.values[cat] ?? 0;
                    if (v === 0) return null;
                    // Final height в % (без scaling)
                    const hPctFinal = Math.max((Math.abs(v) / yMax) * 50, MIN_BAR_H);
                    // Scale by progress
                    const hPct = hPctFinal * progress;
                    if (v > 0) {
                      const stackUpPctFinal = (stackUp / yMax) * 50;
                      const stackUpPct = stackUpPctFinal * progress;
                      stackUp += v;
                      return (
                        <rect
                          key={`${i}-${cat}`}
                          x={`${x}%`}
                          y={`${50 - stackUpPct - hPct}%`}
                          width={`${barW}%`}
                          height={`${hPct}%`}
                          fill={getCategoryColor(cat, theme)}
                          stroke="var(--bar-outline)" strokeWidth={outlineWidth}
                          vectorEffect="non-scaling-stroke"
                        />
                      );
                    } else {
                      const stackDownPctFinal = (stackDown / yMax) * 50;
                      const stackDownPct = stackDownPctFinal * progress;
                      stackDown += -v;
                      return (
                        <rect
                          key={`${i}-${cat}`}
                          x={`${x}%`}
                          y={`${50 + stackDownPct}%`}
                          width={`${barW}%`}
                          height={`${hPct}%`}
                          fill={getCategoryColor(cat, theme)}
                          stroke="var(--bar-outline)" strokeWidth={outlineWidth}
                          vectorEffect="non-scaling-stroke"
                        />
                      );
                    }
                  })}
                </g>
              );
            })}

            {/* Crosshair при hover */}
            {hover && periods[hover.periodIdx] && (
              <line
                x1={`${hover.periodIdx * barSlot + barSlot / 2}%`}
                x2={`${hover.periodIdx * barSlot + barSlot / 2}%`}
                y1="0" y2="100%"
                stroke="var(--text-primary)"
                strokeWidth={1}
                opacity={0.25}
                strokeDasharray="2 3"
                vectorEffect="non-scaling-stroke"
                pointerEvents="none"
              />
            )}
          </svg>
        </div>

        {/* === X-axis labels (DD.MM.YY format) — match с FlowsHistogram.
            bottom: var(--chart-xlabel-bottom) — same CSS-var как у Притоки/Оттоки.
            Gap до bottom grid line ~13px (vs прежние 4px) — labels чуть ниже. */}
        <div
          className="absolute flex justify-between font-semibold px-2"
          style={{
            left: 'var(--chart-pad-left, 100px)',
            right: 'var(--chart-pad-right-single, 95px)',
            bottom: 'var(--chart-xlabel-bottom, 20px)',
            fontSize: 'var(--chart-font-x, 14px)',
            color: 'var(--axis-color, #9CA3B8)',
            fontVariantNumeric: 'tabular-nums',
            pointerEvents: 'none',
          }}
        >
          {(() => {
            const tickCount = Math.min(isMobile ? 3 : 6, periods.length);
            if (tickCount < 2) {
              // Edge case: 1 period — single centered label
              const p = periods[0];
              const date = new Date(p.end_date);
              return (
                <span style={{ margin: '0 auto' }}>
                  {date.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' })}
                </span>
              );
            }
            return Array.from({ length: tickCount }, (_, i) => {
              const idx = Math.min(
                Math.round((i * (periods.length - 1)) / (tickCount - 1)),
                periods.length - 1,
              );
              const p = periods[idx];
              const date = new Date(p.end_date);
              return (
                <span key={`xlab-${i}`}>
                  {date.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' })}
                </span>
              );
            });
          })()}
        </div>


        {/* === Watermark — на нижней горизонтальной линии grid (bottom chart-area).
            После убирания year labels chart-area bottom = pad-bottom.
            Watermark sits с base прямо на bottom grid line + 4px gap. */}
        <div
          style={{
            position: 'absolute',
            left: 'calc(var(--chart-pad-left, 100px) + 4px)',
            bottom: 'calc(var(--chart-pad-bottom, 50px) + 4px)',
            pointerEvents: 'none',
          }}
        >
          <ChartWatermark />
        </div>

        {/* Плавающая дата над графиком — единый стиль/позиция со всеми чартами
            (ChartDatePill: прозрачный текст, низ прижат к верхней грид-линии,
            кламп в границах chart-area чтобы не наезжать на Y-шкалу). */}
        {hover && periods[hover.periodIdx] && (() => {
          const cs = getComputedStyle(document.documentElement);
          const padLeft = parseFloat(cs.getPropertyValue('--chart-pad-left')) || 100;
          const padRight = parseFloat(cs.getPropertyValue('--chart-pad-right-single')) || 95;
          const padTop = parseFloat(cs.getPropertyValue('--chart-pad-top')) || 14;
          const w = containerRef.current?.clientWidth ?? 800;
          const slotW = (w - padLeft - padRight) / periods.length;
          const p = periods[hover.periodIdx];
          return (
            <ChartDatePill
              date={`${p.label} ${p.year}`}
              x={padLeft + (hover.periodIdx + 0.5) * slotW}
              topLineY={padTop}
              minX={padLeft}
              maxX={w - padRight}
            />
          );
        })()}
      </div>

      {/* === Tooltip === */}
      {hover && periods[hover.periodIdx] && (() => {
        const p = periods[hover.periodIdx];
        const entries = categories
          .map((cat) => ({ cat, val: p.values[cat] ?? 0 }))
          .filter(e => e.val !== 0);
        const sortedPositive = entries.filter(e => e.val > 0).sort((a, b) => b.val - a.val);
        const sortedNegative = entries.filter(e => e.val < 0).sort((a, b) => a.val - b.val);
        // Объём торгов между категориями: в balanced market сумма покупок ≈
        // сумме продаж (нетто всегда ~0). Полезная метрика — «сколько денег
        // прокрутилось» в этом периоде = max(покупки, |продажи|).
        const tradingVolume = periodVolume(p, categories);

        // м/м и г/г — изменение объёма торгов к прошлому периоду и к тому же
        // периоду год назад. Считаем по allPeriods (полный список): при узком
        // фильтре («1Г») соседи для сравнения лежат за пределами `periods`.
        const fullPeriods = allPeriods ?? periods;
        const curIdx = fullPeriods.findIndex((x) => x.end_date === p.end_date);
        const prevPeriod = curIdx > 0 ? fullPeriods[curIdx - 1] : undefined;
        const yoyPeriod = fullPeriods.find(
          (x) => x.label === p.label && x.year === p.year - 1,
        );
        const pctVs = (base: number): number | null =>
          base > 0 ? ((tradingVolume - base) / base) * 100 : null;
        const momPct = prevPeriod ? pctVs(periodVolume(prevPeriod, categories)) : null;
        const yoyPct = yoyPeriod ? pctVs(periodVolume(yoyPeriod, categories)) : null;
        const momLabel = p.kind === 'quarter' ? 'кв/кв' : 'м/м';
        const fmtPct = (v: number | null) =>
          v === null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`;
        const pctColor = (v: number | null) =>
          v === null
            ? 'var(--text-muted)'
            : v >= 0
              ? 'var(--funds-flow-positive)'
              : 'var(--funds-flow-negative)';

        const containerW = containerRef.current?.clientWidth ?? 800;
        // Adaptive tooltip width: 260px default, но не больше container-32
        // (gap 16px по бокам). На mobile 360-375px tooltip станет ~330-340px.
        const tooltipW = Math.min(260, Math.max(180, containerW - 32));
        // Переключаем сторону с СЕРЕДИНЫ chart-area.
        const placeLeft = hover.mouseX > containerW / 2;
        const left = placeLeft ? hover.mouseX - tooltipW - 16 : hover.mouseX + 16;
        // Tooltip clamp: карточка в plot-коридоре (между крайними грид-линиями
        // chart-области, с запасом 6px) — не накрывает легенду и дату-пилюлю
        // сверху, не заезжает на подписи оси снизу. Координаты hover.mouseY —
        // от OUTER контейнера (включая зону легенды), поэтому границы коридора
        // считаем через offsetTop chart-контейнера.
        const tooltipMaxH = isMobile ? 180 : 240;
        const csDoc = getComputedStyle(document.documentElement);
        const padTopPx = parseFloat(csDoc.getPropertyValue('--chart-pad-top')) || 14;
        const padBottomPx = parseFloat(csDoc.getPropertyValue('--chart-pad-bottom')) || 50;
        const wrapTop = chartWrapRef.current?.offsetTop ?? 40;
        const wrapH = chartWrapRef.current?.clientHeight ?? (height - wrapTop);
        const minTop = wrapTop + padTopPx + 6;
        const maxTop = wrapTop + wrapH - padBottomPx - 6 - tooltipMaxH;
        const top = Math.max(minTop, Math.min(hover.mouseY - 20, maxTop));

        return (
          <div
            data-export-ignore="true"
            className={`${TOOLTIP.containerClass} absolute z-20`}
            style={{
              ...TOOLTIP.containerStyle,
              left: `${left}px`,
              top: `${top}px`,
              width: `${tooltipW}px`,
              pointerEvents: 'none',
              whiteSpace: 'normal',
            }}
          >
            {sortedPositive.map((e) => (
              <div key={e.cat} className="flex items-center justify-between py-0.5" style={{ gap: 'var(--sp-2)' }}>
                <div className="flex items-center min-w-0" style={{ gap: 'var(--sp-1)' }}>
                  <span className={TOOLTIP.dotClass} style={{ ...TOOLTIP.dotStyle, backgroundColor: getCategoryColor(e.cat, theme) }} />
                  <span className={`${TOOLTIP.labelClass} truncate`} style={TOOLTIP.labelStyle}>{e.cat}</span>
                </div>
                <span className={TOOLTIP.valueClass} style={{ ...TOOLTIP.valueStyle, color: 'var(--funds-flow-positive)' }}>
                  +{e.val.toFixed(2)}
                </span>
              </div>
            ))}
            {sortedPositive.length > 0 && sortedNegative.length > 0 && (
              <div style={{ height: 1, background: 'var(--text-muted)', opacity: 0.25, margin: '4px 0' }} />
            )}
            {sortedNegative.map((e) => (
              <div key={e.cat} className="flex items-center justify-between py-0.5" style={{ gap: 'var(--sp-2)' }}>
                <div className="flex items-center min-w-0" style={{ gap: 'var(--sp-1)' }}>
                  <span className={TOOLTIP.dotClass} style={{ ...TOOLTIP.dotStyle, backgroundColor: getCategoryColor(e.cat, theme) }} />
                  <span className={`${TOOLTIP.labelClass} truncate`} style={TOOLTIP.labelStyle}>{e.cat}</span>
                </div>
                <span className={TOOLTIP.valueClass} style={{ ...TOOLTIP.valueStyle, color: 'var(--funds-flow-negative)' }}>
                  {e.val.toFixed(2)}
                </span>
              </div>
            ))}
            <div style={{ height: 1, background: 'var(--text-primary)', opacity: 0.35, margin: '4px 0' }} />
            <div className="flex items-center justify-between font-bold" style={{ fontSize: 'var(--fs-xs)' }}>
              <span>Объём торгов</span>
              <span style={{ color: 'var(--text-primary)' }}>
                {tradingVolume.toFixed(2)} млрд
              </span>
            </div>
            {/* Изменение объёма: м/м (к прошлому периоду) и г/г (к тому же
                периоду год назад). «—» если периода для сравнения нет. */}
            <div
              className="flex items-center justify-between"
              style={{ fontSize: 'var(--fs-xs)', marginTop: 'var(--sp-1)' }}
            >
              <span>
                <span style={{ color: 'var(--text-muted)' }}>{momLabel} </span>
                <span style={{ color: pctColor(momPct), fontWeight: 700 }}>{fmtPct(momPct)}</span>
              </span>
              <span>
                <span style={{ color: 'var(--text-muted)' }}>г/г </span>
                <span style={{ color: pctColor(yoyPct), fontWeight: 700 }}>{fmtPct(yoyPct)}</span>
              </span>
            </div>
          </div>
        );
      })()}
    </div>
  );
}
