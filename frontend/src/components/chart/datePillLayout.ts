/**
 * Единая логика позиционирования плавающего date-pill (тултипа с датой)
 * на всех графиках Фрейм. Сам pill рендерит компонент ChartDatePill
 * (центрирование на cursor X + кламп в plot-область); здесь — расчёт
 * y-координаты верхней линии, к которой pill прижимается низом.
 *
 * ── Контракт ──
 * Pill bottom edge сидит вплотную к верхней горизонтальной линии графика
 * (PILL_GAP_ABOVE_LINE = 0; стиль пилюли — TOOLTIP.datePillClass, без рамки).
 *
 * ── Почему helper а не hook ──
 * Stateless: считает y-координату из DOM на каждом render'е. React внутри
 * перерисовывает pill при движении курсора → DOM-чтение свежее. Hook бы
 * требовал useState/useEffect и не дал бы преимуществ.
 *
 * ── Различия chart'ов ──
 * • SimpleChart: chartWrapRef → drawing area; line at padding.top
 * • FlowsHistogram: chartContainer → SVG; grid line at 3% chart-area
 * • StrengthPage: containerRef → first .chart-reveal (IndexChart wrapper)
 *
 * Helper унифицирует через optional gridOffsetFrac.
 */

export const PILL_GAP_ABOVE_LINE = 0;

interface ChartTopLineOptions {
  /** Wrapper element с chart-reveal SVG. Если null — query из container'а. */
  wrapper?: HTMLElement | null;
  /** Контейнер для query (если wrapper не задан явно). */
  container?: HTMLElement | null;
  /** Internal padding.top самого графика. Default 8. */
  paddingTop?: number;
  /** Дополнительный offset как fraction of chart-area-height
   *  (например 0.03 для FlowsHistogram где yPct=50-47). Default 0. */
  gridOffsetFrac?: number;
  /** Chart-area-height для расчёта gridOffsetFrac. Required если gridOffsetFrac > 0. */
  chartAreaHeight?: number;
}

/**
 * Возвращает y-координату верхней горизонтальной линии графика
 * в координатах ближайшего relative-positioned предка (offsetParent).
 *
 * Если wrapper не найден — fallback 60 (типичная позиция после title block).
 */
export function computeChartTopLineY(opts: ChartTopLineOptions): number {
  const wrap = opts.wrapper
    ?? (opts.container?.querySelector<HTMLElement>('.chart-reveal') ?? null);
  const wrapTop = wrap?.offsetTop ?? 60;
  const padTop = opts.paddingTop ?? 8;
  const gridOff = (opts.gridOffsetFrac ?? 0) * (opts.chartAreaHeight ?? 0);
  return wrapTop + padTop + gridOff;
}
