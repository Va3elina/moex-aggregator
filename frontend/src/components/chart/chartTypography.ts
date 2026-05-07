/**
 * chartTypography — pure functions для discrete font sizes на 6 breakpoints.
 *
 * SOURCE OF TRUTH для chart axis/legend размеров. Должен совпадать с CSS
 * media queries в index.css (ищи `--chart-font-y`/`--chart-font-x`).
 *
 * Breakpoints выбраны по проектному дизайн-systemу:
 *   320  — Mobile S (старый iPhone SE и узкие Android)
 *   375  — Mobile M (iPhone 12-15 mini)
 *   425  — Mobile L (большинство Android до Pixel 7)
 *   768  — Tablet (iPad portrait, большой Android)
 *   1024 — Desktop (iPad landscape, ноутбуки)
 *   1440 — Desktop wide (full HD, retina)
 *
 * Шаг между breakpoints = 2px для axis (visual continuity), legend на +2 от
 * axis для hierarchy (title > legend > axis).
 */

export interface ChartFontScale {
    axis: number;
    legend: number;
    legendDot: number;
}

/**
 * Возвращает size triple для текущего viewport width.
 * Cascade: vw попадает в один из 6 buckets, размер берётся из таблицы.
 */
export function chartFontScale(vw: number): ChartFontScale {
    if (vw >= 1440) return { axis: 17, legend: 19, legendDot: 16 };
    if (vw >= 1024) return { axis: 15, legend: 17, legendDot: 14 };
    if (vw >= 768)  return { axis: 13, legend: 15, legendDot: 13 };
    if (vw >= 425)  return { axis: 11, legend: 13, legendDot: 11 };
    if (vw >= 375)  return { axis: 10, legend: 12, legendDot: 10 };
    return                  { axis: 9,  legend: 11, legendDot: 9  };
}

/** Convenience helpers для отдельных значений. */
export const axisFontSize = (vw: number): number => chartFontScale(vw).axis;
export const legendFontSize = (vw: number): number => chartFontScale(vw).legend;
export const legendDotSize = (vw: number): number => chartFontScale(vw).legendDot;

/**
 * Сколько X-axis date ticks влезает в chartWidth без overlap.
 *
 * Формула: chartWidth / (fontSize × charW × charsPerLabel).
 * Default 11 chars на label (e.g. "01 янв 25" / "31 дек 25"). Возвращает
 * число в [3..7] — нижний bound гарантирует minimum 3 даты, верхний
 * предотвращает слишком плотное расположение даже на огромных экранах.
 *
 * Использовать в BreadthChart, IndexChart и др. где раньше был hardcoded
 * `Math.min(isMobile ? 4 : 8, ...)` — формула auto-corrects под текущий
 * font size (axis fontSize меняется по 6 breakpoint'ам).
 */
export function xAxisTickCount(
    chartWidth: number,
    fontSize: number,
    charsPerLabel = 11,
): number {
    const charW = fontSize * 0.62; // approximate digit/letter width в bold sans
    const labelMinW = charW * charsPerLabel;
    return Math.max(3, Math.min(7, Math.floor(chartWidth / labelMinW)));
}
