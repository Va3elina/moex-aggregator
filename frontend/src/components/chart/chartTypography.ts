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
    // ⚠️ Десктопные ступени возвращены к дореформенным 14px. Их подняли до
    // 17/15 вместе с экспортной типографикой (#75f9171a), но эта таблица —
    // размеры САЙТА (SimpleChart и мобильные графики), а не экспорта. Песочница
    // её не читает вовсе: LwChartPanes держит свой BASE_FONT_SIZE=11 и
    // масштабирует его только на время снимка (captureFontScale). Поэтому
    // возврат сюда откатывает сайт и НЕ трогает песочницу.
    if (vw >= 1440) return { axis: 14, legend: 16, legendDot: 13 };
    if (vw >= 1024) return { axis: 14, legend: 16, legendDot: 13 };
    if (vw >= 768)  return { axis: 13, legend: 15, legendDot: 13 };
    if (vw >= 425)  return { axis: 11, legend: 13, legendDot: 11 };
    if (vw >= 375)  return { axis: 10, legend: 12, legendDot: 10 };
    return                  { axis: 9,  legend: 11, legendDot: 9  };
}

/** Convenience helpers для отдельных значений. */
export const axisFontSize = (vw: number): number => chartFontScale(vw).axis;
export const legendFontSize = (vw: number): number => chartFontScale(vw).legend;
export const legendDotSize = (vw: number): number => chartFontScale(vw).legendDot;

export interface ChartAxisPadding { left: number; right: number }

/**
 * Отступы под Y-ось (--chart-pad-left/--chart-pad-right-single), но по
 * КОНТЕЙНЕРУ, а не viewport. CSS-переменные в index.css завязаны на media
 * queries по window width — в узкой embed-панели песочницы (containerW << vw
 * реального окна браузера) они не ужимаются, и широкий gutter под цифры оси
 * съедает большую часть узкой панели / цифры наезжают на bars. Таблица
 * зеркалит те же breakpoints (768/1024), что и index.css.
 */
export function axisPadding(containerWidth: number): ChartAxisPadding {
    if (containerWidth < 480) return { left: 40, right: 48 };
    if (containerWidth < 768) return { left: 46, right: 56 };
    if (containerWidth < 1024) return { left: 95, right: 90 };
    return { left: 100, right: 95 };
}

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

/**
 * Множитель font-size ДВИЖКА lightweight-charts (layout.fontSize) перед
 * захватом в PNG-экспорт — см. composeFramedCanvas.computeLayoutScale (тот же
 * REFERENCE_W и та же мотивация). LwChart/LwChartPanes держат fontSize:11
 * ФИКСИРОВАННЫМ независимо от размера контейнера (обычная панель ⩽ REFERENCE_W
 * — выглядит как задумано). Полноэкранный экспорт снимает контейнер в разы
 * шире — без масштабирования ось/легенда/тултип графика остаются тем же
 * 11px и выглядят игрушечно мелкими на большом снимке (Вадим, скрин
 * «текст и цифры очень мелкие», Сбербанк/ОИ в развёрнутой панели).
 * Вниз не клампим — обычные panели (< REFERENCE_W) уже корректны, не трогаем.
 */
export const CAPTURE_FONT_REFERENCE_W = 900;
export function captureFontScale(containerWidth: number): number {
    return Math.max(1, containerWidth / CAPTURE_FONT_REFERENCE_W);
}
