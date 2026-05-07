/**
 * measureText — sync canvas-based text width measurement.
 *
 * Используется ChartLegend и SVG-based price labels чтобы вычислить
 * exact width текста для определения SVG dimensions / pill width.
 *
 * Кэширует HTMLCanvasElement между вызовами — без оверхеда.
 */

let measureCanvas: HTMLCanvasElement | null = null;

function getCtx(): CanvasRenderingContext2D | null {
    if (typeof document === 'undefined') return null;
    if (!measureCanvas) measureCanvas = document.createElement('canvas');
    return measureCanvas.getContext('2d');
}

/**
 * Возвращает width текста в pixels.
 * Font stack совпадает с body — Inter primary, system fallbacks.
 */
export function measureText(
    text: string,
    fontSize: number,
    fontWeight: number = 400,
): number {
    const ctx = getCtx();
    if (!ctx) {
        // SSR fallback: avg char ≈ 0.6 * font-size для sans-serif
        return text.length * fontSize * 0.6;
    }
    ctx.font = `${fontWeight} ${fontSize}px Inter, "Helvetica Neue", Helvetica, Arial, sans-serif`;
    return ctx.measureText(text).width;
}
