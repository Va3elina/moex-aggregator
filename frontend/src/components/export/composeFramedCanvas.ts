/**
 * composeFramedCanvas — берёт raw chart canvas и вписывает его в 16:9 frame
 * с padding и background. Watermark уже on chart (не добавляем сверху).
 *
 * NO HEADER (по решению user — Phase 1).
 * NO date / url footer.
 *
 * Algorithm:
 *   1. Compute 16:9 frame dimensions так чтобы chart aspect-ratio сохранялся
 *   2. Если chart wider than 16:9 → fit by width, pad top+bottom
 *   3. Если chart taller than 16:9 → fit by height, pad left+right
 *   4. Center chart inside frame, fill bg
 */

import type { FrameOptions } from './types';

const DEFAULT_PADDING = 48;     // logical px
const DEFAULT_ASPECT = 16 / 9;
const MIN_FRAME_WIDTH = 1280;   // logical px — минимум для соц-сетей

export function composeFramedCanvas(
    chartCanvas: HTMLCanvasElement,
    options: FrameOptions & { dpr?: number },
): HTMLCanvasElement {
    const padding = options.padding ?? DEFAULT_PADDING;
    const aspect = options.aspectRatio ?? DEFAULT_ASPECT;
    const dpr = options.dpr ?? 1;

    // Chart raw dimensions (canvas pixels — могут быть retina-scaled)
    const chartW = chartCanvas.width;
    const chartH = chartCanvas.height;
    const chartAspect = chartW / chartH;

    // Padding в canvas pixels (scale by DPR)
    const scaledPadding = padding * dpr;

    // Inner content area: chart + symmetric padding на 16:9
    // Если chart wider than 16:9 (rare для charts, но возможно):
    //   width-bounded — use chartW + 2*pad для total width
    // Если chart taller (наш случай для большинства):
    //   width-bounded ИЛИ height-bounded зависит от соотношений
    let totalW: number;
    let totalH: number;

    // Strategy: вписать chart в максимально компактный 16:9 frame с минимум
    // padding. Если chart-aspect > 16:9 — расширяем height. Иначе — width.
    if (chartAspect >= aspect) {
        // Chart шире target ratio → height расширяется до 16:9 при chartW
        totalW = chartW + scaledPadding * 2;
        totalH = totalW / aspect;
    } else {
        // Chart уже target ratio → width расширяется
        totalH = chartH + scaledPadding * 2;
        totalW = totalH * aspect;
    }

    // Минимум для соц-сетей
    const minW = MIN_FRAME_WIDTH * dpr;
    if (totalW < minW) {
        totalW = minW;
        totalH = totalW / aspect;
    }

    const out = document.createElement('canvas');
    out.width = Math.round(totalW);
    out.height = Math.round(totalH);

    const ctx = out.getContext('2d');
    if (!ctx) {
        throw new Error('Failed to get 2D context for framed canvas');
    }

    // Fill background
    ctx.fillStyle = options.background;
    ctx.fillRect(0, 0, out.width, out.height);

    // Center chart inside
    const x = (out.width - chartW) / 2;
    const y = (out.height - chartH) / 2;
    ctx.drawImage(chartCanvas, x, y);

    return out;
}
