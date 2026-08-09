/**
 * composeFramedCanvas — берёт raw chart canvas и вписывает его в frame
 * с padding, background, header (title/asset/period) и footer (url/date).
 *
 * Header layout:
 *   [Title (large, bold)]            [Asset name (medium)]  [TICKER badge]
 *   [Detail tags · separator · etc]
 *
 * Footer layout:
 *   [framedata.ru]                                  [Дата захвата]
 *
 * Если metadata не передан — header/footer не рендерятся (compact mode).
 */

import type { FrameOptions } from './types';

const DEFAULT_PADDING = 18;
const SITE_URL = 'framedata.ru';

// Layout constants — БАЗОВЫЕ значения в LOGICAL pixels при chartW=REFERENCE_W
// (умножаются на DPR и на layoutScale в конце, см. computeLayoutScale ниже).
// Single-column header hierarchy: primary (asset/title) → subtitle (context).
// HEADER_HEIGHT = primary(28) + gap(6) + subtitle(15) ≈ 49 → 50 (минимум air).
// Поля/хедер/футер ужаты, чтобы сам график занимал больше площади кадра.
const HEADER_HEIGHT = 50;
const HEADER_GAP = 6; // gap между header и chart
const FOOTER_HEIGHT = 24;
const FOOTER_GAP = 8;

const PRIMARY_FONT_SIZE = 28;
const SUBTITLE_FONT_SIZE = 15;
const TICKER_FONT_SIZE = 14;
const FOOTER_FONT_SIZE = 14;

// Ширина, при которой все константы выше выглядят «как задумано» (обычная
// панель песочницы/карточка на сайте). Полноэкранный экспорт (Развернуть →
// весь экран песочницы) снимает элемент в разы шире — без масштабирования
// header/footer текст оставался фиксированных 28/15/14px и выглядел игрушечно
// мелким на большом изображении (Вадим, скрин «текст очень мелкий на фото»).
// Масштаб растёт линейно с шириной графика; вниз не клампим (< REFERENCE_W
// уже выглядит корректно на всех текущих SIZE_BY_TYPE панелях — не трогаем).
//
// ВАЖНО: масштаб включается только флагом scaleWithWidth — то есть в песочнице
// и эмбедах, откуда пришла та правка. На сайте графики шире REFERENCE_W почти
// всегда, и шапка раздувалась в полтора-два раза; там оставляем прежний,
// зависящий только от DPR кегль.
const REFERENCE_W = 900;
function computeLayoutScale(chartW: number, dpr: number): number {
    return Math.max(1, chartW / (REFERENCE_W * dpr));
}

const FONT_FAMILY = 'Inter, "Helvetica Neue", Helvetica, Arial, sans-serif';

export function composeFramedCanvas(
    chartCanvas: HTMLCanvasElement,
    options: FrameOptions & { dpr?: number },
): HTMLCanvasElement {
    const padding = options.padding ?? DEFAULT_PADDING;
    const dpr = options.dpr ?? 1;
    const meta = options.metadata;
    const textColor = options.textColor ?? '#0A0A0A';
    const textSecondary = options.textSecondaryColor ?? '#6B6B6B';
    const accent = options.accentColor ?? '#FF5C2B';

    // Chart raw dimensions (canvas pixels — DPR-scaled)
    const chartW = chartCanvas.width;
    const chartH = chartCanvas.height;

    // layoutScale растёт для широких снимков (полноэкранный экспорт) — см.
    // computeLayoutScale. sc заменяет dpr как множитель везде ниже, где раньше
    // был просто dpr: header/footer/padding остаются пропорциональны РЕАЛЬНОМУ
    // размеру снимка, а не только retina-плотности экрана.
    const sc = options.scaleWithWidth ? dpr * computeLayoutScale(chartW, dpr) : dpr;

    const sp = padding * sc;
    const headerH = meta ? HEADER_HEIGHT * sc : 0;
    const headerGap = meta ? HEADER_GAP * sc : 0;
    const footerH = FOOTER_HEIGHT * sc;
    const footerGap = FOOTER_GAP * sc;

    // Total frame: padding + header + gap + chart + gap + footer + padding
    const totalW = chartW + sp * 2;
    const totalH = sp + headerH + headerGap + chartH + footerGap + footerH + sp;

    const out = document.createElement('canvas');
    out.width = Math.round(totalW);
    out.height = Math.round(totalH);

    const ctx = out.getContext('2d');
    if (!ctx) throw new Error('Failed to get 2D context for framed canvas');

    // Fill background
    ctx.fillStyle = options.background;
    ctx.fillRect(0, 0, out.width, out.height);

    // Render header (если есть metadata)
    if (meta) {
        drawHeader(ctx, meta, sp, sp, totalW - sp * 2, headerH, sc, textColor, textSecondary, accent);
    }

    // Center chart horizontally, position vertically after header
    const chartX = (out.width - chartW) / 2;
    const chartY = sp + headerH + headerGap;
    ctx.drawImage(chartCanvas, chartX, chartY);

    // Watermark (Free tier) — большая полупрозрачная надпись поверх chart area
    if (options.watermark) {
        drawWatermark(ctx, chartX, chartY, chartW, chartH, sc);
    }

    // Render footer (всегда — site url + date)
    drawFooter(ctx, sp, totalH - sp - footerH, totalW - sp * 2, footerH, sc, textSecondary);

    return out;
}

/**
 * Watermark для Free tier — наклонная полупрозрачная надпись
 * "framedata.ru" в центре chart area. Размер пропорционален chart width,
 * чтобы читался на любых aspect ratios.
 */
function drawWatermark(
    ctx: CanvasRenderingContext2D,
    x: number, y: number, w: number, h: number,
    dpr: number,
) {
    ctx.save();
    // Размер шрифта ~10% от ширины графика, но не больше высоты
    const fontSize = Math.min(w * 0.1, h * 0.18);
    ctx.font = `800 ${fontSize}px ${FONT_FAMILY}`;
    ctx.fillStyle = 'rgba(128, 128, 128, 0.18)';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';

    // Поворачиваем canvas на -20° и пишем по центру chart area
    const cx = x + w / 2;
    const cy = y + h / 2;
    ctx.translate(cx, cy);
    ctx.rotate(-20 * Math.PI / 180);
    ctx.fillText(SITE_URL, 0, 0);

    ctx.restore();
    void dpr;  // dpr учитывается через fontSize → не нужен явно
}

/**
 * Рендер header — single-column hierarchy:
 *
 *   [Primary: asset or title]  [TICKER badge inline]
 *   [Subtitle: title если есть asset, + details]
 *
 * Если asset не задан — primary = title, subtitle = только details.
 */
function drawHeader(
    ctx: CanvasRenderingContext2D,
    meta: NonNullable<FrameOptions['metadata']>,
    x: number, y: number, w: number, _h: number,
    dpr: number,
    textColor: string,
    textSecondary: string,
    accent: string,
) {
    const primarySize = PRIMARY_FONT_SIZE * dpr;
    const subtitleSize = SUBTITLE_FONT_SIZE * dpr;
    const tickerSize = TICKER_FONT_SIZE * dpr;

    // Primary text: asset (если есть) либо title (fallback)
    const primaryText = meta.asset ?? meta.title;

    // Subtitle: если asset задан и отличается от title — title + details.
    // Иначе только details.
    const subtitleParts: string[] = [];
    if (meta.asset && meta.title && meta.title !== meta.asset) {
        subtitleParts.push(meta.title);
    }
    if (meta.details) {
        subtitleParts.push(...meta.details);
    }
    const subtitle = subtitleParts.join(' · ');

    ctx.textBaseline = 'alphabetic';
    ctx.textAlign = 'left';

    // === Primary line ===
    ctx.fillStyle = textColor;
    ctx.font = `700 ${primarySize}px ${FONT_FAMILY}`;
    // Render primary text at baseline = y + primarySize (since textBaseline='alphabetic')
    const primaryBaseline = y + primarySize * 0.85; // visual top-aligned
    ctx.fillText(primaryText, x, primaryBaseline);

    // Ticker badge — INLINE справа от primary text (10px gap).
    // Не показываем badge если ticker совпадает с primary (asset еще не загрузился
    // и упал в fallback на сам ticker → иначе получаем "CR · CR").
    if (meta.ticker && meta.ticker !== primaryText) {
        ctx.font = `700 ${primarySize}px ${FONT_FAMILY}`;
        const primaryW = ctx.measureText(primaryText).width;
        const tickerBadgeX = x + primaryW + 10 * dpr;

        ctx.font = `700 ${tickerSize}px ${FONT_FAMILY}`;
        const tickerW = ctx.measureText(meta.ticker).width;
        const padX = 8 * dpr;
        const padY = 4 * dpr;
        const badgeW = tickerW + padX * 2;
        const badgeH = tickerSize + padY * 2;
        // Vertical-center badge с primary text
        const badgeY = primaryBaseline - primarySize * 0.7 + (primarySize * 0.7 - badgeH) / 2;

        roundRect(ctx, tickerBadgeX, badgeY, badgeW, badgeH, 4 * dpr);
        ctx.fillStyle = accent;
        ctx.fill();
        ctx.fillStyle = '#FFFFFF';
        ctx.textBaseline = 'middle';
        ctx.textAlign = 'center';
        ctx.fillText(meta.ticker, tickerBadgeX + badgeW / 2, badgeY + badgeH / 2);
        ctx.textBaseline = 'alphabetic';
        ctx.textAlign = 'left';
    }

    // === Subtitle line ===
    if (subtitle) {
        ctx.fillStyle = textSecondary;
        ctx.font = `500 ${subtitleSize}px ${FONT_FAMILY}`;
        const subtitleBaseline = primaryBaseline + 6 * dpr + subtitleSize * 0.85;
        ctx.fillText(subtitle, x, subtitleBaseline);
    }

    // reset (на всякий)
    ctx.textBaseline = 'alphabetic';
    ctx.textAlign = 'left';

    // суппресс unused warnings — переменные могут быть использованы в будущих расширениях
    void w;
}

/**
 * Footer: site URL слева, дата справа.
 */
function drawFooter(
    ctx: CanvasRenderingContext2D,
    x: number, y: number, w: number, h: number,
    dpr: number,
    color: string,
) {
    const fs = FOOTER_FONT_SIZE * dpr;
    ctx.font = `500 ${fs}px ${FONT_FAMILY}`;
    ctx.fillStyle = color;
    ctx.textBaseline = 'middle';
    const cy = y + h / 2;

    // URL слева
    ctx.textAlign = 'left';
    ctx.fillText(SITE_URL, x, cy);

    // Дата справа — формат "7 мая 2026"
    const dateStr = new Date().toLocaleDateString('ru-RU', {
        day: 'numeric',
        month: 'long',
        year: 'numeric',
    });
    ctx.textAlign = 'right';
    ctx.fillText(dateStr, x + w, cy);

    // reset
    ctx.textBaseline = 'alphabetic';
    ctx.textAlign = 'left';
}

/**
 * Helper для rounded rect path. Canvas2D не имеет встроенного roundRect везде
 * (Safari < 16 не поддерживает), поэтому manual implementation.
 */
function roundRect(ctx: CanvasRenderingContext2D, x: number, y: number, w: number, h: number, r: number) {
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.lineTo(x + w - r, y);
    ctx.quadraticCurveTo(x + w, y, x + w, y + r);
    ctx.lineTo(x + w, y + h - r);
    ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
    ctx.lineTo(x + r, y + h);
    ctx.quadraticCurveTo(x, y + h, x, y + h - r);
    ctx.lineTo(x, y + r);
    ctx.quadraticCurveTo(x, y, x + r, y);
    ctx.closePath();
}
