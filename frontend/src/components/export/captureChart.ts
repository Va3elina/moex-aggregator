/**
 * captureChart — обёртка вокруг html2canvas с pre-configured options.
 *
 * html2canvas грузится lazy (dynamic import) — попадает в отдельный chunk,
 * не утяжеляет initial bundle. Загружается ТОЛЬКО при первом клике на 📷.
 *
 * Configuration choices:
 *   - useCORS: true — для возможных внешних логотипов/изображений
 *   - logging: false — silence development noise
 *   - backgroundColor: null — наследовать через computed background
 *   - scale: exportPixelScale — плотность снимка не ниже 2x, независимо от экрана
 */

import { exportPixelScale } from './exportScale';

/**
 * Снимает HTMLElement в HTMLCanvasElement.
 * @param element DOM element для capture
 * @param signal AbortSignal для cancellation если modal закрылся до конца
 * @param scale плотность снимка; по умолчанию exportPixelScale(element).
 *              Вызывающий передаёт своё значение, если тем же множителем
 *              масштабирует шапку кадра и аннотации (ExportModal).
 * @throws Error если capture не удался
 */
export async function captureChart(
    element: HTMLElement,
    signal?: AbortSignal,
    scale?: number,
): Promise<HTMLCanvasElement> {
    if (signal?.aborted) {
        throw new Error('Capture aborted');
    }

    // Lazy-load html2canvas — только при первом вызове
    const html2canvas = (await import('html2canvas')).default;

    if (signal?.aborted) {
        throw new Error('Capture aborted');
    }

    // ВАЖНО: backgroundColor null заставляет html2canvas использовать computed
    // background элемента. На наших chart-card'ах bg = var(--bg-primary), что
    // даст правильный paper/dark цвет для каждой темы.
    const canvas = await html2canvas(element, {
        useCORS: true,
        logging: false,
        backgroundColor: null,
        // Плотность снимка не привязана к плотности экрана: обычный монитор
        // (dpr 1) раньше давал PNG ровно в размер элемента и заметное мыло,
        // ретина при тех же данных — вдвое более чёткий файл. См. exportScale.
        scale: scale ?? exportPixelScale(element),
        // Игнорируем элементы которые не должны попасть в snapshot:
        //   - сама кнопка ChartCaptureButton (data-export-ignore)
        //   - tooltip overlay'ы (если открыты при capture — не нужны)
        ignoreElements: (el) => {
            return (
                el.getAttribute('data-export-ignore') === 'true' ||
                el.classList.contains('chart-tooltip-overlay')
            );
        },
        // onclone: удаляем hover-UI (crosshair-линия, dimming, точки, date-pill,
        // карточка значений) из КЛОНА перед растеризацией. Нужно отдельно от
        // ignoreElements, потому что эти элементы живут ВНУТРИ chart-<svg> —
        // html2canvas сериализует svg целиком и per-element ignoreElements туда
        // не достаёт. Баг: «нажать экспорт и быстро навести курсор» → crosshair
        // попадал в снимок. Помечены классом chart-hover-ui в SimpleChart.
        onclone: (clonedDoc) => {
            clonedDoc.querySelectorAll('.chart-hover-ui, .chart-tooltip-root').forEach((el) => el.remove());
        },
    });

    if (signal?.aborted) {
        throw new Error('Capture aborted');
    }

    return canvas;
}
