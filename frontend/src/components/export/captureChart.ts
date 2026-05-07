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
 *   - scale: window.devicePixelRatio — retina-quality export
 */

/**
 * Снимает HTMLElement в HTMLCanvasElement.
 * @param element DOM element для capture
 * @param signal AbortSignal для cancellation если modal закрылся до конца
 * @throws Error если capture не удался
 */
export async function captureChart(
    element: HTMLElement,
    signal?: AbortSignal,
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
        // scale: devicePixelRatio даёт sharp output на retina screens.
        // На 1x screens = 1, на 2x = 2 (canvas в 4 раза больше пикселей).
        scale: typeof window !== 'undefined' ? window.devicePixelRatio || 1 : 1,
        // Игнорируем элементы которые не должны попасть в snapshot:
        //   - сама кнопка ChartCaptureButton (data-export-ignore)
        //   - tooltip overlay'ы (если открыты при capture — не нужны)
        ignoreElements: (el) => {
            return (
                el.getAttribute('data-export-ignore') === 'true' ||
                el.classList.contains('chart-tooltip-overlay')
            );
        },
    });

    if (signal?.aborted) {
        throw new Error('Capture aborted');
    }

    return canvas;
}
