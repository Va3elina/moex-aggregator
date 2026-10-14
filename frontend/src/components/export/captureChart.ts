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

/** Обёртка таймлайна (data-export-ignore) всё же клонируется — см. collapseNavigator. */
function hasNavigator(el: Element): boolean {
    return el.classList.contains('chart-navigator') || el.querySelector('.chart-navigator') !== null;
}

/** Зазор от низа подписей дат до нижней рамки — как сверху от рамки до легенды. */
const NAV_EXPORT_GAP = 12;

/**
 * Таймлайн-навигатор в PNG не нужен, но просто выкинуть его нельзя: рельс
 * подтянут отрицательным margin-top под даты (в резерв низа графика), а его
 * margin-bottom съедает нижний паддинг карточки. Без него обе выемки пустеют →
 * под датами полоса ~40px. Поэтому рельс остаётся в потоке невидимым, а его
 * margin-bottom подбирается по факту раскладки клона так, чтобы низ карточки
 * встал на NAV_EXPORT_GAP под самыми нижними подписями (даты бывают и
 * svg-<text>, и HTML-span'ами).
 */
function collapseNavigator(nav: HTMLElement, root: HTMLElement): void {
    const view = nav.ownerDocument.defaultView;
    const box = nav.closest<HTMLElement>('[data-export-ignore="true"]') ?? nav;
    nav.replaceChildren();
    // 1px, а не 0: у пустого блока margin-top и margin-bottom схлопываются в один.
    nav.style.height = '1px';
    box.style.visibility = 'hidden';
    const parent = box.parentElement;
    if (!view || !parent) return;

    let card: HTMLElement = root;
    for (let el: HTMLElement | null = parent; el && root.contains(el); el = el.parentElement) {
        if (parseFloat(view.getComputedStyle(el).borderBottomWidth) > 0) { card = el; break; }
    }
    const navTop = nav.getBoundingClientRect().top;
    const range = nav.ownerDocument.createRange();
    let textBottom = 0;
    for (const sib of Array.from(parent.children)) {
        if (sib === box) break;
        sib.querySelectorAll('text, span, div, p').forEach((el) => {
            if (el.tagName !== 'text' && (el.childElementCount > 0 || !el.textContent?.trim())) return;
            // Низ цифр, а не рамки: svg-<text> включает выносной элемент
            // (~0.3em под цифрами); HTML-текст берём рамкой самого текста (Range,
            // без line-height), но html2canvas рисует его на ~0.3em ниже, чем браузер.
            const isSvg = el.tagName === 'text';
            range.selectNodeContents(el);
            const r = isSvg ? el.getBoundingClientRect() : range.getBoundingClientRect();
            if (r.height <= 0 || r.top >= navTop) return;
            const em = parseFloat(view.getComputedStyle(el).fontSize) || 0;
            textBottom = Math.max(textBottom, r.bottom + (isSvg ? -0.3 : 0.3) * em);
        });
    }
    if (textBottom === 0) return;
    // Пустой низ svg под датами срезаем самой карточкой: иначе его
    // непрозрачный фон закрасит нижнюю рамку.
    card.style.overflow = 'hidden';
    const border = parseFloat(view.getComputedStyle(card).borderBottomWidth) || 0;
    const shift = textBottom + NAV_EXPORT_GAP + border - card.getBoundingClientRect().bottom;
    const mb = parseFloat(view.getComputedStyle(nav).marginBottom) || 0;
    nav.style.marginBottom = `${mb + shift}px`;
}

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
                (el.getAttribute('data-export-ignore') === 'true' && !hasNavigator(el)) ||
                el.classList.contains('chart-tooltip-overlay')
            );
        },
        // onclone: удаляем hover-UI (crosshair-линия, dimming, точки, date-pill,
        // карточка значений) из КЛОНА перед растеризацией. Нужно отдельно от
        // ignoreElements, потому что эти элементы живут ВНУТРИ chart-<svg> —
        // html2canvas сериализует svg целиком и per-element ignoreElements туда
        // не достаёт. Баг: «нажать экспорт и быстро навести курсор» → crosshair
        // попадал в снимок. Помечены классом chart-hover-ui в SimpleChart.
        onclone: (clonedDoc, clonedTarget) => {
            clonedDoc.querySelectorAll('.chart-hover-ui, .chart-tooltip-root').forEach((el) => el.remove());
            clonedTarget.querySelectorAll<HTMLElement>('.chart-navigator')
                .forEach((nav) => collapseNavigator(nav, clonedTarget));
        },
    });

    if (signal?.aborted) {
        throw new Error('Capture aborted');
    }

    return canvas;
}
