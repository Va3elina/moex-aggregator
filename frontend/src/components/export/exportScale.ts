/**
 * exportScale — во сколько раз снимок графика плотнее его экранного размера.
 *
 * Раньше html2canvas снимал ровно с window.devicePixelRatio, и качество PNG
 * зависело от машины: на маке (retina, dpr 2) картинка выходила чёткой, на
 * обычном Windows-мониторе (dpr 1) — мыльной при тех же входных данных.
 * Теперь снимаем минимум в MIN_EXPORT_SCALE раз плотнее экрана, а на ретине
 * оставляем родной dpr, если он выше.
 *
 * Верхняя граница нужна против абсурдных холстов на полноэкранном экспорте:
 * широкая панель × 2 легко даёт 5-6 тысяч пикселей, дальше растут только
 * память и время растеризации. Клампим так, чтобы никогда не опуститься ниже
 * dpr — иначе на ретине мы бы УХУДШИЛИ то, что работало.
 */

const MIN_EXPORT_SCALE = 2;
const MAX_EXPORT_DIMENSION = 6000;

function screenDpr(): number {
    return typeof window !== 'undefined' ? window.devicePixelRatio || 1 : 1;
}

/**
 * @param element снимаемый элемент — по его CSS-размеру считается верхний кламп.
 *                Без элемента возвращается неограниченный масштаб.
 */
export function exportPixelScale(element?: HTMLElement | null): number {
    const dpr = screenDpr();
    let scale = Math.max(MIN_EXPORT_SCALE, dpr);

    const longestSide = element
        ? Math.max(element.offsetWidth, element.offsetHeight)
        : 0;
    if (longestSide > 0) {
        scale = Math.min(scale, MAX_EXPORT_DIMENSION / longestSide);
    }

    return Math.max(dpr, scale);
}
