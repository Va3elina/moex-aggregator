/**
 * useViewportWidth — реактивный хук на window.innerWidth.
 *
 * Возвращает текущую ширину viewport в px. Слушает resize и обновляется
 * при пересечении breakpoint'ов.
 *
 * Используется ChartLegend, SimpleChart и другими компонентами которые
 * не имеют ResizeObserver на конкретном parent — им нужна общая viewport
 * width для discrete breakpoint logic.
 *
 * Проектные breakpoints (см. components/chart/chartTypography.ts):
 *   320 / 375 / 425 / 768 / 1024 / 1440
 */
import { useEffect, useState } from 'react';

export function useViewportWidth(): number {
    const [width, setWidth] = useState<number>(() =>
        typeof window !== 'undefined' ? window.innerWidth : 1440,
    );

    useEffect(() => {
        const handler = () => setWidth(window.innerWidth);
        window.addEventListener('resize', handler);
        return () => window.removeEventListener('resize', handler);
    }, []);

    return width;
}
