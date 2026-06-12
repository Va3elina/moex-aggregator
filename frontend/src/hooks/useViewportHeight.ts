/**
 * useViewportHeight — реактивный хук на window.innerHeight.
 *
 * Зеркало useViewportWidth. Нужен чартам внутри модалок/скроллящихся
 * контейнеров, где useFitToViewport неприменим (он меряет от anchor.top,
 * который в скроллящейся модалке зависит от позиции скролла).
 *
 * Паттерн: height={Math.max(MIN, Math.min(BASE, vh - RESERVED))} —
 * на обычных экранах чарт остаётся BASE, на коротких (телефон в
 * landscape, iPhone SE) сжимается, чтобы влезать в видимую область.
 */
import { useEffect, useState } from 'react';

export function useViewportHeight(): number {
    const [height, setHeight] = useState<number>(() =>
        typeof window !== 'undefined' ? window.innerHeight : 900,
    );

    useEffect(() => {
        const handler = () => setHeight(window.innerHeight);
        window.addEventListener('resize', handler);
        return () => window.removeEventListener('resize', handler);
    }, []);

    return height;
}
