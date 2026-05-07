/**
 * useFabric — lazy-load fabric.js при первой попытке войти в annotation mode.
 *
 * Bundle: fabric ~200KB gzipped попадает в отдельный chunk (`fabric-*.js`),
 * грузится ТОЛЬКО при клике "✏️ Рисовать" — пользователи которые просто
 * скачивают PNG (без разметки) не платят за этот chunk.
 *
 * Возвращает namespace модуля fabric (Canvas, PencilBrush, FabricImage и т.д.)
 * либо null пока chunk загружается. Component'ы рендерят skeleton/loader пока
 * fabric === null.
 */

import { useState, useEffect } from 'react';

type FabricModule = typeof import('fabric');

export function useFabric(): FabricModule | null {
    const [fabric, setFabric] = useState<FabricModule | null>(null);

    useEffect(() => {
        let cancelled = false;
        import('fabric').then((mod) => {
            if (!cancelled) setFabric(mod);
        });
        return () => {
            cancelled = true;
        };
    }, []);

    return fabric;
}
