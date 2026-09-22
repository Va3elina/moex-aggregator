/**
 * ChartCaptureButton — кнопка-trigger в углу chart-card.
 *
 * Click → открывает ExportModal. ExportModal отрендерится через portal,
 * поэтому положение кнопки не влияет на положение modal.
 *
 * data-export-ignore="true" — атрибут для html2canvas чтобы не включать
 * саму кнопку в snapshot.
 *
 * Lazy-import ExportModal — ExportModal импортирует html2canvas, который
 * грузится lazy. Из ChartCaptureButton делаем dynamic import самой modal,
 * чтобы initial render кнопки не тащил chunk html2canvas.
 *
 * Прогрев чанков (prefetchExportChunks). Оба чанка раньше качались только по
 * первому клику на 📷. Деплои идут по несколько раз в день, hashed-имена чанков
 * меняются, а старый бандл в долго открытой вкладке ссылается на удалённые
 * файлы → 404 → vite:preloadError → main.tsx перезагружает вкладку прямо
 * посреди работы: юзер выставил период, нажал скриншот и получил сброс всей
 * страницы. Теперь чанки подтягиваются в idle сразу после первого монтирования
 * кнопки (бандл в этот момент ещё свежий) и к клику уже лежат в памяти модуля.
 */

import { useState, useEffect, lazy, Suspense } from 'react';
import { Camera } from 'lucide-react';
import type { ExportMetadata } from './types';
import { useAnalytics } from '../../contexts/AnalyticsContext';
import { useTranslation } from 'react-i18next';

// Lazy-import — modal + html2canvas chunk выделяется отдельно
const ExportModal = lazy(() => import('./ExportModal'));

// Один раз на вкладку: ошибки глотаем (offline и т.п.) — клик по кнопке всё
// равно попробует загрузить заново через lazy()/captureChart.
let exportChunksPrefetched = false;
function prefetchExportChunks() {
    if (exportChunksPrefetched) return;
    exportChunksPrefetched = true;
    import('./ExportModal').catch(() => {});
    import('html2canvas').catch(() => {});
}

function useExportChunksPrefetch() {
    useEffect(() => {
        if (exportChunksPrefetched) return;
        if (typeof window.requestIdleCallback === 'function') {
            const id = window.requestIdleCallback(prefetchExportChunks, { timeout: 5000 });
            return () => window.cancelIdleCallback(id);
        }
        const t = window.setTimeout(prefetchExportChunks, 2000);
        return () => window.clearTimeout(t);
    }, []);
}

interface Props {
    /** Function returning DOM element для capture (lazy для актуального ref) */
    getTargetElement: () => HTMLElement | null;
    /** Имя файла без extension */
    filename: string;
    /** Метаданные для header в экспортированном изображении (title/asset/period) */
    metadata?: ExportMetadata;
    /** CSS property→value pairs для transient style override на target element
     *  ПЕРЕД capture (восстанавливаются после). Used для adjustments что нужны
     *  только в exported PNG (e.g. сократить --chart-pad-left на FlowsHistogram).
     *  Function — значение может зависеть от текущего state. */
    getExportStyles?: () => Record<string, string>;
    /** Дополнительные классы кнопки */
    className?: string;
}

export default function ChartCaptureButton({
    getTargetElement,
    filename,
    metadata,
    getExportStyles,
    className = '',
}: Props) {
    const { t } = useTranslation();
    const [open, setOpen] = useState(false);
    const [target, setTarget] = useState<HTMLElement | null>(null);
    const [styles, setStyles] = useState<Record<string, string> | undefined>(undefined);
    const { track } = useAnalytics();
    useExportChunksPrefetch();

    const handleClick = () => {
        const el = getTargetElement();
        if (!el) {
            console.warn('[ChartCaptureButton] target element is null');
            return;
        }
        setTarget(el);
        // Evaluate styles at click-time (depends on current state e.g. viewMode)
        setStyles(getExportStyles ? getExportStyles() : undefined);
        setOpen(true);

        // Analytics: indicator извлекаем из filename (e.g. "frame-oi-SBER-1y" → "oi")
        const indicator = filename.startsWith('frame-')
            ? filename.split('-')[1] || 'unknown'
            : 'unknown';
        track('chart_export', {
            indicator,
            filename,
            asset: metadata?.asset,
            ticker: metadata?.ticker,
        });
    };

    const handleClose = () => {
        setOpen(false);
        setTarget(null);
    };

    return (
        <>
            <button
                type="button"
                onClick={handleClick}
                data-export-ignore="true"
                className={`editorial-press rounded-full inline-flex items-center justify-center ${className}`}
                style={{
                    backgroundColor: 'var(--bg-secondary)',
                    border: '2px solid var(--text-primary)',
                    color: 'var(--text-primary)',
                    width: 44,
                    height: 44,
                }}
                aria-label={t('Скачать график')}
                title={t('Скачать график')}
            >
                <Camera size={22} />
            </button>
            {open && target && (
                <Suspense fallback={null}>
                    <ExportModal
                        targetElement={target}
                        filename={filename}
                        metadata={metadata}
                        exportStyles={styles}
                        onClose={handleClose}
                    />
                </Suspense>
            )}
        </>
    );
}
