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
 */

import { useState, lazy, Suspense } from 'react';
import { Camera } from 'lucide-react';
import type { ExportMetadata } from './types';

// Lazy-import — modal + html2canvas chunk выделяется отдельно
const ExportModal = lazy(() => import('./ExportModal'));

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
    const [open, setOpen] = useState(false);
    const [target, setTarget] = useState<HTMLElement | null>(null);
    const [styles, setStyles] = useState<Record<string, string> | undefined>(undefined);

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
                aria-label="Скачать график"
                title="Скачать график"
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
