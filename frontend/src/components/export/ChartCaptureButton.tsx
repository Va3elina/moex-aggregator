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

// Lazy-import — modal + html2canvas chunk выделяется отдельно
const ExportModal = lazy(() => import('./ExportModal'));

interface Props {
    /** Function returning DOM element для capture (lazy для актуального ref) */
    getTargetElement: () => HTMLElement | null;
    /** Имя файла без extension */
    filename: string;
    /** Дополнительные классы кнопки */
    className?: string;
}

export default function ChartCaptureButton({
    getTargetElement,
    filename,
    className = '',
}: Props) {
    const [open, setOpen] = useState(false);
    const [target, setTarget] = useState<HTMLElement | null>(null);

    const handleClick = () => {
        const el = getTargetElement();
        if (!el) {
            console.warn('[ChartCaptureButton] target element is null');
            return;
        }
        setTarget(el);
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
                className={`editorial-press rounded-lg p-2 inline-flex items-center justify-center ${className}`}
                style={{
                    backgroundColor: 'var(--bg-primary)',
                    border: '1.5px solid var(--text-primary)',
                    color: 'var(--text-primary)',
                }}
                aria-label="Скачать график"
                title="Скачать график"
            >
                <Camera size={16} />
            </button>
            {open && target && (
                <Suspense fallback={null}>
                    <ExportModal
                        targetElement={target}
                        filename={filename}
                        onClose={handleClose}
                    />
                </Suspense>
            )}
        </>
    );
}
