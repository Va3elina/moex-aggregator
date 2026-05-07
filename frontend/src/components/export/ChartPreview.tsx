/**
 * ChartPreview — показывает framed canvas внутри modal с auto-fit к available space.
 *
 * Принимает HTMLCanvasElement и рендерит его в `<canvas>` через `drawImage`.
 * Аналог `<img>` для canvas — но canvas vs image даёт нам контроль над DPR
 * рендеринга (если будем добавлять annotation layer в Phase 3).
 *
 * Note: при resize окна preview-canvas пересчитывается чтобы fit container.
 */

import { useEffect, useRef } from 'react';

interface Props {
    canvas: HTMLCanvasElement;
}

export default function ChartPreview({ canvas }: Props) {
    const containerRef = useRef<HTMLDivElement>(null);
    const previewRef = useRef<HTMLCanvasElement>(null);

    useEffect(() => {
        const preview = previewRef.current;
        const container = containerRef.current;
        if (!preview || !container) return;

        const draw = () => {
            const cw = container.clientWidth;
            const ch = container.clientHeight;
            const ratio = canvas.width / canvas.height;

            // Scale-to-fit с preserve aspect-ratio
            let w = cw;
            let h = cw / ratio;
            if (h > ch) {
                h = ch;
                w = ch * ratio;
            }

            preview.width = canvas.width;
            preview.height = canvas.height;
            preview.style.width = `${w}px`;
            preview.style.height = `${h}px`;

            const ctx = preview.getContext('2d');
            if (!ctx) return;
            ctx.drawImage(canvas, 0, 0);
        };

        draw();
        const ro = new ResizeObserver(draw);
        ro.observe(container);
        return () => ro.disconnect();
    }, [canvas]);

    return (
        <div
            ref={containerRef}
            className="flex-1 flex items-center justify-center w-full overflow-hidden"
            style={{ minHeight: 0 }}
        >
            <canvas
                ref={previewRef}
                style={{ display: 'block', maxWidth: '100%', maxHeight: '100%' }}
            />
        </div>
    );
}
