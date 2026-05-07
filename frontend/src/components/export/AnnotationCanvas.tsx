/**
 * AnnotationCanvas — fabric.js canvas overlay поверх captured chart.
 *
 * Архитектура:
 *   1. Background = chart canvas (passed from preview), set via FabricImage
 *   2. fabric.Canvas с isDrawingMode = true, freeDrawingBrush для pen
 *   3. History stack (undo/redo): track objects через event 'path:created'
 *   4. Exposes API через ref: undo/redo/clear/exportToCanvas/getCounts
 *
 * Resize handling:
 *   - Native canvas size = background dimensions (px-perfect drawing)
 *   - CSS width/height scale-to-fit container via ResizeObserver
 *   - fabric.Canvas создаёт upper-canvas (event layer) — он тоже скейлится
 *
 * Lazy fabric:
 *   - Пока fabric === null — показываем skeleton placeholder
 *   - Init происходит ТОЛЬКО когда fabric loaded и canvas DOM ready
 *   - React strict-mode: guard через if (fabricRef.current) return
 */

import { forwardRef, useEffect, useImperativeHandle, useRef } from 'react';
import { Loader2 } from 'lucide-react';
import { useFabric } from './useFabric';
import type { Canvas as FabricCanvas, FabricObject } from 'fabric';

export type AnnotationTool = 'pen' | 'select';

export interface AnnotationCanvasHandle {
    undo: () => void;
    redo: () => void;
    clear: () => void;
    exportToCanvas: () => HTMLCanvasElement | null;
    getObjectsCount: () => number;
    getRedoCount: () => number;
}

interface Props {
    background: HTMLCanvasElement;
    tool: AnnotationTool;
    color: string;
    strokeWidth: number;
    /** Callback при изменении истории (для toolbar disabled-state на Undo/Redo) */
    onHistoryChange?: () => void;
}

const AnnotationCanvas = forwardRef<AnnotationCanvasHandle, Props>(
    ({ background, tool, color, strokeWidth, onHistoryChange }, ref) => {
        const canvasRef = useRef<HTMLCanvasElement>(null);
        const containerRef = useRef<HTMLDivElement>(null);
        const fabricRef = useRef<FabricCanvas | null>(null);
        const redoStackRef = useRef<FabricObject[]>([]);
        const fabric = useFabric();

        // Init fabric.Canvas один раз при загрузке fabric и DOM-ready.
        // Strict-mode guard: ref проверяется перед созданием — повторный mount
        // в dev-mode не приведёт к double-init на одном <canvas> element.
        useEffect(() => {
            if (!fabric || !canvasRef.current) return;
            if (fabricRef.current) return;

            const fc = new fabric.Canvas(canvasRef.current, {
                width: background.width,
                height: background.height,
                isDrawingMode: true,
                selection: false,
                // Background fill important — иначе видно прозрачный fabric layer
                // под background image (видно только при partial render glitches).
                backgroundColor: 'transparent',
            });
            fabricRef.current = fc;

            // Init brush — sane defaults, потом обновляются через отдельный effect
            const brush = new fabric.PencilBrush(fc);
            brush.width = strokeWidth;
            brush.color = color;
            fc.freeDrawingBrush = brush;

            // Set background — async (FabricImage.fromURL → Promise)
            let cancelled = false;
            fabric.FabricImage.fromURL(background.toDataURL()).then((img) => {
                if (cancelled || !fabricRef.current) return;
                // Размер background image = native canvas size (1:1 без scale)
                img.set({ left: 0, top: 0, selectable: false, evented: false });
                fabricRef.current.backgroundImage = img;
                fabricRef.current.requestRenderAll();
            });

            // History tracking — каждый новый path сбрасывает redo stack
            // (стандартное textarea behavior — после нового действия forward-history теряется)
            fc.on('path:created', () => {
                redoStackRef.current = [];
                onHistoryChange?.();
            });

            return () => {
                cancelled = true;
                fabricRef.current?.dispose();
                fabricRef.current = null;
            };
            // eslint-disable-next-line react-hooks/exhaustive-deps
        }, [fabric, background]);

        // Update brush + drawing mode при смене tool/color/strokeWidth.
        // Отдельный effect чтобы не пересоздавать canvas при tool change.
        useEffect(() => {
            const fc = fabricRef.current;
            if (!fc) return;
            fc.isDrawingMode = tool === 'pen';
            const brush = fc.freeDrawingBrush;
            if (brush) {
                brush.color = color;
                brush.width = strokeWidth;
            }
        }, [tool, color, strokeWidth]);

        // Scale-to-fit container: native canvas size остаётся = background
        // (drawing px-perfect), CSS scales display size. ResizeObserver реагирует
        // на window resize / modal aspect changes.
        useEffect(() => {
            const container = containerRef.current;
            if (!container) return;

            const fit = () => {
                const cw = container.clientWidth;
                const ch = container.clientHeight;
                if (cw === 0 || ch === 0) return;
                const ratio = background.width / background.height;
                let w = cw;
                let h = cw / ratio;
                if (h > ch) {
                    h = ch;
                    w = ch * ratio;
                }
                // fabric создаёт два canvas: lower (drawing) + upper (event layer).
                // Оба обёрнуты в container .canvas-container — скейлим его.
                const wrap = canvasRef.current?.parentElement;
                if (wrap && wrap.classList.contains('canvas-container')) {
                    wrap.style.width = `${w}px`;
                    wrap.style.height = `${h}px`;
                }
                // Внутренние <canvas> элементы — fabric сам управляет их CSS,
                // но для надёжности скейлим обоих.
                const lower = canvasRef.current;
                const upper = wrap?.querySelector('.upper-canvas') as HTMLCanvasElement | null;
                if (lower) {
                    lower.style.width = `${w}px`;
                    lower.style.height = `${h}px`;
                }
                if (upper) {
                    upper.style.width = `${w}px`;
                    upper.style.height = `${h}px`;
                }
            };

            fit();
            const ro = new ResizeObserver(fit);
            ro.observe(container);
            return () => ro.disconnect();
        }, [background, fabric]);

        // Imperative API. Все методы guard на fabricRef — caller может вызвать
        // до полной инициализации (хотя UI кнопки disabled пока fabric === null).
        useImperativeHandle(
            ref,
            () => ({
                undo: () => {
                    const fc = fabricRef.current;
                    if (!fc) return;
                    const objs = fc.getObjects();
                    const last = objs[objs.length - 1];
                    if (!last) return;
                    redoStackRef.current.push(last);
                    fc.remove(last);
                    fc.requestRenderAll();
                    onHistoryChange?.();
                },
                redo: () => {
                    const fc = fabricRef.current;
                    if (!fc) return;
                    const obj = redoStackRef.current.pop();
                    if (!obj) return;
                    fc.add(obj);
                    fc.requestRenderAll();
                    onHistoryChange?.();
                },
                clear: () => {
                    const fc = fabricRef.current;
                    if (!fc) return;
                    const objs = fc.getObjects();
                    if (objs.length === 0) return;
                    redoStackRef.current = [];
                    fc.remove(...objs);
                    fc.requestRenderAll();
                    onHistoryChange?.();
                },
                exportToCanvas: () => {
                    const fc = fabricRef.current;
                    if (!fc) return null;
                    // toCanvasElement(multiplier) экспортирует ВЕСЬ canvas:
                    // background image + все objects (paths, shapes, text).
                    // multiplier=1 — native resolution (background уже DPR-scaled).
                    return fc.toCanvasElement(1) as HTMLCanvasElement;
                },
                getObjectsCount: () => fabricRef.current?.getObjects().length ?? 0,
                getRedoCount: () => redoStackRef.current.length,
            }),
            [onHistoryChange],
        );

        if (!fabric) {
            return (
                <div className="flex-1 flex flex-col items-center justify-center gap-3" style={{ minHeight: 0 }}>
                    <Loader2 className="animate-spin" size={36} style={{ color: 'var(--accent)' }} />
                    <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-sm)' }}>
                        Загружаем инструменты рисования…
                    </span>
                </div>
            );
        }

        return (
            <div
                ref={containerRef}
                className="flex-1 flex items-center justify-center w-full overflow-hidden"
                style={{ minHeight: 0 }}
            >
                <canvas ref={canvasRef} style={{ display: 'block' }} />
            </div>
        );
    },
);

AnnotationCanvas.displayName = 'AnnotationCanvas';

export default AnnotationCanvas;
