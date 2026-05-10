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

export type AnnotationTool = 'select' | 'pen' | 'line' | 'arrow' | 'rectangle' | 'circle' | 'text';

/**
 * Resolve CSS var строку в реальный hex/rgb. Canvas2D не парсит var(...) в
 * strokeStyle/fillStyle — нужно явно вычислить computed style. Если value
 * не CSS var, возвращается как есть.
 */
function resolveColor(value: string): string {
    if (!value || !value.startsWith('var(')) return value;
    const match = value.match(/var\((--[^,)]+)/);
    if (!match) return value;
    const varName = match[1].trim();
    if (typeof window === 'undefined') return value;
    const computed = getComputedStyle(document.documentElement)
        .getPropertyValue(varName).trim();
    return computed || value;
}

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
    /**
     * Callback после создания фигуры (rect/circle/line/arrow/text). Родитель
     * обычно переключает tool в 'select' — это даёт пользователю сразу
     * редактировать новую фигуру (drag/resize/rotate), а не клацать новые
     * поверх. UX-паттерн как в Figma/Excalidraw.
     */
    onShapeCreated?: () => void;
}

const AnnotationCanvas = forwardRef<AnnotationCanvasHandle, Props>(
    ({ background, tool, color, strokeWidth, onHistoryChange, onShapeCreated }, ref) => {
        const canvasRef = useRef<HTMLCanvasElement>(null);
        const containerRef = useRef<HTMLDivElement>(null);
        const fabricRef = useRef<FabricCanvas | null>(null);
        const redoStackRef = useRef<FabricObject[]>([]);
        const fabric = useFabric();
        // Refs для текущих tool/color/strokeWidth — handlers shape drawing
        // читают через них (вместо closure), чтобы handlers attach один раз
        // и реагировали на изменения без re-attach.
        const toolRef = useRef(tool);
        const colorRef = useRef(color);
        const strokeWidthRef = useRef(strokeWidth);
        const onShapeCreatedRef = useRef(onShapeCreated);
        useEffect(() => { toolRef.current = tool; }, [tool]);
        useEffect(() => { colorRef.current = color; }, [color]);
        useEffect(() => { strokeWidthRef.current = strokeWidth; }, [strokeWidth]);
        useEffect(() => { onShapeCreatedRef.current = onShapeCreated; }, [onShapeCreated]);

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
            brush.color = resolveColor(color);
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

            // Shape drawing handlers (line / arrow / rectangle / circle).
            // Активны только когда toolRef.current ≠ 'pen'. PencilBrush сама
            // обрабатывает pen mode, поэтому handlers просто early-return.
            // Refs дают handlers всегда актуальный tool/color/width без re-attach.
            let startPoint: { x: number; y: number } | null = null;
            let activeShape: FabricObject | null = null;

            // Helper: построить arrow path "M x1 y1 L x2 y2 + V-shape at end"
            const arrowPath = (x1: number, y1: number, x2: number, y2: number): string => {
                const dx = x2 - x1;
                const dy = y2 - y1;
                const len = Math.sqrt(dx * dx + dy * dy);
                if (len < 1) return `M ${x1} ${y1} L ${x2} ${y2}`;
                const headLen = Math.max(10, Math.min(20, len * 0.25));
                const angle = Math.atan2(dy, dx);
                const a1 = angle - Math.PI / 6; // 30°
                const a2 = angle + Math.PI / 6;
                const hx1 = x2 - headLen * Math.cos(a1);
                const hy1 = y2 - headLen * Math.sin(a1);
                const hx2 = x2 - headLen * Math.cos(a2);
                const hy2 = y2 - headLen * Math.sin(a2);
                return `M ${x1} ${y1} L ${x2} ${y2} M ${hx1} ${hy1} L ${x2} ${y2} L ${hx2} ${hy2}`;
            };

            fc.on('mouse:down', (opt) => {
                if (toolRef.current === 'pen') return; // pen handled by PencilBrush
                if (toolRef.current === 'select') return; // select mode: fabric handles object selection
                // Если кликнули на существующий object — fabric обработает selection,
                // не создаём новый shape (избегаем конфликта).
                if (opt.target) return;
                const fcLocal = fabricRef.current;
                if (!fcLocal) return;

                // Text tool: при клике добавляем редактируемый IText в точке клика.
                if (toolRef.current === 'text') {
                    // Если уже есть active object в edit mode — не создавать новый
                    // (даём пользователю кликать внутри существующего текста).
                    const activeObj = fcLocal.getActiveObject();
                    if (activeObj && activeObj.type === 'i-text') return;
                    const p = fcLocal.getViewportPoint(opt.e);
                    const fontSize = Math.max(14, strokeWidthRef.current * 6);
                    const text = new fabric.IText('Текст', {
                        left: p.x,
                        top: p.y - fontSize / 2,
                        fill: resolveColor(colorRef.current),
                        fontSize,
                        fontFamily: 'Inter, "Helvetica Neue", Helvetica, Arial, sans-serif',
                        fontWeight: 600,
                        editable: true,
                    });
                    fcLocal.add(text);
                    fcLocal.setActiveObject(text);
                    text.enterEditing();
                    text.selectAll();
                    redoStackRef.current = [];
                    onHistoryChange?.();
                    // UX: переключаем tool в select. Текст останется в editing
                    // mode (enterEditing/selectAll выше); смена tool на уровне
                    // toolbar не трогает fabric editing state. Зато после клика
                    // вне (commit текста) пользователь будет в select mode,
                    // а не создаст ещё один текст случайным кликом.
                    onShapeCreatedRef.current?.();
                    return;
                }

                const p = fcLocal.getViewportPoint(opt.e);
                startPoint = { x: p.x, y: p.y };
                const c = resolveColor(colorRef.current);
                const w = strokeWidthRef.current;
                const common = {
                    stroke: c,
                    strokeWidth: w,
                    fill: 'transparent',
                    selectable: false,
                    evented: false,
                    strokeUniform: true,
                };
                if (toolRef.current === 'line') {
                    activeShape = new fabric.Line([p.x, p.y, p.x, p.y], { ...common, strokeLineCap: 'round' });
                } else if (toolRef.current === 'arrow') {
                    activeShape = new fabric.Path(arrowPath(p.x, p.y, p.x, p.y), { ...common, strokeLineCap: 'round', strokeLineJoin: 'round' });
                } else if (toolRef.current === 'rectangle') {
                    activeShape = new fabric.Rect({ ...common, left: p.x, top: p.y, width: 0, height: 0, rx: 2, ry: 2 });
                } else if (toolRef.current === 'circle') {
                    activeShape = new fabric.Ellipse({ ...common, left: p.x, top: p.y, rx: 0, ry: 0 });
                }
                if (activeShape) fcLocal.add(activeShape);
            });

            fc.on('mouse:move', (opt) => {
                if (!startPoint || !activeShape) return;
                const fcLocal = fabricRef.current;
                if (!fcLocal) return;
                const p = fcLocal.getViewportPoint(opt.e);
                if (toolRef.current === 'line') {
                    (activeShape as InstanceType<typeof fabric.Line>).set({ x2: p.x, y2: p.y });
                } else if (toolRef.current === 'arrow') {
                    fcLocal.remove(activeShape);
                    const c = resolveColor(colorRef.current);
                    const w = strokeWidthRef.current;
                    activeShape = new fabric.Path(arrowPath(startPoint.x, startPoint.y, p.x, p.y), {
                        stroke: c, strokeWidth: w, fill: 'transparent',
                        selectable: false, evented: false, strokeUniform: true,
                        strokeLineCap: 'round', strokeLineJoin: 'round',
                    });
                    fcLocal.add(activeShape);
                } else if (toolRef.current === 'rectangle') {
                    activeShape.set({
                        left: Math.min(startPoint.x, p.x),
                        top: Math.min(startPoint.y, p.y),
                        width: Math.abs(p.x - startPoint.x),
                        height: Math.abs(p.y - startPoint.y),
                    });
                } else if (toolRef.current === 'circle') {
                    const rx = Math.abs(p.x - startPoint.x) / 2;
                    const ry = Math.abs(p.y - startPoint.y) / 2;
                    activeShape.set({
                        left: Math.min(startPoint.x, p.x),
                        top: Math.min(startPoint.y, p.y),
                        rx, ry,
                    });
                }
                activeShape.setCoords();
                fcLocal.requestRenderAll();
            });

            fc.on('mouse:up', () => {
                if (activeShape) {
                    // После рисования делаем shape manipulable (drag/resize/rotate)
                    activeShape.set({ selectable: true, evented: true });
                    activeShape.setCoords();
                    // UX: автоматически выделяем созданную фигуру — пользователь
                    // сразу видит handles для drag/resize/rotate. Без этого
                    // следующий клик трактовался бы как «начать рисовать новую
                    // фигуру», и поменять старую было бы невозможно без явного
                    // переключения tool→select.
                    const fcLocal = fabricRef.current;
                    if (fcLocal) {
                        fcLocal.setActiveObject(activeShape);
                        fcLocal.requestRenderAll();
                    }
                    redoStackRef.current = [];
                    onHistoryChange?.();
                    // Сообщаем родителю — он переключит tool в 'select'
                    // (паттерн Figma/Excalidraw: после создания → manipulate).
                    onShapeCreatedRef.current?.();
                }
                startPoint = null;
                activeShape = null;
            });

            // Keyboard handler: Delete/Backspace удаляет active object.
            // Не активен когда IText в editing mode (там Backspace удаляет символы).
            const onKeyDown = (e: KeyboardEvent) => {
                const fcLocal = fabricRef.current;
                if (!fcLocal) return;
                if (e.key !== 'Delete' && e.key !== 'Backspace') return;
                const active = fcLocal.getActiveObject();
                if (!active) return;
                // Если это IText в editing mode — не вмешиваемся
                if (active.type === 'i-text' && (active as InstanceType<typeof fabric.IText>).isEditing) return;
                fcLocal.remove(active);
                fcLocal.discardActiveObject();
                fcLocal.requestRenderAll();
                redoStackRef.current = [];
                onHistoryChange?.();
                e.preventDefault();
            };
            window.addEventListener('keydown', onKeyDown);

            return () => {
                cancelled = true;
                window.removeEventListener('keydown', onKeyDown);
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
            // Pen использует PencilBrush (isDrawingMode). Shape tools используют
            // mouse:down/move/up handlers (через toolRef.current).
            fc.isDrawingMode = tool === 'pen';
            // Marquee selection (drag-rect для multi-select) — только в select mode.
            // В drawing modes отключаем чтобы drag по canvas не создавал selection-rect.
            fc.selection = tool === 'select';
            // Cursor подсказывает: в select — стрелка, в drawing — крест.
            fc.defaultCursor = tool === 'select' ? 'default' : 'crosshair';
            fc.hoverCursor = tool === 'select' ? 'move' : 'crosshair';
            const brush = fc.freeDrawingBrush;
            if (brush) {
                brush.color = resolveColor(color);
                brush.width = strokeWidth;
            }

            // UX-бонус: если есть выделенный объект — применить новый color/width
            // к нему «вживую». Так палитра в toolbar работает не только для
            // будущих фигур, но и для текущей (как в Figma/Excalidraw).
            // i-text использует `fill` (заливка), shapes — `stroke` (контур).
            const active = fc.getActiveObject();
            if (active) {
                const c = resolveColor(color);
                if (active.type === 'i-text') {
                    active.set({ fill: c, fontSize: Math.max(14, strokeWidth * 6) });
                } else {
                    active.set({ stroke: c, strokeWidth });
                }
                fc.requestRenderAll();
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
