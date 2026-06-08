/**
 * AnnotationCanvas — fabric.js canvas overlay поверх captured chart.
 *
 * Архитектура:
 *   1. Background = chart canvas (passed from preview), set via FabricImage
 *   2. fabric.Canvas с isDrawingMode = true, freeDrawingBrush для pen
 *   3. History (undo/redo): snapshot-стек JSON-снимков объектов — отменяет
 *      любое изменение (создание, move/resize/rotate, правку текста, удаление)
 *   4. Exposes API через ref: undo/redo/clear/exportToCanvas/canUndo/canRedo
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

import { forwardRef, useCallback, useEffect, useImperativeHandle, useRef } from 'react';
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

/**
 * Парсит resolved CSS-цвет (#rgb / #rrggbb / rgb()/rgba()) в перцептивную
 * яркость 0..1 (WCAG-luma). Для var-цветов передавать уже resolved hex/rgb.
 * Неизвестный формат → 0.5 (нейтрально).
 */
function luminance(color: string): number {
    let r = 0, g = 0, b = 0;
    const c = color.trim();
    const hex = c.match(/^#([0-9a-f]{3}|[0-9a-f]{6})$/i);
    if (hex) {
        let h = hex[1];
        if (h.length === 3) h = h[0] + h[0] + h[1] + h[1] + h[2] + h[2];
        r = parseInt(h.slice(0, 2), 16);
        g = parseInt(h.slice(2, 4), 16);
        b = parseInt(h.slice(4, 6), 16);
    } else {
        const rgb = c.match(/rgba?\(\s*(\d+)[,\s]+(\d+)[,\s]+(\d+)/i);
        if (rgb) {
            r = +rgb[1]; g = +rgb[2]; b = +rgb[3];
        } else {
            return 0.5;
        }
    }
    return (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255;
}

/**
 * Цвет контрастной окантовки — контрастирует ФОН (тему), а НЕ краску.
 * Светлая тема (editorial-light) → почти чёрный, тёмная → белый.
 *
 * Раньше окантовка контрастировала саму краску (luminance(ink)), но оранжевый
 * accent #FF5C2B имеет luma≈0.48 (< 0.6) → попадал в «тёмную краску» → получал
 * БЕЛУЮ окантовку, невидимую на светлой бумаге. Фон/тема — надёжный сигнал:
 * холст лежит поверх скриншота текущей темы, поэтому контраст к теме читается
 * на обоих фонах.
 */
function contrastColor(alpha = 1): string {
    const t = typeof document !== 'undefined'
        ? document.documentElement.getAttribute('data-theme')
        : null;
    let light: boolean;
    if (t === 'editorial-light') light = true;
    else if (t === 'editorial-dark') light = false;
    else {
        // Фоллбэк (нет/неизвестный data-theme) — по яркости фона.
        try {
            const bg = getComputedStyle(document.documentElement).getPropertyValue('--bg-primary');
            light = luminance(bg.trim()) > 0.5;
        } catch {
            light = false;
        }
    }
    return light ? `rgba(0,0,0,${alpha})` : `rgba(255,255,255,${alpha})`;
}


export interface AnnotationCanvasHandle {
    undo: () => void;
    redo: () => void;
    clear: () => void;
    exportToCanvas: () => HTMLCanvasElement | null;
    getObjectsCount: () => number;
    canUndo: () => boolean;
    canRedo: () => boolean;
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
        // История undo/redo: snapshot-стек. Каждый снимок — JSON массива объектов
        // (фон не сериализуется — он живёт отдельно как backgroundImage). Так undo
        // отменяет ЛЮБОЕ изменение: создание, move/resize/rotate, правку текста,
        // перекраску, удаление — а не только последнюю добавленную фигуру.
        const historyRef = useRef<string[]>([]);
        const historyIdxRef = useRef<number>(-1);
        const restoringRef = useRef<boolean>(false);
        const fabric = useFabric();
        // Refs для текущих tool/color/strokeWidth — handlers shape drawing
        // читают через них (вместо closure), чтобы handlers attach один раз
        // и реагировали на изменения без re-attach.
        const toolRef = useRef(tool);
        const colorRef = useRef(color);
        const strokeWidthRef = useRef(strokeWidth);
        const onShapeCreatedRef = useRef(onShapeCreated);
        const onHistoryChangeRef = useRef(onHistoryChange);
        useEffect(() => { toolRef.current = tool; }, [tool]);
        useEffect(() => { colorRef.current = color; }, [color]);
        useEffect(() => { strokeWidthRef.current = strokeWidth; }, [strokeWidth]);
        useEffect(() => { onShapeCreatedRef.current = onShapeCreated; }, [onShapeCreated]);
        useEffect(() => { onHistoryChangeRef.current = onHistoryChange; }, [onHistoryChange]);

        // ── История undo/redo (snapshot-стек) ──────────────────────────────
        // saveSnapshot пишет JSON-снимок объектов на каждое дискретное изменение;
        // restore грузит снимок обратно через enlivenObjects.
        const saveSnapshot = useCallback(() => {
            const fc = fabricRef.current;
            if (!fc || restoringRef.current) return;
            const snap = JSON.stringify(fc.getObjects().map((o) => o.toObject()));
            // Дубль не пишем: object:modified иногда стреляет вхолостую, а пара
            // object:modified + text:editing:exited прилетает на одно событие.
            if (historyRef.current[historyIdxRef.current] === snap) return;
            // Новое действие обрезает redo-ветку.
            historyRef.current = historyRef.current.slice(0, historyIdxRef.current + 1);
            historyRef.current.push(snap);
            historyIdxRef.current = historyRef.current.length - 1;
            onHistoryChangeRef.current?.();
        }, []);

        const restore = useCallback(async (targetIdx: number) => {
            const fc = fabricRef.current;
            if (!fc || !fabric || restoringRef.current) return;
            if (targetIdx < 0 || targetIdx >= historyRef.current.length) return;
            restoringRef.current = true;
            try {
                const objs = JSON.parse(historyRef.current[targetIdx]);
                const enlivened = (await fabric.util.enlivenObjects(objs)) as FabricObject[];
                const live = fabricRef.current;
                if (!live) return;
                live.remove(...live.getObjects());
                live.add(...enlivened);
                enlivened.forEach((o) => o.setCoords());
                live.discardActiveObject();
                live.requestRenderAll();
                historyIdxRef.current = targetIdx;
            } finally {
                restoringRef.current = false;
            }
            onHistoryChangeRef.current?.();
        }, [fabric]);

        const undo = useCallback(() => { void restore(historyIdxRef.current - 1); }, [restore]);
        const redo = useCallback(() => { void restore(historyIdxRef.current + 1); }, [restore]);

        const clearAll = useCallback(() => {
            const fc = fabricRef.current;
            if (!fc || fc.getObjects().length === 0) return;
            fc.remove(...fc.getObjects());
            fc.discardActiveObject();
            fc.requestRenderAll();
            saveSnapshot(); // очистка — тоже шаг истории, её можно отменить
        }, [saveSnapshot]);

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

            // Init brush — sane defaults, потом обновляются через отдельный effect.
            const brush = new fabric.PencilBrush(fc);
            brush.width = strokeWidth;
            brush.color = resolveColor(color);
            fc.freeDrawingBrush = brush;

            // Стартовый снимок — пустой холст (фон в backgroundImage, не в objects).
            // Первый undo вернёт сюда.
            historyRef.current = ['[]'];
            historyIdxRef.current = 0;

            // Set background — async (FabricImage.fromURL → Promise)
            let cancelled = false;
            fabric.FabricImage.fromURL(background.toDataURL()).then((img) => {
                if (cancelled || !fabricRef.current) return;
                // Размер background image = native canvas size (1:1 без scale)
                img.set({ left: 0, top: 0, selectable: false, evented: false });
                fabricRef.current.backgroundImage = img;
                fabricRef.current.requestRenderAll();
            });

            // Снимок истории на каждое дискретное изменение холста:
            //   path:created          — завершён штрих карандаша
            //   object:modified       — завершён drag / resize / rotate объекта
            //   text:editing:exited   — завершено редактирование текста
            fc.on('path:created', () => saveSnapshot());
            fc.on('object:modified', () => saveSnapshot());
            fc.on('text:editing:exited', () => saveSnapshot());

            // Рамка/маркеры выделения — контраст к теме: дефолтный голубой fabric
            // почти не виден на светлой бумаге. Чёрные на светлой / белые на тёмной,
            // залитые непрозрачные маркеры + жирнее рамка → выделение хорошо видно.
            const selColor = contrastColor(1);
            fc.on('object:added', (e) => {
                const o = e.target;
                if (o) o.set({
                    borderColor: selColor,
                    cornerColor: selColor,
                    transparentCorners: false,
                    cornerSize: 10,
                    borderScaleFactor: 2,
                });
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
                    const c = resolveColor(colorRef.current);
                    // Текст: тонкий контур контрастного цвета вокруг
                    // глифов (stroke) — буквы читаются и на светлой бумаге, и на
                    // тёмных свечах. paintFirst:'stroke' кладёт обводку ПОД заливку,
                    // чтобы не «съедать» тонкие штрихи шрифта.
                    const contour = contrastColor(0.95); // контраст к теме, не к краске
                    const text = new fabric.IText('Текст', {
                        left: p.x,
                        top: p.y - fontSize / 2,
                        fill: c,
                        fontSize,
                        fontFamily: 'Inter, "Helvetica Neue", Helvetica, Arial, sans-serif',
                        fontWeight: 600,
                        editable: true,
                        stroke: contour,
                        strokeWidth: Math.max(2, fontSize * 0.08),
                        paintFirst: 'stroke',
                        strokeUniform: true,
                        // Без shadow-гало — для текста чистый контур читается лучше,
                        // чем размытое гало (выглядело как лишняя «подсветка»).
                    });
                    fcLocal.add(text);
                    fcLocal.setActiveObject(text);
                    text.enterEditing();
                    text.selectAll();
                    saveSnapshot();
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
                    saveSnapshot();
                    // Сообщаем родителю — он переключит tool в 'select'
                    // (паттерн Figma/Excalidraw: после создания → manipulate).
                    onShapeCreatedRef.current?.();
                }
                startPoint = null;
                activeShape = null;
            });

            // Keyboard: Ctrl/Cmd+Z — undo, Ctrl/Cmd+Shift+Z или Ctrl+Y — redo,
            // Delete/Backspace — удалить выделенный объект. Когда IText в editing
            // mode — хоткеи не трогаем (там свой текстовый ввод).
            const onKeyDown = (e: KeyboardEvent) => {
                const fcLocal = fabricRef.current;
                if (!fcLocal) return;
                const active = fcLocal.getActiveObject();
                const editingText = active?.type === 'i-text'
                    && (active as InstanceType<typeof fabric.IText>).isEditing;

                if ((e.ctrlKey || e.metaKey) && !editingText) {
                    const key = e.key.toLowerCase();
                    if (key === 'z' && !e.shiftKey) { e.preventDefault(); undo(); return; }
                    if ((key === 'z' && e.shiftKey) || key === 'y') { e.preventDefault(); redo(); return; }
                }

                if (e.key !== 'Delete' && e.key !== 'Backspace') return;
                if (!active || editingText) return;
                fcLocal.remove(active);
                fcLocal.discardActiveObject();
                fcLocal.requestRenderAll();
                saveSnapshot();
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
            // При создании нового элемента (любой tool кроме select) ИГНОРИРУЕМ
            // существующие объекты как цели клика — иначе клик над/рядом со
            // стрелкой (её bbox шире визуала) выбирал бы её вместо создания нового
            // элемента (например текста над стрелкой). skipTargetFind = «не искать
            // объект под курсором» → клик уходит в наш create-handler.
            fc.skipTargetFind = tool !== 'select';
            // В режиме выбора — hit-testing по ПИКСЕЛЯМ (а не по bounding-box) +
            // небольшой допуск: клик ловит саму линию, а не всю диагональную рамку.
            fc.perPixelTargetFind = true;
            fc.targetFindTolerance = 8;
            // Cursor подсказывает: в select — стрелка, в drawing — крест.
            fc.defaultCursor = tool === 'select' ? 'default' : 'crosshair';
            fc.hoverCursor = tool === 'select' ? 'move' : 'crosshair';
            const brush = fc.freeDrawingBrush;
            if (brush && fabric) {
                brush.color = resolveColor(color);
                brush.width = strokeWidth;
            }

            // UX-бонус: если есть выделенный объект — применить новый color/width
            // к нему «вживую». Так палитра в toolbar работает не только для
            // будущих фигур, но и для текущей (как в Figma/Excalidraw).
            // i-text использует `fill` (заливка), shapes — `stroke` (контур).
            // Контур текста тоже пересобираем под новый цвет.
            const active = fc.getActiveObject();
            if (active && fabric) {
                const c = resolveColor(color);
                if (active.type === 'i-text') {
                    const fontSize = Math.max(14, strokeWidth * 6);
                    const contour = contrastColor(0.95); // контраст к теме, не к краске
                    active.set({
                        fill: c,
                        fontSize,
                        stroke: contour,
                        strokeWidth: Math.max(2, fontSize * 0.08),
                        paintFirst: 'stroke',
                    });
                } else {
                    active.set({ stroke: c, strokeWidth });
                }
                fc.requestRenderAll();
                // Перекраска / смена толщины выделенного объекта — шаг истории.
                // Холостой вызов (сменился только tool, цвет тот же) отсечёт
                // dedup внутри saveSnapshot по равенству снимков.
                saveSnapshot();
            }
        }, [tool, color, strokeWidth, saveSnapshot, fabric]);

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
                undo,
                redo,
                clear: clearAll,
                exportToCanvas: () => {
                    const fc = fabricRef.current;
                    if (!fc) return null;
                    // toCanvasElement(multiplier) экспортирует ВЕСЬ canvas:
                    // background image + все objects (paths, shapes, text).
                    // multiplier=1 — native resolution (background уже DPR-scaled).
                    return fc.toCanvasElement(1) as HTMLCanvasElement;
                },
                getObjectsCount: () => fabricRef.current?.getObjects().length ?? 0,
                // canUndo/canRedo — позиция в snapshot-стеке. idx 0 — пустой
                // стартовый снимок, поэтому undo доступен только при idx > 0.
                canUndo: () => historyIdxRef.current > 0,
                canRedo: () => historyIdxRef.current < historyRef.current.length - 1,
            }),
            [undo, redo, clearAll],
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
