/**
 * ChartNavigator — полоса выбора временного диапазона.
 * Показывает мини-график всех данных с перетаскиваемым окном выбора.
 * Аналог Highcharts / TradingView navigator.
 */
import { useRef, useState, useEffect, useLayoutEffect, useMemo, useCallback } from 'react';

interface ChartNavigatorProps {
    data: { time: string; value: number }[];
    onChange: (startIdx: number, endIdx: number, isDrag: boolean) => void;
    color?: string;
    height?: number;
}

const HANDLE_W = 14;
const MIN_WIN_FRAC = 0.01; // минимум 1% данных в окне

// Последняя измеренная ширина — кэшируется между маунтами.
// При переключении viewMode (AUM ↔ Flows) ChartNavigator перемонтируется, и без кэша
// стартует с width=0 что вызывает видимое «дёргание» правого края при появлении.
// С кэшем второй+ маунт получает сразу правильное значение → нет glitch'а.
let lastKnownWidth = 0;

export default function ChartNavigator({
    data,
    onChange,
    color = '#6366f1',
    height = 52,
}: ChartNavigatorProps) {
    const containerRef = useRef<HTMLDivElement>(null);
    const [width, setWidth] = useState(lastKnownWidth);
    const [selFrac, setSelFrac] = useState<[number, number]>([0, 1]);

    // true только во время реального перетаскивания пользователем
    const isDraggingRef = useRef(false);

    // Уникальный ID для градиента (несколько навигаторов на одной странице)
    const gradId = useRef(`ng-${Math.random().toString(36).slice(2, 6)}`).current;

    // Синхронная инициализация ширины до первой отрисовки — иначе хендлы
    // позиционируются в 0 и некликабельны до срабатывания ResizeObserver.
    useLayoutEffect(() => {
        if (containerRef.current) {
            const w = containerRef.current.clientWidth;
            if (w > 0) {
                setWidth(w);
                lastKnownWidth = w;  // кэшируем для последующих маунтов
            }
        }
    }, []);

    // Наблюдаем за шириной контейнера при изменениях размера
    useEffect(() => {
        const el = containerRef.current;
        if (!el) return;
        const ro = new ResizeObserver(entries => {
            const w = entries[0].contentRect.width;
            if (w > 0) {
                setWidth(w);
                lastKnownWidth = w;  // обновляем кэш
            }
        });
        ro.observe(el);
        return () => ro.disconnect();
    }, []);

    // Сброс выделения при смене данных (новый период/инструмент)
    useEffect(() => {
        setSelFrac([0, 1]);
    }, [data]);

    // Стабильная ссылка на onChange — не вызывает бесконечный цикл
    const onChangeRef = useRef(onChange);
    onChangeRef.current = onChange;

    // Сообщаем родителю при изменении выделения.
    // isDraggingRef.current = true только когда пользователь активно тащит ручку.
    // При смене data.length (новый таймфрейм) isDraggingRef = false → родитель не
    // сбрасывает navDragRef в true → морфинг анимируется корректно.
    useEffect(() => {
        if (!data.length || width === 0) return;
        const s = Math.max(0, Math.round(selFrac[0] * (data.length - 1)));
        const e = Math.min(data.length - 1, Math.round(selFrac[1] * (data.length - 1)));
        onChangeRef.current(s, e, isDraggingRef.current);
    }, [selFrac, data.length, width]);

    // Мини-график всех данных — использует фикс. ширину viewBox=1000.
    // SVG рендерится с viewBox + preserveAspectRatio="none" → браузер сам растягивает
    // path до реальной ширины контейнера, без зависимости от измеренной JS-ширины.
    // Это убирает glitch "удлинения" мини-графика при монтировании.
    const VB_WIDTH = 1000;
    const miniPath = useMemo(() => {
        if (!data.length) return null;  // width больше не нужен — используем viewBox
        const vals = data.map(d => d.value);
        const minV = Math.min(...vals);
        const maxV = Math.max(...vals);
        const range = maxV - minV || 1;
        const pt = 5, pb = 5;
        const h = height - pt - pb;

        const pts = data.map((d, i) => ({
            x: (i / Math.max(data.length - 1, 1)) * VB_WIDTH,
            y: pt + h - ((d.value - minV) / range) * h,
        }));

        const line = pts.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x.toFixed(1)} ${p.y.toFixed(1)}`).join(' ');
        const area = `${line} L ${pts[pts.length - 1].x.toFixed(1)} ${height} L 0 ${height} Z`;
        return { line, area };
    }, [data, height]);

    // Перетаскивание
    const startDrag = useCallback((
        e: React.MouseEvent,
        type: 'left' | 'right' | 'window'
    ) => {
        e.preventDefault();
        e.stopPropagation();
        if (width === 0) return;

        isDraggingRef.current = true;
        const startX = e.clientX;
        const startFrac: [number, number] = [selFrac[0], selFrac[1]];

        const onMove = (me: MouseEvent) => {
            const df = (me.clientX - startX) / width;
            const [s, en] = startFrac;
            const winSize = en - s;

            if (type === 'left') {
                const newS = Math.max(0, Math.min(s + df, en - MIN_WIN_FRAC));
                setSelFrac([newS, en]);
            } else if (type === 'right') {
                const newE = Math.min(1, Math.max(en + df, s + MIN_WIN_FRAC));
                setSelFrac([s, newE]);
            } else {
                const ns = Math.max(0, Math.min(s + df, 1 - winSize));
                setSelFrac([ns, ns + winSize]);
            }
        };

        const onUp = () => {
            isDraggingRef.current = false;
            // capture: true — должен совпадать с тем, как listener был добавлен
            window.removeEventListener('mousemove', onMove, true);
            window.removeEventListener('mouseup', onUp);
        };

        // capture: true — срабатывает ДО React-обработчиков (до их stopPropagation)
        window.addEventListener('mousemove', onMove, true);
        window.addEventListener('mouseup', onUp);
    }, [selFrac, width]);

    // Touch-поддержка для мобильных
    const startTouchDrag = useCallback((
        e: React.TouchEvent,
        type: 'left' | 'right' | 'window'
    ) => {
        if (width === 0 || !e.touches.length) return;
        isDraggingRef.current = true;
        const startX = e.touches[0].clientX;
        const startFrac: [number, number] = [selFrac[0], selFrac[1]];

        const onMove = (te: TouchEvent) => {
            if (!te.touches.length) return;
            const df = (te.touches[0].clientX - startX) / width;
            const [s, en] = startFrac;
            const winSize = en - s;

            if (type === 'left') {
                setSelFrac([Math.max(0, Math.min(s + df, en - MIN_WIN_FRAC)), en]);
            } else if (type === 'right') {
                setSelFrac([s, Math.min(1, Math.max(en + df, s + MIN_WIN_FRAC))]);
            } else {
                const ns = Math.max(0, Math.min(s + df, 1 - winSize));
                setSelFrac([ns, ns + winSize]);
            }
        };

        const onEnd = () => {
            isDraggingRef.current = false;
            window.removeEventListener('touchmove', onMove);
            window.removeEventListener('touchend', onEnd);
        };

        window.addEventListener('touchmove', onMove, { passive: false });
        window.addEventListener('touchend', onEnd);
    }, [selFrac, width]);

    // width больше не нужен для рендера (viewBox + CSS %), оставлен для drag калькуляций

    return (
        <div
            ref={containerRef}
            className="relative select-none mt-3 overflow-visible"
            style={{ height: height + 4, paddingLeft: 8, paddingRight: 8 }}
        >
            {/* SVG для мини-графика — viewBox с фикс. шириной 1000, preserveAspectRatio="none"
                позволяет браузеру растянуть path до реальной ширины БЕЗ зависимости от JS-width.
                Мини-график появляется сразу в правильных пропорциях, не «удлиняется» при монтировании. */}
            <svg
                viewBox={`0 0 ${VB_WIDTH} ${height}`}
                preserveAspectRatio="none"
                className="block overflow-visible"
                style={{ position: 'absolute', top: 0, left: 8, right: 8, width: 'calc(100% - 16px)', height: height }}
            >
                <defs>
                    <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor={color} stopOpacity="0.35" />
                        <stop offset="100%" stopColor={color} stopOpacity="0.03" />
                    </linearGradient>
                </defs>
                {miniPath && (
                    <>
                        <path d={miniPath.area} fill={`url(#${gradId})`} />
                        <path d={miniPath.line} fill="none" stroke={color} strokeWidth="1" opacity="0.5" vectorEffect="non-scaling-stroke" />
                    </>
                )}
            </svg>

            {/* Selection и handles как HTML div с CSS %, не зависят от JS-width.
                Это устраняет glitch "расширения правого края" при монтировании — CSS % резолвится браузером сразу. */}
            <div className="absolute" style={{ top: 0, left: 8, right: 8, bottom: 4, pointerEvents: 'none' }}>
                {/* Левая маска (затемнение невыбранной левой области) */}
                <div className="absolute top-0 bottom-0 left-0" style={{ width: `${selFrac[0] * 100}%`, background: 'rgba(0,0,0,0.5)' }} />
                {/* Правая маска */}
                <div className="absolute top-0 bottom-0 right-0" style={{ width: `${(1 - selFrac[1]) * 100}%`, background: 'rgba(0,0,0,0.5)' }} />
                {/* Выбранное окно */}
                <div className="absolute top-0 bottom-0"
                    style={{
                        left: `${selFrac[0] * 100}%`,
                        width: `${(selFrac[1] - selFrac[0]) * 100}%`,
                        background: 'rgba(56,98,251,0.08)',
                        borderTop: '1px solid rgba(56,98,251,0.45)',
                        borderBottom: '1px solid rgba(56,98,251,0.45)',
                        cursor: 'grab',
                        pointerEvents: 'auto',
                    }}
                    onMouseDown={e => startDrag(e, 'window')}
                    onTouchStart={e => startTouchDrag(e, 'window')}
                />
                {/* Левая ручка */}
                <div className="absolute top-1/2 -translate-y-1/2 -translate-x-1/2 flex items-center justify-center"
                    style={{
                        left: `${selFrac[0] * 100}%`,
                        width: HANDLE_W, height: height * 0.7,
                        background: 'rgba(56,98,251,0.9)',
                        borderRadius: 3,
                        cursor: 'ew-resize',
                        pointerEvents: 'auto',
                    }}
                    onMouseDown={e => startDrag(e, 'left')}
                    onTouchStart={e => startTouchDrag(e, 'left')}
                >
                    <svg width="6" height="10" viewBox="0 0 6 10" style={{ pointerEvents: 'none' }}>
                        <path d="M4 1 L1 5 L4 9" fill="none" stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                </div>
                {/* Правая ручка */}
                <div className="absolute top-1/2 -translate-y-1/2 -translate-x-1/2 flex items-center justify-center"
                    style={{
                        left: `${selFrac[1] * 100}%`,
                        width: HANDLE_W, height: height * 0.7,
                        background: 'rgba(56,98,251,0.9)',
                        borderRadius: 3,
                        cursor: 'ew-resize',
                        pointerEvents: 'auto',
                    }}
                    onMouseDown={e => startDrag(e, 'right')}
                    onTouchStart={e => startTouchDrag(e, 'right')}
                >
                    <svg width="6" height="10" viewBox="0 0 6 10" style={{ pointerEvents: 'none' }}>
                        <path d="M2 1 L5 5 L2 9" fill="none" stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                </div>
            </div>
        </div>
    );
}
