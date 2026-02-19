/**
 * ChartNavigator — полоса выбора временного диапазона.
 * Показывает мини-график всех данных с перетаскиваемым окном выбора.
 * Аналог Highcharts / TradingView navigator.
 */
import { useRef, useState, useEffect, useMemo, useCallback } from 'react';

interface ChartNavigatorProps {
    data: { time: string; value: number }[];
    onChange: (startIdx: number, endIdx: number) => void;
    color?: string;
    height?: number;
}

const HANDLE_W = 8;
const MIN_WIN_FRAC = 0.01; // минимум 1% данных в окне

export default function ChartNavigator({
    data,
    onChange,
    color = '#6366f1',
    height = 52,
}: ChartNavigatorProps) {
    const containerRef = useRef<HTMLDivElement>(null);
    const [width, setWidth] = useState(0);
    const [selFrac, setSelFrac] = useState<[number, number]>([0, 1]);

    // Уникальный ID для градиента (несколько навигаторов на одной странице)
    const gradId = useRef(`ng-${Math.random().toString(36).slice(2, 6)}`).current;

    // Наблюдаем за шириной контейнера
    useEffect(() => {
        const el = containerRef.current;
        if (!el) return;
        const ro = new ResizeObserver(entries => {
            const w = entries[0].contentRect.width;
            if (w > 0) setWidth(w);
        });
        ro.observe(el);
        return () => ro.disconnect();
    }, []);

    // Сообщаем родителю при изменении выделения
    useEffect(() => {
        if (!data.length || width === 0) return;
        const s = Math.max(0, Math.round(selFrac[0] * (data.length - 1)));
        const e = Math.min(data.length - 1, Math.round(selFrac[1] * (data.length - 1)));
        onChange(s, e);
    }, [selFrac, data.length, width, onChange]);

    // Мини-график всех данных
    const miniPath = useMemo(() => {
        if (!data.length || width <= 0) return null;
        const vals = data.map(d => d.value);
        const minV = Math.min(...vals);
        const maxV = Math.max(...vals);
        const range = maxV - minV || 1;
        const pt = 5, pb = 5;
        const h = height - pt - pb;

        const pts = data.map((d, i) => ({
            x: (i / Math.max(data.length - 1, 1)) * width,
            y: pt + h - ((d.value - minV) / range) * h,
        }));

        const line = pts.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x.toFixed(1)} ${p.y.toFixed(1)}`).join(' ');
        const area = `${line} L ${pts[pts.length - 1].x.toFixed(1)} ${height} L 0 ${height} Z`;
        return { line, area };
    }, [data, width, height]);

    // Перетаскивание
    const startDrag = useCallback((
        e: React.MouseEvent,
        type: 'left' | 'right' | 'window'
    ) => {
        e.preventDefault();
        e.stopPropagation();
        if (width === 0) return;

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
            window.removeEventListener('mousemove', onMove);
            window.removeEventListener('mouseup', onUp);
        };

        window.addEventListener('mousemove', onMove);
        window.addEventListener('mouseup', onUp);
    }, [selFrac, width]);

    // Touch-поддержка для мобильных
    const startTouchDrag = useCallback((
        e: React.TouchEvent,
        type: 'left' | 'right' | 'window'
    ) => {
        if (width === 0 || !e.touches.length) return;
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
            window.removeEventListener('touchmove', onMove);
            window.removeEventListener('touchend', onEnd);
        };

        window.addEventListener('touchmove', onMove, { passive: false });
        window.addEventListener('touchend', onEnd);
    }, [selFrac, width]);

    const leftPx = selFrac[0] * width;
    const rightPx = selFrac[1] * width;

    return (
        <div
            ref={containerRef}
            className="relative select-none mt-3 border-t border-theme pt-1"
            style={{ height: height + 4 }}
        >
            <svg width="100%" height={height} className="block overflow-visible">
                <defs>
                    <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor={color} stopOpacity="0.35" />
                        <stop offset="100%" stopColor={color} stopOpacity="0.03" />
                    </linearGradient>
                </defs>

                {/* Мини-график */}
                {miniPath && (
                    <>
                        <path d={miniPath.area} fill={`url(#${gradId})`} />
                        <path d={miniPath.line} fill="none" stroke={color} strokeWidth="1" opacity="0.5" />
                    </>
                )}

                {/* Маски невыбранных областей */}
                <rect x={0} y={0} width={Math.max(0, leftPx)} height={height}
                    fill="rgba(0,0,0,0.5)" style={{ pointerEvents: 'none' }} />
                <rect x={rightPx} y={0} width={Math.max(0, width - rightPx)} height={height}
                    fill="rgba(0,0,0,0.5)" style={{ pointerEvents: 'none' }} />

                {/* Выбранное окно */}
                <rect
                    x={leftPx} y={0}
                    width={Math.max(0, rightPx - leftPx)}
                    height={height}
                    fill="rgba(56,98,251,0.08)"
                    stroke="rgba(56,98,251,0.45)"
                    strokeWidth="1"
                    style={{ cursor: 'grab' }}
                    onMouseDown={e => startDrag(e, 'window')}
                    onTouchStart={e => startTouchDrag(e, 'window')}
                />

                {/* Левая ручка */}
                <rect
                    x={leftPx - HANDLE_W / 2} y={height * 0.15}
                    width={HANDLE_W} height={height * 0.7}
                    rx={3} fill="rgba(56,98,251,0.9)"
                    style={{ cursor: 'ew-resize' }}
                    onMouseDown={e => startDrag(e, 'left')}
                    onTouchStart={e => startTouchDrag(e, 'left')}
                />

                {/* Правая ручка */}
                <rect
                    x={rightPx - HANDLE_W / 2} y={height * 0.15}
                    width={HANDLE_W} height={height * 0.7}
                    rx={3} fill="rgba(56,98,251,0.9)"
                    style={{ cursor: 'ew-resize' }}
                    onMouseDown={e => startDrag(e, 'right')}
                    onTouchStart={e => startTouchDrag(e, 'right')}
                />
            </svg>
        </div>
    );
}
