/**
 * ChartNavigator — полоса выбора временного диапазона.
 * line-режим: тонкий рельс с заполненным выбранным отрезком и круглыми ручками,
 * без окантовки, компактной высоты, прижат к датам графика.
 * histogram-режим: приглушённые мини-бары притоков/оттоков (для FlowsHistogram).
 * Аналог Highcharts / TradingView navigator.
 */
import { useRef, useState, useEffect, useLayoutEffect, useMemo, useCallback } from 'react';

interface ChartNavigatorProps {
    data: { time: string; value: number }[];
    onChange: (startIdx: number, endIdx: number, isDrag: boolean) => void;
    color?: string;
    height?: number;
    /** Показывать мини-preview данных внутри. Default true.
        false — рисуем только маску выделения и ручки (пустая тень). */
    showPreview?: boolean;
    /** Тип мини-preview. 'line' (default) — рельс с круглыми ручками для линейных
        графиков. 'histogram' — приглушённые серые мини-бары от нулевой базовой линии
        (приток вверх / отток вниз). Для FlowsHistogram (притоки/оттоки). */
    previewMode?: 'line' | 'histogram';
    /** Цвет мини-баров в histogram-режиме. Default — тот же серый, что у тени
        линейных графиков (заливка nav-mini-area в editorial: text-primary 22%).
        Приток и отток одним цветом. */
    histogramColor?: string;
    /** Отступ слева — сужает навигатор. CSS-length (число px или строка).
        Default 0 — навигатор во всю ширину контейнера (= ширине SVG графика
        «от края до края», как просил коллега). */
    insetLeft?: number | string;
    /** Отступ справа — аналогично insetLeft. Default 0. */
    insetRight?: number | string;
}

/** number → 'Npx', string остаётся как есть (поддержка var(--xxx)). */
function cssLen(v: number | string): string {
    return typeof v === 'number' ? `${v}px` : v;
}

const HANDLE_W = 14;
const KNOB_D = 20;       // диаметр видимого кружка-ручки (line-режим)
const KNOB_HIT = 28;     // ширина прозрачной зоны захвата вокруг кружка
const RAIL_BOX_H = 20;   // высота полосы = диаметр кружка (line-режим, без окантовки)
const MIN_WIN_FRAC = 0.01; // минимум 1% данных в окне

// Последняя измеренная ширина — кэшируется между маунтами.
// При переключении viewMode (AUM ↔ Flows) ChartNavigator перемонтируется, и без кэша
// стартует с width=0 что вызывает видимое «дёргание» правого края при появлении.
// С кэшем второй+ маунт получает сразу правильное значение → нет glitch'а.
let lastKnownWidth = 0;

export default function ChartNavigator({
    data,
    onChange,
    color = 'var(--accent)',
    height = 52,
    showPreview = true,
    previewMode = 'line',
    histogramColor = 'color-mix(in srgb, var(--text-primary) 22%, transparent)',
    insetLeft = 0,
    insetRight = 0,
}: ChartNavigatorProps) {
    const containerRef = useRef<HTMLDivElement>(null);
    // innerRef — измеряем ширину ВНУТРЕННЕЙ области (между inset'ами), а не
    // контейнера. Иначе drag-математика (deltaX / width) рассинхронится с
    // курсором когда insetLeft/insetRight сужают навигатор.
    const innerRef = useRef<HTMLDivElement>(null);
    const [width, setWidth] = useState(lastKnownWidth);
    const ilCss = cssLen(insetLeft);
    const irCss = cssLen(insetRight);
    const [selFrac, setSelFrac] = useState<[number, number]>([0, 1]);

    // true только во время реального перетаскивания пользователем
    const isDraggingRef = useRef(false);

    // Синхронная инициализация ширины до первой отрисовки — иначе хендлы
    // позиционируются в 0 и некликабельны до срабатывания ResizeObserver.
    // Мерим innerRef (область между inset'ами) — это реальная plot-ширина.
    useLayoutEffect(() => {
        if (innerRef.current) {
            const w = innerRef.current.clientWidth;
            if (w > 0) {
                setWidth(w);
                lastKnownWidth = w;  // кэшируем для последующих маунтов
            }
        }
    }, []);

    // Наблюдаем за шириной inner-области при изменениях размера
    useEffect(() => {
        const el = innerRef.current;
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

    // viewBox фикс. ширины 1000 + preserveAspectRatio="none" → браузер сам растягивает
    // контент до реальной ширины контейнера, без зависимости от измеренной JS-ширины.
    // Это убирает glitch "удлинения" мини-графика при монтировании.
    const VB_WIDTH = 1000;

    // Мини-гистограмма: приглушённые столбики от нулевой базовой линии
    // (приток вверх / отток вниз). Тот же viewBox=1000.
    // Высота бара масштабируется по max|value| относительно центральной оси.
    const miniHist = useMemo(() => {
        if (data.length < 1) return null;
        const vals = data.map(d => d.value);
        const maxAbs = Math.max(1e-9, ...vals.map(v => Math.abs(v)));
        const pt = 5, pb = 5;
        const h = height - pt - pb;
        const mid = pt + h / 2;
        const half = h / 2;
        // Центр бара — point-маппинг i/(n-1)*W, тот же, что у маппинга ручки в индекс
        // (round(frac*(n-1)) в onChange). band-метод (i+0.5)/n сдвигал бар на полслота,
        // из-за чего «бар под ручкой» не совпадал с реально выбираемым индексом.
        const span = Math.max(data.length - 1, 1);
        const bw = Math.max(0.6, (VB_WIDTH / data.length) * 0.6);

        const bars = vals.map((v, i) => {
            const cx = (i / span) * VB_WIDTH;
            const bh = Math.max(0.4, (Math.abs(v) / maxAbs) * half);
            return { x: cx - bw / 2, y: v >= 0 ? mid - bh : mid, h: bh };
        });

        return { bars, bw, mid };
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

    // line-режим с превью → рельс + круглые ручки (без окантовки, компактно).
    // Иначе (histogram / пустой) — старый вид с масками и прямоугольными хендлами.
    const isRail = showPreview && previewMode === 'line';

    // ‹ › — двойной шеврон внутри кружка, намёк «тяни по горизонтали».
    const knobChevrons = (
        <span style={{ width: KNOB_D, height: KNOB_D, borderRadius: '50%', background: color, border: '2px solid var(--bg-elevated)', display: 'flex', alignItems: 'center', justifyContent: 'center', boxSizing: 'border-box' }}>
            {/* Квадратный чётный viewBox (12×12) → целые отступы в 16px-области кружка,
                без пол-пиксельного смещения. Шевроны зеркальны относительно x=6 и
                центрированы по y=6. */}
            <svg width="12" height="12" viewBox="0 0 12 12" style={{ display: 'block', pointerEvents: 'none' }}>
                <path d="M4.5 3.5 L2 6 L4.5 8.5" fill="none" stroke="var(--text-inverse)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                <path d="M7.5 3.5 L10 6 L7.5 8.5" fill="none" stroke="var(--text-inverse)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
        </span>
    );

    return (
        <div
            ref={containerRef}
            className={`chart-navigator relative select-none overflow-visible${isRail ? ' chart-navigator--bare' : ' mt-3'}`}
            // inset'ы применяются как margin КОНТЕЙНЕРА — навигатор ровно по plot-area.
            // line-режим: высота = кружок, без окантовки; вертикальные отступы (прижать
            // к датам + симметрия снизу) заданы в CSS .chart-navigator--bare через calc.
            style={{ height: isRail ? RAIL_BOX_H : height + 4, marginLeft: ilCss, marginRight: irCss }}
        >
            {/* SVG нужен только для histogram-превью (мини-бары). В line-режиме
                рельс/заливка/ручки рисуются HTML-див'ами ниже. viewBox + preserveAspectRatio
                растягивают бары до реальной ширины без зависимости от JS-width. */}
            {previewMode === 'histogram' && (
                <svg
                    viewBox={`0 0 ${VB_WIDTH} ${height}`}
                    preserveAspectRatio="none"
                    className="nav-mini-svg block overflow-visible"
                    style={{ position: 'absolute', top: 0, left: 8, right: 8, width: 'calc(100% - 16px)', height: height }}
                >
                    {showPreview && miniHist && (
                        <>
                            {/* Нулевая базовая линия — едва заметная, помогает читать вверх/вниз */}
                            <line x1="0" y1={miniHist.mid} x2={VB_WIDTH} y2={miniHist.mid}
                                stroke={histogramColor} strokeWidth="1" opacity="0.5" vectorEffect="non-scaling-stroke" />
                            {/* Серые мини-бары: приток вверх / отток вниз.
                                Прозрачность зашита в histogramColor (color-mix). */}
                            {miniHist.bars.map((b, i) => (
                                <rect key={i} className="nav-mini-bar" x={b.x.toFixed(1)} y={b.y.toFixed(1)}
                                    width={miniHist.bw.toFixed(1)} height={b.h.toFixed(1)}
                                    fill={histogramColor} />
                            ))}
                        </>
                    )}
                </svg>
            )}

            {/* Selection и handles как HTML div с CSS %, не зависят от JS-width.
                innerRef → drag-математика мерит реальную ширину (= суженный контейнер). */}
            <div ref={innerRef} className="nav-inner absolute" style={{ top: 0, left: 8, right: 8, bottom: 4, pointerEvents: 'none' }}>
                {isRail ? (
                    <>
                        {/* Рельс-дорожка — тонкая линия по центру во всю ширину */}
                        <div className="nav-rail absolute" style={{ left: 0, right: 0, top: '50%', height: 4, transform: 'translateY(-50%)', borderRadius: 2, background: 'color-mix(in srgb, var(--text-primary) 14%, transparent)' }} />
                        {/* Заполненный выбранный отрезок между ручками */}
                        <div className="nav-rail-fill absolute" style={{ left: `${selFrac[0] * 100}%`, width: `${(selFrac[1] - selFrac[0]) * 100}%`, top: '50%', height: 4, transform: 'translateY(-50%)', borderRadius: 2, background: color }} />
                        {/* Прозрачная зона перетаскивания всего окна */}
                        <div className="nav-window-grab absolute top-0 bottom-0"
                            style={{ left: `${selFrac[0] * 100}%`, width: `${(selFrac[1] - selFrac[0]) * 100}%`, cursor: 'grab', pointerEvents: 'auto' }}
                            onMouseDown={e => startDrag(e, 'window')}
                            onTouchStart={e => startTouchDrag(e, 'window')}
                        />
                        {/* Левая круглая ручка. Колонка-хитзона на всю высоту (легко попасть,
                            особенно на тач), видимый кружок KNOB_D по центру. translateX(-50%)
                            центрирует кружок ровно на крайней точке окна (overflow:visible). */}
                        <div className="nav-knob nav-knob-left absolute flex items-center justify-center"
                            style={{ left: `${selFrac[0] * 100}%`, top: 0, bottom: 0, transform: 'translateX(-50%)', width: KNOB_HIT, cursor: 'ew-resize', pointerEvents: 'auto' }}
                            onMouseDown={e => startDrag(e, 'left')}
                            onTouchStart={e => startTouchDrag(e, 'left')}
                        >
                            {knobChevrons}
                        </div>
                        {/* Правая круглая ручка */}
                        <div className="nav-knob nav-knob-right absolute flex items-center justify-center"
                            style={{ left: `${selFrac[1] * 100}%`, top: 0, bottom: 0, transform: 'translateX(-50%)', width: KNOB_HIT, cursor: 'ew-resize', pointerEvents: 'auto' }}
                            onMouseDown={e => startDrag(e, 'right')}
                            onTouchStart={e => startTouchDrag(e, 'right')}
                        >
                            {knobChevrons}
                        </div>
                    </>
                ) : (
                    <>
                        {/* Маски невыбранной области — theme-aware через --nav-mask */}
                        <div className="absolute top-0 bottom-0 left-0" style={{ width: `${selFrac[0] * 100}%`, background: 'var(--nav-mask, rgba(0,0,0,0.5))' }} />
                        <div className="absolute top-0 bottom-0 right-0" style={{ width: `${(1 - selFrac[1]) * 100}%`, background: 'var(--nav-mask, rgba(0,0,0,0.5))' }} />
                        {/* Выбранное окно — accent цвет с прозрачностью + accent border */}
                        <div className="nav-selection absolute top-0 bottom-0"
                            style={{
                                left: `${selFrac[0] * 100}%`,
                                width: `${(selFrac[1] - selFrac[0]) * 100}%`,
                                background: 'color-mix(in srgb, var(--accent) 8%, transparent)',
                                borderTop: '1px solid color-mix(in srgb, var(--accent) 50%, transparent)',
                                borderBottom: '1px solid color-mix(in srgb, var(--accent) 50%, transparent)',
                                cursor: 'grab',
                                pointerEvents: 'auto',
                            }}
                            onMouseDown={e => startDrag(e, 'window')}
                            onTouchStart={e => startTouchDrag(e, 'window')}
                        />
                        {/* Левая ручка — accent цвет. Adaptive translateX держит handle в контейнере. */}
                        <div className="nav-handle nav-handle-left absolute flex items-center justify-center"
                            style={{
                                left: `${selFrac[0] * 100}%`,
                                top: 0, bottom: 0,
                                transform: `translateX(-${selFrac[0] * 100}%)`,
                                width: HANDLE_W,
                                background: 'var(--accent)',
                                borderRadius: 3,
                                cursor: 'ew-resize',
                                pointerEvents: 'auto',
                            }}
                            onMouseDown={e => startDrag(e, 'left')}
                            onTouchStart={e => startTouchDrag(e, 'left')}
                        >
                            <svg width="6" height="10" viewBox="0 0 6 10" style={{ pointerEvents: 'none' }}>
                                <path d="M4 1 L1 5 L4 9" fill="none" stroke="var(--text-inverse)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                            </svg>
                        </div>
                        {/* Правая ручка */}
                        <div className="nav-handle nav-handle-right absolute flex items-center justify-center"
                            style={{
                                left: `${selFrac[1] * 100}%`,
                                top: 0, bottom: 0,
                                transform: `translateX(-${selFrac[1] * 100}%)`,
                                width: HANDLE_W,
                                background: 'var(--accent)',
                                borderRadius: 3,
                                cursor: 'ew-resize',
                                pointerEvents: 'auto',
                            }}
                            onMouseDown={e => startDrag(e, 'right')}
                            onTouchStart={e => startTouchDrag(e, 'right')}
                        >
                            <svg width="6" height="10" viewBox="0 0 6 10" style={{ pointerEvents: 'none' }}>
                                <path d="M2 1 L5 5 L2 9" fill="none" stroke="var(--text-inverse)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                            </svg>
                        </div>
                    </>
                )}
            </div>
        </div>
    );
}
