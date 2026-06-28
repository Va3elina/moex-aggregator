import { useState, useRef, useEffect, useCallback } from 'react';

// Текст специально без тире и кавычек.
export const DOLLAR_STALE_TEXT =
    'На выходных доллар не обновляется: индекс РТС и курс не торгуются. Рублёвый режим работает и в выходные.';

interface DollarStaleHintProps {
    className?: string;
    style?: React.CSSProperties;
}

/**
 * Кликабельный маркер «доллар не обновляется на выходных» — красный кружок с
 * восклицательным знаком (без тени). По нажатию открывает поповер с пояснением.
 * Позиционируется родителем через style: в углу графика или внахлёст на угол
 * кнопки $. Поповер position:fixed, чтобы не обрезался overflow-hidden графика.
 */
export default function DollarStaleHint({ className, style }: DollarStaleHintProps) {
    const [open, setOpen] = useState(false);
    const [coords, setCoords] = useState<{ top: number; left: number } | null>(null);
    const btnRef = useRef<HTMLButtonElement>(null);

    const close = useCallback(() => setOpen(false), []);

    useEffect(() => {
        if (!open) return;
        const onDown = (e: MouseEvent | TouchEvent) => {
            if (btnRef.current && !btnRef.current.contains(e.target as Node)) close();
        };
        document.addEventListener('mousedown', onDown);
        document.addEventListener('touchstart', onDown);
        window.addEventListener('scroll', close, true);
        window.addEventListener('resize', close);
        return () => {
            document.removeEventListener('mousedown', onDown);
            document.removeEventListener('touchstart', onDown);
            window.removeEventListener('scroll', close, true);
            window.removeEventListener('resize', close);
        };
    }, [open, close]);

    const toggle = useCallback((e: React.MouseEvent) => {
        e.stopPropagation();
        e.preventDefault();
        if (open) { setOpen(false); return; }
        const r = btnRef.current?.getBoundingClientRect();
        if (r) {
            const CARD = 232;
            let left = r.left;
            if (left + CARD > window.innerWidth - 8) left = window.innerWidth - CARD - 8;
            setCoords({ top: r.bottom + 8, left: Math.max(8, left) });
        }
        setOpen(true);
    }, [open]);

    return (
        <>
            <button
                ref={btnRef}
                type="button"
                aria-label="Доллар не обновляется на выходных"
                onClick={toggle}
                className={className}
                style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    cursor: 'pointer',
                    border: 'none',
                    background: 'transparent',
                    padding: 0,
                    lineHeight: 0,
                    ...style,
                }}
            >
                <span
                    style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        width: 22,
                        height: 22,
                        borderRadius: '50%',
                        background: 'var(--funds-flow-negative, #FF4D4D)',
                        color: '#fff',
                        fontWeight: 800,
                        fontSize: 14,
                        lineHeight: 1,
                        border: '1.5px solid var(--text-primary)',
                    }}
                >
                    !
                </span>
            </button>
            {open && coords && (
                <div
                    role="tooltip"
                    onClick={(e) => e.stopPropagation()}
                    style={{
                        position: 'fixed',
                        top: coords.top,
                        left: coords.left,
                        zIndex: 60,
                        width: 232,
                        background: 'var(--bg-primary)',
                        border: '1.5px solid var(--text-primary)',
                        borderRadius: 10,
                        boxShadow: '3px 3px 0 var(--text-primary)',
                        padding: 'var(--sp-2) var(--sp-3)',
                        fontSize: 'var(--fs-xs)',
                        color: 'var(--text-secondary)',
                        lineHeight: 1.45,
                    }}
                >
                    {DOLLAR_STALE_TEXT}
                </div>
            )}
        </>
    );
}
