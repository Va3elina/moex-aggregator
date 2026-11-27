/**
 * previewParts — мелкие SVG-детали превью апселла (окно-панель, курсор).
 * Файл экспортирует только компоненты (react-refresh), всё прочее — в previewKit.ts.
 */
import type { CSSProperties, ReactNode } from 'react';
import type { TermPalette } from './previewKit';

/** Общее для всех сцен (кроме Терминала): длительность петли, пауза, reduced-motion. */
const BASE_CSS = `
.fp-sc { display:block; width:100%; height:auto; }
.fp-sc * { animation-duration: 10s; animation-iteration-count: infinite; animation-timing-function: cubic-bezier(.4,0,.2,1); }
.fp-paused .fp-sc * { animation-play-state: paused; }
@media (prefers-reduced-motion: reduce) {
  .fp-sc * { animation: none !important; }
}
`;

/**
 * Холст сцены 480×250: «экран» начинается в (34, 26) и уходит за правый/нижний
 * край. Правый верхний угол (x > 400, y < 56) держим пустым — там крестик окна.
 */
export function Frame({ p, css, className, fill, children }: {
    p: TermPalette; css: string; className?: string; fill?: string; children: ReactNode;
}) {
    return (
        <>
            <style>{BASE_CSS + css}</style>
            <svg className={`fp-sc${className ? ` ${className}` : ''}`} viewBox="0 0 480 250" aria-hidden="true"
                fontFamily="Inter, system-ui, sans-serif">
                <rect x={34} y={26} width={480} height={260} rx={12} fill={fill ?? p.panel} stroke={p.border} />
                {children}
            </svg>
        </>
    );
}

/** Окно-панель Терминала: рамка, заголовок, две «кнопки окна», линия под шапкой. */
export function WindowChrome({ x, y, w, h, title, p }: { x: number; y: number; w: number; h: number; title: string; p: TermPalette }) {
    return (
        <>
            <rect x={x} y={y} width={w} height={h} rx={6} fill={p.panel} stroke={p.border} />
            <text x={x + 9} y={y + 14} fontSize={7.5} fill={p.muted} fontWeight={600}>{title}</text>
            <circle cx={x + w - 18} cy={y + 11} r={2} fill={p.dot} />
            <circle cx={x + w - 10} cy={y + 11} r={2} fill={p.dot} />
            <line x1={x} x2={x + w} y1={y + 21} y2={y + 21} stroke={p.faint} />
        </>
    );
}

/** Курсор-стрелка. Положение задаёт CSS-анимация класса (className). */
export function Cursor({ className, style }: { className: string; style?: CSSProperties }) {
    return (
        <g className={className} style={style}>
            <path d="M0 0 L0 14 L4 10.5 L7 17 L9.5 16 L6.5 9.5 L11.5 9.5 Z" fill="#fff" stroke="#111" strokeWidth={1} strokeLinejoin="round" />
        </g>
    );
}
