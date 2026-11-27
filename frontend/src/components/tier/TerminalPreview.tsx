/**
 * TerminalPreview — анимированное превью Терминала для окна апселла
 * (реестр и смысл — в featurePreviews.ts). Тема — как у самого Терминала.
 */
import type { CSSProperties } from 'react';
import { DOWN, UP, arc, usePreviewPalette, type PreviewVisualProps } from './previewKit';
import { WindowChrome } from './previewParts';


/* ── Терминал ─────────────────────────────────────────────────────────────
 * Сюжет петли (12 с):
 *   Лист 1 — два окна живут (линия ОИ рисуется, бары фондов подрастают) →
 *   курсор берёт третье окно (тепловая карта) → тащит, магнит подсвечивает
 *   слот → окно встаёт.
 *   Лист 2 — курсор щёлкает по вкладке «Лист 2», стол меняется на другой набор
 *   (сезонность, сила рынка, скринер сигналов) → в конце петли тихо обратно.
 * Палитра своя, тёмная — как у самого Терминала (sandbox.css), не зависит от
 * темы сайта. Сброс анимаций листа 1 (линия/окно/бары) делается в 90–92%, пока
 * лист 1 скрыт, чтобы при возврате ничего не «прыгало».
 */
const TERMINAL_CSS = `
.fp-term { display:block; width:100%; height:auto; }
.fp-term * { animation-duration: 12s; animation-iteration-count: infinite; animation-timing-function: cubic-bezier(.4,0,.2,1); }
.fp-paused .fp-term * { animation-play-state: paused; }
.fp-term .fp-line { stroke-dasharray: 260; stroke-dashoffset: 0; animation-name: fp-draw; animation-timing-function: linear; }
.fp-term .fp-bar { transform-box: fill-box; transform-origin: 50% 100%; animation-name: fp-grow; }
.fp-term .fp-win-c { animation-name: fp-drag; }
.fp-term .fp-cursor { transform: translate(408px, 150px); animation-name: fp-cursor; }
.fp-term .fp-snap { opacity: 0; animation-name: fp-snap; }
.fp-term .fp-cell { animation-name: fp-cell; }
.fp-term .fp-s1 { animation-name: fp-s1; }
.fp-term .fp-s2 { opacity: 0; animation-name: fp-s2; }
.fp-term .fp-tab { animation-name: fp-tab; }
.fp-term .fp-t1 { fill: var(--t-text); animation-name: fp-t1; }
.fp-term .fp-t2 { fill: var(--t-muted); animation-name: fp-t2; }
.fp-term .fp-sbar { transform-box: fill-box; animation-name: fp-sgrow; }
.fp-term .fp-sbar.up { transform-origin: 50% 100%; }
.fp-term .fp-sbar.down { transform-origin: 50% 0%; }
.fp-term .fp-needle { transform-box: view-box; transform-origin: 382px 166px; transform: rotate(38deg); animation-name: fp-needle; }
.fp-term .fp-row { animation-name: fp-row; }
@keyframes fp-draw { 0% { stroke-dashoffset: 260; } 41%,90% { stroke-dashoffset: 0; } 92%,100% { stroke-dashoffset: 260; } }
@keyframes fp-grow { 0%,6% { transform: scaleY(.25); } 22%,90% { transform: scaleY(1); } 92%,100% { transform: scaleY(.25); } }
@keyframes fp-drag {
  0%,8%   { transform: translate(-178px, 22px); opacity: 0; }
  12%,16% { transform: translate(-178px, 22px); opacity: 1; }
  33%,90% { transform: translate(0, 0); opacity: 1; }
  92%,100% { transform: translate(-178px, 22px); opacity: 0; }
}
@keyframes fp-cursor {
  0%      { transform: translate(430px, 250px); opacity: 0; }
  6%      { transform: translate(430px, 250px); opacity: 1; }
  15%,16% { transform: translate(150px, 222px); }
  33%     { transform: translate(328px, 200px); }
  44%,50% { transform: translate(408px, 150px); }
  60%     { transform: translate(140px, 44px) scale(1); }
  62%     { transform: translate(140px, 44px) scale(.82); }
  64%     { transform: translate(140px, 44px) scale(1); }
  74%     { transform: translate(250px, 120px); opacity: 1; }
  88%,100% { transform: translate(250px, 120px); opacity: 0; }
}
@keyframes fp-snap { 0%,19% { opacity: 0; } 24%,31% { opacity: 1; } 37%,100% { opacity: 0; } }
@keyframes fp-cell { 0%,34% { opacity: .35; } 42%,90% { opacity: 1; } 92%,100% { opacity: .35; } }
@keyframes fp-s1 { 0%,63% { opacity: 1; } 66%,96% { opacity: 0; } 100% { opacity: 1; } }
@keyframes fp-s2 { 0%,63% { opacity: 0; } 66%,96% { opacity: 1; } 100% { opacity: 0; } }
@keyframes fp-tab { 0%,62% { transform: translateX(0); } 65%,96% { transform: translateX(54px); } 100% { transform: translateX(0); } }
@keyframes fp-t1 { 0%,62% { fill: var(--t-text); } 65%,96% { fill: var(--t-muted); } 100% { fill: var(--t-text); } }
@keyframes fp-t2 { 0%,62% { fill: var(--t-muted); } 65%,96% { fill: var(--t-text); } 100% { fill: var(--t-muted); } }
@keyframes fp-sgrow { 0%,65% { transform: scaleY(0); } 76%,100% { transform: scaleY(1); } }
@keyframes fp-needle { 0%,66% { transform: rotate(-80deg); } 74% { transform: rotate(52deg); } 79%,100% { transform: rotate(38deg); } }
@keyframes fp-row { 0%,66% { opacity: 0; transform: translateX(-8px); } 74%,100% { opacity: 1; transform: translateX(0); } }
@media (prefers-reduced-motion: reduce) {
  .fp-term * { animation: none !important; }
}
`;


// Тепловая карта окна C: 6×3, зелёный/красный разной насыщенности.
const HEAT = [
    [0.9, -0.4, 0.3, 0.7, -0.8, 0.2],
    [-0.3, 0.6, -0.9, 0.4, 0.1, -0.5],
    [0.5, 0.2, -0.6, -0.2, 0.8, 0.4],
];
const heatFill = (v: number) => (v >= 0 ? `rgba(91,212,156,${0.3 + v * 0.7})` : `rgba(239,111,111,${0.3 - v * 0.7})`);

// Лист 2: сезонность по месяцам (%), строки скринера.
const SEASON = [6, -4, 9, 3, -7, 5, 11, -3, -8, 4, 7, 10];
const SCREENER: Array<[string, number, boolean]> = [
    ['SBER', 0.8, true],
    ['GAZP', 0.55, false],
    ['LKOH', 0.7, true],
];
export default function TerminalPreview({ theme }: PreviewVisualProps) {
    const p = usePreviewPalette(theme);
    const bars = [34, 52, 28, 61, 44, 70, 38, 56];
    return (
        <>
            <style>{TERMINAL_CSS}</style>
            <svg className="fp-term" viewBox="0 0 480 250" aria-hidden="true" fontFamily="Inter, system-ui, sans-serif"
                style={{ '--t-text': p.text, '--t-muted': p.muted } as CSSProperties}>
                {/* Корпус терминала, уходит за правый и нижний край — как витрина. */}
                <rect x={34} y={26} width={480} height={260} rx={12} fill={p.body} stroke={p.border} />
                {/* Шапка: лого-точка + листы. Подложка активной вкладки ездит. */}
                <circle cx={52} cy={42} r={5} fill="var(--accent, #FF5C2B)" />
                <rect className="fp-tab" x={66} y={34} width={50} height={16} rx={4} fill={p.chip} />
                <text className="fp-t1" x={75} y={45} fontSize={7.5} fontWeight={600}>Лист 1</text>
                <text className="fp-t2" x={129} y={45} fontSize={7.5} fontWeight={600}>Лист 2</text>
                <text x={176} y={45} fontSize={9} fill={p.muted}>+</text>
                <line x1={34} x2={480} y1={58} y2={58} stroke={p.faint} />
                {/* Левая рейка с иконками */}
                {[72, 92, 112, 132, 152].map((cy) => (
                    <rect key={cy} x={44} y={cy} width={14} height={14} rx={3} fill={p.chip} />
                ))}

                {/* ── Лист 1 ── */}
                <g className="fp-s1">
                    {/* Окно A — Открытые позиции, линия рисуется */}
                    <WindowChrome x={72} y={68} w={210} h={112} title="Открытые позиции · Si" p={p} />
                    <path
                        className="fp-line"
                        d="M82 160 L100 152 L114 156 L130 138 L146 143 L160 126 L176 131 L192 112 L208 118 L224 102 L240 108 L256 94 L272 98"
                        fill="none" stroke="var(--accent, #FF5C2B)" strokeWidth={2} strokeLinejoin="round" strokeLinecap="round"
                    />
                    <line x1={82} x2={272} y1={170} y2={170} stroke={p.faint} strokeDasharray="2 3" />

                    {/* Окно B — Деньги в фондах, бары подрастают */}
                    <WindowChrome x={292} y={68} w={180} h={112} title="Деньги в фондах" p={p} />
                    {bars.map((h, i) => (
                        <rect key={i} className="fp-bar" style={{ animationDelay: `${i * 0.08}s` }}
                            x={304 + i * 20} y={170 - h} width={12} height={h} rx={2}
                            fill={i === 5 ? 'var(--accent, #FF5C2B)' : p.bar} />
                    ))}

                    {/* Слот магнита — подсвечивается, пока окно тащат */}
                    <rect className="fp-snap" x={292} y={190} width={180} height={80} rx={6}
                        fill="none" stroke="var(--accent, #FF5C2B)" strokeWidth={1.5} strokeDasharray="4 3" />

                    {/* Окно C — Тепловая карта, его перетаскивают */}
                    <g className="fp-win-c">
                        <WindowChrome x={292} y={190} w={180} h={80} title="Тепловая карта" p={p} />
                        {HEAT.map((row, r) => row.map((v, c) => (
                            <rect key={`${r}-${c}`} className="fp-cell" x={300 + c * 28} y={218 + r * 15}
                                width={25} height={12} rx={2} fill={heatFill(v)} />
                        )))}
                    </g>
                </g>

                {/* ── Лист 2: другой набор индикаторов ── */}
                <g className="fp-s2">
                    {/* Сезонность — бары вверх/вниз от нуля */}
                    <WindowChrome x={72} y={68} w={210} h={112} title="Сезонность · IMOEX" p={p} />
                    <line x1={82} x2={272} y1={128} y2={128} stroke={p.bar} />
                    {SEASON.map((v, i) => {
                        const h = Math.abs(v) * 3.4;
                        return (
                            <rect key={i} className={`fp-sbar ${v >= 0 ? 'up' : 'down'}`}
                                style={{ animationDelay: `${i * 0.03}s` }}
                                x={86 + i * 15.5} y={v >= 0 ? 128 - h : 128} width={10} height={h} rx={1.5}
                                fill={v >= 0 ? UP : DOWN} />
                        );
                    })}

                    {/* Сила рынка — полукруглая шкала со стрелкой */}
                    <WindowChrome x={292} y={68} w={180} h={112} title="Сила рынка" p={p} />
                    <path d={arc(382, 166, 52, 180, 120)} stroke={DOWN} strokeWidth={8} fill="none" />
                    <path d={arc(382, 166, 52, 118, 62)} stroke={p.bar} strokeWidth={8} fill="none" />
                    <path d={arc(382, 166, 52, 60, 0)} stroke={UP} strokeWidth={8} fill="none" />
                    <line className="fp-needle" x1={382} y1={166} x2={382} y2={124}
                        stroke={p.text} strokeWidth={2.5} strokeLinecap="round" />
                    <circle cx={382} cy={166} r={4} fill={p.text} />

                    {/* Скринер сигналов — строки выезжают по очереди */}
                    <WindowChrome x={72} y={190} w={400} h={80} title="Скринер сигналов" p={p} />
                    {SCREENER.map(([tk, w, up], i) => (
                        <g key={tk} className="fp-row" style={{ animationDelay: `${i * 0.12}s` }}>
                            <text x={84} y={225 + i * 16} fontSize={7.5} fill={p.text} fontWeight={600}>{tk}</text>
                            <rect x={124} y={219 + i * 16} width={200} height={6} rx={3} fill={p.faint} />
                            <rect x={124} y={219 + i * 16} width={200 * w} height={6} rx={3} fill={up ? UP : DOWN} opacity={0.85} />
                            <rect x={340} y={216 + i * 16} width={56} height={12} rx={6}
                                fill={up ? 'rgba(91,212,156,.2)' : 'rgba(239,111,111,.2)'} />
                            <text x={368} y={225 + i * 16} fontSize={7} textAnchor="middle" fill={up ? UP : DOWN} fontWeight={600}>
                                {up ? 'покупки ↑' : 'продажи ↓'}
                            </text>
                        </g>
                    ))}
                </g>

                {/* Курсор */}
                <g className="fp-cursor">
                    <path d="M0 0 L0 14 L4 10.5 L7 17 L9.5 16 L6.5 9.5 L11.5 9.5 Z" fill="#fff" stroke="#111" strokeWidth={1} strokeLinejoin="round" />
                </g>
            </svg>
        </>
    );
}



