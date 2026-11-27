/**
 * PreviewScene — анимированная «витрина» закрытой функции в окне апселла.
 *
 * Вместо замка показываем сам инструмент: окно индикатора в цветах Терминала,
 * курсор щёлкает по чипу закрытой функции, и график меняется так, как
 * изменится у юзера с тарифом (что именно — см. ChartArch в previewKit.ts).
 * Плюс отдельные сцены: Терминал, сигналы ОИ, уведомления, CSV; сцены фондов и
 * расширения нарисованы по настоящему интерфейсу — в FundsScenes.tsx.
 * Какая сцена для какого замка — решает resolvePreview (featurePreviews.ts).
 *
 * Всё — inline-SVG + CSS-анимация: ноль веса, резко на ретине. Базовые стили =
 * финальный кадр, keyframes ведут к нему, поэтому при prefers-reduced-motion
 * виден готовый результат. Петля 10 с, у Терминала своя (12 с).
 */
import type { CSSProperties } from 'react';
import TerminalPreview from './TerminalPreview';
import { Cursor, Frame } from './previewParts';
import { ExtensionScene, FundTradesScene, FundsPickScene } from './FundsScenes';
import {
    ACCENT, DOWN, SERIES2, UP, chipWidth, linePath, series, usePreviewPalette,
    type PreviewMode, type SceneSpec, type TermPalette,
} from './previewKit';

const SCENE_CSS = `

/* курсор: из угла → к цели (--cx/--cy) → клик → в сторону → гаснет */
.fp-sc .fc-cursor { opacity: 0; animation-name: fc-cursor; }
@keyframes fc-cursor {
  0%      { transform: translate(440px, 240px); opacity: 0; }
  6%      { transform: translate(440px, 240px); opacity: 1; }
  24%     { transform: translate(var(--cx), var(--cy)) scale(1); }
  26.5%   { transform: translate(var(--cx), var(--cy)) scale(.82); }
  29%     { transform: translate(var(--cx), var(--cy)) scale(1); opacity: 1; }
  46%     { transform: translate(400px, 205px); opacity: 1; }
  58%,100% { transform: translate(400px, 205px); opacity: 0; }
}
/* чипы: цель загорается, прежний гаснет */
.fp-sc .fc-on { animation-name: fc-on; }
.fp-sc .fc-off { opacity: 0; animation-name: fc-off; }
@keyframes fc-on  { 0%,28% { opacity: 0; } 31%,92% { opacity: 1; } 96%,100% { opacity: 0; } }
@keyframes fc-off { 0%,28% { opacity: 1; } 31%,92% { opacity: 0; } 96%,100% { opacity: 1; } }
/* график «до» → «после» */
.fp-sc .fc-before { opacity: 0; animation-name: fc-before; }
.fp-sc .fc-after { animation-name: fc-after; }
@keyframes fc-before { 0%,30% { opacity: 1; } 34%,92% { opacity: 0; } 96%,100% { opacity: 1; } }
@keyframes fc-after  { 0%,31% { opacity: 0; } 34%,92% { opacity: 1; } 96%,100% { opacity: 0; } }
.fp-sc .fc-draw { stroke-dasharray: 1; stroke-dashoffset: 0; animation-name: fc-draw; animation-timing-function: linear; }
@keyframes fc-draw { 0%,32% { stroke-dashoffset: 1; } 62%,100% { stroke-dashoffset: 0; } }
.fp-sc .fc-grow { transform-box: fill-box; transform-origin: 50% 100%; animation-name: fc-grow; }
.fp-sc .fc-grow.dn { transform-origin: 50% 0%; }
@keyframes fc-grow { 0%,32% { transform: scaleY(0); } 50%,100% { transform: scaleY(1); } }
.fp-sc .fc-pop { transform-box: fill-box; transform-origin: 50% 50%; animation-name: fc-pop; }
@keyframes fc-pop { 0%,32% { transform: scale(.4); opacity: 0; } 42%,100% { transform: scale(1); opacity: 1; } }
/* без чипов (solo): «до» нет, «после» просто живёт */
.fp-sc.solo .fc-before { display: none; }
.fp-sc.solo .fc-after { animation-name: fc-solo; }
.fp-sc.solo .fc-draw { animation-name: fc-draw-solo; }
.fp-sc.solo .fc-grow { animation-name: fc-grow-solo; }
.fp-sc.solo .fc-pop { animation-name: fc-pop-solo; }
@keyframes fc-solo { 0%,90% { opacity: 1; } 95%,100% { opacity: 0; } }
@keyframes fc-draw-solo { 0%,4% { stroke-dashoffset: 1; } 40%,100% { stroke-dashoffset: 0; } }
@keyframes fc-grow-solo { 0%,4% { transform: scaleY(0); } 24%,100% { transform: scaleY(1); } }
@keyframes fc-pop-solo { 0%,4% { transform: scale(.4); opacity: 0; } 16%,100% { transform: scale(1); opacity: 1; } }

/* скринер: строки приходят по одной, «живой» индикатор мигает */
.fp-sc .fs-row { animation-name: fs-row; }
@keyframes fs-row { 0%,4% { opacity: 0; transform: translateY(-6px); } 10%,90% { opacity: 1; transform: translateY(0); } 95%,100% { opacity: 0; } }
.fp-sc .fs-flash { opacity: 0; animation-name: fs-flash; }
@keyframes fs-flash { 0%,8% { opacity: 0; } 11% { opacity: .5; } 24%,100% { opacity: 0; } }
.fp-sc .fs-live { animation-name: fs-live; animation-duration: 1.4s; }
@keyframes fs-live { 0%,100% { opacity: 1; } 50% { opacity: .25; } }

/* уведомления: линия доходит до уровня → точка → колокольчик → сообщение */
.fp-sc .fa-draw { stroke-dasharray: 1; stroke-dashoffset: 0; animation-name: fa-draw; animation-timing-function: linear; }
@keyframes fa-draw { 0%,4% { stroke-dashoffset: 1; } 42%,100% { stroke-dashoffset: 0; } }
.fp-sc .fa-dot { transform-box: fill-box; transform-origin: 50% 50%; animation-name: fa-dot; }
@keyframes fa-dot { 0%,36% { transform: scale(0); } 40% { transform: scale(1.8); } 46%,92% { transform: scale(1); } 96%,100% { transform: scale(0); } }
.fp-sc .fa-bell { transform-box: fill-box; transform-origin: 50% 10%; animation-name: fa-bell; }
@keyframes fa-bell { 0%,40% { transform: rotate(0); } 42% { transform: rotate(16deg); } 44% { transform: rotate(-14deg); } 46% { transform: rotate(10deg); } 48% { transform: rotate(-6deg); } 50%,100% { transform: rotate(0); } }
.fp-sc .fa-msg { animation-name: fa-msg; }
@keyframes fa-msg { 0%,44% { opacity: 0; transform: translate(40px, 0); } 52%,90% { opacity: 1; transform: translate(0, 0); } 96%,100% { opacity: 0; transform: translate(0, 0); } }

/* CSV: клик по кнопке → файл вылетает и падает в загрузки */
.fp-sc .fcsv-file { animation-name: fcsv-file; }
@keyframes fcsv-file { 0%,29% { opacity: 0; transform: translate(-52px, -100px) scale(.5); } 34% { opacity: 1; } 50%,90% { opacity: 1; transform: translate(0, 0) scale(1); } 96%,100% { opacity: 0; transform: translate(0, 0) scale(1); } }

`;

/* Геометрия: «экран» начинается в (34, 26) и уходит за правый/нижний край.
 * Правый верхний угол (x > 400, y < 56) держим пустым — там крестик закрытия
 * (см. Frame в previewParts.tsx). */
const CX = 50;
const CY = 88;
const CW = 410;
const CH = 128;

function Grid({ p }: { p: TermPalette }) {
    return (
        <>
            {[0, 1, 2, 3, 4].map((k) => (
                <line key={k} x1={CX} x2={CX + CW} y1={CY + (CH * k) / 4} y2={CY + (CH * k) / 4}
                    stroke={p.faint} strokeDasharray={k === 4 ? undefined : '2 3'} />
            ))}
        </>
    );
}

/* ── Графики «до/после» по типу индикатора ────────────────────────────── */

/** Линия: к основной серии добавляется вторая (юрлица, число трейдеров). */
function LineCharts({ seed }: { seed: number }) {
    const base = series(120, seed);
    const second = series(120, seed + 7).map((v) => 0.1 + v * 0.55);
    const a = <path d={linePath(base, CX, CY + 8, CW, CH - 16)} fill="none" stroke={ACCENT} strokeWidth={1.8} strokeLinejoin="round" />;
    return (
        <>
            <g className="fc-before">{a}</g>
            <g className="fc-after">
                {a}
                <path className="fc-draw" pathLength={1} d={linePath(second, CX, CY + 8, CW, CH - 16)} fill="none" stroke={SERIES2} strokeWidth={1.8} strokeLinejoin="round" />
            </g>
        </>
    );
}

function barVals(n: number, seed: number): number[] {
    return series(n, seed, 0.9).map((v, i) => (v - 0.5) * 2 * (0.45 + 0.55 * Math.abs(Math.sin(i * 1.3 + seed))));
}

function Bars({ vals, grow }: { vals: number[]; grow?: boolean }) {
    const zy = CY + CH / 2;
    const step = CW / vals.length;
    const w = step * 0.55;
    return (
        <>
            {vals.map((v, i) => {
                const h = Math.max(2, Math.abs(v) * (CH / 2) * 0.9);
                const up = v >= 0;
                return (
                    <rect key={i} className={grow ? `fc-grow${up ? '' : ' dn'}` : undefined}
                        style={grow ? { animationDelay: `${i * 0.02}s` } : undefined}
                        x={CX + i * step + (step - w) / 2} y={up ? zy - h : zy} width={w} height={h} rx={1.5}
                        fill={up ? UP : DOWN} />
                );
            })}
        </>
    );
}

/**
 * Столбики: те же 12 столбиков пересчитываются — выбросы сглажены (фильтр
 * сезонности: медиана вместо среднего / без дивидендных гэпов).
 */
function BarCharts({ p, seed }: { p: TermPalette; seed: number }) {
    const before = barVals(12, seed).map((v, i) => (i === 3 || i === 8 ? v * 1.9 : v));
    const after = before.map((v) => Math.sign(v) * Math.min(Math.abs(v), 0.45));
    return (
        <>
            <line x1={CX} x2={CX + CW} y1={CY + CH / 2} y2={CY + CH / 2} stroke={p.dot} />
            <g className="fc-before"><Bars vals={before} /></g>
            <g className="fc-after"><Bars vals={after} grow /></g>
        </>
    );
}

/* ── Сцена «индикатор + клик по закрытой функции» ─────────────────────── */

function ChartScene({ spec, p }: { spec: SceneSpec; p: TermPalette }) {
    const chips = spec.chips ?? [];
    const solo = chips.length === 0;
    const seed = spec.seed ?? 3;
    const placed = chips.reduce<Array<{ label: string; x: number; w: number }>>((acc, label) => {
        const prev = acc[acc.length - 1];
        acc.push({ label, x: prev ? prev.x + prev.w + 6 : CX, w: chipWidth(label) });
        return acc;
    }, []);
    const target = placed[placed.length - 1];
    const cursorStyle = target
        ? ({ '--cx': `${target.x + target.w / 2}px`, '--cy': `${58 + 9}px` } as CSSProperties)
        : undefined;
    const chip = (c: { label: string; x: number; w: number }, on: boolean) => (
        <>
            <rect x={c.x} y={58} width={c.w} height={17} rx={8.5} fill={on ? ACCENT : p.chip} />
            <text x={c.x + c.w / 2} y={69.5} fontSize={7.5} fontWeight={600} textAnchor="middle" fill={on ? '#fff' : p.muted}>{c.label}</text>
        </>
    );
    return (
        <Frame p={p} css={SCENE_CSS} className={solo ? 'solo' : undefined}>
            <text x={CX} y={47} fontSize={10} fontWeight={700} fill={p.text}>{spec.header}</text>
            {placed.map((c, i) => (
                <g key={i}>
                    {chip(c, false)}
                    {i === 0 && placed.length > 1 && <g className="fc-off">{chip(c, true)}</g>}
                    {i === placed.length - 1 && <g className="fc-on">{chip(c, true)}</g>}
                </g>
            ))}
            <Grid p={p} />
            {spec.arch === 'bars' && <BarCharts p={p} seed={seed} />}
            {(spec.arch ?? 'line') === 'line' && <LineCharts seed={seed} />}
            {target && <Cursor className="fc-cursor" style={cursorStyle} />}
        </Frame>
    );
}

/* ── Сигналы по открытым позициям (лента в реальном времени) ─────────────────────────────────────────────────── */

const SIGNALS: Array<[string, number, boolean, string]> = [
    ['Si', 0.82, true, '14:32:05'],
    ['BR', 0.64, false, '14:31:48'],
    ['RI', 0.71, true, '14:31:12'],
    ['GD', 0.45, true, '14:30:57'],
    ['NG', 0.58, false, '14:30:21'],
];

function ScreenerScene({ spec, p }: { spec: SceneSpec; p: TermPalette }) {
    return (
        <Frame p={p} css={SCENE_CSS}>
            <text x={CX} y={47} fontSize={10} fontWeight={700} fill={p.text}>{spec.header}</text>
            <circle className="fs-live" cx={CX + 4} cy={66} r={3.5} fill={UP} />
            <text x={CX + 12} y={69} fontSize={7.5} fontWeight={600} fill={p.muted}>в реальном времени</text>
            {['Время', 'Актив', 'Изменение ОИ', 'Сигнал'].map((h, i) => (
                <text key={h} x={[CX, CX + 56, CX + 100, CX + 318][i]} y={92} fontSize={7} fill={p.muted}>{h}</text>
            ))}
            {SIGNALS.map(([tk, w, up, tm], i) => {
                const y = 100 + i * 24;
                return (
                    <g key={tk} className="fs-row" style={{ animationDelay: `${i * 0.5}s` }}>
                        <rect className="fs-flash" style={{ animationDelay: `${i * 0.5}s` }} x={CX - 6} y={y - 4} width={430} height={20} rx={4} fill={ACCENT} />
                        <text x={CX} y={y + 9} fontSize={7.5} fill={p.muted}>{tm}</text>
                        <text x={CX + 56} y={y + 9} fontSize={8} fontWeight={700} fill={p.text}>{tk}</text>
                        <rect x={CX + 100} y={y + 2} width={200} height={7} rx={3.5} fill={p.chip} />
                        <rect x={CX + 100} y={y + 2} width={200 * w} height={7} rx={3.5} fill={up ? UP : DOWN} />
                        <rect x={CX + 318} y={y - 1} width={62} height={13} rx={6.5} fill={up ? 'rgba(91,212,156,.2)' : 'rgba(239,111,111,.2)'} />
                        <text x={CX + 349} y={y + 8} fontSize={7} textAnchor="middle" fontWeight={600} fill={up ? UP : DOWN}>
                            {up ? 'покупки ↑' : 'продажи ↓'}
                        </text>
                    </g>
                );
            })}
        </Frame>
    );
}

/* ── Уведомления ──────────────────────────────────────────────────────── */

function AlertsScene({ spec, p }: { spec: SceneSpec; p: TermPalette }) {
    const vals = series(60, spec.seed ?? 5).map((v, i) => v * 0.75 + (i > 44 ? (i - 44) * 0.016 : 0));
    const level = 0.72;
    const top = CY + 4, h = CH - 8;
    const idx = Math.max(1, vals.findIndex((v, i) => i > 30 && v >= level));
    const px = CX + (CW * idx) / (vals.length - 1);
    const py = top + h - vals[idx] * h;
    const ly = top + h - level * h;
    return (
        <Frame p={p} css={SCENE_CSS}>
            <text x={CX} y={47} fontSize={10} fontWeight={700} fill={p.text}>{spec.header}</text>
            {/* колокольчик */}
            <g className="fa-bell">
                <path d="M372 38 a7 7 0 0 1 7 7 v5 l2.5 3.5 h-19 l2.5 -3.5 v-5 a7 7 0 0 1 7 -7 z" fill={ACCENT} />
                <circle cx={372} cy={55.5} r={2} fill={ACCENT} />
            </g>
            <Grid p={p} />
            <line x1={CX} x2={CX + CW} y1={ly} y2={ly} stroke={ACCENT} strokeDasharray="4 3" />
            <rect x={CX + 4} y={ly - 13} width={44} height={11} rx={3} fill={ACCENT} />
            <text x={CX + 26} y={ly - 5} fontSize={6.5} fontWeight={700} fill="#fff" textAnchor="middle">уровень</text>
            <path className="fa-draw" pathLength={1} d={linePath(vals, CX, top, CW, h)} fill="none" stroke={SERIES2} strokeWidth={1.8} strokeLinejoin="round" />
            <circle className="fa-dot" cx={px} cy={py} r={4} fill={ACCENT} stroke={p.panel} strokeWidth={1.5} />
            {/* сообщение в мессенджере */}
            <g className="fa-msg">
                <rect x={262} y={150} width={206} height={56} rx={10} fill={p.panel} stroke={p.border} />
                <circle cx={282} cy={170} r={10} fill={ACCENT} />
                <text x={282} y={173.5} fontSize={9} fontWeight={800} fill="#fff" textAnchor="middle">F</text>
                <text x={298} y={167} fontSize={8} fontWeight={700} fill={p.text}>Фрейм</text>
                <text x={456} y={167} fontSize={6.5} fill={p.muted} textAnchor="end">сейчас</text>
                <text x={298} y={181} fontSize={7.5} fill={p.text}>Индикатор пересёк ваш уровень</text>
                <text x={298} y={194} fontSize={7} fill={p.muted}>Открыть график →</text>
            </g>
        </Frame>
    );
}

/* ── Экспорт CSV ──────────────────────────────────────────────────────── */

function CsvScene({ spec, p }: { spec: SceneSpec; p: TermPalette }) {
    const rows = [
        ['27.11.2026', '1 240,3', '+1,2%'],
        ['26.11.2026', '1 225,6', '−0,4%'],
        ['25.11.2026', '1 230,5', '+0,8%'],
        ['24.11.2026', '1 220,7', '+0,3%'],
        ['21.11.2026', '1 217,1', '−0,6%'],
    ];
    const btn = { x: 326, y: 36, w: 58, h: 18 };
    return (
        <Frame p={p} css={SCENE_CSS}>
            <text x={CX} y={47} fontSize={10} fontWeight={700} fill={p.text}>{spec.header}</text>
            <rect x={btn.x} y={btn.y} width={btn.w} height={btn.h} rx={9} fill={p.chip} />
            <g className="fc-on">
                <rect x={btn.x} y={btn.y} width={btn.w} height={btn.h} rx={9} fill={ACCENT} />
            </g>
            <text x={btn.x + btn.w / 2} y={btn.y + 12} fontSize={7.5} fontWeight={700} textAnchor="middle" fill={p.text}>CSV ↓</text>
            {['Дата', 'Значение', 'Изменение'].map((h, i) => (
                <text key={h} x={CX + i * 110} y={78} fontSize={7.5} fontWeight={700} fill={p.muted}>{h}</text>
            ))}
            <line x1={CX} x2={CX + CW} y1={84} y2={84} stroke={p.faint} />
            {rows.map((r, i) => (
                <g key={i}>
                    {r.map((c, j) => (
                        <text key={j} x={CX + j * 110} y={100 + i * 19} fontSize={8} fill={p.text}>{c}</text>
                    ))}
                    <line x1={CX} x2={CX + CW} y1={106 + i * 19} y2={106 + i * 19} stroke={p.faint} />
                </g>
            ))}
            {/* файл */}
            <g className="fcsv-file">
                <path d="M392 150 h30 l10 10 v40 h-40 z" fill={p.panel} stroke={p.border} />
                <path d="M422 150 v10 h10" fill="none" stroke={p.border} />
                <rect x={392} y={176} width={40} height={12} fill={UP} />
                <text x={412} y={185} fontSize={7.5} fontWeight={800} fill="#fff" textAnchor="middle">CSV</text>
            </g>
            <Cursor className="fc-cursor" style={{ '--cx': `${btn.x + btn.w / 2}px`, '--cy': `${btn.y + 10}px` } as CSSProperties} />
        </Frame>
    );
}

export default function PreviewScene({ spec, theme }: { spec: SceneSpec; theme?: PreviewMode }) {
    const p = usePreviewPalette(theme);
    switch (spec.kind) {
        case 'terminal': return <TerminalPreview theme={theme} />;
        case 'screener': return <ScreenerScene spec={spec} p={p} />;
        case 'alerts': return <AlertsScene spec={spec} p={p} />;
        case 'csv': return <CsvScene spec={spec} p={p} />;
        case 'extension': return <ExtensionScene spec={spec} p={p} />;
        case 'fundpick': return <FundsPickScene spec={spec} p={p} />;
        case 'fundtrades': return <FundTradesScene spec={spec} p={p} />;
        default: return <ChartScene spec={spec} p={p} />;
    }
}
