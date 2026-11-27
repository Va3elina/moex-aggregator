/**
 * FundsScenes — сцены апселла, нарисованные по настоящему интерфейсу:
 *
 *   FundsPickScene  — «Деньги в фондах», режим «Притоки-Оттоки»: сверху линия
 *                     индекса категории, снизу гистограмма потоков (FlowsHistogram).
 *                     Клик по таблетке «Фонды: Акции» → окно «Фонды акций» со
 *                     списком и галочками (FundPickerModal) → снимаем пару фондов
 *                     (выбор применяется сразу) → бейдж «4 из 6», гистограмма
 *                     пересчитана.
 *   FundTradesScene — «Сделки фондов», вкладка «Общий портфель»: слева блок
 *                     «Сделки» (PortfolioMoversPanel: «Чистые покупки ↑ / продажи ↓»,
 *                     нейтральные полосы, пресеты 1М/6М/1Г + календарь), справа
 *                     «Структура» (PortfolioTreemap). Три замка — три хода курсора:
 *                       fresh  — месяц данных → свежий месяц в списке;
 *                       picker — «Все фонды» → снять фонды в пикере;
 *                       range  — календарь → два месяца → диапазон «май – авг».
 *   ExtensionScene  — терминал Т-Инвестиций: кнопка «F» в тулбаре → список
 *                     индикаторов → плавающая панель → тащим её, магнит
 *                     прилипает к краям виджетов терминала (widget.js snapMove),
 *                     и панель встаёт в раскладку как родной виджет.
 *
 * Общая механика «открыл → выбрал → результат» — классы fx-* (2 или 3 клика),
 * точки курсора задаются CSS-переменными --p1x/--p1y … --p3x/--p3y.
 */
import type { CSSProperties, ReactNode } from 'react';
import { Cursor, Frame, WindowChrome } from './previewParts';
import { ACCENT, DOWN, SERIES2, UP, linePath, series, type SceneSpec, type TermPalette } from './previewKit';

const FX_CSS = `
.fp-sc .fx-cursor { opacity: 0; }
.fx3 .fx-cursor { animation-name: fx-cursor3; }
.fx2 .fx-cursor { animation-name: fx-cursor2; }
@keyframes fx-cursor3 {
  0%      { transform: translate(440px, 240px); opacity: 0; }
  6%      { transform: translate(440px, 240px); opacity: 1; }
  18%     { transform: translate(var(--p1x), var(--p1y)) scale(1); }
  20%     { transform: translate(var(--p1x), var(--p1y)) scale(.82); }
  22%     { transform: translate(var(--p1x), var(--p1y)) scale(1); }
  32%     { transform: translate(var(--p2x), var(--p2y)) scale(1); }
  34%     { transform: translate(var(--p2x), var(--p2y)) scale(.82); }
  36%     { transform: translate(var(--p2x), var(--p2y)) scale(1); }
  44%     { transform: translate(var(--p3x), var(--p3y)) scale(1); }
  46%     { transform: translate(var(--p3x), var(--p3y)) scale(.82); }
  48%     { transform: translate(var(--p3x), var(--p3y)) scale(1); opacity: 1; }
  62%     { transform: translate(430px, 228px); opacity: 1; }
  72%,100% { transform: translate(430px, 228px); opacity: 0; }
}
@keyframes fx-cursor2 {
  0%      { transform: translate(440px, 240px); opacity: 0; }
  6%      { transform: translate(440px, 240px); opacity: 1; }
  18%     { transform: translate(var(--p1x), var(--p1y)) scale(1); }
  20%     { transform: translate(var(--p1x), var(--p1y)) scale(.82); }
  22%     { transform: translate(var(--p1x), var(--p1y)) scale(1); }
  32%     { transform: translate(var(--p2x), var(--p2y)) scale(1); }
  34%     { transform: translate(var(--p2x), var(--p2y)) scale(.82); }
  36%     { transform: translate(var(--p2x), var(--p2y)) scale(1); opacity: 1; }
  52%     { transform: translate(430px, 228px); opacity: 1; }
  62%,100% { transform: translate(430px, 228px); opacity: 0; }
}
/* всплывающее окно/список: открывается кликом 1, закрывается после последнего */
.fp-sc .fx-pop { opacity: 0; }
.fx3 .fx-pop { animation-name: fx-pop3; }
.fx2 .fx-pop { animation-name: fx-pop2; }
@keyframes fx-pop3 { 0%,21% { opacity: 0; transform: translateY(4px); } 24%,48% { opacity: 1; transform: translateY(0); } 52%,100% { opacity: 0; } }
@keyframes fx-pop2 { 0%,21% { opacity: 0; transform: translateY(4px); } 24%,36% { opacity: 1; transform: translateY(0); } 40%,100% { opacity: 0; } }
/* эффекты кликов 2 и 3 (снять галочку / выбрать месяц) */
.fp-sc .fx-c2-off { animation-name: fx-c2-off; }
.fp-sc .fx-c3-off { animation-name: fx-c3-off; }
.fp-sc .fx-c2-on { opacity: 0; animation-name: fx-c2-on; }
.fp-sc .fx-c3-on { opacity: 0; animation-name: fx-c3-on; }
@keyframes fx-c2-off { 0%,33% { opacity: 1; } 35%,100% { opacity: 0; } }
@keyframes fx-c3-off { 0%,45% { opacity: 1; } 47%,100% { opacity: 0; } }
@keyframes fx-c2-on  { 0%,33% { opacity: 0; } 35%,60% { opacity: 1; } 64%,100% { opacity: 0; } }
@keyframes fx-c3-on  { 0%,45% { opacity: 0; } 47%,60% { opacity: 1; } 64%,100% { opacity: 0; } }
/* результат: «до» → «после» */
.fp-sc .fx-before { opacity: 0; }
.fx3 .fx-before { animation-name: fx-before3; }
.fx2 .fx-before { animation-name: fx-before2; }
.fx3 .fx-after { animation-name: fx-after3; }
.fx2 .fx-after { animation-name: fx-after2; }
@keyframes fx-before3 { 0%,48% { opacity: 1; } 52%,92% { opacity: 0; } 96%,100% { opacity: 1; } }
@keyframes fx-after3  { 0%,48% { opacity: 0; } 52%,92% { opacity: 1; } 96%,100% { opacity: 0; } }
@keyframes fx-before2 { 0%,36% { opacity: 1; } 40%,92% { opacity: 0; } 96%,100% { opacity: 1; } }
@keyframes fx-after2  { 0%,36% { opacity: 0; } 40%,92% { opacity: 1; } 96%,100% { opacity: 0; } }
.fp-sc .fx-grow { transform-box: fill-box; transform-origin: 0% 50%; }
.fx3 .fx-grow { animation-name: fx-grow3; }
.fx2 .fx-grow { animation-name: fx-grow2; }
.fp-sc .fx-grow.v { transform-origin: 50% 100%; }
.fp-sc .fx-grow.v.dn { transform-origin: 50% 0%; }
@keyframes fx-grow3 { 0%,50% { transform: scale(0, 1); } 64%,100% { transform: scale(1, 1); } }
@keyframes fx-grow2 { 0%,38% { transform: scale(0, 1); } 52%,100% { transform: scale(1, 1); } }
.fp-sc .fx-grow.v { animation-name: fx-growv; }
@keyframes fx-growv { 0%,50% { transform: scaleY(0); } 64%,100% { transform: scaleY(1); } }

/* расширение: кнопка F → список → панель → перетаскивание → магнит */
.fp-sc .fe-cursor { opacity: 0; animation-name: fe-cursor; }
@keyframes fe-cursor {
  0%      { transform: translate(440px, 240px); opacity: 0; }
  6%      { transform: translate(440px, 240px); opacity: 1; }
  16%     { transform: translate(378px, 42px) scale(1); }
  18%     { transform: translate(378px, 42px) scale(.82); }
  20%     { transform: translate(378px, 42px) scale(1); }
  27%     { transform: translate(332px, 74px) scale(1); }
  29%     { transform: translate(332px, 74px) scale(.82); }
  31%     { transform: translate(332px, 74px) scale(1); }
  38%     { transform: translate(166px, 104px) scale(1); }
  40%     { transform: translate(166px, 104px) scale(.88); }
  58%     { transform: translate(316px, 154px) scale(.88); }
  60%     { transform: translate(316px, 154px) scale(1); opacity: 1; }
  70%     { transform: translate(430px, 228px); opacity: 1; }
  78%,100% { transform: translate(430px, 228px); opacity: 0; }
}
.fp-sc .fe-menu { opacity: 0; animation-name: fe-menu; }
@keyframes fe-menu { 0%,19% { opacity: 0; transform: translateY(-4px); } 21%,31% { opacity: 1; transform: translateY(0); } 33%,100% { opacity: 0; } }
.fp-sc .fe-item { opacity: 0; animation-name: fe-item; }
@keyframes fe-item { 0%,25% { opacity: 0; } 27%,31% { opacity: 1; } 33%,100% { opacity: 0; } }
.fp-sc .fe-panel { animation-name: fe-panel; }
@keyframes fe-panel {
  0%,31%  { opacity: 0; transform: translate(-150px, -50px); }
  35%,40% { opacity: 1; transform: translate(-150px, -50px); }
  58%,92% { opacity: 1; transform: translate(0, 0); }
  96%,100% { opacity: 0; transform: translate(0, 0); }
}
.fp-sc .fe-float { opacity: 0; animation-name: fe-float; }
@keyframes fe-float { 0%,31% { opacity: 0; } 35%,56% { opacity: 1; } 60%,100% { opacity: 0; } }
.fp-sc .fe-guide { opacity: 0; animation-name: fe-guide; }
@keyframes fe-guide { 0%,50% { opacity: 0; } 54%,60% { opacity: 1; } 66%,100% { opacity: 0; } }
`;

type P = { p: TermPalette };
const pts = (a: [number, number], b: [number, number], c?: [number, number]) => ({
    '--p1x': `${a[0]}px`, '--p1y': `${a[1]}px`,
    '--p2x': `${b[0]}px`, '--p2y': `${b[1]}px`,
    '--p3x': `${(c ?? b)[0]}px`, '--p3y': `${(c ?? b)[1]}px`,
}) as CSSProperties;

function Check({ x, y, on, p }: { x: number; y: number; on: boolean } & P) {
    return on ? (
        <>
            <rect x={x} y={y} width={10} height={10} rx={2} fill={ACCENT} />
            <path d={`M${x + 2.5} ${y + 5.2} l2 2 l3.6 -4`} fill="none" stroke="#fff" strokeWidth={1.4} strokeLinecap="round" strokeLinejoin="round" />
        </>
    ) : (
        <rect x={x + 0.5} y={y + 0.5} width={9} height={9} rx={2} fill="none" stroke={p.dot} />
    );
}

/** Поп-ап с тенью и рамкой. */
function Pop({ x, y, w, h, p, children, className = 'fx-pop' }: { x: number; y: number; w: number; h: number; children: ReactNode; className?: string } & P) {
    return (
        <g className={className}>
            <rect x={x + 3} y={y + 4} width={w} height={h} rx={9} fill="rgba(0,0,0,0.16)" />
            <rect x={x} y={y} width={w} height={h} rx={9} fill={p.panel} stroke={p.border} />
            {children}
        </g>
    );
}

/* ── Деньги в фондах: свой набор фондов ──────────────────────────────── */

const MONEY_FUNDS: Array<[string, string]> = [
    ['TMOS', '182 млрд'], ['SBMX', '96 млрд'], ['EQMX', '74 млрд'],
    ['AKME', '41 млрд'], ['DIVD', '33 млрд'], ['BCSD', '12 млрд'],
];

function flowVals(n: number, seed: number) {
    return series(n, seed, 1.1).map((v, i) => (v - 0.5) * 2 * (0.5 + 0.5 * Math.abs(Math.sin(i * 1.7 + seed))));
}

function Flows({ vals, grow }: { vals: number[]; grow?: boolean }) {
    const x0 = 50, w = 410, zy = 196, hh = 36;
    const step = w / vals.length, bw = step * 0.6;
    return (
        <>
            {vals.map((v, i) => {
                const h = Math.max(1.5, Math.abs(v) * hh);
                const up = v >= 0;
                return (
                    <rect key={i} className={grow ? `fx-grow v${up ? '' : ' dn'}` : undefined}
                        style={grow ? { animationDelay: `${i * 0.012}s` } : undefined}
                        x={x0 + i * step + (step - bw) / 2} y={up ? zy - h : zy} width={bw} height={h} rx={1}
                        fill={up ? UP : DOWN} />
                );
            })}
        </>
    );
}

export function FundsPickScene({ spec, p }: { spec: SceneSpec } & P) {
    const before = flowVals(28, 5);
    const after = flowVals(28, 16).map((v) => v * 0.8);
    return (
        <Frame p={p} css={FX_CSS} className="fx3">
            <text x={50} y={47} fontSize={10} fontWeight={700} fill={p.text}>{spec.header}</text>
            {/* таблетка «Фонды: Акции» со счётчиком */}
            <rect x={50} y={55} width={132} height={22} rx={6} fill={p.chip} />
            <rect x={56} y={60} width={12} height={12} rx={3} fill={p.dot} />
            <text x={74} y={69.5} fontSize={7.5} fontWeight={700} fill={p.text}>Фонды: Акции</text>
            <g className="fx-before">
                <rect x={150} y={60} width={26} height={12} rx={4} fill={p.bar} />
                <text x={163} y={68.8} fontSize={6.5} fontWeight={800} fill={p.text} textAnchor="middle">Все</text>
            </g>
            <g className="fx-after">
                <rect x={146} y={60} width={32} height={12} rx={4} fill={ACCENT} />
                <text x={162} y={68.8} fontSize={6.5} fontWeight={800} fill="#fff" textAnchor="middle">4 из 6</text>
            </g>
            {/* сверху — индекс категории, снизу — притоки/оттоки */}
            <text x={50} y={92} fontSize={6.5} fill={p.muted}>Индекс МосБиржи</text>
            <path d={linePath(series(80, 2), 50, 96, 410, 40)} fill="none" stroke={SERIES2} strokeWidth={1.5} strokeLinejoin="round" />
            <line x1={50} x2={460} y1={145} y2={145} stroke={p.faint} />
            <text x={50} y={156} fontSize={6.5} fill={p.muted}>Притоки и оттоки, млрд ₽</text>
            <line x1={50} x2={460} y1={196} y2={196} stroke={p.dot} />
            <g className="fx-before"><Flows vals={before} /></g>
            <g className="fx-after"><Flows vals={after} grow /></g>
            {/* окно «Фонды акций» */}
            <Pop x={196} y={58} w={250} h={180} p={p}>
                <text x={208} y={75} fontSize={8.5} fontWeight={700} fill={p.text}>Фонды акций</text>
                <text x={268} y={75} fontSize={6.5} fill={p.muted}>На 26.11.2026</text>
                <line x1={196} x2={446} y1={82} y2={82} stroke={p.faint} />
                <text x={226} y={93} fontSize={6.5} fill={p.muted}>Название</text>
                <text x={434} y={93} fontSize={6.5} fill={p.muted} textAnchor="end">СЧА</text>
                {MONEY_FUNDS.map(([tk, nav], i) => {
                    const y = 99 + i * 19;
                    return (
                        <g key={tk}>
                            <Check x={208} y={y} on={false} p={p} />
                            {i === 1 && <g className="fx-c2-off"><Check x={208} y={y} on p={p} /></g>}
                            {i === 3 && <g className="fx-c3-off"><Check x={208} y={y} on p={p} /></g>}
                            {i !== 1 && i !== 3 && <Check x={208} y={y} on p={p} />}
                            <text x={226} y={y + 8} fontSize={7.5} fontWeight={600} fill={p.text}>{tk}</text>
                            <text x={434} y={y + 8} fontSize={7.5} fill={p.muted} textAnchor="end">{nav}</text>
                        </g>
                    );
                })}
                <rect x={208} y={216} width={226} height={15} rx={5} fill={ACCENT} />
                <text x={321} y={226.5} fontSize={7.5} fontWeight={700} fill="#fff" textAnchor="middle">Готово</text>
            </Pop>
            <Cursor className="fx-cursor" style={pts([116, 66], [213, 123], [213, 161])} />
        </Frame>
    );
}

/* ── Сделки фондов: Общий портфель ───────────────────────────────────── */

const TILE_COLORS = ['#21A038', '#2E6BE6', '#E8483B', '#F2B705', '#7B61FF', '#14A3A3', '#9A958C'];

function Movers({ p, buys, sells, grow }: { buys: Array<[string, number]>; sells: Array<[string, number]>; grow?: boolean } & P) {
    const row = ([tk, v]: [string, number], y: number, key: string) => (
        <g key={key}>
            <text x={50} y={y + 8} fontSize={7.5} fontWeight={700} fill={p.text}>{tk}</text>
            <rect className={grow ? 'fx-grow' : undefined} x={84} y={y + 2} width={v * 120} height={7} rx={3.5} fill={p.bar} />
            <text x={266} y={y + 8} fontSize={7.5} fontWeight={700} fill={p.text} textAnchor="end">
                {(v * 5).toFixed(1).replace('.', ',')}<tspan fill={p.muted} fontWeight={400}> млрд ₽</tspan>
            </text>
        </g>
    );
    return (
        <>
            {buys.map((b, i) => row(b, 112 + i * 16, `b${i}`))}
            {sells.map((s, i) => row(s, 178 + i * 16, `s${i}`))}
        </>
    );
}

function Treemap({ seed }: { seed: number }) {
    const w = series(7, seed, 1.5).map((v) => 0.4 + v).sort((a, b) => b - a);
    const tk = seed % 2 ? ['SBER', 'LKOH', 'T', 'YDEX', 'GAZP', 'PLZL', 'Прочие'] : ['SBER', 'T', 'LKOH', 'GAZP', 'YDEX', 'NVTK', 'Прочие'];
    const X = 280, Y = 100, W = 186, H = 140;
    const rows = [[0, 1], [2, 3, 4], [5]];
    const sum = (ix: number[]) => ix.reduce((s, i) => s + w[i], 0);
    const total = sum([0, 1, 2, 3, 4, 5]);
    let y = Y;
    const tiles: ReactNode[] = [];
    rows.forEach((ix, r) => {
        const rh = ((H - 18) * sum(ix)) / total;
        let x = X;
        ix.forEach((i) => {
            const tw = (W * w[i]) / sum(ix);
            tiles.push(
                <g key={`${r}-${i}`}>
                    <rect x={x + 1} y={y + 1} width={tw - 2} height={rh - 2} rx={3} fill={TILE_COLORS[i]} opacity={0.88} />
                    {tw > 26 && rh > 14 && <text x={x + 5} y={y + 11} fontSize={7} fontWeight={700} fill="#fff">{tk[i]}</text>}
                </g>,
            );
            x += tw;
        });
        y += rh;
    });
    tiles.push(
        <g key="other">
            <rect x={X + 1} y={y + 1} width={W - 2} height={15} rx={3} fill={TILE_COLORS[6]} opacity={0.6} />
            <text x={X + 5} y={y + 11} fontSize={7} fontWeight={700} fill="#fff">{tk[6]}</text>
        </g>,
    );
    return <>{tiles}</>;
}

const BUYS_A: Array<[string, number]> = [['SBER', 0.84], ['T', 0.55], ['YDEX', 0.38]];
const SELLS_A: Array<[string, number]> = [['GAZP', 0.62], ['MTSS', 0.31]];
const BUYS_B: Array<[string, number]> = [['LKOH', 0.92], ['SBER', 0.6], ['PLZL', 0.34]];
const SELLS_B: Array<[string, number]> = [['NVTK', 0.7], ['VTBR', 0.4]];

export function FundTradesScene({ spec, p }: { spec: SceneSpec } & P) {
    const v = spec.variant;
    const pill = (x: number, w: number, label: string, on = false) => (
        <>
            <rect x={x} y={55} width={w} height={20} rx={6} fill={on ? ACCENT : p.chip} />
            <text x={x + w / 2} y={68} fontSize={7.5} fontWeight={700} textAnchor="middle" fill={on ? '#fff' : p.text}>{label}</text>
        </>
    );
    const preset = (x: number, label: string, on: boolean) => (
        <>
            <rect x={x} y={57} width={22} height={16} rx={8} fill={on ? ACCENT : p.chip} />
            <text x={x + 11} y={68} fontSize={7} fontWeight={700} textAnchor="middle" fill={on ? '#fff' : p.muted}>{label}</text>
        </>
    );
    const calendar = (on: boolean) => (
        <>
            <rect x={378} y={57} width={20} height={16} rx={5} fill={on ? ACCENT : p.chip} />
            <rect x={383.5} y={61} width={9} height={8} rx={1.5} fill="none" stroke={on ? '#fff' : p.muted} strokeWidth={1.1} />
            <line x1={383.5} x2={392.5} y1={63.5} y2={63.5} stroke={on ? '#fff' : p.muted} strokeWidth={1.1} />
        </>
    );
    const mode = v === 'fresh' ? 'fx2' : 'fx3';
    const cursor = v === 'fresh' ? pts([192, 65], [196, 92])
        : v === 'picker' ? pts([94, 65], [167, 113], [167, 173])
        : pts([388, 65], [271, 124], [271, 146]);
    // месяц сделок под заголовком «Сделки» (для 1М — один месяц)
    const sub = (label: string) => <text x={84} y={92} fontSize={7} fill={p.muted}>{label}</text>;
    const changed = (beforeNode: ReactNode, afterNode: ReactNode) => (
        <><g className="fx-before">{beforeNode}</g><g className="fx-after">{afterNode}</g></>
    );
    const months = ['дек', 'ноя', 'окт', 'сен', 'авг', 'июл', 'июн', 'май', 'апр', 'мар', 'фев', 'янв'];
    return (
        <Frame p={p} css={FX_CSS} className={v ? mode : 'solo'}>
            <text x={50} y={47} fontSize={10} fontWeight={700} fill={p.text}>{spec.header}</text>
            {/* контролы: фонды · месяц данных · пресеты · календарь */}
            {v === 'picker'
                ? changed(pill(50, 88, 'Все фонды'), pill(50, 88, '3 из 5 фондов', true))
                : pill(50, 88, 'Все фонды')}
            {v === 'fresh'
                ? changed(pill(144, 92, 'Сентябрь 2026'), pill(144, 92, 'Октябрь 2026', true))
                : pill(144, 92, 'Октябрь 2026')}
            {v === 'range'
                ? changed(<>{preset(304, '1М', true)}{preset(328, '6М', false)}{preset(352, '1Г', false)}{calendar(false)}</>,
                    <>{preset(304, '1М', false)}{preset(328, '6М', false)}{preset(352, '1Г', false)}{calendar(true)}</>)
                : <>{preset(304, '1М', true)}{preset(328, '6М', false)}{preset(352, '1Г', false)}{calendar(false)}</>}

            {/* «Сделки» */}
            <text x={50} y={92} fontSize={8.5} fontWeight={700} fill={p.text}>Сделки</text>
            {v === 'range' ? changed(sub('окт 2026'), sub('май – авг 2026'))
                : v === 'fresh' ? changed(sub('сен 2026'), sub('окт 2026'))
                : sub('окт 2026')}
            <text x={50} y={106} fontSize={6.5} fontWeight={700} fill={UP}>Чистые покупки ↑</text>
            <text x={50} y={172} fontSize={6.5} fontWeight={700} fill={DOWN}>Чистые продажи ↓</text>
            {v
                ? changed(<Movers p={p} buys={BUYS_A} sells={SELLS_A} />, <Movers p={p} buys={BUYS_B} sells={SELLS_B} grow />)
                : <Movers p={p} buys={BUYS_A} sells={SELLS_A} />}

            {/* «Структура» */}
            <line x1={274} x2={274} y1={84} y2={250} stroke={p.faint} />
            <text x={280} y={92} fontSize={8.5} fontWeight={700} fill={p.text}>Структура</text>
            {v ? changed(<Treemap seed={3} />, <Treemap seed={8} />) : <Treemap seed={3} />}

            {/* всплывающие окна */}
            {v === 'fresh' && (
                <Pop x={144} y={79} w={112} h={60} p={p}>
                    {['Октябрь 2026', 'Сентябрь 2026', 'Август 2026'].map((m, i) => (
                        <text key={m} x={154} y={95 + i * 17} fontSize={7.5} fontWeight={i === 1 ? 700 : 500} fill={p.text}>{m}</text>
                    ))}
                    <rect className="fx-c2-on" x={148} y={85} width={104} height={14} rx={4} fill={ACCENT} opacity={0.18} />
                </Pop>
            )}
            {v === 'picker' && (
                <Pop x={150} y={60} w={242} h={176} p={p}>
                    <text x={162} y={77} fontSize={8.5} fontWeight={700} fill={p.text}>Фонды акций</text>
                    <line x1={150} x2={392} y1={84} y2={84} stroke={p.faint} />
                    {['TMOS', 'SBMX', 'EQMX', 'AKME', 'DIVD'].map((tk, i) => {
                        const y = 90 + i * 20;
                        return (
                            <g key={tk}>
                                <Check x={162} y={y + 18} on={false} p={p} />
                                {i === 0 && <g className="fx-c2-off"><Check x={162} y={y + 18} on p={p} /></g>}
                                {i === 3 && <g className="fx-c3-off"><Check x={162} y={y + 18} on p={p} /></g>}
                                {i !== 0 && i !== 3 && <Check x={162} y={y + 18} on p={p} />}
                                <text x={180} y={y + 26} fontSize={7.5} fontWeight={600} fill={p.text}>{tk}</text>
                            </g>
                        );
                    })}
                    <rect x={162} y={216} width={218} height={14} rx={5} fill={ACCENT} />
                    <text x={271} y={226} fontSize={7.5} fontWeight={700} fill="#fff" textAnchor="middle">Готово</text>
                </Pop>
            )}
            {v === 'range' && (
                <Pop x={250} y={78} w={152} h={92} p={p}>
                    <text x={258} y={92} fontSize={7.5} fontWeight={700} fill={p.text}>2026</text>
                    {months.map((m, i) => {
                        const c = i % 4, r = Math.floor(i / 4);
                        const x = 258 + c * 35, y = 98 + r * 22;
                        const grey = i < 2;
                        const inRange = i >= 4 && i <= 8; // авг … апр
                        return (
                            <g key={m}>
                                {inRange && <rect className={i === 4 ? 'fx-c2-on' : 'fx-c3-on'} x={x} y={y} width={31} height={17} rx={4} fill={ACCENT}
                                    opacity={i === 4 || i === 8 ? 1 : 0.3} />}
                                <text x={x + 15.5} y={y + 11.5} fontSize={7} textAnchor="middle" fill={grey ? p.dot : p.text}>{m}</text>
                            </g>
                        );
                    })}
                </Pop>
            )}
            {v && <Cursor className="fx-cursor" style={cursor} />}
        </Frame>
    );
}

/* ── Расширение для терминала Т-Инвестиций ───────────────────────────── */

export function ExtensionScene({ spec, p }: { spec: SceneSpec } & P) {
    const candles = series(22, 11, 0.4);
    const slot = { x: 256, y: 146, w: 214, h: 98 };
    return (
        <Frame p={p} css={FX_CSS} fill={p.body}>
            {/* тулбар терминала с кнопкой «F» расширения */}
            {[0, 1, 2].map((i) => <circle key={i} cx={50 + i * 10} cy={42} r={3} fill={p.dot} />)}
            <text x={86} y={45} fontSize={8} fontWeight={700} fill={p.text}>{spec.header}</text>
            <rect x={368} y={32} width={20} height={20} rx={6} fill={ACCENT} />
            <text x={378} y={45.5} fontSize={9} fontWeight={800} fill="#fff" textAnchor="middle">F</text>
            {/* виджеты терминала: график и стакан; справа снизу — свободное место */}
            <WindowChrome x={40} y={60} w={210} h={184} title="SBER · 1ч" p={p} />
            {candles.map((v, i) => {
                const x = 50 + i * 9;
                const o = 220 - v * 120, c = 220 - candles[Math.min(i + 1, candles.length - 1)] * 120;
                const up = c < o;
                return (
                    <g key={i}>
                        <line x1={x + 3} x2={x + 3} y1={Math.min(o, c) - 6} y2={Math.max(o, c) + 6} stroke={up ? UP : DOWN} />
                        <rect x={x} y={Math.min(o, c)} width={6} height={Math.max(2, Math.abs(o - c))} fill={up ? UP : DOWN} />
                    </g>
                );
            })}
            <WindowChrome x={256} y={60} w={214} h={80} title="Стакан" p={p} />
            {[0, 1, 2, 3, 4].map((i) => (
                <g key={i}>
                    <rect x={330} y={86 + i * 10} width={20 + ((i * 37) % 60)} height={7} fill={i < 2 ? 'rgba(239,111,111,.25)' : 'rgba(91,212,156,.25)'} />
                    <text x={264} y={92 + i * 10} fontSize={6.5} fill={i < 2 ? DOWN : UP}>{(312.4 - i * 0.1).toFixed(1)}</text>
                </g>
            ))}
            {/* направляющие магнита по краям соседних виджетов */}
            <g className="fe-guide">
                <line x1={slot.x} x2={slot.x} y1={56} y2={250} stroke={ACCENT} strokeDasharray="3 3" />
                <line x1={250} x2={480} y1={slot.y - 3} y2={slot.y - 3} stroke={ACCENT} strokeDasharray="3 3" />
                <line x1={slot.x + slot.w} x2={slot.x + slot.w} y1={56} y2={250} stroke={ACCENT} strokeDasharray="3 3" />
            </g>
            {/* панель Фрейма: появляется плавающей, встаёт в раскладку */}
            <g className="fe-panel">
                <rect className="fe-float" x={slot.x + 4} y={slot.y + 6} width={slot.w} height={slot.h} rx={6} fill="rgba(0,0,0,0.22)" />
                <WindowChrome x={slot.x} y={slot.y} w={slot.w} h={slot.h} title="F  Открытые позиции · Si" p={p} />
                <path d={linePath(series(40, 3), slot.x + 10, slot.y + 30, slot.w - 20, 52)} fill="none" stroke={ACCENT} strokeWidth={1.6} />
                <path d={linePath(series(40, 10).map((x) => 0.15 + x * 0.6), slot.x + 10, slot.y + 30, slot.w - 20, 52)} fill="none" stroke={SERIES2} strokeWidth={1.6} />
            </g>
            {/* список индикаторов по кнопке «F» */}
            <Pop x={286} y={56} w={110} h={72} p={p} className="fe-menu">
                <text x={296} y={68} fontSize={6.5} fill={p.muted}>Рынок</text>
                <rect className="fe-item" x={290} y={70} width={102} height={13} rx={4} fill={ACCENT} opacity={0.18} />
                <text x={296} y={79.5} fontSize={7.5} fontWeight={600} fill={p.text}>Открытые позиции</text>
                <text x={296} y={93} fontSize={7.5} fill={p.text}>Сигналы</text>
                <text x={296} y={108} fontSize={6.5} fill={p.muted}>Фонды</text>
                <text x={296} y={120} fontSize={7.5} fill={p.text}>Деньги в фондах</text>
            </Pop>
            <Cursor className="fe-cursor" />
        </Frame>
    );
}
