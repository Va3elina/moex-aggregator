/**
 * CompanyShareChart — режимы «Позиция» и «% в обращении» в «Потоках
 * по компании» (пункты общего ряда режимов, см. CompanyFlowsTab).
 *
 * Оформление — по макету «Силы рынка» (StrengthPage): ДВЕ визуально
 * разделённые секции в одной карточке, у каждой своя центрированная легенда,
 * между ними разделитель border-b. Верхняя — недельная линия цены акции
 * (контекст), нижняя — помесячная гистограмма ПОЗИЦИИ бумаги у выбранных
 * фондов. Курсор общий: вертикаль идёт сквозь обе панели, тултип один на обе.
 *
 * Значение месяца — три режима (пункты общего списка в CompanyFlowsTab):
 *   rub      — Σ(СЧА_фонда × доля_%/100): АБСОЛЮТНАЯ позиция фондов в бумаге,
 *              в рублях (ось — адаптивно млн/млрд ₽);
 *   cap      — Σ позиций / free-float капа компании (ffcap из /company-weights,
 *              источник MOEXBMI), %: сколько торгуемой части компании лежит
 *              в выбранных фондах;
 *   overhang — Σ позиций / медианный дневной оборот торгов за скользящие три
 *              месяца, только будние сессии (turnover из /price-weekly), дни:
 *              за сколько дней торгов фонды могли бы выйти из бумаги, забирая
 *              весь оборот («Навес»).
 * В расчёт месяца входят только фонды с ПОЛНЫМ снапшотом (weights[i] != null);
 * 0 — честный ноль (фонд отчитался без бумаги), null — дыра данных → месяц
 * без бара (разрыв), а не ложный ноль. Месяц без единой СЧА тоже null: без
 * СЧА рубли не восстановить, ложный ноль хуже дыры.
 *
 * Ось X — МЕСЯЦЫ (слоты), общая для обеих панелей. Недели цены раскладываются
 * внутри слота своего месяца дробно ((k+0.5)/K): линия остаётся гладкой, а бар
 * месяца геометрически совпадает со своим участком линии — таймфреймы разные,
 * сетка одна. Нет истории цены (облигация/ОФЗ) → верхней секции нет,
 * гистограмма забирает всю высоту.
 *
 * Механика (навигатор-брашер, watermark, пилюля даты, тултип с разбивкой по
 * фондам) — 1-в-1 с CompanyFlowsPriceMap. В тултипе дополнительно «Сделки за
 * месяц» (нетто из /company-flows): подсказка, двигали долю сделки или
 * переоценка.
 */
import {
    useEffect,
    useMemo,
    useLayoutEffect,
    useRef,
    useState,
} from 'react';
import { Percent } from 'lucide-react';
import { GRID, CROSSHAIR, ANIMATION, FUND_PALETTE, cssVar } from '../../config/chartTheme';
import { useIsMobile } from '../../hooks/useIsMobile';
import ChartWatermark from '../ChartWatermark';
import ChartNavigator from '../ChartNavigator';
import ChartLegend from '../chart/ChartLegend';
import { ChartTooltip, TooltipRow, ChartDatePill, ChartAxisPill } from '../chart';
import { useChartReveal } from '../chart/useChartReveal';
import { useNoChartAnim } from '../chart/chartAnim';
import { resampleVals, morphPts } from '../../utils/chartAnimation';

const easeOutCubic = ANIMATION.easing;

// Цвет линии цены — акцентный оранжевый: цена главная, тянет взгляд на себя.
const PRICE_LINE_COLOR = 'var(--accent)';
// Бары доли — deep indigo (холодный фон под линией): доля не несёт знака,
// зелёный/красный тут были бы ложью.
const BAR_COLOR = FUND_PALETTE[0];

// Полоса подписей оси X внутри нижней секции, px.
const XLABEL_H = 40;

export type ShareMode = 'rub' | 'cap' | 'overhang';

export interface CompanyShareFundSeries {
    label: string;
    color: string;
    /** Доля бумаги в % от СЧА фонда, по месяцам (null = нет полного снапшота). */
    weights: (number | null)[];
    /** Полная СЧА фонда на тот же снапшот (для веса «по капиталу»). */
    navs: (number | null)[];
}

interface CompanyShareChartProps {
    /** Панельный режим (окно терминала/расширения): без карточки-рамки и
     *  внутренних отступов — график занимает всю площадь окна. См.
     *  CompanyFlowsTab embedded. На сайте не задаётся → вид прежний. */
    bare?: boolean;
    /** "YYYY-MM" — месячная ось (уже обрезана периодом в родителе). */
    months: string[];
    /** Серии по фондам (уже отфильтрованные пикером), выровнено с months. */
    funds: CompanyShareFundSeries[];
    /** Free-float капа компании (руб), выровнено с months (null = нет в MOEXBMI). */
    ffcap?: (number | null)[];
    /** Медианный дневной оборот торгов (руб) за скользящее окно «месяц + два
     *  предыдущих», выровнено с months — знаменатель режима «Навес»
     *  (null = месяц без торгов/данных). */
    turnover?: (number | null)[];
    /** ISO-даты понедельников недель, ASC — вся история цены. */
    weeks: string[];
    /** Недельные закрытия, выровнено с weeks. */
    closes: number[];
    shareMode: ShareMode;
    /** Имя бумаги для легенды — подпись линии цены. */
    assetName?: string;
    height?: number;
    loading?: boolean;
    /** Все фонды сняты пользователем — empty-state. */
    noFundsSelected?: boolean;
    /** Нет истории цены (не акция) → верхней секции нет. */
    priceMissing?: boolean;
    /** Смена бумаги / фондов / периода / веса: сброс навигатора. Волна и
     *  reveal играют только на первом рендере, дальше бары морфят. */
    animTrigger?: string;
}

// ── Форматтеры ──
function fmtPct(v: number): string {
    const d = v >= 10 ? 1 : v >= 0.1 ? 2 : 3;
    return `${v.toLocaleString('ru-RU', { maximumFractionDigits: d })}%`;
}

// Рубли — адаптивно (решение владельца 2026-08-10): по одной бумаге позиция
// фондов гуляет от сотен млн до десятков млрд, фиксированные «млрд» дали бы
// «0,0» на половине бумаг. suffix=false — для оси (короче, ₽ и так ясен).
function fmtRub(v: number, suffix = true): string {
    const sfx = suffix ? ' ₽' : '';
    if (v === 0) return `0${sfx}`; // нулевой тик оси — «0», а не «0 тыс»
    if (v >= 1e9) {
        const b = v / 1e9;
        const d = b >= 100 ? 0 : b >= 10 ? 1 : 2;
        return `${b.toLocaleString('ru-RU', { maximumFractionDigits: d })} млрд${sfx}`;
    }
    if (v >= 1e6) {
        const m = v / 1e6;
        return `${m.toLocaleString('ru-RU', { maximumFractionDigits: m >= 10 ? 0 : 1 })} млн${sfx}`;
    }
    return `${(v / 1e3).toLocaleString('ru-RU', { maximumFractionDigits: 0 })} тыс${sfx}`;
}

// Дни (режим «Навес»): навес по бумаге гуляет от долей дня до сотен дней,
// поэтому знаки — адаптивно, как у рублей.
function fmtDays(v: number): string {
    const d = v >= 10 ? 0 : v >= 1 ? 1 : 2;
    return `${v.toLocaleString('ru-RU', { maximumFractionDigits: d })} дн`;
}

function fmtPrice(v: number): string {
    if (v >= 1000) return Math.round(v).toLocaleString('ru-RU');
    if (v >= 100) return v.toFixed(1);
    if (v >= 1) return v.toFixed(2);
    return v.toFixed(4);
}

function monthLabel(m: string): string {
    return new Date(`${m}-01T00:00:00`).toLocaleDateString('ru-RU', { month: 'long', year: 'numeric' });
}

export default function CompanyShareChart({
    months: monthsAll,
    funds: fundsAll,
    ffcap: ffcapAll,
    turnover: turnoverAll,
    weeks: weeksAll,
    closes: closesAll,
    shareMode,
    assetName,
    height = 420,
    loading = false,
    noFundsSelected = false,
    priceMissing = false,
    animTrigger,
    bare = false,
}: CompanyShareChartProps) {
    const isMobile = useIsMobile();
    // Обёртка обеих панелей — общая система координат для курсора и тултипа.
    const wrapRef = useRef<HTMLDivElement>(null);
    const priceSvgRef = useRef<SVGSVGElement>(null);
    const shareSvgRef = useRef<SVGSVGElement>(null);

    // ── Значение месяца по выбранному весу (по ПОЛНОМУ набору месяцев). ──
    // rub — абсолютная позиция Σ(СЧА×доля), руб; cap — Σ позиций / free-float
    // капа компании, %; overhang — Σ позиций / медианный дневной оборот
    // месяца = дней торгов на выход фондов из бумаги («Навес»).
    const shareValsAll = useMemo(() => {
        return monthsAll.map((_, i) => {
            let rub = 0, hasNav = false, cnt = 0;
            for (const f of fundsAll) {
                const w = f.weights[i];
                if (w == null) continue;
                cnt++;
                const nav = f.navs[i];
                if (nav != null && nav > 0) { rub += nav * (w / 100); hasNav = true; }
            }
            // Нет ни одного полного снапшота или ни одной СЧА — дыра, а не
            // ложный ноль: без СЧА рубли позиции не восстановить.
            if (cnt === 0 || !hasNav) return null;
            if (shareMode === 'rub') return rub;
            if (shareMode === 'overhang') {
                const t = turnoverAll?.[i];
                return t != null && t > 0 ? rub / t : null;
            }
            const cap = ffcapAll?.[i];
            return cap != null && cap > 0 ? (rub / cap) * 100 : null;
        });
    }, [monthsAll, fundsAll, ffcapAll, turnoverAll, shareMode]);

    // ── Обрезка пустого левого хвоста: до первого месяца с долей > 0. ──
    const trimStart = useMemo(() => {
        for (let i = 0; i < monthsAll.length; i++) {
            const v = shareValsAll[i];
            if (v != null && v > 0) return i;
        }
        return 0;
    }, [monthsAll, shareValsAll]);

    const months = useMemo(() => monthsAll.slice(trimStart), [monthsAll, trimStart]);
    const shareVals = useMemo(() => shareValsAll.slice(trimStart), [shareValsAll, trimStart]);
    const ffcap = useMemo(() => ffcapAll?.slice(trimStart), [ffcapAll, trimStart]);
    const turnover = useMemo(() => turnoverAll?.slice(trimStart), [turnoverAll, trimStart]);
    const funds = useMemo(
        () => fundsAll.map(f => ({
            ...f,
            weights: f.weights.slice(trimStart),
            navs: f.navs.slice(trimStart),
        })),
        [fundsAll, trimStart],
    );

    const hasPrice = !priceMissing && weeksAll.length > 1;

    // ── Резерв панели цены на время загрузки. ──
    // При смене бумаги родитель сразу отдаёт weeks=[] (гард по тикеру), и
    // hasPrice падает в false до прихода новой цены: верхняя панель
    // схлопывалась (topH=0), гистограмма на полсекунды раздувалась во весь
    // холст, а затем «падала вниз». Пока грузится цена, держим место панели,
    // если у ПРЕДЫДУЩЕЙ бумаги цена была (облигация → облигация место не
    // резервирует — панели не было и не будет).
    const prevHadPriceRef = useRef(false);
    useEffect(() => {
        if (!loading) prevHadPriceRef.current = hasPrice;
    }, [loading, hasPrice]);
    const showPricePanel = hasPrice || (loading && !priceMissing && prevHadPriceRef.current);

    // Высоты секций: из общего бюджета вычитаем хром второй секции (её легенда
    // с зазором + разделитель) и полосу подписей X; остальное делим 2:1 в
    // пользу цены — как в «Силе рынка» (--strength-chart-top-height 300 /
    // bottom 150). Без цены гистограмма забирает всё (там же — solo-height).
    //
    // CHROME_H подобран так, чтобы карточка «Доли» была РОВНО той же высоты,
    // что карточка «Карты сделок» (там весь бюджет уходит в один плот): при
    // прежних 64px она была на 19px ниже и график прыгал при смене режима.
    // Обе формулы линейны по height, поэтому поправка — константа.
    const CHROME_H = 45;
    const inner = Math.max(height - CHROME_H - XLABEL_H, 240);
    const topH = showPricePanel ? Math.round(inner * (2 / 3)) : 0;
    const botH = inner - topH;

    // ── Недели цены, разложенные по слотам месяцев. ──
    // Для каждого месяца — список [weekIdx...]; неделя k из K сидит на дробной
    // позиции (k+0.5)/K внутри слота. Сравнение "YYYY-MM-DD".slice(0,7) —
    // лексикографика ISO.
    const weeksByMonth = useMemo(() => {
        const map = new Map<string, number[]>();
        if (!months.length) return map;
        const lo = months[0];
        const hi = months[months.length - 1];
        for (let i = 0; i < weeksAll.length; i++) {
            const m = weeksAll[i].slice(0, 7);
            if (m < lo || m > hi) continue;
            const v = closesAll[i];
            if (v == null || !(v > 0)) continue;
            let arr = map.get(m);
            if (!arr) { arr = []; map.set(m, arr); }
            arr.push(i);
        }
        return map;
    }, [months, weeksAll, closesAll]);

    const hasData = months.length > 0 && shareVals.some(v => v != null);

    // ── Навигатор: [start, end] по индексам МЕСЯЦЕВ. ──
    const [navRange, setNavRange] = useState<[number, number]>([0, 0]);
    useLayoutEffect(() => {
        if (months.length > 0) setNavRange([0, months.length - 1]);
    }, [months.length, animTrigger]);

    // Превью навигатора: линия цены (последнее закрытие месяца); дыры тянем
    // предыдущим значением. Нет цены → сама доля.
    const navigatorData = useMemo(() => {
        let prev = 0;
        return months.map((m, i) => {
            let v: number | null = null;
            if (hasPrice) {
                const wk = weeksByMonth.get(m);
                if (wk && wk.length) v = closesAll[wk[wk.length - 1]];
            } else {
                v = shareVals[i] ?? null;
            }
            if (v == null) v = prev;
            prev = v;
            return { time: `${m}-01`, value: v };
        });
    }, [months, hasPrice, weeksByMonth, closesAll, shareVals]);

    // ── Видимое окно ──
    const visStart = navRange[0];
    const visCount = Math.max(navRange[1] - navRange[0] + 1, 1);
    const visMonthIdx = useMemo(
        () => Array.from({ length: visCount }, (_, i) => visStart + i).filter(i => i < months.length),
        [visStart, visCount, months.length],
    );

    // Шкала цены видимого окна (по неделям видимых месяцев), поля 6%.
    const [priceLo, priceHi] = useMemo(() => {
        if (!hasPrice) return [0, 1];
        const vals: number[] = [];
        for (const mi of visMonthIdx) {
            const wk = weeksByMonth.get(months[mi]);
            if (wk) for (const wi of wk) vals.push(closesAll[wi]);
        }
        if (!vals.length) return [0, 1];
        let lo = Math.min(...vals);
        let hi = Math.max(...vals);
        if (lo === hi) { lo *= 0.97; hi *= 1.03; }
        const pad = (hi - lo) * 0.06;
        return [lo - pad, hi + pad];
    }, [hasPrice, visMonthIdx, months, weeksByMonth, closesAll]);

    // Шкала доли: 0..max видимого окна ×1.12 (бар максимума не упирается в
    // верхнюю грид-линию — она же потолок шкалы, см. shareTicks).
    const shareMax = useMemo(() => {
        let mx = 0;
        for (const mi of visMonthIdx) {
            const v = shareVals[mi];
            if (v != null && v > mx) mx = v;
        }
        return (mx || 0.0001) * 1.12;
    }, [visMonthIdx, shareVals]);

    // Координаты в СВОЁМ viewBox 0..1000 каждой панели.
    const slotX = (mi: number, frac = 0.5) => ((mi - visStart + frac) / visCount) * 1000;
    // Верхний зазор панели цены задан в px снаружи (--chart-pad-top), поэтому
    // здесь верхний тик стоит на 0: иначе inset задваивался бы и грид-линия
    // уезжала ниже, чем в OI.
    const priceY = (close: number) =>
        ((1 - (close - priceLo) / (priceHi - priceLo)) * 0.95) * 1000;
    const shareY = (v: number) => (1 - v / shareMax) * 1000;

    // Линия цены: недели видимых месяцев, дробно внутри слотов.
    const targetLinePts = useMemo(() => {
        if (!hasPrice) return [] as { x: number; y: number }[];
        const pts: { x: number; y: number }[] = [];
        for (const mi of visMonthIdx) {
            const wk = weeksByMonth.get(months[mi]);
            if (!wk) continue;
            for (let k = 0; k < wk.length; k++) {
                pts.push({
                    x: slotX(mi, (k + 0.5) / wk.length),
                    y: priceY(closesAll[wk[k]]),
                });
            }
        }
        return pts;
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [hasPrice, visMonthIdx, months, weeksByMonth, closesAll, priceLo, priceHi, visStart, visCount]);

    // ── Морф линии при смене данных (бумага/период/режим) — как в OI:
    // из текущих отображаемых точек в новые (morphPts ресемплирует). Драг
    // навигатора — мгновенно. Смена данных тянет за собой сброс навигатора
    // следующим коммитом (эффект navRange выше) — морф в полёте ретаргетится
    // в новое окно, а не обрывается snap'ом.
    const [linePts, setLinePts] = useState<{ x: number; y: number }[]>([]);
    const linePtsRef = useRef<{ x: number; y: number }[]>([]);
    const lineRafRef = useRef<number | null>(null);
    // Отдельный ref, НЕ lineRafRef: cleanup эффекта обнуляет rafRef до
    // выполнения нового тела — проверка «морф в полёте» через него всегда ложь.
    const lineMorphActiveRef = useRef(false);
    const prevLineDataRef = useRef<unknown[] | null>(null);
    useLayoutEffect(() => {
        const dataKey = [months, weeksAll, closesAll];
        const prev = prevLineDataRef.current;
        const dataChanged = !prev || prev.some((v, i) => v !== dataKey[i]);
        prevLineDataRef.current = dataKey;
        const morphInFlight = lineMorphActiveRef.current;
        if (lineRafRef.current) cancelAnimationFrame(lineRafRef.current);
        lineRafRef.current = null;
        const from = linePtsRef.current;
        const needMorph = (dataChanged || morphInFlight)
            && from.length >= 2 && targetLinePts.length >= 2;
        if (!needMorph) {
            // Стартовый кадр/драг — мгновенно, до paint.
            lineMorphActiveRef.current = false;
            linePtsRef.current = targetLinePts;
            setLinePts(targetLinePts);
            return;
        }
        lineMorphActiveRef.current = true;
        let start: number | null = null;
        const tick = (ts: number) => {
            if (start == null) start = ts;
            const t = Math.min((ts - start) / ANIMATION.morphDuration, 1);
            // Финальный кадр — ТОЧНЫЕ целевые точки, не morphPts(…, 1): при
            // разной длине рядов morphPts ресемплирует и линия оставалась
            // сглаженной копией (срезанные вершины) до следующего драга.
            const pts = t >= 1 ? targetLinePts : morphPts(from, targetLinePts, easeOutCubic(t));
            linePtsRef.current = pts;
            setLinePts(pts);
            if (t < 1) {
                lineRafRef.current = requestAnimationFrame(tick);
            } else {
                lineRafRef.current = null;
                lineMorphActiveRef.current = false;
            }
        };
        lineRafRef.current = requestAnimationFrame(tick);
        return () => {
            if (lineRafRef.current) {
                cancelAnimationFrame(lineRafRef.current);
                lineRafRef.current = null;
            }
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [targetLinePts]);
    const linePath = useMemo(
        () => (linePts.length >= 2
            ? linePts.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(2)},${p.y.toFixed(2)}`).join(' ')
            : ''),
        [linePts],
    );

    // Тики цены (4) и доли (максимум / середина / 0).
    const priceTicks = useMemo(
        () => (hasPrice
            ? Array.from({ length: 4 }, (_, i) => priceLo + ((3 - i) / 3) * (priceHi - priceLo))
            : []),
        [hasPrice, priceLo, priceHi],
    );
    // Верхний тик — потолок шкалы (с headroom), а не максимум бара: только так
    // верхняя грид-линия встаёт на самый верх плота и её зазор от легенды равен
    // зазору легенды от разделителя. Бар максимума headroom'а не касается.
    const shareTicks = useMemo(
        () => [shareMax, shareMax / 2, 0],
        [shareMax],
    );

    // ── Анимация баров — эталонная схема «Деньги в фондах» (FundsMoneyPage). ──
    // Волна с нуля (каскад слева направо) один раз — на первом рендере с
    // данными. Дальнейшие смены (режим, бумага, период) морфят из текущих
    // отображаемых значений: ресемпл к новой длине + нормализация со старой
    // шкалы на новую (морф в визуальном пространстве — иначе при смене режима
    // ₽→% старые значения в новой шкале вылетали бы за холст). useLayoutEffect
    // + синхронный стартовый кадр до paint — без вспышки «25-го кадра».
    // Драг навигатора shareVals не меняет — окно слайсит уже готовые бары.
    const [dispVals, setDispVals] = useState<number[]>([]);
    const dispRef = useRef<number[]>([]);
    const wavedRef = useRef(false);
    const rafRef = useRef<number | null>(null);
    const targetVals = useMemo(
        () => shareVals.map(v => (v != null && v > 0 ? v : 0)),
        [shareVals],
    );
    const noAnim = useNoChartAnim();
    useLayoutEffect(() => {
        if (!hasData) {
            dispRef.current = [];
            setDispVals([]);
            return;
        }
        if (rafRef.current) cancelAnimationFrame(rafRef.current);
        const target = targetVals;
        const wave = !wavedRef.current || dispRef.current.length === 0;
        wavedRef.current = true;
        // Песочница: без волны появления (см. chart/chartAnim.ts).
        if (wave && noAnim) {
            dispRef.current = target;
            setDispVals(target);
            return;
        }
        const newMax = Math.max(...target, 0.0001);
        const oldMax = Math.max(...dispRef.current, 0.0001);
        const from = wave
            ? new Array<number>(target.length).fill(0)
            : resampleVals(dispRef.current, target.length).map(v => (v / oldMax) * newMax);
        // Стартовый кадр — до первого paint, чтобы не мигнуть старыми барами.
        dispRef.current = from;
        setDispVals(from);
        const totalDuration = wave ? ANIMATION.waveDuration : ANIMATION.morphDuration;
        const staggerDelay = wave ? ANIMATION.waveStagger : 0;
        let start: number | null = null;
        const tick = (ts: number) => {
            if (start == null) start = ts;
            const elapsed = ts - start;
            const next = target.map((v, i) => {
                const barDelay = (i / target.length) * staggerDelay;
                const barElapsed = Math.max(0, elapsed - barDelay);
                const t = Math.min(barElapsed / (totalDuration - staggerDelay), 1);
                return from[i] + (v - from[i]) * easeOutCubic(t);
            });
            dispRef.current = next;
            setDispVals(next);
            if (elapsed < totalDuration) rafRef.current = requestAnimationFrame(tick);
        };
        rafRef.current = requestAnimationFrame(tick);
        return () => {
            if (rafRef.current) cancelAnimationFrame(rafRef.current);
        };
    }, [targetVals, hasData, noAnim]);

    // ── Reveal линии цены слева направо (rAF clip-rect, юниты viewBox 0..1000).
    // Стартует, когда цена впервые готова, и играет один раз — как в OI.
    const revealW = useChartReveal(hasPrice && linePath !== '', 1000);

    // ── Hover: ближайший месяц с данными по X (общий на обе панели). ──
    const [hoveredMi, setHoveredMi] = useState<number | null>(null); // индекс в months
    const [tooltipPos, setTooltipPos] = useState<{ x: number; y: number } | null>(null);

    const hoverableIdx = useMemo(
        () => visMonthIdx.filter(mi => shareVals[mi] != null),
        [visMonthIdx, shareVals],
    );

    const handleMouseMove = (e: React.MouseEvent<HTMLDivElement>) => {
        const geomSvg = shareSvgRef.current;
        if (!hoverableIdx.length || !wrapRef.current || !geomSvg) return;
        const wrapRect = wrapRef.current.getBoundingClientRect();
        const svgRect = geomSvg.getBoundingClientRect();
        const xInChart = e.clientX - svgRect.left;
        if (xInChart < 0 || xInChart > svgRect.width) return;
        let best = hoverableIdx[0];
        let bestDist = Infinity;
        for (const mi of hoverableIdx) {
            const mx = (slotX(mi) / 1000) * svgRect.width;
            const d = Math.abs(mx - xInChart);
            if (d < bestDist) { bestDist = d; best = mi; }
        }
        setHoveredMi(best);
        const snapX = (svgRect.left - wrapRect.left) + (slotX(best) / 1000) * svgRect.width;
        setTooltipPos({ x: snapX, y: e.clientY - wrapRect.top });
    };
    const handleMouseLeave = () => {
        setHoveredMi(null);
        setTooltipPos(null);
    };

    // ── Геометрия оверлеев (пилюля даты + тултип) ──
    // Оба живут ВНУТРИ wrapRef, значит их offsetParent — сам wrapRef, и
    // координаты считаются от его верхнего левого угла. Прибавлять offsetTop/
    // offsetLeft обёртки (как это делает StrengthPage, где пилюля лежит на
    // уровне карточки, а обёртка — вложенный .chart-plot) здесь НЕЛЬЗЯ:
    // это двойной учёт, из-за него пилюля уезжала вниз и вправо от курсора.
    //
    // topLineY — верхняя грид-линия ПЕРВОЙ панели (цена — на самом верху
    // своего viewBox; без цены — верхний тик доли 10.7%). Контракт тот же, что в
    // «Силе рынка»: низ пилюли = topLineY + PILL_GAP_ABOVE_LINE, а тултип
    // клампится в коридор от этой линии до полосы X-подписей.
    const overlayGeom = (() => {
        const wrap = wrapRef.current;
        const topSvg = priceSvgRef.current ?? shareSvgRef.current;
        if (!wrap || !topSvg) return null;
        const wrapRect = wrap.getBoundingClientRect();
        const svgRect = topSvg.getBoundingClientRect();
        return {
            plotLeft: svgRect.left - wrapRect.left,
            plotWidth: svgRect.width,
            topLineY: svgRect.top - wrapRect.top,
        };
    })();

    // Разбивка тултипа: вклад каждого фонда В ЕДИНИЦАХ РЕЖИМА (top-6 по
    // убыванию): rub — его позиция ₽, cap — его % от free-float капы,
    // overhang — его дни выхода (знаменатель у всех общий — оборот месяца,
    // поэтому вклады аддитивны и сумма строк равна значению бара).
    // Фонд без СЧА показать нечем — строка выпадает.
    const hoverBreakdown = useMemo(() => {
        if (hoveredMi == null) return null;
        const cap = ffcap?.[hoveredMi];
        const turn = turnover?.[hoveredMi];
        const rows = funds
            .map(f => {
                const w = f.weights[hoveredMi];
                if (w == null || w === 0) return null;
                const nav = f.navs[hoveredMi];
                if (nav == null || nav <= 0) return null;
                const rub = nav * (w / 100);
                if (shareMode === 'rub') return { label: f.label, color: f.color, v: rub };
                if (shareMode === 'overhang') {
                    return turn != null && turn > 0
                        ? { label: f.label, color: f.color, v: rub / turn }
                        : null;
                }
                return cap != null && cap > 0
                    ? { label: f.label, color: f.color, v: (rub / cap) * 100 }
                    : null;
            })
            .filter((r): r is { label: string; color: string; v: number } => r != null)
            .sort((a, b) => b.v - a.v);
        return { rows, share: shareVals[hoveredMi] };
    }, [hoveredMi, funds, shareVals, shareMode, ffcap, turnover]);

    // Цена hovered месяца — последнее закрытие месяца, то же значение, что
    // рисует превью навигатора. Первой строкой тултипа: доля без ценового
    // контекста не говорит, дорого ли фонды набирали.
    const hoverPrice = useMemo(() => {
        if (!hasPrice || hoveredMi == null) return null;
        const wk = weeksByMonth.get(months[hoveredMi]);
        if (!wk || !wk.length) return null;
        return closesAll[wk[wk.length - 1]] ?? null;
    }, [hasPrice, hoveredMi, months, weeksByMonth, closesAll]);

    // Последние значения видимого окна — для статичных таблеток на правой оси
    // (без курсора). Цена: закрытие последней недели последнего месяца окна;
    // доля/капитал: последний месяц с данными (месяцы без снапшота дают null).
    const lastVisPrice = useMemo(() => {
        if (!hasPrice) return null;
        for (let i = visMonthIdx.length - 1; i >= 0; i--) {
            const wk = weeksByMonth.get(months[visMonthIdx[i]]);
            if (wk && wk.length) return closesAll[wk[wk.length - 1]] ?? null;
        }
        return null;
    }, [hasPrice, visMonthIdx, months, weeksByMonth, closesAll]);

    const lastVisShare = useMemo(() => {
        for (let i = visMonthIdx.length - 1; i >= 0; i--) {
            const v = shareVals[visMonthIdx[i]];
            if (v != null) return v;
        }
        return null;
    }, [visMonthIdx, shareVals]);

    const shareModeLabel = shareMode === 'rub' ? 'Позиция фондов'
        : shareMode === 'overhang' ? 'Навес' : '% в обращении';
    // Значения режима: rub — рубли (адаптив млн/млрд), overhang — дни, cap — %.
    const fmtVal = (v: number) => (shareMode === 'rub' ? fmtRub(v)
        : shareMode === 'overhang' ? fmtDays(v) : fmtPct(v));
    const fmtAxis = (v: number) => (shareMode === 'rub' ? fmtRub(v, false)
        : shareMode === 'overhang' ? fmtDays(v) : fmtPct(v));

    // Ширина бара: 66% слота, но не шире 22 (узкое окно → бары не распухают).
    const barHalf = Math.min((0.33 / visCount) * 1000, 22);

    // Общие CSS-отступы области графика (выравнивание с навигатором).
    const padArea = {
        left: 'var(--chart-pad-left, 100px)',
        right: 'var(--chart-pad-right-single, 95px)',
    } as const;

    // Вертикальный курсор — свой <line> в каждой панели на одном slotX.
    // Нейтральный серый, а не акцент: оранжевым курсор спорил с барами доли и
    // кругляшами сделок, у которых цвет несёт смысл.
    const crosshair = (mi: number) => (
        <line
            x1={slotX(mi)}
            y1="0"
            x2={slotX(mi)}
            y2="1000"
            stroke={CROSSHAIR.color}
            strokeWidth={CROSSHAIR.strokeWidth}
            vectorEffect="non-scaling-stroke"
            strokeDasharray={CROSSHAIR.dashArray}
            style={{ pointerEvents: 'none' }}
        />
    );

    // ─────────────────────────────────────────────────────────────────────
    return (
        <div
            className={bare ? 'relative' : 'rounded-2xl p-5 bg-theme-primary border border-theme relative'}
            style={{ ['--chart-height' as string]: `${height}px` }}
        >
            {noFundsSelected ? (
                <div
                    className="flex flex-col items-center justify-center text-center"
                    style={{ height: 'calc(var(--chart-height, 420px) + 100px)', gap: 'var(--sp-3)', padding: 'var(--sp-6)' }}
                >
                    <div
                        style={{
                            width: 56, height: 56, borderRadius: 12,
                            background: 'var(--accent)', border: '2px solid var(--text-primary)',
                            boxShadow: 'var(--shadow-hard-chip, 3px 3px 0 var(--text-primary))',
                            display: 'flex', alignItems: 'center', justifyContent: 'center',
                            marginBottom: 'var(--sp-2)',
                        }}
                    >
                        <Percent size={28} strokeWidth={2.4} color="#FFFFFF" />
                    </div>
                    <div className="font-semibold text-theme-primary" style={{ fontSize: 'var(--fs-lg)' }}>
                        Не выбрано ни одного фонда
                    </div>
                    <div className="text-theme-secondary" style={{ fontSize: 'var(--fs-sm)', maxWidth: 360 }}>
                        Отметьте фонды в фильтре выше, чтобы увидеть долю бумаги в их портфелях.
                    </div>
                </div>
            ) : loading && !hasData ? (
                <div className="flex items-center justify-center" style={{ height: 'calc(var(--chart-height, 420px) + 100px)' }}>
                    <div className="flex flex-col items-center" style={{ gap: 'var(--sp-3)' }}>
                        <div className="w-8 h-8 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
                        <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-base)' }}>Загрузка...</span>
                    </div>
                </div>
            ) : !hasData ? (
                <div className="flex items-center justify-center text-center" style={{ height: 'calc(var(--chart-height, 420px) + 100px)', color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)' }}>
                    Нет данных за период
                </div>
            ) : (<>
                {loading && (
                    <div className="absolute top-4 left-4 z-20 flex items-center gap-2 rounded-lg border border-theme shadow-md" style={{ background: 'var(--bg-primary)', padding: 'var(--sp-2) var(--sp-3)' }}>
                        <div className="w-4 h-4 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
                        <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-xs)' }}>Обновление...</span>
                    </div>
                )}

                {/* Обёртка обеих секций — общий курсор, тултип и пилюля даты. */}
                {/* Без chart-reveal на обёртке: линия цены рисуется reveal-клипом
                    в своём SVG, бары растут «волной» (dispVals) — оси, пилюли и
                    подписи видны с первого кадра. */}
                <div
                    ref={wrapRef}
                    className="relative cursor-crosshair"
                    style={{ touchAction: 'none' }}
                    onMouseMove={handleMouseMove}
                    onMouseLeave={handleMouseLeave}
                    onTouchStart={(e) => {
                        if (!e.touches[0]) return;
                        const t = e.touches[0];
                        handleMouseMove({ clientX: t.clientX, clientY: t.clientY, currentTarget: e.currentTarget } as React.MouseEvent<HTMLDivElement>);
                    }}
                    onTouchMove={(e) => {
                        if (!e.touches[0]) return;
                        const t = e.touches[0];
                        handleMouseMove({ clientX: t.clientX, clientY: t.clientY, currentTarget: e.currentTarget } as React.MouseEvent<HTMLDivElement>);
                    }}
                    onTouchEnd={handleMouseLeave}
                >
                    {/* ── Верхняя секция: цена (как IMOEX в «Силе рынка»). ──
                        marginTop с вычетом 20px — та же компенсация паддинга
                        карточки (p-5), что в «Столбцах» и «Карте сделок». Именно
                        margin, НЕ padding: отрицательный calc-padding невалиден
                        и молча схлопывается в 0 — легенда стояла на 12px ниже,
                        чем в OI, и прыгала при смене режима. */}
                    {showPricePanel && (
                        <div className="pb-1 border-b border-theme relative overflow-hidden" style={{ marginTop: 'calc(var(--chart-legend-top-gap, 8px) - 20px)' }}>
                            <div className="flex items-center justify-center relative z-10" style={{ marginBottom: 'var(--chart-legend-mb, 2px)' }}>
                                <ChartLegend
                                    items={[{ color: PRICE_LINE_COLOR, label: assetName || 'Цена' }]}
                                    fontWeight={600}
                                    style={{ color: 'var(--text-primary)' }}
                                />
                            </div>
                            <div className="relative" style={{ height: topH }}>
                                {/* Верхний inset — в px (--chart-pad-top), не в процентах
                                    высоты: только так верхняя грид-линия встаёт на том же
                                    расстоянии от легенды, что в OI, на любой высоте панели. */}
                                <div className="absolute" style={{ top: 'var(--chart-pad-top, 14px)', bottom: 0, left: padArea.left, right: padArea.right }}>
                                    <svg ref={priceSvgRef} width="100%" height="100%" viewBox="0 0 1000 1000" preserveAspectRatio="none">
                                        <defs>
                                            {/* Reveal-клип линии цены: ширина rect (юниты
                                                viewBox) анимируется useChartReveal (rAF). */}
                                            <clipPath id="sharePriceRevealClip">
                                                <rect x={0} y={0} width={Math.max(revealW ?? 1000, 0)} height={1000} />
                                            </clipPath>
                                        </defs>
                                        {priceTicks.map((p, i) => (
                                            <line key={`pg-${i}`} x1="0" y1={priceY(p)} x2="1000" y2={priceY(p)} stroke={GRID.major} strokeWidth="1" vectorEffect="non-scaling-stroke" />
                                        ))}
                                        {linePath && (
                                            <g clipPath="url(#sharePriceRevealClip)">
                                                <path
                                                    d={linePath}
                                                    fill="none"
                                                    stroke={PRICE_LINE_COLOR}
                                                    strokeWidth="2"
                                                    vectorEffect="non-scaling-stroke"
                                                    strokeLinecap="round"
                                                    strokeLinejoin="round"
                                                />
                                            </g>
                                        )}
                                        {hoveredMi !== null && crosshair(hoveredMi)}
                                    </svg>
                                </div>
                                {/* Ось Y цены — справа. Тот же верхний inset, что у
                                    плота, иначе подписи разъедутся с грид-линиями. */}
                                <div className="absolute pointer-events-none" style={{ top: 'var(--chart-pad-top, 14px)', bottom: 0, right: 0, width: padArea.right }}>
                                    {priceTicks.map((p, i) => (
                                        <div key={`pl-${i}`} className="absolute" style={{ top: `${priceY(p) / 10}%`, left: 12, transform: 'translateY(-50%)' }}>
                                            <span className="font-semibold" style={{ fontSize: 'var(--chart-font-y, 16px)', color: 'var(--axis-color, #9CA3B8)' }}>
                                                {fmtPrice(p)}
                                            </span>
                                        </div>
                                    ))}

                                    {/* Таблетка цены на оси — только последнее
                                        значение окна; под курсором цену показывает
                                        тултип, дубль на оси не нужен. */}
                                    {(() => {
                                        const v = lastVisPrice;
                                        if (v == null || !(v > 0)) return null;
                                        return (
                                            <ChartAxisPill
                                                value={fmtPrice(v)}
                                                color={PRICE_LINE_COLOR}
                                                topFrac={priceY(v) / 1000}
                                            />
                                        );
                                    })()}
                                </div>
                            </div>
                        </div>
                    )}

                    {/* ── Нижняя секция: гистограмма доли (как Breadth). ──
                        Без цены секция первая в карточке — та же margin-компенсация
                        p-5, что у верхней, чтобы легенда стояла как в OI. */}
                    <div className="relative overflow-hidden" style={showPricePanel ? { paddingTop: 'var(--sp-2)' } : { marginTop: 'calc(var(--chart-legend-top-gap, 8px) - 20px)' }}>
                        {/* Зазор под легендой = зазору над ней (--sp-2 от разделителя):
                            легенда сидит ровно посередине между полосой и графиком. */}
                        <div className="flex items-center justify-center relative z-10" style={{ marginBottom: showPricePanel ? 'var(--sp-2)' : 'var(--chart-legend-mb, 2px)' }}>
                            <ChartLegend
                                items={[{ color: BAR_COLOR, label: `${shareModeLabel} (${shareMode === 'rub' ? '₽' : shareMode === 'overhang' ? 'дни' : '%'})` }]}
                                fontWeight={600}
                                style={{ color: 'var(--text-primary)' }}
                            />
                        </div>
                        <div className="relative" style={{ height: botH + XLABEL_H }}>
                            <div className="absolute" style={{ top: 0, bottom: XLABEL_H, left: padArea.left, right: padArea.right }}>
                                <svg ref={shareSvgRef} width="100%" height="100%" viewBox="0 0 1000 1000" preserveAspectRatio="none">
                                    {shareTicks.map((v, i) => (
                                        <line key={`sg-${i}`} x1="0" y1={shareY(v)} x2="1000" y2={shareY(v)} stroke={GRID.major} strokeWidth="1" vectorEffect="non-scaling-stroke" />
                                    ))}
                                    {/* Бары: высоты из dispVals (анимированные значения);
                                        0 → пустой слот (пропуск/разрыв данных). */}
                                    {visMonthIdx.map((mi) => {
                                        const v = dispVals[mi] ?? 0;
                                        if (v <= 0) return null;
                                        const x = slotX(mi);
                                        const yTop = shareY(v);
                                        const dim = hoveredMi !== null && hoveredMi !== mi;
                                        return (
                                            <rect
                                                key={mi}
                                                x={x - barHalf}
                                                y={yTop}
                                                width={barHalf * 2}
                                                height={1000 - yTop}
                                                fill={BAR_COLOR}
                                                opacity={dim ? 0.45 : 0.92}
                                            />
                                        );
                                    })}
                                    {hoveredMi !== null && crosshair(hoveredMi)}
                                </svg>
                            </div>
                            {/* Ось Y доли — справа. */}
                            <div className="absolute pointer-events-none" style={{ top: 0, bottom: XLABEL_H, right: 0, width: padArea.right }}>
                                {shareTicks.map((v, i) => (
                                    <div key={`sl-${i}`} className="absolute" style={{ top: `${shareY(v) / 10}%`, left: 12, transform: 'translateY(-50%)' }}>
                                        <span className="font-semibold" style={{ fontSize: 'var(--chart-font-y, 16px)', color: 'var(--axis-color, #9CA3B8)' }}>
                                            {fmtAxis(v)}
                                        </span>
                                    </div>
                                ))}

                                {/* Таблетка режима на оси — только последнее
                                    значение окна. Цвет баров, формат оси. */}
                                {(() => {
                                    const v = lastVisShare;
                                    if (v == null) return null;
                                    return (
                                        <ChartAxisPill
                                            value={fmtAxis(v)}
                                            color={BAR_COLOR}
                                            topFrac={shareY(v) / 1000}
                                        />
                                    );
                                })()}
                            </div>
                            {/* Подписи оси X — по видимым месяцам. */}
                            <div className="absolute flex justify-between font-semibold px-2" style={{ bottom: 8, left: padArea.left, right: padArea.right, fontSize: 'var(--chart-font-x, 14px)', color: 'var(--axis-color, #9CA3B8)' }}>
                                {(() => {
                                    const vis = visMonthIdx.map(mi => months[mi]);
                                    if (!vis.length) return null;
                                    const tickCount = Math.min(isMobile ? 3 : 6, vis.length);
                                    return Array.from({ length: tickCount }, (_, i) => {
                                        const idx = Math.min(Math.round(i * (vis.length - 1) / Math.max(tickCount - 1, 1)), vis.length - 1);
                                        if (!vis[idx]) return null;
                                        return (
                                            <span key={i}>{new Date(`${vis[idx]}-01T00:00:00`).toLocaleDateString('ru-RU', { month: 'short', year: '2-digit' })}</span>
                                        );
                                    });
                                })()}
                            </div>
                            {/* Watermark — в нижней панели, как в гистограммах.
                                В окне терминала (bare) не рисуем: панель тесная,
                                знак ложится прямо на данные. */}
                            {!bare && <ChartWatermark
                                left={`calc(${padArea.left} + 5px)`}
                                bottom={`${XLABEL_H + 5}px`}
                            />}
                        </div>
                    </div>

                    {/* Тултип: доля + сделки месяца + разбивка по фондам. */}
                    {hoveredMi !== null && tooltipPos && hoverBreakdown && hoverBreakdown.share != null && (() => {
                        const MAX_ROWS = 6;
                        const shown = hoverBreakdown.rows.slice(0, MAX_ROWS);
                        const extra = hoverBreakdown.rows.length - shown.length;
                        // cardStyle: вертикальный padding поднят до --sp-2 — у
                        // карточки с шапкой-ценой дефолтный --sp-1 прижимал первую
                        // строку к верхней грани.
                        return (
                            <ChartTooltip
                                x={tooltipPos.x}
                                y={tooltipPos.y}
                                clampTop={overlayGeom?.topLineY ?? cssVar('--chart-pad-top', 14)}
                                clampBottom={XLABEL_H}
                                cardStyle={{ padding: 'var(--sp-2)' }}
                            >
                                {/* Шапка: цена бумаги и итоговая доля — две строки
                                    одного веса, каждая со своей точкой цвета своей
                                    серии (линия цены / бары долей). Ниже разделителя
                                    всё про отдельные фонды. */}
                                <div style={{ marginBottom: 'var(--sp-1)', paddingBottom: 'var(--sp-1)', borderBottom: '1px solid var(--border-color)' }}>
                                    {hoverPrice != null && (
                                        <TooltipRow
                                            color={PRICE_LINE_COLOR}
                                            label={assetName || 'Цена'}
                                            value={`${fmtPrice(hoverPrice)} ₽`}
                                            labelClass="font-bold"
                                            labelColor="var(--text-primary)"
                                            valueColor="var(--text-primary)"
                                        />
                                    )}
                                    <TooltipRow
                                        color={BAR_COLOR}
                                        label={shareModeLabel}
                                        value={fmtVal(hoverBreakdown.share)}
                                        labelClass="font-bold"
                                        labelColor="var(--text-primary)"
                                        valueColor="var(--text-primary)"
                                    />
                                </div>
                                {shown.length > 0 && (
                                    // Своего разделителя у блока фондов нет — его
                                    // отрезает разделитель шапки прямо над ним.
                                    <div>
                                        {shown.map((r, i) => (
                                            <TooltipRow
                                                key={i}
                                                hideDot
                                                color={r.color}
                                                label={r.label}
                                                value={fmtVal(r.v)}
                                                labelClass="font-semibold"
                                                labelColor="var(--axis-color, #9CA3B8)"
                                                valueColor="var(--axis-color, #9CA3B8)"
                                            />
                                        ))}
                                        {extra > 0 && (
                                            <div className="text-theme-secondary" style={{ fontSize: 'var(--fs-2xs)', marginTop: 'var(--sp-1)' }}>
                                                и ещё {extra} {extra === 1 ? 'фонд' : extra >= 2 && extra <= 4 ? 'фонда' : 'фондов'}
                                            </div>
                                        )}
                                    </div>
                                )}
                            </ChartTooltip>
                        );
                    })()}

                    {/* Плавающая пилюля даты — hovered месяц, над верхней панелью. */}
                    {hoveredMi !== null && overlayGeom && months[hoveredMi] && (
                        <ChartDatePill
                            date={monthLabel(months[hoveredMi])}
                            x={overlayGeom.plotLeft + (slotX(hoveredMi) / 1000) * overlayGeom.plotWidth}
                            topLineY={overlayGeom.topLineY}
                            minX={overlayGeom.plotLeft}
                            maxX={overlayGeom.plotLeft + overlayGeom.plotWidth}
                        />
                    )}
                </div>

                {/* Навигатор: превью — линия цены (или доли, если цены нет). */}
                {navigatorData.length > 1 && (
                    <div data-export-ignore="true">
                        <ChartNavigator
                            data={navigatorData}
                            color={hasPrice ? PRICE_LINE_COLOR : BAR_COLOR}
                            previewMode="line"
                            onChange={(s, e) => setNavRange([s, e])}
                            insetLeft="var(--chart-pad-left)"
                            insetRight="var(--chart-pad-right-single)"
                        />
                    </div>
                )}
            </>)}
        </div>
    );
}
