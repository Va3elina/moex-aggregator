/**
 * FundDetailModal — детальная карточка фонда, собранная композицией уже
 * существующих на сайте блоков:
 *   - СЧА крупно + график «Цена пая, ₽» (SimpleChart) + плашки returns;
 *   - «Приток и отток денег» — гистограмма CompanyFlowsHistogram (та же, что
 *     в «Потоках по компании») поверх /funds/flows с fund_ids по одному фонду;
 *   - «Состав фонда» — Donut + список бумаг в стиле «Обзора портфеля»
 *     (логотип · имя · полоса · доля · объём, топ-10 + разворот);
 *   - «Что купили и продали» — diff месячных снапшотов (new/accumulated/
 *     reduced/sold_out) из того же ответа detail.
 *
 * Контракт обобщён: карточка принимает `loadDetail()` от родителя (замыкает
 * ticker/period/id), а СЧА/returns/has_distributions берутся из ответа detail,
 * иначе из пропов.
 *
 * `enableDrilldown`:
 *   - true  → состав + покупки/продажи + клик в актив открывает
 *             AssetHistoryModal («как фонд покупал бумагу»).
 *   - false → БАЗОВАЯ карточка: шапка + СЧА + доходность + притоки-оттоки.
 */
import { useEffect, useMemo, useRef, useState, type CSSProperties } from 'react';
import { Calendar } from 'lucide-react';
import {
    getAssetHistory,
    getFundsFlows,
    type FundTradesDetail,
    type FundTradesPeriod,
    type FundReturns,
    type AssetHistory,
    type FundsFlowsResponse,
    type FundCategory,
    type FundPeriod,
} from '../../services/api';
import SimpleChart, { type ChartAnnotation } from '../SimpleChart';
import SegmentedControl from '../SegmentedControl';
import MonthRangePicker, { monthRangeLabel, type MonthRange } from '../fundtrades/MonthRangePicker';
import ChartCaptureButton from '../export/ChartCaptureButton';
import CompanyFlowsHistogram from '../fundtrades/CompanyFlowsHistogram';
import { DONUT_COLORS, assetColor, resolveFundTicker, fundAssetName, fundAssetColor, isOfzBond } from '../../config/fundConfig';
import InstrumentIcon from '../InstrumentIcon';
import Donut from './Donut';
import { useViewportWidth } from '../../hooks/useViewportWidth';
import { useViewportHeight } from '../../hooks/useViewportHeight';

// ════════════════════════════════════════════════════════════════════
// Shared formatters (экспортируются — переиспользуются в FundTradesPage)
// ════════════════════════════════════════════════════════════════════

export function formatRubShort(amount: number | null): string {
    if (amount === null || amount === undefined) return '—';
    const abs = Math.abs(amount);
    if (abs >= 1e9) return `${(amount / 1e9).toFixed(2)} млрд ₽`;
    if (abs >= 1e6) return `${(amount / 1e6).toFixed(1)} млн ₽`;
    if (abs >= 1e3) return `${(amount / 1e3).toFixed(0)} тыс ₽`;
    return `${amount.toFixed(0)} ₽`;
}

export function formatShares(positions: number | null): string {
    if (positions === null || positions === undefined) return '—';
    return positions.toLocaleString('ru-RU');
}

// Доходность в %: «+12.3%» / «−4.1%» / «—». Знак «−» — типографский минус.
export function formatReturnPct(v: number | null | undefined): string {
    if (v === null || v === undefined) return '—';
    const sign = v > 0 ? '+' : v < 0 ? '−' : '';
    return `${sign}${Math.abs(v).toFixed(1)}%`;
}

// Цвет доходности: зелёный для роста, красный для падения, нейтральный для 0/—.
export function returnColor(v: number | null | undefined): string {
    if (v === null || v === undefined || v === 0) return 'var(--text-muted)';
    return v > 0 ? 'var(--funds-flow-positive)' : 'var(--funds-flow-negative)';
}

function formatSnapshotDate(iso: string): string {
    // 2026-04-30 → "30 апр 2026"
    const d = new Date(iso);
    const months = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек'];
    return `${d.getDate()} ${months[d.getMonth()]} ${d.getFullYear()}`;
}

// "2025-08-29" → "авг 2025" — компактный месяц+год для оси и тултипа графика.
function formatMonthYearShort(iso: string): string {
    const d = new Date(iso);
    const mm = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек'];
    return `${mm[d.getMonth()]} ${d.getFullYear()}`;
}

// ════════════════════════════════════════════════════════════════════
// Список бумаг в стиле «Обзора портфеля» (CombinedPortfolioView):
// fade-обрезка имени, компактный объём, логотип с фолбэком-кругом.
// ════════════════════════════════════════════════════════════════════

// Длинное имя обрезаем плавным затуханием справа (fade-out), не многоточием.
const FADE_MASK = 'linear-gradient(to right, #000 0, #000 calc(100% - 20px), transparent 100%)';
const nameFade: CSSProperties = {
    overflow: 'hidden', whiteSpace: 'nowrap',
    maskImage: FADE_MASK, WebkitMaskImage: FADE_MASK,
};

// Компактный объём: «12.7 млрд», «540 млн», «12 тыс» (без ₽ — единица в шапке).
function fmtVolShort(v: number): string {
    if (v >= 1e9) return `${(v / 1e9).toFixed(1)} млрд`;
    if (v >= 1e6) return `${(v / 1e6).toFixed(0)} млн`;
    return `${Math.round(v / 1e3)} тыс`;
}

// Логотип бумаги: InstrumentIcon по резолвнутому тикеру; ОФЗ — иконка RB;
// иначе цветной круг (тот же фолбэк, что в таблице состава раньше).
function HoldingLogo({ name, isin, idx, size }: { name: string; isin: string | null; idx: number; size: number }) {
    const tk = resolveFundTicker(name, isin);
    if (tk) return <InstrumentIcon sectype={tk} size={size} rounded="full" />;
    if (isOfzBond(name)) return <InstrumentIcon sectype="RB" size={size} rounded="full" />;
    return (
        <span style={{
            width: size, height: size, borderRadius: '50%', flexShrink: 0, display: 'inline-block',
            backgroundColor: fundAssetColor(name, isin) ?? DONUT_COLORS[idx % DONUT_COLORS.length],
        }} />
    );
}

// Тип изменения состава — теперь только подпись в тултипе строки (бейджи ушли
// вместе с редизайном раздела под «Сделки» из «Общего портфеля»).
const CHANGE_META: Record<string, { label: string }> = {
    new:         { label: 'Новая' },
    accumulated: { label: 'Докупили' },
    reduced:     { label: 'Сократили' },
    sold_out:    { label: 'Продана' },
};

// ════════════════════════════════════════════════════════════════════
// Сплит-коррекция позиций (используется AssetHistoryContent)
// ════════════════════════════════════════════════════════════════════

// Детект сплита акций в истории позиций + back-adjustment (как экспирация/ролловер
// фьючерсов на OI). Сплит: количество ×R, цена ÷R, СТОИМОСТЬ непрерывна (amount ≈ const)
// — именно это отличает сплит от реальной покупки (там сумма растёт ~×R). Без коррекции
// один снапшот после сплита «выстреливает» в N× и сплющивает историю. Возвращаем
// множитель на дату (история домножается до текущего масштаба, последние = raw) +
// маркеры «Сплит» для SimpleChart.
// Стандартные коэффициенты сплита. Берём ближайший (в лог-шкале) к наблюдаемому,
// а НЕ сырое отношение количеств — иначе реальная торговля на границе сплита
// «съедается» в ноль (множитель ровно подгонял бы pre к post). T 1:10: наблюдаем
// ~10.13 (шум от продажи фонда + движения цены) → берём 10, и Δ показывает реальный сдвиг.
const SPLIT_RATIOS = [2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 25, 30, 40, 50, 100, 150, 200, 500, 1000];
function nearestSplitRatio(observed: number): number {
    const cands = observed >= 1 ? SPLIT_RATIOS : SPLIT_RATIOS.map((r) => 1 / r);
    let best = cands[0], bestD = Infinity;
    for (const c of cands) {
        const d = Math.abs(Math.log(observed) - Math.log(c));
        if (d < bestD) { bestD = d; best = c; }
    }
    return best;
}

function splitAdjustPositions(
    timeline: AssetHistory['timeline'],
): { factorByDate: Map<string, number>; annotations: ChartAnnotation[] } {
    const chrono = [...timeline].sort((a, b) => a.snapshot_date.localeCompare(b.snapshot_date));
    const factorByDate = new Map<string, number>();
    const splits: { date: string; ratio: number }[] = [];
    const splitRatio = (a: AssetHistory['timeline'][number], b: AssetHistory['timeline'][number]): number => {
        if (!a.positions || !b.positions || !a.price_rub || !b.price_rub) return 0;
        const posR = b.positions / a.positions;
        const priceR = a.price_rub / b.price_rub;
        const amtR = a.amount_rub && b.amount_rub ? b.amount_rub / a.amount_rub : 1;
        // value непрерывна + кол-во и цена двигаются согласованно (posR ≈ priceR)
        const ok = Math.abs(amtR - 1) < 0.4 && Math.abs(posR / priceR - 1) < 0.4;
        // Множитель = ближайший стандартный коэффициент к geomean(posR,priceR)
        // (geomean балансирует шум от торговли фонда и движения цены).
        if (ok && posR > 1.8 && priceR > 1.8) return nearestSplitRatio(Math.sqrt(posR * priceR));   // прямой
        if (ok && posR < 0.55 && priceR < 0.55) return nearestSplitRatio(Math.sqrt(posR * priceR)); // обратный
        return 0;
    };
    let factor = 1;
    for (let i = chrono.length - 1; i >= 0; i--) {
        factorByDate.set(chrono[i].snapshot_date, factor);
        if (i > 0) {
            const R = splitRatio(chrono[i - 1], chrono[i]);
            if (R) { splits.push({ date: chrono[i].snapshot_date, ratio: R }); factor *= R; }
        }
    }
    const annotations: ChartAnnotation[] = splits.map((s) => {
        const n = s.ratio >= 1 ? Math.round(s.ratio) : Math.round(1 / s.ratio);
        return {
            time: s.date,
            label: 'Сплит',
            description: `Сплит акций ~1:${n}${s.ratio < 1 ? ' (обратный)' : ''} · ${formatMonthYearShort(s.date)}. Кол-во и цена в графике/таблице скорректированы под сплит.`,
            color: 'var(--accent)',
            textColor: 'var(--text-inverse)',
        };
    });
    return { factorByDate, annotations };
}

// ════════════════════════════════════════════════════════════════════
// Fund Detail Modal — объём + график доходности + (опц.) donut состава
// ════════════════════════════════════════════════════════════════════

// Пресеты периода diff'а — как у «Сделок» в «Общем портфеле» (1М/6М/1Г).
type MoversDiffPeriod = Extract<FundTradesPeriod, '1m' | '6m' | '1y'>;

interface FundDetailModalProps {
    ticker: string;
    // Лоадер от родителя (замыкает ticker/id). Период diff'а («Что купили и
    // продали») выбирается ВНУТРИ модалки (1М/6М/1Г) и передаётся аргументом;
    // лоадеры без периодов (напр. getFundsDetail) аргумент просто игнорируют.
    loadDetail: (period: FundTradesPeriod, range: MonthRange | null) => Promise<FundTradesDetail>;
    navRub?: number | null;        // fallback для СЧА (если нет в detail.fund)
    returns?: FundReturns | null;  // fallback для плашек (если нет в detail.performance)
    hasDistributions?: boolean;    // фонд платит доход → пояснение под графиком
    enableDrilldown: boolean;      // true → состав + клик в актив; false → базовая карточка
    onClose: () => void;
}

export default function FundDetailModal({
    ticker,
    loadDetail,
    navRub,
    returns,
    hasDistributions,
    enableDrilldown,
    onClose,
}: FundDetailModalProps) {
    const [data, setData] = useState<FundTradesDetail | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    // (G) drill-down на бумагу: «как этот фонд/УК покупал актив» (тот же
    // AssetHistoryModal, что в «Обзоре снапшота»). Открывается кликом по
    // сектору пончика ИЛИ по строке списка состава. Только при enableDrilldown.
    const [drillDown, setDrillDown] = useState<{ asset_name: string; isin: string | null } | null>(null);
    // Hover-связь пончик↔список состава (индекс позиции в holds).
    const [modalHover, setModalHover] = useState<number | null>(null);
    // Развороты списков: состав (топ-10 → все) и изменения (топ-10 → все).
    const [showAllHoldings, setShowAllHoldings] = useState(false);
    const [showAllDiff, setShowAllDiff] = useState(false);
    // Период diff'а «Что купили и продали» — сегменты 1М/6М/1Г в шапке раздела
    // (те же пресеты, что у «Сделок» в «Общем портфеле»). Смена периода
    // перезагружает detail; старый контент остаётся на экране (см. эффект ниже).
    const [diffPeriod, setDiffPeriod] = useState<MoversDiffPeriod>('1m');
    // Произвольный диапазон месяцев (кнопка-календарь справа от пресетов) — тот же
    // MonthRangePicker, что в «Сделках» Общего портфеля. Задан → пресет игнорируется.
    const [diffRange, setDiffRange] = useState<MonthRange | null>(null);

    // График цены пая — номинал в рублях по всей истории. Проценты пробовали
    // (нормировка к первой точке), но шкала доходности здесь не нужна: движение
    // читают по самой цене, а срезы доходности дают плашки под графиком.
    // Мемоизируем по data, чтобы hover доната НЕ пересоздавал массив →
    // SimpleChart не сбрасывал зум навигатора на каждый ре-рендер.
    const payChartData = useMemo(
        () => (data?.performance?.timeline ?? [])
            .filter((p) => p.pay != null)
            .map((p) => ({ time: p.date, value: p.pay as number })),
        [data],
    );

    // Притоки-оттоки этого фонда: /funds/flows умеет fund_ids → помесячный срез
    // по одному фонду. Net flow там очищен от рыночного роста пая, т.е. бары —
    // именно «занесли/забрали деньги», а не переоценка. Период — каскад
    // 'all' → '3y' → '1y': длинные окна могут быть закрыты тарифом (403), тогда
    // пробуем короче; совсем не вышло → блок не показывается.
    const [flows, setFlows] = useState<FundsFlowsResponse | null>(null);
    const [flowsLoading, setFlowsLoading] = useState(false);
    const fundId = data?.fund?.fund_id;
    const fundCategory = data?.fund?.category;
    useEffect(() => {
        if (!fundId || !fundCategory) return;
        let cancelled = false;
        setFlows(null);
        setFlowsLoading(true);
        (async () => {
            for (const p of ['all', '3y', '1y'] as FundPeriod[]) {
                try {
                    const r = await getFundsFlows(fundCategory as FundCategory, '1m', p, [fundId]);
                    if (!cancelled) setFlows(r);
                    return;
                } catch { /* период недоступен — пробуем короче */ }
            }
        })().finally(() => { if (!cancelled) setFlowsLoading(false); });
        return () => { cancelled = true; };
    }, [fundId, fundCategory]);

    const flowMonths = useMemo(
        () => flows?.flows.map((f) => f.period_end.slice(0, 7)) ?? [],
        [flows],
    );
    // Значения приходят в млрд ₽ → в ₽ (CompanyFlowsHistogram сам делит на 1e6
    // и подписывает «млн ₽» — масштаб потоков одного фонда как раз миллионный).
    const flowSeries = useMemo(
        () => flows
            ? [{ label: 'Чистый поток', color: 'var(--accent)', values: flows.flows.map((f) => f.flow * 1e9) }]
            : [],
        [flows],
    );

    // loadDetail обычно — inline-замыкание от родителя (новая ссылка на каждый
    // ре-рендер). Держим его в ref и перезапускаем загрузку ТОЛЬКО при смене
    // ticker/периода diff'а — иначе любой ре-рендер родителя ресетил бы loading +
    // сбрасывал зум навигатора графика (полированное поведение «Покупок фондов»).
    // Смена периода diff'а НЕ прячет контент (data остаётся до прихода нового
    // ответа — только раздел изменений слегка тускнеет); смена тикера — прячет.
    const loadDetailRef = useRef(loadDetail);
    loadDetailRef.current = loadDetail;
    const prevTickerRef = useRef(ticker);
    useEffect(() => {
        let cancelled = false;
        if (prevTickerRef.current !== ticker) {
            prevTickerRef.current = ticker;
            setData(null);
        }
        setLoading(true);
        setError(null);
        loadDetailRef.current(diffPeriod, diffRange)
            .then((d) => { if (!cancelled) setData(d); })
            .catch((e: Error) => { if (!cancelled) setError(e.message); })
            .finally(() => { if (!cancelled) setLoading(false); });
        return () => { cancelled = true; };
    }, [ticker, diffPeriod, diffRange]);

    // Календарь диапазона живёт на месяцах последнего успешного ответа: пока
    // грузится новый период, список не должен схлопываться.
    const diffMonthsRef = useRef<string[]>([]);
    if (data?.available_months?.length) diffMonthsRef.current = data.available_months;
    const diffMonths = diffMonthsRef.current;

    // СЧА/returns/has_distributions: из ответа detail если есть, иначе из пропов.
    // detail.fund в текущем типе не несёт nav_rub/has_distributions — читаем мягко
    // (на случай, если backend их добавит), с фолбэком на пропы от родителя.
    const detailFund = data?.fund as
        | (FundTradesDetail['fund'] & { nav_rub?: number | null; has_distributions?: boolean })
        | undefined;
    const navValue = detailFund?.nav_rub ?? navRub ?? null;
    // СЧА показываем в центре пончика состава, если пончик вообще есть. Иначе
    // (базовая карточка / состав не публикуется) — блоком в шапке карточки.
    const navInDonut = enableDrilldown && navValue != null && (data?.current_holdings.length ?? 0) > 0;
    // Мобильная адаптация. Модал шарится с десктопом («Деньги в фондах»),
    // поэтому размеры реактивные: на мобиле пончик ужимаем под ширину экрана
    // (иначе size:380 вылезал за правый край), график ниже, паддинги меньше,
    // высота в dvh (iOS не режет верх/низ динамическим тулбаром).
    const vw = useViewportWidth();
    const vh = useViewportHeight();
    const isMobile = vw < 768;
    const hasDist = detailFund?.has_distributions ?? hasDistributions ?? false;

    return (
        <>
        <div
            onClick={onClose}
            style={{
                position: 'fixed',
                inset: 0,
                background: 'rgba(0, 0, 0, 0.5)',
                zIndex: 1000,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                padding: isMobile ? 4 : 16,
            }}
        >
            <div
                onClick={(e) => e.stopPropagation()}
                style={{
                    background: 'var(--bg-primary)',
                    border: '1.5px solid var(--text-primary)',
                    borderRadius: 14,
                    width: '100%',
                    maxWidth: 1240,
                    maxHeight: isMobile ? '94dvh' : '92vh',
                    overflow: 'auto',
                    boxShadow: '0 10px 40px rgba(0,0,0,0.3)',
                }}
            >
                {/* Header */}
                <div
                    style={{
                        padding: '16px 20px',
                        borderBottom: '1px solid var(--border-color)',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                        position: 'sticky',
                        top: 0,
                        background: 'var(--bg-primary)',
                        zIndex: 1,
                    }}
                >
                    {/* Флекс по базовой линии: имя, тикер и дата среза сидят на одной
                        строке текста, а не «прыгают» по вертикали из-за разных кеглей
                        и иконки календаря. */}
                    <h2
                        style={{
                            margin: 0,
                            display: 'flex',
                            alignItems: 'baseline',
                            flexWrap: 'wrap',
                            columnGap: 10,
                            rowGap: 2,
                            minWidth: 0,
                            fontSize: 'var(--fs-lg)',
                            fontWeight: 800,
                            color: 'var(--text-primary)',
                        }}
                    >
                        <span style={{ minWidth: 0 }}>{data?.fund.name || data?.fund.ticker || ticker}</span>
                        {data?.fund.name && (data?.fund.ticker || ticker) && (
                            <span
                                style={{
                                    fontWeight: 400,
                                    fontSize: 'var(--fs-sm)',
                                    fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace',
                                    color: 'var(--text-secondary)',
                                }}
                            >
                                {data?.fund.ticker || ticker}
                            </span>
                        )}
                        {/* Дата среза состава — здесь же, в шапке-легенде после тикера.
                            Иконка выровнена по тексту (translateY), сам спан — по baseline. */}
                        {data?.current_snapshot_date && (
                            <span
                                style={{
                                    fontWeight: 600,
                                    fontSize: 'var(--fs-2xs)',
                                    color: 'var(--text-muted)',
                                    whiteSpace: 'nowrap',
                                }}
                            >
                                <Calendar size={12} style={{ display: 'inline-block', verticalAlign: '-0.15em', marginRight: 5 }} />
                                Состав на {formatSnapshotDate(data.current_snapshot_date)}
                            </span>
                        )}
                    </h2>
                    <button
                        onClick={onClose}
                        style={{
                            background: 'transparent',
                            border: 'none',
                            color: 'var(--text-secondary)',
                            cursor: 'pointer',
                            fontSize: 20,
                            padding: 12,
                            margin: -8,
                        }}
                    >
                        ✕
                    </button>
                </div>

                <div style={{ padding: isMobile ? 12 : 20 }}>
                    {loading && !data && <div style={{ color: 'var(--text-muted)' }}>Загружаем…</div>}
                    {error && <div style={{ color: 'var(--danger, #ef4444)' }}>{error}</div>}
                    {data && !error && (
                        <>
                            {/* (2) Объём — полная СЧА (AUM). Когда есть пончик состава,
                                значение живёт в его центре и блок не рендерится вовсе
                                (дата среза переехала в шапку карточки). */}
                            {!navInDonut && (
                            <div style={{ marginBottom: 20 }}>
                                <div
                                    style={{
                                        fontSize: 'var(--fs-2xs)',
                                        color: 'var(--text-muted)',
                                        textTransform: 'uppercase',
                                        letterSpacing: '0.06em',
                                        fontWeight: 700,
                                        marginBottom: 2,
                                    }}
                                >
                                    Объём (СЧА)
                                </div>
                                <div
                                    style={{
                                        fontSize: 'var(--fs-2xl)',
                                        fontWeight: 800,
                                        fontVariantNumeric: 'tabular-nums',
                                        color: 'var(--text-primary)',
                                        lineHeight: 1.1,
                                    }}
                                >
                                    {navValue != null ? formatRubShort(navValue) : '—'}
                                </div>
                            </div>
                            )}

                            {/* (3) График цены пая (₽) + плашки доходности по периодам */}
                            {(() => {
                                const perf = data.performance;
                                const ret = perf?.returns ?? returns ?? null;
                                const chartData = payChartData; // мемоизирован выше (стабильная ссылка)
                                return (
                                    <div style={{ marginBottom: 24 }}>
                                        {/* Заголовка у раздела нет: график читается сам —
                                            легенда «Цена пая, ₽» + плашки доходности под ним. */}
                                        {/* Мобила: график во всю ширину карточки (компенсируем
                                            боковой паддинг тела −12) — «по шире», + выше («по больше»). */}
                                        <div style={{ marginLeft: isMobile ? -12 : 0, marginRight: isMobile ? -12 : 0 }}>
                                        {chartData.length > 1 ? (
                                            <SimpleChart
                                                data={chartData}
                                                height={Math.max(220, Math.min(isMobile ? 340 : 460, vh - 240))}
                                                primaryLabel="Цена пая, ₽"
                                                legendPosition="top"
                                                formatValue={(v) => `${v.toLocaleString('ru-RU', { maximumFractionDigits: 2 })} ₽`}
                                                formatPrimaryAxis={(v) => v.toLocaleString('ru-RU', { maximumFractionDigits: v >= 100 ? 0 : 2 })}
                                                formatTime={formatMonthYearShort}
                                                tooltipDateFormat={formatMonthYearShort}
                                                clampEdgeLabels
                                                mobilePadRight={isMobile ? 14 : undefined}
                                                showValueHeader={false}
                                                showDownloadButton={false}
                                                showNavigator={!isMobile}
                                            />
                                        ) : (
                                            <div>
                                                <div
                                                    style={{
                                                        padding: '24px 16px',
                                                        textAlign: 'center',
                                                        color: 'var(--text-muted)',
                                                        fontSize: 'var(--fs-sm)',
                                                        border: '1px solid var(--border-color)',
                                                        borderRadius: 10,
                                                        background: 'var(--bg-secondary)',
                                                    }}
                                                >
                                                    Недостаточно истории для графика цены пая
                                                </div>
                                            </div>
                                        )}
                                        </div>

                                        {/* Плашки returns 1м/3м/6м/1г + «за всё время».
                                            Пустые периоды (фонд младше периода) прячем — иначе
                                            у молодых фондов сплошные «—». «За всё время» (с первого
                                            дня данных) есть всегда → заполняет молодые фонды. */}
                                        <div
                                            style={{
                                                display: 'grid',
                                                gridTemplateColumns: 'repeat(auto-fit, minmax(72px, 1fr))',
                                                gap: 8,
                                                marginTop: 12,
                                            }}
                                        >
                                            {[
                                                { label: '1 мес', v: ret?.m1 },
                                                { label: '3 мес', v: ret?.m3 },
                                                { label: '6 мес', v: ret?.m6 },
                                                { label: '1 год', v: ret?.y1 },
                                                { label: 'Всё время', v: ret?.all },
                                            ].filter(({ v }) => v != null).map(({ label, v }) => (
                                                <div
                                                    key={label}
                                                    style={{
                                                        padding: '10px 12px',
                                                        background: 'var(--bg-secondary)',
                                                        borderRadius: 8,
                                                        border: '1px solid var(--border-color)',
                                                    }}
                                                >
                                                    <div
                                                        style={{
                                                            fontSize: 'var(--fs-2xs)',
                                                            color: 'var(--text-muted)',
                                                            textTransform: 'uppercase',
                                                            letterSpacing: '0.04em',
                                                            fontWeight: 600,
                                                            marginBottom: 3,
                                                        }}
                                                    >
                                                        {label}
                                                    </div>
                                                    <div
                                                        style={{
                                                            fontSize: 'var(--fs-md)',
                                                            fontWeight: 800,
                                                            fontVariantNumeric: 'tabular-nums',
                                                            color: returnColor(v),
                                                        }}
                                                    >
                                                        {formatReturnPct(v)}
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                        {hasDist && (
                                            <div style={{ marginTop: 10, fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', lineHeight: 1.4 }}>
                                                Плашки — полная доходность, с&nbsp;учётом выплат дохода.
                                                Линия графика — цена пая: в&nbsp;даты выплат она снижается.
                                            </div>
                                        )}
                                    </div>
                                );
                            })()}

                            {/* (3b) Приток и отток денег инвесторов — /funds/flows по одному
                                фонду. Гистограмма — тот же самодостаточный
                                CompanyFlowsHistogram, что в «Потоках по компании». Это
                                принципиально другая метрика, чем доходность: чистый поток
                                денег, очищенный от роста рынка. */}
                            {(flowsLoading || flowMonths.length > 1) && (
                                <div style={{ marginBottom: 24 }}>
                                    <h3
                                        style={{
                                            fontSize: 'var(--fs-md)',
                                            fontWeight: 700,
                                            color: 'var(--text-primary)',
                                            marginBottom: 4,
                                        }}
                                    >
                                        Приток и отток денег
                                    </h3>
                                    <div style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', marginBottom: 10, lineHeight: 1.4 }}>
                                        Сколько денег инвесторы занесли в фонд и забрали из него по месяцам.
                                        Рост рынка вычтен — это не доходность, а именно движение денег.
                                    </div>
                                    <div style={{ marginLeft: isMobile ? -12 : 0, marginRight: isMobile ? -12 : 0 }}>
                                        <CompanyFlowsHistogram
                                            months={flowMonths}
                                            series={flowSeries}
                                            title="Чистый приток и отток денег (млн ₽)"
                                            height={isMobile ? 260 : 320}
                                            loading={flowsLoading}
                                            animTrigger={ticker}
                                            tooltipLabels={{ pos: 'Приток', neg: 'Отток' }}
                                        />
                                    </div>
                                </div>
                            )}

                            {/* (4) Donut состава + список топ-позиций — только при enableDrilldown */}
                            {enableDrilldown && (
                                <>
                                    <h3
                                        style={{
                                            fontSize: 'var(--fs-md)',
                                            fontWeight: 700,
                                            color: 'var(--text-primary)',
                                            marginBottom: 12,
                                        }}
                                    >
                                        Состав фонда
                                    </h3>
                                    {data.current_holdings.length > 0 ? (() => {
                                        // (G) holdings пончика = топ-10 + «Прочее»; colors — параллельный
                                        // массив (фирменный/индекс, «Прочее» серый). maxSlices = длине →
                                        // Donut НЕ агрегирует сам, индекс слайса 1:1 совпадает с массивом,
                                        // поэтому onSliceClick(i) корректно мапится на бумагу.
                                        // TOP = 10 — синхронно с превью списка: сектора пончика 1:1
                                        // соответствуют видимым строкам (hover-связь без «слепых» зон).
                                        const holds = data.current_holdings;
                                        const TOP = 10;
                                        const topHolds = holds.slice(0, TOP);
                                        const restWeight = holds.slice(TOP).reduce((s, h) => s + (h.weight ?? 0), 0);
                                        const donutHoldings = [
                                            ...topHolds.map((h) => ({ name: h.asset_name, weight: (h.weight ?? 0) / 100 })),
                                            ...(restWeight > 0 ? [{ name: 'Прочее', weight: restWeight / 100 }] : []),
                                        ];
                                        const donutColors = donutHoldings.map((h, i) =>
                                            h.name === 'Прочее'
                                                ? 'var(--text-muted)'
                                                : (assetColor(h.name) ?? DONUT_COLORS[i % DONUT_COLORS.length]),
                                        );
                                        const isSelected = (h: typeof holds[number]) =>
                                            drillDown != null
                                            && drillDown.asset_name === h.asset_name
                                            && (drillDown.isin ?? null) === (h.isin ?? null);
                                        const openAsset = (h: typeof holds[number]) =>
                                            setDrillDown({ asset_name: h.asset_name, isin: h.isin ?? null });
                                        // Список в стиле «Обзора портфеля»: логотип · имя (fade) ·
                                        // нейтральная полоса · доля · объём. Цвет несёт пончик.
                                        const top10W = topHolds.reduce((s, h) => s + (h.weight ?? 0), 0);
                                        const maxW = Math.max(...holds.map((h) => h.weight ?? 0), 0.0001);
                                        const shownHolds = showAllHoldings ? holds : topHolds;
                                        const listGrid = isMobile
                                            ? '24px minmax(0, 1fr) 56px'
                                            : '30px minmax(110px, 1.1fr) minmax(60px, 1fr) 60px 84px';
                                        return (
                                        <div
                                            style={{
                                                display: 'flex',
                                                gap: isMobile ? 14 : 24,
                                                flexWrap: 'wrap',
                                                alignItems: 'flex-start',
                                            }}
                                        >
                                            <div style={{ flexShrink: 0, margin: '0 auto', lineHeight: 0 }}>
                                                <Donut
                                                    holdings={donutHoldings}
                                                    colors={donutColors}
                                                    maxSlices={donutHoldings.length}
                                                    centerCount={data.current_holdings.length}
                                                    centerLabel={navInDonut ? 'Объём (СЧА)' : undefined}
                                                    centerValue={navInDonut ? formatRubShort(navValue) : undefined}
                                                    size={isMobile ? Math.min(330, vw - 80) : 380}
                                                    outerRadius={90}
                                                    innerRadius={56}
                                                    highlightIndex={modalHover == null ? null : (modalHover < topHolds.length ? modalHover : (restWeight > 0 ? topHolds.length : null))}
                                                    onHoverChange={(s) => setModalHover(s == null ? null : (s < topHolds.length ? s : null))}
                                                    onSliceClick={(i) => {
                                                        // «Прочее» — последний слайс, если добавлен; клик игнорим.
                                                        if (i < topHolds.length) openAsset(topHolds[i]);
                                                    }}
                                                />
                                            </div>
                                            <div style={{ flex: 1, minWidth: isMobile ? 220 : 320 }}>
                                                {/* Концентрация: сколько позиций и сколько весит топ-10 —
                                                    отличает индексный фонд от концентрированного. */}
                                                <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-muted)', fontWeight: 600, marginBottom: 8, fontVariantNumeric: 'tabular-nums' }}>
                                                    {holds.length} позиций · топ-10 занимают {Math.min(top10W, 100).toFixed(1).replace('.', ',')}%
                                                </div>
                                                {/* Шапка списка — как в «Обзоре портфеля». */}
                                                <div style={{ display: 'grid', gridTemplateColumns: listGrid, gap: 10, padding: '4px 0 8px', borderBottom: '1.5px solid var(--text-primary)', fontSize: 'var(--fs-3xs, 10px)', fontWeight: 800, letterSpacing: '0.06em', textTransform: 'uppercase', color: 'var(--text-muted)' }}>
                                                    <span /><span>Бумага</span>{!isMobile && <span />}<span style={{ textAlign: 'right' }}>Доля</span>{!isMobile && <span style={{ textAlign: 'right' }}>Объём</span>}
                                                </div>
                                                {shownHolds.map((h, i) => {
                                                    const selected = isSelected(h);
                                                    const w = h.weight ?? 0;
                                                    const pct = Math.max(2, (w / maxW) * 100);
                                                    const hov = modalHover === i;
                                                    const last = i === shownHolds.length - 1;
                                                    return (
                                                        <div
                                                            key={h.asset_name}
                                                            onClick={() => openAsset(h)}
                                                            onMouseEnter={() => setModalHover(i)}
                                                            onMouseLeave={() => setModalHover(null)}
                                                            role="button"
                                                            tabIndex={0}
                                                            onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openAsset(h); } }}
                                                            title={`По бумаге: ${fundAssetName(h.asset_name, h.isin)}`}
                                                            style={{
                                                                display: 'grid',
                                                                gridTemplateColumns: listGrid,
                                                                gap: 10,
                                                                alignItems: 'center',
                                                                padding: '6px 8px',
                                                                margin: '0 -8px',
                                                                borderRadius: 8,
                                                                cursor: 'pointer',
                                                                background: selected
                                                                    ? 'color-mix(in srgb, var(--accent) 14%, transparent)'
                                                                    : hov ? 'color-mix(in srgb, var(--text-primary) 5%, transparent)' : 'transparent',
                                                                borderBottom: last ? 'none' : '1px dashed color-mix(in srgb, var(--text-primary) 12%, transparent)',
                                                                transition: 'background 0.12s ease',
                                                            }}
                                                        >
                                                            <HoldingLogo name={h.asset_name} isin={h.isin} idx={i} size={isMobile ? 24 : 30} />
                                                            <span style={{ minWidth: 0, fontSize: 'var(--fs-sm)', fontWeight: 700, color: 'var(--text-primary)', ...nameFade }}>
                                                                {fundAssetName(h.asset_name, h.isin)}
                                                            </span>
                                                            {/* Полоса нейтральная — цвет несёт пончик слева. */}
                                                            {!isMobile && (
                                                                <div style={{ height: 9, background: 'color-mix(in srgb, var(--text-primary) 8%, transparent)', borderRadius: 5, overflow: 'hidden', minWidth: 0 }}>
                                                                    <div style={{ width: `${pct}%`, height: '100%', background: 'color-mix(in srgb, var(--text-primary) 32%, transparent)', borderRadius: 5 }} />
                                                                </div>
                                                            )}
                                                            <span style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums', fontSize: 'var(--fs-xs)', fontWeight: 800, color: 'var(--text-primary)' }}>
                                                                {h.weight !== null ? `${w.toFixed(2)}%` : '—'}
                                                            </span>
                                                            {!isMobile && (
                                                                <span style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums', fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>
                                                                    {h.amount_rub != null ? fmtVolShort(h.amount_rub) : '—'}
                                                                </span>
                                                            )}
                                                        </div>
                                                    );
                                                })}
                                                {/* Разворот «Прочие бумаги» — та же неяркая ссылка, что в
                                                    «Обзоре портфеля»; разворачивает список inline (модалка
                                                    и так скроллится). */}
                                                {holds.length > TOP && (
                                                    <div style={{ display: 'flex', justifyContent: 'flex-end', alignItems: 'center', paddingTop: 9 }}>
                                                        <button
                                                            onClick={() => setShowAllHoldings((v) => !v)}
                                                            onMouseEnter={(e) => { e.currentTarget.style.color = 'var(--text-primary)'; e.currentTarget.style.textDecoration = 'underline'; }}
                                                            onMouseLeave={(e) => { e.currentTarget.style.color = 'var(--text-muted)'; e.currentTarget.style.textDecoration = 'none'; }}
                                                            style={{ display: 'inline-flex', alignItems: 'center', gap: 5, padding: 0, background: 'transparent', border: 'none', fontSize: 'var(--fs-xs)', fontWeight: 600, color: 'var(--text-muted)', cursor: 'pointer', whiteSpace: 'nowrap', transition: 'color 0.12s ease' }}
                                                        >
                                                            {showAllHoldings
                                                                ? <>Свернуть <span style={{ fontSize: '0.85em' }}>↑</span></>
                                                                : <>Прочие бумаги · ещё {holds.length - TOP} <span style={{ fontSize: '0.85em' }}>↓</span></>}
                                                        </button>
                                                    </div>
                                                )}
                                            </div>
                                        </div>
                                        );
                                    })() : (
                                        <div
                                            style={{
                                                padding: '24px 16px',
                                                textAlign: 'center',
                                                color: 'var(--text-muted)',
                                                fontSize: 'var(--fs-sm)',
                                            }}
                                        >
                                            Состав не публикуется
                                        </div>
                                    )}

                                    {/* (5) Что купили и продали — diff текущего и предыдущего
                                        месячных снапшотов (уже приходит в ответе detail).
                                        Период сравнения (1М/6М/1Г + произвольный диапазон
                                        месяцев через календарь) выбирается в шапке раздела —
                                        кнопки периодов переехали сюда из «Сделок» Общего
                                        портфеля. Клик по строке — тот же drill-down в бумагу.
                                        Раздел виден, когда есть что показать ИЛИ юзер уже
                                        трогал период (иначе после пустого 6М он бы исчез
                                        вместе с кнопками). */}
                                    {data.current_snapshot_date && (data.diff.length > 0 || diffPeriod !== '1m' || diffRange != null) && (() => {
                                        // Структура «Сделок» из «Общего портфеля» (PortfolioMoversPanel):
                                        // две секции с нейтральными полосами и величиной справа, единица
                                        // (п.п.) — в подписи секции. Отличие: секции стоят не стопкой,
                                        // а колонками — покупки слева, продажи справа. Масштаб полос общий
                                        // по обеим колонкам, чтобы покупки и продажи сравнивались.
                                        const DIFF_PREVIEW = 8;
                                        const dw = (d: typeof data.diff[number]) => d.delta_weight ?? 0;
                                        const byAbs = (a: typeof data.diff[number], b: typeof data.diff[number]) => Math.abs(dw(b)) - Math.abs(dw(a));
                                        const buys = data.diff.filter((d) => dw(d) > 0 || d.change_type === 'new').sort(byAbs);
                                        const sells = data.diff.filter((d) => dw(d) < 0 || d.change_type === 'sold_out').sort(byAbs);
                                        const maxAbs = Math.max(0.0001, ...data.diff.map((d) => Math.abs(dw(d))));
                                        const rowGrid = isMobile
                                            ? '24px minmax(40px, 1fr) minmax(24px, 0.7fr) max-content'
                                            : '30px minmax(60px, 1fr) minmax(28px, 0.9fr) max-content';
                                        const sectionLabel = (text: string, up: boolean) => (
                                            <div style={{ display: 'flex', alignItems: 'baseline', gap: 7, fontSize: 'var(--fs-sm)', fontWeight: 800, letterSpacing: '0.02em', color: 'var(--text-primary)', margin: '0 0 4px' }}>
                                                {text}<span style={{ fontSize: '0.95em', fontWeight: 700 }}>{up ? '↑' : '↓'}</span>
                                                <span style={{ fontSize: 'var(--fs-xs)', fontWeight: 600, letterSpacing: 0, color: 'var(--text-muted)' }}>(п.п.)</span>
                                            </div>
                                        );
                                        const diffRow = (d: typeof data.diff[number], i: number, last: boolean) => {
                                            const delta = d.delta_weight;
                                            const pct = Math.max(2, (Math.abs(delta ?? 0) / maxAbs) * 100);
                                            const openDiffAsset = () => setDrillDown({ asset_name: d.asset_name, isin: d.isin ?? null });
                                            const meta = CHANGE_META[d.change_type];
                                            const isEvent = d.change_type === 'new' || d.change_type === 'sold_out';
                                            return (
                                                <div
                                                    key={`${d.asset_name}|${d.isin ?? ''}`}
                                                    onClick={openDiffAsset}
                                                    role="button"
                                                    tabIndex={0}
                                                    onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openDiffAsset(); } }}
                                                    title={`По бумаге: ${fundAssetName(d.asset_name, d.isin ?? null)}${meta ? ` · ${meta.label}` : ''}`}
                                                    style={{
                                                        display: 'grid',
                                                        gridTemplateColumns: rowGrid,
                                                        gap: 10,
                                                        alignItems: 'center',
                                                        padding: '7px 6px',
                                                        margin: '0 -6px',
                                                        borderRadius: 8,
                                                        cursor: 'pointer',
                                                        borderBottom: last ? 'none' : '1px dashed color-mix(in srgb, var(--text-primary) 12%, transparent)',
                                                        transition: 'background 0.12s ease',
                                                    }}
                                                    onMouseEnter={(e) => { e.currentTarget.style.background = 'color-mix(in srgb, var(--text-primary) 5%, transparent)'; }}
                                                    onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
                                                >
                                                    <HoldingLogo name={d.asset_name} isin={d.isin ?? null} idx={i} size={isMobile ? 24 : 30} />
                                                    <span style={{ minWidth: 0, fontSize: 'var(--fs-md)', fontWeight: 600, color: 'var(--text-primary)', ...nameFade }}>
                                                        {fundAssetName(d.asset_name, d.isin ?? null)}
                                                    </span>
                                                    {/* Полоса нейтральная — направление задаёт колонка. Вход/выход
                                                        из бумаги (событие) заливаем плотнее, чем докупку/сокращение. */}
                                                    <div style={{ height: 9, background: 'color-mix(in srgb, var(--text-primary) 8%, transparent)', borderRadius: 5, overflow: 'hidden', minWidth: 0 }}>
                                                        <div style={{ width: `${pct}%`, height: '100%', background: `color-mix(in srgb, var(--text-primary) ${isEvent ? 55 : 32}%, transparent)`, borderRadius: 5 }} />
                                                    </div>
                                                    <span style={{ textAlign: 'right', whiteSpace: 'nowrap', fontVariantNumeric: 'tabular-nums', fontSize: 'var(--fs-sm)', fontWeight: 600, color: 'var(--text-primary)' }}>
                                                        {delta == null ? '—' : `${delta > 0 ? '+' : '−'}${Math.abs(delta).toFixed(2)}`}
                                                    </span>
                                                </div>
                                            );
                                        };
                                        const column = (items: typeof data.diff, label: string, up: boolean) => {
                                            const shown = showAllDiff ? items : items.slice(0, DIFF_PREVIEW);
                                            return (
                                                <div style={{ minWidth: 0 }}>
                                                    {sectionLabel(label, up)}
                                                    {shown.length === 0 ? (
                                                        <div style={{ padding: '10px 2px', color: 'var(--text-muted)', fontSize: 'var(--fs-sm)' }}>
                                                            Ничего {up ? 'не купили' : 'не продали'}
                                                        </div>
                                                    ) : shown.map((d, i) => diffRow(d, i, i === shown.length - 1))}
                                                </div>
                                            );
                                        };
                                        const hiddenCount = Math.max(0, buys.length - DIFF_PREVIEW) + Math.max(0, sells.length - DIFF_PREVIEW);
                                        const hasDiff = data.diff.length > 0 && data.previous_snapshot_date != null;
                                        return (
                                            <div style={{ marginTop: 28 }}>
                                                {/* Шапка как у «Сделок» Общего портфеля: заголовок +
                                                    подзаголовок-диапазон слева, сегменты периода справа. */}
                                                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 10, flexWrap: 'wrap', marginBottom: 12 }}>
                                                    <div style={{ display: 'flex', flexDirection: 'column', minWidth: 0 }}>
                                                        <h3
                                                            style={{
                                                                fontSize: 'var(--fs-md)',
                                                                fontWeight: 700,
                                                                color: 'var(--text-primary)',
                                                                margin: 0,
                                                            }}
                                                        >
                                                            Что купили и продали
                                                        </h3>
                                                        <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', marginTop: 2, lineHeight: 1.4 }}>
                                                            {diffRange
                                                                ? <>{monthRangeLabel(diffRange.from, diffRange.to)} · свой диапазон</>
                                                                : hasDiff
                                                                    ? <>Изменения состава: {formatSnapshotDate(data.previous_snapshot_date!)} → {formatSnapshotDate(data.current_snapshot_date!)} (месячные срезы)</>
                                                                    : <>Месячные срезы состава</>}
                                                        </span>
                                                    </div>
                                                    {/* align-items: stretch — кнопка-календарь тянется в высоту сегментов. */}
                                                    <div style={{ display: 'flex', alignItems: 'stretch', gap: 6 }}>
                                                        <SegmentedControl<string>
                                                            options={[
                                                                { key: '1m', label: '1М' },
                                                                { key: '6m', label: '6М' },
                                                                { key: '1y', label: '1Г' },
                                                            ]}
                                                            // Свой диапазон — активной пилюли нет (период показывает календарь).
                                                            value={diffRange ? 'custom' : diffPeriod}
                                                            onChange={(k) => { setDiffRange(null); setDiffPeriod(k as MoversDiffPeriod); }}
                                                        />
                                                        {diffMonths.length > 1 && (
                                                            <MonthRangePicker
                                                                availableMonths={diffMonths}
                                                                value={diffRange}
                                                                onChange={(r) => setDiffRange(r)}
                                                                onReset={() => setDiffRange(null)}
                                                            />
                                                        )}
                                                    </div>
                                                </div>
                                                {!hasDiff ? (
                                                    <div style={{ padding: '14px 2px', color: 'var(--text-muted)', fontSize: 'var(--fs-sm)', lineHeight: 1.5 }}>
                                                        {loading
                                                            ? 'Загружаем…'
                                                            : diffRange
                                                                ? 'Нет среза состава на начало выбранного диапазона.'
                                                                : 'Нет более раннего среза за выбранный период.'}
                                                    </div>
                                                ) : (
                                                <>
                                                {/* Покупки слева, продажи справа. На мобиле — стопкой.
                                                    При перезагрузке периода — слегка тускнеет. */}
                                                <div style={{
                                                    display: 'grid',
                                                    gridTemplateColumns: isMobile ? '1fr' : '1fr 1fr',
                                                    gap: isMobile ? 18 : 28,
                                                    alignItems: 'start',
                                                    opacity: loading ? 0.5 : 1,
                                                    transition: 'opacity 0.15s ease',
                                                }}>
                                                    {column(buys, 'Покупки', true)}
                                                    {column(sells, 'Продажи', false)}
                                                </div>
                                                {hiddenCount > 0 && (
                                                    <div style={{ display: 'flex', justifyContent: 'flex-end', alignItems: 'center', paddingTop: 9 }}>
                                                        <button
                                                            onClick={() => setShowAllDiff((v) => !v)}
                                                            onMouseEnter={(e) => { e.currentTarget.style.color = 'var(--text-primary)'; e.currentTarget.style.textDecoration = 'underline'; }}
                                                            onMouseLeave={(e) => { e.currentTarget.style.color = 'var(--text-muted)'; e.currentTarget.style.textDecoration = 'none'; }}
                                                            style={{ display: 'inline-flex', alignItems: 'center', gap: 5, padding: 0, background: 'transparent', border: 'none', fontSize: 'var(--fs-xs)', fontWeight: 600, color: 'var(--text-muted)', cursor: 'pointer', whiteSpace: 'nowrap', transition: 'color 0.12s ease' }}
                                                        >
                                                            {showAllDiff
                                                                ? <>Свернуть <span style={{ fontSize: '0.85em' }}>↑</span></>
                                                                : <>Все изменения · ещё {hiddenCount} <span style={{ fontSize: '0.85em' }}>↓</span></>}
                                                        </button>
                                                    </div>
                                                )}
                                                </>
                                                )}
                                            </div>
                                        );
                                    })()}
                                </>
                            )}
                        </>
                    )}
                </div>
            </div>
        </div>

        {/* (G) drill-down: «как этот фонд/УК покупал актив» — тот же
            AssetHistoryModal, что в «Обзоре снапшота» (та же сигнатура пропсов).
            z-index выше overlay'я FundDetailModal → ложится сверху.
            Только при enableDrilldown. */}
        {enableDrilldown && drillDown && (
            <AssetHistoryModal
                ticker={ticker}
                asset_name={drillDown.asset_name}
                isin={drillDown.isin}
                onClose={() => setDrillDown(null)}
            />
        )}
        </>
    );
}

// ════════════════════════════════════════════════════════════════════
// Asset History Modal — drill-down: график positions по одной позиции в одном фонде
// ════════════════════════════════════════════════════════════════════

export function AssetHistoryModal({
    ticker,
    asset_name,
    isin,
    onClose,
}: {
    ticker: string;
    asset_name: string;
    isin: string | null;
    onClose: () => void;
}) {
    const [data, setData] = useState<AssetHistory | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const isMobile = useViewportWidth() < 768;

    useEffect(() => {
        let cancel = false;
        setLoading(true);
        setError(null);
        getAssetHistory(ticker, isin ? { isin } : { assetName: asset_name })
            .then((d) => !cancel && setData(d))
            .catch((e) => !cancel && setError(e.message))
            .finally(() => !cancel && setLoading(false));
        return () => { cancel = true; };
    }, [ticker, asset_name, isin]);

    return (
        <div
            onClick={onClose}
            style={{
                position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                zIndex: 1000, padding: isMobile ? 4 : 16,
            }}
        >
            <div
                onClick={(e) => e.stopPropagation()}
                style={{
                    background: 'var(--bg-primary)',
                    maxWidth: 1240, width: '100%', maxHeight: isMobile ? '94dvh' : '92vh', overflow: 'auto',
                    border: '1.5px solid var(--text-primary)',
                    /* Верхний паддинг убран — он переехал внутрь sticky-шапки,
                       чтобы шапка липла к самому верху без зазора. */
                    padding: isMobile ? '0 12px 16px' : '0 28px 24px',
                    boxShadow: '0 16px 60px rgba(0,0,0,0.3)',
                }}
            >
                {/* Header — sticky: закреплён сверху, при скролле стоит на месте
                    (вместе с кнопкой закрыть). bg перекрывает уезжающий контент. */}
                <div style={{
                    display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between',
                    position: 'sticky', top: 0, zIndex: 5,
                    background: 'var(--bg-primary)',
                    paddingTop: isMobile ? 16 : 24,
                    marginBottom: 20, paddingBottom: 16,
                    borderBottom: '1.5px solid var(--text-primary)',
                }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 12, minWidth: 0 }}>
                        {(() => {
                            // Лого актива по резолвнутому тикеру (как в Сезонности/ОИ).
                            // ОФЗ — герб Минфина (raw_152), иначе без лого.
                            const assetTk = resolveFundTicker(asset_name, isin);
                            if (assetTk) return <InstrumentIcon sectype={assetTk} size={isMobile ? 34 : 42} rounded="full" />;
                            if (isOfzBond(asset_name)) return <InstrumentIcon sectype="RB" size={isMobile ? 34 : 42} rounded="full" />;
                            return null;
                        })()}
                        <div style={{ minWidth: 0 }}>
                            <h3 style={{
                                fontFamily: 'var(--font-serif, Georgia, serif)',
                                fontSize: 'var(--fs-2xl)', margin: 0, marginBottom: 4,
                                color: 'var(--text-primary)',
                            }}>
                                {fundAssetName(asset_name, isin)}
                            </h3>
                            <div style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-tertiary)' }}>
                                {ticker} {isin && <span style={{ marginLeft: 8 }}>· {isin}</span>}
                            </div>
                        </div>
                    </div>
                    <button
                        onClick={onClose}
                        style={{
                            border: 'none', background: 'transparent',
                            fontSize: 24, cursor: 'pointer', color: 'var(--text-tertiary)',
                            padding: '4px 8px',
                        }}
                        aria-label="Закрыть"
                    >
                        ×
                    </button>
                </div>

                {loading && (
                    <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-tertiary)' }}>
                        Загрузка истории...
                    </div>
                )}
                {error && (
                    <div style={{ padding: 16, color: 'var(--mood-red)' }}>{error}</div>
                )}

                {data && !loading && <AssetHistoryContent data={data} assetName={asset_name} ticker={ticker} />}
            </div>
        </div>
    );
}

function AssetHistoryContent({ data, assetName, ticker }: { data: AssetHistory; assetName: string; ticker: string }) {
    const chartAnchorRef = useRef<HTMLDivElement>(null);
    const isMobile = useViewportWidth() < 768;
    const vh = useViewportHeight();

    // Сплит-коррекция: непрерывная серия (история домножена на множитель сплита) +
    // маркеры «Сплит». Снимает «один снапшот = пик, остальное плоское».
    const { factorByDate, annotations } = useMemo(() => splitAdjustPositions(data.timeline), [data.timeline]);
    const adjPos = (p: AssetHistory['timeline'][number]) =>
        p.positions == null ? null : Math.round(p.positions * (factorByDate.get(p.snapshot_date) ?? 1));
    // adjusted positions/delta/price по дате (delta — в хронологии; цена = raw/множитель,
    // сумма/вес остаются raw → adj_pos × adj_price = amount сходится).
    const adjByDate = useMemo(() => {
        const chrono = [...data.timeline].sort((a, b) => a.snapshot_date.localeCompare(b.snapshot_date));
        const m = new Map<string, { pos: number | null; delta: number | null; price: number | null }>();
        let prev: number | null = null;
        for (const p of chrono) {
            const f = factorByDate.get(p.snapshot_date) ?? 1;
            const pos = p.positions == null ? null : Math.round(p.positions * f);
            const price = p.price_rub == null ? null : p.price_rub / f;
            const delta = prev == null || pos == null ? null : pos - prev;
            m.set(p.snapshot_date, { pos, delta, price });
            if (pos != null) prev = pos;
        }
        return m;
    }, [data.timeline, factorByDate]);

    const points = data.timeline.filter(p => p.positions !== null);
    const firstPos = adjPos(points[0]) ?? 0;
    const lastPos = adjPos(points[points.length - 1]) ?? 0;
    const totalDelta = lastPos - firstPos;
    const totalDeltaColor = totalDelta >= 0 ? 'var(--mood-green)' : 'var(--mood-red)';

    // Данные для SimpleChart: скорректированные позиции (штуки) по снапшотам.
    const chartData = points.map(p => ({ time: p.snapshot_date, value: adjPos(p)! }));

    // Единицу (млн шт / тыс шт / шт) выносим в ЛЕГЕНДУ, а на оси Y — голые числа
    // в этом масштабе (не дублируем «млн» на каждой метке).
    const yMaxAbs = chartData.reduce((m, d) => Math.max(m, Math.abs(d.value)), 1);
    const yScale = yMaxAbs >= 1e6 ? 1e6 : yMaxAbs >= 1e3 ? 1e3 : 1;
    const yUnit = yScale === 1e6 ? 'млн шт' : yScale === 1e3 ? 'тыс шт' : 'шт';
    const fmtYScaled = (v: number) => {
        const x = v / yScale;
        return yScale === 1
            ? Math.round(x).toLocaleString('ru-RU')
            : x.toLocaleString('ru-RU', { maximumFractionDigits: Math.abs(x) >= 100 ? 0 : Math.abs(x) >= 10 ? 1 : 2 });
    };

    // Сортировка таблицы «Все снапшоты» — кликабельные колонки.
    const [snapSort, setSnapSort] = useState<'date' | 'positions' | 'delta' | 'amount' | 'price' | 'weight'>('date');
    const [snapDir, setSnapDir] = useState<'asc' | 'desc'>('desc');
    const snapColumns = [
        { key: 'date', label: 'Дата', align: 'left' },
        { key: 'positions', label: 'Штук', align: 'right' },
        { key: 'delta', label: 'Δ', align: 'right' },
        { key: 'amount', label: 'На сумму', align: 'right' },
        { key: 'price', label: 'Цена', align: 'right' },
        { key: 'weight', label: 'Доля', align: 'right' },
    ] as const;
    const sortedTimeline = useMemo(() => {
        const num = (v: number | null) => (v == null ? -Infinity : v);
        const val = (p: AssetHistory['timeline'][number]): string | number => {
            const adj = adjByDate.get(p.snapshot_date);
            switch (snapSort) {
                case 'date': return p.snapshot_date;
                case 'positions': return num(adj?.pos ?? p.positions);
                case 'delta': return num(adj?.delta ?? p.delta_positions);
                case 'amount': return num(p.amount_rub);
                case 'price': return num(adj?.price ?? p.price_rub);
                case 'weight': return num(p.weight);
            }
        };
        return [...data.timeline].sort((a, b) => {
            const av = val(a), bv = val(b);
            const cmp = typeof av === 'string' ? av.localeCompare(bv as string) : (av as number) - (bv as number);
            return snapDir === 'asc' ? cmp : -cmp;
        });
    }, [data.timeline, snapSort, snapDir, adjByDate]);
    const onSnapSort = (key: typeof snapSort) => {
        if (snapSort === key) setSnapDir((d) => (d === 'asc' ? 'desc' : 'asc'));
        else { setSnapSort(key); setSnapDir('desc'); }
    };

    return (
        <>
            {/* Summary */}
            <div style={{
                display: 'flex', gap: 32, flexWrap: 'wrap',
                marginBottom: 20, padding: '12px 0',
            }}>
                <SummaryStat
                    label="ПЕРВЫЙ СНАПШОТ"
                    value={formatSnapshotDate(data.first_seen)}
                    sub={`${formatShares(firstPos)} шт`}
                />
                <SummaryStat
                    label="ПОСЛЕДНИЙ СНАПШОТ"
                    value={formatSnapshotDate(data.last_seen)}
                    sub={`${formatShares(lastPos)} шт`}
                />
                <SummaryStat
                    label="ИЗМЕНЕНИЕ"
                    value={`${totalDelta >= 0 ? '+' : ''}${formatShares(totalDelta)} шт`}
                    sub={`за ${data.snapshots_count} снапшота${data.snapshots_count > 1 ? 'ов' : ''}`}
                    color={totalDeltaColor}
                />
                <SummaryStat
                    label="ТЕКУЩАЯ ДОЛЯ"
                    value={points[points.length - 1]?.weight !== null
                        ? `${points[points.length - 1].weight!.toFixed(2)}%`
                        : '—'}
                    sub={formatRubShort(points[points.length - 1]?.amount_rub || null)}
                />
            </div>

            {/* График позиций — SimpleChart (интерактивный, accent-линия + hover/crosshair) */}
            {/* Один контейнер — родной SimpleChart (rounded-2xl border bg-primary).
                chartAnchorRef — голая обёртка-цель для html2canvas (без своей рамки,
                иначе двойной контейнер). Камера-экспорт абсолютом ВНУТРИ угла
                контейнера (top/right 16 == SimpleChart top-4/right-4), снаружи
                chartAnchorRef → в snapshot не попадёт. */}
            <div style={{ position: 'relative', marginBottom: 24, marginLeft: isMobile ? -12 : 0, marginRight: isMobile ? -12 : 0 }}>
                <div ref={chartAnchorRef}>
                    <SimpleChart
                        data={chartData}
                        height={Math.max(220, Math.min(470, vh - 220))}
                        primaryLabel={`${assetName}, ${yUnit}`}
                        legendPosition="top"
                        formatValue={(v) => formatShares(Math.round(v))}
                        formatPrimaryAxis={fmtYScaled}
                        formatTime={formatMonthYearShort}
                        tooltipDateFormat={formatMonthYearShort}
                        clampEdgeLabels
                        mobilePadRight={isMobile ? 14 : undefined}
                        showValueHeader={false}
                        showDownloadButton={false}
                        annotations={annotations}
                    />
                </div>
                {/* Камера-экспорт — только десктоп: на мобиле убрана (рудимент). */}
                {!isMobile && (
                <div data-export-ignore="true" style={{ position: 'absolute', top: 16, right: 16, zIndex: 3 }}>
                    <ChartCaptureButton
                        getTargetElement={() => chartAnchorRef.current}
                        filename={`frame-fund-${ticker}`}
                        metadata={{
                            title: assetName,
                            asset: ticker,
                            details: ['Позиции по снапшотам, шт'],
                        }}
                    />
                </div>
                )}
            </div>

            {/* Table of all snapshots */}
            <div>
                <div style={{
                    fontSize: 'var(--fs-xs)', fontWeight: 700,
                    letterSpacing: '0.08em', marginBottom: 8,
                    paddingBottom: 4, borderBottom: '1.5px solid var(--text-primary)',
                }}>
                    ВСЕ СНАПШОТЫ
                </div>
                <table style={{
                    width: '100%', borderCollapse: 'collapse',
                    fontSize: 'var(--fs-sm)', fontVariantNumeric: 'tabular-nums',
                }}>
                    <thead>
                        <tr>
                            {snapColumns.map((c) => {
                                const active = snapSort === c.key;
                                return (
                                    <th
                                        key={c.key}
                                        onClick={() => onSnapSort(c.key)}
                                        style={{
                                            textAlign: c.align,
                                            padding: '6px 8px',
                                            fontWeight: 600,
                                            cursor: 'pointer',
                                            userSelect: 'none',
                                            whiteSpace: 'nowrap',
                                            color: active ? 'var(--text-primary)' : 'var(--text-tertiary)',
                                        }}
                                    >
                                        {c.label}
                                        <span style={{ opacity: active ? 1 : 0.35, marginLeft: 3 }}>
                                            {active ? (snapDir === 'asc' ? '▲' : '▼') : '⇅'}
                                        </span>
                                    </th>
                                );
                            })}
                        </tr>
                    </thead>
                    <tbody>
                        {sortedTimeline.map((p) => {
                            // Скорректированные под сплит значения (raw для сумм/веса).
                            const adj = adjByDate.get(p.snapshot_date);
                            const apos = adj?.pos ?? p.positions;
                            const adelta = adj?.delta ?? p.delta_positions;
                            const aprice = adj?.price ?? p.price_rub;
                            const dColor = !adelta ? 'var(--text-tertiary)'
                                : adelta > 0 ? 'var(--mood-green)' : 'var(--mood-red)';
                            return (
                                <tr key={p.snapshot_date} style={{ borderBottom: '1px solid var(--border-soft, rgba(0,0,0,0.05))' }}>
                                    <td style={{ padding: '6px 8px' }}>{formatSnapshotDate(p.snapshot_date)}</td>
                                    <td style={{ padding: '6px 8px', textAlign: 'right' }}>{formatShares(apos)}</td>
                                    <td style={{ padding: '6px 8px', textAlign: 'right', color: dColor, fontWeight: 600 }}>
                                        {adelta === null ? '—'
                                            : adelta === 0 ? '0'
                                            : `${adelta > 0 ? '+' : ''}${formatShares(adelta)}`}
                                    </td>
                                    <td style={{ padding: '6px 8px', textAlign: 'right' }}>
                                        {formatRubShort(p.amount_rub)}
                                    </td>
                                    <td style={{ padding: '6px 8px', textAlign: 'right' }}>
                                        {aprice !== null ? `${aprice.toFixed(2)} ₽` : '—'}
                                    </td>
                                    <td style={{ padding: '6px 8px', textAlign: 'right' }}>
                                        {p.weight !== null ? `${p.weight.toFixed(2)}%` : '—'}
                                    </td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
        </>
    );
}

function SummaryStat({
    label, value, sub, color,
}: {
    label: string;
    value: string;
    sub?: string;
    color?: string;
}) {
    return (
        <div>
            <div style={{
                fontSize: 'var(--fs-xs)', fontWeight: 700,
                letterSpacing: '0.08em', color: 'var(--text-tertiary)',
                marginBottom: 4,
            }}>
                {label}
            </div>
            <div style={{
                fontSize: 'var(--fs-lg)', fontWeight: 700,
                fontVariantNumeric: 'tabular-nums',
                color: color || 'var(--text-primary)',
            }}>
                {value}
            </div>
            {sub && (
                <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-tertiary)', marginTop: 2 }}>
                    {sub}
                </div>
            )}
        </div>
    );
}

export type { FundDetailModalProps };
