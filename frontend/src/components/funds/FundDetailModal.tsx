/**
 * FundDetailModal — детальная карточка фонда (объём СЧА + график доходности
 * пая + опционально donut состава с drill-down в актив).
 *
 * Вынесен из FundTradesPage, чтобы переиспользовать в «Деньги в фондах».
 * Контракт обобщён: вместо внутреннего `getFundTradesDetail(ticker, period)`
 * карточка принимает `loadDetail()` от родителя (замыкает ticker/period/id),
 * а СЧА/returns/has_distributions берутся из ответа detail, иначе из пропов.
 *
 * `enableDrilldown`:
 *   - true  → секция «Состав фонда» (donut + таблица топ-активов) + клик в
 *             актив открывает AssetHistoryModal («как фонд покупал бумагу»).
 *   - false → БАЗОВАЯ карточка: шапка + СЧА + «Доходность пая» (график +
 *             плашки), без состава и без drill-down.
 *
 * Всё поведение «Покупок фондов» сохранено вербатим: анимаций нет, но есть
 * мемоизация payChartData + дефолт-окно графика «последний год», своп
 * имя-главное/тикер-вторичный в шапке, hover-связь донат↔таблица состава.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { Calendar } from 'lucide-react';
import {
    getAssetHistory,
    type FundTradesDetail,
    type FundReturns,
    type AssetHistory,
} from '../../services/api';
import SimpleChart, { type ChartAnnotation } from '../SimpleChart';
import ChartCaptureButton from '../export/ChartCaptureButton';
import { DONUT_COLORS, assetColor } from '../../config/fundConfig';
import Donut from './Donut';
import { useViewportWidth } from '../../hooks/useViewportWidth';

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

interface FundDetailModalProps {
    ticker: string;
    loadDetail: () => Promise<FundTradesDetail>;   // лоадер от родителя (замыкает ticker/period/id)
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
    // Hover-связь пончик↔таблица состава (индекс позиции в holds).
    const [modalHover, setModalHover] = useState<number | null>(null);

    // Pay-график: точки (СЧА на пай) + дефолтное окно «последний год».
    // Мемоизируем по data, чтобы hover доната (setModalHover) НЕ пересоздавал
    // массив → SimpleChart не сбрасывал бы зум навигатора на каждый ре-рендер.
    const payChartData = useMemo(
        () => (data?.performance?.timeline ?? [])
            .filter((p) => p.pay != null)
            .map((p) => ({ time: p.date, value: p.pay })),
        [data],
    );
    // Дефолтное окно навигатора: первый индекс за последние 12 месяцев. Лечит
    // диссонанс «линия СЧА вверх за всю историю / доходность за 3 мес вниз» —
    // по умолчанию виден последний год, остальное доступно перетаскиванием.
    const payInitialStartIndex = useMemo(() => {
        if (payChartData.length < 2) return 0;
        const lastT = payChartData[payChartData.length - 1].time;
        const cutoff = new Date(lastT);
        cutoff.setFullYear(cutoff.getFullYear() - 1);
        const cutoffMs = cutoff.getTime();
        const idx = payChartData.findIndex((d) => new Date(d.time).getTime() >= cutoffMs);
        return idx > 0 ? idx : 0;
    }, [payChartData]);

    // loadDetail обычно — inline-замыкание от родителя (новая ссылка на каждый
    // ре-рендер). Держим его в ref и перезапускаем загрузку ТОЛЬКО при смене
    // ticker — иначе любой ре-рендер родителя ресетил бы loading + сбрасывал зум
    // навигатора графика (полированное поведение «Покупок фондов»).
    const loadDetailRef = useRef(loadDetail);
    loadDetailRef.current = loadDetail;
    useEffect(() => {
        let cancelled = false;
        setLoading(true);
        setError(null);
        loadDetailRef.current()
            .then((d) => { if (!cancelled) setData(d); })
            .catch((e: Error) => { if (!cancelled) setError(e.message); })
            .finally(() => { if (!cancelled) setLoading(false); });
        return () => { cancelled = true; };
    }, [ticker]);

    // СЧА/returns/has_distributions: из ответа detail если есть, иначе из пропов.
    // detail.fund в текущем типе не несёт nav_rub/has_distributions — читаем мягко
    // (на случай, если backend их добавит), с фолбэком на пропы от родителя.
    const detailFund = data?.fund as
        | (FundTradesDetail['fund'] & { nav_rub?: number | null; has_distributions?: boolean })
        | undefined;
    const navValue = detailFund?.nav_rub ?? navRub ?? null;
    // Мобильная адаптация. Модал шарится с десктопом («Деньги в фондах»),
    // поэтому размеры реактивные: на мобиле пончик ужимаем под ширину экрана
    // (иначе size:380 вылезал за правый край), график ниже, паддинги меньше,
    // высота в dvh (iOS не режет верх/низ динамическим тулбаром).
    const vw = useViewportWidth();
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
                    <h2
                        style={{
                            margin: 0,
                            fontSize: 'var(--fs-lg)',
                            fontWeight: 800,
                            color: 'var(--text-primary)',
                        }}
                    >
                        {data?.fund.name || data?.fund.ticker || ticker}
                        {data?.fund.name && (data?.fund.ticker || ticker) && (
                            <span
                                style={{
                                    marginLeft: 10,
                                    fontWeight: 400,
                                    fontSize: 'var(--fs-sm)',
                                    fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace',
                                    color: 'var(--text-secondary)',
                                }}
                            >
                                {data?.fund.ticker || ticker}
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
                            padding: 4,
                        }}
                    >
                        ✕
                    </button>
                </div>

                <div style={{ padding: isMobile ? 12 : 20 }}>
                    {loading && <div style={{ color: 'var(--text-muted)' }}>Загружаем…</div>}
                    {error && <div style={{ color: 'var(--danger, #ef4444)' }}>{error}</div>}
                    {data && !loading && (
                        <>
                            {/* (2) Объём — полная СЧА (AUM) крупно */}
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
                                {data.current_snapshot_date && (
                                    <div
                                        style={{
                                            display: 'flex',
                                            alignItems: 'center',
                                            gap: 5,
                                            marginTop: 6,
                                            fontSize: 'var(--fs-2xs)',
                                            color: 'var(--text-muted)',
                                        }}
                                    >
                                        <Calendar size={12} style={{ flexShrink: 0 }} />
                                        Состав на {data.current_snapshot_date}
                                    </div>
                                )}
                            </div>

                            {/* (3) График доходности (СЧА на пай) + плашки returns */}
                            {(() => {
                                const perf = data.performance;
                                const ret = perf?.returns ?? returns ?? null;
                                const chartData = payChartData; // мемоизирован выше (стабильная ссылка)
                                return (
                                    <div style={{ marginBottom: 24 }}>
                                        <h3
                                            style={{
                                                fontSize: 'var(--fs-md)',
                                                fontWeight: 700,
                                                color: 'var(--text-primary)',
                                                marginBottom: 10,
                                            }}
                                        >
                                            Доходность пая
                                        </h3>
                                        {/* Мобила: график во всю ширину карточки (компенсируем
                                            боковой паддинг тела −12) — «по шире», + выше («по больше»). */}
                                        <div style={{ marginLeft: isMobile ? -12 : 0, marginRight: isMobile ? -12 : 0 }}>
                                        {chartData.length > 1 ? (
                                            <SimpleChart
                                                data={chartData}
                                                initialStartIndex={payInitialStartIndex}
                                                height={isMobile ? 340 : 460}
                                                primaryLabel="СЧА на пай, ₽"
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
                                                Недостаточно истории для графика доходности
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
                                                Доходность — полная, с&nbsp;учётом выплат дохода (по&nbsp;данным Cbonds).
                                                График показывает цену пая — она снижается в&nbsp;даты выплат.
                                            </div>
                                        )}
                                    </div>
                                );
                            })()}

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
                                        const holds = data.current_holdings;
                                        const TOP = 14;
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
                                            <div style={{ flex: 1, minWidth: 240 }}>
                                                <table
                                                    style={{
                                                        width: '100%',
                                                        borderCollapse: 'collapse',
                                                        fontSize: 'var(--fs-sm)',
                                                    }}
                                                >
                                                    <thead>
                                                        <tr>
                                                            <th style={{
                                                                textAlign: 'left',
                                                                padding: '8px 12px',
                                                                color: 'var(--text-muted)',
                                                                fontSize: 'var(--fs-2xs)',
                                                                textTransform: 'uppercase',
                                                                letterSpacing: '0.05em',
                                                                fontWeight: 700,
                                                                borderBottom: '2px solid var(--text-primary)',
                                                            }}>Актив</th>
                                                            <th style={{
                                                                textAlign: 'right',
                                                                padding: '8px 12px',
                                                                color: 'var(--text-muted)',
                                                                fontSize: 'var(--fs-2xs)',
                                                                textTransform: 'uppercase',
                                                                letterSpacing: '0.05em',
                                                                fontWeight: 700,
                                                                borderBottom: '2px solid var(--text-primary)',
                                                            }}>Доля, %</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                                        {holds.slice(0, 30).map((h, i) => {
                                                            const selected = isSelected(h);
                                                            return (
                                                            <tr
                                                                key={h.asset_name}
                                                                onClick={() => openAsset(h)}
                                                                onMouseEnter={() => setModalHover(i)}
                                                                onMouseLeave={() => setModalHover(null)}
                                                                style={{
                                                                    cursor: 'pointer',
                                                                    background: selected
                                                                        ? 'color-mix(in srgb, var(--accent) 14%, transparent)'
                                                                        : (modalHover === i ? 'var(--bg-secondary)' : 'transparent'),
                                                                    transition: 'background 100ms',
                                                                }}
                                                            >
                                                                <td style={{
                                                                    padding: '7px 12px',
                                                                    borderBottom: '1px solid color-mix(in srgb, var(--border-color) 60%, transparent)',
                                                                    color: 'var(--text-primary)',
                                                                }}>
                                                                    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
                                                                        <span
                                                                            style={{
                                                                                width: 8,
                                                                                height: 8,
                                                                                borderRadius: '50%',
                                                                                flexShrink: 0,
                                                                                // (C) точка = цвет актива (фирменный/editorial-индекс) у ВСЕХ строк.
                                                                                backgroundColor: assetColor(h.asset_name) ?? DONUT_COLORS[i % DONUT_COLORS.length],
                                                                            }}
                                                                        />
                                                                        {h.asset_name}
                                                                    </span>
                                                                </td>
                                                                <td style={{
                                                                    padding: '7px 12px',
                                                                    textAlign: 'right',
                                                                    borderBottom: '1px solid color-mix(in srgb, var(--border-color) 60%, transparent)',
                                                                    fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace',
                                                                    color: 'var(--text-primary)',
                                                                    fontWeight: 600,
                                                                }}>
                                                                    {h.weight !== null ? h.weight.toFixed(2) : '—'}
                                                                </td>
                                                            </tr>
                                                            );
                                                        })}
                                                    </tbody>
                                                </table>
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
                    padding: isMobile ? '16px 12px' : '24px 28px',
                    boxShadow: '0 16px 60px rgba(0,0,0,0.3)',
                }}
            >
                {/* Header */}
                <div style={{
                    display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between',
                    marginBottom: 20, paddingBottom: 16,
                    borderBottom: '1.5px solid var(--text-primary)',
                }}>
                    <div>
                        <h3 style={{
                            fontFamily: 'var(--font-serif, Georgia, serif)',
                            fontSize: 'var(--fs-2xl)', margin: 0, marginBottom: 4,
                            color: 'var(--text-primary)',
                        }}>
                            {asset_name}
                        </h3>
                        <div style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-tertiary)' }}>
                            {ticker} {isin && <span style={{ marginLeft: 8 }}>· {isin}</span>}
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
                        height={470}
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
