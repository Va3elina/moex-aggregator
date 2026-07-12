// CombinedPortfolioView — блок «Обзор портфеля» на вкладке «Общий портфель».
//
// Редизайн (июль 2026, макет Claude Design): на десктопе слева таблица бумаг
// «логотип · имя+тикер · полоса · вес · объём · фондов» (полосы нейтральные,
// цвет несёт только карта), справа колонка «Структура» — карта-блоки топ-11
// бумаг брендовыми цветами (площадь ∝ весу) + «Прочие», под ней плитка
// доходности. Режим
// взвешивания (По капиталу / По доле) переключается в шапке блока, рядом
// пилюля актуальности данных (месяц свежайшего снапшота выбранных фондов).
// Наведение на блок карты подсвечивает строку таблицы и наоборот.
//
// variant='embedded' — без собственной карточки: блок живёт внутри общей
// карточки вкладки (единая рамка на «Покупки фондов» + «Обзор портфеля»).
// Мобилка — прежний стек: метрики → пончик → список, строки двухэтажные.
//
// Веса приходят с бэкенда в двух режимах (weight_rub — по деньгам, weight_avg —
// средняя доля по фондам); выбранный режим персистится снаружи (`mode`).

import { useMemo, useState, type CSSProperties } from 'react';
import { DONUT_COLORS, fundAssetName, fundAssetColor, resolveFundTicker } from '../../config/fundConfig';
import Donut from '../funds/Donut';
import InstrumentIcon from '../InstrumentIcon';
import SegmentedControl from '../SegmentedControl';
import HelpTooltip from '../HelpTooltip';
import Dropdown from '../Dropdown';
import { formatReturnPct, returnColor } from '../funds/FundDetailModal';
import type { FundPortfolio, FundPortfolioHolding } from '../../services/api';

type PeriodKey = 'm1' | 'm3' | 'm6' | 'y1';
const PERIOD_LABEL: Record<PeriodKey, string> = { m1: '1 мес', m3: '3 мес', m6: '6 мес', y1: '1 год' };
// Доходность: показываем выбранный период, а если данных нет (молодой фонд) —
// самый длинный доступный (год → 6м → 3м → 1м), чтобы не было «—».
const RET_ORDER: PeriodKey[] = ['y1', 'm6', 'm3', 'm1'];

// Карта «Структура» (slice-and-dice треемап): топ-11 бумаг раскладываются рядами
// 2·3·3·3, «Прочие» — отдельной полосой снизу. ПЛОЩАДЬ плитки пропорциональна её
// весу: высота ряда ∝ сумме весов ряда, ширина плитки внутри ряда ∝ её весу →
// area = rowH × width ∝ вес. Строки отсортированы по убыванию, поэтому верхние
// ряды крупнее. TM_H_BUDGET — суммарная высота рядов (px), TM_ROW_MIN — минимум
// на ряд для читаемости мелких строк.
const TREEMAP_TOP = 11;
const TM_CHUNKS = [2, 3, 3, 3];
const TM_H_BUDGET = 300;
const TM_ROW_MIN = 46;

// Мобилка (пончик): слайсы и превью списка держим равными, наведение на любой
// сектор подсвечивает строку и наоборот, без «слепых» секторов.
const DONUT_TOP = 11;
const LIST_PREVIEW = 11;

// Сетка строки списка (десктоп): логотип · имя (фикс) · полоса · вес · объём · фнд.
const D_GRID = '30px 104px minmax(48px, 1fr) 60px 76px 38px';

// Длинное имя обрезаем плавным затуханием справа (fade-out), не многоточием.
const FADE_MASK = 'linear-gradient(to right, #000 0, #000 calc(100% - 20px), transparent 100%)';
const nameFade: CSSProperties = {
    overflow: 'hidden', whiteSpace: 'nowrap',
    maskImage: FADE_MASK, WebkitMaskImage: FADE_MASK,
};

interface Props {
    portfolio: FundPortfolio | null;
    loading: boolean;
    mode: 'rub' | 'share';        // rub = вес по деньгам, share = средняя доля по фондам
    period: PeriodKey;            // предпочтительный период для строки доходности
    variant?: 'desktop' | 'mobile' | 'embedded';
    // Задан — тумблер «По капиталу / По доле» рендерится в шапке блока
    // (десктоп-макет). Мобилка управляет режимом своими чипами в ⚙️-sheet.
    onModeChange?: (mode: 'rub' | 'share') => void;
    // Клик по бумаге (строка таблицы / плитка карты) → «Потоки по компании».
    onAssetClick?: (h: FundPortfolioHolding) => void;
    // Month-picker актуальности данных (десктоп). Задан availableMonths →
    // пилюля становится кликабельным Dropdown'ом месяцев. asOf undefined =
    // последний. monthLocked/onMonthLockedClick — гейт свежего среза (Free/гость).
    availableMonths?: string[];
    asOf?: string;
    onAsOfChange?: (m: string) => void;
    monthLocked?: (m: string) => boolean;
    onMonthLockedClick?: () => void;
}

const blockStyle = (isMobile: boolean): CSSProperties => ({
    background: 'var(--bg-secondary)',
    border: '2px solid var(--text-primary)',
    borderRadius: 16,
    boxShadow: '4px 4px 0 var(--text-primary)',
    padding: isMobile ? '12px 14px 14px' : '13px 16px 15px',
    position: 'relative',
    minWidth: 0,
});

function plural(n: number, one: string, few: string, many: string): string {
    const m10 = n % 10, m100 = n % 100;
    if (m10 === 1 && m100 !== 11) return one;
    if (m10 >= 2 && m10 <= 4 && (m100 < 10 || m100 >= 20)) return few;
    return many;
}

const MONTHS_LOWER = ['январь', 'февраль', 'март', 'апрель', 'май', 'июнь',
    'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь'];

// "2026-07-31" → "июль 2026" — для пилюли актуальности данных.
function monthYearLower(iso: string): string {
    const d = new Date(iso);
    return `${MONTHS_LOWER[d.getMonth()]} ${d.getFullYear()}`;
}

// "2026-07-31" → "Июль 2026" — для пунктов month-picker (с заглавной).
function monthYearCap(iso: string): string {
    const s = monthYearLower(iso);
    return s.charAt(0).toUpperCase() + s.slice(1);
}

function StatTile({ label, value, color }: { label: string; value: string; color?: string }) {
    return (
        <div style={{ background: 'var(--bg-primary)', border: '1px solid var(--border-color)', borderRadius: 10, padding: '8px 11px', minWidth: 0 }}>
            <div style={{ fontSize: 'var(--fs-2xs)', fontWeight: 800, textTransform: 'uppercase', letterSpacing: '0.04em', color: 'var(--text-muted)', marginBottom: 2, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                {label}
            </div>
            <div style={{ fontSize: 'var(--fs-md)', fontWeight: 800, fontVariantNumeric: 'tabular-nums', color: color ?? 'var(--text-primary)', lineHeight: 1.1, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                {value}
            </div>
        </div>
    );
}

// Компактный объём: «112.7 млрд», «540 млн», «12 тыс» (без ₽ — валюта в шапке).
function fmtVolShort(v: number): string {
    if (v >= 1e9) return `${(v / 1e9).toFixed(1)} млрд`;
    if (v >= 1e6) return `${(v / 1e6).toFixed(0)} млн`;
    return `${Math.round(v / 1e3)} тыс`;
}

// Итог структуры: «196,0 млрд ₽» — запятая как в остальных рублёвых шапках.
function fmtTotalRub(v: number): string {
    if (v >= 1e9) return `${(v / 1e9).toFixed(1).replace('.', ',')} млрд ₽`;
    return `${(v / 1e6).toFixed(0)} млн ₽`;
}

// Тёмный текст на светлых брендовых заливках (золото Полюса и т.п.).
function isLightHex(color: string): boolean {
    if (!/^#[0-9a-fA-F]{6}$/.test(color)) return false;
    const r = parseInt(color.slice(1, 3), 16), g = parseInt(color.slice(3, 5), 16), b = parseInt(color.slice(5, 7), 16);
    return 0.2126 * r + 0.7152 * g + 0.0722 * b > 160;
}

// Логотип бумаги: InstrumentIcon по резолвнутому тикеру (внутри свой фолбэк-круг),
// иначе цветной круг с первой буквой.
function AssetLogo({ h, size, color }: { h: FundPortfolioHolding; size: number; color: string }) {
    const ticker = resolveFundTicker(h.asset_name, h.isin);
    if (ticker) return <InstrumentIcon sectype={ticker} size={size} rounded="full" />;
    return (
        <span style={{ width: size, height: size, borderRadius: '50%', background: color, color: '#fff', fontSize: Math.round(size * 0.42), fontWeight: 800, display: 'inline-flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0, lineHeight: 1 }}>
            {(fundAssetName(h.asset_name, h.isin).trim().charAt(0) || '?').toUpperCase()}
        </span>
    );
}

export default function CombinedPortfolioView({ portfolio, loading, mode, period, variant = 'desktop', onModeChange, onAssetClick, availableMonths, asOf, onAsOfChange, monthLocked, onMonthLockedClick }: Props) {
    const [modalOpen, setModalOpen] = useState(false);
    const [hoverIdx, setHoverIdx] = useState<number | null>(null);
    const isMobile = variant === 'mobile';
    const embedded = variant === 'embedded';
    // embedded — без собственной карточки: рамку несёт общая карточка вкладки.
    // embedded — тянемся на всю высоту ячейки (outer grid stretch), чтобы футер
    // «Прочие бумаги» можно было прижать к нижнему краю блока (marginTop:auto).
    const wrapStyle: CSSProperties = embedded
        ? { position: 'relative', minWidth: 0, display: 'flex', flexDirection: 'column', height: '100%' }
        : blockStyle(isMobile);

    const wOf = (h: FundPortfolioHolding) => (mode === 'rub' ? h.weight_rub : h.weight_avg);

    const sorted = useMemo(() => {
        if (!portfolio) return [];
        return [...portfolio.holdings].sort((a, b) => wOf(b) - wOf(a));
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [portfolio, mode]);

    const { donutHoldings, donutColors } = useMemo(() => {
        const top = sorted.slice(0, DONUT_TOP);
        const otherW = sorted.slice(DONUT_TOP).reduce((s, h) => s + wOf(h), 0);
        const items = top.map((h) => ({ name: fundAssetName(h.asset_name, h.isin), weight: wOf(h) }));
        if (otherW > 0.5) items.push({ name: 'Прочее', weight: otherW });
        const colors = items.map((it, i) =>
            it.name === 'Прочее'
                ? 'var(--text-muted)'
                : (fundAssetColor(top[i]?.asset_name ?? it.name, top[i]?.isin ?? null) ?? DONUT_COLORS[i % DONUT_COLORS.length]),
        );
        return { donutHoldings: items, donutColors: colors };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [sorted, mode]);

    if (loading && !portfolio) {
        return <div style={{ ...wrapStyle, color: 'var(--text-muted)', fontSize: 'var(--fs-sm)' }}>Собираем общий портфель…</div>;
    }
    if (!portfolio || portfolio.num_funds === 0 || sorted.length === 0) {
        return (
            <div style={{ ...wrapStyle, textAlign: 'center' }}>
                <p style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-secondary)', maxWidth: 420, margin: '12px auto', lineHeight: 1.5 }}>
                    Нет данных по выбранным управляющим компаниям. Выберите другие УК или дождитесь публикации составов.
                </p>
            </div>
        );
    }

    // Доходность: предпочтительный период, иначе самый длинный доступный.
    const rr = portfolio.returns;
    let retK: PeriodKey = period;
    if (rr[period] == null) retK = RET_ORDER.find((k) => rr[k] != null) ?? period;
    const ret = rr[retK];

    const maxW = Math.max(...sorted.map(wOf), 0.0001);
    const rest = sorted.slice(LIST_PREVIEW);

    // Свежесть данных: месяц самого свежего снапшота среди выбранных фондов.
    const freshISO = portfolio.funds.reduce<string | null>(
        (acc, f) => (f.snapshot_date && (!acc || f.snapshot_date > acc) ? f.snapshot_date : acc), null);

    const kpis = (
        <>
            <StatTile label="Объём в фондах" value={`${fmtVolShort(portfolio.total_value_rub)} ₽`} />
            <StatTile label={`Доходность · ${PERIOD_LABEL[retK]}`} value={formatReturnPct(ret ?? undefined)} color={returnColor(ret ?? undefined)} />
        </>
    );

    const deskRow = (h: FundPortfolioHolding, idx: number, last: boolean, interactive: boolean) => {
        const w = wOf(h);
        const color = fundAssetColor(h.asset_name, h.isin) ?? DONUT_COLORS[idx % DONUT_COLORS.length];
        const pct = Math.max(2, (w / maxW) * 100);
        const ticker = resolveFundTicker(h.asset_name, h.isin);
        const hov = interactive && hoverIdx === idx;
        const link = interactive && idx < TREEMAP_TOP;
        // Клик по бумаге (только в интерактивном превью, не в модалке) → потоки по компании.
        const click = interactive && onAssetClick ? () => onAssetClick(h) : undefined;
        return (
            <div
                key={h.akey}
                onMouseEnter={link ? () => setHoverIdx(idx) : undefined}
                onMouseLeave={link ? () => setHoverIdx(null) : undefined}
                onClick={click}
                role={click ? 'button' : undefined}
                tabIndex={click ? 0 : undefined}
                onKeyDown={click ? (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); click(); } } : undefined}
                title={click ? `По бумаге: ${fundAssetName(h.asset_name, h.isin)}` : undefined}
                style={{ display: 'grid', gridTemplateColumns: D_GRID, gap: 10, alignItems: 'center', padding: '7px 8px', margin: '0 -8px', borderRadius: 8, cursor: click ? 'pointer' : 'default', background: hov ? 'color-mix(in srgb, var(--text-primary) 5%, transparent)' : 'transparent', borderBottom: last ? 'none' : '1px dashed color-mix(in srgb, var(--text-primary) 12%, transparent)', transition: 'background 0.12s ease' }}
            >
                <AssetLogo h={h} size={30} color={color} />
                <span style={{ minWidth: 0, display: 'flex', flexDirection: 'column', lineHeight: 1.12 }}>
                    <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 700, color: 'var(--text-primary)', ...nameFade }}>{fundAssetName(h.asset_name, h.isin)}</span>
                    {ticker && <span style={{ fontFamily: 'var(--font-mono, ui-monospace, monospace)', fontSize: 'var(--fs-3xs, 10px)', letterSpacing: '0.05em', color: 'var(--text-muted)', ...nameFade }}>{ticker}</span>}
                </span>
                {/* Полосы нейтральные — цвет несёт карта «Структура» справа. */}
                <div style={{ height: 9, background: 'color-mix(in srgb, var(--text-primary) 8%, transparent)', borderRadius: 5, overflow: 'hidden', minWidth: 0 }}>
                    <div style={{ width: `${pct}%`, height: '100%', background: 'color-mix(in srgb, var(--text-primary) 32%, transparent)', borderRadius: 5 }} />
                </div>
                <span style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums', fontSize: 'var(--fs-sm)', fontWeight: 800, color: 'var(--text-primary)' }}>{w.toFixed(2)}%</span>
                <span style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums', fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>{fmtVolShort(h.value_rub)}</span>
                <span style={{ textAlign: 'right', fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)' }}>{h.funds_holding} ф.</span>
            </div>
        );
    };

    const mobRow = (h: FundPortfolioHolding, idx: number, last: boolean) => {
        const w = wOf(h);
        const color = fundAssetColor(h.asset_name, h.isin) ?? DONUT_COLORS[idx % DONUT_COLORS.length];
        const pct = Math.max(2, (w / maxW) * 100);
        const link = idx < DONUT_TOP;
        return (
            <div
                key={h.akey}
                onMouseEnter={link ? () => setHoverIdx(idx) : undefined}
                onMouseLeave={link ? () => setHoverIdx(null) : undefined}
                style={{ padding: '8px 2px', borderBottom: last ? 'none' : '1px dashed color-mix(in srgb, var(--text-primary) 12%, transparent)' }}
            >
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <AssetLogo h={h} size={22} color={color} />
                    <span style={{ flex: 1, minWidth: 0, fontSize: 'var(--fs-sm)', fontWeight: 600, color: 'var(--text-primary)', ...nameFade }}>{fundAssetName(h.asset_name, h.isin)}</span>
                    <span style={{ fontVariantNumeric: 'tabular-nums', fontSize: 'var(--fs-sm)', fontWeight: 800, color: 'var(--text-primary)', flexShrink: 0 }}>{w.toFixed(2)}%</span>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 5, paddingLeft: 30 }}>
                    <div style={{ flex: 1, minWidth: 0, height: 6, background: 'color-mix(in srgb, var(--text-primary) 8%, transparent)', borderRadius: 4, overflow: 'hidden' }}>
                        <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: 4 }} />
                    </div>
                    <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', flexShrink: 0, minWidth: 92, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{fmtVolShort(h.value_rub)} ₽ · {h.funds_holding} ф.</span>
                </div>
            </div>
        );
    };

    const listHeader = (
        <div style={{ display: 'grid', gridTemplateColumns: D_GRID, gap: 10, padding: '4px 0 8px', borderBottom: '1.5px solid var(--text-primary)', fontSize: 'var(--fs-3xs, 10px)', fontWeight: 800, letterSpacing: '0.06em', textTransform: 'uppercase', color: 'var(--text-muted)' }}>
            <span /><span>Бумага</span><span /><span style={{ textAlign: 'right' }}>Вес</span><span style={{ textAlign: 'right' }}>Объём</span><span style={{ textAlign: 'right' }}>Фнд</span>
        </div>
    );

    // Неяркая мелкая надпись-ссылка (как в макете): по умолчанию приглушённая,
    // на hover темнеет и подчёркивается — «становится кликабельной». Без рамки и
    // фона, чтобы не выглядела полноценной кнопкой.
    // marginTop:auto — прижимает футер к низу колонки-таблицы (десктоп, flex-col).
    // На мобилке (обычный поток) auto схлопывается в 0 и просто идёт под списком.
    const restBtn = sorted.length > LIST_PREVIEW && (
        <div style={{ display: 'flex', justifyContent: 'flex-end', alignItems: 'center', paddingTop: 9, marginTop: 'auto' }}>
            <button
                onClick={() => setModalOpen(true)}
                onMouseEnter={(e) => { e.currentTarget.style.color = 'var(--text-primary)'; e.currentTarget.style.textDecoration = 'underline'; }}
                onMouseLeave={(e) => { e.currentTarget.style.color = 'var(--text-muted)'; e.currentTarget.style.textDecoration = 'none'; }}
                onFocus={(e) => { e.currentTarget.style.color = 'var(--text-primary)'; e.currentTarget.style.textDecoration = 'underline'; }}
                onBlur={(e) => { e.currentTarget.style.color = 'var(--text-muted)'; e.currentTarget.style.textDecoration = 'none'; }}
                style={{ display: 'inline-flex', alignItems: 'center', gap: 5, padding: 0, background: 'transparent', border: 'none', fontSize: 'var(--fs-xs)', fontWeight: 600, color: 'var(--text-muted)', cursor: 'pointer', whiteSpace: 'nowrap', transition: 'color 0.12s ease' }}
            >
                Прочие бумаги · ещё {rest.length} <span style={{ fontSize: '0.85em' }}>↓</span>
            </button>
        </div>
    );

    // Модалка полного состава — поверх страницы, свой скролл, страница не растягивается.
    const modal = modalOpen && (
        <div onClick={() => setModalOpen(false)} style={{ position: 'fixed', inset: 0, zIndex: 200, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 16 }}>
            <div onClick={(e) => e.stopPropagation()} style={{ background: 'var(--bg-secondary)', border: '2px solid var(--text-primary)', borderRadius: 16, boxShadow: '6px 6px 0 var(--text-primary)', width: 'min(720px, 100%)', maxHeight: '84vh', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '12px 16px', borderBottom: '1.5px solid var(--text-primary)' }}>
                    <span style={{ fontSize: 'var(--fs-md)', fontWeight: 800 }}>Полный состав · {portfolio.num_assets} {plural(portfolio.num_assets, 'бумага', 'бумаги', 'бумаг')}</span>
                    <button onClick={() => setModalOpen(false)} aria-label="Закрыть" style={{ width: 32, height: 32, flexShrink: 0, border: '1.5px solid var(--text-primary)', background: 'var(--bg-secondary)', borderRadius: 8, cursor: 'pointer', fontSize: 'var(--fs-sm)', color: 'var(--text-primary)' }}>✕</button>
                </div>
                <div style={{ flex: 1, overflowY: 'auto', padding: '2px 16px 14px' }}>
                    <div style={{ position: 'sticky', top: 0, background: 'var(--bg-secondary)' }}>{listHeader}</div>
                    {sorted.map((h, i) => deskRow(h, i, i === sorted.length - 1, false))}
                </div>
            </div>
        </div>
    );

    if (isMobile) {
        return (
            <div style={wrapStyle}>
                <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', gap: 10, paddingBottom: 9, marginBottom: 13, borderBottom: '1.5px solid var(--text-primary)' }}>
                    <span style={{ fontSize: 'var(--fs-md)', fontWeight: 800, letterSpacing: '-0.01em', color: 'var(--text-primary)' }}>Обзор портфеля</span>
                    <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', fontWeight: 600, whiteSpace: 'nowrap' }}>{portfolio.num_funds} ф. · {portfolio.num_assets} бум.</span>
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8, marginBottom: 12 }}>{kpis}</div>
                <div style={{ display: 'flex', justifyContent: 'center', marginBottom: 6 }}>
                    <Donut holdings={donutHoldings} colors={donutColors} size={200} outerRadius={90} innerRadius={60} maxSlices={donutHoldings.length} centerCount={portfolio.num_assets} showCenterText highlightIndex={hoverIdx} onHoverChange={setHoverIdx} />
                </div>
                <div>
                    {sorted.slice(0, LIST_PREVIEW).map((h, i) => mobRow(h, i, i === Math.min(LIST_PREVIEW, sorted.length) - 1))}
                    {restBtn}
                </div>
                {modal}
            </div>
        );
    }

    // ── Десктоп: карта «Структура» (slice-and-dice, площадь ∝ весу) ──
    // Ряды 2·3·3·3 из топ-11; «Прочие» — отдельной полосой снизу. Высота ряда ∝
    // сумме весов ряда, ширина плитки ∝ её весу → площадь ∝ вес (мелкие бумаги
    // видимо меньше крупных, а не наравне, как было при равных рядах).
    const tmTop = sorted.slice(0, TREEMAP_TOP);
    const tmRestW = sorted.slice(TREEMAP_TOP).reduce((s, h) => s + wOf(h), 0);
    const tmTopSum = Math.max(tmTop.reduce((s, h) => s + wOf(h), 0), 0.0001);
    const tmRows: { h: FundPortfolioHolding; idx: number }[][] = [];
    let cursor = 0;
    for (const size of TM_CHUNKS) {
        if (cursor >= tmTop.length) break;
        tmRows.push(tmTop.slice(cursor, cursor + size).map((h, j) => ({ h, idx: cursor + j })));
        cursor += size;
    }

    const treemap = (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {tmRows.map((row, ri) => {
                const rowSum = row.reduce((s, { h }) => s + wOf(h), 0);
                // Высота ряда ∝ его суммарному весу (доля от бюджета высоты), но не
                // ниже минимума — иначе нижние лёгкие ряды становятся нечитаемыми.
                const rowH = Math.max(TM_ROW_MIN, Math.round(TM_H_BUDGET * rowSum / tmTopSum));
                return (
                    <div key={ri} style={{ display: 'flex', gap: 6, height: rowH }}>
                        {row.map(({ h, idx }) => {
                            const w = wOf(h);
                            const color = fundAssetColor(h.asset_name, h.isin) ?? DONUT_COLORS[idx % DONUT_COLORS.length];
                            const dark = isLightHex(color);
                            const label = resolveFundTicker(h.asset_name, h.isin) ?? fundAssetName(h.asset_name, h.isin).split(' ')[0];
                            const click = onAssetClick ? () => onAssetClick(h) : undefined;
                            return (
                                <div
                                    key={h.akey}
                                    onMouseEnter={() => setHoverIdx(idx)}
                                    onMouseLeave={() => setHoverIdx(null)}
                                    onClick={click}
                                    role={click ? 'button' : undefined}
                                    tabIndex={click ? 0 : undefined}
                                    onKeyDown={click ? (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); click(); } } : undefined}
                                    title={`${fundAssetName(h.asset_name, h.isin)} · ${w.toFixed(2)}%${click ? ' — потоки по компании' : ''}`}
                                    // Ширина ∝ весу (flex). minWidth — мягкий пол, чтобы очень
                                    // мелкая плитка сохранила подпись; ряды отсортированы, поэтому
                                    // соседи близки по весу и пол почти не искажает пропорции.
                                    // Подпись прижата к верхнему левому углу (justify-start),
                                    // padding симметричный со всех сторон (отступ сверху = слева).
                                    // lineHeight:1 + gap дают гарантированный зазор: при
                                    // минимальной высоте ряда (46px) 8+14+2+11+8=43 < 46, тикер
                                    // и % не наезжают.
                                    style={{ flex: Math.max(w, 0.1), minWidth: 44, borderRadius: 10, background: color, color: dark ? '#1a1712' : '#fff', padding: 8, display: 'flex', flexDirection: 'column', justifyContent: 'flex-start', gap: 2, overflow: 'hidden', outline: hoverIdx === idx ? '2px solid var(--text-primary)' : 'none', outlineOffset: -2, transition: 'outline-color 0.12s ease', cursor: click ? 'pointer' : 'default' }}
                                >
                                    <div style={{ fontWeight: 800, fontSize: 'var(--fs-sm)', lineHeight: 1, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{label}</div>
                                    <div style={{ fontSize: 'var(--fs-2xs)', fontWeight: 600, lineHeight: 1, opacity: 0.92, fontVariantNumeric: 'tabular-nums' }}>{w.toFixed(1).replace('.', ',')}%</div>
                                </div>
                            );
                        })}
                    </div>
                );
            })}
            {tmRestW > 0.5 && (
                <div style={{ height: 42, borderRadius: 10, background: 'color-mix(in srgb, var(--text-primary) 16%, var(--bg-primary))', color: 'var(--text-primary)', padding: '0 12px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8 }}>
                    <span style={{ fontWeight: 700, fontSize: 'var(--fs-xs)' }}>Прочие бумаги</span>
                    <span style={{ fontWeight: 700, fontSize: 'var(--fs-xs)', fontVariantNumeric: 'tabular-nums' }}>{tmRestW.toFixed(1).replace('.', ',')}%</span>
                </div>
            )}
        </div>
    );

    // Свежесть среза: freshestMonth — самый свежий ДОСТУПНЫЙ месяц (не locked),
    // он же дефолт month-picker'а. Зелёный кружок «актуальные данные» показываем
    // ТОЛЬКО когда выбран он; на историческом месяце кружка нет.
    const freshestMonth = availableMonths?.find((m) => !(monthLocked?.(m))) ?? availableMonths?.[0];
    const currentMonth = asOf ?? freshestMonth;
    const isFreshest = !!freshestMonth && currentMonth === freshestMonth;

    return (
        <div style={wrapStyle}>
            {/* Шапка: заголовок+счётчики слева; пилюля актуальности + режим справа. */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 14, flexWrap: 'wrap', marginBottom: 16 }}>
                <div>
                    <div style={{ fontSize: 'var(--fs-lg)', fontWeight: 800, letterSpacing: '-0.01em', color: 'var(--text-primary)' }}>Обзор портфеля</div>
                    <div style={{ fontSize: 'var(--fs-xs)', fontWeight: 600, color: 'var(--text-muted)', marginTop: 2, fontVariantNumeric: 'tabular-nums' }}>
                        {portfolio.num_funds} {plural(portfolio.num_funds, 'фонд', 'фонда', 'фондов')} · {portfolio.num_assets} {plural(portfolio.num_assets, 'бумага', 'бумаги', 'бумаг')}
                    </div>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
                    {availableMonths && availableMonths.length > 0 && onAsOfChange ? (
                        // Кликабельный month-picker. Зелёный кружок «актуальные данные»
                        // рендерим ТОЛЬКО когда выбран самый свежий срез; hover по нему
                        // поясняет, что это последний доступный снапшот.
                        <div style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
                            {isFreshest && (
                                <span
                                    title="Актуальные данные — показан самый свежий доступный срез портфеля"
                                    style={{ width: 10, height: 10, borderRadius: '50%', background: 'var(--mood-green, #4a9959)', flexShrink: 0, cursor: 'help' }}
                                />
                            )}
                            <Dropdown<string>
                                options={availableMonths.map((m) => ({ key: m, label: monthYearCap(m), locked: monthLocked?.(m) }))}
                                value={currentMonth ?? availableMonths[0]}
                                onChange={onAsOfChange}
                                onLockedClick={onMonthLockedClick ? () => onMonthLockedClick() : undefined}
                                minWidth={132}
                            />
                        </div>
                    ) : freshISO && (
                        <div title="Месяц самого свежего снапшота выбранных фондов" style={{ display: 'inline-flex', alignItems: 'center', gap: 8, border: '1.5px solid var(--text-primary)', borderRadius: 999, padding: '7px 14px', fontSize: 'var(--fs-xs)', fontWeight: 700, color: 'var(--text-primary)', whiteSpace: 'nowrap' }}>
                            <span style={{ width: 8, height: 8, borderRadius: '50%', background: 'var(--mood-green, #4a9959)', flexShrink: 0 }} />
                            Актуальные данные <span style={{ color: 'var(--text-muted)', fontWeight: 600 }}>· {monthYearLower(freshISO)}</span>
                        </div>
                    )}
                    {onModeChange && (
                        <SegmentedControl<'rub' | 'share'>
                            options={[
                                { key: 'rub', label: 'По капиталу' },
                                { key: 'share', label: 'По доле' },
                            ]}
                            value={mode}
                            onChange={onModeChange}
                            trailing={
                                <HelpTooltip
                                    align="right"
                                    title="Как считается вес"
                                    sections={[
                                        { heading: 'По капиталу', body: 'Доля бумаги от суммарной стоимости всех позиций выбранных фондов: крупные фонды влияют сильнее.' },
                                        { heading: 'По доле', body: 'Средняя доля бумаги по фондам, каждый фонд с равным весом — виден консенсус управляющих без перекоса на гигантов.' },
                                    ]}
                                />
                            }
                        />
                    )}
                </div>
            </div>

            {/* flex:1 — сетка забирает высоту под шапкой; stretch тянет колонку-
                таблицу на полную высоту блока, чтобы футер (marginTop:auto) сел
                у нижнего края, а не висел сразу под последней строкой. */}
            <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) 264px', gap: 26, alignItems: 'stretch', flex: 1, minHeight: 0 }}>
                {/* Таблица бумаг */}
                <div style={{ minWidth: 0, display: 'flex', flexDirection: 'column' }}>
                    {listHeader}
                    {sorted.slice(0, LIST_PREVIEW).map((h, i) => deskRow(h, i, i === Math.min(LIST_PREVIEW, sorted.length) - 1, true))}
                    {restBtn}
                </div>

                {/* Структура + доходность */}
                <div style={{ minWidth: 0 }}>
                    <div style={{ fontSize: 'var(--fs-3xs, 10px)', fontWeight: 800, letterSpacing: '0.07em', textTransform: 'uppercase', color: 'var(--text-muted)', margin: '2px 0 10px' }}>
                        Структура · <span style={{ color: 'var(--text-primary)', fontVariantNumeric: 'tabular-nums' }}>{fmtTotalRub(portfolio.total_value_rub)}</span>
                    </div>
                    {treemap}
                    <div style={{ marginTop: 16 }}>
                        <div style={{ background: 'var(--bg-secondary)', border: '1.5px solid var(--text-primary)', borderRadius: 14, padding: '12px 16px', boxShadow: '3px 3px 0 color-mix(in srgb, var(--text-primary) 12%, transparent)' }}>
                            <div style={{ fontSize: 'var(--fs-3xs, 10px)', fontWeight: 800, letterSpacing: '0.07em', textTransform: 'uppercase', color: 'var(--text-muted)' }}>Доходность · {PERIOD_LABEL[retK]}</div>
                            <div style={{ fontSize: 'var(--fs-xl, 22px)', fontWeight: 800, marginTop: 4, fontVariantNumeric: 'tabular-nums', color: returnColor(ret ?? undefined) }}>{formatReturnPct(ret ?? undefined)}</div>
                        </div>
                    </div>
                </div>
            </div>
            {modal}
        </div>
    );
}
