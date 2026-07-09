// CombinedPortfolioView — блок «Обзор портфеля» на вкладке «Общий портфель».
//
// Утверждённый дизайн (июль 2026): единый белый блок с заголовком. Внутри слева
// пончик и метрики 2×2, справа (за разделителем) список бумаг строками
// «логотип · имя+тикер · полоса · вес % · объём · фондов». Полоса стоит сразу за
// именем фиксированной ширины (длинные имена усекаются), у всех строк на одной
// вертикали. По умолчанию показываем топ-11 (= числу секторов пончика), кнопка
// «Все N» открывает полный список МОДАЛКОЙ поверх, не растягивая страницу.
//
// Веса приходят с бэкенда в двух режимах (weight_rub — по деньгам, weight_avg —
// средняя доля по фондам); режим выбирается снаружи (`mode`). Компонент общий для
// десктопа и мобилки (мобилка: метрики → пончик → список стеком, строки двухэтажные).

import { useMemo, useState, type CSSProperties } from 'react';
import { DONUT_COLORS, fundAssetName, fundAssetColor, resolveFundTicker } from '../../config/fundConfig';
import Donut from '../funds/Donut';
import InstrumentIcon from '../InstrumentIcon';
import { formatReturnPct, returnColor } from '../funds/FundDetailModal';
import type { FundPortfolio, FundPortfolioHolding } from '../../services/api';

type PeriodKey = 'm1' | 'm3' | 'm6' | 'y1';
const PERIOD_LABEL: Record<PeriodKey, string> = { m1: '1 мес', m3: '3 мес', m6: '6 мес', y1: '1 год' };
// Доходность: показываем выбранный период, а если данных нет (молодой фонд) —
// самый длинный доступный (год → 6м → 3м → 1м), чтобы не было «—».
const RET_ORDER: PeriodKey[] = ['y1', 'm6', 'm3', 'm1'];

// Слайсы пончика и превью списка держим равными: наведение на любой сектор
// подсвечивает строку и наоборот, без «слепых» секторов.
const DONUT_TOP = 11;
const LIST_PREVIEW = 11;

// Сетка строки списка (десктоп): логотип · имя (фикс) · полоса · вес · объём · фнд.
const D_GRID = '26px 104px minmax(48px, 1fr) 52px 72px 34px';

interface Props {
    portfolio: FundPortfolio | null;
    loading: boolean;
    mode: 'rub' | 'share';        // rub = вес по деньгам, share = средняя доля по фондам
    period: PeriodKey;            // предпочтительный период для строки доходности
    variant?: 'desktop' | 'mobile';
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

function BlockHead({ title, meta }: { title: string; meta?: string }) {
    return (
        <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', gap: 10, paddingBottom: 9, marginBottom: 13, borderBottom: '1.5px solid var(--text-primary)' }}>
            <span style={{ fontSize: 'var(--fs-md)', fontWeight: 800, letterSpacing: '-0.01em', color: 'var(--text-primary)' }}>{title}</span>
            {meta && <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', fontWeight: 600, whiteSpace: 'nowrap' }}>{meta}</span>}
        </div>
    );
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

export default function CombinedPortfolioView({ portfolio, loading, mode, period, variant = 'desktop' }: Props) {
    const [modalOpen, setModalOpen] = useState(false);
    const [hoverIdx, setHoverIdx] = useState<number | null>(null);
    const isMobile = variant === 'mobile';

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
        return <div style={{ ...blockStyle(isMobile), color: 'var(--text-muted)', fontSize: 'var(--fs-sm)' }}>Собираем общий портфель…</div>;
    }
    if (!portfolio || portfolio.num_funds === 0 || sorted.length === 0) {
        return (
            <div style={{ ...blockStyle(isMobile), textAlign: 'center' }}>
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
    const restW = rest.reduce((s, h) => s + wOf(h), 0);
    const restV = rest.reduce((s, h) => s + h.value_rub, 0);

    const kpis = (
        <>
            <StatTile label="Объём в фондах" value={`${fmtVolShort(portfolio.total_value_rub)} ₽`} />
            <StatTile label={`Доходность · ${PERIOD_LABEL[retK]}`} value={formatReturnPct(ret ?? undefined)} color={returnColor(ret ?? undefined)} />
            <StatTile label="Фондов" value={String(portfolio.num_funds)} />
            <StatTile label="Бумаг" value={String(portfolio.num_assets)} />
        </>
    );

    const deskRow = (h: FundPortfolioHolding, idx: number, last: boolean, interactive: boolean) => {
        const w = wOf(h);
        const color = fundAssetColor(h.asset_name, h.isin) ?? DONUT_COLORS[idx % DONUT_COLORS.length];
        const pct = Math.max(2, (w / maxW) * 100);
        const ticker = resolveFundTicker(h.asset_name, h.isin);
        const hov = interactive && hoverIdx === idx;
        const link = interactive && idx < DONUT_TOP;
        return (
            <div
                key={h.akey}
                onMouseEnter={link ? () => setHoverIdx(idx) : undefined}
                onMouseLeave={link ? () => setHoverIdx(null) : undefined}
                style={{ display: 'grid', gridTemplateColumns: D_GRID, gap: 10, alignItems: 'center', padding: '6px 8px', margin: '0 -8px', borderRadius: 8, background: hov ? 'color-mix(in srgb, var(--text-primary) 5%, transparent)' : 'transparent', borderBottom: last ? 'none' : '1px dashed color-mix(in srgb, var(--text-primary) 12%, transparent)', transition: 'background 0.12s ease' }}
            >
                <AssetLogo h={h} size={26} color={color} />
                <span style={{ minWidth: 0, display: 'flex', flexDirection: 'column', lineHeight: 1.12 }}>
                    <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 700, color: 'var(--text-primary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '100%' }}>{fundAssetName(h.asset_name, h.isin)}</span>
                    {ticker && <span style={{ fontFamily: 'var(--font-mono, ui-monospace, monospace)', fontSize: 'var(--fs-3xs, 10px)', letterSpacing: '0.05em', color: 'var(--text-muted)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '100%' }}>{ticker}</span>}
                </span>
                <div style={{ height: 8, background: 'color-mix(in srgb, var(--text-primary) 8%, transparent)', borderRadius: 4, overflow: 'hidden', minWidth: 0 }}>
                    <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: 4 }} />
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
                    <span style={{ flex: 1, minWidth: 0, fontSize: 'var(--fs-sm)', fontWeight: 600, color: 'var(--text-primary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{fundAssetName(h.asset_name, h.isin)}</span>
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
        <div style={{ display: 'grid', gridTemplateColumns: D_GRID, gap: 10, padding: '4px 0 7px', borderBottom: '1.5px solid var(--text-primary)', fontSize: 'var(--fs-3xs, 10px)', fontWeight: 800, letterSpacing: '0.06em', textTransform: 'uppercase', color: 'var(--text-muted)' }}>
            <span /><span>Бумага</span><span /><span style={{ textAlign: 'right' }}>Вес</span><span style={{ textAlign: 'right' }}>Объём</span><span style={{ textAlign: 'right' }}>Фнд</span>
        </div>
    );

    const footer = (
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 10, paddingTop: 9, marginTop: 4, flexWrap: 'wrap' }}>
            {rest.length > 0 ? (
                <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', fontVariantNumeric: 'tabular-nums' }}>
                    Прочее · {rest.length} {plural(rest.length, 'бумага', 'бумаги', 'бумаг')} · {restW.toFixed(1)}% · {fmtVolShort(restV)} ₽
                </span>
            ) : <span />}
            {sorted.length > LIST_PREVIEW && (
                <button onClick={() => setModalOpen(true)} className="editorial-press" style={{ padding: '5px 14px', background: 'var(--bg-primary)', border: '1.5px solid var(--border-color)', borderRadius: 999, fontSize: 'var(--fs-xs)', fontWeight: 700, color: 'var(--text-secondary)', cursor: 'pointer', whiteSpace: 'nowrap' }}>
                    Все {sorted.length} ↓
                </button>
            )}
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
            <div style={blockStyle(true)}>
                <BlockHead title="Обзор портфеля" meta={`${portfolio.num_funds} · ${portfolio.num_assets} бум.`} />
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8, marginBottom: 12 }}>{kpis}</div>
                <div style={{ display: 'flex', justifyContent: 'center', marginBottom: 6 }}>
                    <Donut holdings={donutHoldings} colors={donutColors} size={200} outerRadius={90} innerRadius={60} maxSlices={donutHoldings.length} centerCount={portfolio.num_assets} showCenterText highlightIndex={hoverIdx} onHoverChange={setHoverIdx} />
                </div>
                <div>
                    {sorted.slice(0, LIST_PREVIEW).map((h, i) => mobRow(h, i, i === Math.min(LIST_PREVIEW, sorted.length) - 1))}
                    {footer}
                </div>
                {modal}
            </div>
        );
    }

    return (
        <div style={blockStyle(false)}>
            <BlockHead title="Обзор портфеля" meta={`${portfolio.num_funds} ${plural(portfolio.num_funds, 'фонд', 'фонда', 'фондов')} · ${portfolio.num_assets} ${plural(portfolio.num_assets, 'бумага', 'бумаги', 'бумаг')}`} />
            <div style={{ display: 'grid', gridTemplateColumns: '240px minmax(0, 1fr)', gap: 20, alignItems: 'start' }}>
                <div style={{ minWidth: 0 }}>
                    <div style={{ display: 'flex', justifyContent: 'center' }}>
                        <Donut holdings={donutHoldings} colors={donutColors} size={210} outerRadius={90} innerRadius={60} maxSlices={donutHoldings.length} centerCount={portfolio.num_assets} showCenterText highlightIndex={hoverIdx} onHoverChange={setHoverIdx} />
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8, marginTop: 12 }}>{kpis}</div>
                </div>
                <div style={{ borderLeft: '1.5px solid var(--border-color)', paddingLeft: 18, minWidth: 0 }}>
                    {listHeader}
                    {sorted.slice(0, LIST_PREVIEW).map((h, i) => deskRow(h, i, i === Math.min(LIST_PREVIEW, sorted.length) - 1, true))}
                    {footer}
                </div>
            </div>
            {modal}
        </div>
    );
}
