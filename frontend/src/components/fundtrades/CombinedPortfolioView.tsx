// CombinedPortfolioView — «Общий портфель»: горизонтальная компоновка в один экран.
//
// Утверждённый дизайн (июль 2026): слева белая карточка с пончиком и метрики 2×2
// под ним; справа белая карточка-список бумаг строками «логотип · имя · бар веса ·
// вес % · объём ₽ · фондов». Бар в строке — горизонтальная полоса в цвете сектора
// пончика (ширина от максимального веса). Хвост списка за кнопкой «Показать все»,
// чтобы вкладка по умолчанию умещалась в экран без вертикального скролла.
//
// Веса приходят с бэкенда в двух режимах (weight_rub — по деньгам, weight_avg —
// средняя доля по фондам); режим выбирается снаружи (`mode`). Компонент общий для
// десктопа и мобилки (мобилка: тот же порядок стеком, строки двухэтажные).

import { useMemo, useState, type CSSProperties } from 'react';
import { DONUT_COLORS, fundAssetName, fundAssetColor, resolveFundTicker } from '../../config/fundConfig';
import Donut from '../funds/Donut';
import InstrumentIcon from '../InstrumentIcon';
import { formatRubShort, formatReturnPct, returnColor } from '../funds/FundDetailModal';
import type { FundPortfolio, FundPortfolioHolding } from '../../services/api';

type PeriodKey = 'm1' | 'm3' | 'm6' | 'y1';
const PERIOD_LABEL: Record<PeriodKey, string> = { m1: '1 мес', m3: '3 мес', m6: '6 мес', y1: '1 год' };

// Слайсы пончика и видимые строки списка держим равными: наведение на любой
// сектор подсвечивает строку и наоборот, без «слепых» секторов.
const DONUT_TOP = 11;
const LIST_PREVIEW = 11;

interface Props {
    portfolio: FundPortfolio | null;
    loading: boolean;
    mode: 'rub' | 'share';        // rub = вес по деньгам, share = средняя доля по фондам
    period: PeriodKey;            // период для строки доходности
    variant?: 'desktop' | 'mobile';
}

// Белая карточка на бежевой paper-подложке — как плитки в «Составе фондов».
const cardStyle: CSSProperties = {
    background: 'var(--bg-secondary)',
    border: '1.5px solid var(--border-color)',
    borderRadius: 12,
};

function StatTile({ label, value, color }: { label: string; value: string; color?: string }) {
    return (
        <div style={{ ...cardStyle, padding: '10px 13px', minWidth: 0 }}>
            <div
                style={{
                    fontSize: 'var(--fs-2xs)',
                    fontWeight: 700,
                    textTransform: 'uppercase',
                    letterSpacing: '0.06em',
                    color: 'var(--text-muted)',
                    marginBottom: 3,
                    whiteSpace: 'nowrap',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                }}
            >
                {label}
            </div>
            <div
                style={{
                    fontSize: 'var(--fs-md)',
                    fontWeight: 800,
                    fontVariantNumeric: 'tabular-nums',
                    color: color ?? 'var(--text-primary)',
                    lineHeight: 1.1,
                    whiteSpace: 'nowrap',
                }}
            >
                {value}
            </div>
        </div>
    );
}

// Объём в колонке списка — компактно, без «₽» (валюта в шапке колонки).
function fmtVolShort(v: number): string {
    if (v >= 1e9) return `${(v / 1e9).toFixed(1)} млрд`;
    if (v >= 1e6) return `${(v / 1e6).toFixed(0)} млн`;
    return `${Math.round(v / 1e3)} тыс`;
}

// Логотип бумаги: по резолвнутому тикеру через InstrumentIcon (внутри свой
// фолбэк-круг), для нерезолвящихся бумаг — цветной круг с первой буквой.
function AssetLogo({ h, size, color }: { h: FundPortfolioHolding; size: number; color: string }) {
    const ticker = resolveFundTicker(h.asset_name, h.isin);
    if (ticker) return <InstrumentIcon sectype={ticker} size={size} rounded="full" />;
    return (
        <span
            style={{
                width: size,
                height: size,
                borderRadius: '50%',
                background: color,
                color: '#fff',
                fontSize: Math.round(size * 0.42),
                fontWeight: 800,
                display: 'inline-flex',
                alignItems: 'center',
                justifyContent: 'center',
                flexShrink: 0,
                lineHeight: 1,
            }}
        >
            {(fundAssetName(h.asset_name, h.isin).trim().charAt(0) || '?').toUpperCase()}
        </span>
    );
}

export default function CombinedPortfolioView({ portfolio, loading, mode, period, variant = 'desktop' }: Props) {
    const [expanded, setExpanded] = useState(false);
    const [hoverIdx, setHoverIdx] = useState<number | null>(null);
    const isMobile = variant === 'mobile';

    const wOf = (h: FundPortfolioHolding) => (mode === 'rub' ? h.weight_rub : h.weight_avg);

    // Сортировка по весу выбранного режима (бэкенд отдаёт по value_rub).
    const sorted = useMemo(() => {
        if (!portfolio) return [];
        return [...portfolio.holdings].sort((a, b) => wOf(b) - wOf(a));
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [portfolio, mode]);

    // Данные пончика: топ-N по весу режима + «Прочее» (точный остаток, не 100−Σ,
    // чтобы кэш в режиме средней доли не раздувал сектор).
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
        return (
            <div style={{ padding: '24px 4px', color: 'var(--text-muted)', fontSize: 'var(--fs-sm)' }}>
                Собираем общий портфель…
            </div>
        );
    }
    if (!portfolio || portfolio.num_funds === 0 || sorted.length === 0) {
        return (
            <div
                style={{
                    padding: 32,
                    textAlign: 'center',
                    background: 'var(--bg-secondary)',
                    border: '1.5px dashed var(--border-color)',
                    borderRadius: 12,
                }}
            >
                <p style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-secondary)', maxWidth: 420, margin: '0 auto', lineHeight: 1.5 }}>
                    Нет данных по выбранным управляющим компаниям. Выберите другие УК или дождитесь публикации составов.
                </p>
            </div>
        );
    }

    const ret = portfolio.returns[period];
    const maxW = Math.max(...sorted.map(wOf), 0.0001);
    const visible = expanded ? sorted : sorted.slice(0, LIST_PREVIEW);
    // Хвост за превью — строка-резюме «Прочее» в футере списка.
    const rest = sorted.slice(LIST_PREVIEW);
    const restW = rest.reduce((s, h) => s + wOf(h), 0);
    const restV = rest.reduce((s, h) => s + h.value_rub, 0);

    const kpis = (
        <>
            <StatTile label="Объём в фондах" value={formatRubShort(portfolio.total_value_rub)} />
            <StatTile
                label={`Доходность · ${PERIOD_LABEL[period]}`}
                value={formatReturnPct(ret ?? undefined)}
                color={returnColor(ret ?? undefined)}
            />
            <StatTile label="Фондов" value={String(portfolio.num_funds)} />
            <StatTile label="Бумаг" value={String(portfolio.num_assets)} />
        </>
    );

    // Колонки строки списка (десктоп): логотип · имя+тикер · бар · вес · объём · фондов.
    const rowGrid = '28px minmax(0, 1.1fr) minmax(56px, 1fr) 58px 78px 40px';

    const listRows = visible.map((h, i) => {
        const w = wOf(h);
        const color = fundAssetColor(h.asset_name, h.isin) ?? DONUT_COLORS[i % DONUT_COLORS.length];
        const pct = Math.max(2, (w / maxW) * 100);
        const onDonut = i < DONUT_TOP;
        const ticker = resolveFundTicker(h.asset_name, h.isin);
        const hovered = hoverIdx === i;
        const bar = (
            <div style={{ flex: 1, minWidth: 0, height: isMobile ? 6 : 8, background: 'color-mix(in srgb, var(--text-primary) 8%, transparent)', borderRadius: 4, overflow: 'hidden' }}>
                <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: 4 }} />
            </div>
        );
        const common = {
            key: h.akey,
            onMouseEnter: onDonut ? () => setHoverIdx(i) : undefined,
            onMouseLeave: onDonut ? () => setHoverIdx(null) : undefined,
        };
        if (isMobile) {
            // Мобилка: двухэтажная строка — имя+вес сверху, бар+объём снизу.
            return (
                <div
                    {...common}
                    style={{
                        padding: '8px 2px',
                        borderBottom: i === visible.length - 1 ? 'none' : '1px dashed color-mix(in srgb, var(--text-primary) 12%, transparent)',
                    }}
                >
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                        <AssetLogo h={h} size={22} color={color} />
                        <span style={{ flex: 1, minWidth: 0, fontSize: 'var(--fs-sm)', fontWeight: 600, color: 'var(--text-primary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                            {fundAssetName(h.asset_name, h.isin)}
                        </span>
                        <span style={{ fontVariantNumeric: 'tabular-nums', fontSize: 'var(--fs-sm)', fontWeight: 800, color: 'var(--text-primary)', flexShrink: 0 }}>
                            {w.toFixed(2)}%
                        </span>
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 5, paddingLeft: 30 }}>
                        {bar}
                        <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', flexShrink: 0, minWidth: 92, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                            {fmtVolShort(h.value_rub)} ₽ · {h.funds_holding} ф.
                        </span>
                    </div>
                </div>
            );
        }
        return (
            <div
                {...common}
                style={{
                    display: 'grid',
                    gridTemplateColumns: rowGrid,
                    gap: 10,
                    alignItems: 'center',
                    padding: '6px 8px',
                    margin: '0 -8px',
                    borderRadius: 8,
                    background: hovered ? 'color-mix(in srgb, var(--text-primary) 5%, transparent)' : 'transparent',
                    borderBottom: i === visible.length - 1 ? 'none' : '1px dashed color-mix(in srgb, var(--text-primary) 12%, transparent)',
                    transition: 'background 0.12s ease',
                }}
            >
                <AssetLogo h={h} size={26} color={color} />
                <span style={{ minWidth: 0, lineHeight: 1.15 }}>
                    <span style={{ display: 'block', fontSize: 'var(--fs-sm)', fontWeight: 700, color: 'var(--text-primary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {fundAssetName(h.asset_name, h.isin)}
                    </span>
                    {ticker && (
                        <span style={{ display: 'block', fontFamily: 'var(--font-mono, ui-monospace, monospace)', fontSize: 'var(--fs-3xs, 10px)', letterSpacing: '0.05em', color: 'var(--text-muted)' }}>
                            {ticker}
                        </span>
                    )}
                </span>
                {bar}
                <span style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums', fontSize: 'var(--fs-sm)', fontWeight: 800, color: 'var(--text-primary)' }}>
                    {w.toFixed(2)}%
                </span>
                <span style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums', fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>
                    {fmtVolShort(h.value_rub)}
                </span>
                <span style={{ textAlign: 'right', fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)' }}>
                    {h.funds_holding} ф.
                </span>
            </div>
        );
    });

    const listFooter = (
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 10, paddingTop: 10, flexWrap: 'wrap' }}>
            {!expanded && rest.length > 0 ? (
                <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', fontVariantNumeric: 'tabular-nums' }}>
                    Прочее · {rest.length} бумаг · {restW.toFixed(1)}% · {fmtVolShort(restV)} ₽
                </span>
            ) : <span />}
            {sorted.length > LIST_PREVIEW && (
                <button
                    onClick={() => setExpanded((e) => !e)}
                    className="editorial-press"
                    style={{
                        padding: '5px 14px',
                        background: 'var(--bg-primary)',
                        border: '1.5px solid var(--border-color)',
                        borderRadius: 999,
                        fontSize: 'var(--fs-xs)',
                        fontWeight: 700,
                        color: 'var(--text-secondary)',
                        cursor: 'pointer',
                    }}
                >
                    {expanded ? '↑ Свернуть' : `Все ${sorted.length} ↓`}
                </button>
            )}
        </div>
    );

    const listHeader = !isMobile && (
        <div
            style={{
                display: 'grid',
                gridTemplateColumns: rowGrid,
                gap: 10,
                padding: '6px 0',
                borderBottom: '1.5px solid var(--text-primary)',
                fontSize: 'var(--fs-3xs, 10px)',
                fontWeight: 800,
                letterSpacing: '0.07em',
                textTransform: 'uppercase',
                color: 'var(--text-muted)',
            }}
        >
            <span />
            <span>Бумага</span>
            <span />
            <span style={{ textAlign: 'right' }}>Вес</span>
            <span style={{ textAlign: 'right' }}>Объём, ₽</span>
            <span style={{ textAlign: 'right' }}>Фнд</span>
        </div>
    );

    if (isMobile) {
        // Мобилка: метрики 2×2 → пончик → список, всё стеком.
        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>{kpis}</div>
                <div style={{ ...cardStyle, padding: 14, display: 'flex', justifyContent: 'center' }}>
                    <Donut
                        holdings={donutHoldings}
                        colors={donutColors}
                        size={200}
                        outerRadius={90}
                        innerRadius={60}
                        maxSlices={donutHoldings.length}
                        centerCount={portfolio.num_assets}
                        showCenterText
                        highlightIndex={hoverIdx}
                        onHoverChange={setHoverIdx}
                    />
                </div>
                <div style={{ ...cardStyle, padding: '4px 14px 12px' }}>
                    {listRows}
                    {listFooter}
                </div>
            </div>
        );
    }

    // Десктоп: слева пончик + метрики 2×2, справа карточка-список.
    return (
        <div style={{ display: 'grid', gridTemplateColumns: 'minmax(280px, 340px) minmax(0, 1fr)', gap: 16, alignItems: 'start' }}>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                <div style={{ ...cardStyle, padding: 18, display: 'flex', justifyContent: 'center' }}>
                    <Donut
                        holdings={donutHoldings}
                        colors={donutColors}
                        size={252}
                        outerRadius={90}
                        innerRadius={60}
                        maxSlices={donutHoldings.length}
                        centerCount={portfolio.num_assets}
                        showCenterText
                        highlightIndex={hoverIdx}
                        onHoverChange={setHoverIdx}
                    />
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>{kpis}</div>
            </div>
            <div style={{ ...cardStyle, padding: '4px 16px 12px' }}>
                {listHeader}
                {listRows}
                {listFooter}
            </div>
        </div>
    );
}
