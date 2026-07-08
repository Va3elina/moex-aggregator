// CombinedPortfolioView — презентационный компонент «Общего портфеля»: KPI-плашка
// (суммарная СЧА, стоимость акций, nav-взвешенная доходность, число фондов/бумаг) +
// пончик агрегированного состава + ранжированный список бумаг с весами и рублями.
//
// Веса приходят с бэкенда в двух режимах (weight_rub — по деньгам, weight_avg —
// средняя доля по фондам); режим выбирается снаружи (`mode`). Компонент общий для
// десктопа и мобилки (десктоп: пончик слева + список справа; мобилка: стек). Пончик
// и его формат переиспользуются из «Покупок фондов» (Donut + fundConfig-цвета).

import { useMemo, useState } from 'react';
import { DONUT_COLORS, fundAssetName, fundAssetColor } from '../../config/fundConfig';
import Donut from '../funds/Donut';
import { formatRubShort, formatReturnPct, returnColor } from '../funds/FundDetailModal';
import type { FundPortfolio, FundPortfolioHolding } from '../../services/api';

type PeriodKey = 'm1' | 'm3' | 'm6' | 'y1';
const PERIOD_LABEL: Record<PeriodKey, string> = { m1: '1 мес', m3: '3 мес', m6: '6 мес', y1: '1 год' };

// Сколько бумаг рисуем секторами пончика; остальное сворачивается в «Прочее».
const DONUT_TOP = 12;
// Сколько строк списка показываем до «Показать все».
const LIST_PREVIEW = 15;

interface Props {
    portfolio: FundPortfolio | null;
    loading: boolean;
    mode: 'rub' | 'share';        // rub = вес по деньгам, share = средняя доля по фондам
    period: PeriodKey;            // период для строки доходности
    variant?: 'desktop' | 'mobile';
}

function StatTile({ label, value, color }: { label: string; value: string; color?: string }) {
    return (
        <div
            style={{
                flex: '1 1 128px',
                minWidth: 108,
                padding: '11px 14px',
                background: 'var(--bg-secondary)',
                border: '1.5px solid var(--border-color)',
                borderRadius: 12,
            }}
        >
            <div
                style={{
                    fontSize: 'var(--fs-2xs)',
                    fontWeight: 700,
                    textTransform: 'uppercase',
                    letterSpacing: '0.06em',
                    color: 'var(--text-muted)',
                    marginBottom: 4,
                    whiteSpace: 'nowrap',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                }}
            >
                {label}
            </div>
            <div
                style={{
                    fontSize: 'var(--fs-lg)',
                    fontWeight: 800,
                    fontVariantNumeric: 'tabular-nums',
                    color: color ?? 'var(--text-primary)',
                    lineHeight: 1.1,
                }}
            >
                {value}
            </div>
        </div>
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

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: isMobile ? 16 : 20 }}>
            {/* KPI-плашка */}
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10 }}>
                <StatTile label="Суммарная СЧА" value={formatRubShort(portfolio.total_nav_rub)} />
                <StatTile label="Стоимость акций" value={formatRubShort(portfolio.total_value_rub)} />
                <StatTile
                    label={`Доходность · ${PERIOD_LABEL[period]}`}
                    value={formatReturnPct(ret ?? undefined)}
                    color={returnColor(ret ?? undefined)}
                />
                <StatTile label="Фондов" value={String(portfolio.num_funds)} />
                <StatTile label="Бумаг" value={String(portfolio.num_assets)} />
            </div>

            {/* Пончик + ранжированный список */}
            <div
                style={{
                    display: 'flex',
                    flexDirection: isMobile ? 'column' : 'row',
                    gap: isMobile ? 16 : 24,
                    alignItems: isMobile ? 'center' : 'flex-start',
                }}
            >
                <div style={{ flexShrink: 0, lineHeight: 0 }}>
                    <Donut
                        holdings={donutHoldings}
                        colors={donutColors}
                        size={isMobile ? 200 : 240}
                        outerRadius={90}
                        innerRadius={60}
                        maxSlices={donutHoldings.length}
                        centerCount={portfolio.num_assets}
                        showCenterText
                        highlightIndex={hoverIdx}
                        onHoverChange={setHoverIdx}
                    />
                </div>
                <div style={{ flex: 1, minWidth: 0, width: isMobile ? '100%' : undefined }}>
                    {visible.map((h, i) => {
                        const w = wOf(h);
                        const color = fundAssetColor(h.asset_name, h.isin) ?? DONUT_COLORS[i % DONUT_COLORS.length];
                        const pct = Math.max(2, (w / maxW) * 100);
                        const onDonut = i < DONUT_TOP; // строки топ-N связаны с секторами пончика
                        return (
                            <div
                                key={h.akey}
                                onMouseEnter={onDonut ? () => setHoverIdx(i) : undefined}
                                onMouseLeave={onDonut ? () => setHoverIdx(null) : undefined}
                                style={{
                                    padding: '8px 8px',
                                    margin: '0 -8px',
                                    borderRadius: 8,
                                    background: hoverIdx === i ? 'color-mix(in srgb, var(--text-primary) 5%, transparent)' : 'transparent',
                                    borderBottom: i === visible.length - 1
                                        ? 'none'
                                        : '1px dashed color-mix(in srgb, var(--text-primary) 12%, transparent)',
                                    transition: 'background 0.12s ease',
                                }}
                            >
                                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                    <span style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-muted)', fontFamily: 'ui-monospace, monospace', flexShrink: 0, minWidth: 20 }}>
                                        {i + 1}.
                                    </span>
                                    <span style={{ width: 9, height: 9, borderRadius: '50%', flexShrink: 0, backgroundColor: color }} />
                                    <span
                                        style={{
                                            flex: 1,
                                            minWidth: 0,
                                            fontSize: 'var(--fs-sm)',
                                            color: 'var(--text-primary)',
                                            fontWeight: 500,
                                            overflow: 'hidden',
                                            textOverflow: 'ellipsis',
                                            whiteSpace: 'nowrap',
                                        }}
                                    >
                                        {fundAssetName(h.asset_name, h.isin)}
                                    </span>
                                    <span style={{ fontVariantNumeric: 'tabular-nums', fontSize: 'var(--fs-sm)', fontWeight: 700, color: 'var(--text-primary)', flexShrink: 0 }}>
                                        {w.toFixed(2)}%
                                    </span>
                                </div>
                                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 5, paddingLeft: 28 }}>
                                    <div style={{ flex: 1, height: 6, background: 'color-mix(in srgb, var(--text-primary) 8%, transparent)', borderRadius: 3, overflow: 'hidden' }}>
                                        <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: 3 }} />
                                    </div>
                                    <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', flexShrink: 0, minWidth: 100, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                                        {formatRubShort(h.value_rub)} · {h.funds_holding} ф.
                                    </span>
                                </div>
                            </div>
                        );
                    })}
                    {sorted.length > LIST_PREVIEW && (
                        <button
                            onClick={() => setExpanded((e) => !e)}
                            className="editorial-press"
                            style={{
                                marginTop: 10,
                                padding: '5px 14px',
                                background: 'var(--bg-secondary)',
                                border: '1.5px solid var(--border-color)',
                                borderRadius: 999,
                                fontSize: 'var(--fs-xs)',
                                fontWeight: 600,
                                color: 'var(--text-secondary)',
                                cursor: 'pointer',
                            }}
                        >
                            {expanded ? '↑ Свернуть' : `Показать все · ${sorted.length} ↓`}
                        </button>
                    )}
                </div>
            </div>
        </div>
    );
}
