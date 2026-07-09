// PortfolioMoversPanel — блок «Покупки фондов» рядом с обзором портфеля на вкладке
// «Общий портфель». Показывает ЧИСТУЮ покупку/продажу бумаг за выбранный период
// (месяц / полгода / год / 3 года) across выбранных фондов, консенсусом из /movers.
//
// Строки в том же формате, что список состава: логотип · имя+тикер (и сколько
// фондов двигали бумагу) · полоса · величина ₽ справа. Полоса и величина зелёные
// для докупок, красные для распродаж. Клик по строке — «Потоки по компании».

import { type CSSProperties } from 'react';
import { fundAssetName, resolveFundTicker } from '../../config/fundConfig';
import InstrumentIcon from '../InstrumentIcon';
import type { FundTradesMovers, FundTradesMover } from '../../services/api';

export type MoversPeriod = '1m' | '6m' | '1y' | '3y';
const PERIOD_SUB: Record<MoversPeriod, string> = { '1m': 'за 1 месяц', '6m': 'за полгода', '1y': 'за год', '3y': 'за 3 года' };

const GREEN = 'var(--mood-green, #4a9959)';
const RED = 'var(--mood-red, #b85645)';

interface Props {
    movers: FundTradesMovers | null;
    loading: boolean;
    period: MoversPeriod;
    variant?: 'desktop' | 'mobile';
    onAssetClick?: (m: FundTradesMover) => void;
}

const isIsin = (s?: string | null): s is string => !!s && /^[A-Z]{2}[A-Z0-9]{10}$/.test(s);

// Величина со знаком: «+1.41 млрд ₽», «−540 млн ₽».
function fmtSigned(v: number): string {
    const a = Math.abs(v);
    const s = v > 0 ? '+' : '−';
    if (a >= 1e9) return `${s}${(a / 1e9).toFixed(2)} млрд ₽`;
    if (a >= 1e6) return `${s}${(a / 1e6).toFixed(1)} млн ₽`;
    return `${s}${Math.round(a / 1e3)} тыс ₽`;
}

function MoverLogo({ m, color }: { m: FundTradesMover; color: string }) {
    const isin = isIsin(m.akey) ? m.akey : null;
    const ticker = resolveFundTicker(m.asset_name, isin);
    if (ticker) return <InstrumentIcon sectype={ticker} size={22} rounded="full" />;
    return (
        <span style={{ width: 22, height: 22, borderRadius: '50%', background: color, color: '#fff', fontSize: 10, fontWeight: 800, display: 'inline-flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0, lineHeight: 1 }}>
            {(fundAssetName(m.asset_name, isin).trim().charAt(0) || '?').toUpperCase()}
        </span>
    );
}

const blockStyle: CSSProperties = {
    background: 'var(--bg-secondary)',
    border: '2px solid var(--text-primary)',
    borderRadius: 16,
    boxShadow: '4px 4px 0 var(--text-primary)',
    padding: '13px 16px 15px',
    display: 'flex',
    flexDirection: 'column',
    minWidth: 0,
};

export default function PortfolioMoversPanel({ movers, loading, period, variant = 'desktop', onAssetClick }: Props) {
    const head = (
        <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', gap: 10, paddingBottom: 9, marginBottom: 11, borderBottom: '1.5px solid var(--text-primary)' }}>
            <span style={{ fontSize: 'var(--fs-md)', fontWeight: 800, letterSpacing: '-0.01em', color: 'var(--text-primary)' }}>Покупки фондов</span>
            <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', fontWeight: 600, whiteSpace: 'nowrap' }}>чистая покупка {PERIOD_SUB[period]}</span>
        </div>
    );

    if (loading && !movers) {
        return <div style={{ ...blockStyle, ...(variant === 'mobile' ? { padding: '12px 14px 14px' } : null) }}>{head}<div style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-sm)', padding: '10px 0' }}>Собираем движения…</div></div>;
    }

    const buys = movers?.top_accumulated ?? [];
    const sells = movers?.top_reduced ?? [];
    const empty = buys.length === 0 && sells.length === 0;
    // Общий масштаб полос — по обеим секциям, чтобы распродажи и докупки сравнивались.
    const maxAbs = Math.max(0.0001, ...[...buys, ...sells].map((m) => Math.abs(m.total_delta_amount)));

    const row = (m: FundTradesMover, positive: boolean, last: boolean) => {
        const col = positive ? GREEN : RED;
        const pct = Math.max(2, (Math.abs(m.total_delta_amount) / maxAbs) * 100);
        const isin = isIsin(m.akey) ? m.akey : null;
        const ticker = resolveFundTicker(m.asset_name, isin);
        const cnt = positive ? m.funds_buying : m.funds_selling;
        const click = onAssetClick ? () => onAssetClick(m) : undefined;
        return (
            <div
                key={m.akey}
                onClick={click}
                role={click ? 'button' : undefined}
                tabIndex={click ? 0 : undefined}
                onKeyDown={click ? (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); click(); } } : undefined}
                title={click ? `Потоки по компании: ${fundAssetName(m.asset_name, isin)}` : undefined}
                onMouseEnter={click ? (e) => { e.currentTarget.style.background = 'color-mix(in srgb, var(--text-primary) 5%, transparent)'; } : undefined}
                onMouseLeave={click ? (e) => { e.currentTarget.style.background = 'transparent'; } : undefined}
                style={{ display: 'grid', gridTemplateColumns: '22px minmax(0, 1fr) minmax(30px, 0.8fr) 88px', gap: 8, alignItems: 'center', padding: '6px 6px', margin: '0 -6px', borderRadius: 8, cursor: click ? 'pointer' : 'default', borderBottom: last ? 'none' : '1px dashed color-mix(in srgb, var(--text-primary) 12%, transparent)', transition: 'background 0.12s ease' }}
            >
                <MoverLogo m={m} color={col} />
                <span style={{ minWidth: 0, display: 'flex', flexDirection: 'column', lineHeight: 1.12 }}>
                    <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 700, color: 'var(--text-primary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '100%' }}>{fundAssetName(m.asset_name, isin)}</span>
                    <span style={{ fontFamily: 'var(--font-mono, ui-monospace, monospace)', fontSize: 'var(--fs-3xs, 10px)', letterSpacing: '0.05em', color: 'var(--text-muted)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '100%' }}>{[ticker, cnt > 0 ? `${cnt} ф.` : null].filter(Boolean).join(' · ')}</span>
                </span>
                <div style={{ height: 8, background: 'color-mix(in srgb, var(--text-primary) 8%, transparent)', borderRadius: 4, overflow: 'hidden', minWidth: 0 }}>
                    <div style={{ width: `${pct}%`, height: '100%', background: col, borderRadius: 4 }} />
                </div>
                <span style={{ textAlign: 'right', fontFamily: 'var(--font-mono, ui-monospace, monospace)', fontVariantNumeric: 'tabular-nums', fontSize: 'var(--fs-xs)', fontWeight: 800, color: col, whiteSpace: 'nowrap' }}>{fmtSigned(m.total_delta_amount)}</span>
            </div>
        );
    };

    const sectionLabel = (text: string, color: string, arrow: string, mt = 0) => (
        <div style={{ display: 'flex', alignItems: 'center', gap: 7, fontSize: 'var(--fs-xs)', fontWeight: 800, letterSpacing: '0.02em', color, margin: `${mt}px 0 3px` }}>
            <span>{arrow}</span>{text}
        </div>
    );

    return (
        <div style={{ ...blockStyle, ...(variant === 'mobile' ? { padding: '12px 14px 14px' } : null) }}>
            {head}
            {empty ? (
                <div style={{ padding: '14px 2px', color: 'var(--text-muted)', fontSize: 'var(--fs-sm)', lineHeight: 1.5 }}>
                    {movers && movers.funds_in_month === 0
                        ? 'За выбранный период у этих фондов ещё нет сопоставимого среза. Выберите период короче.'
                        : 'Нет заметных чистых движений за период.'}
                </div>
            ) : (
                <>
                    {buys.length > 0 && sectionLabel('Докупили', GREEN, '↗')}
                    {buys.slice(0, 5).map((m, i) => row(m, true, i === Math.min(5, buys.length) - 1))}
                    {sells.length > 0 && sectionLabel('Распродали', RED, '↘', 13)}
                    {sells.slice(0, 5).map((m, i) => row(m, false, i === Math.min(5, sells.length) - 1))}
                </>
            )}
        </div>
    );
}
