// PortfolioMoversPanel — блок «Покупки фондов» рядом с обзором портфеля на вкладке
// «Общий портфель». Показывает ЧИСТУЮ покупку/продажу бумаг за выбранный период
// (месяц / полгода / год / 3 года) across выбранных фондов, консенсусом из /movers.
//
// Редизайн (июль 2026, макет Claude Design): переключатель периода 1М/6М/1Г/3Г
// живёт в шапке блока (prop onPeriodChange), под заголовком — фактический
// диапазон месяцев консенсуса («январь – июль 2026»). Полосы
// нейтральные, величины тёмные: направление читается секциями «Чистые покупки ↑» /
// «Чистые продажи ↓», единица (млрд/млн ₽) приглушена отдельным спаном.
//
// variant='embedded' — без собственной карточки (блок внутри общей карточки
// вкладки). Клик по строке — «Потоки по компании».

import { type CSSProperties } from 'react';
import { fundAssetName, resolveFundTicker } from '../../config/fundConfig';
import InstrumentIcon from '../InstrumentIcon';
import SegmentedControl from '../SegmentedControl';
import Skeleton from '../Skeleton';
import type { FundTradesMovers, FundTradesMover } from '../../services/api';

export type MoversPeriod = '1m' | '6m' | '1y' | '3y';
const PERIOD_SUB: Record<MoversPeriod, string> = { '1m': 'за 1 месяц', '6m': 'за полгода', '1y': 'за год', '3y': 'за 3 года' };
const PERIOD_MONTHS: Record<MoversPeriod, number> = { '1m': 1, '6m': 6, '1y': 12, '3y': 36 };

interface Props {
    movers: FundTradesMovers | null;
    loading: boolean;
    period: MoversPeriod;
    variant?: 'desktop' | 'mobile' | 'embedded';
    onAssetClick?: (m: FundTradesMover) => void;
    // Задан — сегменты 1М/6М/1Г/3Г рендерятся в шапке блока (десктоп-макет).
    // Мобилка управляет периодом своими чипами в ⚙️-sheet.
    onPeriodChange?: (p: MoversPeriod) => void;
}

const isIsin = (s?: string | null): s is string => !!s && /^[A-Z]{2}[A-Z0-9]{10}$/.test(s);

// Единица одна на весь блок (по максимуму) и вынесена в подзаголовок — не в
// каждую строку. Масштаб задаёт делитель и число знаков: млрд 2, млн 1, тыс 0.
function scaleOf(maxAbs: number): { unit: string; div: number; dec: number } {
    if (maxAbs >= 1e9) return { unit: 'млрд ₽', div: 1e9, dec: 2 };
    if (maxAbs >= 1e6) return { unit: 'млн ₽', div: 1e6, dec: 1 };
    return { unit: 'тыс ₽', div: 1e3, dec: 0 };
}

// Величина со знаком в общей единице блока: «+9.02», «−4.39».
function fmtSignedNum(v: number, div: number, dec: number): string {
    return `${v > 0 ? '+' : '−'}${(Math.abs(v) / div).toFixed(dec)}`;
}

const MONTHS_LOWER = ['янв', 'фев', 'мар', 'апр', 'май', 'июн',
    'июл', 'авг', 'сен', 'окт', 'ноя', 'дек'];

// Длинное имя обрезаем не многоточием, а плавным затуханием справа (fade-out).
const FADE_MASK = 'linear-gradient(to right, #000 0, #000 calc(100% - 20px), transparent 100%)';
const nameFade: CSSProperties = {
    overflow: 'hidden', whiteSpace: 'nowrap',
    maskImage: FADE_MASK, WebkitMaskImage: FADE_MASK,
};

// Диапазон консенсуса: конец = resolved_month, начало = минус длина периода.
// «янв – июл 2026», через границу года — «окт 2025 – июл 2026».
function monthRangeLabel(endISO: string, period: MoversPeriod): string {
    const end = new Date(endISO);
    const start = new Date(end.getFullYear(), end.getMonth() - PERIOD_MONTHS[period], 1);
    const sM = MONTHS_LOWER[start.getMonth()], eM = MONTHS_LOWER[end.getMonth()];
    if (start.getFullYear() === end.getFullYear()) return `${sM} – ${eM} ${end.getFullYear()}`;
    return `${sM} ${start.getFullYear()} – ${eM} ${end.getFullYear()}`;
}

function MoverLogo({ m, size }: { m: FundTradesMover; size: number }) {
    const isin = isIsin(m.akey) ? m.akey : null;
    const ticker = resolveFundTicker(m.asset_name, isin);
    if (ticker) return <InstrumentIcon sectype={ticker} size={size} rounded="full" />;
    return (
        <span style={{ width: size, height: size, borderRadius: '50%', background: 'var(--text-muted)', color: '#fff', fontSize: Math.round(size * 0.42), fontWeight: 800, display: 'inline-flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0, lineHeight: 1 }}>
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

export default function PortfolioMoversPanel({ movers, loading, period, variant = 'desktop', onAssetClick, onPeriodChange }: Props) {
    const isMobile = variant === 'mobile';
    const embedded = variant === 'embedded';
    // embedded — без собственной карточки: рамку несёт общая карточка вкладки.
    const wrapStyle: CSSProperties = embedded
        ? { display: 'flex', flexDirection: 'column', minWidth: 0 }
        : { ...blockStyle, ...(isMobile ? { padding: '12px 14px 14px' } : null) };

    const buys = movers?.top_accumulated ?? [];
    const sells = movers?.top_reduced ?? [];
    const absVals = [...buys, ...sells].map((m) => Math.abs(m.total_delta_amount));
    const hasData = absVals.length > 0;
    // Общий масштаб полос — по обеим секциям, чтобы распродажи и докупки сравнивались.
    const maxAbs = Math.max(0.0001, ...absVals);
    const scale = scaleOf(maxAbs);

    // Подзаголовок — только диапазон месяцев консенсуса («апр – май 2026»).
    // Единица (млрд/млн ₽) живёт рядом с легендами секций, не здесь.
    const sub = movers?.resolved_month
        ? monthRangeLabel(movers.resolved_month, period)
        : PERIOD_SUB[period];

    const head = (
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 10, flexWrap: 'wrap', marginBottom: 4 }}>
            {/* Заголовок + период стопкой, выровнены по верху — текст не провисает
                относительно высокого сегмент-контрола справа. */}
            <div style={{ display: 'flex', flexDirection: 'column', minWidth: 0 }}>
                <span style={{ fontSize: 'var(--fs-lg)', fontWeight: 800, letterSpacing: '-0.01em', lineHeight: 1.1, color: 'var(--text-primary)' }}>Покупки фондов</span>
                <span style={{ fontSize: 'var(--fs-xs)', fontWeight: 600, color: 'var(--text-muted)', marginTop: 2 }}>{sub}</span>
            </div>
            {onPeriodChange && (
                <SegmentedControl<MoversPeriod>
                    options={[
                        { key: '1m', label: '1М' },
                        { key: '6m', label: '6М' },
                        { key: '1y', label: '1Г' },
                        { key: '3y', label: '3Г' },
                    ]}
                    value={period}
                    onChange={onPeriodChange}
                />
            )}
        </div>
    );

    if (loading && !movers) {
        // Высота skeleton'а держит примерный размер финального списка (до 5
        // строк «Покупки» + до 5 строк «Продажи»), чтобы контейнер не «прыгал».
        return (
            <div style={wrapStyle}>
                {head}
                <Skeleton height={420} rounded="lg" />
            </div>
        );
    }

    const empty = buys.length === 0 && sells.length === 0;

    const row = (m: FundTradesMover, last: boolean) => {
        const pct = Math.max(2, (Math.abs(m.total_delta_amount) / maxAbs) * 100);
        const isin = isIsin(m.akey) ? m.akey : null;
        const num = fmtSignedNum(m.total_delta_amount, scale.div, scale.dec);
        const click = onAssetClick ? () => onAssetClick(m) : undefined;
        return (
            <div
                key={m.akey}
                onClick={click}
                role={click ? 'button' : undefined}
                tabIndex={click ? 0 : undefined}
                onKeyDown={click ? (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); click(); } } : undefined}
                title={click ? `По бумаге: ${fundAssetName(m.asset_name, isin)}` : undefined}
                onMouseEnter={click ? (e) => { e.currentTarget.style.background = 'color-mix(in srgb, var(--text-primary) 5%, transparent)'; } : undefined}
                onMouseLeave={click ? (e) => { e.currentTarget.style.background = 'transparent'; } : undefined}
                style={{ display: 'grid', gridTemplateColumns: '30px 112px minmax(40px, 1fr) max-content', gap: 10, alignItems: 'center', padding: '7px 6px', margin: '0 -6px', borderRadius: 8, cursor: click ? 'pointer' : 'default', borderBottom: last ? 'none' : '1px dashed color-mix(in srgb, var(--text-primary) 12%, transparent)', transition: 'background 0.12s ease' }}
            >
                <MoverLogo m={m} size={30} />
                <span style={{ minWidth: 0, fontSize: 'var(--fs-md)', fontWeight: 600, color: 'var(--text-primary)', ...nameFade }}>{fundAssetName(m.asset_name, isin)}</span>
                {/* Полосы нейтральные — направление задают секции «Чистые покупки/продажи». */}
                <div style={{ height: 9, background: 'color-mix(in srgb, var(--text-primary) 8%, transparent)', borderRadius: 5, overflow: 'hidden', minWidth: 0 }}>
                    <div style={{ width: `${pct}%`, height: '100%', background: 'color-mix(in srgb, var(--text-primary) 32%, transparent)', borderRadius: 5 }} />
                </div>
                <span style={{ textAlign: 'right', whiteSpace: 'nowrap', fontVariantNumeric: 'tabular-nums', fontSize: 'var(--fs-sm)', fontWeight: 600, color: 'var(--text-secondary)' }}>{num}</span>
            </div>
        );
    };

    const sectionLabel = (text: string, up: boolean, mt = 0) => (
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 7, fontSize: 'var(--fs-sm)', fontWeight: 800, letterSpacing: '0.02em', color: 'var(--text-primary)', margin: `${mt}px 0 4px` }}>
            {text}<span style={{ fontSize: '0.95em', fontWeight: 700 }}>{up ? '↑' : '↓'}</span>
            {hasData && <span style={{ fontSize: 'var(--fs-xs)', fontWeight: 600, letterSpacing: 0, color: 'var(--text-muted)' }}>({scale.unit})</span>}
        </div>
    );

    return (
        <div style={wrapStyle}>
            {head}
            {empty ? (
                <div style={{ padding: '14px 2px', color: 'var(--text-muted)', fontSize: 'var(--fs-sm)', lineHeight: 1.5 }}>
                    Нет данных за этот период по выбранным УК. Выберите другие управляющие компании.
                </div>
            ) : (
                <>
                    {buys.length > 0 && sectionLabel('Чистые покупки', true)}
                    {buys.slice(0, 5).map((m, i) => row(m, i === Math.min(5, buys.length) - 1))}
                    {sells.length > 0 && sectionLabel('Чистые продажи', false, 16)}
                    {sells.slice(0, 5).map((m, i) => row(m, i === Math.min(5, sells.length) - 1))}
                </>
            )}
        </div>
    );
}
