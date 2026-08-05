// PortfolioTreemap — карта «Структура» состава портфеля (slice-and-dice треемап).
//
// Общий блок для «Общего портфеля» (CombinedPortfolioView) и карточки фонда
// (FundDetailModal): топ-10 бумаг раскладываются рядами 2·3·3·2, «Прочие» —
// отдельной полосой снизу. ПЛОЩАДЬ плитки пропорциональна её весу: высота ряда
// ∝ сумме весов ряда, ширина плитки внутри ряда ∝ её весу → area = rowH × width
// ∝ вес. Строки отсортированы по убыванию, поэтому верхние ряды крупнее.
// TM_H_BUDGET — суммарная высота рядов (px), TM_ROW_MIN — минимум на ряд для
// читаемости мелких строк.
//
// Наведение связано с таблицей бумаг снаружи (hoverIdx/onHoverChange), индекс —
// позиция бумаги в отсортированном списке.

import type { CSSProperties } from 'react';

export const TREEMAP_TOP = 10;
const TM_CHUNKS = [2, 3, 3, 2];
const TM_H_BUDGET = 300;
const TM_ROW_MIN = 46;

export interface TreemapItem {
    key: string;
    label: string;    // подпись плитки (тикер или первое слово имени)
    name: string;     // полное имя бумаги — для title
    weight: number;   // вес в процентах
    color: string;
}

// Тёмный текст на светлых брендовых заливках (золото Полюса и т.п.).
export function isLightHex(color: string): boolean {
    if (!/^#[0-9a-fA-F]{6}$/.test(color)) return false;
    const r = parseInt(color.slice(1, 3), 16), g = parseInt(color.slice(3, 5), 16), b = parseInt(color.slice(5, 7), 16);
    return 0.2126 * r + 0.7152 * g + 0.0722 * b > 160;
}

interface Props {
    items: TreemapItem[];          // топ-N бумаг (уже отсортированы по убыванию веса)
    restWeight: number;            // суммарный вес хвоста → полоса «Прочие бумаги»
    hoverIdx?: number | null;
    onHoverChange?: (idx: number | null) => void;
    onItemClick?: (idx: number) => void;
    clickHint?: string;            // хвост тултипа для кликабельной плитки
    style?: CSSProperties;
}

export default function PortfolioTreemap({ items, restWeight, hoverIdx, onHoverChange, onItemClick, clickHint, style }: Props) {
    const topSum = Math.max(items.reduce((s, it) => s + it.weight, 0), 0.0001);
    const rows: { it: TreemapItem; idx: number }[][] = [];
    let cursor = 0;
    for (const size of TM_CHUNKS) {
        if (cursor >= items.length) break;
        rows.push(items.slice(cursor, cursor + size).map((it, j) => ({ it, idx: cursor + j })));
        cursor += size;
    }

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6, ...style }}>
            {rows.map((row, ri) => {
                const rowSum = row.reduce((s, { it }) => s + it.weight, 0);
                // Высота ряда ∝ его суммарному весу (доля от бюджета высоты), но не
                // ниже минимума — иначе нижние лёгкие ряды становятся нечитаемыми.
                const rowH = Math.max(TM_ROW_MIN, Math.round(TM_H_BUDGET * rowSum / topSum));
                return (
                    <div key={ri} style={{ display: 'flex', gap: 6, height: rowH }}>
                        {row.map(({ it, idx }) => {
                            const dark = isLightHex(it.color);
                            const click = onItemClick ? () => onItemClick(idx) : undefined;
                            return (
                                <div
                                    key={it.key}
                                    onMouseEnter={() => onHoverChange?.(idx)}
                                    onMouseLeave={() => onHoverChange?.(null)}
                                    onClick={click}
                                    role={click ? 'button' : undefined}
                                    tabIndex={click ? 0 : undefined}
                                    onKeyDown={click ? (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); click(); } } : undefined}
                                    title={`${it.name} · ${it.weight.toFixed(2)}%${click && clickHint ? ` — ${clickHint}` : ''}`}
                                    // Ширина ∝ весу (flex). minWidth — мягкий пол, чтобы очень
                                    // мелкая плитка сохранила подпись; ряды отсортированы, поэтому
                                    // соседи близки по весу и пол почти не искажает пропорции.
                                    // Подпись прижата к верхнему левому углу (justify-start),
                                    // padding симметричный со всех сторон (отступ сверху = слева).
                                    // lineHeight:1 + gap дают гарантированный зазор: при
                                    // минимальной высоте ряда (46px) 8+14+2+11+8=43 < 46, тикер
                                    // и % не наезжают.
                                    style={{ flex: Math.max(it.weight, 0.1), minWidth: 44, borderRadius: 10, background: it.color, color: dark ? '#1a1712' : '#fff', padding: 8, display: 'flex', flexDirection: 'column', justifyContent: 'flex-start', gap: 2, overflow: 'hidden', outline: hoverIdx === idx ? '2px solid var(--text-primary)' : 'none', outlineOffset: -2, transition: 'outline-color 0.12s ease', cursor: click ? 'pointer' : 'default' }}
                                >
                                    <div style={{ fontWeight: 800, fontSize: 'var(--fs-sm)', lineHeight: 1, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{it.label}</div>
                                    <div style={{ fontSize: 'var(--fs-2xs)', fontWeight: 600, lineHeight: 1, opacity: 0.92, fontVariantNumeric: 'tabular-nums' }}>{it.weight.toFixed(1).replace('.', ',')}%</div>
                                </div>
                            );
                        })}
                    </div>
                );
            })}
            {restWeight > 0.5 && (
                <div style={{ height: 42, borderRadius: 10, background: 'color-mix(in srgb, var(--text-primary) 16%, var(--bg-primary))', color: 'var(--text-primary)', padding: '0 12px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8 }}>
                    <span style={{ fontWeight: 700, fontSize: 'var(--fs-xs)' }}>Прочие бумаги</span>
                    <span style={{ fontWeight: 700, fontSize: 'var(--fs-xs)', fontVariantNumeric: 'tabular-nums' }}>{restWeight.toFixed(1).replace('.', ',')}%</span>
                </div>
            )}
        </div>
    );
}
