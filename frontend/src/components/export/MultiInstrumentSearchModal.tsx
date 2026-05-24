/**
 * MultiInstrumentSearchModal — multi-select picker инструментов для CSV-экспорта.
 *
 * Стилистически идентичен InstrumentSearchModal (тот же InstrumentIcon,
 * day_change_pct, объём), но с чекбоксами вместо click-to-select и sticky
 * footer'ом "N выбрано · Готово".
 *
 * Поддерживает source='funds' для CSV экспорта БПИФ — грузит /api/funds/
 * categories вместо /api/instruments и не показывает category-chips
 * (chip-row для funds = 4 категории, отдельный layout).
 */
import { useState, useEffect, useMemo } from 'react';
import { Search, X, Check } from 'lucide-react';
import { apiFetch } from '../../services/api';
import InstrumentIcon from '../InstrumentIcon';
import { formatCompact } from '../../utils/formatNumber';

interface Instrument {
    sec_id: string;
    sectype: string;
    name: string;
    type: string;
    group?: string;
    daily_volume?: number;
    day_change_pct?: number | null;
}

interface Props {
    /** Уже выбранные тикеры (для pre-check). */
    initial: string[];
    /** Source данных. */
    source?: 'instruments' | 'funds';
    /** Фильтр по типу — только для instruments source. */
    filterType?: 'stock' | 'futures' | 'no-futures';
    /** Заголовок modal. */
    title?: string;
    /** Максимум выбираемых элементов. */
    maxItems?: number;
    onConfirm: (tickers: string[]) => void;
    onClose: () => void;
}

// Категории для instruments. Match column: 'group' (Акции/Валюта/...) или
// 'type' (futures). Список выровнен с CATEGORY_FILTERS из обычного modal'а.
const INST_CATEGORIES: { key: string; label: string; match?: 'group' | 'type' }[] = [
    { key: 'all', label: 'Все' },
    { key: 'Акции', label: 'Акции', match: 'group' },
    { key: 'futures', label: 'Фьючерсы', match: 'type' },
    { key: 'Валюта', label: 'Валюта', match: 'group' },
    { key: 'Индексы', label: 'Индексы', match: 'group' },
    { key: 'Сырьё', label: 'Сырьё', match: 'group' },
];

export default function MultiInstrumentSearchModal({
    initial,
    source = 'instruments',
    filterType,
    title = 'Выберите инструменты',
    maxItems,
    onConfirm,
    onClose,
}: Props) {
    const [instruments, setInstruments] = useState<Instrument[]>([]);
    const [loading, setLoading] = useState(true);
    const [search, setSearch] = useState('');
    const [category, setCategory] = useState('all');
    const [selected, setSelected] = useState<Set<string>>(() => new Set(initial));

    // Esc + body scroll lock.
    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape') onClose();
        };
        document.addEventListener('keydown', onKey);
        const prev = document.body.style.overflow;
        document.body.style.overflow = 'hidden';
        return () => {
            document.removeEventListener('keydown', onKey);
            document.body.style.overflow = prev;
        };
    }, [onClose]);

    useEffect(() => {
        let cancelled = false;
        async function load() {
            try {
                if (source === 'funds') {
                    const resp = await apiFetch('/api/funds/categories');
                    const data = await resp.json();
                    const flat: Instrument[] = [];
                    for (const cat of data.categories ?? []) {
                        for (const f of cat.funds ?? []) {
                            flat.push({
                                sec_id: String(f.id ?? f.ticker),
                                sectype: f.ticker,
                                name: f.name ?? f.ticker,
                                type: 'fund',
                                group: cat.name ?? cat.key,
                            });
                        }
                    }
                    if (!cancelled) setInstruments(flat);
                } else if (filterType === 'stock' || filterType === 'futures') {
                    const resp = await apiFetch(`/api/instruments?type=${filterType}`);
                    const data = await resp.json();
                    if (!cancelled) setInstruments(data.instruments || []);
                } else {
                    const resp = await apiFetch('/api/instruments');
                    const data = await resp.json();
                    if (!cancelled) setInstruments(data.instruments || []);
                }
            } catch (e) {
                console.error('[MultiInstrumentSearchModal] load failed:', e);
            } finally {
                if (!cancelled) setLoading(false);
            }
        }
        load();
        return () => { cancelled = true; };
    }, [source, filterType]);

    const toggle = (sectype: string) => {
        setSelected((prev) => {
            const next = new Set(prev);
            if (next.has(sectype)) {
                next.delete(sectype);
            } else {
                if (maxItems && next.size >= maxItems) return prev;
                next.add(sectype);
            }
            return next;
        });
    };

    // Категории для chip-row — динамика для funds, статика для instruments.
    const categories = useMemo(() => {
        if (source === 'funds') {
            const uniqueCats = Array.from(
                new Set(instruments.map((i) => i.group).filter(Boolean)),
            ) as string[];
            return [
                { key: 'all', label: 'Все', match: undefined as 'group' | 'type' | undefined },
                ...uniqueCats.map((g) => ({ key: g, label: g, match: 'group' as const })),
            ];
        }
        return INST_CATEGORIES.filter(
            (c) => !(filterType === 'no-futures' && c.key === 'futures'),
        );
    }, [source, filterType, instruments]);

    // Filter.
    const filtered = useMemo(() => {
        return instruments.filter((inst) => {
            if (filterType === 'no-futures' && inst.type === 'futures') return false;

            if (category !== 'all') {
                const cat = categories.find((c) => c.key === category);
                if (cat?.match === 'type' && inst.type !== cat.key) return false;
                if (cat?.match === 'group') {
                    // Special: "Акции" chip также пускает stock-type без group.
                    if (category === 'Акции') {
                        if (inst.group !== 'Акции' && inst.type !== 'stock') return false;
                    } else if (inst.group !== cat.key) return false;
                }
            }

            if (search) {
                const q = search.toLowerCase();
                return (
                    inst.sectype.toLowerCase().includes(q) ||
                    inst.name.toLowerCase().includes(q)
                );
            }
            return true;
        });
    }, [instruments, filterType, category, categories, search]);

    // Unique по sectype.
    const unique = useMemo(() => {
        const seen = new Set<string>();
        return filtered.filter((i) => {
            if (seen.has(i.sectype)) return false;
            seen.add(i.sectype);
            return true;
        });
    }, [filtered]);

    return (
        <div
            onClick={(e) => {
                if (e.target === e.currentTarget) onClose();
            }}
            style={{
                position: 'fixed',
                inset: 0,
                background: 'color-mix(in srgb, var(--text-primary) 60%, transparent)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                zIndex: 2000,
                padding: 16,
            }}
        >
            <div
                role="dialog"
                aria-modal="true"
                style={{
                    background: 'var(--bg-primary)',
                    border: '2px solid var(--text-primary)',
                    borderRadius: 16,
                    boxShadow: '6px 6px 0 var(--text-primary)',
                    width: '100%',
                    maxWidth: 620,
                    maxHeight: '85vh',
                    display: 'flex',
                    flexDirection: 'column',
                }}
            >
                {/* Header */}
                <div
                    style={{
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                        padding: '14px 18px',
                        borderBottom: '1.5px solid var(--text-primary)',
                        flexShrink: 0,
                    }}
                >
                    <h3
                        style={{
                            fontSize: 'var(--fs-md)',
                            fontWeight: 800,
                            color: 'var(--text-primary)',
                            margin: 0,
                        }}
                    >
                        {title}
                    </h3>
                    <button
                        onClick={onClose}
                        aria-label="Закрыть"
                        style={{
                            background: 'transparent',
                            border: 'none',
                            cursor: 'pointer',
                            color: 'var(--text-primary)',
                        }}
                    >
                        <X size={18} />
                    </button>
                </div>

                {/* Sticky search + categories */}
                <div
                    style={{
                        padding: '12px 18px 8px',
                        background: 'var(--bg-primary)',
                        borderBottom: '1px solid color-mix(in srgb, var(--text-primary) 10%, transparent)',
                        flexShrink: 0,
                    }}
                >
                    <div style={{ position: 'relative', marginBottom: 10 }}>
                        <Search
                            size={14}
                            style={{
                                position: 'absolute',
                                left: 12,
                                top: '50%',
                                transform: 'translateY(-50%)',
                                color: 'var(--text-muted)',
                            }}
                        />
                        <input
                            value={search}
                            onChange={(e) => setSearch(e.target.value)}
                            placeholder="Поиск тикера или названия..."
                            style={{
                                width: '100%',
                                padding: '8px 12px 8px 32px',
                                background: 'var(--bg-secondary)',
                                border: '1.5px solid var(--text-primary)',
                                borderRadius: 10,
                                color: 'var(--text-primary)',
                                fontSize: 'var(--fs-sm)',
                            }}
                        />
                    </div>

                    {/* Category chips — wrap, not horizontal scroll (фикс обрезания) */}
                    <div
                        style={{
                            display: 'flex',
                            flexWrap: 'wrap',
                            gap: 6,
                        }}
                    >
                        {categories.map((c) => {
                            const active = category === c.key;
                            return (
                                <button
                                    key={c.key}
                                    type="button"
                                    onClick={() => setCategory(c.key)}
                                    style={{
                                        padding: '4px 12px',
                                        background: active ? 'var(--accent)' : 'var(--bg-secondary)',
                                        color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
                                        border: '1.5px solid var(--text-primary)',
                                        borderRadius: 999,
                                        fontSize: 'var(--fs-xs)',
                                        fontWeight: 600,
                                        cursor: 'pointer',
                                        whiteSpace: 'nowrap',
                                    }}
                                >
                                    {c.label}
                                </button>
                            );
                        })}
                    </div>
                </div>

                {/* List */}
                <div style={{ flex: 1, overflowY: 'auto', padding: '4px 8px' }}>
                    {loading ? (
                        <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-muted)' }}>
                            Загрузка...
                        </div>
                    ) : unique.length === 0 ? (
                        <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-muted)' }}>
                            Не найдено
                        </div>
                    ) : (
                        unique.map((inst) => {
                            const checked = selected.has(inst.sectype);
                            const limitReached = !!maxItems && !checked && selected.size >= maxItems;
                            return (
                                <button
                                    key={inst.sectype}
                                    type="button"
                                    onClick={() => toggle(inst.sectype)}
                                    disabled={limitReached}
                                    style={{
                                        display: 'flex',
                                        alignItems: 'center',
                                        gap: 14,
                                        width: '100%',
                                        padding: '10px 12px',
                                        background: checked
                                            ? 'color-mix(in srgb, var(--accent) 10%, transparent)'
                                            : 'transparent',
                                        border: 'none',
                                        borderRadius: 10,
                                        cursor: limitReached ? 'not-allowed' : 'pointer',
                                        textAlign: 'left',
                                        color: 'var(--text-primary)',
                                        opacity: limitReached ? 0.4 : 1,
                                        transition: 'background 0.12s',
                                    }}
                                >
                                    {/* Checkbox */}
                                    <span
                                        aria-hidden="true"
                                        style={{
                                            width: 20,
                                            height: 20,
                                            borderRadius: 5,
                                            border: '1.5px solid var(--text-primary)',
                                            background: checked ? 'var(--accent)' : 'transparent',
                                            display: 'flex',
                                            alignItems: 'center',
                                            justifyContent: 'center',
                                            flexShrink: 0,
                                        }}
                                    >
                                        {checked && (
                                            <Check size={13} strokeWidth={3} color="var(--text-inverse)" />
                                        )}
                                    </span>

                                    {/* Icon — pretty логотип/sprite для тикера */}
                                    <InstrumentIcon sectype={inst.sectype} size={32} />

                                    {/* Ticker + name */}
                                    <div
                                        style={{
                                            display: 'flex',
                                            alignItems: 'center',
                                            gap: 8,
                                            flex: 1,
                                            minWidth: 0,
                                        }}
                                    >
                                        <span style={{ fontWeight: 700, fontSize: 'var(--fs-sm)' }}>
                                            {inst.sectype}
                                        </span>
                                        <span
                                            style={{
                                                color: 'var(--text-secondary)',
                                                fontSize: 'var(--fs-xs)',
                                                overflow: 'hidden',
                                                textOverflow: 'ellipsis',
                                                whiteSpace: 'nowrap',
                                            }}
                                        >
                                            {inst.name}
                                        </span>
                                    </div>

                                    {/* Day change % + volume */}
                                    {(inst.day_change_pct != null || inst.daily_volume) && (
                                        <div
                                            style={{
                                                textAlign: 'right',
                                                lineHeight: 1.2,
                                                flexShrink: 0,
                                                minWidth: 64,
                                            }}
                                        >
                                            {inst.day_change_pct != null && (
                                                <div
                                                    style={{
                                                        fontSize: 'var(--fs-xs)',
                                                        fontWeight: 600,
                                                        color: inst.day_change_pct >= 0
                                                            ? 'var(--funds-flow-positive)'
                                                            : 'var(--funds-flow-negative)',
                                                    }}
                                                >
                                                    {inst.day_change_pct >= 0 ? '+' : ''}
                                                    {inst.day_change_pct.toFixed(2)}%
                                                </div>
                                            )}
                                            {inst.daily_volume ? (
                                                <div style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)' }}>
                                                    {formatCompact(inst.daily_volume)}
                                                </div>
                                            ) : null}
                                        </div>
                                    )}

                                    {/* Group badge — только если нет day_change (для fund-mode) */}
                                    {inst.day_change_pct == null && inst.group && (
                                        <span
                                            style={{
                                                fontSize: 'var(--fs-2xs)',
                                                color: 'var(--text-muted)',
                                                padding: '2px 6px',
                                                background: 'var(--bg-secondary)',
                                                borderRadius: 4,
                                                flexShrink: 0,
                                            }}
                                        >
                                            {inst.group}
                                        </span>
                                    )}
                                </button>
                            );
                        })
                    )}
                </div>

                {/* Footer */}
                <div
                    style={{
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                        gap: 10,
                        padding: '12px 18px',
                        borderTop: '1.5px solid var(--text-primary)',
                        background: 'var(--bg-primary)',
                        flexShrink: 0,
                    }}
                >
                    <span style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-secondary)' }}>
                        Выбрано:{' '}
                        <strong
                            style={{
                                color: maxItems && selected.size >= maxItems
                                    ? 'var(--accent)'
                                    : 'var(--text-primary)',
                            }}
                        >
                            {selected.size}
                        </strong>
                        {maxItems ? ` / ${maxItems}` : ''}
                    </span>
                    <div style={{ display: 'flex', gap: 8 }}>
                        <button
                            onClick={onClose}
                            style={{
                                padding: '6px 14px',
                                background: 'var(--bg-secondary)',
                                border: '1.5px solid var(--text-primary)',
                                borderRadius: 999,
                                color: 'var(--text-primary)',
                                fontSize: 'var(--fs-sm)',
                                fontWeight: 700,
                                cursor: 'pointer',
                            }}
                        >
                            Отмена
                        </button>
                        <button
                            onClick={() => onConfirm(Array.from(selected))}
                            disabled={selected.size === 0}
                            style={{
                                padding: '6px 14px',
                                background: 'var(--accent)',
                                color: 'var(--text-inverse)',
                                border: '1.5px solid var(--text-primary)',
                                borderRadius: 999,
                                fontSize: 'var(--fs-sm)',
                                fontWeight: 700,
                                cursor: selected.size === 0 ? 'not-allowed' : 'pointer',
                                boxShadow: 'var(--shadow-hard-chip)',
                                opacity: selected.size === 0 ? 0.5 : 1,
                            }}
                        >
                            Готово
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
}
