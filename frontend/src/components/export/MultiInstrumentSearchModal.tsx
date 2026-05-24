/**
 * MultiInstrumentSearchModal — multi-select picker инструментов для CSV-экспорта.
 *
 * Отличия от обычного InstrumentSearchModal:
 *   - Каждый item с чекбоксом, клик = toggle (не закрытие).
 *   - Counter "N выбрано" + sticky footer с "Готово" / "Отмена".
 *   - onConfirm(tickers) возвращает массив, не один.
 *
 * Стиль соответствует InstrumentSearchModal — поиск + категории + список.
 * Простая реализация без favorites (для CSV exports не нужно).
 */
import { useState, useEffect, useMemo } from 'react';
import { Search, X, Check } from 'lucide-react';
import { apiFetch } from '../../services/api';

interface Instrument {
    sec_id: string;
    sectype: string;
    name: string;
    type: string;
    group?: string;
}

interface Props {
    /** Уже выбранные тикеры (для pre-check). */
    initial: string[];
    /** Source данных: instruments (акции/фьючерсы/индексы из /api/instruments)
     *  ИЛИ funds (БПИФ из /api/funds/categories). */
    source?: 'instruments' | 'funds';
    /** Фильтр по типу — как в InstrumentSearchModal. (instruments mode only) */
    filterType?: 'stock' | 'futures' | 'no-futures';
    /** Заголовок modal. */
    title?: string;
    /** Максимум выбираемых элементов. Защищает backend от monster-ZIP'ов. */
    maxItems?: number;
    onConfirm: (tickers: string[]) => void;
    onClose: () => void;
}

const CATEGORY_FILTERS = [
    { key: 'all', label: 'Все' },
    { key: 'Акции', label: 'Акции', match: 'group' as const },
    { key: 'futures', label: 'Фьючерсы', match: 'type' as const },
    { key: 'Валюта', label: 'Валюта', match: 'group' as const },
    { key: 'Индексы', label: 'Индексы', match: 'group' as const },
    { key: 'Сырьё', label: 'Сырьё', match: 'group' as const },
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

    // Load — instruments или funds-categories в зависимости от source prop.
    useEffect(() => {
        let cancelled = false;
        async function load() {
            try {
                if (source === 'funds') {
                    // Funds endpoint имеет nested структуру categories[].funds[].
                    // Flatten в Instrument-like shape для общего list-renderer'а.
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
                                // Категория фонда → group (для chip-фильтра).
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
                // Лимит: если уже выбрано max, не даём добавить ещё.
                if (maxItems && next.size >= maxItems) return prev;
                next.add(sectype);
            }
            return next;
        });
    };

    // Filter logic: filterType + category + search.
    const filtered = useMemo(() => {
        return instruments.filter((inst) => {
            if (filterType === 'no-futures' && inst.type === 'futures') return false;

            // Category filter.
            if (category !== 'all') {
                const cat = CATEGORY_FILTERS.find((c) => c.key === category);
                if (cat?.match === 'type') {
                    if (inst.type !== cat.key) return false;
                } else if (cat?.match === 'group') {
                    if (inst.group !== cat.key && inst.type !== 'stock') return false;
                    // Special case "Акции" с stock-type fallback (как в MobileAssetSearch).
                    if (category === 'Акции') {
                        if (inst.group !== 'Акции' && inst.type !== 'stock') return false;
                    } else if (inst.group !== cat.key) return false;
                }
            }

            // Search.
            if (search) {
                const q = search.toLowerCase();
                return (
                    inst.sectype.toLowerCase().includes(q) ||
                    inst.name.toLowerCase().includes(q)
                );
            }
            return true;
        });
    }, [instruments, filterType, category, search]);

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
            // Очень высокий z-index — мы рендеримся ПОВЕРХ CsvExportModal.
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
                    maxWidth: 580,
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

                {/* Search */}
                <div style={{ padding: '12px 18px 8px' }}>
                    <div style={{ position: 'relative' }}>
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
                </div>

                {/* Category chips — для funds-source делаем динамически
                    из самих фондов (категории = Деньги/Акции/Облигации/Золото). */}
                <div
                    style={{
                        display: 'flex',
                        gap: 6,
                        padding: '0 18px 10px',
                        overflowX: 'auto',
                    }}
                >
                    {(source === 'funds'
                        ? [{ key: 'all', label: 'Все категории' as string, match: undefined },
                           ...Array.from(new Set(instruments.map(i => i.group).filter(Boolean)))
                               .map(g => ({ key: g!, label: g!, match: 'group' as const }))]
                        : CATEGORY_FILTERS.filter(
                            (c) => !(filterType === 'no-futures' && c.key === 'futures'),
                          )
                    ).map((c) => (
                        <button
                            key={c.key}
                            type="button"
                            onClick={() => setCategory(c.key)}
                            style={{
                                padding: '4px 10px',
                                background: category === c.key ? 'var(--accent)' : 'var(--bg-secondary)',
                                color: category === c.key ? 'var(--text-inverse)' : 'var(--text-primary)',
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
                    ))}
                </div>

                {/* List */}
                <div
                    style={{
                        flex: 1,
                        overflowY: 'auto',
                        borderTop: '1px dashed color-mix(in srgb, var(--text-primary) 15%, transparent)',
                    }}
                >
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
                                        gap: 12,
                                        width: '100%',
                                        padding: '10px 18px',
                                        background: checked
                                            ? 'color-mix(in srgb, var(--accent) 10%, transparent)'
                                            : 'transparent',
                                        border: 'none',
                                        borderBottom:
                                            '1px solid color-mix(in srgb, var(--text-primary) 8%, transparent)',
                                        cursor: limitReached ? 'not-allowed' : 'pointer',
                                        textAlign: 'left',
                                        color: 'var(--text-primary)',
                                        opacity: limitReached ? 0.4 : 1,
                                    }}
                                >
                                    <span
                                        aria-hidden="true"
                                        style={{
                                            width: 18,
                                            height: 18,
                                            borderRadius: 4,
                                            border: '1.5px solid var(--text-primary)',
                                            background: checked ? 'var(--accent)' : 'transparent',
                                            display: 'flex',
                                            alignItems: 'center',
                                            justifyContent: 'center',
                                            flexShrink: 0,
                                        }}
                                    >
                                        {checked && (
                                            <Check size={12} strokeWidth={3} color="var(--text-inverse)" />
                                        )}
                                    </span>
                                    <span style={{ flex: 1, minWidth: 0 }}>
                                        <div style={{ fontWeight: 700, fontSize: 'var(--fs-sm)' }}>
                                            {inst.sectype}
                                        </div>
                                        <div
                                            style={{
                                                fontSize: 'var(--fs-xs)',
                                                color: 'var(--text-muted)',
                                                overflow: 'hidden',
                                                textOverflow: 'ellipsis',
                                                whiteSpace: 'nowrap',
                                            }}
                                        >
                                            {inst.name}
                                        </div>
                                    </span>
                                    {inst.group && (
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
