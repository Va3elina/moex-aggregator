// FundPicker — иерархический выбор фондов для /fund-trades: список ФОНДОВ,
// СГРУППИРОВАННЫХ ПО УК. Заголовок-группа = аватар УК (UK_LOGOS) + имя УК;
// под ним фонды-строки: чекбокс(multi)/радио(single) + тикер + короткое имя.
//
// Стиль повторяет popover «Категории» из CbrFlowsPage (editorial border 1.5px
// var(--text-primary) + hard-shadow, rich-строки) и паттерн поповера/аватара из
// UkMultiSelect: fixed-backdrop zIndex 19 + absolute popup zIndex 20, закрытие
// по клику вне. Каждая строка с hover-анимацией (bg highlight + лёгкий сдвиг).
//
// mode='multi'  (Потоки):   чекбоксы, множественный выбор; кнопка «N фондов» / «Все фонды».
// mode='single' (Снапшот):  радио, ровно один фонд; кнопка = «тикер · имя» с аватаром УК.

import { useMemo, useState } from 'react';
import { UK_LOGOS } from '../../config/fundConfig';

export interface FundPickerFund {
    ticker: string;
    name: string;
    uk?: string | null;
    uk_id?: number | string | null;
}

export interface FundPickerProps {
    funds: FundPickerFund[];
    mode: 'multi' | 'single';
    selected: Set<string>;          // ключ = ticker; для single — 0..1 элемент
    onChange: (next: Set<string>) => void;
    buttonLabel?: (n: number, total: number) => string; // текст кнопки
    minWidth?: number;
}

// Аватар УК: круг ~size px. Картинка из UK_LOGOS[uk_id].img либо буква на цветном
// bg. Лого ищем по uk_id (приоритет), затем по имени УК. Нет записи → нейтральный
// кружок с первой буквой имени УК.
function UkAvatar({
    ukId,
    ukName,
    size = 22,
}: {
    ukId?: number | string | null;
    ukName?: string | null;
    size?: number;
}) {
    const logo =
        (ukId != null ? UK_LOGOS[String(ukId)] : undefined) ??
        (ukName ? UK_LOGOS[ukName] : undefined);
    const fallbackLetter = (ukName ?? '').trim().charAt(0).toUpperCase() || '?';
    return (
        <div
            style={{
                width: size,
                height: size,
                borderRadius: '50%',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                flexShrink: 0,
                overflow: 'hidden',
                fontWeight: 900,
                fontSize: Math.round(size * 0.5),
                lineHeight: 1,
                backgroundColor: logo
                    ? (logo.img ? undefined : logo.bg)
                    : 'var(--bg-secondary)',
                color: logo ? logo.color : 'var(--text-secondary)',
            }}
        >
            {logo
                ? (logo.img
                    ? <img src={logo.img} alt={logo.name} style={{ width: '100%', height: '100%', objectFit: 'cover' }} />
                    : logo.letter)
                : fallbackLetter}
        </div>
    );
}

interface UkGroup {
    key: string;            // стабильный ключ группы (uk_id ?? uk-имя ?? '—')
    ukId?: number | string | null;
    name: string;           // отображаемое имя УК (UK_LOGOS.name → uk-поле → «Прочие»)
    funds: FundPickerFund[];
}

// Группировка фондов по УК: ключ группы — uk_id (если есть), иначе имя УК.
// Имя берём из UK_LOGOS[uk_id].name (канон), затем из поля uk, иначе «Прочие».
function groupByUk(funds: FundPickerFund[]): UkGroup[] {
    const order: string[] = [];
    const map = new Map<string, UkGroup>();
    for (const f of funds) {
        const groupKey = f.uk_id != null ? `id:${f.uk_id}` : `nm:${(f.uk ?? '').trim() || '—'}`;
        let g = map.get(groupKey);
        if (!g) {
            const logo = f.uk_id != null ? UK_LOGOS[String(f.uk_id)] : undefined;
            const name = logo?.name ?? (f.uk ?? '').trim() ?? '';
            g = {
                key: groupKey,
                ukId: f.uk_id,
                name: name || 'Прочие',
                funds: [],
            };
            map.set(groupKey, g);
            order.push(groupKey);
        }
        g.funds.push(f);
    }
    return order.map((k) => map.get(k)!);
}

export default function FundPicker({
    funds,
    mode,
    selected,
    onChange,
    buttonLabel,
    minWidth = 220,
}: FundPickerProps) {
    const [open, setOpen] = useState(false);
    const [hover, setHover] = useState<string | null>(null);

    const groups = useMemo(() => groupByUk(funds), [funds]);
    const total = funds.length;
    const allActive = mode === 'multi' && selected.size === 0;

    // Выбранный фонд (single) — для метки кнопки с аватаром УК.
    const selectedFund = useMemo<FundPickerFund | null>(() => {
        if (mode !== 'single' || selected.size === 0) return null;
        const t = selected.values().next().value as string | undefined;
        return funds.find((f) => f.ticker === t) ?? null;
    }, [mode, selected, funds]);

    // Текст кнопки. Кастомный buttonLabel имеет приоритет в multi.
    const label = (() => {
        if (mode === 'single') {
            if (!selectedFund) return 'Выбрать фонд';
            return `${selectedFund.ticker} · ${selectedFund.name}`;
        }
        if (buttonLabel) return buttonLabel(selected.size, total);
        return allActive ? 'Все фонды' : `${selected.size} фондов`;
    })();

    const active = mode === 'single' ? !!selectedFund : !allActive;

    const toggleMulti = (ticker: string) => {
        const next = new Set(selected);
        if (next.has(ticker)) next.delete(ticker);
        else next.add(ticker);
        onChange(next);
    };

    const pickSingle = (ticker: string) => {
        onChange(new Set([ticker]));
        setOpen(false);
    };

    return (
        <div style={{ position: 'relative', display: 'inline-flex' }}>
            <button
                type="button"
                onClick={() => setOpen((o) => !o)}
                className="editorial-press"
                style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: 6,
                    padding: '6px 14px',
                    maxWidth: 280,
                    background: active ? 'var(--accent)' : 'var(--bg-secondary)',
                    color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
                    border: '2px solid var(--text-primary)',
                    borderRadius: 999,
                    fontSize: 'var(--fs-xs)',
                    fontWeight: active ? 700 : 600,
                    cursor: 'pointer',
                    boxShadow: active ? '3px 3px 0 var(--text-primary)' : 'none',
                    whiteSpace: 'nowrap',
                }}
            >
                {mode === 'single' && selectedFund && (
                    <UkAvatar ukId={selectedFund.uk_id} ukName={selectedFund.uk} size={18} />
                )}
                <span
                    style={{
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                    }}
                >
                    {label}
                </span>
                <span style={{ fontSize: '0.75em', opacity: 0.85, flexShrink: 0 }}>▾</span>
            </button>

            {open && (
                <>
                    {/* fixed-backdrop — закрытие по клику вне (как UkMultiSelect / меню периода) */}
                    <div
                        onClick={() => setOpen(false)}
                        style={{ position: 'fixed', inset: 0, zIndex: 19 }}
                    />
                    <div
                        style={{
                            position: 'absolute',
                            top: 'calc(100% + 6px)',
                            left: 0,
                            zIndex: 20,
                            display: 'flex',
                            flexDirection: 'column',
                            minWidth,
                            maxWidth: 'min(360px, calc(100vw - 32px))',
                            maxHeight: 360,
                            overflowY: 'auto',
                            background: 'var(--bg-primary)',
                            border: '1.5px solid var(--text-primary)',
                            borderRadius: 10,
                            boxShadow: '3px 3px 0 var(--text-primary)',
                        }}
                    >
                        {/* «Все фонды» — только в multi, сброс selected в пусто */}
                        {mode === 'multi' && (
                            <button
                                type="button"
                                onClick={() => { onChange(new Set()); setOpen(false); }}
                                onMouseEnter={() => setHover('__all__')}
                                onMouseLeave={() => setHover(null)}
                                style={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    gap: 8,
                                    padding: '9px 14px',
                                    textAlign: 'left',
                                    cursor: 'pointer',
                                    background: allActive
                                        ? 'var(--bg-secondary)'
                                        : (hover === '__all__' ? 'color-mix(in srgb, var(--text-primary) 6%, transparent)' : 'transparent'),
                                    color: 'var(--text-primary)',
                                    border: 'none',
                                    borderBottom: '1px solid var(--border-color)',
                                    fontSize: 'var(--fs-xs)',
                                    fontWeight: allActive ? 700 : 500,
                                    whiteSpace: 'nowrap',
                                    transform: hover === '__all__' ? 'translateX(2px)' : 'translateX(0)',
                                    transition: 'background 150ms, transform 150ms',
                                }}
                            >
                                <span style={{ width: 16, textAlign: 'center', opacity: allActive ? 1 : 0 }}>✓</span>
                                Все фонды
                            </button>
                        )}

                        {groups.map((g) => (
                            <div key={g.key}>
                                {/* Заголовок-группа УК: аватар + имя (sticky-подобный визуал) */}
                                <div
                                    style={{
                                        display: 'flex',
                                        alignItems: 'center',
                                        gap: 8,
                                        padding: '8px 14px 6px',
                                        background: 'var(--bg-secondary)',
                                        borderTop: '1px solid var(--border-color)',
                                        borderBottom: '1px solid var(--border-color)',
                                    }}
                                >
                                    <UkAvatar ukId={g.ukId} ukName={g.name} size={20} />
                                    <span
                                        style={{
                                            fontSize: 'var(--fs-xs)',
                                            fontWeight: 800,
                                            color: 'var(--text-secondary)',
                                            textTransform: 'uppercase',
                                            letterSpacing: '0.02em',
                                            overflow: 'hidden',
                                            textOverflow: 'ellipsis',
                                            whiteSpace: 'nowrap',
                                        }}
                                    >
                                        {g.name}
                                    </span>
                                </div>

                                {/* Фонды-строки: чекбокс(multi)/радио(single) + тикер + имя */}
                                {g.funds.map((f) => {
                                    const on = selected.has(f.ticker);
                                    const isHover = hover === f.ticker;
                                    return (
                                        <button
                                            key={f.ticker}
                                            type="button"
                                            onClick={() => (mode === 'multi' ? toggleMulti(f.ticker) : pickSingle(f.ticker))}
                                            onMouseEnter={() => setHover(f.ticker)}
                                            onMouseLeave={() => setHover(null)}
                                            style={{
                                                display: 'flex',
                                                alignItems: 'center',
                                                gap: 8,
                                                width: '100%',
                                                padding: '8px 14px',
                                                textAlign: 'left',
                                                cursor: 'pointer',
                                                background: on
                                                    ? 'var(--bg-secondary)'
                                                    : (isHover ? 'color-mix(in srgb, var(--text-primary) 6%, transparent)' : 'transparent'),
                                                color: 'var(--text-primary)',
                                                border: 'none',
                                                fontSize: 'var(--fs-xs)',
                                                fontWeight: on ? 700 : 500,
                                                whiteSpace: 'nowrap',
                                                transform: isHover ? 'translateX(2px)' : 'translateX(0)',
                                                transition: 'background 150ms, transform 150ms',
                                            }}
                                        >
                                            {/* Индикатор выбора: квадрат-чекбокс (multi) / круг-радио (single) */}
                                            <span
                                                style={{
                                                    width: 16,
                                                    height: 16,
                                                    flexShrink: 0,
                                                    display: 'flex',
                                                    alignItems: 'center',
                                                    justifyContent: 'center',
                                                    borderRadius: mode === 'single' ? '50%' : 4,
                                                    background: on ? 'var(--accent)' : 'transparent',
                                                    border: `2px solid ${on ? 'var(--accent)' : 'var(--text-muted)'}`,
                                                    transition: 'all 150ms',
                                                }}
                                            >
                                                {on && (mode === 'single'
                                                    ? <span style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--text-inverse)' }} />
                                                    : (
                                                        <svg viewBox="0 0 14 14" width="10" height="10" fill="none" stroke="var(--text-inverse)" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                                                            <polyline points="2 7 6 11 12 3" />
                                                        </svg>
                                                    ))}
                                            </span>
                                            <span
                                                style={{
                                                    fontFamily: 'var(--font-mono, monospace)',
                                                    fontWeight: 700,
                                                    flexShrink: 0,
                                                    color: 'var(--text-primary)',
                                                }}
                                            >
                                                {f.ticker}
                                            </span>
                                            <span
                                                style={{
                                                    overflow: 'hidden',
                                                    textOverflow: 'ellipsis',
                                                    color: 'var(--text-secondary)',
                                                }}
                                                title={f.name}
                                            >
                                                {f.name}
                                            </span>
                                        </button>
                                    );
                                })}
                            </div>
                        ))}

                        {groups.length === 0 && (
                            <div
                                style={{
                                    padding: '14px',
                                    fontSize: 'var(--fs-xs)',
                                    color: 'var(--text-secondary)',
                                    textAlign: 'center',
                                }}
                            >
                                Нет фондов
                            </div>
                        )}
                    </div>
                </>
            )}
        </div>
    );
}
