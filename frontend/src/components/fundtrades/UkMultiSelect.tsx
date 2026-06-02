// Общий мультиселект УК для /fund-trades — аватар (UK_LOGOS) + имя + чекбокс.
// Кнопка: «Все УК» (пусто) либо «N из M УК». Множественный выбор (toggle в Set).
// Попап повторяет меню периода «Доходность» в FundTradesPage:
// fixed-backdrop zIndex 19 + absolute popup zIndex 20, editorial border 1.5px
// var(--text-primary) + boxShadow '3px 3px 0 var(--text-primary)'. Закрытие по
// клику вне. Editorial-токены var(--*).

import { useState } from 'react';
import { UK_LOGOS } from '../../config/fundConfig';

export interface UkOption {
    key: string;
    name: string;
    uk_id?: number | string | null;
}

export interface UkMultiSelectProps {
    options: UkOption[];
    selected: Set<string>;          // пусто = «Все»
    onChange: (next: Set<string>) => void;
    allLabel?: string;              // default «Все УК»
    minWidth?: number;
}

// Аватар УК ~22px: картинка из UK_LOGOS[...].img либо буква на цветном bg.
// Лого ищем по uk_id (приоритет), затем по key. Нет записи → нейтральный кружок.
function UkAvatar({ opt, size = 22 }: { opt: UkOption; size?: number }) {
    const logo = UK_LOGOS[String(opt.uk_id ?? opt.key)] ?? UK_LOGOS[opt.key];
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
                : (opt.name.trim().charAt(0).toUpperCase() || '?')}
        </div>
    );
}

export default function UkMultiSelect({
    options,
    selected,
    onChange,
    allLabel = 'Все УК',
    minWidth = 150,
}: UkMultiSelectProps) {
    const [open, setOpen] = useState(false);

    const allActive = selected.size === 0;
    const buttonLabel = allActive
        ? allLabel
        : `${selected.size} из ${options.length} УК`;

    const toggle = (key: string) => {
        const next = new Set(selected);
        if (next.has(key)) next.delete(key);
        else next.add(key);
        onChange(next);
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
                    gap: 4,
                    padding: '6px 14px',
                    background: allActive ? 'var(--bg-secondary)' : 'var(--accent)',
                    color: allActive ? 'var(--text-primary)' : 'var(--text-inverse)',
                    border: '2px solid var(--text-primary)',
                    borderRadius: 999,
                    fontSize: 'var(--fs-xs)',
                    fontWeight: allActive ? 600 : 700,
                    cursor: 'pointer',
                    boxShadow: allActive ? 'none' : '3px 3px 0 var(--text-primary)',
                    whiteSpace: 'nowrap',
                }}
            >
                {buttonLabel}
                <span style={{ fontSize: '0.75em', opacity: 0.85 }}>▾</span>
            </button>

            {open && (
                <>
                    {/* fixed-backdrop — закрытие по клику вне (как меню периода) */}
                    <div
                        onClick={() => setOpen(false)}
                        style={{ position: 'fixed', inset: 0, zIndex: 19 }}
                    />
                    <div
                        style={{
                            position: 'absolute', top: 'calc(100% + 6px)', left: 0, zIndex: 20,
                            display: 'flex', flexDirection: 'column', minWidth,
                            maxHeight: 320, overflowY: 'auto',
                            background: 'var(--bg-primary)', border: '1.5px solid var(--text-primary)',
                            borderRadius: 10, boxShadow: '3px 3px 0 var(--text-primary)',
                        }}
                    >
                        {/* «Все УК» — сброс selected в пусто, active если пусто */}
                        <button
                            type="button"
                            onClick={() => { onChange(new Set()); setOpen(false); }}
                            style={{
                                display: 'flex', alignItems: 'center', gap: 8,
                                padding: '9px 14px', textAlign: 'left', cursor: 'pointer',
                                background: allActive ? 'var(--bg-secondary)' : 'transparent',
                                color: 'var(--text-primary)', border: 'none',
                                borderBottom: '1px solid var(--border-color)',
                                fontSize: 'var(--fs-xs)', fontWeight: allActive ? 700 : 500,
                                whiteSpace: 'nowrap',
                            }}
                        >
                            <span style={{ width: 16, textAlign: 'center', opacity: allActive ? 1 : 0 }}>✓</span>
                            {allLabel}
                        </button>

                        {options.map((opt) => {
                            const on = selected.has(opt.key);
                            return (
                                <button
                                    key={opt.key}
                                    type="button"
                                    onClick={() => toggle(opt.key)}
                                    style={{
                                        display: 'flex', alignItems: 'center', gap: 8,
                                        padding: '8px 14px', textAlign: 'left', cursor: 'pointer',
                                        background: on ? 'var(--bg-secondary)' : 'transparent',
                                        color: 'var(--text-primary)', border: 'none',
                                        fontSize: 'var(--fs-xs)', fontWeight: on ? 700 : 500,
                                        whiteSpace: 'nowrap',
                                    }}
                                >
                                    <span style={{ width: 16, textAlign: 'center', opacity: on ? 1 : 0 }}>✓</span>
                                    <UkAvatar opt={opt} />
                                    {opt.name}
                                </button>
                            );
                        })}
                    </div>
                </>
            )}
        </div>
    );
}
