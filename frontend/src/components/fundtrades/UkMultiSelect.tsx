// Общий мультиселект УК для /fund-trades — аватар (UK_LOGOS) + имя + чекбокс.
// Кнопка: «Все УК» (пусто) либо «N из M УК». Множественный выбор (toggle в Set).
// Стиль повторяет popover «Категории» в CbrFlowsPage: editorial-карточка
// (border 2px var(--text-primary) + hard-shadow), header с title + счётчик,
// RICH-строки (цветной чекбокс 22px + аватар УК + имя) в единой левой сетке,
// hover-анимация строк (bg highlight + лёгкий сдвиг, transition). Закрытие по
// клику вне (fixed-backdrop). Editorial-токены var(--*).

import { useState } from 'react';
import { UK_LOGOS } from '../../config/fundConfig';
import { useViewportWidth } from '../../hooks/useViewportWidth';
import { X } from 'lucide-react';

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
    // Размер кнопки-триггера: 'sm' (как sort-пилюли, default) | 'md' (крупнее —
    // чтобы выделить фильтр среди мелких кнопок). Влияет только на padding/fontSize.
    size?: 'sm' | 'md';
}

// Аватар УК ~24px: картинка из UK_LOGOS[...].img либо буква на цветном bg.
// Лого ищем по uk_id (приоритет), затем по key. Нет записи → нейтральный кружок.
function UkAvatar({ opt, size = 24 }: { opt: UkOption; size?: number }) {
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

// Цветной чекбокс 22px в стиле CbrFlows: заполненный accent-квадрат с белой
// галкой когда выбран; пустой контур var(--text-muted) когда нет.
function CheckBox({ on }: { on: boolean }) {
    return (
        <div
            style={{
                width: 22,
                height: 22,
                flexShrink: 0,
                borderRadius: 6,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                background: on ? 'var(--accent)' : 'transparent',
                border: `2px solid ${on ? 'var(--accent)' : 'var(--text-muted)'}`,
                transition: 'all 150ms',
            }}
        >
            {on && (
                <svg viewBox="0 0 14 14" width="12" height="12" fill="none" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                    <polyline points="2 7 6 11 12 3" />
                </svg>
            )}
        </div>
    );
}

export default function UkMultiSelect({
    options,
    selected,
    onChange,
    allLabel = 'Все УК',
    minWidth = 150,
    size = 'sm',
}: UkMultiSelectProps) {
    const [open, setOpen] = useState(false);
    const [hovered, setHovered] = useState<string | null>(null);
    // На мобиле absolute-дропдаун (top:100%) уезжал за низ экрана — кнопка
    // стоит у дна ⚙️-sheet'а. Рендерим как фикс-панель поверх sheet'а (z-index
    // 101), как модалка FundPicker. `.fm-sheet` без постоянного transform →
    // position:fixed корректно цепляется к viewport.
    const isMobile = useViewportWidth() < 768;

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
                    padding: size === 'md' ? '8px 18px' : '6px 14px',
                    background: allActive ? 'var(--bg-secondary)' : 'var(--accent)',
                    color: allActive ? 'var(--text-primary)' : 'var(--text-inverse)',
                    border: '2px solid var(--text-primary)',
                    borderRadius: 999,
                    fontSize: size === 'md' ? 'var(--fs-sm)' : 'var(--fs-xs)',
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
                    {/* Backdrop — закрытие по клику вне. Мобила: полупрозрачный
                        ПОВЕРХ sheet'а; десктоп: прозрачный catcher. */}
                    <div
                        onClick={() => setOpen(false)}
                        style={{
                            position: 'fixed', inset: 0,
                            zIndex: isMobile ? 110 : 19,
                            background: isMobile ? 'rgba(0,0,0,0.5)' : 'transparent',
                        }}
                    />
                    <div
                        style={
                            isMobile
                                ? {
                                      // Фикс-панель снизу, поверх ⚙️-sheet'а (.fm-sheet z-index 101).
                                      position: 'fixed', left: 12, right: 12, bottom: 16, zIndex: 111,
                                      maxHeight: '70vh', overflowY: 'auto',
                                      background: 'var(--bg-primary)', border: '2px solid var(--text-primary)',
                                      borderRadius: 16,
                                      boxShadow: 'var(--shadow-hard-card, 6px 6px 0 var(--text-primary))',
                                  }
                                : {
                                      position: 'absolute', top: 'calc(100% + var(--sp-2))', left: 0, zIndex: 20,
                                      minWidth, width: 'max-content', maxWidth: 'min(360px, calc(100vw - 32px))',
                                      maxHeight: '70vh', overflowY: 'auto',
                                      background: 'var(--bg-primary)', border: '2px solid var(--text-primary)',
                                      borderRadius: 16,
                                      boxShadow: 'var(--shadow-hard-card, 6px 6px 0 var(--text-primary))',
                                  }
                        }
                    >
                        {/* Header popover — title + счётчик; на мобиле + кнопка ✕
                            (закрытие не только по backdrop'у) + sticky, чтобы X
                            оставался виден при скролле списка. */}
                        <div
                            className="flex items-center justify-between border-b border-theme"
                            style={{
                                padding: 'var(--sp-3) var(--sp-4)',
                                position: isMobile ? 'sticky' : undefined,
                                top: isMobile ? 0 : undefined,
                                background: 'var(--bg-primary)',
                            }}
                        >
                            <span
                                className="font-bold"
                                style={{ fontSize: 'var(--fs-base)', color: 'var(--text-primary)' }}
                            >
                                {allLabel}
                            </span>
                            <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--sp-3)' }}>
                                <span
                                    className="font-mono font-bold"
                                    style={{ fontSize: 'var(--fs-xs)', color: 'var(--accent)' }}
                                >
                                    {allActive ? options.length : selected.size}/{options.length}
                                </span>
                                {isMobile && (
                                    <button
                                        type="button"
                                        onClick={() => setOpen(false)}
                                        aria-label="Закрыть"
                                        style={{
                                            display: 'flex', alignItems: 'center', justifyContent: 'center',
                                            width: 44, height: 44, flexShrink: 0,
                                            borderRadius: 8,
                                            border: '1.5px solid var(--text-primary)',
                                            background: 'var(--bg-secondary)',
                                            color: 'var(--text-primary)',
                                            cursor: 'pointer',
                                        }}
                                    >
                                        <X size={16} strokeWidth={2.4} />
                                    </button>
                                )}
                            </div>
                        </div>

                        {/* Items list — единый left-grid: чекбокс 22px + аватар + имя */}
                        <div style={{ padding: 'var(--sp-2)' }}>
                            {/* «Все УК» — сброс selected в пусто, active если пусто */}
                            <button
                                type="button"
                                onClick={() => { onChange(new Set()); setOpen(false); }}
                                onMouseEnter={() => setHovered('__all__')}
                                onMouseLeave={() => setHovered(null)}
                                className="w-full text-left rounded-xl"
                                style={{
                                    display: 'flex', alignItems: 'center', gap: 'var(--sp-3)',
                                    padding: 'var(--sp-3)', marginBottom: 'var(--sp-1)',
                                    cursor: 'pointer', border: '1.5px solid transparent',
                                    background: allActive
                                        ? 'color-mix(in srgb, var(--text-primary) 4%, transparent)'
                                        : hovered === '__all__'
                                            ? 'color-mix(in srgb, var(--text-primary) 7%, transparent)'
                                            : 'transparent',
                                    borderColor: allActive
                                        ? 'color-mix(in srgb, var(--text-primary) 12%, transparent)'
                                        : 'transparent',
                                    transform: hovered === '__all__' ? 'translateX(2px)' : 'translateX(0)',
                                    transition: 'all 150ms',
                                }}
                            >
                                <CheckBox on={allActive} />
                                <span
                                    className="font-bold"
                                    style={{
                                        fontSize: 'var(--fs-sm)', color: 'var(--text-primary)',
                                        whiteSpace: 'nowrap',
                                    }}
                                >
                                    {allLabel}
                                </span>
                            </button>

                            {options.map((opt) => {
                                const on = selected.has(opt.key);
                                const isHover = hovered === opt.key;
                                return (
                                    <button
                                        key={opt.key}
                                        type="button"
                                        onClick={() => toggle(opt.key)}
                                        onMouseEnter={() => setHovered(opt.key)}
                                        onMouseLeave={() => setHovered(null)}
                                        className="w-full text-left rounded-xl"
                                        style={{
                                            display: 'flex', alignItems: 'center', gap: 'var(--sp-3)',
                                            padding: 'var(--sp-3)', marginBottom: 'var(--sp-1)',
                                            cursor: 'pointer', border: '1.5px solid transparent',
                                            background: on
                                                ? 'color-mix(in srgb, var(--text-primary) 4%, transparent)'
                                                : isHover
                                                    ? 'color-mix(in srgb, var(--text-primary) 7%, transparent)'
                                                    : 'transparent',
                                            borderColor: on
                                                ? 'color-mix(in srgb, var(--text-primary) 12%, transparent)'
                                                : 'transparent',
                                            transform: isHover ? 'translateX(2px)' : 'translateX(0)',
                                            transition: 'all 150ms',
                                        }}
                                    >
                                        <CheckBox on={on} />
                                        <UkAvatar opt={opt} />
                                        <span
                                            className="font-semibold flex-1 min-w-0 truncate"
                                            style={{
                                                fontSize: 'var(--fs-sm)', color: 'var(--text-primary)',
                                                fontWeight: on ? 700 : 600,
                                            }}
                                            title={opt.name}
                                        >
                                            {opt.name}
                                        </span>
                                    </button>
                                );
                            })}
                        </div>
                    </div>
                </>
            )}
        </div>
    );
}
